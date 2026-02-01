// Package sstable is an on-disk table: sorted series keys, JSON metadata
// (metric+labels) per series, Gorilla-compressed (ts, value) blocks, and
// (format v2) postings and label-value indexes before a fixed trailer.
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/blockcache"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
	"github.com/vyshnavi-d-p-3/helios/pkg/gorilla"
)

var fileMagicV1 = []byte{'H', 'S', 'S', 'T', 1, 0, 0, 0}

type seriesMeta struct {
	Metric string            `json:"m"`
	Labels map[string]string `json:"l,omitempty"`
}

// WriteFromMemtable writes a memtable to path (key order: series, then time)
// using format v2: data blocks, postings, label values, and a v2 trailer.
// An empty memtable is an error.
func WriteFromMemtable(path string, m *memtable.Memtable) error {
	if m.Len() == 0 {
		return errors.New("sstable: empty memtable")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	groups, minT, maxT, err := buildGroups(m)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(fileMagicV2); err != nil {
		return err
	}
	if err := writeI64(f, minT); err != nil {
		return err
	}
	if err := writeI64(f, maxT); err != nil {
		return err
	}
	if err := writeU32(f, uint32(len(groups))); err != nil {
		return err
	}
	for _, g := range groups {
		if err := writeSeriesBlock(f, g); err != nil {
			return err
		}
	}
	postingsOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	pm, lv := buildPostingsAndLabelValues(groups)
	if err := writeV2Postings(f, pm); err != nil {
		return err
	}
	lvOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := writeV2LabelValues(f, lv); err != nil {
		return err
	}
	if err := writeV2Trailer(f, postingsOff, lvOff); err != nil {
		return err
	}
	return f.Sync()
}

func buildGroups(m *memtable.Memtable) ([][]storage.Sample, int64, int64, error) {
	var groups [][]storage.Sample
	var cur *storage.RowKey
	var curGroup []storage.Sample
	var minT, maxT int64
	firstT := true
	m.ForEach(func(s storage.Sample) bool {
		rk := storage.RowKeyOf(s)
		if firstT {
			minT, maxT, firstT = s.Timestamp, s.Timestamp, false
		} else {
			if s.Timestamp < minT {
				minT = s.Timestamp
			}
			if s.Timestamp > maxT {
				maxT = s.Timestamp
			}
		}
		if cur == nil || cur.Series != rk.Series {
			if cur != nil && len(curGroup) > 0 {
				cp := make([]storage.Sample, len(curGroup))
				copy(cp, curGroup)
				groups = append(groups, cp)
			}
			k := rk
			cur = &k
			curGroup = []storage.Sample{s}
		} else {
			curGroup = append(curGroup, s)
		}
		return true
	})
	if cur != nil && len(curGroup) > 0 {
		cp := make([]storage.Sample, len(curGroup))
		copy(cp, curGroup)
		groups = append(groups, cp)
	}
	if len(groups) == 0 {
		return nil, 0, 0, errors.New("sstable: no data")
	}
	return groups, minT, maxT, nil
}

func writeSeriesBlock(f *os.File, samples []storage.Sample) error {
	if len(samples) == 0 {
		return errors.New("sstable: empty group")
	}
	s0 := samples[0]
	series := storage.SeriesKey(s0.Metric, s0.Labels)
	meta, err := json.Marshal(seriesMeta{Metric: s0.Metric, Labels: s0.Labels})
	if err != nil {
		return err
	}
	enc := gorilla.NewEncoder()
	for i := range samples {
		enc.Encode(samples[i].Timestamp, samples[i].Value)
	}
	blob := enc.Finish()
	if err := writeU32(f, uint32(len(series))); err != nil {
		return err
	}
	if _, err := io.WriteString(f, series); err != nil {
		return err
	}
	if err := writeU32(f, uint32(len(meta))); err != nil {
		return err
	}
	if _, err := f.Write(meta); err != nil {
		return err
	}
	if err := writeU32(f, uint32(len(samples))); err != nil {
		return err
	}
	if err := writeU32(f, uint32(len(blob))); err != nil {
		return err
	}
	_, err = f.Write(blob)
	return err
}

func writeI64(f *os.File, v int64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	_, err := f.Write(b[:])
	return err
}

func writeU32(f *os.File, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := f.Write(b[:])
	return err
}

func readI64(f io.Reader) (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

func readU32(f io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

// Table is a read-only SST with an in-memory series key index. Format v2 also
// loads postings and label-value metadata for set-style queries.
type Table struct {
	path        string
	file        *os.File // open for block reads; closed by Close
	id          uint64
	version     uint8
	minT        int64
	maxT        int64
	series      []string
	off         []int64
	postings    map[string][]uint64
	labelValues map[string][]string
	cache       *blockcache.Cache
}

// Version is the on-disk table format (1 = blocks only, 2 = postings + label index).
func (t *Table) Version() int { return int(t.version) }

// PostingList returns a copy of series IDs for a postings key "name=value", or nil.
func (t *Table) PostingList(key string) []uint64 {
	if t == nil || t.postings == nil {
		return nil
	}
	ids := t.postings[key]
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint64, len(ids))
	copy(out, ids)
	return out
}

// LabelValues returns a copy of distinct values for a label name, or nil.
func (t *Table) LabelValues(name string) []string {
	if t == nil || t.labelValues == nil {
		return nil
	}
	vs := t.labelValues[name]
	if len(vs) == 0 {
		return nil
	}
	out := make([]string, len(vs))
	copy(out, vs)
	return out
}

// AllLabelNames returns sorted distinct label names from the on-disk v2
// label-value index, or nil for v1 tables.
func (t *Table) AllLabelNames() []string {
	if t == nil || t.labelValues == nil {
		return nil
	}
	out := make([]string, 0, len(t.labelValues))
	for n := range t.labelValues {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// OpenTable opens a file and builds a series name index. v1 tables (no trailer)
// and v2 tables (with postings and a trailer) are both supported.
func OpenTable(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	closeOnErr := true
	defer func() {
		if closeOnErr {
			_ = f.Close()
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 28 {
		return nil, errors.New("sstable: file too small")
	}
	m := make([]byte, 8)
	if _, err := io.ReadFull(f, m); err != nil {
		return nil, err
	}
	if m[0] != 'H' || m[1] != 'S' || m[2] != 'S' || m[3] != 'T' {
		return nil, errors.New("sstable: bad magic")
	}
	ver := m[4]
	if ver != 1 && ver != 2 {
		return nil, errors.New("sstable: unknown format version")
	}
	if ver == 1 && string(m) != string(fileMagicV1) {
		return nil, errors.New("sstable: bad v1 header")
	}
	if ver == 2 && string(m) != string(fileMagicV2) {
		return nil, errors.New("sstable: bad v2 header")
	}
	minT, err := readI64(f)
	if err != nil {
		return nil, err
	}
	maxT, err := readI64(f)
	if err != nil {
		return nil, err
	}
	n, err := readU32(f)
	if err != nil {
		return nil, err
	}
	series := make([]string, 0, n)
	offsets := make([]int64, 0, n)
	for i := 0; i < int(n); i++ {
		pos, _ := f.Seek(0, io.SeekCurrent)
		kl, err := readU32(f)
		if err != nil {
			return nil, err
		}
		kb := make([]byte, kl)
		if _, err := io.ReadFull(f, kb); err != nil {
			return nil, err
		}
		ml, err := readU32(f)
		if err != nil {
			return nil, err
		}
		if _, err := f.Seek(int64(ml), io.SeekCurrent); err != nil {
			return nil, err
		}
		// nPoints + Gorilla len + payload
		if _, err := f.Seek(4, io.SeekCurrent); err != nil {
			return nil, err
		}
		gl, err := readU32(f)
		if err != nil {
			return nil, err
		}
		if _, err := f.Seek(int64(gl), io.SeekCurrent); err != nil {
			return nil, err
		}
		series = append(series, string(kb))
		offsets = append(offsets, pos)
		_ = i
	}
	t := &Table{
		path:    path,
		file:    f,
		id:      parseSSTableID(path),
		version: ver,
		minT:    minT,
		maxT:    maxT,
		series:  series,
		off:     offsets,
	}
	if ver == 1 {
		closeOnErr = false
		return t, nil
	}
	dataEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// v2: trailer is last 16 bytes; must match end of data blocks
	pOff, lvOff, err := readV2Trailer(f)
	if err != nil {
		return nil, err
	}
	if dataEnd != pOff {
		return nil, errors.New("sstable: v2 data/postings offset mismatch")
	}
	if lvOff+int64(v2trailerSize) > st.Size() {
		return nil, errors.New("sstable: v2 file layout")
	}
	pr := io.NewSectionReader(f, pOff, lvOff-pOff)
	pm, err := readV2Postings(pr)
	if err != nil {
		return nil, err
	}
	t.postings = pm
	lr := io.NewSectionReader(f, lvOff, st.Size()-int64(v2trailerSize)-lvOff)
	lvm, err := readV2LabelValues(lr)
	if err != nil {
		return nil, err
	}
	t.labelValues = lvm
	closeOnErr = false
	return t, nil
}

// Close releases the open file. Safe to call more than once.
func (t *Table) Close() error {
	if t == nil || t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}

// Path returns the backing path.
func (t *Table) Path() string { return t.path }
func (t *Table) ID() uint64   { return t.id }

// SetBlockCache sets an optional decoded-block cache for this table.
func (t *Table) SetBlockCache(c *blockcache.Cache) { t.cache = c }

// MinTime and MaxTime are the min/max sample timestamps in this table (file header; ms).
func (t *Table) MinTime() int64 { return t.minT }
func (t *Table) MaxTime() int64 { return t.maxT }

// AllSeries returns a copy of the sorted canonical series key strings in this table.
func (t *Table) AllSeries() []string {
	if t == nil {
		return nil
	}
	out := make([]string, len(t.series))
	copy(out, t.series)
	return out
}

// QueryRange returns points for a canonical series key in [start,end] inclusive.
func (t *Table) QueryRange(series string, start, end int64) []storage.Sample {
	if start > end {
		return nil
	}
	idx := sort.SearchStrings(t.series, series)
	if idx >= len(t.series) || t.series[idx] != series {
		return nil
	}
	if t.cache != nil {
		if cached, ok := t.cache.Get(blockcache.Key{SSTableID: t.id, SeriesID: uint64(idx)}); ok {
			return filterSamplesInRange(cached, start, end)
		}
	}
	f := t.file
	if f == nil {
		var err error
		f, err = os.Open(t.path)
		if err != nil {
			return nil
		}
		defer f.Close()
	}
	if _, err := f.Seek(t.off[idx], io.SeekStart); err != nil {
		return nil
	}
	kl, err := readU32(f)
	if err != nil {
		return nil
	}
	if _, err := f.Seek(int64(kl), io.SeekCurrent); err != nil {
		return nil
	}
	ml, err := readU32(f)
	if err != nil {
		return nil
	}
	metaB := make([]byte, ml)
	if _, err := io.ReadFull(f, metaB); err != nil {
		return nil
	}
	nPts, err := readU32(f)
	if err != nil {
		return nil
	}
	gl, err := readU32(f)
	if err != nil {
		return nil
	}
	grB := make([]byte, gl)
	if _, err := io.ReadFull(f, grB); err != nil {
		return nil
	}
	var meta seriesMeta
	if err := json.Unmarshal(metaB, &meta); err != nil {
		return nil
	}
	ts, val, err := gorilla.DecodeAll(grB)
	if err != nil {
		return nil
	}
	if nPts > 0 && uint32(len(ts)) > nPts {
		ts = ts[:nPts]
		val = val[:nPts]
	}
	all := make([]storage.Sample, 0, len(ts))
	for i := range ts {
		all = append(all, storage.Sample{
			Metric:    meta.Metric,
			Labels:    meta.Labels,
			Timestamp: ts[i],
			Value:     val[i],
		})
	}
	if t.cache != nil {
		t.cache.Put(blockcache.Key{SSTableID: t.id, SeriesID: uint64(idx)}, all)
	}
	out := filterSamplesInRange(all, start, end)
	if len(out) == 0 {
		return nil
	}
	return out
}

func filterSamplesInRange(samples []storage.Sample, start, end int64) []storage.Sample {
	if len(samples) == 0 {
		return nil
	}
	out := make([]storage.Sample, 0, len(samples))
	for i := range samples {
		if samples[i].Timestamp < start {
			continue
		}
		if samples[i].Timestamp > end {
			break
		}
		out = append(out, samples[i])
	}
	return out
}

func parseSSTableID(path string) uint64 {
	base := filepath.Base(path)
	if len(base) < 8 {
		return 0
	}
	head := strings.ToLower(base[:8])
	if id, err := strconv.ParseUint(head, 16, 64); err == nil {
		return id
	}
	return 0
}
