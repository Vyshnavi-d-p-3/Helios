// Package sstable is a v1 on-disk table: sorted series keys, JSON metadata
// (metric+labels) per series, and Gorilla-compressed (ts, value) blocks.
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
	"github.com/vyshnavi-d-p-3/helios/pkg/gorilla"
)

var fileMagic = []byte{'H', 'S', 'S', 'T', 1, 0, 0, 0}

type seriesMeta struct {
	Metric string            `json:"m"`
	Labels map[string]string `json:"l,omitempty"`
}

// WriteFromMemtable writes a memtable to path (key order: series, then time).
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
	if _, err := f.Write(fileMagic); err != nil {
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

// Table is a read-only SST with an in-memory series key index.
type Table struct {
	path   string
	series []string
	off    []int64
}

// OpenTable opens a file and builds a series name index.
func OpenTable(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
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
	if string(m) != string(fileMagic) {
		return nil, errors.New("sstable: bad magic")
	}
	if _, err := readI64(f); err != nil { // minT
		return nil, err
	}
	if _, err := readI64(f); err != nil { // maxT
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
	return &Table{path: path, series: series, off: offsets}, nil
}

// Path returns the backing path.
func (t *Table) Path() string { return t.path }

// QueryRange returns points for a canonical series key in [start,end] inclusive.
func (t *Table) QueryRange(series string, start, end int64) []storage.Sample {
	if start > end {
		return nil
	}
	idx := sort.SearchStrings(t.series, series)
	if idx >= len(t.series) || t.series[idx] != series {
		return nil
	}
	f, err := os.Open(t.path)
	if err != nil {
		return nil
	}
	defer f.Close()
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
	var out []storage.Sample
	for i := range ts {
		if ts[i] < start {
			continue
		}
		if ts[i] > end {
			break
		}
		out = append(out, storage.Sample{
			Metric:    meta.Metric,
			Labels:    meta.Labels,
			Timestamp: ts[i],
			Value:     val[i],
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
