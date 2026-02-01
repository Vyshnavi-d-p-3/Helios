// SSTable format v2: after data blocks, postings and label-value sections, then a
// 16-byte trailer (postings offset, label-values offset) at EOF.

package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

const (
	v2trailerSize = 16
)

var fileMagicV2 = []byte{'H', 'S', 'S', 'T', 2, 0, 0, 0}

// buildPostingsAndLabelValues derives postings (key "name=value" → series IDs)
// and the label-value index from grouped samples (one unique series per group).
func buildPostingsAndLabelValues(groups [][]storage.Sample) (map[string][]uint64, map[string][]string) {
	postings := make(map[string][]uint64)
	labelSeen := make(map[string]map[string]struct{})

	for id, g := range groups {
		if len(g) == 0 {
			continue
		}
		s0 := g[0]
		k := id
		uid := uint64(k)
		addPost := func(key string) {
			postings[key] = append(postings[key], uid)
		}
		addPost(encodeLabelPair("__name__", s0.Metric))
		if len(s0.Labels) == 0 {
			labelNames := labelSeen["__name__"]
			if labelNames == nil {
				labelNames = make(map[string]struct{})
				labelSeen["__name__"] = labelNames
			}
			labelNames[s0.Metric] = struct{}{}
		} else {
			for name, val := range s0.Labels {
				addPost(encodeLabelPair(name, val))
				names := labelSeen[name]
				if names == nil {
					names = make(map[string]struct{})
					labelSeen[name] = names
				}
				names[val] = struct{}{}
			}
			// also index __name__ in label values
			nn := labelSeen["__name__"]
			if nn == nil {
				nn = make(map[string]struct{})
				labelSeen["__name__"] = nn
			}
			nn[s0.Metric] = struct{}{}
		}
	}

	for k := range postings {
		ids := postings[k]
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		ids = uniqueSortedUint64(ids)
		postings[k] = ids
	}

	lv := make(map[string][]string, len(labelSeen))
	for name, vs := range labelSeen {
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		lv[name] = vals
	}
	return postings, lv
}

func encodeLabelPair(name, value string) string {
	var b strings.Builder
	b.Grow(len(name) + 1 + len(value))
	b.WriteString(name)
	b.WriteByte('=')
	b.WriteString(value)
	return b.String()
}

func uniqueSortedUint64(a []uint64) []uint64 {
	if len(a) < 2 {
		return a
	}
	w := 1
	for i := 1; i < len(a); i++ {
		if a[i] == a[i-1] {
			continue
		}
		a[w] = a[i]
		w++
	}
	return a[:w]
}

func writeV2Postings(w io.Writer, postings map[string][]uint64) error {
	keys := make([]string, 0, len(postings))
	for k := range postings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if err := writeU32w(w, uint32(len(keys))); err != nil {
		return err
	}
	for _, k := range keys {
		ids := postings[k]
		kb := []byte(k)
		if len(kb) > 0xffff {
			return fmt.Errorf("sstable: posting key too long")
		}
		if err := writeU16w(w, uint16(len(kb))); err != nil {
			return err
		}
		if _, err := w.Write(kb); err != nil {
			return err
		}
		if err := writeU32w(w, uint32(len(ids))); err != nil {
			return err
		}
		if err := writeDeltaUvarintSeq(w, ids); err != nil {
			return err
		}
	}
	return nil
}

func readV2Postings(r io.Reader) (map[string][]uint64, error) {
	n, err := readU32r(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]uint64, n)
	for i := uint32(0); i < n; i++ {
		kl, err := readU16r(r)
		if err != nil {
			return nil, err
		}
		kb := make([]byte, kl)
		if _, err := io.ReadFull(r, kb); err != nil {
			return nil, err
		}
		nc, err := readU32r(r)
		if err != nil {
			return nil, err
		}
		ids, err := readDeltaUvarintSeq(r, int(nc))
		if err != nil {
			return nil, err
		}
		out[string(kb)] = ids
	}
	return out, nil
}

func writeV2LabelValues(w io.Writer, lv map[string][]string) error {
	names := make([]string, 0, len(lv))
	for name := range lv {
		names = append(names, name)
	}
	sort.Strings(names)
	if err := writeU32w(w, uint32(len(names))); err != nil {
		return err
	}
	for _, name := range names {
		vals := lv[name]
		nb := []byte(name)
		if len(nb) > 0xffff {
			return errors.New("sstable: label name too long")
		}
		if err := writeU16w(w, uint16(len(nb))); err != nil {
			return err
		}
		if _, err := w.Write(nb); err != nil {
			return err
		}
		if err := writeU32w(w, uint32(len(vals))); err != nil {
			return err
		}
		for _, v := range vals {
			vb := []byte(v)
			if len(vb) > 0xffff {
				return errors.New("sstable: label value too long")
			}
			if err := writeU16w(w, uint16(len(vb))); err != nil {
				return err
			}
			if _, err := w.Write(vb); err != nil {
				return err
			}
		}
	}
	return nil
}

func readV2LabelValues(r io.Reader) (map[string][]string, error) {
	n, err := readU32r(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, n)
	for i := uint32(0); i < n; i++ {
		nl, err := readU16r(r)
		if err != nil {
			return nil, err
		}
		nb := make([]byte, nl)
		if _, err := io.ReadFull(r, nb); err != nil {
			return nil, err
		}
		vc, err := readU32r(r)
		if err != nil {
			return nil, err
		}
		vals := make([]string, 0, vc)
		for j := uint32(0); j < vc; j++ {
			vl, err := readU16r(r)
			if err != nil {
				return nil, err
			}
			vb := make([]byte, vl)
			if _, err := io.ReadFull(r, vb); err != nil {
				return nil, err
			}
			vals = append(vals, string(vb))
		}
		out[string(nb)] = vals
	}
	return out, nil
}

func writeU16w(w io.Writer, v uint16) error {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU32w(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readU16r(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func readU32r(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func writeDeltaUvarintSeq(w io.Writer, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	var buf [binary.MaxVarintLen64]byte
	var prev uint64
	for i, id := range ids {
		var x uint64
		if i == 0 {
			x = id
		} else {
			if id < prev {
				return errors.New("sstable: non-monotonic series ids")
			}
			x = id - prev
		}
		n := binary.PutUvarint(buf[:], x)
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		prev = id
	}
	return nil
}

// uvarintByteReader implements io.ByteReader with single-byte reads so binary.ReadUvarint
// does not buffer past the end of a size-limited reader (e.g. io.SectionReader).
type uvarintByteReader struct{ r io.Reader }

func (b uvarintByteReader) ReadByte() (byte, error) {
	var c [1]byte
	n, err := b.r.Read(c[:])
	if n == 0 {
		if err == nil {
			err = io.EOF
		}
		return 0, err
	}
	return c[0], nil
}

func readDeltaUvarintSeq(r io.Reader, want int) ([]uint64, error) {
	if want == 0 {
		return nil, nil
	}
	// Do not use bufio here: a bufio.Reader can prefetch past the uvarint and past a
	// short SectionReader's limit, dropping bytes the caller still needs to parse.
	if br, ok := r.(io.ByteReader); ok {
		return readDeltaUvarintWithByteReader(br, want)
	}
	return readDeltaUvarintWithByteReader(uvarintByteReader{r: r}, want)
}

func readDeltaUvarintWithByteReader(br io.ByteReader, want int) ([]uint64, error) {
	out := make([]uint64, 0, want)
	for len(out) < want {
		d, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		var id uint64
		if len(out) == 0 {
			id = d
		} else {
			id = out[len(out)-1] + d
		}
		out = append(out, id)
	}
	return out, nil
}

func writeV2Trailer(f *os.File, postingsOff, labelValuesOff int64) error {
	if err := writeI64to(f, postingsOff); err != nil {
		return err
	}
	return writeI64to(f, labelValuesOff)
}

func writeI64to(f *os.File, v int64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	_, err := f.Write(b[:])
	return err
}

func readV2Trailer(f *os.File) (postingsOff, labelValuesOff int64, err error) {
	st, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if st.Size() < v2trailerSize {
		return 0, 0, errors.New("sstable: v2 file too small for trailer")
	}
	if _, err := f.Seek(st.Size()-v2trailerSize, 0); err != nil {
		return 0, 0, err
	}
	p, err := readI64(f)
	if err != nil {
		return 0, 0, err
	}
	l, err := readI64(f)
	if err != nil {
		return 0, 0, err
	}
	return p, l, nil
}
