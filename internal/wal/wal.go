// Package wal is an append-only write-ahead log with length-prefixed records
// and CRC32C (Castagnoli) over each payload. Used for crash recovery before
// data is durably in SSTables.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// Magic precedes the first record in a WAL file.
var (
	magicV1 = []byte{'H', 'L', 'W', '1'} // legacy JSON payloads
	magicV2 = []byte{'H', 'L', 'W', '2'} // binary payloads
)

// MaxRecordBytes caps a single record payload to bound allocations.
const MaxRecordBytes = 32 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Entry is a batch of samples written as one durable WAL record.
type Entry struct {
	Seq     uint64           `json:"seq"`
	Samples []storage.Sample `json:"samples"`
}

// WAL is a process-local write-ahead log. A single writer is assumed; use a mutex if sharing.
type WAL struct {
	path string
	mu   sync.Mutex
	f    *os.File
	wb   *bufio.Writer
	seq  uint64
	v2   bool
}

// Open creates or opens the WAL at path, replays to discover the next sequence number,
// and leaves the file positioned for appends.
func Open(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var seq uint64
	if st.Size() == 0 {
		if _, err := f.Write(magicV2); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		lastSeq, useV2, err := replay(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		seq = lastSeq
		return openReadyWAL(path, f, seq, useV2)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return openReadyWAL(path, f, seq, true)
}

func openReadyWAL(path string, f *os.File, seq uint64, useV2 bool) (*WAL, error) {
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{
		path: path,
		f:    f,
		wb:   bufio.NewWriterSize(f, 1<<20),
		seq:  seq,
		v2:   useV2,
	}, nil
}

// Path returns the backing file path.
func (w *WAL) Path() string { return w.path }

// NextSeq returns the sequence number that will be assigned to the next Append.
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq + 1
}

// Append encodes a batch, writes a CRC32C-protected record, and syncs the file.
// Sync follows config elsewhere; for now every append fsyncs (safe default).
func (w *WAL) Append(samples []storage.Sample) (seq uint64, err error) {
	if len(samples) == 0 {
		return 0, errors.New("wal: empty batch")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	seq = w.seq
	var payload []byte
	if w.v2 {
		payload, err = encodeEntryBinary(seq, samples)
		if err != nil {
			w.seq--
			return 0, err
		}
	} else {
		entry := Entry{Seq: seq, Samples: samples}
		payload, err = json.Marshal(&entry)
		if err != nil {
			w.seq--
			return 0, err
		}
	}
	if len(payload) > MaxRecordBytes {
		w.seq--
		return 0, fmt.Errorf("wal: record %d bytes exceeds max", len(payload))
	}
	if err := writeRecord(w.wb, payload); err != nil {
		w.seq--
		return 0, err
	}
	if err := w.wb.Flush(); err != nil {
		return 0, err
	}
	if err := w.f.Sync(); err != nil {
		return 0, err
	}
	return seq, nil
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	var err error
	if w.wb != nil {
		err = w.wb.Flush()
	}
	if c := w.f.Close(); err == nil {
		err = c
	}
	w.f = nil
	w.wb = nil
	return err
}

// Truncate clears the WAL to an empty (magic-only) file, resets the sequence, and
// reopens the writer. Call after a successful SST flush so on-disk data matches
// the logical checkpoint; new writes get fresh sequence numbers.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		if w.wb != nil {
			_ = w.wb.Flush()
		}
		_ = w.f.Close()
		w.f, w.wb = nil, nil
	}
	if err := os.Truncate(w.path, 0); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(magicV2); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	w.f = f
	w.wb = bufio.NewWriterSize(f, 1<<20)
	w.seq = 0
	w.v2 = true
	return nil
}

func writeRecord(bw *bufio.Writer, payload []byte) error {
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := bw.Write(payload); err != nil {
		return err
	}
	crc := crc32.Checksum(payload, castagnoli)
	binary.LittleEndian.PutUint32(hdr[:], crc)
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	return nil
}

// replay returns the last sequence number seen, or 0 for an empty log after magic.
// The file is rewound to offset 0; caller repositions for append.
func replay(f *os.File) (uint64, bool, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, false, err
	}
	m := make([]byte, len(magicV2))
	if _, err := io.ReadFull(f, m); err != nil {
		return 0, false, err
	}
	useV2 := true
	switch string(m) {
	case string(magicV2):
		useV2 = true
	case string(magicV1):
		useV2 = false
	default:
		return 0, false, errors.New("wal: bad magic")
	}
	var lastSeq uint64
	for {
		var hdr [4]byte
		_, err := io.ReadFull(f, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, false, err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > MaxRecordBytes {
			return 0, false, fmt.Errorf("wal: bad record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return 0, false, err
		}
		var csum [4]byte
		if _, err := io.ReadFull(f, csum[:]); err != nil {
			return 0, false, err
		}
		want := binary.LittleEndian.Uint32(csum[:])
		got := crc32.Checksum(payload, castagnoli)
		if want != got {
			return 0, false, fmt.Errorf("wal: crc mismatch (record seq replay)")
		}
		e, err := decodeEntry(payload, useV2)
		if err != nil {
			return 0, false, err
		}
		lastSeq = e.Seq
	}
	return lastSeq, useV2, nil
}

// Scan reads the WAL from the beginning, invoking fn for each valid entry. Used for recovery.
func Scan(path string, fn func(e Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	m := make([]byte, len(magicV2))
	if _, err := io.ReadFull(f, m); err != nil {
		return err
	}
	useV2 := true
	switch string(m) {
	case string(magicV2):
		useV2 = true
	case string(magicV1):
		useV2 = false
	default:
		return errors.New("wal: bad magic")
	}
	for {
		var hdr [4]byte
		_, err := io.ReadFull(f, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 {
			return nil
		}
		if n > MaxRecordBytes {
			return fmt.Errorf("wal: bad record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		var csum [4]byte
		if _, err := io.ReadFull(f, csum[:]); err != nil {
			return nil
		}
		want := binary.LittleEndian.Uint32(csum[:])
		if want != crc32.Checksum(payload, castagnoli) {
			return fmt.Errorf("wal: checksum mismatch at offset")
		}
		e, err := decodeEntry(payload, useV2)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

func decodeEntry(payload []byte, useV2 bool) (Entry, error) {
	if useV2 {
		return decodeEntryBinary(payload)
	}
	var e Entry
	if err := json.Unmarshal(payload, &e); err != nil {
		return Entry{}, err
	}
	return e, nil
}

func encodeEntryBinary(seq uint64, samples []storage.Sample) ([]byte, error) {
	total := 8 + 4
	for _, s := range samples {
		total += 2 + len(s.Metric)
		total += 2
		for k, v := range s.Labels {
			total += 2 + len(k) + 2 + len(v)
		}
		total += 8 + 8
	}
	buf := make([]byte, 0, total)
	buf = binary.LittleEndian.AppendUint64(buf, seq)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(samples)))
	for _, s := range samples {
		if len(s.Metric) > 0xffff {
			return nil, fmt.Errorf("wal: metric too long")
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.Metric)))
		buf = append(buf, s.Metric...)
		if len(s.Labels) > 0xffff {
			return nil, fmt.Errorf("wal: too many labels")
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.Labels)))
		for k, v := range s.Labels {
			if len(k) > 0xffff || len(v) > 0xffff {
				return nil, fmt.Errorf("wal: label too long")
			}
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
			buf = append(buf, k...)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v)))
			buf = append(buf, v...)
		}
		buf = binary.LittleEndian.AppendUint64(buf, uint64(s.Timestamp))
		buf = binary.LittleEndian.AppendUint64(buf, mathFloat64bits(s.Value))
	}
	return buf, nil
}

func decodeEntryBinary(payload []byte) (Entry, error) {
	if len(payload) < 12 {
		return Entry{}, errors.New("wal: binary payload too short")
	}
	off := 0
	seq := binary.LittleEndian.Uint64(payload[off : off+8])
	off += 8
	count := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4
	samples := make([]storage.Sample, 0, count)
	for i := 0; i < count; i++ {
		metric, nOff, err := readLPString(payload, off)
		if err != nil {
			return Entry{}, err
		}
		off = nOff
		if off+2 > len(payload) {
			return Entry{}, errors.New("wal: truncated label count")
		}
		labelCount := int(binary.LittleEndian.Uint16(payload[off : off+2]))
		off += 2
		labels := make(map[string]string, labelCount)
		for j := 0; j < labelCount; j++ {
			name, n2, err := readLPString(payload, off)
			if err != nil {
				return Entry{}, err
			}
			off = n2
			value, n3, err := readLPString(payload, off)
			if err != nil {
				return Entry{}, err
			}
			off = n3
			labels[name] = value
		}
		if off+16 > len(payload) {
			return Entry{}, errors.New("wal: truncated sample body")
		}
		ts := int64(binary.LittleEndian.Uint64(payload[off : off+8]))
		off += 8
		val := mathFloat64frombits(binary.LittleEndian.Uint64(payload[off : off+8]))
		off += 8
		samples = append(samples, storage.Sample{
			Metric:    metric,
			Labels:    labels,
			Timestamp: ts,
			Value:     val,
		})
	}
	if off != len(payload) {
		return Entry{}, errors.New("wal: trailing bytes in binary payload")
	}
	return Entry{Seq: seq, Samples: samples}, nil
}

func readLPString(payload []byte, off int) (string, int, error) {
	if off+2 > len(payload) {
		return "", off, errors.New("wal: truncated length-prefixed string")
	}
	n := int(binary.LittleEndian.Uint16(payload[off : off+2]))
	off += 2
	if off+n > len(payload) {
		return "", off, errors.New("wal: invalid length-prefixed string")
	}
	return string(payload[off : off+n]), off + n, nil
}

func mathFloat64bits(v float64) uint64    { return math.Float64bits(v) }
func mathFloat64frombits(v uint64) float64 { return math.Float64frombits(v) }
