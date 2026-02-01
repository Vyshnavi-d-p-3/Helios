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
	"os"
	"path/filepath"
	"sync"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// Magic precedes the first record in a WAL file.
var magic = []byte{'H', 'L', 'W', '1'}

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
		if _, err := f.Write(magic); err != nil {
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
		lastSeq, err := replay(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		seq = lastSeq
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{
		path: path,
		f:    f,
		wb:   bufio.NewWriterSize(f, 1<<20),
		seq:  seq,
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
	entry := Entry{Seq: seq, Samples: samples}
	payload, err := json.Marshal(&entry)
	if err != nil {
		w.seq--
		return 0, err
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
func replay(f *os.File) (uint64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	m := make([]byte, len(magic))
	if _, err := io.ReadFull(f, m); err != nil {
		return 0, err
	}
	if string(m) != string(magic) {
		return 0, errors.New("wal: bad magic")
	}
	var lastSeq uint64
	for {
		var hdr [4]byte
		_, err := io.ReadFull(f, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > MaxRecordBytes {
			return 0, fmt.Errorf("wal: bad record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return 0, err
		}
		var csum [4]byte
		if _, err := io.ReadFull(f, csum[:]); err != nil {
			return 0, err
		}
		want := binary.LittleEndian.Uint32(csum[:])
		got := crc32.Checksum(payload, castagnoli)
		if want != got {
			return 0, fmt.Errorf("wal: crc mismatch (record seq replay)")
		}
		var e Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			return 0, err
		}
		lastSeq = e.Seq
	}
	return lastSeq, nil
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
	m := make([]byte, len(magic))
	if _, err := io.ReadFull(f, m); err != nil {
		return err
	}
	if string(m) != string(magic) {
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
		var e Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}
