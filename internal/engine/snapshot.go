package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/vyshnavi-d-p-3/helios/internal/sstable"
)

type snapshot struct {
	dir      string
	manifest snapshotManifest
}

type snapshotManifest struct {
	Files []string `json:"files"`
}

// NewSnapshot captures current immutable SST files and (if non-empty) current
// memtable into a temporary snapshot directory.
func (e *Engine) NewSnapshot() (raft.FSMSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tmp, err := os.MkdirTemp("", "helios-snap-*")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, t := range e.sst {
		src := t.Path()
		name := filepath.Base(src)
		dst := filepath.Join(tmp, name)
		if err := os.Link(src, dst); err != nil {
			if err := copyFile(src, dst); err != nil {
				_ = os.RemoveAll(tmp)
				return nil, fmt.Errorf("snapshot link %s: %w", src, err)
			}
		}
		files = append(files, name)
	}
	if e.mem != nil && e.mem.Len() > 0 {
		memSnap := "memtable-snap.sst"
		if err := sstable.WriteFromMemtable(filepath.Join(tmp, memSnap), e.mem); err != nil {
			_ = os.RemoveAll(tmp)
			return nil, err
		}
		files = append(files, memSnap)
	}
	return &snapshot{
		dir: tmp,
		manifest: snapshotManifest{
			Files: files,
		},
	}, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	manifestBytes, err := json.Marshal(s.manifest)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := writeLenPrefixed(sink, manifestBytes); err != nil {
		_ = sink.Cancel()
		return err
	}
	for _, name := range s.manifest.Files {
		path := filepath.Join(s.dir, name)
		f, err := os.Open(path)
		if err != nil {
			_ = sink.Cancel()
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			_ = sink.Cancel()
			return err
		}
		if err := writeString(sink, name); err != nil {
			_ = f.Close()
			_ = sink.Cancel()
			return err
		}
		if err := writeUint64(sink, uint64(info.Size())); err != nil {
			_ = f.Close()
			_ = sink.Cancel()
			return err
		}
		if _, err := io.Copy(sink, f); err != nil {
			_ = f.Close()
			_ = sink.Cancel()
			return err
		}
		_ = f.Close()
	}
	return sink.Close()
}

func (s *snapshot) Release() {
	_ = os.RemoveAll(s.dir)
}

// RestoreFromSnapshot replaces on-disk sst state with files from snapshot stream.
func (e *Engine) RestoreFromSnapshot(rc io.ReadCloser) error {
	defer rc.Close()
	e.mu.Lock()
	defer e.mu.Unlock()

	manifestBytes, err := readLenPrefixed(rc)
	if err != nil {
		return err
	}
	var m snapshotManifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return err
	}

	for _, t := range e.sst {
		_ = t.Close()
	}
	e.sst = nil
	e.frozen = nil
	if e.mem != nil {
		e.mem.Clear()
	}

	sstDir := filepath.Join(e.cfg.DataDir, "sst")
	if err := os.RemoveAll(sstDir); err != nil {
		return err
	}
	if err := os.MkdirAll(sstDir, 0o750); err != nil {
		return err
	}
	for range m.Files {
		name, err := readString(rc)
		if err != nil {
			return err
		}
		size, err := readUint64(rc)
		if err != nil {
			return err
		}
		outPath := filepath.Join(sstDir, filepath.Base(name))
		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		if _, err := io.CopyN(f, rc, int64(size)); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	if err := e.loadSSTs(); err != nil {
		return err
	}
	e.rebuildSeriesCardLocked()
	if e.wal != nil {
		if err := e.wal.Truncate(); err != nil {
			return err
		}
	}
	return nil
}

func writeLenPrefixed(w io.Writer, b []byte) error {
	if len(b) > int(^uint32(0)) {
		return fmt.Errorf("snapshot chunk too large")
	}
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(b)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readLenPrefixed(r io.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	l := binary.LittleEndian.Uint32(n[:])
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeString(w io.Writer, s string) error {
	return writeLenPrefixed(w, []byte(s))
}

func readString(r io.Reader) (string, error) {
	b, err := readLenPrefixed(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func writeUint64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
