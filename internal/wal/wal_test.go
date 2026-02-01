package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestWAL_append_scan_roundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.wal")
	w, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	samples := []storage.Sample{
		{Metric: "m", Timestamp: 1, Value: 1.0},
	}
	seq, err := w.Append(samples)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("seq=%d", seq)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var count int
	err = Scan(p, func(e Entry) error {
		count++
		if e.Seq != 1 || len(e.Samples) != 1 {
			t.Fatalf("entry %+v", e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d", count)
	}

	// Reopen: next seq should continue
	w2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if w2.NextSeq() != 2 {
		t.Fatalf("next=%d", w2.NextSeq())
	}
	s2, err := w2.Append(samples)
	if err != nil {
		t.Fatal(err)
	}
	if s2 != 2 {
		t.Fatalf("seq2=%d", s2)
	}
	_ = w2.Close()
}

func TestWAL_rejectsEmptyBatch(t *testing.T) {
	w, err := Open(filepath.Join(t.TempDir(), "x.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Append(nil); err == nil {
		t.Fatal("expected error")
	}
	if _, err := w.Append([]storage.Sample{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestWAL_rejectsCorruptCrc(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bad.wal")
	w, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Append([]storage.Sample{{Metric: "x", Timestamp: 1, Value: 1}})
	_ = w.Close()
	b, _ := os.ReadFile(p)
	// flip one byte in the middle of the payload (after magic+len+some payload)
	if len(b) < 20 {
		t.Fatalf("file short: %d", len(b))
	}
	b[12] ^= 0x40
	_ = os.WriteFile(p, b, 0o644)
	if _, err := Open(p); err == nil {
		t.Fatal("expected replay error on corrupt wal")
	}
}
