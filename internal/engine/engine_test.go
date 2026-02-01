package engine

import (
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestEngine_write_reopen(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	cfg.MemtableMaxSize = 64 * 1024 * 1024

	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := storage.Sample{Metric: "m", Timestamp: 10, Value: 3.5}
	if err := e.Write([]storage.Sample{s}); err != nil {
		t.Fatal(err)
	}
	if e.MemLen() != 1 {
		t.Fatalf("mem len %d", e.MemLen())
	}
	_ = e.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.MemLen() != 1 {
		t.Fatalf("after reopen mem len %d", e2.MemLen())
	}
	sk := storage.SeriesKey("m", nil)
	qr := e2.QueryRange(sk, 0, 20)
	if len(qr) != 1 || qr[0].Value != 3.5 {
		t.Fatalf("query %+v", qr)
	}
}

func TestEngine_emptyWalPath(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "d")
	_, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
}
