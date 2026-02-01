package engine

import (
	"testing"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestEnforceRetention_dropsSST(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.RetentionPeriod = 1 * time.Hour
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	if err := e.Write([]storage.Sample{{Metric: "m", Timestamp: old, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if e.SSTCount() != 1 {
		t.Fatalf("sst %d", e.SSTCount())
	}
	removed, err := e.EnforceRetention()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed %d", removed)
	}
	if e.SSTCount() != 0 {
		t.Fatalf("sst after %d", e.SSTCount())
	}
}

func TestEnforceRetention_keepsRecentSST(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.RetentionPeriod = 1 * time.Hour
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	recent := time.Now().Add(-5 * time.Minute).UnixMilli()
	if err := e.Write([]storage.Sample{{Metric: "m", Timestamp: recent, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	removed, err := e.EnforceRetention()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed %d", removed)
	}
	if e.SSTCount() != 1 {
		t.Fatalf("sst %d", e.SSTCount())
	}
}

func TestEnforceRetention_zeroConfigNoop(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.RetentionPeriod = 0
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = e.Write([]storage.Sample{{Metric: "m", Timestamp: 1, Value: 1}})
	_ = e.Flush()
	removed, err := e.EnforceRetention()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 || e.SSTCount() != 1 {
		t.Fatalf("removed=%d sst=%d", removed, e.SSTCount())
	}
}
