package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestEngine_flush_then_read(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "x", Timestamp: 5, Value: 3},
	})
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if eng.MemLen() != 0 {
		t.Fatalf("mem after flush: %d", eng.MemLen())
	}
	if eng.SSTCount() != 1 {
		t.Fatalf("sst count %d", eng.SSTCount())
	}
	sk := storage.SeriesKey("x", nil)
	q := eng.QueryRange(sk, 0, 10)
	if len(q) != 1 || q[0].Value != 3 {
		t.Fatalf("query from sst %+v", q)
	}
	_ = eng.Write([]storage.Sample{
		{Metric: "x", Timestamp: 6, Value: 4},
	})
	q2 := eng.QueryRange(sk, 0, 20)
	if len(q2) != 2 {
		t.Fatalf("merged %+v", q2)
	}
}

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

func TestEngine_CheckQueryTimeRange(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	cfg.MaxQueryWindow = time.Hour
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CheckQueryTimeRange(0, 2*time.Hour.Milliseconds()); err == nil {
		t.Fatal("expected error for range wider than cap")
	}
	if err := eng.CheckQueryTimeRange(0, 30*time.Minute.Milliseconds()); err != nil {
		t.Fatalf("unexpected: %v", err)
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

func TestEngine_QueryRangeByLabelFilter(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"env": "a", "k": "1"}, Timestamp: 1, Value: 1},
		{Metric: "m", Labels: map[string]string{"env": "b", "k": "2"}, Timestamp: 2, Value: 2},
	})
	rows := eng.QueryRangeByLabelFilter("m", map[string]string{"env": "a"}, 0, 10)
	if len(rows) != 1 || len(rows[0].Samples) != 1 || rows[0].Samples[0].Value != 1 {
		t.Fatalf("mem filter: %+v", rows)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"env": "a", "k": "1"}, Timestamp: 3, Value: 10},
	})
	rows2 := eng.QueryRangeByLabelFilter("m", map[string]string{"env": "a"}, 0, 10)
	if len(rows2) != 1 {
		t.Fatalf("expected 1 series, got %d", len(rows2))
	}
	// t=1 from sst, t=3 from mem, mem wins on ts=3? same series - merge
	pts := rows2[0].Samples
	if len(pts) != 2 {
		t.Fatalf("points %+v", pts)
	}
}

func TestEngine_AllLabelNamesValues(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m1", Labels: map[string]string{"e": "p"}, Timestamp: 1, Value: 1},
	})
	names := eng.AllLabelNames()
	if len(names) != 2 {
		t.Fatalf("labels: %v", names)
	}
	vs := eng.AllLabelValues("__name__")
	if len(vs) != 1 || vs[0] != "m1" {
		t.Fatalf("__name__ values: %v", vs)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	names2 := eng.AllLabelNames()
	if len(names2) != 2 {
		t.Fatalf("after flush: %v", names2)
	}
	vs2 := eng.AllLabelValues("e")
	if len(vs2) != 1 || vs2[0] != "p" {
		t.Fatalf("e: %v", vs2)
	}
}

func TestEngine_SeriesWithLabelsInWindow(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Timestamp: 5, Value: 1},
		{Metric: "m", Labels: map[string]string{"a": "2"}, Timestamp: 6, Value: 1},
	})
	lab := eng.SeriesWithLabelsInWindow("m", map[string]string{"a": "1"}, 0, 10, true)
	if len(lab) != 1 || lab[0]["__name__"] != "m" || lab[0]["a"] != "1" {
		t.Fatalf("partial: %+v", lab)
	}
	u := eng.SeriesWithLabelsInWindowUnion([]string{"m", "m"}, 0, 10)
	if len(u) != 2 {
		t.Fatalf("union: %+v", u)
	}
}

func TestEngine_CompactL0(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{{Metric: "x", Timestamp: 1, Value: 10}})
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if eng.SSTCount() != 1 {
		t.Fatalf("sst %d", eng.SSTCount())
	}
	_ = eng.Write([]storage.Sample{{Metric: "x", Timestamp: 2, Value: 20}})
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if eng.SSTCount() != 2 {
		t.Fatalf("want 2 sst: %d", eng.SSTCount())
	}
	if err := eng.CompactL0(); err != nil {
		t.Fatal(err)
	}
	if eng.SSTCount() != 1 {
		t.Fatalf("after compact: %d", eng.SSTCount())
	}
	sk := storage.SeriesKey("x", nil)
	q := eng.QueryRange(sk, 0, 10)
	if len(q) != 2 || q[0].Value != 10 || q[1].Value != 20 {
		t.Fatalf("query after compact: %+v", q)
	}
	if err := eng.CompactL0(); err != nil {
		t.Fatal(err)
	}
	if eng.SSTCount() != 1 {
		t.Fatalf("noop compact should keep 1 sst: %d", eng.SSTCount())
	}
}
