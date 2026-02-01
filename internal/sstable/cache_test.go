package sstable

import (
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/blockcache"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestQueryRange_UsesBlockCache(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "m", Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "m", Timestamp: 2, Value: 2})

	p := filepath.Join(t.TempDir(), "00000001.sst")
	if err := WriteFromMemtable(p, m); err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tab.Close() }()

	c := blockcache.New(100)
	tab.SetBlockCache(c)

	sk := storage.SeriesKey("m", nil)
	_ = tab.QueryRange(sk, 0, 10) // miss + populate
	_ = tab.QueryRange(sk, 0, 10) // hit

	s := c.Snapshot()
	if s.Hits == 0 {
		t.Fatalf("expected cache hit, got snapshot %+v", s)
	}
}

func TestCacheInvalidateSSTable_RemovesEntries(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "m", Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "m", Timestamp: 2, Value: 2})

	p := filepath.Join(t.TempDir(), "00000002.sst")
	if err := WriteFromMemtable(p, m); err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tab.Close() }()

	c := blockcache.New(100)
	tab.SetBlockCache(c)
	sk := storage.SeriesKey("m", nil)
	_ = tab.QueryRange(sk, 0, 10) // populate
	before := c.Snapshot()
	if before.Samples == 0 {
		t.Fatalf("expected populated cache, got %+v", before)
	}

	c.InvalidateSSTable(tab.ID())
	after := c.Snapshot()
	if after.Samples >= before.Samples {
		t.Fatalf("expected fewer samples after invalidation, before=%+v after=%+v", before, after)
	}
}
