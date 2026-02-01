package sstable

import (
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestWriteFromMemtable_queryRoundTrip(t *testing.T) {
	m := memtable.New(0)
	m.Put(storage.Sample{Metric: "a", Timestamp: 10, Value: 1})
	m.Put(storage.Sample{Metric: "a", Timestamp: 20, Value: 2})
	p := filepath.Join(t.TempDir(), "1.sst")
	if err := WriteFromMemtable(p, m); err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTable(p)
	if err != nil {
		t.Fatal(err)
	}
	sk := storage.SeriesKey("a", nil)
	samples := tab.QueryRange(sk, 0, 30)
	if len(samples) != 2 || samples[0].Value != 1 || samples[1].Value != 2 {
		t.Fatalf("got %+v", samples)
	}
}
