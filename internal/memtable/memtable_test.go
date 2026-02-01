package memtable

import (
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestMemtable_QueryRange(t *testing.T) {
	m := New(0)
	sk := storage.SeriesKey("http_req", map[string]string{"status": "500"})
	m.Put(storage.Sample{Metric: "http_req", Labels: map[string]string{"status": "500"}, Timestamp: 100, Value: 1})
	m.Put(storage.Sample{Metric: "http_req", Labels: map[string]string{"status": "500"}, Timestamp: 200, Value: 2})
	m.Put(storage.Sample{Metric: "other", Labels: nil, Timestamp: 150, Value: 9})

	got := m.QueryRange(sk, 100, 200)
	if len(got) != 2 {
		t.Fatalf("len=%d %+v", len(got), got)
	}
	if got[0].Value != 1 || got[1].Value != 2 {
		t.Fatalf("values %+v", got)
	}
}

func TestMemtable_ForEachUniqueSeriesKeyFromNameIndex(t *testing.T) {
	m := New(0)
	m.Put(storage.Sample{Metric: "a", Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "b", Timestamp: 1, Value: 1})
	seen := make(map[string]int)
	m.ForEachUniqueSeriesKeyFromNameIndex(func(sk string) bool {
		seen[sk]++
		return true
	})
	if len(seen) != 2 {
		t.Fatalf("want 2 unique, got %v", seen)
	}
}

func TestMemtable_SeriesKeysForLabelFilter(t *testing.T) {
	m := New(0)
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"a": "1"}, Timestamp: 1, Value: 1})
	m.Put(storage.Sample{Metric: "m", Labels: map[string]string{"a": "2"}, Timestamp: 2, Value: 2})
	keys := m.SeriesKeysForLabelFilter("m", map[string]string{"a": "1"})
	if len(keys) != 1 || keys[0] != storage.SeriesKey("m", map[string]string{"a": "1"}) {
		t.Fatalf("filter a=1: %v", keys)
	}
	all := m.SeriesKeysForLabelFilter("m", nil)
	if len(all) != 2 {
		t.Fatalf("metric-only: %v", all)
	}
	m.Clear()
	if m.SeriesKeysForLabelFilter("m", nil) != nil {
		t.Fatal("index should clear with mem")
	}
}

func TestMemtable_LabelNames(t *testing.T) {
	m := New(0)
	if len(m.LabelNames()) != 0 {
		t.Fatalf("empty: %v", m.LabelNames())
	}
	m.Put(storage.Sample{Metric: "a", Labels: map[string]string{"j": "x"}, Timestamp: 1, Value: 1})
	names := m.LabelNames()
	if len(names) != 2 {
		t.Fatalf("want __name__ and j: %v", names)
	}
	vs := m.LabelValuesForName("__name__")
	if len(vs) != 1 || vs[0] != "a" {
		t.Fatalf("__name__ values: %v", vs)
	}
	vs2 := m.LabelValuesForName("j")
	if len(vs2) != 1 || vs2[0] != "x" {
		t.Fatalf("j values: %v", vs2)
	}
}
