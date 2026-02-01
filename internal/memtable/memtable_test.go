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
