package blockcache

import (
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestCache_HitMiss(t *testing.T) {
	c := New(1000)
	k := Key{SSTableID: 1, SeriesID: 42}
	if _, ok := c.Get(k); ok {
		t.Fatal("unexpected hit on empty cache")
	}
	samples := []storage.Sample{{Timestamp: 1, Value: 1.0}}
	c.Put(k, samples)
	got, ok := c.Get(k)
	if !ok {
		t.Fatal("expected hit")
	}
	if len(got) != 1 || got[0].Value != 1.0 {
		t.Fatalf("got %+v", got)
	}
}

func TestCache_EvictsLRUBySamples(t *testing.T) {
	c := New(10) // budget: 10 samples
	for i := 0; i < 5; i++ {
		c.Put(Key{SeriesID: uint64(i)}, makeSamples(3)) // 5 * 3 = 15 samples
	}
	// We've inserted 15 samples into a 10-sample budget. Some must have been evicted.
	snap := c.Snapshot()
	if snap.Samples > 10 {
		t.Fatalf("over budget: %d samples", snap.Samples)
	}
	// Earliest-inserted keys should be gone.
	if _, ok := c.Get(Key{SeriesID: 0}); ok {
		t.Fatal("expected key 0 to be evicted")
	}
	// Most recent must still be there.
	if _, ok := c.Get(Key{SeriesID: 4}); !ok {
		t.Fatal("expected key 4 to be cached")
	}
}

func TestCache_InvalidateSSTable(t *testing.T) {
	c := New(1000)
	c.Put(Key{SSTableID: 1, SeriesID: 1}, makeSamples(2))
	c.Put(Key{SSTableID: 1, SeriesID: 2}, makeSamples(2))
	c.Put(Key{SSTableID: 2, SeriesID: 1}, makeSamples(2))

	c.InvalidateSSTable(1)
	if _, ok := c.Get(Key{SSTableID: 1, SeriesID: 1}); ok {
		t.Fatal("expected eviction of SSTableID=1 entries")
	}
	if _, ok := c.Get(Key{SSTableID: 2, SeriesID: 1}); !ok {
		t.Fatal("expected SSTableID=2 entry to remain")
	}
}

func TestCache_HitMissCounters(t *testing.T) {
	c := New(100)
	c.Put(Key{SeriesID: 1}, makeSamples(1))
	c.Get(Key{SeriesID: 1})
	c.Get(Key{SeriesID: 1})
	c.Get(Key{SeriesID: 999})

	snap := c.Snapshot()
	if snap.Hits != 2 || snap.Misses != 1 {
		t.Fatalf("hits=%d misses=%d, want 2/1", snap.Hits, snap.Misses)
	}
}

func makeSamples(n int) []storage.Sample {
	out := make([]storage.Sample, n)
	for i := range out {
		out[i] = storage.Sample{Timestamp: int64(i), Value: float64(i)}
	}
	return out
}
