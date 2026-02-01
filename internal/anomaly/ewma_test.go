package anomaly

import (
	"math"
	"testing"
	"time"
)

func TestEWMADetector_ConstantStream(t *testing.T) {
	d := NewEWMADetector(0.3, 3.0, 10)
	for i := 0; i < 100; i++ {
		if e := d.Update(42.0); e != nil {
			t.Fatalf("unexpected anomaly at i=%d: %+v", i, e)
		}
	}
}

func TestEWMADetector_DetectsSpike(t *testing.T) {
	d := NewEWMADetector(0.3, 3.0, 10)
	// Train on a stable mean of 10 with small jitter.
	for i := 0; i < 50; i++ {
		d.Update(10.0 + float64(i%3-1)*0.1)
	}
	if e := d.Update(100.0); e == nil || e.Type != Spike {
		t.Fatalf("expected spike, got %+v", e)
	}
}

func TestEWMADetector_DetectsDrop(t *testing.T) {
	d := NewEWMADetector(0.3, 3.0, 10)
	for i := 0; i < 50; i++ {
		d.Update(10.0 + float64(i%3-1)*0.1)
	}
	if e := d.Update(0.1); e == nil || e.Type != Drop {
		t.Fatalf("expected drop, got %+v", e)
	}
}

func TestEWMADetector_NoAlertDuringWarmup(t *testing.T) {
	d := NewEWMADetector(0.3, 3.0, 10)
	// Even with violently varying samples, warmup suppresses alerts.
	values := []float64{1, 1000, 1, 1000, 1, 1000, 1, 1000, 1, 1000}
	for i, v := range values {
		if e := d.Update(v); e != nil {
			t.Fatalf("unexpected alert at i=%d: %+v", i, e)
		}
	}
}

func TestEWMADetector_RejectsNaN(t *testing.T) {
	d := NewEWMADetector(0.3, 3.0, 10)
	for i := 0; i < 50; i++ {
		d.Update(10.0)
	}
	statsBefore := d.Stats()
	if e := d.Update(math.NaN()); e != nil {
		t.Fatalf("unexpected event for NaN: %+v", e)
	}
	if e := d.Update(math.Inf(1)); e != nil {
		t.Fatalf("unexpected event for +Inf: %+v", e)
	}
	statsAfter := d.Stats()
	if statsBefore != statsAfter {
		t.Fatalf("state changed despite NaN/Inf:\nbefore=%+v\nafter=%+v", statsBefore, statsAfter)
	}
}

func TestRegistry_RespectsMaxSeries(t *testing.T) {
	cfg := DefaultRegistryConfig()
	cfg.MaxSeries = 100
	r := NewRegistry(cfg)
	// Push 200 unique series; expect 100 evictions.
	for i := 0; i < 200; i++ {
		key := makeKey(i)
		r.Process("m", nil, key, 1_000, 1.0)
	}
	snap := r.Snapshot()
	if snap.SeriesCount != 100 {
		t.Fatalf("series count = %d, want 100", snap.SeriesCount)
	}
	if snap.EvictionsTotal != 100 {
		t.Fatalf("evictions = %d, want 100", snap.EvictionsTotal)
	}
}

func TestRegistry_PrunesStale(t *testing.T) {
	cfg := DefaultRegistryConfig()
	cfg.MaxSeries = 1000
	cfg.StaleAfter = 1 * time.Second
	r := NewRegistry(cfg)
	now := time.Unix(1_000_000, 0)
	for i := 0; i < 50; i++ {
		// timestamp in ms; lastSeenUnix = ts/1000
		r.Process("m", nil, makeKey(i), now.Unix()*1000, 1.0)
	}
	pruned := r.PruneStale(now.Add(2 * time.Second)) // 2s past stale window
	if pruned != 50 {
		t.Fatalf("pruned = %d, want 50", pruned)
	}
}

func TestRegistry_FanOut(t *testing.T) {
	r := NewRegistry(DefaultRegistryConfig())
	ch := make(chan AnomalyEvent, 4)
	r.Subscribe(ch)

	// Train, then trigger a spike.
	for i := 0; i < 50; i++ {
		r.Process("m", nil, "k1", int64(i*1000), 5.0)
	}
	r.Process("m", nil, "k1", 60_000, 500.0)

	select {
	case e := <-ch:
		if e.Type != Spike {
			t.Fatalf("expected spike, got %v", e.Type)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("no event delivered to subscriber")
	}
}

func makeKey(i int) string {
	const hex = "0123456789abcdef"
	return string([]byte{hex[i&0xf], hex[(i>>4)&0xf]}) + "k"
}
