package anomaly

import (
	"math"
	"testing"
)

func TestRegistryProcess_rejectsNaN(t *testing.T) {
	r := NewRegistry(RegistryConfig{MaxSeries: 8, StaleAfter: 0, Alpha: 0.3, Threshold: 3, Warmup: 1})
	if e := r.Process("m", nil, "a", 1, 1.0); e != nil {
		t.Fatalf("first: %v", e)
	}
	if e := r.Process("m", nil, "a", 2, math.NaN()); e != nil {
		t.Fatalf("expected nil on NaN, got %v", e)
	}
	m := r.Metrics()
	if m.NaNRejects != 1 {
		t.Fatalf("NaNRejects=%d", m.NaNRejects)
	}
}
