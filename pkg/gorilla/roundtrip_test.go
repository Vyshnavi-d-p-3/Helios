package gorilla

import (
	"math"
	"testing"
)

func TestRoundTrip_monotonicRealistic(t *testing.T) {
	var ts int64 = 1_700_000_000_000
	var dt int64 = 15_000
	vals := []float64{1, 1.1, 1.2, 3.2e5, 3.2e5 + 0.1}
	enc := NewEncoder()
	for i := 0; i < len(vals); i++ {
		enc.Encode(ts+int64(i)*dt, vals[i])
	}
	compressed := enc.Finish()

	tsOut, vOut, err := DecodeAll(compressed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(tsOut) != len(vals) || len(vOut) != len(vals) {
		t.Fatalf("count mismatch: got %d/%d want %d", len(tsOut), len(vOut), len(vals))
	}
	for i := range vals {
		if tsOut[i] != ts+int64(i)*dt {
			t.Errorf("ts[%d] got %d want %d", i, tsOut[i], ts+int64(i)*dt)
		}
		if vOut[i] != vals[i] {
			t.Errorf("v[%d] got %g want %g", i, vOut[i], vals[i])
		}
	}
}

func TestRoundTrip_xorsShareHigh32Bits(t *testing.T) {
	// Drives the encoder's leading-zeros clamp: consecutive values with identical high 32 bits.
	var ts int64 = 1_000
	a := math.Float64frombits(0x0000_0000_3fff_ffff) // < 2^32, high 32 clear
	b := math.Float64frombits(0x0000_0000_3fff_fffe)
	enc := NewEncoder()
	enc.Encode(ts, a)
	enc.Encode(ts+1, b)
	compressed := enc.Finish()
	tsOut, vOut, err := DecodeAll(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if vOut[0] != a || vOut[1] != b {
		t.Fatalf("got %#v %v", vOut[0], vOut[1])
	}
	if tsOut[0] != ts || tsOut[1] != ts+1 {
		t.Fatalf("ts %v %v", tsOut[0], tsOut[1])
	}
}
