package cluster

import (
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestEncodeDecodeBatch_roundTrip(t *testing.T) {
	in := []storage.Sample{
		{Metric: "cpu", Labels: map[string]string{"host": "a"}, Timestamp: 1000, Value: 1.5},
		{Metric: "cpu", Labels: map[string]string{"host": "b"}, Timestamp: 2000, Value: 2.5},
	}
	b, err := encodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("len(out)=%d want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Metric != in[i].Metric || out[i].Timestamp != in[i].Timestamp || out[i].Value != in[i].Value {
			t.Fatalf("sample[%d] mismatch: got %+v want %+v", i, out[i], in[i])
		}
		if out[i].Labels["host"] != in[i].Labels["host"] {
			t.Fatalf("labels mismatch: got %+v want %+v", out[i].Labels, in[i].Labels)
		}
	}
}
