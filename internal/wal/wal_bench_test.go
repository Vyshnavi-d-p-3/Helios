package wal

import (
	"path/filepath"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func BenchmarkAppend_Binary(b *testing.B) {
	p := filepath.Join(b.TempDir(), "bench.wal")
	w, err := Open(p)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	samples := make([]storage.Sample, 100)
	for i := range samples {
		samples[i] = storage.Sample{
			Metric:    "req_total",
			Labels:    map[string]string{"job": "api", "env": "dev"},
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(samples); err != nil {
			b.Fatal(err)
		}
	}
}
