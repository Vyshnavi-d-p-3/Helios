package engine

import (
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestCardinality_rejectsOverCap(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.MaxSeriesPerMetric = 2
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = e.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Timestamp: 1, Value: 1},
		{Metric: "m", Labels: map[string]string{"a": "2"}, Timestamp: 1, Value: 1},
	})
	if err := e.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"a": "3"}, Timestamp: 1, Value: 1},
	}); err == nil {
		t.Fatal("expected cardinality error")
	}
}

func TestCardinality_batchExceedsBeforeAppend(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.MaxSeriesPerMetric = 1
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Timestamp: 1, Value: 1},
		{Metric: "m", Labels: map[string]string{"a": "2"}, Timestamp: 1, Value: 1},
	}); err == nil {
		t.Fatal("expected error for two new series in one batch when cap=1")
	}
}
