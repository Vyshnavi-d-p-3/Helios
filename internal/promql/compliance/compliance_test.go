package compliance

import (
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/promql"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

type fixtureEngine struct {
	rows []promql.Series
}

func (f *fixtureEngine) SelectSeries(metric string, _ []promql.Matcher, start, end int64) ([]promql.Series, error) {
	out := make([]promql.Series, 0, len(f.rows))
	for _, r := range f.rows {
		if r.Metric != metric {
			continue
		}
		filtered := make([]storage.Sample, 0, len(r.Samples))
		for _, s := range r.Samples {
			if s.Timestamp >= start && s.Timestamp <= end {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) == 0 {
			continue
		}
		out = append(out, promql.Series{
			Metric:  r.Metric,
			Labels:  r.Labels,
			Samples: filtered,
		})
	}
	return out, nil
}

func TestPromQLSubsetCompliance_Smoke(t *testing.T) {
	rows := []promql.Series{
		{
			Metric: "req_total",
			Labels: map[string]string{"job": "api", "status": "200"},
			Samples: []storage.Sample{
				{Timestamp: 0, Value: 0},
				{Timestamp: 60_000, Value: 60},
				{Timestamp: 120_000, Value: 120},
			},
		},
		{
			Metric: "req_total",
			Labels: map[string]string{"job": "api", "status": "500"},
			Samples: []storage.Sample{
				{Timestamp: 0, Value: 0},
				{Timestamp: 60_000, Value: 12},
				{Timestamp: 120_000, Value: 24},
			},
		},
		{
			Metric: "latency",
			Labels: map[string]string{"job": "api"},
			Samples: []storage.Sample{
				{Timestamp: 0, Value: 10},
				{Timestamp: 60_000, Value: 20},
				{Timestamp: 120_000, Value: 30},
			},
		},
	}
	eng := &fixtureEngine{rows: rows}

	tests := []struct {
		name       string
		query      string
		ts         int64
		wantVector int
		wantFirst  float64
	}{
		{name: "vector selector", query: `req_total{job="api"}`, ts: 120_000, wantVector: 2, wantFirst: 120},
		{name: "rate", query: `rate(req_total[1m])`, ts: 120_000, wantVector: 2, wantFirst: 1},
		{name: "irate", query: `irate(req_total[1m])`, ts: 120_000, wantVector: 2, wantFirst: 1},
		{name: "increase", query: `increase(req_total[1m])`, ts: 120_000, wantVector: 2, wantFirst: 60},
		{name: "avg over time", query: `avg_over_time(latency[2m])`, ts: 120_000, wantVector: 1, wantFirst: 20},
		{name: "sum by", query: `sum by (job) (req_total{job="api"})`, ts: 120_000, wantVector: 1, wantFirst: 144},
		{name: "count by", query: `count by (job) (req_total{job="api"})`, ts: 120_000, wantVector: 1, wantFirst: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := promql.Parse(tc.query)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.query, err)
			}
			got, err := promql.EvalInstant(eng, expr, tc.ts)
			if err != nil {
				t.Fatalf("eval %q: %v", tc.query, err)
			}
			if len(got.Vector) != tc.wantVector {
				t.Fatalf("%q vector len=%d want=%d", tc.query, len(got.Vector), tc.wantVector)
			}
			if tc.wantVector > 0 {
				if diff(got.Vector[0].Value, tc.wantFirst) > 1e-9 {
					t.Fatalf("%q first value=%v want=%v", tc.query, got.Vector[0].Value, tc.wantFirst)
				}
			}
		})
	}
}

func TestPromQLSubsetCompliance_UnsupportedExamples(t *testing.T) {
	unsupported := []string{
		`req_total + req_total`,
		`sum without (job) (req_total)`,
		`req_total[5m:30s]`,
		`histogram_quantile(0.9, req_total[5m])`,
	}
	for _, q := range unsupported {
		t.Run(q, func(t *testing.T) {
			if _, err := promql.Parse(q); err == nil {
				t.Fatalf("expected unsupported query to fail parse: %q", q)
			}
		})
	}
}

func diff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}
