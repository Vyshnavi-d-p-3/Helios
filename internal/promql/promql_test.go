package promql

import (
	"reflect"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestParse_Selectors(t *testing.T) {
	cases := []struct {
		in       string
		wantErr  bool
		wantKind string
	}{
		{`up`, false, "vector"},
		{`up{}`, false, "vector"},
		{`up{job="api"}`, false, "vector"},
		{`up{job="api", env="prod"}`, false, "vector"},
		{`up{job="api"}[5m]`, false, "range"},
		{`{__name__="up"}`, false, "vector"},
		{`up{a!="x"}`, false, "vector"},
		{`up{a=~"5.."}`, false, "vector"},
		{`up{a!~"5.."}`, false, "vector"},
		{`rate(up[5m])`, false, "fn"},
		{`sum(up)`, false, "agg"},
		{`sum by (job) (up)`, false, "agg"},
		{`sum(up) by (job)`, false, "agg"},
		{`sum by (job) (rate(http_requests_total[5m]))`, false, "agg"},
		{`avg_over_time(http_latency[1h])`, false, "fn"},
		{`{`, true, ""},
		{`up{a=}`, true, ""},
		{`up{a=`, true, ""},
		{`rate(up)`, true, ""}, // rate() requires range vector
		{`unknown_func(up[5m])`, true, ""},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			expr, err := Parse(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %#v", c.in, expr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", c.in, err)
			}
		})
	}
}

func TestParse_Matcher(t *testing.T) {
	expr, err := Parse(`http_requests_total{status=~"5..", method!="GET"}[5m]`)
	if err != nil {
		t.Fatal(err)
	}
	vs, ok := expr.(*VectorSelector)
	if !ok {
		t.Fatalf("expected VectorSelector, got %T", expr)
	}
	if vs.Metric != "http_requests_total" {
		t.Errorf("Metric = %q", vs.Metric)
	}
	if vs.Range != 5*60*1000 {
		t.Errorf("Range = %d ms", vs.Range)
	}
	if len(vs.Matchers) != 2 {
		t.Fatalf("matchers = %d", len(vs.Matchers))
	}
	if vs.Matchers[0].Op != MatchRegex || vs.Matchers[0].Value != "5.." {
		t.Errorf("matcher 0 = %+v", vs.Matchers[0])
	}
	if vs.Matchers[1].Op != MatchNotEqual || vs.Matchers[1].Value != "GET" {
		t.Errorf("matcher 1 = %+v", vs.Matchers[1])
	}
}

func TestParse_Aggregation(t *testing.T) {
	expr, err := Parse(`sum by (status) (rate(http_requests_total[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	agg, ok := expr.(*AggregateExpr)
	if !ok {
		t.Fatalf("expected AggregateExpr, got %T", expr)
	}
	if agg.Op != "sum" {
		t.Errorf("Op = %q", agg.Op)
	}
	if !reflect.DeepEqual(agg.By, []string{"status"}) {
		t.Errorf("By = %v", agg.By)
	}
	fc, ok := agg.Inner.(*FunctionCall)
	if !ok || fc.Name != "rate" {
		t.Errorf("inner = %#v", agg.Inner)
	}
}

// fakeEngine is a minimal Engine for tests.
type fakeEngine struct {
	rows []Series
}

func (f *fakeEngine) SelectSeries(metric string, _ []Matcher, start, end int64) ([]Series, error) {
	out := make([]Series, 0, len(f.rows))
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
		out = append(out, Series{Metric: r.Metric, Labels: r.Labels, Samples: filtered})
	}
	return out, nil
}

func TestEval_RateOverCounter(t *testing.T) {
	// 5 samples 15s apart, value increases by 5 each step → rate = 1/3 per second
	samples := make([]storage.Sample, 0, 5)
	for i := 0; i < 5; i++ {
		samples = append(samples, storage.Sample{
			Metric:    "req",
			Timestamp: int64(i * 15_000),
			Value:     float64(i * 5),
		})
	}
	eng := &fakeEngine{rows: []Series{{Metric: "req", Samples: samples}}}

	expr, err := Parse(`rate(req[60s])`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := EvalInstant(eng, expr, 60_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Vector) != 1 {
		t.Fatalf("vector len %d", len(r.Vector))
	}
	got := r.Vector[0].Value
	want := 20.0 / 60.0 // delta=20 over 60s window
	if absDelta(got, want) > 1e-9 {
		t.Errorf("rate = %v, want %v", got, want)
	}
}

func TestEval_RateHandlesCounterReset(t *testing.T) {
	// counter goes 10, 20, 5 (reset!), 15
	samples := []storage.Sample{
		{Timestamp: 0, Value: 10},
		{Timestamp: 15_000, Value: 20},
		{Timestamp: 30_000, Value: 5},
		{Timestamp: 45_000, Value: 15},
	}
	eng := &fakeEngine{rows: []Series{{Metric: "c", Samples: samples}}}
	expr, _ := Parse(`rate(c[60s])`)
	r, _ := EvalInstant(eng, expr, 45_000)
	// expected delta: (20-10) + 5 (reset, so we count 5 as the new increment) + (15-5) = 25
	want := 25.0 / 60.0
	got := r.Vector[0].Value
	if absDelta(got, want) > 1e-9 {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestEval_AvgOverTime(t *testing.T) {
	samples := []storage.Sample{
		{Timestamp: 0, Value: 1},
		{Timestamp: 15_000, Value: 3},
		{Timestamp: 30_000, Value: 5},
	}
	eng := &fakeEngine{rows: []Series{{Metric: "x", Samples: samples}}}
	expr, _ := Parse(`avg_over_time(x[1m])`)
	r, _ := EvalInstant(eng, expr, 60_000)
	if got := r.Vector[0].Value; absDelta(got, 3.0) > 1e-9 {
		t.Errorf("got %v", got)
	}
}

func TestEval_SumByGroup(t *testing.T) {
	rows := []Series{
		{
			Metric: "req",
			Labels: map[string]string{"status": "200"},
			Samples: []storage.Sample{{Timestamp: 0, Value: 10}, {Timestamp: 60_000, Value: 20}},
		},
		{
			Metric: "req",
			Labels: map[string]string{"status": "500"},
			Samples: []storage.Sample{{Timestamp: 0, Value: 1}, {Timestamp: 60_000, Value: 3}},
		},
		{
			Metric: "req",
			Labels: map[string]string{"status": "200"},
			Samples: []storage.Sample{{Timestamp: 0, Value: 100}, {Timestamp: 60_000, Value: 200}},
		},
	}
	eng := &fakeEngine{rows: rows}
	expr, _ := Parse(`sum by (status) (req)`)
	r, _ := EvalInstant(eng, expr, 60_000)
	if len(r.Vector) != 2 {
		t.Fatalf("got %d groups", len(r.Vector))
	}
	got := map[string]float64{}
	for _, v := range r.Vector {
		got[v.Labels["status"]] = v.Value
	}
	if got["200"] != 220 || got["500"] != 3 {
		t.Errorf("got %+v", got)
	}
}

func TestEval_RangeQueryProducesMatrix(t *testing.T) {
	samples := []storage.Sample{
		{Timestamp: 0, Value: 0}, {Timestamp: 30_000, Value: 30},
		{Timestamp: 60_000, Value: 60}, {Timestamp: 90_000, Value: 90},
	}
	eng := &fakeEngine{rows: []Series{{Metric: "x", Samples: samples}}}
	expr, _ := Parse(`x`)
	r, err := EvalRange(eng, expr, 0, 90_000, 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if r.Type != "matrix" {
		t.Fatalf("type = %s", r.Type)
	}
	if len(r.Matrix) != 1 {
		t.Fatalf("series = %d", len(r.Matrix))
	}
	if len(r.Matrix[0].Samples) != 4 {
		t.Errorf("samples = %d", len(r.Matrix[0].Samples))
	}
}

func absDelta(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}
