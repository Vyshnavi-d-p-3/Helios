package promql

import (
	"fmt"
	"math"
	"regexp"
	"sort"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// Series is one time-series result row from the engine.
// It mirrors engine.FilterSeries but is redeclared here so the promql package
// has no import dependency on internal/engine (which depends on internal/api).
type Series struct {
	Metric  string
	Labels  map[string]string
	Samples []storage.Sample
}

// Engine is the minimal interface the evaluator needs.
//
// The Helios engine already exposes QueryRangeByLabelFilter; the adapter at
// the call site converts that to []Series. Keeping the interface narrow here
// makes the evaluator trivially testable with a fake engine.
type Engine interface {
	// SelectSeries returns all series whose metric and labels satisfy the
	// matchers, with samples in [startMs, endMs].
	SelectSeries(metric string, matchers []Matcher, startMs, endMs int64) ([]Series, error)
}

// Result is the output of an evaluation. Exactly one field is set.
//
// Vector: instant query result — one (metric, labels, value) per series at the
// query timestamp.
// Matrix: range query result — one (metric, labels, []sample) per series.
// Scalar: a single number (result of a NumberLiteral or aggregating to one).
type Result struct {
	Type   string // "vector" | "matrix" | "scalar"
	Vector []VectorSample
	Matrix []Series
	Scalar float64
}

// VectorSample is a single point with full identity.
type VectorSample struct {
	Metric    string
	Labels    map[string]string
	Timestamp int64
	Value     float64
}

// EvalInstant evaluates expr at a single timestamp.
//
// Convention: a range function like rate() inside an instant query implicitly
// uses (timestamp - range, timestamp]. Everything else evaluates at exactly
// timestamp, with a 5-minute staleness window applied to bare selectors.
func EvalInstant(eng Engine, expr Expr, timestamp int64) (*Result, error) {
	v, err := evalAt(eng, expr, timestamp)
	if err != nil {
		return nil, err
	}
	return &Result{Type: "vector", Vector: v}, nil
}

// EvalRange evaluates expr at every step in [start, end].
func EvalRange(eng Engine, expr Expr, startMs, endMs, stepMs int64) (*Result, error) {
	if stepMs <= 0 {
		return nil, fmt.Errorf("step must be > 0")
	}
	if endMs < startMs {
		return nil, fmt.Errorf("end < start")
	}

	// Bucket per-series across all steps.
	buckets := make(map[string]*Series) // seriesKey -> Series with samples
	order := []string{}                 // stable iteration order for tests

	for ts := startMs; ts <= endMs; ts += stepMs {
		samples, err := evalAt(eng, expr, ts)
		if err != nil {
			return nil, err
		}
		for _, s := range samples {
			key := serieskey(s.Metric, s.Labels)
			b, ok := buckets[key]
			if !ok {
				b = &Series{Metric: s.Metric, Labels: copyLabels(s.Labels)}
				buckets[key] = b
				order = append(order, key)
			}
			b.Samples = append(b.Samples, storage.Sample{
				Metric:    s.Metric,
				Labels:    s.Labels,
				Timestamp: s.Timestamp,
				Value:     s.Value,
			})
		}
	}

	matrix := make([]Series, 0, len(order))
	for _, key := range order {
		matrix = append(matrix, *buckets[key])
	}
	return &Result{Type: "matrix", Matrix: matrix}, nil
}

// evalAt is the inner evaluator. Returns an instant vector (one sample per series).
func evalAt(eng Engine, expr Expr, ts int64) ([]VectorSample, error) {
	switch e := expr.(type) {

	case *NumberLiteral:
		return []VectorSample{{Timestamp: ts, Value: e.Value}}, nil

	case *VectorSelector:
		return selectInstant(eng, e, ts)

	case *FunctionCall:
		return evalFunction(eng, e, ts)

	case *AggregateExpr:
		return evalAggregate(eng, e, ts)
	}
	return nil, fmt.Errorf("evaluator: unsupported expression %T", expr)
}

// selectInstant fetches the most recent sample within a 5-minute staleness
// window for every series matching the selector at time `ts`.
func selectInstant(eng Engine, vs *VectorSelector, ts int64) ([]VectorSample, error) {
	const stalenessMs = 5 * 60 * 1000

	start := ts - stalenessMs
	if vs.Range > 0 {
		start = ts - vs.Range
	}

	series, err := eng.SelectSeries(vs.Metric, vs.Matchers, start, ts)
	if err != nil {
		return nil, err
	}
	out := make([]VectorSample, 0, len(series))
	for _, s := range series {
		if len(s.Samples) == 0 {
			continue
		}
		// Last sample wins for instant queries
		last := s.Samples[len(s.Samples)-1]
		out = append(out, VectorSample{
			Metric:    s.Metric,
			Labels:    s.Labels,
			Timestamp: ts,
			Value:     last.Value,
		})
	}
	return out, nil
}

// selectRange returns the full sample slice for each series; used by range
// functions like rate().
func selectRange(eng Engine, vs *VectorSelector, ts int64) ([]Series, error) {
	if vs.Range <= 0 {
		return nil, fmt.Errorf("range function requires a [duration] selector")
	}
	return eng.SelectSeries(vs.Metric, vs.Matchers, ts-vs.Range, ts)
}

func evalFunction(eng Engine, fc *FunctionCall, ts int64) ([]VectorSample, error) {
	vs, ok := fc.Arg.(*VectorSelector)
	if !ok {
		return nil, fmt.Errorf("function %s requires a vector selector argument", fc.Name)
	}
	series, err := selectRange(eng, vs, ts)
	if err != nil {
		return nil, err
	}

	out := make([]VectorSample, 0, len(series))
	for _, s := range series {
		if len(s.Samples) == 0 {
			continue
		}
		v, ok := applyRangeFunction(fc.Name, s.Samples, vs.Range)
		if !ok {
			continue // not enough samples to compute (e.g., rate needs ≥2)
		}
		out = append(out, VectorSample{
			Metric:    s.Metric,
			Labels:    s.Labels,
			Timestamp: ts,
			Value:     v,
		})
	}
	return out, nil
}

// applyRangeFunction implements the supported range functions. Returns
// (value, true) on success or (_, false) when the function cannot produce
// a result (e.g., rate with one sample).
func applyRangeFunction(name string, samples []storage.Sample, rangeMs int64) (float64, bool) {
	switch name {
	case "rate":
		return rateFn(samples, rangeMs)
	case "irate":
		return irateFn(samples)
	case "increase":
		v, ok := rateFn(samples, rangeMs)
		if !ok {
			return 0, false
		}
		return v * float64(rangeMs) / 1000.0, true
	case "avg_over_time":
		return reduceMean(samples), true
	case "sum_over_time":
		return reduceSum(samples), true
	case "count_over_time":
		return float64(len(samples)), true
	case "min_over_time":
		return reduceMin(samples), true
	case "max_over_time":
		return reduceMax(samples), true
	case "stddev_over_time":
		return reduceStddev(samples), true
	}
	return 0, false
}

// rateFn implements PromQL rate(). It returns the per-second average rate of
// increase, accounting for counter resets.
//
// The scaling factor: PromQL's rate() extrapolates to the full window if the
// first/last sample don't span it. We use the simple formula
// (delta / duration_seconds), where delta sums positive increments and treats
// any decrease as a counter reset (the "missing" amount equals the value just
// before the reset).
func rateFn(samples []storage.Sample, rangeMs int64) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	delta := 0.0
	for i := 1; i < len(samples); i++ {
		curr := samples[i].Value
		prev := samples[i-1].Value
		if curr >= prev {
			delta += curr - prev
		} else {
			// Counter reset — treat the drop from prev to 0 as the missing piece,
			// then add the new value as the increment from 0.
			delta += curr
		}
	}
	durSec := float64(rangeMs) / 1000.0
	if durSec <= 0 {
		return 0, false
	}
	return delta / durSec, true
}

// irateFn returns the per-second instant rate of increase based only on the
// last two samples (matches Prometheus' irate() semantics).
func irateFn(samples []storage.Sample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	a := samples[len(samples)-2]
	b := samples[len(samples)-1]
	durMs := b.Timestamp - a.Timestamp
	if durMs <= 0 {
		return 0, false
	}
	delta := b.Value - a.Value
	if delta < 0 {
		delta = b.Value // counter reset
	}
	return delta / (float64(durMs) / 1000.0), true
}

func reduceSum(s []storage.Sample) float64 {
	v := 0.0
	for _, x := range s {
		v += x.Value
	}
	return v
}

func reduceMean(s []storage.Sample) float64 {
	if len(s) == 0 {
		return 0
	}
	return reduceSum(s) / float64(len(s))
}

func reduceMin(s []storage.Sample) float64 {
	m := math.Inf(1)
	for _, x := range s {
		if x.Value < m {
			m = x.Value
		}
	}
	return m
}

func reduceMax(s []storage.Sample) float64 {
	m := math.Inf(-1)
	for _, x := range s {
		if x.Value > m {
			m = x.Value
		}
	}
	return m
}

func reduceStddev(s []storage.Sample) float64 {
	if len(s) == 0 {
		return 0
	}
	mean := reduceMean(s)
	sq := 0.0
	for _, x := range s {
		d := x.Value - mean
		sq += d * d
	}
	return math.Sqrt(sq / float64(len(s)))
}

// evalAggregate implements sum/count/avg/min/max with `by ()` grouping.
func evalAggregate(eng Engine, agg *AggregateExpr, ts int64) ([]VectorSample, error) {
	inner, err := evalAt(eng, agg.Inner, ts)
	if err != nil {
		return nil, err
	}
	if len(inner) == 0 {
		return nil, nil
	}

	// Group by the requested labels (empty = single global group)
	groups := make(map[string]*aggGroup)
	groupOrder := []string{}

	for _, s := range inner {
		gl := groupingLabels(s.Labels, agg.By)
		gk := serieskey("", gl)
		g, ok := groups[gk]
		if !ok {
			g = &aggGroup{labels: gl}
			groups[gk] = g
			groupOrder = append(groupOrder, gk)
		}
		g.values = append(g.values, s.Value)
	}

	out := make([]VectorSample, 0, len(groupOrder))
	for _, gk := range groupOrder {
		g := groups[gk]
		var v float64
		switch agg.Op {
		case "sum":
			for _, x := range g.values {
				v += x
			}
		case "count":
			v = float64(len(g.values))
		case "avg":
			for _, x := range g.values {
				v += x
			}
			v /= float64(len(g.values))
		case "min":
			v = math.Inf(1)
			for _, x := range g.values {
				if x < v {
					v = x
				}
			}
		case "max":
			v = math.Inf(-1)
			for _, x := range g.values {
				if x > v {
					v = x
				}
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation %q", agg.Op)
		}
		out = append(out, VectorSample{
			Metric:    "",
			Labels:    g.labels,
			Timestamp: ts,
			Value:     v,
		})
	}
	return out, nil
}

type aggGroup struct {
	labels map[string]string
	values []float64
}

// groupingLabels returns just the labels named in `by`, or an empty map if
// `by` is empty (which means: aggregate everything into one group).
func groupingLabels(in map[string]string, by []string) map[string]string {
	if len(by) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(by))
	for _, name := range by {
		if v, ok := in[name]; ok {
			out[name] = v
		}
	}
	return out
}

// MatcherToRegex compiles a regex matcher's value. Anchors are added like
// Prometheus does (^...$). Invalid regexes return an error at parse time
// rather than being silently treated as literals.
func MatcherToRegex(m Matcher) (*regexp.Regexp, error) {
	if m.Op != MatchRegex && m.Op != MatchNotRegex {
		return nil, fmt.Errorf("not a regex matcher")
	}
	return regexp.Compile("^(?:" + m.Value + ")$")
}

// ─── helpers ────────────────────────────────────────────────────────────────

func serieskey(metric string, labels map[string]string) string {
	if len(labels) == 0 {
		return metric
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := metric + "{"
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += k + "=" + labels[k]
	}
	out += "}"
	return out
}

func copyLabels(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
