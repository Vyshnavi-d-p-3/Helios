// Package promqlbridge adapts the existing internal/engine.Engine to the
// internal/promql.Engine interface.
//
// This indirection exists for two reasons:
//   1. The promql package can be unit-tested with a fake engine (no FS/WAL).
//   2. Future engines (a Raft-replicated coordinator, an in-memory test fixture)
//      can satisfy promql.Engine without depending on the concrete engine type.
//
// Add this file under internal/api/promqlbridge.go (or wherever the API
// constructs the evaluator). It does not introduce a new public package.
package api

import (
	"fmt"
	"regexp"

	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/promql"
)

// engineAdapter implements promql.Engine on top of *engine.Engine.
type engineAdapter struct {
	e *engine.Engine
}

func newEngineAdapter(e *engine.Engine) *engineAdapter {
	return &engineAdapter{e: e}
}

// SelectSeries fetches series matching the metric and label matchers.
//
// The current Helios engine only supports equality matchers natively; for non-
// equality matchers we use the engine's broader QueryRangeByLabelFilter (which
// returns all series sharing the equality subset of the matchers) and filter
// the results in this adapter. That is correct but not maximally efficient;
// pushing matcher evaluation into the postings index is a subsequent optimization.
func (a *engineAdapter) SelectSeries(metric string, matchers []promql.Matcher, startMs, endMs int64) ([]promql.Series, error) {
	if a.e == nil {
		return nil, fmt.Errorf("engine: nil")
	}
	if err := a.e.CheckQueryTimeRange(startMs, endMs); err != nil {
		return nil, err
	}

	// Split matchers into the equality subset (passed to the engine to use the
	// postings path) and the rest (applied here as a filter).
	eq, rest, err := splitMatchers(matchers)
	if err != nil {
		return nil, err
	}

	rows := a.e.QueryRangeByLabelFilter(metric, eq, startMs, endMs)

	var out []promql.Series
	for _, r := range rows {
		// Re-derive labels from the first sample (engine returns them per-sample).
		var labels map[string]string
		if len(r.Samples) > 0 {
			labels = copyMap(r.Samples[0].Labels)
		}
		if len(rest) > 0 && !matchesAll(labels, rest) {
			continue
		}
		out = append(out, promql.Series{
			Metric:  metric,
			Labels:  labels,
			Samples: r.Samples,
		})
	}
	return out, nil
}

// splitMatchers separates equality matchers from all others.
func splitMatchers(in []promql.Matcher) (eq map[string]string, rest []promql.Matcher, err error) {
	eq = map[string]string{}
	for _, m := range in {
		switch m.Op {
		case promql.MatchEqual:
			eq[m.Name] = m.Value
		case promql.MatchNotEqual, promql.MatchRegex, promql.MatchNotRegex:
			rest = append(rest, m)
		default:
			return nil, nil, fmt.Errorf("unsupported matcher op %v", m.Op)
		}
	}
	return eq, rest, nil
}

// matchesAll returns true iff every matcher in `ms` accepts the labels.
func matchesAll(labels map[string]string, ms []promql.Matcher) bool {
	for _, m := range ms {
		v := labels[m.Name]
		switch m.Op {
		case promql.MatchNotEqual:
			if v == m.Value {
				return false
			}
		case promql.MatchRegex:
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil || !re.MatchString(v) {
				return false
			}
		case promql.MatchNotRegex:
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				return false
			}
			if re.MatchString(v) {
				return false
			}
		}
	}
	return true
}

func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
