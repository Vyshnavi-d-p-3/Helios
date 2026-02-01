package engine

import (
	"sort"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// LabelMatchSpec is one partial selector: metric and optional label subset
// (extra labels on the series are allowed), matching label-filtered queries.
type LabelMatchSpec struct {
	Metric string
	Labels map[string]string
}

// SeriesWithLabelsInWindow returns Prometheus-style label sets (including __name__)
// for every series that has at least one point in [start,end] on (metric, want).
// When partial is false, want defines the full label set; when true, a subset
// (same as QueryRangeByLabelFilter).
func (e *Engine) SeriesWithLabelsInWindow(metric string, want map[string]string, start, end int64, partial bool) []map[string]string {
	if start > end || metric == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := e.collectLabelFilterKeysLocked(metric, want, partial)
	seen := make(map[string]struct{}, len(keys))
	var out []map[string]string
	for _, sk := range keys {
		pts := e.queryRangeLocked(sk, start, end)
		if len(pts) == 0 {
			continue
		}
		s0 := pts[0]
		lm := storage.LabelMapForProm(s0)
		k := storage.SeriesKey(s0.Metric, s0.Labels)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, lm)
	}
	return out
}

// SeriesWithLabelsUnionMatchSpecs runs partial label match for each spec and
// unions/deduplicates the resulting series. Used for multiple match[] parameters.
func (e *Engine) SeriesWithLabelsUnionMatchSpecs(specs []LabelMatchSpec, start, end int64) []map[string]string {
	if start > end {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seriesWithLabelsUnionMatchSpecsLocked(specs, start, end)
}

func (e *Engine) seriesWithLabelsUnionMatchSpecsLocked(specs []LabelMatchSpec, start, end int64) []map[string]string {
	seen := make(map[string]struct{})
	var out []map[string]string
	for _, sp := range specs {
		if sp.Metric == "" {
			continue
		}
		keys := e.collectLabelFilterKeysLocked(sp.Metric, sp.Labels, true)
		for _, sk := range keys {
			pts := e.queryRangeLocked(sk, start, end)
			if len(pts) == 0 {
				continue
			}
			s0 := pts[0]
			k := storage.SeriesKey(s0.Metric, s0.Labels)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, storage.LabelMapForProm(s0))
		}
	}
	return out
}

// SeriesWithLabelsInWindowUnion is like partial match with an empty want for
// each of several bare metric names, merged and deduplicated.
func (e *Engine) SeriesWithLabelsInWindowUnion(metrics []string, start, end int64) []map[string]string {
	if start > end {
		return nil
	}
	uniq := make(map[string]struct{}, len(metrics))
	for _, m := range metrics {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		uniq[m] = struct{}{}
	}
	if len(uniq) == 0 {
		return nil
	}
	metricList := make([]string, 0, len(uniq))
	for m := range uniq {
		metricList = append(metricList, m)
	}
	sort.Strings(metricList)
	specs := make([]LabelMatchSpec, 0, len(metricList))
	for _, m := range metricList {
		specs = append(specs, LabelMatchSpec{Metric: m, Labels: nil})
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seriesWithLabelsUnionMatchSpecsLocked(specs, start, end)
}
