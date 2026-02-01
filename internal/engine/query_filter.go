package engine

import (
	"sort"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// FilterSeries holds points for one time series from QueryRangeByLabelFilter.
type FilterSeries struct {
	Series  string
	Samples []storage.Sample
}

// collectLabelFilterKeysLocked must be called with e.mu held. When partial is
// false, returns at most one key: storage.SeriesKey(metric, want). When partial
// is true, returns the union of series keys that match the metric and label
// subset in mem and SSTs.
func (e *Engine) collectLabelFilterKeysLocked(metric string, want map[string]string, partial bool) []string {
	if metric == "" {
		return nil
	}
	if !partial {
		return []string{storage.SeriesKey(metric, want)}
	}
	cand := make(map[string]struct{})
	for _, sk := range e.mem.SeriesKeysForLabelFilter(metric, want) {
		cand[sk] = struct{}{}
	}
	for _, fm := range e.frozen {
		for _, sk := range fm.SeriesKeysForLabelFilter(metric, want) {
			cand[sk] = struct{}{}
		}
	}
	for _, t := range e.sst {
		for _, sk := range t.LocalSeriesForFilter(metric, want) {
			cand[sk] = struct{}{}
		}
	}
	keys := make([]string, 0, len(cand))
	for k := range cand {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// QueryRangeByLabelFilter returns every series that matches the metric and label
// subset (extra labels on the series are allowed) with points in [start,end]
// from SSTs and memtable, merged the same way as QueryRange. Empty want matches
// all series for the metric.
func (e *Engine) QueryRangeByLabelFilter(metric string, want map[string]string, start, end int64) []FilterSeries {
	if start > end || metric == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := e.collectLabelFilterKeysLocked(metric, want, true)
	out := make([]FilterSeries, 0, len(keys))
	for _, sk := range keys {
		pts := e.queryRangeLocked(sk, start, end)
		if len(pts) == 0 {
			continue
		}
		out = append(out, FilterSeries{Series: sk, Samples: pts})
	}
	return out
}
