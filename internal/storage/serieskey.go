package storage

import (
	"sort"
	"strings"
)

// SeriesKey returns a stable, comparable string for (metric, labels).
// Labels are ordered lexicographically by name so the same time series always
// maps to the same key.
func SeriesKey(metric string, labels map[string]string) string {
	if len(labels) == 0 {
		return metric
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.Grow(len(metric) + 32*len(keys))
	b.WriteString(metric)
	for _, k := range keys {
		b.WriteByte(0) // field sep (metric names/label keys don't contain NUL in practice)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// RowKey is the memtable key: canonical label set + time (NULs may appear in Series).
type RowKey struct {
	Series string
	TS     int64
}

// RowKeyOf builds the memtable key for a sample.
func RowKeyOf(s Sample) RowKey {
	return RowKey{Series: SeriesKey(s.Metric, s.Labels), TS: s.Timestamp}
}

// PostingNameMetric is the v2 inverted-index key for the metric (Prometheus __name__).
func PostingNameMetric(metric string) string {
	if metric == "" {
		return ""
	}
	return "__name__=" + metric
}

// ParseSeriesKeyString decodes a canonical series key from SeriesKey. If there are
// no labels, the result is the bare metric and labels is nil.
func ParseSeriesKeyString(s string) (metric string, labels map[string]string) {
	i := strings.IndexByte(s, 0)
	if i < 0 {
		if s == "" {
			return "", nil
		}
		return s, nil
	}
	metric = s[:i]
	labels = make(map[string]string)
	p := i + 1
	for p < len(s) {
		rest := s[p:]
		eq := strings.IndexByte(rest, '=')
		if eq < 0 {
			return metric, labels
		}
		eq += p
		key := s[p:eq]
		valStart := eq + 1
		rel := strings.IndexByte(s[valStart:], 0)
		if rel < 0 {
			labels[key] = s[valStart:]
			break
		}
		abs := valStart + rel
		labels[key] = s[valStart:abs]
		p = abs + 1
	}
	return metric, labels
}

// SeriesKeyMatchesFilter reports whether key encodes metric and all want labels
// (a subset: extra labels on the series are allowed).
func SeriesKeyMatchesFilter(key, metric string, want map[string]string) bool {
	m, lab := ParseSeriesKeyString(key)
	if m != metric {
		return false
	}
	if len(want) == 0 {
		return true
	}
	if lab == nil {
		return false
	}
	for k, v := range want {
		if lab[k] != v {
			return false
		}
	}
	return true
}

// MatchSampleLabelFilter returns true if s is under metric and every label in want
// matches; extra labels on s are allowed. Empty want only checks the metric name.
func MatchSampleLabelFilter(s Sample, metric string, want map[string]string) bool {
	if s.Metric != metric {
		return false
	}
	if len(want) == 0 {
		return true
	}
	if s.Labels == nil {
		return false
	}
	for k, v := range want {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}
