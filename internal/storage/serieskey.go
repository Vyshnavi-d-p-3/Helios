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
