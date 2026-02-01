package sstable

import "github.com/vyshnavi-d-p-3/helios/internal/storage"

// intersectSortedUint64 returns the set intersection of sorted, deduplicated uint64
// lists (conjunction). Empty input yields nil; any empty set yields nil.
func intersectSortedUint64(sets [][]uint64) []uint64 {
	if len(sets) == 0 {
		return nil
	}
	out := sets[0]
	for i := 1; i < len(sets); i++ {
		out = intersectTwoSorted(out, sets[i])
		if len(out) == 0 {
			return nil
		}
	}
	return out
}

func intersectTwoSorted(a, b []uint64) []uint64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	i, j := 0, 0
	out := make([]uint64, 0, minlen(len(a), len(b)))
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func minlen(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LocalSeriesForFilter returns the canonical series keys in this table whose
// metric and label subset match (extra labels on the series are allowed).
// v2 uses postings; v1 scans the in-memory series list.
func (t *Table) LocalSeriesForFilter(metric string, want map[string]string) []string {
	if t == nil || metric == "" {
		return nil
	}
	if t.postings == nil {
		return t.scanSeriesForFilter(metric, want)
	}
	sets := make([][]uint64, 0, 1+len(want))
	if pl := t.postings[storage.PostingNameMetric(metric)]; len(pl) > 0 {
		cp := make([]uint64, len(pl))
		copy(cp, pl)
		sets = append(sets, cp)
	} else {
		return nil
	}
	for k, v := range want {
		if k == "" {
			continue
		}
		pl := t.postings[postingK(k, v)]
		if len(pl) == 0 {
			return nil
		}
		cp := make([]uint64, len(pl))
		copy(cp, pl)
		sets = append(sets, cp)
	}
	ids := intersectSortedUint64(sets)
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if int(id) < len(t.series) {
			out = append(out, t.series[id])
		}
	}
	return out
}

func postingK(name, value string) string {
	return name + "=" + value
}

func (t *Table) scanSeriesForFilter(metric string, want map[string]string) []string {
	var out []string
	for _, sk := range t.series {
		if storage.SeriesKeyMatchesFilter(sk, metric, want) {
			out = append(out, sk)
		}
	}
	return out
}
