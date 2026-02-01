package memtable

import (
	"sort"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// postingKey mirrors sstable: "name=value" and storage.PostingNameMetric for __name__.
func postingNameVal(name, value string) string { return name + "=" + value }

func (m *Memtable) indexPut(s storage.Sample) {
	sk := storage.RowKeyOf(s).Series
	if pm := storage.PostingNameMetric(s.Metric); pm != "" {
		m.postingAdd(pm, sk)
	}
	if s.Labels == nil {
		return
	}
	for k, v := range s.Labels {
		if k == "" {
			continue
		}
		m.postingAdd(postingNameVal(k, v), sk)
	}
}

func (m *Memtable) postingAdd(pkey, sk string) {
	sub := m.posting[pkey]
	if sub == nil {
		sub = make(map[string]struct{})
		m.posting[pkey] = sub
	}
	sub[sk] = struct{}{}
}

// SeriesKeysForLabelFilter returns canonical series keys in this memtable that match
// metric and a subset of label values (the same rules as sstable/ engine). Empty
// want matches all series for the metric. O(∩) set sizes, not O(points).
func (m *Memtable) SeriesKeysForLabelFilter(metric string, want map[string]string) []string {
	if m == nil || metric == "" {
		return nil
	}
	plist := m.posting[storage.PostingNameMetric(metric)]
	if len(plist) == 0 {
		return nil
	}
	sets := make([]map[string]struct{}, 0, 1+len(want))
	sets = append(sets, plist)
	for k, v := range want {
		if k == "" {
			continue
		}
		pl := m.posting[postingNameVal(k, v)]
		if len(pl) == 0 {
			return nil
		}
		sets = append(sets, pl)
	}
	return intersectStringSetMaps(sets)
}

func intersectStringSetMaps(sets []map[string]struct{}) []string {
	if len(sets) == 0 {
		return nil
	}
	bi := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[bi]) {
			bi = i
		}
	}
	if len(sets[bi]) == 0 {
		return nil
	}
	out := make([]string, 0, len(sets[bi]))
	for s := range sets[bi] {
		ok := true
		for j, mset := range sets {
			if j == bi {
				continue
			}
			if _, o := mset[s]; !o {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
