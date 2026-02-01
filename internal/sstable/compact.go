// L0-style compaction: merge several SSTs into one v2 file with the same
// (series, timestamp) conflict rule as the engine: later input tables win.

package sstable

import (
	"errors"
	"math"
	"sort"

	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// CompactTo writes a single on-disk sstable to path that merges tables in
// order. For duplicate (series, timestamp) across tables, the sample from the
// table later in the slice wins, matching [Engine.queryRangeLocked]. Tables may
// be v1 or v2; output is always v2.
func CompactTo(path string, tables []*Table) error {
	if path == "" {
		return errors.New("sstable: empty path")
	}
	if len(tables) < 1 {
		return errors.New("sstable: at least one table is required")
	}
	serSet := make(map[string]struct{})
	for _, t := range tables {
		if t == nil {
			continue
		}
		for _, sk := range t.AllSeries() {
			serSet[sk] = struct{}{}
		}
	}
	keys := make([]string, 0, len(serSet))
	for sk := range serSet {
		keys = append(keys, sk)
	}
	sort.Strings(keys)
	m := memtable.New(0)
	for _, sk := range keys {
		for _, s := range mergeSeriesAcross(tables, sk) {
			m.Put(s)
		}
	}
	if m.Len() == 0 {
		return errors.New("sstable: compact produced an empty memtable")
	}
	return WriteFromMemtable(path, m)
}

func mergeSeriesAcross(tables []*Table, sk string) []storage.Sample {
	last := make(map[int64]storage.Sample)
	for _, t := range tables {
		if t == nil {
			continue
		}
		pts := t.QueryRange(sk, 0, math.MaxInt64)
		if len(pts) == 0 {
			continue
		}
		for i := range pts {
			s := pts[i]
			last[s.Timestamp] = s
		}
	}
	if len(last) == 0 {
		return nil
	}
	tss := make([]int64, 0, len(last))
	for ts := range last {
		tss = append(tss, ts)
	}
	sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
	out := make([]storage.Sample, 0, len(tss))
	for _, ts := range tss {
		out = append(out, last[ts])
	}
	return out
}
