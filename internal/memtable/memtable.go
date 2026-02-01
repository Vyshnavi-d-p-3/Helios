package memtable

import (
	"strings"

	"github.com/huandu/skiplist"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// memKeyCmp orders RowKey; CalcScore is 0 so ordering uses Compare only (not float score).
type memKeyCmp struct{}

func (memKeyCmp) Compare(lhs, rhs interface{}) int {
	a := lhs.(storage.RowKey)
	b := rhs.(storage.RowKey)
	if a.Series < b.Series {
		return -1
	}
	if a.Series > b.Series {
		return 1
	}
	if a.TS < b.TS {
		return -1
	}
	if a.TS > b.TS {
		return 1
	}
	return 0
}

func (memKeyCmp) CalcScore(interface{}) float64 { return 0 }

// Memtable is an in-memory sorted map of points keyed by
// (series, timestamp) using a skip list. Values are full samples.
// posting maps "label_name=value" (and __name__=metric) to sets of canonical
// series keys for fast label-filter queries; it is kept in sync on Put and cleared
// on Clear.
type Memtable struct {
	list      *skiplist.SkipList
	posting   map[string]map[string]struct{}
	approxB   int64
	maxBytes  int64
	pointSize int
}

var rowCmp = memKeyCmp{}

// New returns an empty memtable. maxBytes is reserved for future hard caps.
func New(maxBytes int64) *Memtable {
	return &Memtable{
		list:      skiplist.New(rowCmp),
		posting:   make(map[string]map[string]struct{}),
		maxBytes:  maxBytes,
		pointSize: 32,
	}
}

// Put inserts or overwrites a sample. Time complexity O(log n).
func (m *Memtable) Put(s storage.Sample) {
	k := storage.RowKeyOf(s)
	if m.list.Get(k) == nil {
		m.approxB += int64(len(k.Series) + 8 + m.pointSize)
	}
	m.list.Set(k, s)
	m.indexPut(s)
}

// Len is the number of points stored.
func (m *Memtable) Len() int { return m.list.Len() }

// ApproxBytes is a rough footprint estimate.
func (m *Memtable) ApproxBytes() int64 { return m.approxB }

// ForEachUniqueSeriesKeyFromNameIndex calls fn once per unique canonical
// series key that appears in the __name__= posting lists (O(unique series) in mem).
func (m *Memtable) ForEachUniqueSeriesKeyFromNameIndex(fn func(sk string) bool) {
	if m == nil {
		return
	}
	prefix := "__name__="
	for pkey, set := range m.posting {
		if !strings.HasPrefix(pkey, prefix) {
			continue
		}
		for sk := range set {
			if !fn(sk) {
				return
			}
		}
	}
}

// ForEach iterates in (series, time) order. Stops if fn returns false.
func (m *Memtable) ForEach(fn func(s storage.Sample) bool) {
	for e := m.list.Front(); e != nil; e = e.Next() {
		if !fn(e.Value.(storage.Sample)) {
			return
		}
	}
}

// Clear removes all points (e.g. after a successful flush).
func (m *Memtable) Clear() {
	m.list.Init()
	m.approxB = 0
	m.posting = make(map[string]map[string]struct{})
}

// QueryRange returns samples for one series string with timestamp in [start, end] inclusive.
// Results are ordered by time ascending.
func (m *Memtable) QueryRange(series string, start, end int64) []storage.Sample {
	if start > end {
		return nil
	}
	elem := m.list.Find(storage.RowKey{Series: series, TS: start})
	if elem == nil {
		return nil
	}
	var out []storage.Sample
	for e := elem; e != nil; e = e.Next() {
		k := e.Key().(storage.RowKey)
		if k.Series != series {
			break
		}
		s := e.Value.(storage.Sample)
		if s.Timestamp > end {
			break
		}
		if s.Timestamp < start {
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
