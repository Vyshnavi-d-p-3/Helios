package engine

import (
	"fmt"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// rebuildSeriesCardLocked rebuilds the per-metric distinct series set from the
// memtable and all open SSTs. Must hold e.mu.
func (e *Engine) rebuildSeriesCardLocked() {
	e.seriesCard = make(map[string]map[string]struct{})
	if e.mem != nil {
		e.mem.ForEachUniqueSeriesKeyFromNameIndex(func(sk string) bool {
			metric, _ := storage.ParseSeriesKeyString(sk)
			if metric == "" {
				return true
			}
			e.addSeriesToCardLocked(metric, sk)
			return true
		})
	}
	for _, fm := range e.frozen {
		if fm == nil {
			continue
		}
		fm.ForEachUniqueSeriesKeyFromNameIndex(func(sk string) bool {
			metric, _ := storage.ParseSeriesKeyString(sk)
			if metric == "" {
				return true
			}
			e.addSeriesToCardLocked(metric, sk)
			return true
		})
	}
	for _, t := range e.sst {
		if t == nil {
			continue
		}
		for _, sk := range t.AllSeries() {
			metric, _ := storage.ParseSeriesKeyString(sk)
			if metric == "" {
				continue
			}
			e.addSeriesToCardLocked(metric, sk)
		}
	}
}

func (e *Engine) addSeriesToCardLocked(metric, sk string) {
	if e.seriesCard[metric] == nil {
		e.seriesCard[metric] = make(map[string]struct{})
	}
	e.seriesCard[metric][sk] = struct{}{}
}

// cardinalityCheckBatchLocked ensures the batch will not add more **new** distinct
// series for any metric than MaxSeriesPerMetric allows (including new keys
// introduced within the same batch). Must hold e.mu.
func (e *Engine) cardinalityCheckBatchLocked(samples []storage.Sample) error {
	if e == nil {
		return nil
	}
	n := e.cfg.MaxSeriesPerMetric
	if n <= 0 {
		return nil
	}
	pen := make(map[string]map[string]struct{}) // new series in this batch, per metric
	for i := range samples {
		metric := samples[i].Metric
		if metric == "" {
			continue
		}
		sk := storage.RowKeyOf(samples[i]).Series
		if e.seriesKeyCountedLocked(metric, sk) {
			continue
		}
		if pen[metric] == nil {
			pen[metric] = make(map[string]struct{})
		}
		if _, dup := pen[metric][sk]; dup {
			continue
		}
		pen[metric][sk] = struct{}{}
		base := 0
		if e.seriesCard != nil {
			if set, ok := e.seriesCard[metric]; ok {
				base = len(set)
			}
		}
		if base+len(pen[metric]) > n {
			return fmt.Errorf("engine: max series per metric (%d) exceeded for metric %q", n, metric)
		}
	}
	return nil
}

func (e *Engine) seriesKeyCountedLocked(metric, sk string) bool {
	if e.seriesCard == nil {
		return false
	}
	set, ok := e.seriesCard[metric]
	if !ok {
		return false
	}
	_, ok = set[sk]
	return ok
}

// cardinalityRecordLocked records the series key after a successful Put. Must hold e.mu.
func (e *Engine) cardinalityRecordLocked(s storage.Sample) {
	if e == nil {
		return
	}
	if e.cfg.MaxSeriesPerMetric <= 0 {
		return
	}
	if s.Metric == "" {
		return
	}
	sk := storage.RowKeyOf(s).Series
	if e.seriesCard == nil {
		e.seriesCard = make(map[string]map[string]struct{})
	}
	e.addSeriesToCardLocked(s.Metric, sk)
}
