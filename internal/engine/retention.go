package engine

import (
	"os"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/sstable"
)

// EnforceRetention removes on-disk SSTs whose newest sample (file MaxTime) is
// older than cfg.RetentionPeriod relative to the wall clock. It does not trim
// the memtable or rewrite the WAL: flush data you want in SSTs before GC, and
// note that in-memory (and not-yet-flushed) points may still be older than the
// retention window. When RetentionPeriod is 0, this is a no-op.
func (e *Engine) EnforceRetention() (removed int, err error) {
	if e == nil {
		return 0, nil
	}
	if e.cfg.RetentionPeriod <= 0 {
		return 0, nil
	}
	now := time.Now().UnixMilli()
	cutoff := now - e.cfg.RetentionPeriod.Milliseconds()

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.sst) == 0 {
		return 0, nil
	}
	var keep []*sstable.Table
	for _, t := range e.sst {
		if t == nil {
			continue
		}
		if t.MaxTime() >= cutoff {
			keep = append(keep, t)
			continue
		}
		_ = t.Close()
		if err := os.Remove(t.Path()); err != nil {
			return removed, err
		}
		removed++
	}
	e.sst = keep
	if removed > 0 {
		e.rebuildSeriesCardLocked()
	}
	return removed, nil
}
