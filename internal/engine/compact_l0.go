package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vyshnavi-d-p-3/helios/internal/sstable"
)

// CompactL0 merges all on-disk sstables into a single v2 sstable, preserving the
// same (series, timestamp) tie-break as queries: files later in the sorted
// name list win on duplicates. A no-op if there is fewer than two tables.
// The memtable and WAL are unchanged.
func (e *Engine) CompactL0() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.sst) < 2 {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(e.cfg.DataDir, "sst"), 0o750); err != nil {
		return err
	}
	outPath := filepath.Join(e.cfg.DataDir, "sst", fmt.Sprintf("%08d.sst", e.sstID))
	if err := sstable.CompactTo(outPath, e.sst); err != nil {
		return err
	}
	newT, err := sstable.OpenTable(outPath)
	if err != nil {
		_ = os.Remove(outPath)
		return err
	}
	for _, t := range e.sst {
		if t != nil {
			_ = t.Close()
			_ = os.Remove(t.Path())
		}
	}
	e.sst = []*sstable.Table{newT}
	e.sstID++
	return nil
}
