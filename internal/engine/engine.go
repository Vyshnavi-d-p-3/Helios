// Package engine ties the WAL and memtable for the single-node write path.
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/sstable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
	"github.com/vyshnavi-d-p-3/helios/internal/wal"
)

// Engine coordinates durable append (WAL), memtable, and optional SSTs on disk.
type Engine struct {
	cfg   config.Config
	mu    sync.Mutex
	wal   *wal.WAL
	mem   *memtable.Memtable
	sst   []*sstable.Table
	sstID int
	// seriesCard maps metric name -> set of canonical series keys; used when
	// MaxSeriesPerMetric > 0. Rebuilt on Open and after retention removes SSTs.
	seriesCard map[string]map[string]struct{}
}

// Open loads configuration paths, replays the WAL, opens the WAL for appends,
// and opens every *.sst under data/sst/ (sorted by name) for read.
func Open(cfg config.Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	walPath := filepath.Join(cfg.DataDir, "wal", "000001.log")
	mem := memtable.New(cfg.MemtableMaxSize)
	if err := wal.Scan(walPath, func(e wal.Entry) error {
		for i := range e.Samples {
			mem.Put(e.Samples[i])
		}
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}
	eng := &Engine{cfg: cfg, wal: w, mem: mem}
	if err := eng.loadSSTs(); err != nil {
		_ = w.Close()
		return nil, err
	}
	eng.mu.Lock()
	eng.rebuildSeriesCardLocked()
	eng.mu.Unlock()
	return eng, nil
}

func (e *Engine) loadSSTs() error {
	pat := filepath.Join(e.cfg.DataDir, "sst", "*.sst")
	matches, err := filepath.Glob(pat)
	if err != nil {
		return err
	}
	sort.Strings(matches)
	for _, p := range matches {
		t, err := sstable.OpenTable(p)
		if err != nil {
			return err
		}
		e.sst = append(e.sst, t)
	}
	e.sstID = len(matches) + 1
	return nil
}

// Write appends a batch to the WAL, then updates the memtable. WAL is first
// so a crash can replay into the memtable without losing durability.
func (e *Engine) Write(samples []storage.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cfg.MemtableMaxSize > 0 {
		est := int64(len(samples) * 256)
		if e.mem.ApproxBytes()+est > e.cfg.MemtableMaxSize {
			return fmt.Errorf("engine: memtable soft cap would be exceeded (approx + batch estimate)")
		}
	}
	if err := e.cardinalityCheckBatchLocked(samples); err != nil {
		return err
	}
	if _, err := e.wal.Append(samples); err != nil {
		return err
	}
	for i := range samples {
		e.mem.Put(samples[i])
		e.cardinalityRecordLocked(samples[i])
	}
	return nil
}

// QueryRange returns samples for a series key from SSTs and the memtable.
// Later layers (e.g. mem after SST) win on the same (series, timestamp).
func (e *Engine) QueryRange(series string, start, end int64) []storage.Sample {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryRangeLocked(series, start, end)
}

func (e *Engine) queryRangeLocked(series string, start, end int64) []storage.Sample {
	if start > end {
		return nil
	}
	last := make(map[int64]storage.Sample)
	for _, t := range e.sst {
		for _, s := range t.QueryRange(series, start, end) {
			if s.Timestamp >= start && s.Timestamp <= end {
				last[s.Timestamp] = s
			}
		}
	}
	for _, s := range e.mem.QueryRange(series, start, end) {
		if s.Timestamp >= start && s.Timestamp <= end {
			last[s.Timestamp] = s
		}
	}
	if len(last) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(last))
	for ts := range last {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]storage.Sample, 0, len(keys))
	for _, k := range keys {
		out = append(out, last[k])
	}
	return out
}

// Flush materializes the memtable to a new .sst file, truncates the WAL, and
// leaves the memtable empty. A no-op if the memtable is empty.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mem.Len() == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(e.cfg.DataDir, "sst"), 0o750); err != nil {
		return err
	}
	sstPath := filepath.Join(e.cfg.DataDir, "sst", fmt.Sprintf("%08d.sst", e.sstID))
	if err := sstable.WriteFromMemtable(sstPath, e.mem); err != nil {
		return err
	}
	t, err := sstable.OpenTable(sstPath)
	if err != nil {
		return err
	}
	e.sst = append(e.sst, t)
	e.sstID++
	e.mem.Clear()
	if err := e.wal.Truncate(); err != nil {
		return err
	}
	return nil
}

// SSTCount is the number of on-disk sstables in use.
func (e *Engine) SSTCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sst)
}

// MemLen returns the number of points in the memtable.
func (e *Engine) MemLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mem.Len()
}

// NextWALSeq returns the next sequence number the WAL will assign.
func (e *Engine) NextWALSeq() uint64 {
	return e.wal.NextSeq()
}

// RetentionPeriod returns the configured max on-disk age for SSTs (0 = no limit).
func (e *Engine) RetentionPeriod() time.Duration {
	if e == nil {
		return 0
	}
	return e.cfg.RetentionPeriod
}

// MaxSeriesPerMetric returns the per-metric series cardinality cap (0 = unlimited).
func (e *Engine) MaxSeriesPerMetric() int {
	if e == nil {
		return 0
	}
	return e.cfg.MaxSeriesPerMetric
}

// RetentionGCTickInterval is the configured background GC period (0 = disabled).
func (e *Engine) RetentionGCTickInterval() time.Duration {
	if e == nil {
		return 0
	}
	return e.cfg.RetentionGCTickInterval
}

// Close flushes the WAL and releases resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, t := range e.sst {
		if t != nil {
			_ = t.Close()
		}
	}
	e.sst = e.sst[:0]
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}
