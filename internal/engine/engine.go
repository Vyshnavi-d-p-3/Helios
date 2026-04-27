// Package engine ties the WAL and memtable for the single-node write path.
package engine

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/anomaly"
	"github.com/vyshnavi-d-p-3/helios/internal/blockcache"
	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/sstable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
	"github.com/vyshnavi-d-p-3/helios/internal/wal"
)

var ErrTooManyFrozen = errors.New("engine: memtable flush queue saturated")

// Engine coordinates durable append (WAL), memtable, and optional SSTs on disk.
type Engine struct {
	cfg    config.Config
	mu     sync.Mutex
	wal    *wal.WAL
	mem    *memtable.Memtable // active memtable
	frozen []*memtable.Memtable
	sst    []*sstable.Table
	sstID  int
	// seriesCard maps metric name -> set of canonical series keys; used when
	// MaxSeriesPerMetric > 0. Rebuilt on Open and after retention removes SSTs.
	seriesCard   map[string]map[string]struct{}
	anomalyReg   *anomaly.Registry
	cache        *blockcache.Cache
	flushCh      chan struct{}
	stopCh       chan struct{}
	flushWG      sync.WaitGroup
	flushCond    *sync.Cond
	clusterApply func([]storage.Sample) error
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
	eng := &Engine{
		cfg:     cfg,
		wal:     w,
		mem:     mem,
		cache:   blockcache.New(cfg.BlockCacheMaxSamples),
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	eng.flushCond = sync.NewCond(&eng.mu)
	if err := eng.loadSSTs(); err != nil {
		_ = w.Close()
		return nil, err
	}
	eng.mu.Lock()
	eng.rebuildSeriesCardLocked()
	eng.mu.Unlock()
	eng.flushWG.Add(1)
	go eng.runBackgroundFlusher()
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
		t.SetBlockCache(e.cache)
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
			e.frozen = append(e.frozen, e.mem)
			e.mem = memtable.New(e.cfg.MemtableMaxSize)
			if len(e.frozen) > e.cfg.MaxFrozenMemtables {
				return ErrTooManyFrozen
			}
			select {
			case e.flushCh <- struct{}{}:
			default:
			}
		}
	}
	if err := e.cardinalityCheckBatchLocked(samples); err != nil {
		return err
	}
	if e.clusterApply != nil {
		return e.clusterApply(samples)
	}
	if _, err := e.wal.Append(samples); err != nil {
		return err
	}
	for i := range samples {
		e.mem.Put(samples[i])
		e.cardinalityRecordLocked(samples[i])
		if e.anomalyReg != nil && !math.IsNaN(samples[i].Value) && !math.IsInf(samples[i].Value, 0) {
			key := storage.SeriesKey(samples[i].Metric, samples[i].Labels)
			e.anomalyReg.Process(samples[i].Metric, samples[i].Labels, key, samples[i].Timestamp, samples[i].Value)
		}
	}
	return nil
}

// WriteDirect applies a batch to local durability/memtable without cluster forwarding.
// Used by Raft FSM apply path on all nodes.
func (e *Engine) WriteDirect(samples []storage.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.wal.Append(samples); err != nil {
		return err
	}
	for i := range samples {
		e.mem.Put(samples[i])
		e.cardinalityRecordLocked(samples[i])
		if e.anomalyReg != nil && !math.IsNaN(samples[i].Value) && !math.IsInf(samples[i].Value, 0) {
			key := storage.SeriesKey(samples[i].Metric, samples[i].Labels)
			e.anomalyReg.Process(samples[i].Metric, samples[i].Labels, key, samples[i].Timestamp, samples[i].Value)
		}
	}
	return nil
}

// SetClusterApply wires leader replication apply callback into the write path.
func (e *Engine) SetClusterApply(fn func([]storage.Sample) error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clusterApply = fn
}

// AttachAnomalyRegistry wires optional anomaly detection into the write path.
func (e *Engine) AttachAnomalyRegistry(r *anomaly.Registry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.anomalyReg = r
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
	for _, fm := range e.frozen {
		for _, s := range fm.QueryRange(series, start, end) {
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
	if e.mem.Len() > 0 {
		e.frozen = append(e.frozen, e.mem)
		e.mem = memtable.New(e.cfg.MemtableMaxSize)
	}
	select {
	case e.flushCh <- struct{}{}:
	default:
	}
	for len(e.frozen) > 0 {
		e.flushCond.Wait()
	}
	e.mu.Unlock()
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

// BlockCacheSnapshot returns cache stats and whether cache is enabled.
func (e *Engine) BlockCacheSnapshot() (blockcache.Stats, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cache == nil {
		return blockcache.Stats{}, false
	}
	return e.cache.Snapshot(), true
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

// MaxQueryWindow returns the cap on query/read time range (0 = unlimited).
func (e *Engine) MaxQueryWindow() time.Duration {
	if e == nil {
		return 0
	}
	return e.cfg.MaxQueryWindow
}

// CheckQueryTimeRange returns an error if [startMs,endMs] is wider than
// the configured max query window (no-op when the cap is 0 or end < start).
func (e *Engine) CheckQueryTimeRange(startMs, endMs int64) error {
	if e == nil {
		return nil
	}
	mw := e.cfg.MaxQueryWindow
	if mw <= 0 || endMs < startMs {
		return nil
	}
	span := endMs - startMs
	if span > mw.Milliseconds() {
		return fmt.Errorf("time range must be <= %s (got %s; -max-query-window)", mw, (time.Duration(span) * time.Millisecond).String())
	}
	return nil
}

// Close flushes the WAL and releases resources.
func (e *Engine) Close() error {
	close(e.stopCh)
	e.flushWG.Wait()
	e.mu.Lock()
	for _, t := range e.sst {
		if t != nil {
			_ = t.Close()
		}
	}
	e.sst = e.sst[:0]
	if e.wal == nil {
		e.mu.Unlock()
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	e.mu.Unlock()
	return err
}

func (e *Engine) runBackgroundFlusher() {
	defer e.flushWG.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case <-e.flushCh:
			for {
				e.mu.Lock()
				if len(e.frozen) == 0 {
					e.mu.Unlock()
					break
				}
				m := e.frozen[0]
				e.mu.Unlock()
				if err := e.flushMemtableToSST(m); err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				e.mu.Lock()
				if len(e.frozen) > 0 && e.frozen[0] == m {
					e.frozen = e.frozen[1:]
				}
				e.flushCond.Broadcast()
				e.mu.Unlock()
			}
		}
	}
}

func (e *Engine) flushMemtableToSST(m *memtable.Memtable) error {
	if m == nil || m.Len() == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(e.cfg.DataDir, "sst"), 0o750); err != nil {
		return err
	}
	e.mu.Lock()
	id := e.sstID
	e.mu.Unlock()
	sstPath := filepath.Join(e.cfg.DataDir, "sst", fmt.Sprintf("%08d.sst", id))
	if err := sstable.WriteFromMemtable(sstPath, m); err != nil {
		return err
	}
	t, err := sstable.OpenTable(sstPath)
	if err != nil {
		return err
	}
	t.SetBlockCache(e.cache)
	e.mu.Lock()
	e.sst = append(e.sst, t)
	if e.sstID == id {
		e.sstID++
	}
	e.mu.Unlock()
	return nil
}
