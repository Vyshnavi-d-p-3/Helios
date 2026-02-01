// Package engine ties the WAL and memtable for the single-node write path.
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
	"github.com/vyshnavi-d-p-3/helios/internal/wal"
)

// Engine coordinates durable append (WAL) and in-memory reads (memtable).
type Engine struct {
	cfg config.Config
	mu  sync.Mutex
	wal *wal.WAL
	mem *memtable.Memtable
}

// Open loads configuration paths, replays the WAL into a memtable, then opens
// the WAL for new appends. On a missing WAL the memtable starts empty; Open
// then creates the file.
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
	return &Engine{cfg: cfg, wal: w, mem: mem}, nil
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
	if _, err := e.wal.Append(samples); err != nil {
		return err
	}
	for i := range samples {
		e.mem.Put(samples[i])
	}
	return nil
}

// QueryRange reads from the memtable for a canonical series string.
func (e *Engine) QueryRange(series string, start, end int64) []storage.Sample {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mem.QueryRange(series, start, end)
}

// MemLen returns the number of points in the memtable (for metrics/debug).
func (e *Engine) MemLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mem.Len()
}

// NextWALSeq returns the next sequence number the WAL will assign.
func (e *Engine) NextWALSeq() uint64 {
	return e.wal.NextSeq()
}

// Close flushes the WAL and releases resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}
