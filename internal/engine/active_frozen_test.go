package engine

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/memtable"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestEngine_FreezeWithoutImmediateError(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.MemtableMaxSize = 1 << 20
	cfg.MaxFrozenMemtables = 100
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	batch := make([]storage.Sample, 5000)
	for i := range batch {
		batch[i] = storage.Sample{Metric: "m", Labels: map[string]string{"k": "x"}, Timestamp: int64(i), Value: float64(i)}
	}
	for i := 0; i < 5; i++ {
		if err := eng.Write(batch); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}
}

func TestEngine_TooManyFrozenReturnsError(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.MemtableMaxSize = 1 << 20
	cfg.MaxFrozenMemtables = 1
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.mu.Lock()
	eng.frozen = make([]*memtable.Memtable, 0, cfg.MaxFrozenMemtables)
	for i := 0; i < cfg.MaxFrozenMemtables; i++ {
		eng.frozen = append(eng.frozen, memtable.New(cfg.MemtableMaxSize))
	}
	eng.mu.Unlock()

	batch := make([]storage.Sample, 5000)
	for i := range batch {
		batch[i] = storage.Sample{Metric: "m", Labels: map[string]string{"k": "y"}, Timestamp: int64(i), Value: float64(i)}
	}
	var got error
	for i := 0; i < 200; i++ {
		if err := eng.Write(batch); err != nil {
			got = err
			break
		}
	}
	if got == nil {
		t.Fatal("expected saturation error")
	}
	if !errors.Is(got, ErrTooManyFrozen) {
		t.Fatalf("want ErrTooManyFrozen, got %v", got)
	}
}

func TestEngine_ReadAfterFreezeBeforeFlush(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.MemtableMaxSize = 1 << 20
	cfg.MaxFrozenMemtables = 8
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	big := strings.Repeat("z", 32*1024)
	lbl := map[string]string{"k": big}
	if err := eng.Write([]storage.Sample{{Metric: "x", Labels: lbl, Timestamp: 1, Value: 10}}); err != nil {
		t.Fatal(err)
	}
	// Push enough to trigger at least one freeze.
	for i := 2; i < 30; i++ {
		_ = eng.Write([]storage.Sample{{Metric: "x", Labels: lbl, Timestamp: int64(i), Value: float64(i)}})
	}
	got := eng.QueryRange(storage.SeriesKey("x", lbl), 1, 5)
	if len(got) == 0 {
		t.Fatal("expected data visible after freeze")
	}
}

func TestEngine_FlushDrainsFrozen(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.MemtableMaxSize = 1 << 20
	cfg.MaxFrozenMemtables = 4
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	big := strings.Repeat("p", 32*1024)
	for i := 0; i < 40; i++ {
		if err := eng.Write([]storage.Sample{{Metric: "x", Labels: map[string]string{"k": big}, Timestamp: int64(i), Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if eng.MemLen() != 0 {
		t.Fatalf("active memtable not empty: %d", eng.MemLen())
	}
	if eng.SSTCount() == 0 {
		t.Fatal("expected flushed SSTables")
	}
}

func TestEngine_ConcurrentReadsWritesDuringFreeze(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.MemtableMaxSize = 1 << 20
	cfg.MaxFrozenMemtables = 8
	eng, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ts := int64(id * 10_000)
			big := strings.Repeat("c", 64*1024)
			for time.Since(start) < 300*time.Millisecond {
				_ = eng.Write([]storage.Sample{{Metric: "m", Labels: map[string]string{"k": big}, Timestamp: ts, Value: float64(ts)}})
				ts++
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < 300*time.Millisecond {
				_ = eng.QueryRange(storage.SeriesKey("m", map[string]string{"k": strings.Repeat("c", 64*1024)}), 0, time.Now().UnixMilli())
			}
		}()
	}
	wg.Wait()
}
