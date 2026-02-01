// Package blockcache implements a bounded LRU cache for decoded Gorilla blocks
// from SSTables.
//
// Why have one: every series read decompresses its Gorilla block from disk.
// Hot series (queried thousands of times per minute by Grafana) get re-decoded
// every time. Caching the decoded slice eliminates Gorilla decode + IO for
// repeated reads, turning typical query p99 from milliseconds to microseconds.
//
// Why a custom implementation rather than a third-party LRU: the cache key is
// (sstable file id, series id within sstable) — two uint64 values. Stringifying
// them on every lookup is wasteful, and the cache is small enough that a
// hand-rolled implementation is easier to reason about for memory accounting
// (we measure size by sample count, not by serialized bytes).
package blockcache

import (
	"container/list"
	"sync"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// Key uniquely identifies a Gorilla block within the engine.
type Key struct {
	SSTableID uint64
	SeriesID  uint64
}

// Cache is a bounded LRU. Eviction is by total cached sample count, not by
// number of entries; this matches the resource we're actually trying to
// bound (memory).
type Cache struct {
	mu       sync.Mutex
	maxSize  int // maximum total samples
	currSize int

	entries map[Key]*list.Element // -> *cacheEntry
	lru     *list.List

	hits   uint64
	misses uint64
}

type cacheEntry struct {
	key     Key
	samples []storage.Sample
}

// New constructs a cache with the given total sample budget.
//
// A budget of 1,000,000 samples at ~20 bytes per sample (timestamp + value +
// pointer overhead) is roughly 25 MB.
func New(maxSamples int) *Cache {
	if maxSamples < 1 {
		maxSamples = 1
	}
	return &Cache{
		maxSize: maxSamples,
		entries: make(map[Key]*list.Element, 1024),
		lru:     list.New(),
	}
}

// Get returns cached samples and updates LRU order. The returned slice MUST
// NOT be mutated by callers — it is shared with the cache.
func (c *Cache) Get(k Key) ([]storage.Sample, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[k]; ok {
		c.lru.MoveToFront(elem)
		c.hits++
		return elem.Value.(*cacheEntry).samples, true
	}
	c.misses++
	return nil, false
}

// Put inserts samples into the cache, evicting LRU entries until size fits.
// The samples slice is stored by reference; do not mutate after Put.
func (c *Cache) Put(k Key, samples []storage.Sample) {
	if len(samples) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already present, replace.
	if elem, ok := c.entries[k]; ok {
		old := elem.Value.(*cacheEntry)
		c.currSize -= len(old.samples)
		old.samples = samples
		c.currSize += len(samples)
		c.lru.MoveToFront(elem)
	} else {
		entry := &cacheEntry{key: k, samples: samples}
		elem := c.lru.PushFront(entry)
		c.entries[k] = elem
		c.currSize += len(samples)
	}

	for c.currSize > c.maxSize {
		c.evictOldestLocked()
	}
}

// Invalidate removes a single entry, e.g. when its source SSTable is removed
// by compaction or retention.
func (c *Cache) Invalidate(k Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[k]; ok {
		entry := elem.Value.(*cacheEntry)
		c.lru.Remove(elem)
		delete(c.entries, k)
		c.currSize -= len(entry.samples)
	}
}

// InvalidateSSTable removes every entry for the given SSTable ID.
//
// Linear in cache size; fine because this is called at compaction/retention
// boundaries, not on the hot path.
func (c *Cache) InvalidateSSTable(sstableID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, elem := range c.entries {
		if k.SSTableID != sstableID {
			continue
		}
		entry := elem.Value.(*cacheEntry)
		c.lru.Remove(elem)
		delete(c.entries, k)
		c.currSize -= len(entry.samples)
	}
}

// Stats is a snapshot for /metrics.
type Stats struct {
	Entries int
	Samples int
	Hits    uint64
	Misses  uint64
}

// Snapshot reports cache occupancy and hit/miss counters.
func (c *Cache) Snapshot() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries: len(c.entries),
		Samples: c.currSize,
		Hits:    c.hits,
		Misses:  c.misses,
	}
}

func (c *Cache) evictOldestLocked() {
	back := c.lru.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*cacheEntry)
	c.lru.Remove(back)
	delete(c.entries, entry.key)
	c.currSize -= len(entry.samples)
}
