// Package anomaly implements streaming EWMA-based anomaly detection.
//
// Each tracked series gets its own EWMADetector. Each new sample updates the
// detector's running mean and EWMA variance; samples whose z-score exceeds a
// threshold are emitted as AnomalyEvents to subscribers.
//
// The registry is bounded by an LRU + stale-eviction policy. Without these
// bounds the detector state grows linearly in unique series count and OOMs
// the process at TSDB cardinality.
//
// The detector runs in the ingest hot path. The non-anomalous Update path is
// allocation-free; per-series state is 64 bytes plus the LRU list element.
package anomaly

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// AnomalyType classifies a detected anomaly.
type AnomalyType string

const (
	Spike AnomalyType = "spike"
	Drop  AnomalyType = "drop"
	Drift AnomalyType = "drift"
)

// AnomalyEvent is emitted when a sample crosses the detection threshold.
type AnomalyEvent struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp int64             `json:"ts"`
	Value     float64           `json:"value"`
	Expected  float64           `json:"expected"`
	ZScore    float64           `json:"z_score"`
	Type      AnomalyType       `json:"type"`
}

// EWMADetector tracks a single series. Two EWMAs are maintained:
//   - "fast" (alpha ≈ 0.3): tracks recent behaviour, basis for z-scores.
//   - "slow" (alpha = 0.05): tracks long-term baseline; divergence between fast
//     and slow indicates a level shift (drift) rather than a transient outlier.
//
// The variance update is a standard EWMA variance, not Welford's algorithm —
// Welford's computes batch variance over a known sample count. EWMA variance
// is the exponentially weighted analogue.
type EWMADetector struct {
	fastAlpha float64
	fastMean  float64
	slowAlpha float64
	slowMean  float64
	variance  float64

	threshold float64
	warmup    int
	count     int64

	lastSeenUnix int64 // wall-clock seconds; used by the registry's LRU
}

// NewEWMADetector returns a detector with the given parameters.
//
// Suggested defaults:
//
//	alpha=0.3, threshold=3.0, warmup=10
func NewEWMADetector(alpha, threshold float64, warmup int) *EWMADetector {
	return &EWMADetector{
		fastAlpha: alpha,
		slowAlpha: 0.05,
		threshold: threshold,
		warmup:    warmup,
	}
}

// Update processes a single sample. Returns an AnomalyEvent if anomalous.
//
// Non-finite values (NaN, ±Inf) are rejected and detector state is left
// unchanged — without this guard, a single bad sample propagates NaN through
// the EWMA forever.
func (d *EWMADetector) Update(value float64) *AnomalyEvent {
	if !isFinite(value) {
		return nil
	}
	d.count++

	if d.count == 1 {
		d.fastMean = value
		d.slowMean = value
		d.variance = 0
		return nil
	}

	prevFast := d.fastMean
	prevSlow := d.slowMean
	prevVariance := d.variance

	var event *AnomalyEvent
	if d.count > int64(d.warmup) {
		stddev := math.Sqrt(prevVariance)
		if stddev < 1e-10 {
			// Constant-valued series: any non-trivial change is anomalous, but
			// don't emit infinitely large z-scores.
			if math.Abs(value-prevFast) >= 1e-10 {
				event = d.buildEventWithExpected(value, prevFast, prevSlow, 100.0)
			}
		} else {
			z := math.Abs(value-prevFast) / stddev
			if z > d.threshold {
				event = d.buildEventWithExpected(value, prevFast, prevSlow, z)
			}
		}
	}

	d.fastMean = d.fastAlpha*value + (1-d.fastAlpha)*d.fastMean
	d.slowMean = d.slowAlpha*value + (1-d.slowAlpha)*d.slowMean
	residual := value - prevFast
	d.variance = d.fastAlpha*(residual*residual) + (1-d.fastAlpha)*d.variance

	return event
}

func (d *EWMADetector) buildEventWithExpected(value, fastMean, slowMean, z float64) *AnomalyEvent {
	var t AnomalyType
	stddev := math.Sqrt(d.variance)
	if stddev > 1e-10 && math.Abs(fastMean-slowMean)/stddev > d.threshold*0.5 {
		t = Drift
	} else if value > fastMean {
		t = Spike
	} else {
		t = Drop
	}
	return &AnomalyEvent{
		Value:    value,
		Expected: fastMean,
		ZScore:   z,
		Type:     t,
	}
}

// Stats is a snapshot of detector state for /metrics.
type EWMAStats struct {
	FastMean float64
	SlowMean float64
	Variance float64
	StdDev   float64
	Count    int64
}

// Stats returns the current detector state.
func (d *EWMADetector) Stats() EWMAStats {
	return EWMAStats{
		FastMean: d.fastMean,
		SlowMean: d.slowMean,
		Variance: d.variance,
		StdDev:   math.Sqrt(d.variance),
		Count:    d.count,
	}
}

// Reset clears state.
func (d *EWMADetector) Reset() {
	d.fastMean, d.slowMean, d.variance, d.count = 0, 0, 0, 0
}

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// ─── Registry with bounded LRU ──────────────────────────────────────────────

// RegistryConfig controls cardinality bounds on the detector registry.
type RegistryConfig struct {
	Alpha     float64
	Threshold float64
	Warmup    int

	// MaxSeries caps tracked series. When exceeded, the LRU series is evicted.
	// 0 disables (NOT recommended in production).
	MaxSeries int

	// StaleAfter evicts series with no sample for this duration. 0 disables.
	StaleAfter time.Duration
}

// DefaultRegistryConfig returns sensible production defaults.
func DefaultRegistryConfig() RegistryConfig {
	return RegistryConfig{
		Alpha:      0.3,
		Threshold:  3.0,
		Warmup:     10,
		MaxSeries:  1_000_000,
		StaleAfter: 6 * time.Hour,
	}
}

// Registry manages per-series detectors with bounded memory.
//
// Concurrency: a single mutex protects detector map + LRU list. At sub-100ns
// per Process call this is fine to ~1M ops/sec/core; if profiling shows
// contention the standard fix is shard-by-hash into N independent registries.
type Registry struct {
	cfg RegistryConfig

	mu        sync.Mutex
	detectors map[string]*lruEntry
	lru       *list.List

	subMu       sync.RWMutex
	subscribers []chan<- AnomalyEvent

	evictionsTotal uint64
	rejectsNaN     uint64
}

type lruEntry struct {
	key      string
	detector *EWMADetector
	elem     *list.Element
}

// NewRegistry constructs a registry. Pass DefaultRegistryConfig() and tweak.
func NewRegistry(cfg RegistryConfig) *Registry {
	if cfg.MaxSeries < 0 {
		cfg.MaxSeries = 0
	}
	return &Registry{
		cfg:       cfg,
		detectors: make(map[string]*lruEntry, 1024),
		lru:       list.New(),
	}
}

// Process feeds a sample through the appropriate detector.
//
// seriesKey identifies the series (the canonical metric+labels key). The
// engine already has a SeriesKey helper that produces a suitable string.
func (r *Registry) Process(metric string, labels map[string]string, seriesKey string, timestamp int64, value float64) *AnomalyEvent {
	if !isFinite(value) {
		r.mu.Lock()
		r.rejectsNaN++
		r.mu.Unlock()
		return nil
	}

	r.mu.Lock()
	det := r.touchLocked(seriesKey)
	event := det.Update(value)
	det.lastSeenUnix = timestamp / 1000
	r.mu.Unlock()

	if event == nil {
		return nil
	}
	event.Metric = metric
	event.Labels = labels
	event.Timestamp = timestamp
	r.notify(*event)
	return event
}

// touchLocked returns (or creates) the detector for the key, evicting the LRU
// when full. Caller holds r.mu.
func (r *Registry) touchLocked(key string) *EWMADetector {
	if entry, ok := r.detectors[key]; ok {
		r.lru.MoveToFront(entry.elem)
		return entry.detector
	}
	if r.cfg.MaxSeries > 0 && len(r.detectors) >= r.cfg.MaxSeries {
		r.evictOldestLocked()
	}
	det := NewEWMADetector(r.cfg.Alpha, r.cfg.Threshold, r.cfg.Warmup)
	entry := &lruEntry{key: key, detector: det}
	entry.elem = r.lru.PushFront(entry)
	r.detectors[key] = entry
	return det
}

func (r *Registry) evictOldestLocked() {
	back := r.lru.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*lruEntry)
	r.lru.Remove(back)
	delete(r.detectors, entry.key)
	r.evictionsTotal++
}

// PruneStale evicts series unseen since now-StaleAfter. Run periodically from
// a background goroutine; not on the hot path.
func (r *Registry) PruneStale(now time.Time) int {
	if r.cfg.StaleAfter == 0 {
		return 0
	}
	cutoff := now.Add(-r.cfg.StaleAfter).Unix()

	r.mu.Lock()
	defer r.mu.Unlock()

	pruned := 0
	for e := r.lru.Back(); e != nil; {
		entry := e.Value.(*lruEntry)
		prev := e.Prev()
		if entry.detector.lastSeenUnix < cutoff {
			r.lru.Remove(e)
			delete(r.detectors, entry.key)
			pruned++
		}
		e = prev
	}
	r.evictionsTotal += uint64(pruned)
	return pruned
}

// Subscribe registers a buffered channel for fan-out. Channels MUST be buffered;
// full or unbuffered channels drop events to keep the ingest path non-blocking.
func (r *Registry) Subscribe(ch chan<- AnomalyEvent) {
	r.subMu.Lock()
	r.subscribers = append(r.subscribers, ch)
	r.subMu.Unlock()
}

// Unsubscribe removes a channel from the fan-out.
func (r *Registry) Unsubscribe(ch chan<- AnomalyEvent) {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	for i, c := range r.subscribers {
		if c == ch {
			r.subscribers = append(r.subscribers[:i], r.subscribers[i+1:]...)
			return
		}
	}
}

func (r *Registry) notify(event AnomalyEvent) {
	r.subMu.RLock()
	defer r.subMu.RUnlock()
	for _, ch := range r.subscribers {
		select {
		case ch <- event:
		default:
			// Drop on full subscriber.
		}
	}
}

// Metrics is a snapshot for /metrics.
type Metrics struct {
	SeriesCount    int
	EvictionsTotal uint64
	NaNRejects     uint64
}

// Snapshot returns the current registry-level counters.
func (r *Registry) Snapshot() Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Metrics{
		SeriesCount:    len(r.detectors),
		EvictionsTotal: r.evictionsTotal,
		NaNRejects:     r.rejectsNaN,
	}
}
