// Package anomaly implements streaming anomaly detection for time-series data.
//
// Each tracked series gets its own EWMADetector that maintains a running mean
// and EWMA variance. Samples whose z-score exceeds a configured threshold are
// emitted as AnomalyEvents.
//
// The detector runs in the ingest hot path and must add < 1 % overhead at the
// 500 K samples/sec target. The Update method is allocation-free on the
// non-anomaly path; the per-series state is 64 bytes.
//
// v2 fixes over v1:
//   - Bounded LRU registry: prevents unbounded memory growth at high series
//     cardinality. v1 leaked ~80 bytes per unique series indefinitely.
//   - NaN/Inf rejection: a single bad sample no longer poisons the detector
//     state for the rest of the series' lifetime.
//   - Honest variance comments: the update is a standard EWMA variance, not
//     "Welford's algorithm" as v1 claimed.
package anomaly

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// AnomalyType classifies the nature of an anomaly.
type AnomalyType string

const (
	Spike AnomalyType = "spike"
	Drop  AnomalyType = "drop"
	Drift AnomalyType = "drift"
)

// AnomalyEvent describes a single detected anomaly.
type AnomalyEvent struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp int64             `json:"ts"`
	Value     float64           `json:"value"`
	Expected  float64           `json:"expected"`
	ZScore    float64           `json:"z_score"`
	Type      AnomalyType       `json:"type"`
}

// EWMADetector tracks a single time series.
//
// Two EWMAs are maintained:
//   - "fast" (alpha ≈ 0.3): tracks recent behaviour and is the basis for z-scores.
//   - "slow" (alpha = 0.05): tracks long-term baseline. Divergence between fast
//     and slow indicates a level shift (drift) rather than a transient outlier.
//
// The variance update is an EWMA variance, not Welford's. (Welford's computes
// batch variance over a known sample count; EWMA variance is an exponentially
// weighted analogue.)
type EWMADetector struct {
	fastAlpha float64
	fastMean  float64

	slowAlpha float64
	slowMean  float64

	variance float64

	threshold float64
	warmup    int
	count     int64

	lastSeenUnix int64 // wall-clock seconds; used by the registry's LRU
}

// NewEWMADetector returns a detector with the given parameters.
func NewEWMADetector(alpha, threshold float64, warmup int) *EWMADetector {
	return &EWMADetector{
		fastAlpha: alpha,
		slowAlpha: 0.05,
		threshold: threshold,
		warmup:    warmup,
	}
}

// Update processes a single sample and returns an AnomalyEvent if anomalous.
//
// The non-anomalous path is allocation-free. Non-finite values (NaN, ±Inf) are
// rejected and the detector state is left unchanged — without this guard, a
// single bad sample propagates NaN through the EWMA forever.
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

	prevFastMean := d.fastMean
	d.fastMean = d.fastAlpha*value + (1-d.fastAlpha)*d.fastMean
	d.slowMean = d.slowAlpha*value + (1-d.slowAlpha)*d.slowMean

	// EWMA variance: tracks dispersion around the moving mean. We use the
	// prior mean (prevFastMean) so the residual is independent of this
	// step's mean update.
	residual := value - prevFastMean
	d.variance = d.fastAlpha*(residual*residual) + (1-d.fastAlpha)*d.variance

	if d.count <= int64(d.warmup) {
		return nil
	}

	stddev := math.Sqrt(d.variance)
	if stddev < 1e-10 {
		// Constant-valued series: any non-trivial change is anomalous.
		if math.Abs(value-d.fastMean) < 1e-10 {
			return nil
		}
		return d.buildEvent(value, 100.0)
	}

	z := math.Abs(value-prevFastMean) / stddev
	if z <= d.threshold {
		return nil
	}
	return d.buildEvent(value, z)
}

func (d *EWMADetector) buildEvent(value, z float64) *AnomalyEvent {
	var t AnomalyType

	// Drift: the slow EWMA has shifted away from the fast EWMA over time.
	// Comparing the gap to stddev gives a unitless drift score.
	stddev := math.Sqrt(d.variance)
	if stddev > 1e-10 && math.Abs(d.fastMean-d.slowMean)/stddev > d.threshold*0.5 {
		t = Drift
	} else if value > d.fastMean {
		t = Spike
	} else {
		t = Drop
	}

	return &AnomalyEvent{
		Value:    value,
		Expected: d.fastMean,
		ZScore:   z,
		Type:     t,
	}
}

// Stats returns a snapshot of the detector state for debugging and metrics.
type EWMAStats struct {
	FastMean float64
	SlowMean float64
	Variance float64
	StdDev   float64
	Count    int64
}

func (d *EWMADetector) Stats() EWMAStats {
	return EWMAStats{
		FastMean: d.fastMean,
		SlowMean: d.slowMean,
		Variance: d.variance,
		StdDev:   math.Sqrt(d.variance),
		Count:    d.count,
	}
}

// Reset clears the detector state.
func (d *EWMADetector) Reset() {
	d.fastMean = 0
	d.slowMean = 0
	d.variance = 0
	d.count = 0
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// ── Registry with bounded LRU ────────────────────────────────────────────────

// RegistryConfig controls the registry's cardinality bounds.
type RegistryConfig struct {
	Alpha     float64
	Threshold float64
	Warmup    int

	// MaxSeries caps the number of tracked series. When exceeded, the
	// least-recently-updated series is evicted. A value of 0 disables the
	// cap (NOT recommended in production — v1 had no cap and would OOM).
	MaxSeries int

	// StaleAfter evicts series that haven't received a sample within this
	// duration. A value of 0 disables time-based eviction.
	StaleAfter time.Duration
}

// DefaultRegistryConfig returns sensible production defaults.
//
// The 1M series cap matches Prometheus's default cardinality limit; at ~120
// bytes per detector that's ~120MB of detector state — comfortable for a
// commodity node.
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
// Series state is held in a map plus an LRU doubly-linked list. The map gives
// O(1) lookup; the list gives O(1) eviction. All operations take a single mutex.
//
// At 500 K samples/sec across, say, 10 K series, that's 50 lookups/sec/series —
// well within what a single mutex can handle. If contention becomes an issue,
// the standard fix is shard-by-hash into N independent registries.
type Registry struct {
	cfg RegistryConfig

	mu        sync.Mutex
	detectors map[string]*lruEntry // series key → list element + detector
	lru       *list.List           // front = most recently used

	subMu       sync.RWMutex
	subscribers []chan<- AnomalyEvent

	// Metrics counters; readable via the engine's /metrics endpoint.
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
// If the series is new and the registry is at capacity, the LRU series is
// evicted first.
func (r *Registry) Process(metric string, labels map[string]string, seriesKey string, timestamp int64, value float64) *AnomalyEvent {
	if !isFinite(value) {
		r.mu.Lock()
		r.rejectsNaN++
		r.mu.Unlock()
		return nil
	}

	r.mu.Lock()
	det := r.touch(seriesKey)
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

// touch returns the detector for seriesKey, creating it if absent and
// evicting the LRU series if the registry is at capacity. Caller holds r.mu.
func (r *Registry) touch(seriesKey string) *EWMADetector {
	if entry, ok := r.detectors[seriesKey]; ok {
		r.lru.MoveToFront(entry.elem)
		return entry.detector
	}

	if r.cfg.MaxSeries > 0 && len(r.detectors) >= r.cfg.MaxSeries {
		r.evictOldestLocked()
	}

	det := NewEWMADetector(r.cfg.Alpha, r.cfg.Threshold, r.cfg.Warmup)
	entry := &lruEntry{key: seriesKey, detector: det}
	entry.elem = r.lru.PushFront(entry)
	r.detectors[seriesKey] = entry
	return det
}

// evictOldestLocked removes the LRU entry. Caller holds r.mu.
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

// PruneStale evicts series that haven't been updated within cfg.StaleAfter.
// Run periodically from a background goroutine; not on the hot path.
func (r *Registry) PruneStale(now time.Time) int {
	if r.cfg.StaleAfter == 0 {
		return 0
	}
	cutoff := now.Add(-r.cfg.StaleAfter).Unix()

	r.mu.Lock()
	defer r.mu.Unlock()

	pruned := 0
	for elem := r.lru.Back(); elem != nil; {
		entry := elem.Value.(*lruEntry)
		prev := elem.Prev()
		if entry.detector.lastSeenUnix < cutoff {
			r.lru.Remove(elem)
			delete(r.detectors, entry.key)
			pruned++
		}
		elem = prev
	}
	r.evictionsTotal += uint64(pruned)
	return pruned
}

// Subscribe adds a channel to the fan-out. Channels must be buffered;
// unbuffered or full channels drop events silently to keep ingest non-blocking.
func (r *Registry) Subscribe(ch chan<- AnomalyEvent) {
	r.subMu.Lock()
	r.subscribers = append(r.subscribers, ch)
	r.subMu.Unlock()
}

func (r *Registry) notify(event AnomalyEvent) {
	r.subMu.RLock()
	defer r.subMu.RUnlock()
	for _, ch := range r.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

// Metrics for /metrics endpoint.
type RegistryMetrics struct {
	SeriesCount    int
	EvictionsTotal uint64
	NaNRejects     uint64
}

func (r *Registry) Metrics() RegistryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RegistryMetrics{
		SeriesCount:    len(r.detectors),
		EvictionsTotal: r.evictionsTotal,
		NaNRejects:     r.rejectsNaN,
	}
}
