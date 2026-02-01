// Command loadgen generates synthetic Prometheus remote_write traffic
// against Helios for benchmarking.
//
// Example:
//
//	go run ./scripts/loadgen \
//	    -url=http://localhost:9090/api/v1/write \
//	    -series=1000 -rate=100000 -duration=60s
//
// Output is written to stdout in a compact format suitable for awk/cut:
//
//	[t+05s] sent=500000 ok=499000 err=1000 mb_sent=12.4 p99_ms=14
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	var (
		url        = flag.String("url", "http://localhost:9090/api/v1/write", "remote_write URL")
		nSeries    = flag.Int("series", 100, "number of unique series")
		nLabels    = flag.Int("labels", 4, "labels per series (in addition to __name__)")
		ratePerSec = flag.Int("rate", 10000, "samples per second target")
		duration   = flag.Duration("duration", 30*time.Second, "test duration")
		batchSize  = flag.Int("batch", 500, "samples per request")
		workers    = flag.Int("workers", 4, "concurrent HTTP senders")
	)
	flag.Parse()

	// Pre-generate series identities so the hot loop only allocates samples.
	seriesSet := generateSeries(*nSeries, *nLabels)

	// Channel of pre-built (snappy + protobuf) request bodies. The hot loop
	// fills the channel; senders drain it.
	jobs := make(chan []byte, 256)

	var (
		stats     senderStats
		startTime = time.Now()
	)

	var senderWG sync.WaitGroup
	for i := 0; i < *workers; i++ {
		senderWG.Add(1)
		go runSender(&senderWG, *url, jobs, &stats)
	}

	// Status reporter
	stop := make(chan struct{})
	go reportProgress(stop, &stats, startTime)

	// Main producer loop. Targets `rate` samples/sec by emitting bursts of
	// `batchSize` and pacing accordingly.
	endAt := startTime.Add(*duration)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	curIdx := 0

	batchInterval := time.Duration(float64(time.Second) * float64(*batchSize) / float64(*ratePerSec))
	tick := time.NewTicker(batchInterval)
	defer tick.Stop()

	for now := startTime; now.Before(endAt); now = <-tick.C {
		batch := make([]prompb.TimeSeries, 0, len(seriesSet))
		for i := 0; i < *batchSize; i++ {
			idx := curIdx % len(seriesSet)
			s := seriesSet[idx]
			curIdx++
			value := nextValue(s.Metric, s.Counter, rng)
			s.Counter = value
			seriesSet[idx] = s

			batch = append(batch, prompb.TimeSeries{
				Labels:  s.Labels,
				Samples: []prompb.Sample{{Timestamp: now.UnixMilli(), Value: value}},
			})
		}

		body, err := encodeRemoteWrite(batch)
		if err != nil {
			log.Printf("encode: %v", err)
			continue
		}
		jobs <- body
	}

	close(jobs)
	senderWG.Wait()
	close(stop)

	final := stats.snapshot()
	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("\nFINAL: sent=%d ok=%d err=%d duration=%.1fs throughput=%.0f samples/s p50=%dms p99=%dms\n",
		final.sent, final.ok, final.err, elapsed,
		float64(final.sent)/elapsed,
		final.p50, final.p99)
}

// senderStats accumulates stats across worker goroutines.
type senderStats struct {
	sent atomic.Uint64
	ok   atomic.Uint64
	err  atomic.Uint64

	mu        sync.Mutex
	latencies []time.Duration // capped to last N
}

const maxLatencyHistory = 100_000

func (s *senderStats) record(lat time.Duration, isErr bool) {
	s.sent.Add(1)
	if isErr {
		s.err.Add(1)
	} else {
		s.ok.Add(1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencies) >= maxLatencyHistory {
		s.latencies = s.latencies[len(s.latencies)/2:]
	}
	s.latencies = append(s.latencies, lat)
}

type statsSnap struct {
	sent, ok, err uint64
	p50, p99      int64 // ms
}

func (s *senderStats) snapshot() statsSnap {
	out := statsSnap{
		sent: s.sent.Load(),
		ok:   s.ok.Load(),
		err:  s.err.Load(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencies) > 0 {
		sorted := make([]time.Duration, len(s.latencies))
		copy(sorted, s.latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		p50idx := len(sorted) * 50 / 100
		p99idx := len(sorted) * 99 / 100
		if p99idx >= len(sorted) {
			p99idx = len(sorted) - 1
		}
		out.p50 = sorted[p50idx].Milliseconds()
		out.p99 = sorted[p99idx].Milliseconds()
	}
	return out
}

func runSender(wg *sync.WaitGroup, url string, jobs <-chan []byte, stats *senderStats) {
	defer wg.Done()
	client := &http.Client{Timeout: 30 * time.Second}

	for body := range jobs {
		start := time.Now()
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			stats.record(time.Since(start), true)
			continue
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

		resp, err := client.Do(req)
		isErr := err != nil
		if resp != nil {
			isErr = isErr || resp.StatusCode >= 300
			_ = resp.Body.Close()
		}
		stats.record(time.Since(start), isErr)
	}
}

func reportProgress(stop <-chan struct{}, stats *senderStats, startTime time.Time) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			snap := stats.snapshot()
			elapsed := time.Since(startTime).Seconds()
			fmt.Fprintf(os.Stderr, "[t+%02ds] sent=%d ok=%d err=%d throughput=%.0f/s p99=%dms\n",
				int(elapsed), snap.sent, snap.ok, snap.err,
				float64(snap.sent)/elapsed, snap.p99)
		}
	}
}

func encodeRemoteWrite(ts []prompb.TimeSeries) ([]byte, error) {
	req := &prompb.WriteRequest{Timeseries: ts}
	raw, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, raw), nil
}

type seriesIdentity struct {
	Metric  string
	Labels  []prompb.Label
	Counter float64 // most-recent value, for monotonic counter simulation
}

// generateSeries fabricates `n` series with realistic-shaped names and labels.
// We deliberately use a small number of metric names with many label combos so
// queries like `sum by (status) (m)` exercise the postings index.
func generateSeries(n, labelsPerSeries int) []seriesIdentity {
	const numMetrics = 10
	metrics := []string{
		"http_requests_total", "http_request_duration_seconds",
		"db_query_duration_seconds", "cpu_usage_ratio", "memory_used_bytes",
		"disk_io_seconds_total", "network_bytes_total", "queue_depth",
		"goroutines", "errors_total",
	}
	jobs := []string{"api", "worker", "frontend", "ingest", "query", "scheduler"}
	envs := []string{"prod", "staging", "dev"}
	hosts := []string{"web-1", "web-2", "web-3", "db-1", "db-2"}

	out := make([]seriesIdentity, 0, n)
	for i := 0; i < n; i++ {
		metric := metrics[i%numMetrics]
		labels := []prompb.Label{
			{Name: "__name__", Value: metric},
			{Name: "job", Value: jobs[i%len(jobs)]},
			{Name: "env", Value: envs[i%len(envs)]},
			{Name: "host", Value: hosts[i%len(hosts)]},
		}
		// Use the rest of the label budget on instance-style labels.
		for j := 4; j < labelsPerSeries+1; j++ {
			labels = append(labels, prompb.Label{
				Name:  fmt.Sprintf("tag%d", j),
				Value: fmt.Sprintf("v%d", i+j),
			})
		}
		out = append(out, seriesIdentity{
			Metric: metric,
			Labels: labels,
		})
	}
	return out
}

// nextValue produces realistic-shaped values for the given metric.
//   - *_total → monotonic counter incrementing 1-10 each step
//   - *_duration_seconds, *_ratio → gauge in [0, 1]
//   - *_bytes → gauge in [1e6, 1e9]
//   - default → small Brownian motion around 50
func nextValue(metric string, prev float64, rng *rand.Rand) float64 {
	switch {
	case len(metric) >= 6 && metric[len(metric)-6:] == "_total":
		return prev + float64(rng.Intn(10)+1)
	case stringEnds(metric, "_seconds") || stringEnds(metric, "_ratio"):
		return rng.Float64()
	case stringEnds(metric, "_bytes"):
		return 1e6 + rng.Float64()*9e8
	}
	return prev + (rng.Float64()-0.5)*5
}

func stringEnds(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
