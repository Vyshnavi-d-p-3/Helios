package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registerMetrics adds GET /metrics in Prometheus text exposition format.
func (h *Handler) registerMetrics(mux *http.ServeMux) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	h.ingestSamplesCommitted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "ingest",
			Name:      "samples_committed_total",
			Help:      "Samples accepted into the engine after WAL append (per batch, count=len(samples)).",
		},
	)
	h.ingestRejected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "ingest",
			Name:      "rejected_total",
			Help:      "Write batches rejected before commit (e.g. memtable or cardinality).",
		},
		[]string{"reason"},
	)
	// Pre-declare label values so they appear in /metrics with 0.
	h.ingestRejected.WithLabelValues("memtable")
	h.ingestRejected.WithLabelValues("cardinality")
	h.readRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "read",
			Name:      "requests_total",
			Help:      "Successful read API requests (query instant, query range, remote read).",
		},
		[]string{"kind"},
	)
	for _, k := range []string{"query_instant", "query_range", "remote_read"} {
		h.readRequests.WithLabelValues(k)
	}
	h.apiRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "helios",
			Subsystem: "api",
			Name:      "requests_total",
			Help:      "Successful non-query API requests (metadata, admin).",
		},
		[]string{"handler"},
	)
	for _, k := range []string{"labels", "label_values", "series", "flush", "compact", "retention"} {
		h.apiRequests.WithLabelValues(k)
	}
	reg.MustRegister(h.ingestSamplesCommitted, h.ingestRejected, h.readRequests, h.apiRequests)
	reg.MustRegister(
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "helios",
				Subsystem: "engine",
				Name:      "memtable_points",
				Help:      "Count of float samples in the in-memory memtable.",
			},
			func() float64 { return float64(h.Eng.MemLen()) },
		),
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "helios",
				Subsystem: "engine",
				Name:      "sstables_open",
				Help:      "Number of on-disk L0 sstable files opened by the engine.",
			},
			func() float64 { return float64(h.Eng.SSTCount()) },
		),
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: "helios",
				Subsystem: "wal",
				Name:      "next_sequence",
				Help:      "Monotonic sequence number to be used for the next WAL segment name.",
			},
			func() float64 { return float64(h.Eng.NextWALSeq()) },
		),
	)
	v := h.Version
	if v == "" {
		v = "unknown"
	}
	bi := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   "helios",
			Subsystem:   "build",
			Name:        "info",
			Help:        "Labeled 1; value indicates this binary. Version is in the label.",
			ConstLabels: prometheus.Labels{"version": v},
		},
	)
	bi.Set(1)
	reg.MustRegister(bi)

	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
}
