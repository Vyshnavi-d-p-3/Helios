// Package api exposes a minimal HTTP surface: JSON or Prometheus remote_write
// (protobuf, optionally Snappy) on POST /api/v1/write; remote_read (SAMPLES) on
// POST /api/v1/read; health, label names/values, series list, flush/compact, and
// ad-hoc query routes; GET /metrics; and GET /-/healthy, GET /-/ready (operator probes).
// Exposing the API to untrusted callers requires a reverse proxy, auth, and (if
// pprof is enabled) binding only to loopback. Prefer defaults: pprof off, bounded query windows.
package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vyshnavi-d-p-3/helios/internal/cluster"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

const maxWriteBodyBytes = 4 << 20

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	Eng     *engine.Engine
	Version string
	Promql  *engineAdapter
	Anomaly *AnomalyHandler
	// Cluster enables leader forwarding on POST /api/v1/write and exposes /cluster/* when non-nil.
	Cluster *cluster.Node
	// Set by registerMetrics; optional for tests that bypass NewServeMux.
	ingestSamplesCommitted prometheus.Counter
	ingestRejected         *prometheus.CounterVec
	readRequests           *prometheus.CounterVec
	apiRequests            *prometheus.CounterVec
}

func (h *Handler) recordReadRequest(kind string) {
	if h == nil || h.readRequests == nil {
		return
	}
	h.readRequests.WithLabelValues(kind).Inc()
}

func (h *Handler) recordAPIRequest(handler string) {
	if h == nil || h.apiRequests == nil {
		return
	}
	h.apiRequests.WithLabelValues(handler).Inc()
}

// NewServeMux returns a mux with write, flush, compact, health, label discovery, series, and query routes.
func NewServeMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	if h.Promql == nil && h.Eng != nil {
		h.Promql = newEngineAdapter(h.Eng)
	}
	h.registerMetrics(mux)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /-/healthy", h.probeHealthy)
	mux.HandleFunc("GET /-/ready", h.probeReady)
	mux.HandleFunc("GET /livez", h.livez)
	mux.HandleFunc("GET /api/v1/status", h.status)
	writeFn := http.HandlerFunc(h.write)
	if h.Cluster != nil {
		writeFn = h.Cluster.LeaderForwardingHandler(h.write)
	}
	mux.HandleFunc("POST /api/v1/write", writeFn)
	mux.HandleFunc("POST /api/v1/read", h.read)
	mux.HandleFunc("POST /api/v1/flush", h.flush)
	mux.HandleFunc("POST /api/v1/compact", h.compactL0)
	mux.HandleFunc("POST /api/v1/retention", h.retentionEnforce)
	h.registerQuery(mux)
	h.registerLabelRoutes(mux)
	h.registerSeriesRoute(mux)
	if h.Anomaly != nil {
		h.Anomaly.Register(mux)
	}
	if h.Cluster != nil {
		mux.HandleFunc("GET /cluster/leader", h.Cluster.LeaderInfoHandler())
		mux.HandleFunc("POST /cluster/join", h.Cluster.JoinHandler())
	}
	return mux
}

func (h *Handler) livez(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (h *Handler) flush(w http.ResponseWriter, r *http.Request) {
	_ = r
	if err := h.Eng.Flush(); err != nil {
		log.Printf("api: flush: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.recordAPIRequest("flush")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) compactL0(w http.ResponseWriter, r *http.Request) {
	_ = r
	if err := h.Eng.CompactL0(); err != nil {
		log.Printf("api: compact: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.recordAPIRequest("compact")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) retentionEnforce(w http.ResponseWriter, r *http.Request) {
	_ = r
	removed, err := h.Eng.EnforceRetention()
	if err != nil {
		log.Printf("api: retention: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"removed": removed,
	})
	h.recordAPIRequest("retention")
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "version": h.Version})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":                   "ok",
		"version":                  h.Version,
		"mem_points":               h.Eng.MemLen(),
		"sstables":                 h.Eng.SSTCount(),
		"next_wal_seq":             h.Eng.NextWALSeq(),
		"retention_period_ms":      h.Eng.RetentionPeriod().Milliseconds(),
		"retention_gc_interval_ms": h.Eng.RetentionGCTickInterval().Milliseconds(),
		"max_series_per_metric":    h.Eng.MaxSeriesPerMetric(),
		"max_query_window_ms":      h.Eng.MaxQueryWindow().Milliseconds(),
	})
}

// writeRequest is the JSON shape for POST /api/v1/write (Prometheus also uses protobuf; see remotewrite.go).
type writeRequest struct {
	Samples []storage.Sample `json:"samples"`
}

func (h *Handler) write(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body := http.MaxBytesReader(w, r.Body, maxWriteBodyBytes+1)
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, "request body", http.StatusBadRequest)
		return
	}
	if int64(len(data)) > maxWriteBodyBytes {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	samples, err := decodeWriteBody(data, r.Header.Get("Content-Type"), r.Header.Get("Content-Encoding"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.Eng.Write(samples); err != nil {
		if strings.Contains(err.Error(), "memtable") {
			if h.ingestRejected != nil {
				h.ingestRejected.WithLabelValues("memtable").Inc()
			}
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if strings.Contains(err.Error(), "max series per metric") {
			if h.ingestRejected != nil {
				h.ingestRejected.WithLabelValues("cardinality").Inc()
			}
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		log.Printf("api: write: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if h.ingestSamplesCommitted != nil {
		h.ingestSamplesCommitted.Add(float64(len(samples)))
	}
	w.WriteHeader(http.StatusNoContent)
}
