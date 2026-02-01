// Anomaly stream HTTP handler.
//
// GET /api/v1/anomalies streams Server-Sent Events (SSE). Each anomaly is one
// `data: <json>\n\n` frame. SSE was chosen over WebSockets because:
//   - Browsers and curl both consume it natively (no library required).
//   - Reverse proxies (nginx, Cloudflare) handle SSE well with default config.
//   - Read-only event push is exactly what SSE was designed for.
//
// Optional query parameters:
//   - metric: glob pattern matched against the metric name
//   - min_z:  minimum z-score (string-encoded float); default 0
//   - types:  comma-separated subset of {spike,drop,drift}
//
// Add this file to internal/api/ alongside the existing handlers.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/anomaly"
)

// AnomalyHandler holds a reference to the registry. It is registered by
// NewServeMux when an anomaly registry is provided.
type AnomalyHandler struct {
	Reg *anomaly.Registry
}

// RegisterAnomalyRoutes adds the anomaly-related HTTP routes.
//
// Call from NewServeMux:
//
//	if h.Anomaly != nil {
//	    h.Anomaly.Register(mux)
//	}
func (h *AnomalyHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/anomalies", h.stream)
}

func (h *AnomalyHandler) stream(w http.ResponseWriter, r *http.Request) {
	if h.Reg == nil {
		http.Error(w, "anomaly detection disabled", http.StatusServiceUnavailable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	filter, err := parseAnomalyFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Buffered channel — drops events for slow clients rather than blocking
	// the registry's notify path.
	ch := make(chan anomaly.AnomalyEvent, 64)
	h.Reg.Subscribe(ch)
	defer h.Reg.Unsubscribe(ch)

	ctx := r.Context()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	// Initial comment so curl prints something immediately.
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			// Send a comment frame to keep proxies from killing the connection.
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev := <-ch:
			if !filter.match(ev) {
				continue
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// anomalyFilter is the parsed form of the request filter.
type anomalyFilter struct {
	metricPattern string
	minZ          float64
	types         map[string]struct{}
}

func (f *anomalyFilter) match(ev anomaly.AnomalyEvent) bool {
	if f.metricPattern != "" {
		ok, err := path.Match(f.metricPattern, ev.Metric)
		if err != nil || !ok {
			return false
		}
	}
	if ev.ZScore < f.minZ {
		return false
	}
	if len(f.types) > 0 {
		if _, ok := f.types[string(ev.Type)]; !ok {
			return false
		}
	}
	return true
}

func parseAnomalyFilter(r *http.Request) (*anomalyFilter, error) {
	f := &anomalyFilter{}
	if v := r.URL.Query().Get("metric"); v != "" {
		f.metricPattern = v
	}
	if v := r.URL.Query().Get("min_z"); v != "" {
		z, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("min_z: %w", err)
		}
		f.minZ = z
	}
	if v := r.URL.Query().Get("types"); v != "" {
		f.types = map[string]struct{}{}
		for _, t := range strings.Split(v, ",") {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if t != "spike" && t != "drop" && t != "drift" {
				return nil, fmt.Errorf("types: %q is not one of spike,drop,drift", t)
			}
			f.types[t] = struct{}{}
		}
	}
	return f, nil
}

// _ = context.Background suppresses an unused-import warning in some builds.
var _ = context.Background
