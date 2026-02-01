package api

import (
	"io"
	"net/http"
)

// probeHealthy is a liveness probe (Prometheus-style GET /-/healthy).
func (h *Handler) probeHealthy(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

// probeReady is a readiness probe (GET /-/ready). The engine must be usable.
func (h *Handler) probeReady(w http.ResponseWriter, r *http.Request) {
	_ = r
	if h.Eng == nil {
		http.Error(w, "engine not initialized", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}
