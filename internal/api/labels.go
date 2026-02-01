package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// registerLabelRoutes adds Prometheus-style label discovery (subset).
func (h *Handler) registerLabelRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/labels", h.labels)
	mux.HandleFunc("GET /api/v1/label/{name}/values", h.labelValues)
}

// labels: GET /api/v1/labels
func (h *Handler) labels(w http.ResponseWriter, r *http.Request) {
	_ = r
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(queryResponse{
		Status: "success",
		Data:   h.Eng.AllLabelNames(),
	})
	h.recordAPIRequest("labels")
}

// labelValues: GET /api/v1/label/{name}/values
func (h *Handler) labelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if un, err := url.PathUnescape(name); err == nil {
		name = un
	}
	name = strings.TrimSpace(name)
	if name == "" {
		http.Error(w, "label name is required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(queryResponse{
		Status: "success",
		Data:   h.Eng.AllLabelValues(name),
	})
	h.recordAPIRequest("label_values")
}
