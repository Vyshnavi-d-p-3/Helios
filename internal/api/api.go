// Package api exposes a minimal HTTP surface for local development. Prometheus
// remote_write and full PromQL are follow-ups.
package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

const maxWriteBodyBytes = 4 << 20

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	Eng     *engine.Engine
	Version string
}

// NewServeMux returns a mux with JSON ingest and health routes.
func NewServeMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /api/v1/status", h.status)
	mux.HandleFunc("POST /api/v1/write", h.writeJSON)
	return mux
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
		"status":       "ok",
		"version":      h.Version,
		"mem_points":   h.Eng.MemLen(),
		"next_wal_seq": h.Eng.NextWALSeq(),
	})
}

// writeRequest is a simple JSON body until remote_write (protobuf) is added.
type writeRequest struct {
	Samples []storage.Sample `json:"samples"`
}

func (h *Handler) writeJSON(w http.ResponseWriter, r *http.Request) {
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
	var req writeRequest
	if err := json.Unmarshal(data, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Samples) == 0 {
		http.Error(w, "empty samples", http.StatusBadRequest)
		return
	}
	if err := h.Eng.Write(req.Samples); err != nil {
		if strings.Contains(err.Error(), "memtable") {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
