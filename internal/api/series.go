package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/selector"
)

// registerSeriesRoute adds GET /api/v1/series.
func (h *Handler) registerSeriesRoute(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/series", h.series)
}

// series: Prometheus-style, subset.
//
//   - start, end: unix **milliseconds** (same as this package's query_range).
//   - match[]: repeated, each a metric name or `metric{label="value",...}` (double-quoted
//     values, comma separated). Subset match per selector; results are unioned and deduplicated.
//   - or metric + optional l_* + partial: same as query_range; series with points in the window.
func (h *Handler) series(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start, err := parseInt64Param(r, "start")
	if err != nil {
		http.Error(w, "start (unix ms): "+err.Error(), http.StatusBadRequest)
		return
	}
	end, err := parseInt64Param(r, "end")
	if err != nil {
		http.Error(w, "end (unix ms): "+err.Error(), http.StatusBadRequest)
		return
	}
	if end < start {
		http.Error(w, "end before start", http.StatusBadRequest)
		return
	}

	matches := r.Form["match[]"]
	var data any
	if len(matches) > 0 {
		var specs []engine.LabelMatchSpec
		for _, raw := range matches {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			metric, lab, err := selector.ParseSeriesSelector(raw)
			if err != nil {
				http.Error(w, "match[]: "+err.Error(), http.StatusBadRequest)
				return
			}
			var want map[string]string
			if len(lab) > 0 {
				want = lab
			}
			specs = append(specs, engine.LabelMatchSpec{Metric: metric, Labels: want})
		}
		if len(specs) == 0 {
			http.Error(w, "at least one non-empty match[] is required", http.StatusBadRequest)
			return
		}
		data = h.Eng.SeriesWithLabelsUnionMatchSpecs(specs, start, end)
	} else {
		metric := strings.TrimSpace(r.Form.Get("metric"))
		if metric == "" {
			http.Error(w, "metric or at least one match[] is required", http.StatusBadRequest)
			return
		}
		labels := parseLabelParams(r)
		partial := parseBoolParam(r, "partial")
		data = h.Eng.SeriesWithLabelsInWindow(metric, labels, start, end, partial)
	}
	if data == nil {
		data = []map[string]string{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(queryResponse{
		Status: "success",
		Data:   data,
	})
	h.recordAPIRequest("series")
}
