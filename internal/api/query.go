package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// queryResponse mirrors a tiny subset of the Prometheus /api/v1 response shape.
type queryResponse struct {
	Status string `json:"status"`
	Data   any    `json:"data"`
}

// matrixResult is one series worth of (timestamp, value) pairs in milliseconds.
type matrixResult struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Values [][2]float64      `json:"values"` // [ [ ts_ms, value ], ... ]
}

// registerQuery adds GET /api/v1/query (instant) and /api/v1/query_range.
func (h *Handler) registerQuery(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/query", h.queryInstant)
	mux.HandleFunc("GET /api/v1/query_range", h.queryRange)
}

// queryInstant: GET /api/v1/query?metric=&t=<unix_ms> with optional l_<name>=<value>
// Set partial=1 to return one sample per matching series (same semantics as query_range).
func (h *Handler) queryInstant(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric := strings.TrimSpace(r.Form.Get("metric"))
	if metric == "" {
		http.Error(w, "metric is required", http.StatusBadRequest)
		return
	}
	ts, err := parseInt64Param(r, "t")
	if err != nil {
		http.Error(w, fmt.Sprintf("t (unix ms) is required: %v", err), http.StatusBadRequest)
		return
	}
	labels := parseLabelParams(r)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if parseBoolParam(r, "partial") {
		rows := h.Eng.QueryRangeByLabelFilter(metric, labels, ts, ts)
		var vec []vectorElem
		for i := range rows {
			for _, p := range rows[i].Samples {
				vec = append(vec, vectorElem{Metric: p.Metric, Labels: p.Labels, Timestamp: p.Timestamp, Value: p.Value})
			}
		}
		_ = enc.Encode(queryResponse{
			Status: "success",
			Data: map[string]any{
				"resultType": "vector",
				"result":     vec,
			},
		})
		h.recordReadRequest("query_instant")
		return
	}
	sk := storage.SeriesKey(metric, labels)
	points := h.Eng.QueryRange(sk, ts, ts)
	_ = enc.Encode(queryResponse{
		Status: "success",
		Data: map[string]any{
			"resultType": "vector",
			"result":     toVectorResult(metric, labels, points),
		},
	})
	h.recordReadRequest("query_instant")
}

// queryRange: GET /api/v1/query_range?metric=&start=&end= with optional l_* labels.
// Set partial=1 to match any series whose metric and given labels match (extra
// labels on the series are allowed); the result can contain several series.
func (h *Handler) queryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric := strings.TrimSpace(r.Form.Get("metric"))
	if metric == "" {
		http.Error(w, "metric is required", http.StatusBadRequest)
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
	labels := parseLabelParams(r)
	partial := parseBoolParam(r, "partial")
	var mat []matrixResult
	if partial {
		rows := h.Eng.QueryRangeByLabelFilter(metric, labels, start, end)
		mat = make([]matrixResult, 0, len(rows))
		for i := range rows {
			if len(rows[i].Samples) == 0 {
				continue
			}
			s0 := rows[i].Samples[0]
			mat = append(mat, matrixResult{
				Metric: s0.Metric,
				Labels: s0.Labels,
				Values: toValuePairs(rows[i].Samples),
			})
		}
	} else {
		sk := storage.SeriesKey(metric, labels)
		points := h.Eng.QueryRange(sk, start, end)
		if len(points) > 0 {
			mat = []matrixResult{{
				Metric: metric,
				Labels: labels,
				Values: toValuePairs(points),
			}}
		} else {
			mat = []matrixResult{}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(queryResponse{
		Status: "success",
		Data: map[string]any{
			"resultType": "matrix",
			"result":     mat,
		},
	})
	h.recordReadRequest("query_range")
}

func parseBoolParam(r *http.Request, name string) bool {
	s := strings.TrimSpace(strings.ToLower(r.Form.Get(name)))
	return s == "1" || s == "true" || s == "yes"
}

func parseInt64Param(r *http.Request, name string) (int64, error) {
	s := strings.TrimSpace(r.Form.Get(name))
	if s == "" {
		return 0, fmt.Errorf("missing %q", name)
	}
	return strconv.ParseInt(s, 10, 64)
}

// parseLabelParams collects query parameters of the form l_<name>=<value> into a label map.
func parseLabelParams(r *http.Request) map[string]string {
	out := make(map[string]string)
	for k, vs := range r.Form {
		if !strings.HasPrefix(k, "l_") || len(vs) == 0 {
			continue
		}
		n := strings.TrimPrefix(k, "l_")
		if n == "" {
			continue
		}
		out[n] = vs[0]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func toValuePairs(samples []storage.Sample) [][2]float64 {
	if len(samples) == 0 {
		return [][2]float64{}
	}
	out := make([][2]float64, 0, len(samples))
	for i := range samples {
		out = append(out, [2]float64{float64(samples[i].Timestamp), samples[i].Value})
	}
	return out
}

// instant vector element (Prometheus-style single sample).
type vectorElem struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
}

func toVectorResult(metric string, labels map[string]string, points []storage.Sample) []vectorElem {
	if len(points) == 0 {
		return []vectorElem{}
	}
	p := points[0]
	return []vectorElem{{
		Metric:    metric,
		Labels:    labels,
		Timestamp: p.Timestamp,
		Value:     p.Value,
	}}
}
