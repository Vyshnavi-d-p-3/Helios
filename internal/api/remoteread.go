package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func (h *Handler) read(w http.ResponseWriter, r *http.Request) {
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
	req, err := decodeReadRequest(data, r.Header.Get("Content-Type"), r.Header.Get("Content-Encoding"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !readAcceptsSamples(req) {
		http.Error(w, "server supports only SAMPLES read response (set accepted_response_types to include SAMPLES)", http.StatusNotAcceptable)
		return
	}
	resp, err := h.runReadRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pb, err := resp.Marshal()
	if err != nil {
		http.Error(w, "marshal response", http.StatusInternalServerError)
		return
	}
	enc := snappy.Encode(nil, pb)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	h.recordReadRequest("remote_read")
	_, _ = w.Write(enc)
}

func readAcceptsSamples(req *prompb.ReadRequest) bool {
	arts := req.GetAcceptedResponseTypes()
	if len(arts) == 0 {
		return true
	}
	for _, t := range arts {
		if t == prompb.ReadRequest_SAMPLES {
			return true
		}
	}
	return false
}

func decodeReadRequest(data []byte, contentType, contentEncoding string) (*prompb.ReadRequest, error) {
	if len(data) == 0 {
		return nil, errors.New("empty body")
	}
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "json") {
		return nil, errors.New("remote read expects application/x-protobuf (JSON not supported)")
	}
	payload := data
	ce := strings.ToLower(contentEncoding)
	if ce == "snappy" {
		p, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("snappy: %w", err)
		}
		payload = p
	} else {
		if p, err := snappy.Decode(nil, data); err == nil {
			payload = p
		}
	}
	var rr prompb.ReadRequest
	if err := rr.Unmarshal(payload); err != nil {
		return nil, err
	}
	return &rr, nil
}

func (h *Handler) runReadRequest(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	queries := req.GetQueries()
	out := &prompb.ReadResponse{Results: make([]*prompb.QueryResult, 0, len(queries))}
	for _, q := range queries {
		qr, err := h.runReadQuery(q)
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, qr)
	}
	return out, nil
}

func (h *Handler) runReadQuery(q *prompb.Query) (*prompb.QueryResult, error) {
	if q == nil {
		return &prompb.QueryResult{}, nil
	}
	start, end := q.GetStartTimestampMs(), q.GetEndTimestampMs()
	if start > end {
		return nil, errors.New("start_timestamp_ms > end_timestamp_ms")
	}
	if err := h.Eng.CheckQueryTimeRange(start, end); err != nil {
		return nil, err
	}
	plan, err := planRemoteReadMatchers(q.GetMatchers())
	if err != nil {
		return nil, err
	}
	series := h.Eng.QueryRangeByLabelFilter(plan.metric, plan.eqNarrow, start, end)
	if plan.pred != nil {
		series = filterFilterSeriesByLabels(series, plan.pred)
	}
	return filterSeriesListToResult(series), nil
}

func filterFilterSeriesByLabels(series []engine.FilterSeries, pred func(map[string]string) bool) []engine.FilterSeries {
	if pred == nil {
		return series
	}
	var out []engine.FilterSeries
	for _, fs := range series {
		if len(fs.Samples) == 0 {
			continue
		}
		if pred(storage.LabelMapForProm(fs.Samples[0])) {
			out = append(out, fs)
		}
	}
	return out
}

func labelMapToPrompb(m map[string]string) []prompb.Label {
	if len(m) == 0 {
		return nil
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	labels := make([]prompb.Label, 0, len(names))
	for _, n := range names {
		labels = append(labels, prompb.Label{Name: n, Value: m[n]})
	}
	return labels
}

func filterSeriesListToResult(series []engine.FilterSeries) *prompb.QueryResult {
	qr := &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(series))}
	for _, fs := range series {
		if len(fs.Samples) == 0 {
			continue
		}
		lm := storage.LabelMapForProm(fs.Samples[0])
		ts := &prompb.TimeSeries{
			Labels:  labelMapToPrompb(lm),
			Samples: make([]prompb.Sample, 0, len(fs.Samples)),
		}
		for _, s := range fs.Samples {
			ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: s.Timestamp, Value: s.Value})
		}
		qr.Timeseries = append(qr.Timeseries, ts)
	}
	return qr
}
