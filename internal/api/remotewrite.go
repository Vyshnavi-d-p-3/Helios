package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func decodeWriteBody(data []byte, contentType, contentEncoding string) ([]storage.Sample, error) {
	if len(data) == 0 {
		return nil, errors.New("empty body")
	}
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "json") {
		return decodeJSONWrite(data)
	}
	// Protobuf remote_write (typical: application/x-protobuf + snappy)
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
	var wr prompb.WriteRequest
	if err := wr.Unmarshal(payload); err != nil {
		// Heuristic: bare JSON without Content-Type
		if data[0] == '{' {
			if s, jerr := decodeJSONWrite(data); jerr == nil {
				return s, nil
			}
		}
		return nil, err
	}
	samples, err := prompbToSamples(&wr)
	if err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, errors.New("no time series or samples in remote write")
	}
	return samples, nil
}

func decodeJSONWrite(data []byte) ([]storage.Sample, error) {
	var req writeRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	if len(req.Samples) == 0 {
		return nil, errors.New("empty samples")
	}
	return req.Samples, nil
}

func prompbToSamples(wr *prompb.WriteRequest) ([]storage.Sample, error) {
	var out []storage.Sample
	for _, ts := range wr.Timeseries {
		metric, labels := prompbLabelsToMetricAndLabels(ts.Labels)
		if metric == "" {
			continue
		}
		for _, s := range ts.Samples {
			out = append(out, storage.Sample{
				Metric:    metric,
				Labels:    labels,
				Timestamp: s.Timestamp, // ms
				Value:     s.Value,
			})
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// prompbLabelsToMetricAndLabels splits __name__ into the metric and all other labels.
func prompbLabelsToMetricAndLabels(labs []prompb.Label) (metric string, labels map[string]string) {
	labels = make(map[string]string)
	for _, l := range labs {
		if l.Name == "__name__" {
			metric = l.Value
			continue
		}
		labels[l.Name] = l.Value
	}
	if len(labels) == 0 {
		labels = nil
	}
	return metric, labels
}
