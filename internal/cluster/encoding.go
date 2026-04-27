package cluster

import (
	"sort"

	"github.com/prometheus/prometheus/prompb"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

// encodeBatch serializes a slice of samples for Raft replication.
// We reuse prompb.WriteRequest to avoid introducing a new wire format.
func encodeBatch(samples []storage.Sample) ([]byte, error) {
	ts := make([]prompb.TimeSeries, 0, len(samples))
	for _, s := range samples {
		ls := make([]prompb.Label, 0, len(s.Labels)+1)
		ls = append(ls, prompb.Label{Name: "__name__", Value: s.Metric})
		// Keep label order deterministic for stable test snapshots.
		names := make([]string, 0, len(s.Labels))
		for k := range s.Labels {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			ls = append(ls, prompb.Label{Name: k, Value: s.Labels[k]})
		}
		ts = append(ts, prompb.TimeSeries{
			Labels:  ls,
			Samples: []prompb.Sample{{Timestamp: s.Timestamp, Value: s.Value}},
		})
	}
	req := &prompb.WriteRequest{Timeseries: ts}
	return req.Marshal()
}

func decodeBatch(b []byte) ([]storage.Sample, error) {
	var req prompb.WriteRequest
	if err := req.Unmarshal(b); err != nil {
		return nil, err
	}
	out := make([]storage.Sample, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		base := storage.Sample{Labels: make(map[string]string, len(ts.Labels))}
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				base.Metric = l.Value
			} else {
				base.Labels[l.Name] = l.Value
			}
		}
		for _, p := range ts.Samples {
			s := base
			s.Timestamp = p.Timestamp
			s.Value = p.Value
			out = append(out, s)
		}
	}
	return out, nil
}
