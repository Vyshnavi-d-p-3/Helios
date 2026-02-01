package storage

// Sample is a single float64 time-series point with optional labels.
type Sample struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp int64             `json:"ts"` // unix milliseconds
	Value     float64           `json:"value"`
}

// LabelMapForProm includes __name__ plus all series labels, suitable for /api/v1/series.
func LabelMapForProm(s Sample) map[string]string {
	m := map[string]string{"__name__": s.Metric}
	if len(s.Labels) == 0 {
		return m
	}
	for k, v := range s.Labels {
		m[k] = v
	}
	return m
}
