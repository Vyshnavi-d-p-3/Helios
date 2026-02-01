package storage

// Sample is a single float64 time-series point with optional labels.
type Sample struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp int64             `json:"ts"` // unix milliseconds
	Value     float64           `json:"value"`
}
