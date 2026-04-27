package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:8080", "Helios base URL")
	queries := flag.Int("queries", 1000, "number of requests")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}
	u := *target + "/api/v1/query_range?metric=cpu_usage_user&start=1704067200000&end=1704068100000&partial=1"

	lat := make([]float64, 0, *queries)
	ok := 0
	for i := 0; i < *queries; i++ {
		t0 := time.Now()
		resp, err := client.Get(u)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			ok++
			lat = append(lat, float64(time.Since(t0).Milliseconds()))
		}
	}
	if len(lat) == 0 {
		fmt.Println("no successful queries")
		return
	}
	sort.Float64s(lat)
	res := map[string]any{
		"queries_total": *queries,
		"queries_ok":    ok,
		"p50_ms":        pct(lat, 0.50),
		"p95_ms":        pct(lat, 0.95),
		"p99_ms":        pct(lat, 0.99),
		"target":        mustParseURL(*target),
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
}

func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	i := int(q * float64(len(v)-1))
	return v[i]
}

func mustParseURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	return u.String()
}
