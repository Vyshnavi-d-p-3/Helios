package api

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vyshnavi-d-p-3/helios/internal/anomaly"
	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestAPI_anomalySSE_oneEvent(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	reg := anomaly.NewRegistry(anomaly.RegistryConfig{
		Alpha:      0.3,
		Threshold:  3.0,
		Warmup:     10,
		MaxSeries:  1000,
		StaleAfter: time.Hour,
	})
	eng.AttachAnomalyRegistry(reg)

	h := &Handler{
		Eng:     eng,
		Version: "test",
		Anomaly: &AnomalyHandler{Reg: reg},
	}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/anomalies", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	metric := "req"
	labels := map[string]string{"job": "api"}
	for i := 0; i < 50; i++ {
		err = eng.Write([]storage.Sample{{
			Metric:    metric,
			Labels:    labels,
			Timestamp: int64(i * 1000),
			Value:     10 + float64(i%3-1)*0.1,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Write([]storage.Sample{{
		Metric:    metric,
		Labels:    labels,
		Timestamp: 60_000,
		Value:     1000,
	}}); err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(time.Second)
	events := 0
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, "data: ") {
			events++
			break
		}
	}
	if events != 1 {
		t.Fatalf("expected exactly one event, got %d", events)
	}
}
