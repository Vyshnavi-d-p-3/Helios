package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
)

func TestAPI_write_and_status(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	body := `{"samples":[{"metric":"m","ts":1,"value":2.5}]}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/write", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("write status %d", resp.StatusCode)
	}

	r2, _ := http.Get(srv.URL + "/api/v1/status")
	defer r2.Body.Close()
	var st map[string]interface{}
	_ = json.NewDecoder(r2.Body).Decode(&st)
	if int(st["mem_points"].(float64)) != 1 {
		t.Fatalf("status %+v", st)
	}
}

func TestAPI_healthz(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "d")
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	h := &Handler{Eng: eng, Version: "x"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
