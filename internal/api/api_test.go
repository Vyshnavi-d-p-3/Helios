package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vyshnavi-d-p-3/helios/internal/config"
	"github.com/vyshnavi-d-p-3/helios/internal/engine"
	"github.com/vyshnavi-d-p-3/helios/internal/storage"
)

func TestAPI_remote_read_snappySamples(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 100, Value: 1},
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 200, Value: 2},
	})

	rr := prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: 0,
			EndTimestampMs:   500,
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
				{Type: prompb.LabelMatcher_EQ, Name: "j", Value: "a"},
			},
		}},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	}
	pb, err := rr.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	enc := snappy.Encode(nil, pb)
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Encoding") != "snappy" {
		t.Fatalf("encoding %q", resp.Header.Get("Content-Encoding"))
	}
	dec, err := snappy.Decode(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	var out prompb.ReadResponse
	if err := out.Unmarshal(dec); err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 1 {
		t.Fatalf("results %d", len(out.Results))
	}
	tsl := out.Results[0].GetTimeseries()
	if len(tsl) != 1 {
		t.Fatalf("timeseries %d", len(tsl))
	}
	if len(tsl[0].Samples) != 2 {
		t.Fatalf("samples %d", len(tsl[0].Samples))
	}
}

func TestAPI_remote_read_neqMatcher(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 100, Value: 1},
		{Metric: "m", Labels: map[string]string{"j": "b"}, Timestamp: 100, Value: 2},
	})
	rr := prompb.ReadRequest{Queries: []*prompb.Query{{
		StartTimestampMs: 0, EndTimestampMs: 200,
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
			{Type: prompb.LabelMatcher_NEQ, Name: "j", Value: "b"},
		},
	}}}
	pb, _ := rr.Marshal()
	enc := snappy.Encode(nil, pb)
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	dec, _ := snappy.Decode(nil, raw)
	var out prompb.ReadResponse
	if err := out.Unmarshal(dec); err != nil {
		t.Fatal(err)
	}
	if len(out.Results[0].Timeseries) != 1 {
		t.Fatalf("ts count %d", len(out.Results[0].Timeseries))
	}
}

func TestAPI_remote_read_regexMatcher(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"j": "api-v1"}, Timestamp: 100, Value: 1},
		{Metric: "m", Labels: map[string]string{"j": "web"}, Timestamp: 100, Value: 2},
	})
	rr := prompb.ReadRequest{Queries: []*prompb.Query{{
		StartTimestampMs: 0, EndTimestampMs: 200,
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
			{Type: prompb.LabelMatcher_RE, Name: "j", Value: "api-.*"},
		},
	}}}
	pb, _ := rr.Marshal()
	enc := snappy.Encode(nil, pb)
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	dec, _ := snappy.Decode(nil, raw)
	var out prompb.ReadResponse
	if err := out.Unmarshal(dec); err != nil {
		t.Fatal(err)
	}
	if len(out.Results[0].Timeseries) != 1 {
		t.Fatalf("ts count %d", len(out.Results[0].Timeseries))
	}
}

func TestAPI_remote_read_nameRegexError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	rr := prompb.ReadRequest{Queries: []*prompb.Query{{
		StartTimestampMs: 0, EndTimestampMs: 10,
		Matchers: []*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_RE, Name: "__name__", Value: "m.*"},
		},
	}}}
	pb, _ := rr.Marshal()
	enc := snappy.Encode(nil, pb)
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestAPI_remote_read_notAcceptableStreamOnly(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	rr := prompb.ReadRequest{
		Queries:               []*prompb.Query{{StartTimestampMs: 0, EndTimestampMs: 10, Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"}}}},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}
	pb, _ := rr.Marshal()
	enc := snappy.Encode(nil, pb)
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotAcceptable {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestAPI_remote_write_snappyProtobuf(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	wr := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "prometheus"},
			},
			Samples: []prompb.Sample{{Timestamp: 1234, Value: 1}},
		}},
	}
	pb, err := wr.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	enc := snappy.Encode(nil, pb)

	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/write", bytes.NewReader(enc))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if eng.MemLen() != 1 {
		t.Fatalf("mem %d", eng.MemLen())
	}
}

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

	r2, err := http.Get(srv.URL + "/api/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Body.Close()
	var st map[string]interface{}
	_ = json.NewDecoder(r2.Body).Decode(&st)
	if int(st["mem_points"].(float64)) != 1 {
		t.Fatalf("status %+v", st)
	}
}

func TestAPI_query_rangeGET(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 100, Value: 1},
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 200, Value: 2},
	})

	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	u := srv.URL + "/api/v1/query_range?metric=m&l_j=a&start=0&end=500"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "success" {
		t.Fatalf("body %+v", out)
	}
}

func TestAPI_query_range_partial(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "m", Labels: map[string]string{"j": "a"}, Timestamp: 10, Value: 1},
		{Metric: "m", Labels: map[string]string{"j": "b"}, Timestamp: 20, Value: 2},
	})
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	u := srv.URL + "/api/v1/query_range?metric=m&start=0&end=100&partial=1"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]float64 `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data.Result) != 2 {
		t.Fatalf("expected 2 series, got %d", len(out.Data.Result))
	}
}

func TestAPI_query_promql_instant(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "up", Labels: map[string]string{"job": "api"}, Timestamp: 10_000, Value: 1},
	})
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/query?query=up&time=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	var out struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "success" || out.Data.ResultType != "vector" {
		t.Fatalf("unexpected response %+v", out)
	}
	if len(out.Data.Result) != 1 {
		t.Fatalf("result len %d", len(out.Data.Result))
	}
	if out.Data.Result[0].Metric["__name__"] != "up" {
		t.Fatalf("metric object %+v", out.Data.Result[0].Metric)
	}
}

func TestAPI_query_promql_matchers(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "up", Labels: map[string]string{"job": "api"}, Timestamp: 10_000, Value: 1},
		{Metric: "up", Labels: map[string]string{"job": "web"}, Timestamp: 10_000, Value: 2},
	})
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/query?query=up%7Bjob%3D%22api%22%7D&time=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data.Result) != 1 || out.Data.Result[0].Metric["job"] != "api" {
		t.Fatalf("unexpected result %+v", out.Data.Result)
	}
}

func TestAPI_query_promql_range(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 0, Value: 0},
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 60_000, Value: 60},
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 120_000, Value: 120},
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 180_000, Value: 180},
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 240_000, Value: 240},
		{Metric: "req", Labels: map[string]string{"job": "api"}, Timestamp: 300_000, Value: 300},
	})
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	u := srv.URL + "/api/v1/query_range?query=rate(req%5B5m%5D)&start=0&end=300&step=15"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "success" || out.Data.ResultType != "matrix" {
		t.Fatalf("unexpected response %+v", out)
	}
}

func TestAPI_query_promql_aggregation(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{
		{Metric: "up", Labels: map[string]string{"job": "api"}, Timestamp: 10_000, Value: 1},
		{Metric: "up", Labels: map[string]string{"job": "api"}, Timestamp: 10_000, Value: 2},
	})
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/query?query=sum%20by%20(job)%20(up)&time=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data.Result) != 1 || out.Data.Result[0].Metric["job"] != "api" {
		t.Fatalf("unexpected result %+v", out.Data.Result)
	}
}

func TestAPI_query_promql_parseError(t *testing.T) {
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

	resp, err := http.Get(srv.URL + "/api/v1/query?query=rate(up)&time=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestAPI_labelRoutes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{{
		Metric: "m", Labels: map[string]string{"a": "b"}, Timestamp: 1, Value: 1,
	}})
	h := &Handler{Eng: eng, Version: "t"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/labels")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("labels status %d", resp.StatusCode)
	}
	resp2, err := http.Get(srv.URL + "/api/v1/label/__name__/values")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("values status %d", resp2.StatusCode)
	}
}

func TestAPI_series(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{{
		Metric: "q", Labels: map[string]string{"k": "v"}, Timestamp: 1, Value: 1,
	}})
	h := &Handler{Eng: eng, Version: "t"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	u := srv.URL + "/api/v1/series?start=0&end=10&metric=q&l_k=v"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	resp2, err := http.Get(srv.URL + "/api/v1/series?start=0&end=10&match[]=q&match[]=q")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("match status %d", resp2.StatusCode)
	}
}

func TestAPI_series_match_braceSelector(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = dir
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{{
		Metric: "q", Labels: map[string]string{"k": "v"}, Timestamp: 1, Value: 1,
	}})
	h := &Handler{Eng: eng, Version: "t"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	q := url.Values{}
	q.Set("start", "0")
	q.Set("end", "10")
	q.Add("match[]", `q{k="v"}`)
	resp, err := http.Get(srv.URL + "/api/v1/series?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestAPI_metrics(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	h := &Handler{Eng: eng, Version: "test"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if !strings.Contains(s, "helios_engine_memtable_points") {
		t.Fatalf("missing helios gauge, got: %.200q", s)
	}
	if !strings.Contains(s, "helios_ingest_samples_committed_total") {
		t.Fatalf("missing ingest counter, got: %.200q", s)
	}
	if !strings.Contains(s, "helios_ingest_rejected_total") {
		t.Fatalf("missing ingest rejected counter, got: %.200q", s)
	}
	if !strings.Contains(s, "helios_read_requests_total") {
		t.Fatalf("missing read requests counter, got: %.200q", s)
	}
	if !strings.Contains(s, "helios_api_requests_total") {
		t.Fatalf("missing api requests counter, got: %.200q", s)
	}
	if !strings.Contains(s, "helios_build_info{version=\"test\"} 1") {
		t.Fatalf("missing build_info, got: %.200q", s)
	}
	if !strings.Contains(s, "go_goroutines") {
		t.Fatalf("missing go collector")
	}
}

func TestAPI_retention(t *testing.T) {
	d := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.DataDir = d
	cfg.RetentionPeriod = 30 * time.Minute
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	_ = eng.Write([]storage.Sample{{Metric: "x", Timestamp: time.Now().Add(-time.Hour).UnixMilli(), Value: 1}})
	_ = eng.Flush()
	if eng.SSTCount() != 1 {
		t.Fatalf("expected 1 sst, got %d", eng.SSTCount())
	}
	h := &Handler{Eng: eng, Version: "t"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/api/v1/retention", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		Status  string `json:"status"`
		Removed int    `json:"removed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "ok" || out.Removed != 1 {
		t.Fatalf("response %+v", out)
	}
	if eng.SSTCount() != 0 {
		t.Fatalf("sst %d", eng.SSTCount())
	}
}

func TestAPI_probes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	h := &Handler{Eng: eng, Version: "t"}
	srv := httptest.NewServer(NewServeMux(h))
	defer srv.Close()
	for _, path := range []string{"/-/healthy", "/-/ready"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
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
