package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	in := flag.String("in", "bench/tsbs/data.influx", "input file (Influx line protocol)")
	target := flag.String("target", "http://127.0.0.1:8080/api/v1/write", "Helios remote_write endpoint")
	batch := flag.Int("batch", 1000, "timeseries per request")
	concurrency := flag.Int("concurrency", 8, "parallel HTTP workers")
	flag.Parse()

	f, err := os.Open(*in)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	jobs := make(chan *prompb.WriteRequest, *concurrency)
	var wg sync.WaitGroup
	var sentSamples int64

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range jobs {
				n := postRemoteWrite(*target, req)
				atomic.AddInt64(&sentSamples, int64(n))
			}
		}()
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	var batchTS []prompb.TimeSeries
	flush := func() {
		if len(batchTS) == 0 {
			return
		}
		jobs <- &prompb.WriteRequest{Timeseries: batchTS}
		batchTS = nil
	}
	for sc.Scan() {
		ts := parseInfluxLine(sc.Text())
		if ts == nil {
			continue
		}
		batchTS = append(batchTS, *ts)
		if len(batchTS) >= *batch {
			flush()
		}
	}
	if err := sc.Err(); err != nil {
		fatal(err)
	}
	flush()
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	total := atomic.LoadInt64(&sentSamples)
	fmt.Printf("sent %d samples in %s — %.0f samples/sec\n",
		total, elapsed.Truncate(time.Millisecond), float64(total)/elapsed.Seconds())
}

func parseInfluxLine(line string) *prompb.TimeSeries {
	if strings.HasPrefix(line, "#") || line == "" {
		return nil
	}
	sp := strings.SplitN(line, " ", 3)
	if len(sp) != 3 {
		return nil
	}
	nameAndTags := sp[0]
	fields := sp[1]
	tsNs, err := strconv.ParseInt(sp[2], 10, 64)
	if err != nil {
		return nil
	}
	tsMs := tsNs / 1_000_000

	parts := strings.Split(nameAndTags, ",")
	measurement := parts[0]
	labels := make([]prompb.Label, 0, len(parts))
	for _, p := range parts[1:] {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			labels = append(labels, prompb.Label{Name: kv[0], Value: kv[1]})
		}
	}

	for _, fp := range strings.Split(fields, ",") {
		kv := strings.SplitN(fp, "=", 2)
		if len(kv) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(kv[1], 64)
		if err != nil {
			continue
		}
		metric := measurement + "_" + kv[0]
		ls := make([]prompb.Label, 0, len(labels)+1)
		ls = append(ls, prompb.Label{Name: "__name__", Value: metric})
		ls = append(ls, labels...)
		return &prompb.TimeSeries{
			Labels:  ls,
			Samples: []prompb.Sample{{Timestamp: tsMs, Value: v}},
		}
	}
	return nil
}

func postRemoteWrite(target string, req *prompb.WriteRequest) int {
	body, err := req.Marshal()
	if err != nil {
		return 0
	}
	compressed := snappy.Encode(nil, body)
	httpReq, _ := http.NewRequest("POST", target, bytes.NewReader(compressed))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return 0
	}
	return len(req.Timeseries)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
