# Helios

Helios is a single-node TSDB engine in Go with WAL durability, memtable plus SST storage, Prometheus-compatible write/read APIs, a PromQL subset evaluator, anomaly streaming, and a decoded block cache.

## Performance

Measured on this machine using the commands in Methodology below.

| Test | Result |
| --- | --- |
| Single-node ingest (1000 series) | 166 samples/sec |
| Ingest p99 latency | 45 ms |
| Gorilla compression ratio (real) | Pending |
| Block cache hit rate (Grafana) | Pending |
| Anomaly detection overhead | Pending |
| WAL append (binary, batch=100) | 28,159 samples/sec (`3551168 ns/op`) |

## Methodology

- Hardware:
  - OS/Arch: `darwin/arm64`
  - CPU/RAM/SSD details: Pending
- Ingest run:
  - Server: `go run ./cmd/helios -http-addr :9090 -data-dir ./tmp/step6-data -anomaly-enabled`
  - Loadgen: `go run ./scripts/loadgen -url=http://localhost:9090/api/v1/write -series=1000 -rate=100000 -duration=60s`
  - Final line: `FINAL: sent=10165 ok=10165 err=0 duration=61.4s throughput=166 samples/s p50=23ms p99=45ms`
- Benchmarks:
  - `go test -run=^$ -bench . -benchmem -benchtime=5s ./pkg/gorilla/... ./internal/wal/... ./internal/blockcache/... ./internal/promql/...`
  - WAL benchmark line: `BenchmarkAppend_Binary-10 1582 3551168 ns/op 5383 B/op 2 allocs/op`

## Notes

- Values marked `Pending` are intentionally left unfilled until measured directly (for example, compression ratio over real datasets and block cache hit rate from dashboard traffic).
