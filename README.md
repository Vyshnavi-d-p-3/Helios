# Helios

Helios is a single-node time-series database written in Go.
It implements WAL + memtable + SSTable storage, Gorilla compression, label-postings filtering,
Prometheus-compatible write/read endpoints, a practical PromQL subset, and streaming EWMA anomaly detection.

## Why this exists

Helios is a systems portfolio project focused on "core TSDB mechanics over platform polish":
durable ingestion, compressed persistence, filterable query path, and measurable runtime behavior.
The goal is for every claim in this README to map to concrete code and repeatable commands.

## Architecture

- **WAL**: framed binary records with CRC32C checksums; replayed on boot.
- **Memtable**: in-memory write buffer with cardinality controls.
- **SSTable v2**: Gorilla-compressed points + label postings for query narrowing.
- **Read path**: query by exact label filters, matcher selectors, and PromQL subset.
- **Retention**: configurable age-based SST cleanup, plus manual retention endpoint.
- **Anomaly path**: EWMA detector registry + SSE stream for high-zscore events.
- **Observability**: `/metrics`, health/readiness probes, and status endpoint.

## Performance

Current baseline numbers (local dev machine, `darwin/arm64`):

| Metric | Value |
| --- | --- |
| Single-node ingest (1000 series) | 166 samples/sec |
| Ingest latency | p50 23ms / p99 45ms |
| WAL append benchmark | 28,159 samples/sec (`3551168 ns/op`) |
| TSBS ingest/query | tooling added in `bench/tsbs/` (run to refresh numbers) |

To regenerate workload numbers:

```bash
# Start Helios in one terminal
go run ./cmd/helios -http-addr :8080 -data-dir ./tmp/helios-bench

# Generate + load TSBS data in another terminal
chmod +x bench/tsbs/load_helios.sh
SCALE=100 TARGET=http://127.0.0.1:8080/api/v1/write ./bench/tsbs/load_helios.sh

# Query latency run
go run ./bench/tsbs/cmd/query_benchmark -target http://127.0.0.1:8080 -queries 1000
```

## API surface

Write:
- `POST /api/v1/write` (JSON or Prometheus remote_write payloads)

Read:
- `GET /api/v1/query`
- `GET /api/v1/query_range`
- `POST /api/v1/read` (Prometheus remote_read)

Metadata + ops:
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- `POST /api/v1/flush`
- `POST /api/v1/compact`
- `POST /api/v1/retention`
- `GET /api/v1/status`
- `GET /metrics`
- `GET /-/healthy`
- `GET /-/ready`

Anomaly detection:
- `GET /api/v1/anomalies` (SSE stream)

## PromQL subset

Implemented subset includes:
- selectors with label matchers (`=`, `!=`, `=~`, `!~`)
- range selectors (`metric[5m]`)
- functions: `rate`, `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time`, `count_over_time`
- aggregations with `by(...)` and `without(...)`: `sum`, `avg`, `min`, `max`, `count`
- scalar/vector and vector/vector arithmetic (`+`, `-`, `*`, `/`)

## What is intentionally out of scope

- Multi-node replication and Raft consensus.
- Full PromQL language compliance.
- Production auth/TLS and tenant isolation.

## Running locally

```bash
go build ./cmd/helios
./helios -data-dir ./data -http-addr :8080
```

## Documents

- `CRITICAL_REVIEW.md`
- `REVISED_PLAN.md`
- `docs/LABEL_INDEX_DESIGN.md`
- `docs/RAFT_BOOTSTRAP_FIX.md`
- `docs/BRANCHING.md`
- `docs/DEMO.md`
- `docs/adr/`
