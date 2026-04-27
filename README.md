# Helios

Helios is a time-series database written in Go (single-node by default; optional 3-node Raft replication).
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
- **Optional clustering**: hashicorp Raft with leader forwarding on `POST /api/v1/write`, SST-backed FSM snapshots, and Bolt-backed Raft logs (enable with `-peers` and/or `-bootstrap`).
- **Observability**: `/metrics`, health/readiness probes, `/livez`, and status endpoint.

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
- `GET /livez` (process up; suitable for minimal container healthchecks)

Cluster (only when `-bootstrap` and/or `-peers` is set):
- `GET /cluster/leader` — Raft leader id and role
- `POST /cluster/join` — leader-only; JSON `{"id","address"}` to add a voter at runtime

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

- Sharded multi-tenant clusters beyond a single Raft group.
- Full PromQL language compliance.
- Production auth/TLS and tenant isolation.

## Running locally

```bash
go build ./cmd/helios
./helios -data-dir ./data -http-addr :8080
```

### Optional: 3-node Raft (Docker Compose)

From `deployments/` (needs a one-time `docker compose build`):

```bash
cd deployments
docker compose up --build
```

Quick checks (containers healthy): `chmod +x smoke_cluster.sh && ./smoke_cluster.sh` from `deployments/` (hits `/livez` and `/cluster/leader` on ports 8080–8082).

Environment highlights: `HELIOS_RAFT_ADDR` (advertised host:port for Raft TCP), `HELIOS_PEERS` (comma-separated `id=host:raftport` or `id@host:raft:http`), `HELIOS_BOOTSTRAP=true` only on the seed node, `HELIOS_RAFT_DIR` for persistent Raft logs (separate from TSDB `HELIOS_DATA_DIR`). Writes to any node are forwarded to the Raft leader’s HTTP endpoint.

## Documents

- `CRITICAL_REVIEW.md`
- `REVISED_PLAN.md`
- `docs/LABEL_INDEX_DESIGN.md`
- `RAFT_BOOTSTRAP_FIX.md` (bootstrap notes; ADR **`docs/adr/0005-raft-replication.md`** is canonical)
- `docs/BRANCHING.md`
- `docs/DEMO.md`
- `docs/distribution/` — blog draft, demo script, LinkedIn / Show HN templates, CV bullets, interview prep
- `docs/adr/`
