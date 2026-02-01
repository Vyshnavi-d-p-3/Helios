# Helios v1 — Critical Review

A staff engineer reviewing the v1 output would flag the following.
Some are real bugs that produce wrong behavior. Others are architectural omissions
that mean the system can't actually do what a TSDB needs to do. A few are just sloppy.

---

## 1. Bugs in the code (would fail at runtime)

### 1a. Gorilla encoder loses data when `leading >= 32`

```go
e.bw.WriteBits(uint64(leading), 5)  // 5 bits → max value 31
```

`LeadingZeros64` returns 0–63 for non-zero XOR. When `leading >= 32`, only the low
5 bits are written, silently corrupting the encoding. This happens whenever consecutive
values share their high 32 bits — which is *common* (e.g., monotonic counters that
stay below 2^32). This is the most cited subtlety in the Gorilla paper and the v1
encoder gets it wrong.

**Fix:** clamp leading to 31 before encoding (the paper does this explicitly).

### 1b. Gorilla encoder relies on integer truncation for `meaningful = 64`

When `leading == 0` and `trailing == 0`, `meaningful = 64`. But the encoder writes
6 bits (`WriteBits(64, 6)`) — which silently writes the low 6 bits = 0. The decoder
happens to map 0 → 64. This works, but it's a footgun: the encoder doesn't document
the mapping, the decoder does, and a future reader will "fix" one or the other and
break compatibility.

**Fix:** explicit mapping in the encoder: `if meaningful == 64 { meaningful = 0 }`.

### 1c. EWMA registry leaks memory unboundedly

```go
r.detectors[seriesKey] = det  // never evicted
```

A real TSDB tracks 10⁶+ unique series. Per-series detector ≈ 80 bytes. That's 80MB
just for detector state, growing forever. No LRU, no TTL, no high-cardinality cap.
At Prometheus scale this would OOM the process within a day.

**Fix:** bounded LRU with a per-series last-seen timestamp; evict on cardinality limit
or staleness threshold.

### 1d. EWMA detector poisons on NaN/Inf

If a single sample is `NaN`, every subsequent computation propagates NaN through
`fastMean`, `variance`, `stddev`. The detector is permanently broken for that series
and emits garbage z-scores. Real metrics pipelines do produce NaNs (division by zero
in client code, missed scrape gaps).

**Fix:** reject non-finite values at the entry point.

### 1e. `config.Parse()` is untestable

```go
func Parse() Config {
    flag.Parse()  // global flag.CommandLine
}
```

`flag.CommandLine` has already been parsed by `go test` before any test runs. The
function can't be called from a test without panics, and there's no way to inject
custom args. The package literally has no tests because of this.

**Fix:** accept a `*flag.FlagSet` and `[]string` args; provide a thin `Parse()` that
wraps `os.Args[1:]`.

### 1f. Drift detection comment lies about the math

The EWMA variance update is described as "Welford's algorithm adapted." It's not.
Welford's algorithm computes batch variance with a sample count. What's there is
a standard EWMA variance update. A reviewer who knows Welford's would catch this in
30 seconds and lose trust in the rest of the file. Words matter at staff level.

---

## 2. Docker Compose has a real bootstrap race

```yaml
helios-1:
  HELIOS_BOOTSTRAP: "true"
helios-2:
  depends_on: { helios-1: { condition: service_healthy } }
```

`helios-1` bootstraps a single-node Raft cluster (voters = [helios-1]). It becomes
healthy. Then helios-2/3 start and try to join. But nobody calls `raft.AddVoter()` —
the v1 prompt for `RaftNode` doesn't include join logic. helios-2 and helios-3 will
sit forever with no leader visibility.

**Fix:** the bootstrap node reads `HELIOS_PEERS` and adds them as non-voters on
startup; promotes to voters when their logs catch up. Or use raft's
`BootstrapCluster` with the full configuration up front and have all nodes wait
for each other before bootstrapping.

---

## 3. The architecture is missing a label index — this is the big one

The v1 design says "metric_name → SSTable index." That's enough for queries like
`http_requests_total`. It is **not** enough for:

```promql
http_requests_total{status="500", method="GET"}
sum(http_requests_total) by (job)
{__name__=~"http_.*"}
```

These queries need an **inverted index**: `(label_name, label_value) → posting list
of series IDs`. Without it, the executor must scan every series with a matching
metric and filter labels in memory — at 10K label-combinations per metric that's
10K Gorilla blocks decoded per query. This is the difference between a TSDB and a
key-value store with timestamps glued on.

Every real TSDB has this:
- Prometheus: `tsdb/index/` with postings per label pair
- VictoriaMetrics: per-tag inverted index in `mergeset`
- InfluxDB IOx: tag column dictionaries with bitmap postings

The v1 design omits this entirely. **A staff engineer would not ship this design.**

**Fix:** see `LABEL_INDEX_DESIGN.md` in this package.

---

## 4. Other architectural gaps

**No cardinality control.** If a misconfigured client emits `request_id` as a label,
the series count explodes (millions of one-sample series). Helios needs a
per-metric cardinality cap that rejects new series with a clear error.

**No block cache.** Every read decodes a Gorilla block from disk. Hot series get
queried thousands of times per minute. A simple LRU block cache (16-64MB) would
turn p99 read latency from milliseconds to microseconds.

**No retention / TTL.** SSTables live forever. Real TSDBs delete data older than N
days. The compaction strategy needs a "drop SSTables where MaxTime < cutoff" pass.

**Raft Apply throughput bottleneck.** hashicorp/raft serializes Apply through a
single goroutine. Sending one sample per Apply would cap at ~5K samples/sec total
(not per node — total). The coordinator MUST batch samples before submitting. This
isn't mentioned in the v1 prompt, so Cursor will generate a per-sample Apply loop
that benchmarks 100× too slow.

**Snapshots not bounded.** `FSM.Snapshot()` is described as "include all SSTable
files + memtable." For a 100GB store that's a 100GB snapshot copied on every
follower restart. Real systems use file-based snapshots that hardlink immutable
SSTables and only stream the memtable + manifest.

**No security mention anywhere.** Raft transport unencrypted, HTTP API unauthenticated.
For a portfolio project that's defensible — but a staff interviewer will ask
"how would you secure this?" and a "we just didn't" is the wrong answer. Mention
mTLS for Raft + bearer tokens for HTTP in the README's "production hardening"
section.

---

## 5. The 10-week plan is genuinely unrealistic

200 build hours for a distributed TSDB with custom storage engine, custom
compression, distributed consensus, query language, AND streaming anomaly detection
is not a thing one person delivers in 10 weeks at staff quality. Reference points:

- VictoriaMetrics took ~2 years to reach v1.
- TimescaleDB had a team of 5+ for the first year.
- Prometheus's tsdb/ package is ~30K LOC and took 2 years.

This is fine for a *portfolio* project — the goal isn't to compete with
VictoriaMetrics, it's to demonstrate the depth. But the plan should be honest:

- **Weeks 1-4:** Single-node storage engine + query (this is already 80% of the
  interview value).
- **Weeks 5-7:** Add Raft for replication on 3 nodes. Skip multi-shard distribution.
  Ship 3 nodes that all hold all data.
- **Weeks 8-9:** EWMA + label index + benchmarks.
- **Week 10:** Polish + blog post.

Skip multi-shard sharding entirely or make it a post-v1 stretch goal. Single-shard
3-node replication demonstrates Raft just as well and is achievable. Sharding adds
weeks of work for no incremental learning signal in interviews.

---

## 6. Missing testing infrastructure

The v1 plan says "use rapid for property-based tests." Good. But:

- No mention of how to make the test suite run in <30s (currently a single naive
  test file in this design takes 15s for SSTable round-trips alone).
- No mention of testcontainers for real Raft cluster integration tests vs in-process
  fakes.
- Cluster tests are flaky by nature (Raft elections take a randomized timeout). The
  guide should explicitly require `eventually()` helpers with bounded retries, not
  fixed sleeps.
- No fault-injection harness. For a project that *demonstrates distributed systems
  rigor*, fault injection is mandatory — it's the difference between "I used Raft"
  and "I tested Raft under partition."

---

## 7. Things that were actually fine

- The .cursorrules file structure is solid. Cursor will use it well.
- The bitstream reader/writer is correct (modulo the optimization notes).
- The phase ordering (storage → query → distribution → anomaly → polish) is right.
- The proto file is reasonable.
- The ADR template is appropriate.

---

## Summary: severity-ranked

| # | Issue | Severity | Time to fix |
|---|---|---|---|
| 1a | Gorilla `leading >= 32` data loss | **Critical** | 5 min |
| 1c | EWMA unbounded memory | **Critical** | 30 min |
| 3 | Missing label index | **Critical (architecture)** | Weeks of design + impl |
| 1d | EWMA NaN poisoning | **High** | 10 min |
| 2 | Docker bootstrap race | **High** | 30 min |
| 1e | Config not testable | **High** | 15 min |
| 4 | Cardinality / cache / retention / Raft batching | **High** | Per-item, hours each |
| 5 | Unrealistic timeline | **High (planning)** | Plan revision |
| 1b | Gorilla `meaningful=64` implicit truncation | **Medium** | 5 min |
| 1f | Welford's mislabeling | **Medium** | 1 min |
| 6 | Test infrastructure gaps | **Medium** | Spread across phases |

The v1 output works as a starting point. With the fixes in this package, it becomes
defensible.
