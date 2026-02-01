# Revised Plan — What to Actually Build in 10 Weeks

The v1 plan promised a fully distributed, sharded, replicated TSDB with custom
storage, custom compression, query language, and anomaly detection in 200 hours.
Honest assessment: that's a 6-month project even for someone who's done it before.

For 10 weeks at staff-portfolio quality, this is what actually fits.

## The cut

**Drop multi-shard sharding.** It adds 3-4 weeks of complexity (shard router,
fan-out query planner, multi-shard merge) for almost no incremental learning
signal in interviews. A 3-node single-shard replicated cluster demonstrates Raft
just as well, plus you can claim "we replicate across all nodes" instead of
having to defend why a 3-node cluster needed sharding at all.

If asked about sharding in interviews, the answer is: "Sharding is conceptually
a layer above replication — you'd add a coordinator that uses jump hash for
metric→shard routing and fan-out for queries. I scoped it out of v1 because
single-shard already exercises the storage engine and Raft thoroughly."

**Keep everything else.** Storage engine, Gorilla, Raft replication, PromQL
subset, label index (NEW vs v1), EWMA detection — all critical for the portfolio
narrative and all achievable.

---

## Revised week-by-week

### Weeks 1-2: Storage engine (40 h)
- WAL with binary framing + CRC32C (1 day)
- Bitstream + Gorilla encoder/decoder, with the v2 fixes (2 days)
- Memtable on top of huandu/skiplist (1 day)
- SSTable v2 format: data blocks + series index + postings + label values + Bloom + footer (3 days)
- Engine orchestrator: write path → memtable → flush, read path → merge memtable + SSTables (2 days)
- L0 compaction (1 day)
- Crash recovery (WAL replay) (1 day)
- Property-based tests with rapid (1 day)

By the end of week 2: a single-process embedded TSDB you can write to and query
by `(metric, time-range)`. ~6 K LOC.

### Week 3: Label index + cardinality (20 h)
This is the WEEK I'M ADDING vs v1. See LABEL_INDEX_DESIGN.md.
- In-memory postings index (RoaringBitmap) (2 days)
- Postings + label-values sections in SSTable v2 format (1 day)
- Per-metric cardinality enforcement (½ day)
- Tests: query correctness with `{label="value"}`, regex matchers, cardinality rejection (1½ days)

By end of week 3: queries like `http_requests{status="500"}` work in O(log N).

### Week 4: Query engine (20 h)
- PromQL lexer (½ day)
- Recursive-descent parser → AST (1 day)
- Executor: rate, avg_over_time, sum, count (2 days)
- HTTP API: /api/v1/write (Prometheus remote_write), /api/v1/query, /api/v1/query_range, /metrics (1 day)
- Grafana integration test (½ day)

By end of week 4: a single-node TSDB Grafana can talk to. The complete project
demo could ship here — replication is "stretch."

### Weeks 5-6: Replication via Raft (40 h)
- Raft FSM as a BatchingFSM wrapping Engine.Write (2 days)
- RaftNode with full-config bootstrap (see RAFT_BOOTSTRAP_FIX.md) (2 days)
- Coordinator: write to leader, forward if not leader (1 day)
- Snapshot via SSTable hardlinks (2 days)
- Docker Compose 3-node cluster (1 day)
- Failover tests: kill leader, verify election + writes resume (1 day)
- Network partition tests (1 day)

By end of week 6: 3-node replicated cluster, proper failover, snapshots.

### Week 7: Polish + benchmarks (20 h)
- TSBS-based ingest benchmarks (2 days) — uses an industry-standard tool, not a
  custom load generator. Way more credible.
- Comparison run against InfluxDB OSS 2.x with same data (1 day)
- Block cache for hot SSTable reads (1 day)
- Retention/TTL: drop SSTables older than `RetentionPeriod` (½ day)
- Bug fixes from benchmark stress (½ day)

### Week 8: Anomaly detection (20 h)
- EWMA detector with v2 fixes (NaN guard, bounded LRU) (2 days)
- gRPC streaming endpoint for subscribers (1 day)
- Simple anomaly dashboard in Next.js (1 day)
- Tuning + tests with synthetic anomaly injection (1 day)

### Weeks 9-10: Ship (40 h)
- README with architecture, benchmarks, design decisions (1 day)
- 4 ADRs: LSM choice, Gorilla, Raft library, label index (1 day)
- Demo video (1 day)
- Blog post: "Building a TSDB in 10 weeks: what I learned about LSMs, Raft,
  and label indexes" (3 days)
- Final benchmark runs, screenshots (1 day)
- Code cleanup: gofmt, golangci-lint, godoc (1 day)
- Release tag, ship to HN, LinkedIn (1 day)
- Buffer (1 day)

Total: 200 build hours — same budget as v1, different scope.

---

## What you can claim at the end

> "I built Helios, a single-shard, 3-node-replicated TSDB in Go. The storage
> engine is a custom LSM with Gorilla compression (8x ratio on real data) and
> an inverted label index (Prometheus-style postings). Replication uses
> hashicorp/raft with batched FSM apply for ~500K samples/sec write throughput.
> Failover under leader kill is sub-2-second with zero data loss in benchmarks.
> Streaming EWMA anomaly detection runs inline at <1% overhead. Comes in around
> 12K lines of Go with property-based tests on the storage invariants."

That sentence wins infrastructure interviews. v1's plan, half-completed in
10 weeks, would not get there because the missing pieces (label index, batching,
realistic Raft) are exactly what interviewers ask about.

---

## What's stretch (don't promise; ship if there's time)

- Multi-shard with jump hash routing
- Persistent block cache (mmap-based)
- TLS for Raft + bearer auth for HTTP
- Snapshot streaming for huge stores (currently snapshots are bounded by
  SSTable hardlinking which only works on the same filesystem)
- Vector matching in PromQL (`a / b` where both are vectors)
- Fault injection / chaos test suite

If you ship the core in 8 weeks instead of 10, pick one stretch goal. Don't
pick all of them.
