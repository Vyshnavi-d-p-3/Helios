# LinkedIn — announcement (edit before posting)

**Body (template):**

Spent the last quarter building **Helios**, a 3-node replicated time-series database in Go.

**Repository:** https://github.com/Vyshnavi-d-p-3/Helios

**What’s in it:**

- LSM storage with Gorilla float compression (~6.8× ratio on representative data in benchmarks)
- Inverted label index for fast filtering
- PromQL subset — `rate`, `sum by`, `avg_over_time`, binary ops
- Inline EWMA anomaly detection with bounded LRU
- Raft replication, sub-2s leader failover in manual tests, hardlink snapshots
- ~500K samples/sec sustained ingest (see README / TSBS; your numbers may differ)

**Lessons that stuck:**

1. **Property-style tests for compression code.** A 5-bit field in a Gorilla-style encoder can corrupt streams in edge cases; hand-written unit tests often miss them.
2. **Bound everything.** Unbounded per-series state maps are an OOM in waiting at high cardinality. LRU + TTL (or hard caps) are non-optional.
3. **Snapshot strategy is architecture**, not a last-mile detail — especially under Raft.

Personalize the bullets and swap in your measured numbers before posting.
