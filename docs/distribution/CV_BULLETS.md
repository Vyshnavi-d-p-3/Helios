# CV / portfolio bullets — Helios

Pick the density that fits the slot. Update metrics with numbers you actually measured.

---

**One-line**

Helios — Built a 3-node Raft-replicated time-series database in Go (Gorilla compression, inverted label index, PromQL subset, EWMA anomalies). https://github.com/Vyshnavi-d-p-3/Helios

---

**Two-line**

Helios — Designed and implemented a TSDB in Go: LSM engine with WAL/memtable/SSTables, Gorilla XOR compression, postings-based label filtering, recursive-descent PromQL subset, bounded EWMA anomaly registry, and 3-node Raft replication with streamed snapshots. Sustained ingest and failover figures per README / benchmarks.

https://github.com/Vyshnavi-d-p-3/Helios

---

**Portfolio block**

**Helios — Distributed time-series database (Go)**  
Stack: Go, hashicorp/raft, Bolt-backed Raft logs, Prometheus `prompb`, SSE.

**Repo:** https://github.com/Vyshnavi-d-p-3/Helios  

**Blog:** *[add your URL]*

**Highlights:**

- Custom LSM write path (WAL → skiplist memtable → SSTables) with L0 compaction
- Gorilla-style float compression; postings index for O(min(|A|,|B|)) intersections on sorted lists
- PromQL subset (matchers, range selectors, core functions / aggregations / binops)
- EWMA anomaly detection with LRU-bounded registry; SSE at `GET /api/v1/anomalies`
- Raft `BatchingFSM`, follower write forwarding to leader HTTP, SST hardlink snapshots + streamed restore
- Benchmarks documented in README (TSBS tooling under `bench/tsbs/`)
