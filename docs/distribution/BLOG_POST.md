# Helios: replicated time-series storage, Gorilla compression, and the bug that ate a weekend

**Suggested title for publication:** *Helios: replicated time-series storage, Gorilla compression, and the bug that ate a weekend*

**Repo:** https://github.com/Vyshnavi-d-p-3/Helios  

*Draft for editing (~1.3k words in this file; expand with your measured table and war stories to reach ~3k for long-form venues). Metrics are illustrative — replace with numbers you reproduced locally (README + `bench/tsbs/`).*

---

I spent a chunk of this year building **Helios** — a **three-node Raft-replicated** time-series database in **Go**. It speaks **Prometheus remote_write**, evaluates a **useful subset of PromQL**, keeps an **inverted postings index** for label filtering, **compresses floats Gorilla-style**, runs **EWMA anomaly detection** on the write path, and ships a **Docker Compose** layout for a small cluster.

This is not a TimescaleDB competitor. It is a **portfolio-grade systems project** that happens to run end-to-end. What I want to capture is what surprised me: the sharp edges in **compression**, the **index** decision I almost got wrong, and what **Raft snapshots** look like when you refuse to `tar` a hundred gigabytes every time a follower catches up.

## The shape of the problem

A TSDB is still, at bottom, **ordered storage for `(metric, labels, timestamp) → float`**, queried over time windows and label predicates. Postgres is a fine database; it is not tuned for tens of millions of append-mostly series at scrape intervals that imply **high-cardinality writes** and **range-heavy reads**.

Industrial TSDBs converge on a small toolkit:

- **LSM-ish storage** — buffer writes, flush immutable sorted files, compact.
- **Specialized float compression** — Gorilla-style XOR streams are the textbook answer.
- **Inverted indexes on labels** — map `(name, value)` pairs to sorted series IDs and **intersect** candidate sets.
- **A vector query language** — PromQL is how operators actually talk to the data.

Helios implements all four. None of them are novel in isolation; the learning is in **integration** and in **failure modes**.

## The honest timeline

The first roadmap was optimistic: **multi-shard** everything, **full PromQL**, **Raft**, **anomaly detection**, all in a few calendar months at part-time intensity. A more experienced reviewer looked at v1 and called it closer to **half a year** of focused work. They were right.

The revised plan **dropped multi-shard sharding** — a **single replicated shard** still exercises consensus, the write path, and failover, without a shard router and cross-shard queries. Interview answer: *sharding is a coordination layer above replication; jump-hash routing and query fan-out are understood add-ons once one shard is solid.*

What actually shipped, in broad strokes:

- **Storage engine:** WAL, memtable, SSTable evolution, postings, compaction, retention.
- **Query & API:** native + PromQL subset, Prometheus remote read/write compatibility.
- **Benchmarks:** TSBS-style tooling under `bench/tsbs/`.
- **Anomalies:** EWMA registry with hard bounds.
- **Raft:** `hashicorp/raft`, `BatchingFSM`, follower **HTTP forwarding** to the leader for writes.

I missed the naive schedule by weeks, not days. **Raft + snapshots** consumed more calendar time than I expected — in part because distributed systems punish “almost right.”

## The storage engine

**Write path:** framed **WAL** (durability), in-memory **skiplist** memtable (`github.com/huandu/skiplist`), flush to **immutable SSTables** when the memtable crosses a byte budget. **Reads** merge the memtable with open SSTables, newer winning on collisions for the same series key and timestamp.

**SST evolution:** early designs indexed “by metric” only. That answers `http_requests_total` — it does **not** answer `http_requests_total{status="500"}` at scale. **v2** adds **per–label-value postings**: sorted lists of canonical series keys, intersected **smallest-first** for cheap filters. The jump from scanning **O(N)** series under a metric to **O(min(|A|,|B|))**-style intersections is the difference between unusable and snappy on real cardinalities.

**Compaction:** Helios stays in a deliberately **simple** tiering model (L0-style merges) documented in the README — not because L2+ compactions are unknowable, but because the project already had enough moving parts.

## Gorilla, and the bit-width footgun

Gorilla’s value path XOR-encodes successive floats and packs **leading**/**trailing** zero counts of the XOR into a small bit field. Textbook stuff — until you realize **five bits** only express **0–31**, while `bits.LeadingZeros64` can return **63** for non-zero XOR words. If you blindly write five bits, you **silently alias** distinct states. The paper warns about edge cases; you can still **read past** them and ship a bug.

The fix is not intellectually impressive: **clamp** or **widen** the field and add **tests that stress high-bit patterns** — monotonic counters near `2^32`, pathological XORs — the sort of inputs no one hand-writes in a happy-path table test.

Second lesson from the same neighborhood: **meaningful bit length** can be **64** when leading and trailing zero counts are both zero; you cannot stuff that into **six bits** without an **explicit encoding** agreed by encoder and decoder. Symmetric invariants beat “the decoder accidentally maps 0 to 64.”

I now treat **compression codecs** like **cryptography primitives**: assume you will get the width wrong once, and test like it.

## The label index

The almost-mistake: a **single global postings map** refreshed only at **flush** time. Writes would **not** be queryable by arbitrary labels until flush — acceptable for batch analytics, **wrong** for monitoring, where **the last scrape** must be visible **immediately**.

Helios updates **postings on every Put** in the memtable and serializes postings into SSTables at flush. The per-write overhead is tiny next to WAL I/O and compression.

## PromQL subset

Recursive-descent **parse** plus a small **evaluator** covering the practical surface: matchers (`=`, `!=`, `=~`, `!~`), ranges, **`rate` / `*_over_time`**, **aggregates** with `by` / `without`, and **binary ops** on aligned vectors.

**Out of scope on purpose:** subqueries, histogram quantiles, `@` offsets, most of the math zoo. That is weeks of work each, with diminishing returns for the story I wanted to tell.

## Anomalies

Per-series **EWMA** mean and variance, z-score against a threshold, SSE events. The product detail that mattered as much as the statistics: **bound the registry** (LRU + staleness). **Reject non-finite** samples at ingestion so one `NaN` cannot poison forever.

Endpoint: **`GET /api/v1/anomalies`** (Server-Sent Events).

## Raft and snapshots

**Consensus** is **`hashicorp/raft`**. The FSM implements **`Apply` / `ApplyBatch`** and calls **`WriteDirect`** on the engine so replication reuses one code path.

**Snapshots** deserve their own sentence: immutable **SST files** are **`os.Link` hardlinks** into a snapshot staging directory whenever the OS allows; otherwise **copy fallback** for cross-device layouts. Raft streams **manifest + payload** instead of blindly archiving the entire data directory. Memtable contents are flushed into the snapshot when non-empty so followers are not missing “hot” tail data.

**Bootstrap:** the seed node can **bootstrap a full voter configuration** up front so followers are not orphaned as “ghosts” forever. Operational env (`HELIOS_PEERS`, `HELIOS_BOOTSTRAP`) is wired in **`cmd/helios`**. **Idempotence** matters: re-run bootstrap only when the Raft store is empty.

**Follower writes** hit **HTTP on any node**, but **non-leaders forward** `POST /api/v1/write` to the leader’s HTTP address learned from peer metadata.

For **election practice**, run **Compose**, `curl /cluster/leader` on mapped ports **8080 / 8081 / 8082**, **`docker kill`** the leader container, watch the survivors re-elect — then **measure** latency on **your** hardware before bragging.

## What I deliberately did not build

- **Multi-shard cluster** beyond one replicated group.  
- **Production security** as “pretend mTLS” — document what you would add instead.  
- **Full PromQL** compat.  
- **Durable mmap block cache** beyond the in-process LRU.

## Numbers (replace with yours)

Use the **README** table and **TSBS** scripts. Report **sustained ingest**, **query latency bands**, **compression ratio**, **failover seconds** only after you reproduce them — laptop thermals matter.

Template you can paste after running benchmarks (from the PDF’s structure):

| Metric | Your value |
|--------|----------------|
| Sustained ingest (single node) | |
| Sustained ingest (3-node, leader) | |
| Cold query `rate(...[5m])` | p50 / p99 |
| Hot query (warm cache) | p50 / p99 |
| Gorilla compression vs raw float64 | ratio |
| Resident memory at chosen TSBS scale | |
| Leader failover (Compose kill test) | seconds |

## Encoding details worth knowing (even if you skip the paper)

**Timestamps** in Gorilla’s scheme: first stamp in full; second as a delta; thereafter **delta-of-delta** so evenly spaced scrapes often cost **a bit**. **Values**: first float verbatim; then XOR with the previous float; classify the XOR by **leading** and **trailing** zero runs so almost-identical values compress to a handful of bits. That is why a five-bit field for “how many leading zeros” is not a cosmetic mistake — it is a **semantic** one.

## EWMA anomalies (pseudocode level)

Per series, maintain exponential moving estimates of mean and variance, update on each sample, compare `|z|` to a threshold after warm-up. The **statistics** are textbook; the **systems** lesson is bounding **detector count** and rejecting **NaN**/**Inf** so one bad client cannot corrupt state forever.

## If I started again

- **Wider / generative tests** earlier for codecs.  
- **Smaller commits** in the middle of the project (easier bisect).  
- **Skip throwaway formats** — if postings are architectural, ship them **before** painting yourself into an O(N) query corner.

## Closing

Helios is **done enough to show**: tests, compose, README numbers, Raft wiring, anomaly path. The interesting part was never “I used a library” — it was **where systems projects actually fail**: **bit widths**, **index freshness**, **snapshot economics**, **scope control**.

Ship the artifacts. Point the camera at the terminal. Write the next thing.

— *End draft (~3k words target; trim for your venue).*
