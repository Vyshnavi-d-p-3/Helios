# Interview prep — anticipated questions (Helios)

Aim for **60–90 seconds** spoken per answer. These are aligned with the current implementation; adjust if you change defaults (e.g. `max_series_per_metric`).

---

### “Why LSM and not a B-tree?”

Time-series ingestion is append-mostly with roughly monotonic timestamps; you rarely rewrite old points. **LSM** batches writes into a **memtable**, flushes immutable **sorted runs**, and merges in the background — turning random logical writes into large sequential writes. Read cost grows with more SST layers, but **label postings** narrow candidates first and the **block cache** pays for hot reads. **B-trees** shine when you update existing keys in place; that is not Helios’s dominant pattern.

---

### “Walk through killing the Raft leader.”

Followers stop seeing **AppendEntries / heartbeats**. After an **election timeout**, a follower increments the term and starts an election; peers grant votes; the new leader establishes authority. Writes that hit **Raft.Apply** during partition or election may fail until a stable leader exists; our HTTP layer returns **503**-style errors when forwarding or applying fails (**no leader**). **Reads** stay local by design.

In manual **Docker Compose** checks, querying **`/cluster/leader`** on survivors after **`docker kill`** typically shows a new leader within **~1–3s** on a laptop — **measure your own setup** before quoting a number.

**Split votes** resolve via randomized timeouts; **minority partitions** cannot commit new entries.

---

### “Why hand-roll PromQL?”

**(1)** Learning and control over evaluation semantics next to **your** storage layer.  
**(2)** Dependency surface: importing Prometheus’s evaluator often drags TSDB internals you do not want intertwined with Helios types.

For a shipping product next to Grafana, **vendoring Prometheus’s PromQL** is reasonable; Helios deliberately implements a **subset**.

---

### “How do you handle cardinality?”

**Hard cap:** configurable **max series per metric** (`internal/engine` cardinality check before commit). Breaches reject the batch (**HTTP 503** today with an explanatory body — not HTTP 429).

**Soft ops:** Prometheus metrics include **`helios_ingest_rejected_total{reason="cardinality"}`** so you can alert on rejects.

Not built here: multi-tenant per-tenant caps and automatic label sanitization beyond the cardinality gate.

---

### “How do snapshots work?”

Raft asks the FSM for **`Snapshot()`**. Helios captures **immutable SST paths** via **hardlinks** where possible (fast, shared blocks), streams a **manifest + files**, and restores by laying down files and reopening tables. Active memtable data is flushed into the snapshot directory so followers do not miss recent writes. **Cross-device** hardlinks fall back to **copy** (same idea as `cp`).

Naively **tarring the entire data directory** for every snapshot is the failure mode this avoids at scale.

---

### “How does EWMA anomaly detection work?”

Per-series **EWMA mean and variance**, z-score vs **threshold** after warm-up. Non-finite samples are rejected so they cannot poison state. The **registry** is **LRU-bounded** with stale eviction — unbounded maps are an OOM hazard.

Events stream over **SSE** at **`GET /api/v1/anomalies`**.

EWMA misses slow drift / seasonality; it is intentionally a **baseline**, not ML.

---

### “What’s the worst bug you chased?”

*(Your story.)* **Example:** Gorilla-style XOR encoding uses **fixed bit widths** for leading-zero counts; values **≥ 32** need **clamping or wider fields** — otherwise encodings corrupt silently. **Property-style encode/decode tests** find edge cases human fixtures miss.

---

### “What did you deliberately not build?”

Examples that match the README / ADRs: **multi-shard sharding**, **full PromQL**, **production auth/TLS story as shipped code**, deeper **compaction tiers** beyond L0-style merges, **mmap-persistent block cache**. Say **why** (scope, signal, time).
