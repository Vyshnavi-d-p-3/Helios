# Label Index Design

This document specifies the inverted label index that Helios v2 adds and v1
omitted. Without it, queries like `http_requests_total{status="500"}` are O(N)
in the total series count for the metric. With it, they are O(log N + matching).

This is the single most important architectural decision in a TSDB. Prometheus,
VictoriaMetrics, and InfluxDB IOx all build essentially this structure. Skipping
it is the difference between "a key-value store with timestamps" and "a TSDB."

---

## The problem

Helios v1 indexes data as:

    metric_name → [series_index_entries] → [Gorilla blocks]

A query for `http_requests_total{status="500"}` resolves the metric name in
O(log N) but then must linearly scan every series with that metric to filter
labels. A metric with 10 K active series (typical for a busy service) requires
10 K Bloom-filter checks and label comparisons per query. At p99 query latency
< 100 ms with 50 SSTables, this never works.

What real PromQL queries look like:

    rate(http_requests_total{status="500", method="GET"}[5m])
    sum(rate(latency_p99{env="prod"}[5m])) by (service)
    {__name__=~"helios_.*", node="helios-1"}

All require fast set operations over label values, not series scans.

---

## The data structure

For each SSTable we add a **postings index** that maps `(label_name,
label_value)` → sorted list of series IDs (uint64) within that SSTable:

    postings: map[string][]uint64
      key:   "label_name=label_value"  (e.g., "status=500")
      value: sorted []uint64           (series IDs in this SSTable)

Plus a **label-value index** that lets us answer `=~` (regex) and `!=`
(negation) queries:

    label_values: map[string][]string
      key:   "label_name"              (e.g., "status")
      value: sorted distinct values    (e.g., ["200", "404", "500"])

Series IDs are local to the SSTable. The series index already maps `seriesID
→ (metric, labels, gorilla_block_offset)`, so once we have a list of IDs we
can find their data.

---

## Query resolution

For `http_requests_total{status="500", method="GET"}`:

1. For each SSTable whose time range overlaps the query window:
   1. Look up postings for `__name__=http_requests_total` → set A
   2. Look up postings for `status=500` → set B
   3. Look up postings for `method=GET` → set C
   4. Compute A ∩ B ∩ C (sorted-list intersection — linear in min size)
   5. For each series ID in the intersection, decode its Gorilla block

Set intersection of sorted uint64 lists is straightforward and fast (~1 ns per
element with branch-free merge). For the typical case where one matcher is
highly selective (`__name__=http_requests_total` returns ~5 K IDs, `status=500`
returns ~50), the intersection is dominated by the smaller set's size.

For regex matchers (`status=~"5.."`):

1. Look up label-values for `status` → ["200", "404", "500", "502", "503"]
2. Filter values matching the regex → ["500", "502", "503"]
3. Take the union of postings for each match → set
4. Intersect with the rest of the matchers as above

---

## On-disk layout (extension to SSTable format v2)

The v1 SSTable format gets two new sections, placed before the Bloom filter:

    ┌─────────────────────────────────────────────────┐
    │ Header                                          │
    ├─────────────────────────────────────────────────┤
    │ Data Blocks (Gorilla-encoded, per series)       │
    ├─────────────────────────────────────────────────┤
    │ Series Index                                    │
    ├─────────────────────────────────────────────────┤
    │ Postings  ← NEW in v2                           │
    │   count: uint32                                 │
    │   per entry:                                    │
    │     keyLen: uint16, key: bytes  (e.g. "status=500")
    │     idCount: uint32                             │
    │     ids: []uint64  (sorted, delta-encoded)      │
    │   sorted by key for binary search               │
    ├─────────────────────────────────────────────────┤
    │ Label Values  ← NEW in v2                       │
    │   count: uint32                                 │
    │   per entry:                                    │
    │     nameLen: uint16, name: bytes                │
    │     valCount: uint32                            │
    │     vals: [](valLen: uint16, val: bytes)        │
    │   sorted by name                                │
    ├─────────────────────────────────────────────────┤
    │ Bloom Filter                                    │
    ├─────────────────────────────────────────────────┤
    │ Footer (now records offsets of new sections)    │
    └─────────────────────────────────────────────────┘

Footer fields added:

    PostingsOffset:    int64
    LabelValuesOffset: int64

The footer's magic+version bumps from 1 to 2. Readers detect old SSTables by
the version byte and fall back to the v1 (linear-scan) query path. This is the
schema-evolution story v1 also lacked.

### Postings encoding details

Series IDs are stored delta-encoded with variable-length integers (Go's
`encoding/binary.PutUvarint`). For a posting list with 10 K consecutive IDs
this is ~1 byte per ID instead of 8 — important because postings dominate
SSTable size for high-cardinality metrics.

For very long posting lists (>10 K IDs) we additionally write a sparse
"skip index" every 1 K entries: `(target_id, byte_offset_within_postings)`.
This lets intersection algorithms binary-skip ahead instead of decoding
every uvarint.

---

## Memtable-side: live in-memory index

The on-disk index is built when the memtable flushes. While the memtable is
active we maintain an **in-memory postings index** alongside it:

    type MemtableIndex struct {
        mu       sync.RWMutex
        postings map[string]*roaring.Bitmap  // key=val → series ID set
        labelVals map[string]map[string]struct{}  // name → set of values
    }

We use `github.com/RoaringBitmap/roaring` for the in-memory bitmaps — its
union/intersect operations are 10-100× faster than naive sorted-list ops at
the cost of slightly higher memory. On flush, the bitmaps are converted to
sorted varint-encoded lists for the on-disk format.

Why bitmaps in memory but sorted lists on disk: bitmaps optimise for
intersection speed, sorted lists optimise for storage size. Memtables are
read-heavy and small; SSTables are storage-bound and read in bulk.

---

## Query planner integration

The PromQL executor's path from AST to data changes as follows:

    AST (MetricSelector with Matchers)
        ↓
    For each (label_name, matcher) in the selector:
        ↓
    Compute candidate posting set per SSTable
        ↓
    Intersect across matchers (per-SSTable)
        ↓
    Resolve series IDs → series metadata (existing series index)
        ↓
    Decode Gorilla blocks for the resulting series
        ↓
    Apply time filter to decoded samples
        ↓
    Return to query executor

The intersection step is where the speedup comes from. For a query that
previously decoded 10 K Gorilla blocks, the postings approach decodes ~50.
Decode cost dominates query latency, so this is a real ~200× speedup on
high-cardinality queries.

---

## Cardinality control hooks here

Because the postings index sees every new series at write time, this is the
natural place to enforce per-metric cardinality limits:

    func (idx *MemtableIndex) Insert(seriesID uint64, labels Labels) error {
        metricName := labels["__name__"]
        // O(1) lookup of current series count for this metric
        if cardinality(metricName) >= maxSeriesPerMetric {
            return ErrCardinalityExceeded
        }
        // ... insert into postings
    }

Without an index, cardinality enforcement requires scanning the entire
series list per write — too expensive for the hot path. With an index, the
count is essentially free (it's already computed for query planning).

---

## What this costs

- **Memory:** ~24 bytes per series per active label per SSTable in memory
  (RoaringBitmap overhead + label-value map entries). For 100 K series with
  10 labels each, that's ~24 MB per SSTable in memory. Acceptable.
- **Disk:** posting lists + label values typically add 5–15 % to SSTable size.
- **Write latency:** ~200 ns per sample to update bitmaps + maps. At 500 K
  samples/sec that's 100 ms/sec of CPU on the write path — visible but
  manageable.
- **Code complexity:** ~600 LOC to add. The query planner, SSTable format,
  memtable, and compaction all need to know about postings.

---

## Build sequencing

Add this in **Phase 2.5** between query engine and distribution:

1. (1 day) MemtableIndex with in-memory bitmaps.
2. (2 days) Postings + label-values sections in SSTable format v2.
3. (1 day) Compaction merges postings (sorted-list union with deduplication).
4. (2 days) Query planner uses postings for matcher resolution.
5. (1 day) Cardinality enforcement at the memtable index.
6. (1 day) Tests: query correctness with real label matchers, fallback to v1
   format SSTables, cardinality rejection paths.

This is the single highest-leverage week of work in the project. Skip it and
you have a toy. Build it and you have something a Stripe/Snowflake/Databricks
infra interviewer will spend the whole loop asking about.

---

## What this does *not* solve

- **Cross-SSTable query optimisation.** A query that hits 50 SSTables must
  do 50 intersections. A global "manifest index" that summarises which
  SSTables can possibly match a label predicate would prune most of them.
  Stretch goal.
- **Schema-on-read for changing label sets.** If a series adds a new label
  partway through its life, postings get fragmented. Real systems handle this
  with a "head block" or by treating each unique labelset as its own series.
  Helios should choose the latter (simpler).
- **Distributed postings.** Across shards, each shard has its own postings.
  Cross-shard queries fan-out and merge. Fine for v1 of distribution.
