# ADR 0003: Label postings index

**Status:** Accepted  
**Date:** 2026-04-27

## Context

Query paths need to answer "series matching label filters" efficiently.
Scanning all series by metric for every filter causes latency spikes as cardinality grows.

## Decision

Store an inverted postings index keyed by `(label_name, label_value)` to candidate series identifiers.
Use exact-match postings for narrowing first, then apply non-equality matcher filtering.

## Alternatives considered

- **No index (full scan)**: simple but does not scale beyond small test datasets.
- **Roaring bitmap postings**: potentially faster set operations at larger scale, more implementation complexity for this stage.

## Consequences

- Big win for common `label=value` filters and metadata endpoints.
- Requires index maintenance during compaction and retention pruning.
