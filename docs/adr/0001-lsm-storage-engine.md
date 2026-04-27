# ADR 0001: LSM storage engine

**Status:** Accepted  
**Date:** 2026-04-27

## Context

Helios needs durable ingest and predictable write performance under append-heavy time-series traffic.
Point updates are rare, reads are mostly range scans by series key plus label-based narrowing.

## Decision

Use an LSM-style layout:
- WAL for durability before ack
- memtable for fast writes
- immutable SSTables for persistence
- merge-on-read and periodic compaction

## Alternatives considered

- **B-tree storage**: better point updates, but higher write amplification and less natural for append-only TS workloads.
- **Columnar batch files only**: good scan throughput, weaker incremental durability story without WAL + replay.

## Consequences

- Strong write path simplicity and crash recovery.
- Requires compaction and retention machinery to control read amplification.
