# ADR 0004: EWMA anomaly detection in write path

**Status:** Accepted  
**Date:** 2026-04-27

## Context

Helios needs lightweight anomaly signaling without introducing external stream processors.
Detection must run inline on writes and remain bounded in memory.

## Decision

Use per-series EWMA mean/variance detectors:
- compute z-score on each new value
- emit events above threshold
- maintain detector state in bounded LRU + stale eviction registry
- expose stream via SSE endpoint

## Alternatives considered

- **Deep-learning based detectors**: stronger modeling potential but operationally heavy and not explainable in this scope.
- **Batch/offline anomaly jobs**: simpler runtime, but no near-real-time signal.

## Consequences

- Cheap, explainable anomaly events with minimal write-path overhead.
- Some false positives during warmup or regime changes are expected and accepted for this phase.
