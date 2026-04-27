# ADR 0002: Gorilla compression for time-series blocks

**Status:** Accepted  
**Date:** 2026-04-27

## Context

Helios stores numeric samples where adjacent timestamps and values are usually correlated.
Raw float64 + int64 storage is expensive for retention goals.

## Decision

Use Gorilla-style encoding:
- delta-of-delta timestamp encoding
- XOR-of-float64 value encoding with leading/trailing zero windows

The implementation explicitly clamps leading zeros to 5-bit range and maps meaningful=64 to 0.

## Alternatives considered

- **General-purpose block compression (zstd/gzip)**: easier integration, weaker point-level decode behavior.
- **Integer-only codecs**: inapplicable to general float series without lossy conversion.

## Consequences

- Good compression for smooth metrics with cheap decode in Go.
- Codec correctness is subtle; requires focused roundtrip tests for edge windows.
