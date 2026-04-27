# ADR 0005: Raft-backed replication with leader forwarding

**Status:** Accepted  
**Date:** 2026-04-27

## Context

Helios needs durable multi-node ingestion without redesigning the storage engine around a separate distributed log product. Operators should be able to run three replicas, send writes to any HTTP endpoint, and rely on consensus for ordering.

## Decision

- Use **hashicorp/raft** with **BoltDB-backed** log/stable stores and **file snapshots**.
- Encode replicated write batches as **prompb.WriteRequest** bytes (same logical samples as remote write).
- Apply commits through **`engine.WriteDirect`** on all nodes so WAL/memtable/SST semantics stay centralized.
- Capture state with **engine-local FSM snapshots** (hardlink SSTables where possible; length-prefixed manifest + files).
- **Leader forwarding**: non-leaders proxy `POST /api/v1/write` to the leader’s HTTP URL derived from peer configuration (`id@host:raft:http` or `id=host:raft` with default HTTP `:8080`).
- **Membership**: bootstrap seed includes full voter set; **`POST /cluster/join`** on the leader adds voters at runtime.

## Alternatives considered

- **Kafka/Pulsar as external log**: stronger ecosystem, but adds operational dependencies and does not reuse Helios WAL semantics without a large ingest rewrite.
- **Gossip-only replication**: simpler wiring, weaker durability/consensus guarantees than needed for portfolio demonstration.

## Consequences

- Single replicated shard only; no automatic data partitioning across Raft groups.
- Followers accept identical datasets; capacity scales by read fan-out and hardware, not write sharding.
- Correct operation depends on accurate **advertised Raft and HTTP addresses** between containers or hosts.
