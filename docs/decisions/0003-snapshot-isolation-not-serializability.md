# ADR 0003 — Snapshot isolation, not full serializability

**Status:** Accepted
**Date:** 2026-07-15

## Context

Full serializability is stronger than the isolation boundary needed for Strata's intended
multi-agent use cases, and substantially harder to implement and verify around vector-index
publication. The project therefore needs a clear ceiling without pretending that the current API
already exposes a full transactional read interface.

## Decision

The v1 design ceiling is snapshot isolation, not full serializability. The intended complete
transaction model is a consistent point-in-time view across row data and vector-index state, with
write conflicts surfaced rather than silently resolved.

The current implementation is narrower: `Dataset::snapshot()` provides immutable snapshot reads,
while `Transaction` is a write-only surface with write-write OCC. It does not provide transactional
scan/search or read-your-own-writes. This ADR is therefore a design ceiling and does not upgrade the
current API beyond the behavior recorded in [`docs/status.md`](../status.md).

## Alternatives considered

- **Full serializability:** rejected for v1 because write skew and other serializability machinery
  would add substantial implementation and verification cost without being required by the target
  product boundary.
- **Read-committed or eventual consistency:** rejected because those levels would weaken the
  coherent snapshot behavior that distinguishes Strata's intended design.

## Consequences

- Positive: the project can focus on write-write OCC, immutable snapshots, and atomic row/index
  publication within the supported shared-handle boundary.
- Negative: write skew and other serializability anomalies remain possible; callers requiring
  serializable multi-row constraints must not infer them from this ADR.
- Current limitation: the Phase 1 audit found correctness and durability blockers inside the current
  shared-handle path. Those findings must be closed or explicitly bounded before the target contract
  can be described as fully verified.

## How to revisit

Revisit only through a new ADR after the correctness and durability baseline is verified. Do not edit
this decision to silently add serializability or to imply a full read/write transaction API.
