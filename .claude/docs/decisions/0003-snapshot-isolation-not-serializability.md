# ADR 0003 — Snapshot isolation, not full serializability

**Status:** Accepted
**Date:** 2026-07-15

## Context

The transaction/conflict layer (Phase 6) is Strata's flagship subsystem. Full serializability is the strongest isolation level a transactional system can offer, but it is also the hardest to implement correctly — especially layered on top of a mutable vector index — and Strata is a solo, ~2.2-year project.

## Decision

Guarantee snapshot isolation as the v1 isolation level: every transaction sees a consistent point-in-time view across both the row store and the vector index, as of the manifest version it started against. Full serializability is an explicit non-goal for v1.

## Alternatives considered

- **Full serializability:** rejected as a research-grade problem on top of a mutable vector index — the added implementation and verification cost is large, and snapshot isolation already covers the target use cases (multi-agent shared memory, concurrent tool calls, live RAG ingestion, pinned-version training reads) described in the spec.
- **Weaker levels (read-committed, eventual consistency):** not seriously considered — these are exactly the guarantees existing vector stores already offer badly (Section 2 of the spec), and matching them would abandon Strata's actual differentiator.

## Consequences

- Positive: OCC + row/key-range conflict detection is tractable to implement and to verify with Phase 7's deterministic-simulation harness within the project's timeline.
- Negative: write skew and certain serializability anomalies are possible under snapshot isolation — callers relying on Strata for invariants that require serializability (e.g. certain multi-row constraints) can be surprised if they don't know the isolation level. This must be documented prominently in client-facing docs, not just here.
- Neutral: this only becomes revisitable once Phase 7's chaos suite is fully green — no earlier code, however well-intentioned, should try to sneak in serializability-only machinery.

## How to revisit

Revisit only after Phase 7's chaos suite is fully green (per the spec's own Non-Goals table). Write a new ADR proposing serializability as a v2 addition; don't edit this one.
