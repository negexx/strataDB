# FUTURE — deferred and refused scope

This is a short index of deferred and refused product scope. Current implementation status and phase ordering live in the [status ledger](status.md) and [roadmap](roadmap.md); this file does not override them. Historical rationale remains in [Scope Addendum v2](scope-addendum-v2.md), [ADR 0008](decisions/0008-adopt-segmented-index-layout.md), and [documentation history](history/README.md).

## Already implemented or partially implemented

- Immutable manifest-listed HNSW segments are the active index layout; see [ADR 0008](decisions/0008-adopt-segmented-index-layout.md).
- Per-file and per-segment statistics support predicate pruning. The existing query primitives are partial; no planner-integrated query engine is implied.

## Deferred capability work

| Capability | Roadmap phase | Current boundary |
|---|---|---|
| Compaction, vacuum, and orphan cleanup | 3 | No reclamation or segment-count bound is implemented. |
| Schema catalog, migration compatibility, point lookup, time travel, stable query API | 2–3 | Current Arrow batches/manifests do not form a complete catalog or migration layer. |
| Independent-open and cross-process coordination | 4 | Shared-handle locking is not a durable conditional-publication protocol. |
| Fork, abort, branch reads, and merge | 5 | Immutable segments are a prerequisite, not a delivered branching feature. |
| Object-store backend and deployment | 6 | `LocalFs` is the implemented backend. |
| Usable Python API and stable administration CLI | 2 | Bindings are a `placeholder_version` module; CLI commands are fixed-shape demo/inspection tooling. |
| Verifiable deletion, staleness tracking, and budget-shaped ANN controls | Later product work | These depend on lifecycle and API foundations. |

## Refused for the current product

- Distributed or multi-node transactions.
- Full SQL parser/optimizer and a distributed query planner.
- Additional ANN families beyond the current HNSW implementation.
- Automatic conflict resolution or a stronger isolation level without a superseding ADR.
- A derivation engine, belief semantics, or agent memory product embedded in the database engine.

These boundaries are deliberate. New work should be proposed against the [roadmap](roadmap.md), while current source and [status ledger](status.md) remain authoritative for claims about what Strata does now.
