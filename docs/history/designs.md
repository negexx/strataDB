# Historical designs and plans

This file replaces the old design and phase-plan tree with a compact chronology. The entries are
historical and may describe mechanisms that were later removed.

## Phase 0 transaction and format specification

The Phase 0 specification established Arrow IPC files, JSON manifests, physical row IDs, tombstones,
write-write OCC, commit locking, and immutable snapshots. It remains useful as rationale, but current
implementation limits and Phase 1 blockers are recorded in [design](../design.md) and the [audit](../audit/phase-1/audit.md).

## Phase 2 and 3 query plans

The query plans proposed dictionary encoding, statistics, compound pruning, group-by, filtering, and
query refinement. The implemented subset is now summarized in `docs/design.md`; schema ownership,
stable APIs, point lookup, planner integration, and lifecycle work remain roadmap items.

## Phase 4 mutable-index material

The old Phase 4 vector specification and W1-W5 plans described a mutable graph and append-only delta
log. PR #31 introduced the segment abstraction; PR #33 completed the immutable segment write/load
cutover. The mutable graph/delta-log path is retired and must not be used as current guidance.

## Phase 5 MVCC and later scope

The Phase 5 MVCC/snapshot-isolation proposal explored broader read/write semantics and cross-process
coordination. Current Strata is narrower: immutable snapshots plus write-write OCC within one shared
handle. Cross-process coordination is Phase 4 of the active roadmap; serializability remains refused.

## Scope amendments

Scope Addendum v1 and the Phase 4 implementation plans documented branching, merge, object storage,
and operational lifecycle ideas. Scope Addendum v2 refined the boundary and explicit refusals. The
current roadmap is the concise source for what is deferred, later, or refused.
