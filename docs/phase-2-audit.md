# Phase 2 Query and Usability Audit

**Date:** 2026-08-03
**Status:** Read-only audit; no Phase 2 implementation approved
**Next phase:** Query and usability

This audit records the independent Sol review performed after the Phase 1 closeout work. It is a
design and triage record, not an exit claim for Phase 1 and not approval to begin implementation.
Phase 1 remains **Partial — blocked** until its CI, portability, and performance-evidence gates are
closed.

## Scope and controlling sources

The review covered the current source tree and these canonical documents:

- [`AGENTS.md`](../AGENTS.md)
- [`architecture.md`](architecture.md)
- [`design.md`](design.md)
- [`decisions.md`](decisions.md)
- [`roadmap.md`](roadmap.md)
- [`status.md`](status.md)
- [`phase-1-audit.md`](phase-1-audit.md)

The supported boundary remains an embedded, single-node database used by one process through a
shared `Dataset` handle, with immutable snapshot reads and write-write OCC. Phase 2 must not imply
serializability, a read/write transaction API, distributed coordination, full SQL, or lifecycle
reclamation.

## Findings

### P0 — Stable query/schema API is blocked

The current scan path accepts caller-supplied projection/schema context and validates requested
fields against the persisted dataset schema by owned name, but it does not yet provide a stable,
typed dataset-owned query contract. A supported Phase 2 query surface needs explicit schema and
reserved-column contracts, with typed requests bound to an immutable `Snapshot`. Do not make callers
depend on raw Arrow-shape details to form a supported query.

### P0 — Duplicate openers can leave the supported concurrency boundary

Multiple `Dataset::open` calls can create independent locks, allocators, and commit-log views. The
Phase 2 facade must explicitly reject or constrain duplicate opens and CLI concurrent writers, or
clearly retain the one-shared-handle precondition. Durable coordination between independent
openers remains Phase 4 work.

### P1 — A fresh Phase 2 design is required before implementation

Define a typed, snapshot-bound query facade before Terra implementation. The recommended nucleus is
a `ScanRequest`/query object covering projection, predicates, aggregation, and vector options while
remaining narrower than DataFusion or full SQL.

### P1 — Projection and filtering need an explicit internal-column contract

The internal scan path needs `_row_id` to filter tombstoned physical rows. The supported path should
read the union of requested output columns, predicate columns, and `_row_id`; apply visibility and
predicate filtering; then remove internal columns before returning the result. Projection must not
leak `_row_id` or permit reserved-name collisions.

### P1 — Point lookup lacks a complete contract

Point lookup should be snapshot-bound and use the physical `RowId` identity. The design must specify
never-allocated and tombstoned behavior, vectorless rows, routing metadata, and compatibility with
the manifest/catalog integrity rules.

### P1 — Group-by is not yet a stable aggregate surface

The current primitive is a full-batch operation without mergeable partial state. Phase 2 must define
null handling, numeric precision, ordering, empty-input behavior, and a mergeable accumulator shape
before promising grouped query results.

### P1 — Vector search semantics are incomplete

The current path uses fixed `ef_search = 32` with arbitrary `k`, returns internal `VectorMatch`
values, reports squared-L2 units, and the CLI can silently drop unresolved IDs. Phase 2 must define
the public result type, distance units, underfilled-`k` behavior, filtering, vector dimensions, and
row hydration/error behavior.

### P1 — Python and CLI surfaces are placeholders

The Python binding needs typed exceptions, a documented Arrow conversion contract, GIL-release
behavior around blocking work, and CI coverage. The CLI currently hardcodes a demo shape, has an
exact-search filtering issue, and can report a version label inconsistent with displayed rows.

### P2 — Narrow-read and layering claims need evidence

Narrow-read I/O and pruning claims are not yet isolated by counters and measured on the current
manifest-listed segment path. Public layers also expose subordinate storage/query/index types and
reserved names that should be hidden behind the supported facade.

### P3 — Remove stale source references

Comments and internal references to retired phase material should be removed or rewritten to point to
the canonical architecture and decision documents.

## Recommended task decomposition

1. **D0 — Sol design:** define the typed snapshot-bound query facade, schema ownership, reserved
   columns, result/error contracts, and duplicate-opener boundary.
2. **G1 — Phase 1 prerequisite:** complete the remaining Phase 1 CI, portability, and evidence gates.
3. **T1 — Facade nucleus:** implement the approved typed snapshot-bound request/result types.
4. **T2 — Projection and predicates:** implement internal-column handling, visibility filtering, and
   projection tests.
5. **T3 — RowId lookup:** implement and test snapshot-bound physical-row lookup semantics.
6. **T4 — Group-by:** add mergeable aggregate state and explicit null/precision/order contracts.
7. **T5 — Vector contract:** define public search results, distance units, filtering, and hydration.
8. **T6 — Python:** implement typed bindings, exceptions, Arrow conversion, GIL handling, and CI.
9. **T7 — CLI:** replace hardcoded demo behavior with the approved facade and integration tests.
10. **T8 — Integration:** run cross-surface regression, docs, and bounded performance verification.

## Explicit non-goals

Phase 2 does not include read/write transactions, serializability, full SQL, compaction or GC,
schema migration, cross-process publication, branches, object storage, additional ANN families, or
agent-memory features.

## Terra readiness

**Not ready for implementation.** Sol design approval and the Phase 1 prerequisite gates must come
first. Once D0 is approved, Luna can dispatch one bounded Terra task at a time with disjoint file
scope and independent review.
