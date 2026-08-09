# Phase 2 Query and Usability Audit

**Date:** 2026-08-03
**Status:** Phase 2 implemented within named bounds; D0 approved and T1-T8 integration gates passed
**Next phase:** Query and usability

This audit records the independent Sol review performed after the Phase 1 closeout work. It is a
design and triage record, not an exit claim for Phase 1. D0 has since been approved for bounded
implementation; this document remains the controlling scope and contract record for Phase 2.
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

### P0 — Stable query/schema API (resolved for the approved Phase 2 slice)

The typed dataset-owned query contract is now implemented in `crates/txn/src/query.rs`, with
explicit schema/reserved-column rules and requests bound to an immutable `Snapshot`. Client
surfaces must use this contract rather than constructing raw Arrow query shapes.

### P0 — Duplicate openers can leave the supported concurrency boundary

Multiple `Dataset::open` calls can create independent locks, allocators, and commit-log views. The
Phase 2 facade must explicitly reject or constrain duplicate opens and CLI concurrent writers, or
clearly retain the one-shared-handle precondition. Durable coordination between independent
openers remains Phase 4 work.

### P1 — Phase 2 design and implementation status

The approved D0 design uses separate typed scan, lookup, group-by, and vector requests/results,
remaining narrower than DataFusion or full SQL. T1-T5 implement and test the Rust contract; T6 and
T7 implement the Python and CLI surfaces, while T8 covers cross-surface integration and closeout.

### P1 — Projection and filtering need an explicit internal-column contract

The internal scan path needs `_row_id` to filter tombstoned physical rows. The supported path should
read the union of requested output columns, predicate columns, and `_row_id`; apply visibility and
predicate filtering; then remove internal columns before returning the result. Projection must not
leak `_row_id` or permit reserved-name collisions.

### P1 — Point lookup contract (resolved for the approved Phase 2 slice)

Point lookup is snapshot-bound and uses physical `RowId` identity. The approved contract specifies
never-allocated/not-found, tombstoned, vectorless, projection, dictionary, and typed engine-error
behavior; the T3 implementation covers those cases.

### P1 — Group-by aggregate surface (resolved for the approved Phase 2 slice)

The approved group-by surface defines null handling, numeric precision, ordering, empty-input behavior,
and mergeable typed partial accumulators. T4 implements and tests those semantics.

### P1 — Vector search semantics (resolved for the approved Phase 2 slice)

The approved vector surface defines the public result type, squared-L2 units, underfilled-`k`
behavior, filtering, dimensions, RowId tie ordering, and typed hydration/error behavior. T5 and the
CLI search path implement those semantics without silently dropping unresolved IDs.

### P1 — Python and CLI surfaces

The approved Python contract returns Arrow IPC stream bytes for tabular results, typed exceptions,
and releases the GIL around blocking engine work. T6 implements this contract. The approved Phase 2 CLI contract uses the typed
facade for `query-scan`, `lookup`, `group-by`, and non-exact `search`, with deterministic line output
(`query-scan` uses result row indexes because physical `_row_id` is reserved and excluded from scan
projections), and `--ack-single-writer` required for every mutating command. The pre-existing `scan`, `filter`,
`inspect`, and `explain` commands remain compatibility-only MVP inspection commands; they are not
promoted as generic schema APIs or broader supported guarantees.

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

D0 is approved. T1-T7 are implemented with focused evidence and independent Terra approvals. T8
integration gates pass: workspace tests, check, clippy, format, diff, stale-claim, and relative-link
verification are green. The final Sol branch review approved the current bounded implementation.
Phase 2 is implemented within these named bounds; Phase 1 remains Partial — blocked independently.
