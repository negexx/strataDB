# Phase 2 Query and Usability Audit

**Date:** 2026-08-13
**Status:** Phase 2 audit complete: implemented within named bounds; no open P0/P1 runtime defect confirmed
**Next phase:** Query and usability

This audit records the independent Sol review performed after the Phase 1 closeout work. It is a
design and triage record, not an exit claim for Phase 1. D0 has since been approved for bounded
implementation; this document remains the controlling scope and contract record for Phase 2.
The historical Phase 1 status sentence above is superseded by the current Phase 1 closeout ledger;
it is retained only as review history.

## Scope and controlling sources

The current audit result supersedes the historical intake wording above: Phase 2 is implemented
within the documented single-process/shared-handle boundary. Cross-process coordination and
serializability are not Phase 2 work and remain reserved/non-claimed for Phase 4.

The review covered the current source tree and these canonical documents:

- [`AGENTS.md`](../../../AGENTS.md)
- [`architecture.md`](../../architecture.md)
- [`design.md`](../../design.md)
- [`decisions.md`](../../decisions.md)
- [`roadmap.md`](../../roadmap.md)
- [`status.md`](../../status.md)
- [Phase 1 audit](../phase-1/audit.md)

The supported boundary remains an embedded, single-node database used by one process through a
shared `Dataset` handle, with immutable snapshot reads and write-write OCC. Phase 2 must not imply
a read/write transaction API, distributed coordination, full SQL, or lifecycle
reclamation.

## Current-state superseding note (2026-08-15)

The original Phase 2 scope and its later **Explicit non-goals** subsection are retained below as
historical audit evidence; they do not override the checked-in current state. Since that audit,
Strata has implemented bounded transaction-base reads (scan/predicate and group operations with a
private overlay, plus existing-base-row lookup) and the one named deterministic
`add_nullable_column` migration. Those additions remain narrower than a general read/write
transaction interface: staged inserts still have no pre-commit physical row ID, vector search does
not merge a staged overlay, and the isolation ceiling remains snapshot isolation rather than
serializability. The Python and CLI surfaces expose stable bounded contracts with documented
packaging/runtime limits, not an "awaiting integration" status. [`status.md`](../../status.md),
[`roadmap.md`](../../roadmap.md), and [`architecture.md`](../../architecture.md) are the controlling
current-state references.

## Findings

### P0 â€” Stable query/schema API (resolved for the approved Phase 2 slice)

The typed dataset-owned query contract is now implemented in `crates/txn/src/query.rs`, with
explicit schema/reserved-column rules and requests bound to an immutable `Snapshot`. Client
surfaces must use this contract rather than constructing raw Arrow query shapes.

### P0 â€” Duplicate openers can leave the supported concurrency boundary

Multiple `Dataset::open` calls can create independent locks, allocators, and commit-log views. The
Phase 2 facade must explicitly reject or constrain duplicate opens and CLI concurrent writers, or
clearly retain the one-shared-handle precondition. Durable coordination between independent
openers remains Phase 4 work.

### P1 â€” Phase 2 design and implementation status

The approved D0 design uses separate typed scan, lookup, group-by, and vector requests/results,
remaining narrower than DataFusion or full SQL. T1-T5 implement and test the Rust contract; T6 and
T7 implement the Python and CLI surfaces, while T8 covers cross-surface integration and closeout.

### P1 â€” Projection and filtering need an explicit internal-column contract

The internal scan path needs `_row_id` to filter tombstoned physical rows. The supported path should
read the union of requested output columns, predicate columns, and `_row_id`; apply visibility and
predicate filtering; then remove internal columns before returning the result. Projection must not
leak `_row_id` or permit reserved-name collisions.

### P1 â€” Point lookup contract (resolved for the approved Phase 2 slice)

Point lookup is snapshot-bound and uses physical `RowId` identity. The approved contract specifies
never-allocated/not-found, tombstoned, vectorless, projection, dictionary, and typed engine-error
behavior; the T3 implementation covers those cases.

### P1 â€” Group-by aggregate surface (resolved for the approved Phase 2 slice)

The approved group-by surface defines null handling, numeric precision, ordering, empty-input behavior,
and mergeable typed partial accumulators. T4 implements and tests those semantics.

### P1 â€” Vector search semantics (resolved for the approved Phase 2 slice)

The approved vector surface defines the public result type, squared-L2 units, underfilled-`k`
behavior, filtering, dimensions, RowId tie ordering, and typed hydration/error behavior. T5 and the
CLI search path implement those semantics without silently dropping unresolved IDs.

### P1 â€” Python and CLI surfaces

The approved Python contract returns Arrow IPC stream bytes for tabular results, typed exceptions,
and releases the GIL around blocking engine work. T6 implements this contract. The approved Phase 2 CLI contract uses the typed
facade for `query-scan`, `lookup`, `group-by`, and non-exact `search`, with deterministic line output
(`query-scan` uses result row indexes because physical `_row_id` is reserved and excluded from scan
projections), and `--ack-single-writer` required for every mutating command. The pre-existing `scan`, `filter`,
`inspect`, and `explain` commands remain compatibility-only MVP inspection commands; they are not
promoted as generic schema APIs or broader supported guarantees.

### P2 â€” Narrow-read and layering claims need evidence

Projection I/O is now isolated by an opt-in, thread-local accounting seam: the current scan path
records exactly the requested projection, filter-only columns, and `_row_id`, with focused native
evidence. Sub-file/row-group pruning remains deferred because each current Arrow data file contains
one record batch and only whole-file statistics. An additive indexed row-group container now
supports explicit selected-group projection reads; automatic predicate-to-group pruning is not
claimed. Public layers still expose subordinate
storage/query/index types; removing that leakage requires an additive facade DTO design and a
separate compatibility/deprecation decision.

### P3 â€” Remove stale source references

Comments and internal references to retired phase material should be removed or rewritten to point to
the canonical architecture and decision documents.

## Recommended task decomposition

1. **D0 â€” Sol design:** define the typed snapshot-bound query facade, schema ownership, reserved
   columns, result/error contracts, and duplicate-opener boundary.
2. **G1 â€” Phase 1 prerequisite:** complete the remaining Phase 1 CI, portability, and evidence gates.
3. **T1 â€” Facade nucleus:** implement the approved typed snapshot-bound request/result types.
4. **T2 â€” Projection and predicates:** implement internal-column handling, visibility filtering, and
   projection tests.
5. **T3 â€” RowId lookup:** implement and test snapshot-bound physical-row lookup semantics.
6. **T4 â€” Group-by:** add mergeable aggregate state and explicit null/precision/order contracts.
7. **T5 â€” Vector contract:** define public search results, distance units, filtering, and hydration.
8. **T6 â€” Python:** implement typed bindings, exceptions, Arrow conversion, GIL handling, and CI.
9. **T7 â€” CLI:** replace hardcoded demo behavior with the approved facade and integration tests.
10. **T8 â€” Integration:** run cross-surface regression, docs, and bounded performance verification.

## Explicit non-goals

Phase 2 does not include read/write transactions, full SQL, compaction or GC,
schema migration, branches, object storage, additional ANN families, or
agent-memory features.

## Current audit disposition

No open P0/P1 runtime defect was confirmed. Remaining findings are P2 evidence/API-boundary
improvements and P3 documentation cleanup. The all-null `Min`/`Max`/`Avg` aggregate edge is
implemented with nullable result cells and regression coverage. Projected-read, shared-reader
fairness/RSS, and packaged-wheel smoke evidence paths are now implemented; their cloud results
remain evidence rather than universal product guarantees.

## Terra readiness

D0 is approved. T1-T7 are implemented with focused evidence and independent Terra approvals. T8
integration gates pass: workspace tests, check, clippy, format, diff, stale-claim, and relative-link
verification are green. The final Sol branch review approved the current bounded implementation.
Phase 2 is implemented within these named bounds. Exact-head GitHub Actions evidence is recorded in
the companion verification report; local native runtime execution remains unavailable on hosts
without the MSVC linker.
