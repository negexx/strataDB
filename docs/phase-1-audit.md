# Phase 1 Sol audit

**Date:** 2026-08-01
**Scope:** embedded local-disk engine, one process, one shared `Dataset` handle.
**Verdict:** Phase 1 is Partial and blocked.

This is the active consolidated audit. It preserves the lane finding IDs so code comments, tests, and
future fixes can still refer to the original evidence without maintaining seven overlapping reports.
Line numbers are deliberately omitted because they are unstable; use the finding ID plus current
source/tests as the anchor.

## Closeout baseline and ledger

The closeout branch is synchronized through merged PR #50,
`8cd7696fdcf34f6253fb11f9e110f6632bc872de` (`Merge Phase 1 audit remediation`). The
[Phase 1 closeout ledger](phase-1-closeout-ledger.md) is the mechanically scannable, row-per-finding
record of current state, dependencies, acceptance assertions, and the future evidence required before
this audit's verdict can change. It does not close Phase 1 or replace this audit as the controlling
finding register.

## Approved remediation design

The Phase 1 remediation keeps Strata's supported boundary at one process using one shared
`Dataset` handle. The user-approved design is intentionally strict and prospective:

- `Dataset::create` receives and persists the dataset-owned logical Arrow schema. Schema evolution,
  migrations, and logical keys remain deferred; `_row_id` and `_timestamp` remain reserved physical
  columns outside that logical schema.
- Manifests gain an explicit format/version and integrity envelope plus row-file ownership metadata.
  Recovery validates filename/payload identity, schema, row bytes, tombstones, segment metadata, and
  cross-segment row/vector ownership before constructing a `Snapshot`.
- A separate durable allocator high-water record is advanced before a row-ID claim is exposed. The
  manifest mirrors the value for diagnostics, but is not authoritative for abandoned pre-publication
  claims after restart.
- Directory synchronization and dataset creation fail closed. The supported durability claim is
  limited to named local filesystem/platform combinations where the ordered sync operations succeed;
  process-abort tests are not presented as universal power-loss proof.
- Delete/update targets must name a live physical row in the transaction's base snapshot and are
  revalidated under the commit lock. `update` is exactly one old row to one replacement row; absent,
  already-dead, malformed, and unsupported shapes have typed errors. Concurrent stale targets remain
  typed row conflicts.
- `InsufficientHistory` remains distinct from `Conflict` and carries retained-history context rather
  than inventing contested row IDs.
- `Dataset`/`Snapshot`/`Transaction` are the supported engine facade. Direct storage/index use is an
  internal implementation surface and invalidates Dataset guarantees; subordinate packages remain
  non-publishable.
- Verification adds direct counterexample regressions, transaction and cache loom gates, exercising
  chaos/checkpoint gates, and current segmented performance/boundedness evidence. Compaction,
  cleanup, cross-process coordination, authenticated tamper protection, and later API work remain
  deferred.

Datasets written without the new schema/integrity metadata are rejected with a typed
`LegacyFormatNeedsMigration` result rather than being opened under an unverified guarantee.
Checksums cover accidental/torn corruption and metadata inconsistency, not an attacker able to
rewrite both payload and checksum. Performance work records a tested operating envelope without
introducing Phase 3 compaction or lifecycle behavior.

## Implementation status after approved remediation slices

The branch implements the approved COR-01 through COR-04, CONC-01 and CONC-03, DUR-01 through
DUR-03, IDX-01 and IDX-02, and ARCH-01 through ARCH-03 slices. Completed targeted tests provide
implementation and recovery evidence for those slices; the supported
`Dataset`/`Snapshot`/`Transaction` facade is now the documented engine boundary, while subordinate
packages are marked non-publishable. Task 7 now configures exact transaction and cache loom
discovery, plus an index whole-binary loom run; checkpoint-abort and fast-chaos gates; and
scheduled/manual thorough-chaos execution.
Local targeted evidence exists, but full Linux CI execution/provenance and thorough 2000/2000 output
remain pending. Task 8 now records bounded Windows synthetic segmented/lifecycle evidence with
reproduction metadata; the optional real fixture, portable host matrix, and universal operating
bounds remain open. This does not change the audit verdict from Partial and blocked.

VER-04 through VER-06, PERF-01 through PERF-05, and the later/deferred findings remain separately
tracked unless the final review proves a dependency for a Phase 1 fix.

## Task 1 durability recovery boundary

`Dataset::create` requires its immediate parent to pre-exist as the caller's durable anchor. It
creates the dataset-owned `data/` directory, synchronizes the dataset directory and that immediate
parent, then publishes the initial manifest through `_versions/`. It does not create or synchronize
an arbitrary caller-owned ancestor chain. A retry after a pre-publication sync failure
re-synchronizes the same bounded dataset/parent chain before publication. The create call returns an
error when any ordered directory operation fails; it never reports that outcome as an acknowledged
durable creation.

The final dataset-directory sync occurs after atomic manifest publication. Consequently an error
at that boundary is uncertain: an initial manifest can be visible even though `Dataset::create`
returned an error. On an error, callers must not assume creation succeeded or immediately retry
`create`. First call `Dataset::open` on the path. If it opens, the initial manifest is visible but
the failed create call remains unacknowledged for durability purposes; preserve/report that error
and repair or move to a filesystem with working directory synchronization before relying on the
dataset. If it reports `NotFound`, creation was not visible and a later `create` attempt may retry
against the same pre-existing immediate-parent anchor, re-synchronizing the bounded chain.
Cross-process coordination is still out of scope.

| Platform/filesystem boundary | Directory-sync behavior | Claim boundary |
|---|---|---|
| Windows local filesystem | Opens a write-capable native directory handle with `FILE_FLAG_BACKUP_SEMANTICS`, then calls `sync_all`. | Included only when both operations succeed; `ERROR_INVALID_FUNCTION`, `ERROR_NOT_SUPPORTED`, and invalid-parameter outcomes fail closed. |
| POSIX local filesystem | Opens the directory and calls `sync_all`. | Included only when both operations succeed; `Unsupported`, `InvalidInput`, and `EINVAL` outcomes fail closed. |
| Any filesystem that rejects directory flushing | Returns typed `DurabilityUnsupported`. | Outside the acknowledged-durability boundary; no fallback or best-effort success exists. |
| Object/remote backends and independent processes | Not part of this Task 1 path. | Out of scope for Phase 1. |

This is an ordered local-operation contract, not universal power-loss proof. Process-abort tests
and successful calls on one host/filesystem do not establish a guarantee for another filesystem or
for cross-process publication.

## Finding register

| ID | Severity | Area | Disposition / required action |
|---|---|---|---|
| COR-01 / CONC-01 / IDX-01 | P0 | Future tombstones | Validate delete targets or define absent-target semantics so a tombstone cannot hide a later acknowledged insert. Add a regression test. |
| COR-02 / CONC-03 | P0 | Row-ID reuse | Persist or otherwise recover the allocator high-water mark so abandoned pre-publication claims cannot be reused after restart. |
| COR-03 / DUR-01 / DUR-02 | P0 | Directory durability | Make directory sync and dataset creation durability fail closed; test acknowledgement and recovery boundaries. |
| COR-04 / DUR-03a | P0 | Manifest identity | Require manifest filename/version and payload/version agreement during recovery. |
| DUR-03b | P0 | Corruption integrity | Define the corruption threat model and protect covered manifest/pruning metadata and row bytes with integrity checks or explicitly document excluded classes. |
| COR-05 / ARCH-02 | P0 | Update/delete contract | Define target existence, replacement cardinality, logical identity, and typed errors; reject unsupported shapes. |
| ARCH-01 | P0 | Schema ownership | Establish dataset-owned schema validation so positional casts cannot relabel or misinterpret acknowledged data. |
| ARCH-03 | P1 | Error semantics | Preserve `InsufficientHistory` instead of converting it into a misleading row-conflict payload. |
| CONC-02 / VER-02 | P1 | Loom gates | Make transaction and live-set-cache models visible CI gates with reproducible commands. |
| ARCH-05 | P1 | Facade boundary | Close invariant-bypassing public storage/index surfaces or explicitly disclaim them outside `Dataset` guarantees. |
| DUR-04 / VER-03 | P1 | Chaos/checkpoints | Prevent thorough durability and checkpoint suites from self-skipping while reporting success. |
| VER-01 | P1 | Regression coverage | Add direct tests for each known counterexample before declaring Phase 1 complete. |
| PERF-01..05 | P1 | Bounds/evidence | Capture current segmented measurements and define supported history, segment, recovery, and memory bounds. Compaction remains Phase 3. |
| IDX-04 / ARCH-04 / VER-07 | P1 | Claim accuracy | Correct decision and status language that treats limited recall experiments or broad snapshot/durability evidence as universal proof. |
| CONC-04 / DUR-08 | Later | Cross-process | Move independent opener and durable conditional publication work to Phase 4; do not expand Phase 1 scope. |
| PERF-02..05 / DUR-06 | Later | Lifecycle | Compaction, vacuum, orphan cleanup, bounded history, and index lifecycle belong to Phase 3; document current growth meanwhile. |
| ARCH-06..08 | Later | Client/backend surfaces | Decide subordinate-crate leakage, backend plumbing, and CLI version semantics during later API stabilization. |

## Complete legacy-ID mapping

The original lane reports used one ID per finding. This table is the lossless crosswalk into the
consolidated register above; IDs marked "merged" retain the same evidence under a shared mechanism.

| Legacy ID | Consolidated disposition |
|---|---|
| COR-01 | Merged with CONC-01 and IDX-01: future tombstone can hide an acknowledged insert. Phase 1 blocker. |
| COR-02 | Merged with CONC-03: abandoned row-ID reservation can be reused after restart. Phase 1 blocker. |
| COR-03 | Merged with DUR-01 and DUR-02: directory durability can fail open. Phase 1 blocker. |
| COR-04 | Merged with DUR-03a: manifest filename and payload versions can disagree. Phase 1 blocker. |
| COR-05 | Merged with ARCH-02: update/delete target and replacement cardinality are under-specified. Phase 1 blocker. |
| CONC-01 | Merged with COR-01 and IDX-01: future/in-flight tombstone visibility hole. Phase 1 blocker. |
| CONC-02 | Merged with VER-02: transaction and cache loom models are not CI gates. Phase 1 blocker. |
| CONC-03 | Merged with COR-02: restart can reuse an abandoned physical row-ID claim. Phase 1 blocker. |
| CONC-04 | Preserved as later Phase 4 work: independent openers lack shared conditional publication. Not a Phase 1 scope expansion. |
| DUR-01 | Merged with COR-03: directory sync errors are discarded before acknowledgement. Phase 1 blocker. |
| DUR-02 | Preserved separately: initial dataset directory entries lack a durable parent boundary. Phase 1 blocker. |
| DUR-03 | Split into DUR-03a (manifest identity) and DUR-03b (validly encoded manifest/row-file integrity); both remain Phase 1 blockers. |
| DUR-04 | Merged with VER-03: process-abort chaos does not prove power-loss durability and can be non-exercising. Phase 1 verification blocker. |
| DUR-05 | Merged with ARCH-04 and VER-07: active wording overstated durability. Corrected in current docs; evidence remains blocked until implementation fixes land. |
| DUR-06 | Preserved as later Phase 3 lifecycle work: failed commits/crashes can leave unreachable files; current growth obligation remains documented. |
| DUR-07 | Preserved as later Phase 3/4/6 boundary work: LocalFs platform/key and durable-delete constraints need an explicit contract. |
| DUR-08 | Preserved as later Phase 4 work: independent openers can race manifest versions; unsupported in the current boundary. |
| IDX-01 | Merged with COR-01 and CONC-01: unrestricted tombstone can hide row and vector. Phase 1 blocker. |
| IDX-02 | Preserved explicitly: recovery does not reject ambiguous cross-segment row/vector identity or vector IDs without row ownership. Phase 1 recovery blocker. |
| IDX-03 | Preserved explicitly: fixed `ef_search` can underfill `k`, including an unbounded API request above 32. Phase 1 contract decision and Phase 2 API work. |
| IDX-04 | Merged with ARCH-04 and VER-07: recall experiment was overgeneralized. Current decisions now bound the claim to its workload. |
| PERF-01 | Bounded Windows synthetic retained-version/segment matrix is recorded in `docs/phase-1-performance.md`; real-fixture, CPU/RAM, and portable-host evidence remain open. |
| PERF-02 | A 40-commit sample records manifest bytes at versions 1/10/20/40; it is evidence, not a universal bound. Incremental manifests/GC remain Phase 3. |
| PERF-03 | A bounded lifecycle reopen point records recovery time; the separately reported manifest bytes are end-of-run after concurrent commits. Multi-scale recovery bounds and lifecycle work remain open. |
| PERF-04 | A bounded Dataset/Snapshot fan-out sample covers K=1…64 synthetic segments; it does not establish a supported maximum. Compaction remains Phase 3. |
| PERF-05 | A bounded sample retains four historical/current snapshots and reports live allocator bytes; RSS and a universal residency bound remain open. |
| PERF-06 | Preserved as later Phase 2/3 query/layout work: public scans lack projection pushdown and sub-file pruning; establish an honest baseline. |
| PERF-07 | Preserved as documentation/query work: projection avoids some array construction but not dominant file-body reads; benchmark and correct the claim. |
| ARCH-01 | Preserved: dataset-owned schema is missing. Phase 1 blocker. |
| ARCH-02 | Merged with COR-05: target validation and singular update semantics are missing. Phase 1 blocker. |
| ARCH-03 | Preserved: insufficient history can be reported as a false row conflict. Phase 1 blocker. |
| ARCH-04 | Merged with IDX-04 and VER-07: accepted decision/active docs overclaimed transaction or recall guarantees. Corrected and bounded in current docs. |
| ARCH-05 | Preserved: public low-level surfaces can bypass `Dataset` invariants. Phase 1 facade-boundary blocker. |
| ARCH-06 | Preserved as later Phase 2 API work: subordinate-crate types leak through the transaction facade. |
| ARCH-07 | Preserved as later Phase 2/6 work: backend abstraction is not threaded through all I/O. |
| ARCH-08 | Preserved as later Phase 2 client work: CLI snapshot labels can disagree with displayed rows. |
| VER-01 | Preserved: known counterexamples lack direct regression gates. Phase 1 blocker. |
| VER-02 | Merged with CONC-02: Task 7 configures transaction/cache exact-model and index whole-binary loom CI gates; CI execution/provenance remains pending. Phase 1 blocker. |
| VER-03 | Merged with DUR-04: Task 7 configures checkpoint-abort and fast-chaos CI gates plus scheduled/manual thorough chaos; CI execution/provenance and 2000/2000 output remain pending. Phase 1 blocker. |
| VER-04 | Preserved as Phase 1 evidence gap: fuzz targets are not build-gated and do not cover all recovery parsers. |
| VER-05 | Preserved as Phase 1 reproducibility hardening: CI action/tool provenance is mutable. |
| VER-06 | Preserved as Phase 1 measurement evidence blocker: bounded Windows synthetic inputs/results are recorded, but portable host/fixture provenance is still missing. |
| VER-07 | Merged with ARCH-04 and DUR-05: current docs now qualify intended guarantees and retain Partial status. |

## Evidence that must be preserved

- Manifest publication is the intended visibility boundary for row data and immutable vector segments
  inside the supported shared-handle scope.
- Immutable snapshots, defensive segment validation, tests, loom models, chaos tooling, fuzz targets,
  and benchmarks provide a useful correctness nucleus, but not a blanket proof.
- Historical recall, chaos, and performance results must state their workload, seed count, and old
  implementation baseline. In particular, the 2,000-seed chaos run and the DBpedia embedding recipe
  are bounded evidence, not general guarantees.

## Exit order

1. Reproduce and fix future tombstones, stale targets, update cardinality, manifest mismatch, and
   allocator restart reuse.
2. Define absent-row, schema, integrity, and supported-facade contracts.
3. Make durability fail closed and add recovery/corruption tests.
4. Gate loom, chaos, fuzz provenance, and known regressions in CI.
5. Capture current segmented performance and operating bounds.
6. Only then advance Phase 2 usability or Phase 3 lifecycle work.

Cross-process transactions, serializability, compaction, full SQL, branching, object storage, and
additional ANN families are not Phase 1 exit requirements.
