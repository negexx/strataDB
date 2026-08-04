# Phase 1 Sol audit

**Date:** 2026-08-01
**Scope:** embedded local-disk engine, one process, one shared `Dataset` handle.
**Verdict:** Phase 1 is Partial and blocked.

This is the active consolidated audit. It preserves the lane finding IDs so code comments, tests, and
future fixes can still refer to the original evidence without maintaining seven overlapping reports.
Line numbers are deliberately omitted because they are unstable; use the finding ID plus current
source/tests as the anchor.

## Closeout baseline and ledger

The original remediation closeout was synchronized through merged PR #50,
`8cd7696fdcf34f6253fb11f9e110f6632bc872de` (`Merge Phase 1 audit remediation`). The current
gap-closure branch is based on merged PR #56, `76d12919b5234f5e089cf26e4ba469e7aaa982f0`, which
includes the subsequent evidence-finalization changes.
The
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
discovery, plus eight exact index loom models; checkpoint-abort and fast-chaos gates; and
scheduled/manual thorough-chaos execution.
Fresh Ubuntu WSL exact-model transaction loom evidence and Ubuntu WSL thorough-chaos evidence
(`2000/2000` seeds with zero violations) now exist. Manual Ubuntu GitHub Actions run
[30861009780](https://github.com/negexx/strataDB/actions/runs/30861009780) also passed the named
loom and thorough-chaos gates. Fuzz build/smoke passes locally; the merged PR #53 CI run passed the
declared fuzz-and-provenance job and retained `fuzz-provenance-30841989478-attempt-1`. The fresh
portability run [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345) passed
the native foundation matrix on Ubuntu and Windows and the pinned fixture segmented smoke on
Ubuntu. Exact-head CI run [30865323724](https://github.com/negexx/strataDB/actions/runs/30865323724)
supplies the retained branch-level command/outcome provenance. Task 8 and the cloud comparison
[30881988012](https://github.com/negexx/strataDB/actions/runs/30881988012) record bounded synthetic
segmented/lifecycle evidence with reproduction metadata; cloud run
[30892210202](https://github.com/negexx/strataDB/actions/runs/30892210202) additionally records a
complete K/mode before/after matrix on a verified 256-row prefix of the pinned real fixture.
Full 100K-row measurements, universal operating bounds, and final branch verification remain open.
This does not change the audit verdict from Partial and blocked.

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
| COR-01 | Historical counterexample; remediated with regression coverage under CONC-01/IDX-01. Final branch verification remains. |
| COR-02 | Historical counterexample; remediated with durable high-water allocation and regression coverage under CONC-03. Final branch verification remains. |
| COR-03 | Historical counterexample; remediated within the named local filesystem boundary under DUR-01/DUR-02. Final branch verification remains. |
| COR-04 | Historical counterexample; remediated with manifest identity/integrity checks under DUR-03a. Final branch verification remains. |
| COR-05 | Historical contract gap; remediated with live-target and replacement-cardinality validation under ARCH-02. Final branch verification remains. |
| CONC-01 | Historical counterexample; remediated with base-snapshot tombstone targeting under COR-01/IDX-01. Final branch verification remains. |
| CONC-02 | Historical verification gap; the named transaction/cache models now pass the manual Ubuntu CI gate, while branch provenance/native-platform evidence remains. |
| CONC-03 | Historical counterexample; remediated with durable row-ID reservation/high-water checks under COR-02. Final branch verification remains. |
| CONC-04 | Preserved as later Phase 4 work: independent openers lack shared conditional publication. Not a Phase 1 scope expansion. |
| DUR-01 | Historical counterexample; directory sync errors now return typed failure within the named local boundary under COR-03. Final branch verification remains. |
| DUR-02 | Historical counterexample; dataset creation now synchronizes the immediate-parent boundary. Final branch verification remains. |
| DUR-03 | Historical counterexample split into DUR-03a (manifest identity) and DUR-03b (valid encoding); both now have integrity checks and regression coverage. Final branch verification remains. |
| DUR-04 | Historical verification gap; checkpoint/fast/thorough chaos now pass the manual Ubuntu CI gate, but they do not prove power-loss durability or native-platform coverage. |
| DUR-05 | Historical claim overstatement; merged with ARCH-04/VER-07 and bounded in current docs. Final canonical review remains. |
| DUR-06 | Preserved as later Phase 3 lifecycle work: failed commits/crashes can leave unreachable files; current growth obligation remains documented. |
| DUR-07 | Preserved as later Phase 3/4/6 boundary work: LocalFs platform/key and durable-delete constraints need an explicit contract. |
| DUR-08 | Preserved as later Phase 4 work: independent openers can race manifest versions; unsupported in the current boundary. |
| IDX-01 | Historical counterexample; remediated with row/vector identity validation and base-snapshot targeting under COR-01/CONC-01. Final branch verification remains. |
| IDX-02 | Historical recovery-integrity counterexample; remediated with manifest-listed row/vector identity validation and regression coverage. Final branch verification remains. |
| IDX-03 | Preserved explicitly: fixed `ef_search` can underfill `k`, including an unbounded API request above 32. Phase 1 contract decision and Phase 2 API work. |
| IDX-04 | Merged with ARCH-04 and VER-07: recall experiment was overgeneralized. Current decisions now bound the claim to its workload. |
| PERF-01 | Bounded cloud synthetic and pinned real-fixture segment matrices plus native Ubuntu/Windows provenance are recorded in `docs/phase-1-performance.md`; full 100K-row behavior and universal operating bounds remain open. |
| PERF-02 | Five-repetition manifest-growth measurements cover versions 1/10/20/40/80/160; they are evidence, not a universal bound. Incremental manifests/GC remain Phase 3. |
| PERF-03 | Typed recovery-byte accounting and a deterministic row-ID load-boundary regression are implemented; multi-scale recovery bounds and fixture lifecycle/recovery evidence remain open. |
| PERF-04 | A bounded Dataset/Snapshot fan-out sample covers K=1…64 synthetic segments and a verified 256-row prefix of the pinned fixture; it does not establish a supported maximum. Compaction remains Phase 3. |
| PERF-05 | Repeated bounded synthetic evidence covers 0/1/4/16/64 retained handles with exact logical-versus-unique manifest/data/segment accounting plus labeled approximate cache/allocator observations; RSS, fixture residency, and universal residency bounds remain open. |
| PERF-06 | Preserved as later Phase 2/3 query/layout work: public scans lack projection pushdown and sub-file pruning; establish an honest baseline. |
| PERF-07 | Preserved as documentation/query work: projection avoids some array construction but not dominant file-body reads; benchmark and correct the claim. |
| ARCH-01 | Dataset-owned schema validation is implemented and regression-covered; schema evolution and final branch verification remain. |
| ARCH-02 | Merged with COR-05: target validation and singular update semantics are implemented and regression-covered; final branch verification remains. |
| ARCH-03 | Typed insufficient-history/error semantics are implemented and regression-covered; final branch verification remains. |
| ARCH-04 | Merged with IDX-04 and VER-07: accepted decision/active docs overclaimed transaction or recall guarantees. Corrected and bounded in current docs. |
| ARCH-05 | Supported public facade and invariant-boundary cleanup are implemented; final branch verification remains. |
| ARCH-06 | Preserved as later Phase 2 API work: subordinate-crate types leak through the transaction facade. |
| ARCH-07 | Preserved as later Phase 2/6 work: backend abstraction is not threaded through all I/O. |
| ARCH-08 | Preserved as later Phase 2 client work: CLI snapshot labels can disagree with displayed rows. |
| VER-01 | Known counterexamples have targeted regression gates; complete branch verification remains. |
| VER-02 | Merged with CONC-02: fresh Ubuntu WSL and manual Ubuntu GitHub Actions execution passed the nine production transaction models plus the separate compact semantic guard; native-platform and final verification remain pending. Phase 1 blocker. |
| VER-03 | Merged with DUR-04: fresh Ubuntu WSL and manual Ubuntu GitHub Actions thorough chaos reached `2000/2000` seeds with zero violations; native-platform and final verification remain pending. Phase 1 blocker. |
| VER-04 | Ubuntu WSL completed both declared nightly ASAN targets and deterministic parser smoke inputs with a stable fuzz lock hash; merged PR #53 also passed the declared fuzz-and-provenance job and retained `fuzz-provenance-30841989478-attempt-1`. Broader fuzz campaign and platform evidence remain open. |
| VER-05 | The evidence workflow pins action SHAs and nightly `2026-07-25`; the merged PR #53 fuzz job retained its artifact, exact-head CI run [30865323724](https://github.com/negexx/strataDB/actions/runs/30865323724) retained branch command/outcome provenance, and native foundation evidence passed on Ubuntu/Windows in [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345). Broader native loom/chaos coverage and final verification remain open. |
| VER-06 | Bounded cloud synthetic inputs/results, native Ubuntu/Windows foundation provenance, Ubuntu pinned-fixture smoke, and the complete K/mode matrix on a verified 256-row fixture prefix are recorded in [30892210202](https://github.com/negexx/strataDB/actions/runs/30892210202); full 100K-row behavior and universal operating bounds remain open. |
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
