# Phase 1 Sol Audit Report — Correctness and Durability Baseline

**Date:** 2026-08-01
**Baseline:** current working tree after the documentation/archive cleanup; unrelated dirty source changes were preserved.
**Scope:** embedded local-disk engine using one shared `Dataset` handle in one process.
**Method:** seven independent Sol lanes, each read-only against the same repository state, followed by controller reconciliation.

## Executive verdict

**Phase 1 is blocked.** Strata has a credible correctness nucleus: row data and immutable vector segments are prepared before manifest publication, the manifest transition is the visibility boundary, snapshots are immutable, segment files have defensive validation, and the repository has substantial tests, property checks, loom models, chaos tooling, fuzz targets, and benchmarks.

The audit also found in-scope counterexamples that prevent a Phase 1 correctness/durability exit. The most serious is that an unrestricted tombstone for a row ID that does not yet exist can hide a later acknowledged insert and its vector, while bypassing the claimed conflict path. Other blockers include row-ID reuse after restart following an abandoned pre-publication claim, swallowed directory-fsync failures, inconsistent manifest filename/payload versions, missing end-to-end integrity for validly encoded manifest and row-file corruption, missing schema ownership, unvalidated update/delete targets, and an undefined supported facade around invariant-bypassing low-level surfaces.

These are not requests for serializability, cross-process transactions, or compaction before their planned phases. They are failures or unresolved contracts inside the currently documented shared-handle baseline.

## Lane results

| Lane | Verdict | Primary disposition |
|---|---|---|
| [Correctness](phase-1/correctness.md) | Blocked | Fix future tombstones, allocator persistence/reuse, durability acknowledgement, manifest consistency, and update cardinality. |
| [Concurrency](phase-1/concurrency.md) | Blocked | Add absent-target semantics and CI-visible loom gates; keep independent openers bounded to Phase 4. |
| [Durability](phase-1/durability.md) | Blocked | Make directory durability fail-closed, define creation/recovery integrity, and separate abort testing from power-loss evidence. |
| [Index atomicity](phase-1/index-atomicity.md) | Blocked | Prevent unrestricted tombstones, validate row/vector identity relationships, and document/fix the fixed-`ef` result bound. |
| [Performance](phase-1/performance.md) | Blocked on evidence/bounds | Retain a current measurement matrix and define supported history/segment/memory bounds; compaction remains Phase 3. |
| [Architecture/API](phase-1/architecture-api.md) | Blocked | Establish schema ownership, target validation, accurate error/API semantics, and the supported facade boundary. |
| [Verification/docs](phase-1/verification-docs.md) | Blocked | Add regression gates, CI loom/chaos evidence, fuzz/benchmark provenance, and correct remaining guarantee wording. |

## Cross-lane findings

### P0 — Phase 1 correctness blockers

1. **Unrestricted tombstones can hide acknowledged inserts.** `delete(row_id)` does not establish that the target exists. A tombstone for a future row ID can later hide an insert of that ID and evade write-write conflict detection. This appears independently as COR-01, CONC-01, and IDX-01.
2. **Row-ID non-reuse is not durable across abandoned pre-publication claims and immediate reopen.** The allocator advances in memory before the manifest makes the new high-water mark durable. This conflicts with the documented “never reuse” invariant and appears as COR-02 and CONC-03.
3. **Durability acknowledgement can be fail-open.** Directory-sync failures are ignored in the local publication path. Dataset creation also lacks a parent-directory durability boundary. This appears as COR-03, DUR-01, and DUR-02.
4. **Recovery accepts self-inconsistent manifest identity.** The highest numeric filename is selected without requiring its payload version to match the filename version. This is COR-04 and part of DUR-03.
5. **Validly encoded manifest and row-file corruption lacks end-to-end integrity protection.** Manifest pruning statistics and segment zone maps can be altered while remaining valid JSON and can then silently suppress matching rows; Arrow payload corruption that remains parseable is not fully detected. Phase 1 must define the supported corruption threat model and either protect covered manifest metadata and row bytes with checksums/integrity plus semantic validation, or explicitly bound and document corruption classes that are not covered. This is the remainder of DUR-03 beyond filename/payload identity.
6. **Update/delete contracts are under-specified and under-validated.** `update` is delete plus unrestricted insert, allowing zero or multiple replacement rows; targets are not validated. This is COR-05 and ARCH-02.
7. **Schema ownership is incomplete.** Without a dataset-owned logical schema, positional casts can acknowledge data that later scans cannot interpret or can silently relabel. This is ARCH-01.
8. **Error semantics can misreport history exhaustion as a row conflict.** `InsufficientHistory` is collapsed into a false row-level conflict payload. This is ARCH-03.

### P1 — Verification and contract blockers

1. Transaction and live-set-cache loom models are not CI gates (CONC-02, VER-02).
2. Public storage/index surfaces can bypass the `Dataset` transaction invariants, while package metadata and visibility do not establish which facade carries Strata's guarantees. Phase 1 must close those surfaces or explicitly disclaim them before making invariant claims (ARCH-05).
3. Thorough chaos and storage checkpoint tests can self-skip in ordinary CI and report success without exercising the intended assertions (DUR-04, VER-03).
4. Known counterexamples do not yet have direct regression tests (VER-01).
5. Current performance evidence is not retained for the segmented implementation; manifest publication is predicted O(history) per commit and O(commits²) cumulatively, recovery and segment fan-out grow with retained history, and eager segment residency lacks a supported operating bound (PERF-01 through PERF-05).
6. Active ADR/documentation language still needs correction where it treats limited recall experiments or broad snapshot/durability guarantees as universal proof (IDX-04, ARCH-04, VER-07).

### Later-phase items, not Phase 1 blockers

- Independent openers and cross-process conditional publication belong to Phase 4 (CONC-04, DUR-08).
- Compaction, vacuum, orphan cleanup, bounded history, and index lifecycle belong to Phase 3, but Phase 1 must document current growth and supported bounds (PERF-02 through PERF-05, DUR-06).
- Subordinate-crate type leakage, backend plumbing, and CLI versioned results should be decided before client/API stabilization, with implementation primarily in Phase 2/6 (ARCH-06 through ARCH-08). ARCH-05's invariant-bypassing public surfaces remain a Phase 1 contract blocker.
- Full SQL, distributed transactions, branching/merge, object storage, and alternate ANN families remain later or deferred capabilities.

## Phase 1 exit-criteria disposition

| Exit area | Result | Required before exit |
|---|---|---|
| Row/index atomic publication | Partial, blocked | Prevent invalid tombstone visibility; validate manifest-to-row/vector relationships; add regression tests. |
| Shared-handle conflicts/snapshots | Partial, blocked | Define absent-target behavior, preserve allocator contract, distinguish insufficient history, and make required loom models CI-visible. |
| Crash durability/recovery and corruption integrity | Partial, blocked | Fail closed on directory sync, define creation durability, validate manifest identity and semantic relationships, define the supported corruption threat model, and protect covered manifest/pruning metadata and row bytes with checksums/integrity or explicitly document excluded corruption classes. Preserve the limits of abort-only chaos evidence. |
| Schema/update/delete API | Partial, blocked | Establish dataset schema ownership and typed target/cardinality semantics. |
| Supported facade/invariant boundary | Partial, blocked | Close invariant-bypassing public storage/index surfaces or explicitly mark them outside `Dataset` guarantees; keep ARCH-06 through ARCH-08 implementation in their later phases. |
| Performance/boundedness | Partial, blocked on evidence | Retain current segmented measurements, define supported version/segment/memory bounds, and link compaction work to Phase 3. |
| Verification/documentation | Partial, blocked | Gate known regressions, make opt-in suites explicit, and align ADR/status/architecture claims with source evidence. |

## Strengths to preserve

- Manifest publication is the intended single visibility boundary for row data and immutable vector segments within the supported shared-handle scope.
- Snapshot reads use immutable manifest/segment state and tombstone filtering.
- Segment headers, body checksums, topology checks, and bounds validation provide a stronger index-file safety foundation than the retired mutable-graph design.
- The test surface is broad and useful; the primary gap is that several high-value suites are opt-in, incomplete, or not wired into CI gates.
- The documentation cleanup now separates current guidance from historical ADR/design material; the remaining corrections are tracked as audit findings rather than hidden in aspirational prose.

## Recommended order of work

1. Add regression tests reproducing the future-tombstone/invisible-insert case, stale deletes, update cardinality, manifest filename/payload mismatch, and allocator restart reuse.
2. Decide and implement the supported absent-row and row-ID allocation contracts; update typed errors and current ADR/status language together.
3. Make local durability fail-closed and test directory/creation/recovery boundaries explicitly.
4. Define the supported corruption threat model; add integrity and semantic validation for covered manifest metadata and row bytes, including pruning metadata, without making an accidental format-compatibility change.
5. Establish dataset-owned schema validation and define or disclaim the supported facade around low-level storage/index surfaces.
6. Add CI-visible transaction/cache loom gates and non-skipping chaos/checkpoint jobs.
7. Capture a controlled segmented performance matrix and define current operating bounds before treating Phase 1 as complete.
8. Only then advance Phase 2 usability work or Phase 3 lifecycle implementation.

## Reconciliation note

The lanes independently rediscovered the same future-tombstone and durability issues. Their evidence was retained in the lane reports; this document records the shared disposition and phase boundaries. Final reconciliation restored DUR-03's full validly encoded manifest/row-file integrity finding and ARCH-05's Phase 1 facade-boundary disposition after review. These are documentation corrections only; no source-code fix was made as part of this audit pass.
