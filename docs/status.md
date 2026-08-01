# Status ledger

This ledger describes the checked-in implementation, not an aspirational API. Evidence pointers name the current source or test that supports each entry.

## Phase 1 audit status — 2026-08-01

**Status: Partial — blocked.** The seven-lane [Sol Phase 1 audit](audits/phase-1-sol-audit-report.md) found in-scope correctness, durability, schema, verification, and performance-evidence blockers. The audit does not expand the supported concurrency scope or pull compaction/cross-process coordination into Phase 1. Use the consolidated report and its lane reports as the current evidence ledger before treating Phase 1 as complete.

The highest-priority blockers are invalid future tombstones that can hide later acknowledged inserts, non-durable row-ID reservation reuse after restart, fail-open directory durability, self-inconsistent manifest recovery, missing end-to-end integrity for covered manifest/pruning metadata and row bytes, missing dataset-owned schema validation, an undefined supported facade around invariant-bypassing low-level surfaces, and absent regression/loom/chaos CI gates. These findings are documented as work to do; they are not silently reclassified as implemented behavior.

## Vocabulary

| Status | Meaning |
|---|---|
| **Implemented** | Present in the checked-in code with direct source or test evidence. |
| **Partial** | A usable slice exists, but important scope, API, verification, or operational work remains. |
| **Proposed** | A planned direction or active proposal; no supported capability claim. |
| **Historical** | Preserved context that does not govern current behavior. |
| **Superseded** | Replaced by a newer decision or mechanism. |
| **Deferred** | Deliberately outside the current implementation plan until a later phase. |

## Capability ledger

| Capability | Status | Evidence and current boundary |
|---|---|---|
| Local storage and manifest primitives | Implemented | [`crates/storage/src/manifest.rs`](../crates/storage/src/manifest.rs) persists manifest versions, data-file entries, and segment entries; [`crates/storage/src/datafile.rs`](../crates/storage/src/datafile.rs) writes columnar batches. This status covers local primitives only, not lifecycle management. |
| Transactions and typed conflicts | Partial | [`crates/txn/src/dataset.rs`](../crates/txn/src/dataset.rs) serializes commit validation and returns [`TxnError::Conflict`](../crates/txn/src/error.rs) with contested row IDs. It is write-write conflict detection, not a claim of serializable transactions. |
| Atomic row-data and vector-segment publication | Partial | [`Transaction::commit`](../crates/txn/src/dataset.rs) publishes data-file and `SegmentEntry` changes through one manifest/snapshot transition; its in-process atomicity tests and [`crates/txn/tests/concurrent_snapshot_isolation.rs`](../crates/txn/tests/concurrent_snapshot_isolation.rs) exercise row/index visibility. The [completed Phase 1 audit](audits/phase-1-sol-audit-report.md) found target-validation and identity-integrity blockers; the guarantee does not extend across independent processes. |
| Snapshots and query reads | Partial | [`crates/txn/src/snapshot.rs`](../crates/txn/src/snapshot.rs) provides immutable snapshots for scan, predicate, explain, and vector-search reads; [`crates/txn/tests/concurrent_snapshot_isolation.rs`](../crates/txn/tests/concurrent_snapshot_isolation.rs) exercises snapshot stability. There is no supported full read/write snapshot-transaction API. |
| Query operators and pruning | Partial | [`crates/query/src/predicate.rs`](../crates/query/src/predicate.rs), [`crates/query/src/group_by.rs`](../crates/query/src/group_by.rs), and [`crates/txn/tests/phase_3_pruning.rs`](../crates/txn/tests/phase_3_pruning.rs) cover predicates, group-by, and file pruning. Planner/catalog completeness remains later work. |
| Point lookup | Proposed | [`Snapshot`](../crates/txn/src/snapshot.rs) exposes scan, predicate scan, explain, and vector search, but no supported public get/lookup-by-row-ID API exists. |
| Immutable vector segments | Implemented | [`crates/storage/src/manifest.rs`](../crates/storage/src/manifest.rs) lists `SegmentEntry` values; [`crates/index/src/segment_set.rs`](../crates/index/src/segment_set.rs) fans out immutable segment search; [`crates/txn/tests/concurrent_snapshot_isolation.rs`](../crates/txn/tests/concurrent_snapshot_isolation.rs) covers snapshot isolation of segment sets. |
| Update/delete identity semantics | Partial | [`crates/txn/src/dataset.rs`](../crates/txn/src/dataset.rs) tombstones deleted or replaced row IDs in the manifest; its `delete_tombstones_a_row_and_it_becomes_invisible` and `update_tombstones_old_row_and_makes_new_row_visible` tests cover the current behavior. The public contract and lifecycle implications remain Phase 1 work. |
| CLI | Partial | [`crates/cli/src/main.rs`](../crates/cli/src/main.rs) provides MVP create, insert, scan, filter, search, inspect, explain, and crash-loop commands. It is not a stable administration surface. |
| PyO3 extension scaffolding | Implemented | [`crates/bindings/src/lib.rs`](../crates/bindings/src/lib.rs) builds a placeholder extension that exports `placeholder_version`. |
| Python database API | Proposed | The current binding module exposes no dataset, transaction, query, or conflict API; Phase 2 must define and verify that public surface. |
| Chaos and concurrency testing | Partial | [`tests/sim/tests/chaos.rs`](../tests/sim/tests/chaos.rs) and [`crates/chaos-worker/src/main.rs`](../crates/chaos-worker/src/main.rs) run real-process crash scenarios; targeted loom models live in [`crates/txn/src/dataset.rs`](../crates/txn/src/dataset.rs). The audit is complete; Phase 1 remains blocked on CI-visible transaction/cache loom and non-skipping chaos/checkpoint evidence. See the [consolidated report](audits/phase-1-sol-audit-report.md). |
| Durability and recovery | Partial | Local write paths fsync file contents and attempt manifest publication in [`crates/storage/src/datafile.rs`](../crates/storage/src/datafile.rs) and [`crates/storage/src/manifest.rs`](../crates/storage/src/manifest.rs); the chaos harness checks bounded process-abort recovery outcomes. The audit is complete; Phase 1 remains blocked on fail-closed directory durability and end-to-end integrity for covered manifest metadata and row bytes. See the [consolidated report](audits/phase-1-sol-audit-report.md). |
| Manifest/segment growth and cleanup obligations | Partial | [`crates/index/src/segment_set.rs`](../crates/index/src/segment_set.rs) records the current one-segment-per-vector-carrying-commit behavior. Phase 1 must bound and document the obligation; no cleanup implementation exists. |
| Compaction and garbage collection | Proposed | No compaction/GC implementation is present. It is operational lifecycle work in Phase 3, after Phase 1 has established safe growth and cleanup obligations. |
| Cross-process coordination | Proposed | The `Dataset` module states its conflict scope is threads/tasks sharing one in-process handle ([`crates/txn/src/dataset.rs`](../crates/txn/src/dataset.rs)). Independent openers, shared allocation, and durable conditional publication are not supported guarantees. |
| Branching and merge | Proposed | [ADR 0008](decisions/0008-adopt-segmented-index-layout.md) adopts the layout prerequisite, but no fork, abort, merge, or branch-aware manifest API exists. |
| Object storage | Proposed | [`crates/storage/src/backend/mod.rs`](../crates/storage/src/backend/mod.rs) defines an abstraction and [`crates/storage/src/backend/local.rs`](../crates/storage/src/backend/local.rs) implements local files only; no object-store backend is implemented. |
| Schema and migrations | Partial | [`crates/txn/src/dataset.rs`](../crates/txn/src/dataset.rs) validates only caller-provided batch shape and reserved hidden-column constraints while maintaining hidden row-id/timestamp columns. It does not enforce a dataset-owned logical schema; schema enforcement remains Partial and blocked by the [Phase 1 audit](audits/phase-1-sol-audit-report.md). There is no stable schema catalog or general migration workflow. |

## Concurrency scope

The current supported concurrency scope is **one process using a shared `Dataset` handle**. The implementation's commit lock, row-ID allocator, recent-write history, and current snapshot live in that handle. Opening the same path independently does not establish a cross-process transaction protocol.

## Legacy phase map

Historical phase labels are useful as document locators only; they do not define completion of the capability phases in [the roadmap](roadmap.md).

| Historical document or group | Status and current capability placement |
|---|---|
| [Phase 0 transaction and format spec](design/phase-0-transaction-and-format-spec.md) | Phase 0 foundation. |
| [Phase 2 encodings/group-by spec](design/phase-2-encodings-and-groupby-spec.md) and [implementation plan](history/design/phase-2-implementation-plan.md) | Historical numbering; implemented query pieces belong to Phase 2. |
| [Phase 3 query-refinement spec](design/phase-3-query-refinement-spec.md) and [implementation plan](history/design/phase-3-implementation-plan.md) | Historical numbering; query usability belongs to Phase 2, while lifecycle work belongs to Phase 3. |
| [Phase 4 vector-index spec](history/design/phase-4-vector-index-spec.md) and [implementation plan](history/design/phase-4-implementation-plan.md) | Superseded where they describe the prior index mechanism; immutable segments are now Phase 1 baseline under [ADR 0008](decisions/0008-adopt-segmented-index-layout.md). |
| [Phase 5 MVCC spec](history/design/phase-5-mvcc-snapshot-isolation-spec.md) | Historical numbering; Phase 1 baseline. It does not prove a full read/write snapshot API. |
| [S1 segmented-index spec](design/phase-s1-segmented-index-spec.md) and [S1 implementation amendments/plans](superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md) ([W2](superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md), [W3](superpowers/specs/2026-07-25-s1-w3-design-amendment.md), [W3.2](superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md), [W4](superpowers/specs/2026-07-26-s1-w4-zone-map-design-amendment.md), [W3.1 plan](superpowers/plans/2026-07-25-s1-w3-1-segment-abstraction.md), [W3.2a plan](superpowers/plans/2026-07-25-s1-w3-2a-segment-write-path.md)) | Phase 1 baseline: immutable segments and manifest publication. |
| [Phase 6 concurrent-write design](superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md) and [plan](superpowers/plans/2026-07-21-phase-6-concurrent-write-engine.md) | Historical numbering; its shared-handle correctness slice is Phase 1. Retired mechanism details are superseded by the segmented baseline. |
| [Phase 7 correctness-harness design](superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md) and [plan](superpowers/plans/2026-07-22-phase-7-correctness-harness.md) | Historical numbering; the audit is complete, Phase 1 verification evidence remains Partial and blocked, and the [consolidated report](audits/phase-1-sol-audit-report.md) records the disposition. |
| [Phase 9 object-storage design](superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md) and [M0 plan](superpowers/plans/2026-07-30-phase9-m0-backend-trait.md) | Historical numbering; Proposed current Phase 6 object storage and deployment. |
| [Phase A group-by optimization design](superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md) and [plan](superpowers/plans/2026-07-19-group-by-phase-a-optimization-plan.md) | Historical subphase; Phase 2 query/usability work. |
