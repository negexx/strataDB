# Phase 3 closeout audit

**Date:** 2026-08-13
**Scope:** one process using one shared `Dataset` handle on the local filesystem.
**Verdict:** **Implemented within named bounds.**

This audit closes the implemented Phase 3 lifecycle slice only. It relies on the
[Phase 3 verification report](../../phase-3-verification-report.md) for command-level provenance
and on the active lifecycle designs for contracts. It does not expand the embedded/shared-handle
boundary or supersede the Phase 4 reservation.

## Acceptance matrix

| Capability | Code | Regression tests | Evidence | Non-claims |
|---|---|---|---|---|
| Inventory | `crates/txn/src/dataset.rs` (`lifecycle_report`); `crates/txn/src/lifecycle.rs` | `crates/txn/tests/lifecycle_inventory.rs` | Native x64 MSVC was verified by explicit `PATH` setup and `where.exe cl`/`where.exe link`; all seven targeted lifecycle test binaries passed locally. Exact-head cloud run 31714285971 completed the lifecycle gate. | One captured-snapshot diagnostic listing; not an atomic filesystem inventory or deletion authority. |
| Manifest retention | `crates/txn/src/dataset.rs` (`prune_manifests`); `crates/txn/src/retention.rs`; `crates/txn/src/retention_executor.rs` | `crates/txn/tests/retention_plan.rs`; `crates/txn/tests/manifest_retention_executor.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud lifecycle evidence includes manifest-retention crash/recovery. | Deletes only eligible historical manifests; never data, segments, temporary objects, arbitrary orphans, or cross-process state. |
| Age retention | `crates/txn/src/dataset.rs` (`prune_manifests_by_age`); `crates/txn/src/retention.rs` | `crates/txn/tests/retention_age.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud lifecycle evidence covers age retention. | Current/latest/active snapshots and legacy zero-timestamp manifests remain protected. |
| Compaction | `crates/txn/src/dataset.rs` (`compact`); `crates/txn/src/compaction.rs` | `crates/txn/tests/compaction.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud evidence includes compaction crash/reopen coverage. | Explicit only; no background compaction, cross-process safety, serializability, universal power-loss proof, or mandatory bound. |
| Vacuum | `crates/txn/src/vacuum.rs` | `crates/txn/tests/vacuum.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud evidence includes the unpadded-current-manifest reopen/vector regression. | Deletes recognized unprotected temporary, `.arrow`, and `.seg` objects only; unknown objects stay outside authority. |
| Maintenance | `crates/txn/src/maintenance.rs` | `crates/txn/tests/maintenance.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud lifecycle evidence covers maintenance. | `storage_bound_met` is one completed run's final inventory observation, not atomic or continuing enforcement. |
| Snapshot protection | Shared-handle snapshot leases in `crates/txn/src/retention.rs`; protection in `crates/txn/src/dataset.rs`, `crates/txn/src/vacuum.rs`, and `Dataset::compact` | `crates/txn/tests/retention_plan.rs`, `crates/txn/tests/manifest_retention_executor.rs`, `crates/txn/tests/retention_age.rs`, `crates/txn/tests/compaction.rs`, `crates/txn/tests/vacuum.rs`, and `crates/txn/tests/maintenance.rs` | All seven targeted lifecycle test binaries passed locally; exact-head cloud lifecycle evidence covers snapshot protection. | Only snapshots from the same shared handle are protected; independent openers/processes are excluded. |
| Crash/retry | `crates/txn/src/retention_executor.rs`; compaction implementation in `crates/txn/src/dataset.rs`; `crates/txn/src/vacuum.rs` | `crates/txn/tests/manifest_retention_executor.rs`; `crates/txn/tests/compaction.rs`; `crates/txn/tests/vacuum.rs` | All seven targeted lifecycle test binaries passed locally; cloud run 31714285971 covers the stated crash/reopen and post-publication cases. | A post-unlink directory-sync failure remains an error; this is bounded local-filesystem evidence, not universal power-loss durability. |
| Lifecycle exclusion | Locking/admission and narrow operations in `crates/txn/src/dataset.rs`, `crates/txn/src/retention_executor.rs`, `crates/txn/src/vacuum.rs`, and `crates/txn/src/maintenance.rs` | `crates/txn/tests/manifest_retention_executor.rs` verifies preparation/executor exclusion; focused tests above verify narrow deletion authority. | CI's crate-scoped lifecycle-coordination loom model is authoritative; no local loom command was run in this closeout. | No cross-process coordination, serializability, distributed transactions, object storage, arbitrary cleanup, or automatic continuous enforcement. |

## Fresh Windows native verification

The 2026-08-13 native x64 MSVC environment was verified by explicit `PATH` setup and
`where.exe cl`/`where.exe link`.

| Command | Result |
|---|---|
| `cargo test -p strata-txn --no-default-features --test lifecycle_inventory --test retention_plan --test retention_age --test manifest_retention_executor --test compaction --test vacuum --test maintenance` | Exit 0; all seven targeted lifecycle test binaries passed. |
| `cargo test --workspace --no-default-features` | Exit 0. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 0. |
| `cargo fmt --check` | Exit 0. |
| `git diff --check` | Exit 0. |

These native Windows results make no local loom claim. Cloud CI remains authoritative for the
loom and broader CI gates.

## Evidence boundary

GitHub Actions run [31714285971](https://github.com/negexx/strataDB/actions/runs/31714285971)
is the canonical completed exact-head functional evidence for implementation head `7be77d5`, merged
as production code in `main` at `65449a9`. Evidence was reviewed against baseline `main` `66408bd`
before this uncommitted documentation closeout; final exact-head provenance must be the commit
containing these docs.

The fresh native Windows results in the verification report verify the native x64 MSVC environment
through explicit `PATH` setup and `where.exe cl`/`where.exe link`; the seven targeted lifecycle test
binaries, workspace test suite, lint, format, and diff checks all exited 0. This is not a local loom
claim. Cloud CI remains authoritative for the loom and broader CI gates.

Phase 4 remains Proposed. The implemented lifecycle policy neither coordinates independent
openers nor changes the snapshot-isolation ceiling; the current API remains immutable snapshot reads
plus write-write OCC rather than a full read/write snapshot transaction API.
