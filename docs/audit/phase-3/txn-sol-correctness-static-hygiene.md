# Strata-Txn Sol Correctness and Static Hygiene Audit

Date: 2026-08-27
Scope: `crates/txn` and the manifest publication boundary in `crates/storage`  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 11 head `2d32dfb`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The normal commit path has coherent
conflict ordering, immutable snapshot publication, and row/index manifest
coupling. The former indeterminate-publication defect is reconciled and
covered by transaction-level recovery tests; this remains bounded to the
documented local, single-process/shared-`Dataset` contract.

## Confirmed findings

### [Resolved P1] Post-publication manifest-sync failure is reconciled

Locations:

- [`crates/txn/src/dataset.rs:2231`](../../../crates/txn/src/dataset.rs#L2231)
- [`crates/storage/src/backend/local.rs:344`](../../../crates/storage/src/backend/local.rs#L344)
- [`crates/storage/src/backend/local.rs:359`](../../../crates/storage/src/backend/local.rs#L359)
- [`crates/storage/src/backend/mod.rs:55`](../../../crates/storage/src/backend/mod.rs#L55)

The commit path now classifies a verified-visible post-publication sync failure,
installs the candidate in the shared commit history and snapshot before
returning the typed indeterminate error, and prohibits blind transaction
replay. Focused tests cover reopen visibility, unique subsequent publication,
row/index state, and compaction/migration publication barriers.

### [Resolved P3] Query validation is active and no longer broadly suppressed

Locations:

- [`crates/txn/src/query.rs:45`](../../../crates/txn/src/query.rs#L45)
- [`crates/txn/src/query.rs:57`](../../../crates/txn/src/query.rs#L57)
- [`crates/txn/src/query.rs:78`](../../../crates/txn/src/query.rs#L78)
- [`crates/txn/src/query.rs:502`](../../../crates/txn/src/query.rs#L502)
- [`crates/txn/src/query.rs:957`](../../../crates/txn/src/query.rs#L957)

The query schema and aggregate validation model is used by the active
snapshot/query paths. Broad `#[allow(dead_code)]` attributes were removed;
the test-only schema accessor is explicitly `#[cfg(test)]`, making future dead
code visible to clippy without suppressing active validation.

### [Resolved P3] Unused recovery wrappers removed

Locations:

- [`crates/txn/src/dataset.rs:3044`](../../../crates/txn/src/dataset.rs#L3044)
- [`crates/txn/src/dataset.rs:3190`](../../../crates/txn/src/dataset.rs#L3190)

The unused local wrappers were deleted. Recovery retains the owner-aware
variants that are the production call path.

## Structural risks

- [`snapshot.rs:73`](../../../crates/txn/src/snapshot.rs#L73): recursive
  predicate translation, cognitive complexity 55.
- [`snapshot.rs:1663`](../../../crates/txn/src/snapshot.rs#L1663): recursive
  filter evaluation, cognitive complexity 39.
- [`dataset.rs:2004`](../../../crates/txn/src/dataset.rs#L2004):
  `Transaction::commit` spans approximately 275 lines and combines conflict
  checking, manifest construction, durability, and in-memory installation.
- [`dataset.rs:3200`](../../../crates/txn/src/dataset.rs#L3200): segment
  loading spans approximately 122 lines and combines integrity validation,
  ownership checks, dimension checks, and accounting.

These are maintainability risks, not independently confirmed correctness bugs.

## Verification evidence

- `cargo fmt --check`: passed.
- `cargo clippy -p strata-txn --all-targets --no-default-features -- -D warnings`: passed.
- `cargo test -p strata-txn --no-default-features`: passed (271 unit tests,
  all integration tests, and 6 doctests; one scheduled stress test ignored).
- Targeted query validation tests: passed (19 unit tests and 9 planner tests).
- `cargo clippy -p strata-txn --all-targets --no-default-features -- -D warnings`:
  passed.
- `cargo fmt --check` and `git diff --check`: passed.
- Codebase-memory fast index: 3,381 nodes and 18,621 edges repository-wide;
  1,075 nodes and 7,085 edges scoped to `crates/txn`.
- No TODO/FIXME/HACK markers or production `unsafe` blocks were found in the
  scoped source scan.

The remaining structural complexity observations are maintainability limits,
not confirmed correctness defects. Full workspace verification and the
separate concurrency/chaos evidence remain governed by their own audit
reports.

