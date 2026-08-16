# Strata-Txn Sol Correctness and Static Hygiene Audit

Date: 2026-08-15  
Scope: `crates/txn` and the manifest publication boundary in `crates/storage`  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** The normal commit path has coherent conflict ordering, immutable
snapshot publication, and row/index manifest coupling, but one uncertain
manifest-publication path prevents approval.

## Confirmed findings

### [P1] Post-publication manifest-sync failure can leave the shared handle stale

Locations:

- [`crates/txn/src/dataset.rs:2231`](../../../../crates/txn/src/dataset.rs:2231)
- [`crates/storage/src/backend/local.rs:344`](../../../../crates/storage/src/backend/local.rs:344)
- [`crates/storage/src/backend/local.rs:359`](../../../../crates/storage/src/backend/local.rs:359)
- [`crates/storage/src/backend/mod.rs:55`](../../../../crates/storage/src/backend/mod.rs:55)

`Transaction::commit` returns the manifest-sync error before updating the
commit log or the in-memory current snapshot. The local backend can rename the
new manifest into visibility before directory synchronization completes. A
sync failure can therefore leave version `N+1` visible on disk while the shared
handle remains at `N`. A subsequent commit may calculate `N+1` again. Since
the publication path is overwrite-capable while the documented manifest
primitive is `put_if_absent`, retry behavior can overwrite the uncertain
manifest on POSIX or fail repeatedly on Windows.

Required follow-up: Sol design of indeterminate-commit reconciliation,
reopen/retry semantics, immutable version publication, and focused regression
coverage before Terra implementation.

### [P3] Dead query-validation implementation is hidden by broad allowances

Locations:

- [`crates/txn/src/query.rs:45`](../../../../crates/txn/src/query.rs:45)
- [`crates/txn/src/query.rs:57`](../../../../crates/txn/src/query.rs:57)
- [`crates/txn/src/query.rs:78`](../../../../crates/txn/src/query.rs:78)
- [`crates/txn/src/query.rs:502`](../../../../crates/txn/src/query.rs:502)
- [`crates/txn/src/query.rs:957`](../../../../crates/txn/src/query.rs:957)

`LogicalType`, `LogicalColumn`, `DatasetSchema`, aggregate-output validation,
and related aliases are retained behind `#[allow(dead_code)]`. The graph found
no production inbound path to `DatasetSchema`. This parallel test-only model
can drift from the active query path.

### [P3] Unused recovery wrappers remain in production source

Locations:

- [`crates/txn/src/dataset.rs:3044`](../../../../crates/txn/src/dataset.rs:3044)
- [`crates/txn/src/dataset.rs:3190`](../../../../crates/txn/src/dataset.rs:3190)

`validate_data_files` and `load_segments` are suppressed with
`#[allow(dead_code)]`; production callers use the owner-aware variants.

## Structural risks

- [`snapshot.rs:73`](../../../../crates/txn/src/snapshot.rs:73): recursive
  predicate translation, cognitive complexity 55.
- [`snapshot.rs:1663`](../../../../crates/txn/src/snapshot.rs:1663): recursive
  filter evaluation, cognitive complexity 39.
- [`dataset.rs:2004`](../../../../crates/txn/src/dataset.rs:2004):
  `Transaction::commit` spans approximately 275 lines and combines conflict
  checking, manifest construction, durability, and in-memory installation.
- [`dataset.rs:3200`](../../../../crates/txn/src/dataset.rs:3200): segment
  loading spans approximately 122 lines and combines integrity validation,
  ownership checks, dimension checks, and accounting.

These are maintainability risks, not independently confirmed correctness bugs.

## Verification evidence

- `cargo fmt --check`: passed.
- `cargo clippy -p strata-txn --all-targets --no-default-features -- -D warnings`: passed.
- `cargo test -p strata-txn --no-default-features`: blocked before execution by
  missing MSVC `link.exe`.
- `cargo check -p strata-txn --no-default-features`: blocked by the same linker
  environment issue.
- Codebase-memory fast index: 3,381 nodes and 18,621 edges repository-wide;
  1,075 nodes and 7,085 edges scoped to `crates/txn`.
- No TODO/FIXME/HACK markers or production `unsafe` blocks were found in the
  scoped source scan.

The linker failure is an environment limitation, not a repository finding.
Runtime tests, loom, and full native verification remain unverified until the
MSVC developer environment is active.

