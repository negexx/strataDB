# Phase 3 Vacuum Implementation Plan

> **For agentic workers:** Execute one task at a time with TDD and an independent review checkpoint.

**Goal:** Implement a bounded `Dataset::vacuum()` operation for safely reclaiming unreferenced local lifecycle objects.

**Architecture:** Reuse the existing lifecycle coordinator, manifest validation, backend listing/deletion, and typed storage errors. Vacuum will rebuild authority under lifecycle exclusivity plus `commit_lock`, validate the current and active snapshot manifests, protect every object listed by every durable manifest, and delete only recognized temporary or orphan data objects absent from that authority.

**Tech Stack:** Rust 2024, pinned Rust 1.97.1, existing `strata-storage::Backend`, `LocalFs`, manifest helpers, and `strata-txn` integration tests.

## Global Constraints

- Supported concurrency remains one process using one shared `Dataset` handle.
- Never delete `_versions/`, row-ID high-water state, or arbitrary unknown files.
- Fail closed for malformed or missing objects referenced by protected manifests.
- Preserve immutable snapshots and the snapshot-isolation ceiling.
- No dependency additions or on-disk format changes.

### Task 1: Add the public vacuum contract

**Files:**
- Create: `crates/txn/src/vacuum.rs`
- Modify: `crates/txn/src/lib.rs`
- Modify: `crates/txn/src/error.rs`
- Test: `crates/txn/tests/vacuum.rs`

- [ ] Define `VacuumReport` and `Dataset::vacuum() -> Result<VacuumReport>` with a typed error path.
- [ ] Add failing tests for empty vacuum and unknown-file preservation.
- [ ] Run `cargo test -p strata-txn --no-default-features --test vacuum`.

### Task 2: Implement protected-object authority

**Files:**
- Modify: `crates/txn/src/dataset.rs` or `crates/txn/src/vacuum.rs`
- Test: `crates/txn/tests/vacuum.rs`

- [ ] Under lifecycle exclusivity and `commit_lock`, list `_versions/` and `data/`.
- [ ] Validate the current and active snapshot manifests and collect their row/segment keys.
- [ ] Add tests proving active historical snapshots keep their files and protected missing files fail closed.

### Task 3: Implement safe deletion and retry behavior

**Files:**
- Modify: `crates/txn/src/vacuum.rs`
- Test: `crates/txn/tests/vacuum.rs`

- [ ] Delete recognized unreferenced `.arrow`/`.seg` objects and existing temporary objects only.
- [ ] Preserve unknown files and obsolete-manifest-only missing keys.
- [ ] Count bytes only after successful deletion and test retry after a post-unlink sync error.

### Task 4: Close out bounded Phase 3 evidence

**Files:**
- Modify: `docs/status.md`
- Modify: `docs/roadmap.md`
- Modify: `bench/benches/lifecycle_bench.rs` if a focused vacuum phase is required

- [ ] Run formatting, targeted tests, workspace check, targeted clippy, and diff validation.
- [ ] Add lifecycle benchmark coverage without claiming universal storage or latency bounds.
- [ ] Update the ledger to mark only the implemented vacuum slice complete.
