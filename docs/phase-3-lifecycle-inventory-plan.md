# Phase 3 Lifecycle Inventory and Diagnostics Implementation Plan

> **For agentic workers:** Execute this plan task-by-task with an independent review after each
> task. Do not broaden the slice into reclamation, compaction, retention, or CLI work.

**Goal:** Add a read-only `Dataset::lifecycle_report()` API that reports snapshot-anchored storage
inventory and unreferenced-object candidates within the embedded single-process boundary.

**Architecture:** A new transaction-layer lifecycle module captures one `Arc<Snapshot>`, enumerates
the dataset backend's `_versions/` and `data/` objects, and joins them against the captured manifest.
The report is observational, uses checked arithmetic, and never acquires the commit lock or mutates
storage. Existing `TxnError` and `Backend` error paths remain authoritative.

**Tech Stack:** Rust 2024, Cargo workspace, Arrow manifests, `strata-storage::Backend`, immutable
`strata-txn::Snapshot`, tempfile-based Rust tests.

## Global Constraints

- Preserve the embedded, single-node, one-process/shared-`Dataset` handle boundary.
- Preserve immutable snapshot reads plus write-write OCC; do not add serializability.
- Do not claim universal durability, latency, memory, recovery, recall, or segment-count guarantees.
- Do not add cross-process coordination, compaction, vacuum, retention, or orphan cleanup.
- Do not add dependencies.
- Use checked `u64` accumulation and typed errors; never wrap or silently omit malformed data.
- Do not delete, rewrite, or publish any object in the diagnostics path.
- Do not recreate the retired agent-planning tree; use the approved design and this plan under `docs/`.

## File Map

- Create `crates/txn/src/lifecycle.rs`: public report type, collection logic, key validation, and
  focused unit tests for arithmetic/classification helpers.
- Modify `crates/txn/src/lib.rs`: register the module and re-export `LifecycleReport`.
- Modify `crates/txn/src/dataset.rs`: add `Dataset::lifecycle_report()` delegating from the live
  dataset handle to the captured snapshot and dataset root.
- Modify `crates/txn/src/error.rs` only if a dedicated typed lifecycle error is required; prefer
  existing `Storage`, `UnsafeManifestPath`, and `ManifestOverflow` variants.
- Create `crates/txn/tests/lifecycle_inventory.rs`: end-to-end dataset, multi-version, vector,
  missing-object, and orphan-candidate coverage.
- Modify `docs/status.md`, `docs/architecture.md`, and `docs/roadmap.md`: record the diagnostic API
  and its non-reclamation boundary without marking Phase 3 complete.

### Task 1: Define the typed report and pure classification helpers

**Files:**

- Create: `crates/txn/src/lifecycle.rs`
- Modify: `crates/txn/src/lib.rs`

**Interfaces:**

- Produce `pub struct LifecycleReport` with these fields: `observed_version: u64`,
  `manifest_object_count: u64`, `manifest_bytes: u64`, `current_manifest_bytes: Option<u64>`,
  `data_object_count: u64`, `data_bytes: u64`, `reachable_data_file_count: u64`,
  `reachable_data_file_bytes: u64`, `reachable_segment_count: u64`, `reachable_segment_bytes: u64`,
  `orphan_candidate_count: u64`, `orphan_candidate_bytes: u64`, `tombstone_count: u64`, and
  `physical_row_count: u64`.
- Produce `pub(crate) fn collect(...) -> Result<LifecycleReport>` for the dataset method in Task 2.
- Keep the report’s fields read-only after construction and document that orphan candidates are not
  safe-to-delete claims.

- [ ] Write unit tests for checked byte/count addition and duplicate reachable-key rejection.
- [ ] Run `cargo test -p strata-txn lifecycle --lib` and verify the new tests fail until helpers exist.
- [ ] Implement checked-add helpers returning `TxnError::ManifestOverflow` with the affected total.
- [ ] Implement manifest-relative key validation using only normal path components and the existing
      `TxnError::UnsafeManifestPath` error contract.
- [ ] Implement pure joining/classification helpers that accept `ObjectMeta` lists and the captured
      `Manifest`, producing reachable sets and candidate totals without filesystem mutation.
- [ ] Run `cargo test -p strata-txn lifecycle --lib` and `cargo fmt --check`.
- [ ] Commit as `feat: define lifecycle inventory report`.

### Task 2: Collect a snapshot-anchored report from `Dataset`

**Files:**

- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/lifecycle.rs`

**Interfaces:**

- Add `pub fn lifecycle_report(&self) -> Result<LifecycleReport>` to `Dataset`.
- The method captures `self.snapshot()` exactly once, constructs `LocalFs` from the dataset root,
  lists `_versions/` and `data/`, and passes those lists plus the captured manifest to
  `lifecycle::collect`.
- Match the captured version’s `_versions/{version:020}.manifest` object to populate
  `current_manifest_bytes`; leave it `None` only if the listing is inconsistent, and treat a missing
  reachable data or segment object as a typed error.

- [ ] Add a failing unit/integration test that calls `Dataset::lifecycle_report()` on a fresh dataset.
- [ ] Run the focused test and confirm the method is absent or the expected report fields are missing.
- [ ] Implement the one-snapshot delegation without acquiring `commit_lock` or reading a second
      manifest.
- [ ] Run the fresh-dataset test, `cargo check -p strata-txn`, and `cargo fmt --check`.
- [ ] Commit as `feat: collect snapshot anchored lifecycle reports`.

### Task 3: Add end-to-end inventory and orphan-candidate tests

**Files:**

- Create: `crates/txn/tests/lifecycle_inventory.rs`
- Modify: `crates/txn/src/lifecycle.rs` only if test-facing helpers need correction

**Interfaces:**

- Consume `Dataset::create`, `Dataset::open`, `Dataset::snapshot`, transaction commit, and
  `Dataset::lifecycle_report`.
- Do not access private fields or add production-only test hooks.

- [ ] Add a fresh-dataset test asserting version 0, one initial manifest object with nonzero
      `manifest_bytes` equal to `current_manifest_bytes`, zero data/segment/tombstone counts, and
      zero data/reachable/orphan bytes for the initial manifest state.
- [ ] Add a row-commit test asserting one reachable data file, its manifest byte length, and physical
      row count.
- [ ] Add a vector-commit test asserting segment count/bytes are counted separately from row-file
      counts and both are reachable.
- [ ] Add a multi-commit test asserting manifest object count/bytes grow while all current manifest
      data files remain reachable.
- [ ] Add a failed-preparation/orphan test using the existing test-only failure seam or direct
      `LocalFs` object creation to prove the object is invisible to the captured manifest and only
      classified as an orphan candidate.
- [ ] Add missing-reachable-object and unsafe-name tests that assert typed errors and no silent
      reclassification.
- [ ] Add a concurrent-commit test that captures a report, completes a later commit, and verifies the
      previously returned report remains unchanged.
- [ ] Run `cargo test -p strata-txn --test lifecycle_inventory -- --nocapture` and record the result.
- [ ] Commit as `test: cover lifecycle inventory reachability and candidates`.

### Task 4: Document the Phase 3 diagnostic boundary

**Files:**

- Modify: `docs/status.md`
- Modify: `docs/architecture.md`
- Modify: `docs/roadmap.md`

**Interfaces:**

- Document `Dataset::lifecycle_report()` as implemented diagnostic evidence only.
- State that orphan candidates may include active-snapshot data and temporary/unknown files, and are
  not safe for deletion without a later retention/cleanup design.

- [ ] Update the capability ledger and architecture API list with the report and its limitations.
- [ ] Keep Phase 3 status `Proposed` and Phase 1 `Partial — blocked`.
- [ ] Add links to the design document and the focused integration test.
- [ ] Run stale-claim scans, relative-link checks, `git diff --check`, and `cargo fmt --check`.
- [ ] Commit as `docs: record phase 3 lifecycle diagnostics boundary`.

### Task 5: Final proportional verification and handoff

**Files:**

- No additional files unless verification exposes an issue in the approved slice.

- [ ] Run `cargo test --workspace --no-default-features -j 1`.
- [ ] Run `cargo check --workspace`.
- [ ] Run `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`.
- [ ] Run `cargo fmt --check` and `git diff --check`.
- [ ] Inspect the final diff and confirm `.serena/` or other machine-local state is not included.
- [ ] Obtain an independent Terra review of the slice; reserve Sol for the final branch review only.
- [ ] Return exact files, commits, test commands, results, and any remaining boundary limitations.

## Completion Criteria

The slice is complete only when the typed API, end-to-end tests, docs, and proportional verification
pass, and the report remains read-only and snapshot-anchored. Completion does not mean Phase 3 is
complete and does not authorize deletion or reclamation.
