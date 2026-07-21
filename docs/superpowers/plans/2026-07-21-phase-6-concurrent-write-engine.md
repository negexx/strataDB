# Phase 6 Concurrent Write Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Phase 5's single-writer assumption in `strata-txn` with real optimistic concurrency control: write-write conflict detection, an atomic commit critical section, and a public `update`/`delete` API — matching the design at `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`.

**Architecture:** A `Mutex`-guarded commit critical section (Approach A from the design) wraps only conflict-check + graph-delta-apply + manifest-write + `ArcSwap` swap — data-file writes stay outside it via a `next_row_id_counter: AtomicU64` that removes the row-id collision race independently of the mutex. Conflicts are detected by walking a small in-memory `CommitLog` of recently-committed write-sets.

**Tech Stack:** Rust 1.90, `arc-swap`, `im` (persistent HashSet), `loom` 0.7 (dev-dependency, crate-scoped cfg), `thiserror`, `criterion` for the exit-evidence benchmark.

## Global Constraints

- Isolation level is snapshot isolation; do not add serializability/read-set machinery (ADR 0003, phase-0 spec §1's "Explicit non-goal"). Write-write conflict detection only.
- No automatic retry on conflict (architecture.md Non-Goals). `commit()` returns `Err(TxnError::Conflict{..})`; the caller decides.
- `unsafe` is not needed anywhere in this plan — do not introduce it.
- `unwrap()`/`expect()` are `clippy::warn`, not banned — fine in tests (with `#[allow(clippy::unwrap_used, clippy::expect_used)]` at the test-module level, matching this file's existing pattern), never in non-test code added by this plan.
- Every task's tests must pass `cargo test -p strata-txn` before moving on; the full workspace gate (`cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`) must be clean before this phase is marked done, per this project's CLAUDE.md "What done means."
- Loom-gated tests: **only** `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`, then run the produced test binary filtered to the target module. Never a workspace-wide `RUSTFLAGS="--cfg loom"` invocation — it breaks `strata-index`'s own build (`.claude/rules/concurrency-txn-layer.md`).
- Per this project's CLAUDE.md model-dispatch table: this work is in `crates/txn/`, the flagship subsystem — escalate to Fable 5 (or Opus 4.8) for Task 6 (the core `commit()` rewrite) specifically, since a wrong call there is expensive to undo. The rest of the tasks are mechanical enough for the default tier.
- An Opus reviewer-subagent sign-off is required before this phase is marked done — not optional, not per-task-skippable (CLAUDE.md "What done means").

## File Structure

- **Modify `crates/storage/src/manifest.rs`:** `Manifest` gains `pub tombstones: Vec<u64>`. Locked-in decomposition decision (not in the original design doc — surfaced while planning): a delete-only transaction has no natural schema to fabricate an empty data file from (there is no dataset-wide fixed schema anywhere in this codebase — each data file carries its own physical schema, and callers pass their own logical schema to `scan`). Storing tombstones as plain `u64`s directly in the manifest (small, always loaded anyway) avoids inventing a schema-less empty data file and avoids reusing/rewriting an existing immutable delta-log file. `replay_index` (Task 4) reads from both `manifest.tombstones` (new commits) and existing delta-log `Tombstone` entries (old test fixtures, e.g. `replay_index_applies_tombstone_entries_from_the_delta_log`) — this is additive, not a breaking change to the existing delta-log-based path.
- **Modify `crates/txn/src/error.rs`:** new `TxnError::Conflict { contested_row_ids: Vec<u64> }` variant.
- **Create `crates/txn/src/commit_log.rs`:** `CommitLog` — bounded ring buffer of `(version, write_set)`, with a `conflicts_with` query method. Pure data structure, no I/O, independently unit-testable.
- **Modify `crates/txn/src/dataset.rs`:** `Transaction.write_set`, `update()`/`delete()`; `Dataset.next_row_id_counter`; `Dataset.commit_lock` (loom cfg-shim); `Dataset.write_attempt_counter` (a second, independent atomic counter — locked in during planning, not in the original design doc: two truly concurrent transactions both writing data files before reaching the lock need a collision-free filename source that has nothing to do with the real commit version, since both may share the same stale `base_manifest.version`; see Task 6); `commit()` rewrite; `write_pending_batches` takes the atomic counter instead of reading `manifest.next_row_id`; `replay_index` reads `manifest.tombstones` too; module-level doc comment update (it currently says Phase 6 conflict detection "is[not] implemented yet" — now false); new loom tests.
- **Modify `crates/txn/src/lib.rs`:** add `pub mod commit_log;`.
- **Create `bench/benches/concurrent_commit_bench.rs`** + **modify `bench/Cargo.toml`:** throughput benchmark, the exit evidence for the mutex-scoping decision.

---

### Task 1: `Manifest.tombstones` field

**Files:**
- Modify: `crates/storage/src/manifest.rs`
- Test: `crates/storage/src/manifest.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `Manifest.tombstones: Vec<u64>` (public field), `Manifest::empty()` initializes it to `Vec::new()`.

- [ ] **Step 1: Write the failing test**

Add to `crates/storage/src/manifest.rs`'s existing `#[cfg(test)] mod tests` block (check the file's current bottom for the exact existing `mod tests` — add this test inside it; if no test module exists yet, add one following this crate's usual `#[cfg(test)] mod tests { use super::*; ... }` shape):

```rust
    #[test]
    fn empty_manifest_has_no_tombstones() {
        let manifest = Manifest::empty();
        assert!(manifest.tombstones.is_empty());
    }

    #[test]
    fn manifest_with_tombstones_round_trips_through_json() {
        let mut manifest = Manifest::empty();
        manifest.tombstones = vec![3, 7, 12];
        let json = serde_json::to_vec(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.tombstones, vec![3, 7, 12]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-storage manifest::tests -- --nocapture`
Expected: FAIL with "no field `tombstones` on type `Manifest`" (compile error).

- [ ] **Step 3: Add the field**

In `crates/storage/src/manifest.rs`, modify the `Manifest` struct and its `empty()` constructor:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Accumulated across every committed version so far.
    pub data_files: Vec<DataFileEntry>,
    /// The row-id to assign to the next inserted row, dataset-wide. Never
    /// resets, never reused — see
    /// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
    pub next_row_id: u64,
    /// Row-ids tombstoned (deleted, or superseded by `update`) as of this
    /// version. Accumulated across every committed version, same as
    /// `data_files` — see Phase 6's design doc for why this lives directly
    /// in the manifest rather than a delta-log file: a delete-only
    /// transaction has no data file to attach one to, since there is no
    /// dataset-wide fixed schema to fabricate an empty batch from.
    #[serde(default)]
    pub tombstones: Vec<u64>,
}

impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-storage manifest::tests -- --nocapture`
Expected: PASS (2 new tests, plus every pre-existing `Manifest`-constructing test in this crate and `strata-txn` still compiles — `Manifest::empty()` is the only constructor used anywhere, so no other call site needs updating).

- [ ] **Step 5: Full-crate regression check and commit**

Run: `cargo test -p strata-storage && cargo test -p strata-txn`
Expected: PASS (confirms `#[serde(default)]` means nothing that constructs `Manifest` via struct-literal elsewhere broke — if it did, add `tombstones: Vec::new()` there too before committing).

```bash
git add crates/storage/src/manifest.rs
git commit -m "feat(storage): add Manifest.tombstones for Phase 6 delete/update support"
```

---

### Task 2: `TxnError::Conflict` variant

**Files:**
- Modify: `crates/txn/src/error.rs`

**Interfaces:**
- Produces: `TxnError::Conflict { contested_row_ids: Vec<u64> }`.

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/error.rs`'s existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn conflict_error_names_contested_rows() {
        let err = TxnError::Conflict {
            contested_row_ids: vec![5, 9],
        };
        assert_eq!(
            err.to_string(),
            "conflict: [5, 9] were modified by another transaction"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-txn error::tests::conflict_error_names_contested_rows`
Expected: FAIL with "no variant named `Conflict`" (compile error).

- [ ] **Step 3: Add the variant**

In `crates/txn/src/error.rs`, add to the `TxnError` enum (after the existing `SchemaMismatch` variant):

```rust
    #[error("conflict: {contested_row_ids:?} were modified by another transaction")]
    Conflict { contested_row_ids: Vec<u64> },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-txn error::tests::conflict_error_names_contested_rows`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/txn/src/error.rs
git commit -m "feat(txn): add TxnError::Conflict variant"
```

---

### Task 3: `CommitLog` data structure

**Files:**
- Create: `crates/txn/src/commit_log.rs`
- Modify: `crates/txn/src/lib.rs` (register the module)

**Interfaces:**
- Consumes: nothing outside `std`.
- Produces: `CommitLog::new(capacity: usize) -> CommitLog`, `CommitLog::push(&mut self, version: u64, write_set: Vec<u64>)`, `CommitLog::conflicts_with(&self, since_version: u64, up_to_version: u64, write_set: &[u64]) -> ConflictCheck` where `ConflictCheck` is an enum `{ Clean, Conflict(Vec<u64>), InsufficientHistory }` — `Dataset::commit` (Task 6) matches on this three-way result: `Clean` proceeds, `Conflict(rows)` returns `TxnError::Conflict`, `InsufficientHistory` (the log wrapped past `since_version`) is also treated as a conflict by the caller but is a distinct variant so tests can assert on it specifically (per the design doc's "conservative conflict" rule, and this project's convention of not stringly-typing distinguishable outcomes).

- [ ] **Step 1: Write the failing tests**

Create `crates/txn/src/commit_log.rs`:

```rust
//! A bounded, in-memory ring buffer of recently-committed transactions'
//! write-sets — see
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! §4. `Snapshot`s don't retain write-set history once unreferenced, so
//! conflict-checking "did anything land between my read-version and now
//! touch my rows" needs its own structure independent of `Snapshot`'s
//! lifetime.

use std::collections::VecDeque;

/// Outcome of [`CommitLog::conflicts_with`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictCheck {
    /// No committed transaction in the checked range touched any of the
    /// queried row-ids.
    Clean,
    /// At least one committed transaction's write-set intersected the
    /// queried write-set. Carries every contested row-id, not just the
    /// first — matches `TxnError::Conflict`'s contract.
    Conflict(Vec<u64>),
    /// The log's oldest entry is newer than `since_version` — some
    /// commits in the requested range have already been evicted, so
    /// "clean" cannot be proven. Treated as a conflict by the caller (see
    /// design doc §4's "conservative conflict" rule), kept as a distinct
    /// variant so tests can assert on it specifically.
    InsufficientHistory,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_log_reports_clean_for_any_range() {
        let log = CommitLog::new(4);
        assert_eq!(log.conflicts_with(0, 0, &[1, 2]), ConflictCheck::Clean);
    }

    #[test]
    fn disjoint_write_sets_are_clean() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10, 11]);
        assert_eq!(log.conflicts_with(0, 1, &[20, 21]), ConflictCheck::Clean);
    }

    #[test]
    fn overlapping_write_sets_conflict_and_name_every_contested_row() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10, 11]);
        log.push(2, vec![10, 30]);
        let result = log.conflicts_with(0, 2, &[10, 20]);
        match result {
            ConflictCheck::Conflict(mut rows) => {
                rows.sort_unstable();
                assert_eq!(rows, vec![10]);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn versions_outside_the_requested_range_are_ignored() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10]);
        // Requested range is (5, 6] — version 1 predates it and must not
        // be treated as a conflict even though its write-set overlaps.
        assert_eq!(log.conflicts_with(5, 6, &[10]), ConflictCheck::Clean);
    }

    #[test]
    fn log_wraparound_reports_insufficient_history() {
        let mut log = CommitLog::new(2);
        log.push(1, vec![10]);
        log.push(2, vec![20]);
        log.push(3, vec![30]); // evicts version 1's entry
        assert_eq!(
            log.conflicts_with(0, 3, &[999]),
            ConflictCheck::InsufficientHistory
        );
    }

    #[test]
    fn requesting_only_still_present_versions_after_wraparound_is_fine() {
        let mut log = CommitLog::new(2);
        log.push(1, vec![10]);
        log.push(2, vec![20]);
        log.push(3, vec![30]); // evicts version 1
        // since_version=2 only needs versions >2 to be present, which they are.
        assert_eq!(log.conflicts_with(2, 3, &[999]), ConflictCheck::Clean);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn commit_log::tests`
Expected: FAIL — `CommitLog` is not defined yet (compile error).

- [ ] **Step 3: Implement `CommitLog`**

Add above the `#[cfg(test)]` block in `crates/txn/src/commit_log.rs`:

```rust
pub struct CommitLog {
    capacity: usize,
    entries: VecDeque<(u64, Vec<u64>)>,
}

impl CommitLog {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::with_capacity(capacity),
        }
    }

    /// Records a newly-committed transaction's version and write-set,
    /// evicting the oldest entry if at capacity.
    pub fn push(&mut self, version: u64, write_set: Vec<u64>) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((version, write_set));
    }

    /// Checks whether any committed transaction with version in
    /// `(since_version, up_to_version]` touched any row-id in
    /// `write_set`. See [`ConflictCheck`] for the three-way result.
    #[must_use]
    pub fn conflicts_with(
        &self,
        since_version: u64,
        up_to_version: u64,
        write_set: &[u64],
    ) -> ConflictCheck {
        if up_to_version <= since_version {
            return ConflictCheck::Clean;
        }
        if let Some((oldest_version, _)) = self.entries.front() {
            if *oldest_version > since_version + 1 && !self.entries.is_empty() {
                return ConflictCheck::InsufficientHistory;
            }
        } else {
            // Empty log but a non-empty range was requested: only "clean"
            // if nothing could possibly have committed in that range,
            // i.e. since_version == up_to_version handled above. An empty
            // log with a real gap to cover has no history at all.
            return ConflictCheck::InsufficientHistory;
        }

        let mut contested: Vec<u64> = Vec::new();
        for (version, entry_write_set) in &self.entries {
            if *version <= since_version || *version > up_to_version {
                continue;
            }
            for row_id in entry_write_set {
                if write_set.contains(row_id) && !contested.contains(row_id) {
                    contested.push(*row_id);
                }
            }
        }
        if contested.is_empty() {
            ConflictCheck::Clean
        } else {
            ConflictCheck::Conflict(contested)
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-txn commit_log::tests`
Expected: PASS (6 tests). If `empty_log_reports_clean_for_any_range` fails against the `InsufficientHistory` branch, double check the `since_version == up_to_version` short-circuit runs first — that's what makes an empty log correctly report `Clean` for a zero-width range instead of falling into the "no history at all" branch.

- [ ] **Step 5: Register the module and commit**

In `crates/txn/src/lib.rs`, add `pub mod commit_log;` after the existing `pub mod dataset;` line.

Run: `cargo test -p strata-txn`
Expected: PASS (all existing tests plus the 6 new ones).

```bash
git add crates/txn/src/commit_log.rs crates/txn/src/lib.rs
git commit -m "feat(txn): add CommitLog for Phase 6 conflict-range tracking"
```

---

### Task 4: `Transaction.write_set` + `update()`/`delete()` API

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `Manifest.tombstones` (Task 1).
- Produces: `Transaction.write_set: Vec<u64>` (private field), `Transaction::update(&mut self, row_id: u64, batch: RecordBatch)`, `Transaction::delete(&mut self, row_id: u64)`. `Transaction` also gains `pending_tombstones: Vec<u64>` (private) — the row-ids `delete`/`update` queue for tombstoning, applied at commit time by writing them into the new manifest's `tombstones` list and merging them into the in-memory tombstone set (mirrors exactly how `pending: Vec<RecordBatch>` already works for inserts). Later tasks (6, 7) rely on `write_set` being populated by both methods.

- [ ] **Step 1: Write the failing tests**

Add to `crates/txn/src/dataset.rs`'s existing `#[cfg(test)] mod tests` block (find the existing test module — it's large; add these near the other `commit`/tombstone-related tests):

```rust
    #[test]
    fn delete_tombstones_a_row_and_it_becomes_invisible() {
        let dir = temp_dir("delete-basic");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();

        assert!(!ds.snapshot().is_visible(0));
    }

    #[test]
    fn update_tombstones_old_row_and_makes_new_row_visible() {
        let dir = temp_dir("update-basic");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let replacement = vector_batch(vec![1i64], cluster_vectors(1, [5.0, 5.0, 5.0], 0.0));
        let mut txn = ds.begin();
        txn.update(0, replacement);
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(!snapshot.is_visible(0), "old row must be tombstoned");
        assert!(snapshot.is_visible(1), "replacement row must be visible");
    }

    #[test]
    fn tombstones_persist_across_reopen() {
        let dir = temp_dir("delete-persists");
        let ds = Dataset::create(&dir).unwrap();

        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut txn = ds.begin();
        txn.insert(batch);
        txn.commit().unwrap();

        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();
        drop(ds);

        let reopened = Dataset::open(&dir).unwrap();
        assert!(!reopened.snapshot().is_visible(0));
    }
```

Check this file's existing test helpers (`temp_dir`, `vector_batch`, `cluster_vectors`) are already defined near the top of the `#[cfg(test)] mod tests` block — this file's existing vector-search tests already use them (confirmed via the `replay_index_applies_tombstone_entries_from_the_delta_log` test read during design). Reuse them as-is; do not redefine.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn dataset::tests::delete_tombstones_a_row_and_it_becomes_invisible dataset::tests::update_tombstones_old_row_and_makes_new_row_visible dataset::tests::tombstones_persist_across_reopen`
Expected: FAIL — `delete`/`update` methods don't exist yet (compile error).

- [ ] **Step 3: Implement `write_set`, `pending_tombstones`, `update`, `delete`**

Modify the `Transaction` struct definition:

```rust
pub struct Transaction {
    dir: PathBuf,
    base_manifest: Manifest,
    graph: Arc<HnswIndex>,
    tombstones: im::HashSet<u64>,
    pending: Vec<RecordBatch>,
    pending_tombstones: Vec<u64>,
    write_set: Vec<u64>,
    current: Arc<ArcSwap<Snapshot>>,
}
```

Update `Dataset::begin` to initialize the two new fields:

```rust
    pub fn begin(&self) -> Transaction {
        let snapshot = self.snapshot();
        Transaction {
            dir: self.dir.clone(),
            base_manifest: snapshot.manifest.as_ref().clone(),
            graph: Arc::clone(&snapshot.graph),
            tombstones: snapshot.tombstones.as_ref().clone(),
            pending: Vec::new(),
            pending_tombstones: Vec::new(),
            write_set: Vec::new(),
            current: Arc::clone(&self.current),
        }
    }
```

Add `update`/`delete` to `impl Transaction` (near `insert`):

```rust
    /// Tombstones `row_id`, making it invisible to every snapshot taken
    /// after this transaction commits — see spec §2. Buffered, not
    /// applied until [`Transaction::commit`] succeeds, same as
    /// [`Transaction::insert`].
    pub fn delete(&mut self, row_id: u64) {
        self.pending_tombstones.push(row_id);
        self.write_set.push(row_id);
    }

    /// Tombstones `row_id` and inserts `batch` as its replacement, within
    /// the same transaction — commits atomically as one unit. Equivalent
    /// to calling [`Transaction::delete`] then [`Transaction::insert`],
    /// provided as one call because that's the common case and keeps the
    /// write-set bookkeeping (used by conflict detection) obviously
    /// correct at the call site rather than relying on the caller to
    /// remember both.
    pub fn update(&mut self, row_id: u64, batch: RecordBatch) {
        self.delete(row_id);
        self.insert(batch);
    }
```

- [ ] **Step 4: Wire tombstone persistence through `commit()` and `replay_index`**

In `Transaction::commit` (still the Phase 5 version at this point — Task 6 rewrites its concurrency behavior, this step only makes today's single-writer version aware of `pending_tombstones`), after the existing `let mut tombstones = self.tombstones;` line and its `for delta in &deltas` loop, add:

```rust
        for row_id in &self.pending_tombstones {
            tombstones.insert(*row_id);
        }
        manifest.tombstones.extend(self.pending_tombstones.iter().copied());
```

Place this immediately after the existing delta-application loop, before `commit_manifest(&self.dir, &manifest)?;`.

In `replay_index` (used by `Dataset::open`), add manifest-level tombstones to the reconstructed set, after the existing `for entry in &manifest.data_files` loop and before `Ok((index, tombstones))`:

```rust
    for row_id in &manifest.tombstones {
        tombstones.insert(*row_id);
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p strata-txn dataset::tests::delete_tombstones_a_row_and_it_becomes_invisible dataset::tests::update_tombstones_old_row_and_makes_new_row_visible dataset::tests::tombstones_persist_across_reopen`
Expected: PASS

- [ ] **Step 6: Full-crate regression check and commit**

Run: `cargo test -p strata-txn && cargo clippy -p strata-txn --all-targets -- -D warnings`
Expected: PASS clean (watch for `clippy::pedantic` flagging the new fields/methods — add doc comments if clippy's `missing_docs_in_private_items`-style lints fire, matching this file's existing density of doc comments).

```bash
git add crates/txn/src/dataset.rs
git commit -m "feat(txn): add Transaction::update/delete with write-set tracking"
```

---

### Task 5: `next_row_id_counter` — decouple row-id allocation from the commit lock

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Dataset.next_row_id_counter: AtomicU64` (private field, initialized from the manifest's `next_row_id` in both `create` and `open`). `write_pending_batches` signature changes from taking `manifest: &mut Manifest` to taking `row_id_counter: &AtomicU64, data_files: &mut Vec<DataFileEntry>` — Task 6 depends on this exact new signature.

This task intentionally does **not** yet fix the concurrent-collision bug end-to-end (that requires Task 6's lock too — seeing two threads insert concurrently and assert no collision would still be flaky before the lock exists, since the *other* Phase-5 bug, the unconditional `ArcSwap::store`, is still present). This task's own test proves the counter itself hands out non-overlapping ranges under concurrent access, in isolation from the rest of `commit`.

- [ ] **Step 1: Write the failing test**

Add near the top of `crates/txn/src/dataset.rs`'s test module, or as a new small test:

```rust
    #[test]
    fn row_id_counter_hands_out_non_overlapping_ranges_under_concurrent_fetch_add() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;

        let counter = AtomicU64::new(0);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let counter_ref: &AtomicU64 = unsafe { std::mem::transmute(&counter) };
                thread::spawn(move || counter_ref.fetch_add(10, Ordering::SeqCst))
            })
            .collect();
        let mut bases: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        bases.sort_unstable();
        for (i, base) in bases.iter().enumerate() {
            assert_eq!(*base, (i as u64) * 10, "ranges must be contiguous, non-overlapping");
        }
    }
```

Note: this draft test uses `unsafe { transmute }` to fake a `'static` reference for the thread spawn purely to exercise `AtomicU64::fetch_add`'s contract in isolation — that's not how the real counter will be used (it's a `Dataset` field, and `Dataset` is `Clone` + already `Arc`-backed internally, so real usage in Task 6 borrows it through a cloned `Dataset`/`Arc`, no `transmute` involved). Prefer `std::thread::scope` instead to borrow the stack-local `counter` safely without `unsafe` — rewrite the test body using `thread::scope`:

```rust
    #[test]
    fn row_id_counter_hands_out_non_overlapping_ranges_under_concurrent_fetch_add() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let counter = AtomicU64::new(0);
        let mut bases: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| counter.fetch_add(10, Ordering::SeqCst)))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        bases.sort_unstable();
        for (i, base) in bases.iter().enumerate() {
            assert_eq!(*base, (i as u64) * 10, "ranges must be contiguous, non-overlapping");
        }
    }
```

Use the `thread::scope` version — it needs no `unsafe` and matches this workspace's "safe Rust by default" convention.

- [ ] **Step 2: Run test to verify it passes as-is**

Run: `cargo test -p strata-txn dataset::tests::row_id_counter_hands_out_non_overlapping_ranges_under_concurrent_fetch_add`
Expected: PASS immediately — this test only exercises `std::sync::atomic::AtomicU64`, not any new `Dataset` field yet. It's here to lock in the contract before wiring it into `Dataset`/`write_pending_batches`, and will keep passing unmodified through the rest of this task.

- [ ] **Step 3: Add `next_row_id_counter` to `Dataset` and update `create`/`open`**

Modify the `Dataset` struct:

```rust
#[derive(Clone)]
pub struct Dataset {
    dir: PathBuf,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
}
```

(`Arc`-wrapped so `Dataset::clone()` — already derived, used throughout this file's tests and any multi-threaded caller — shares one counter across clones, same sharing model `current: Arc<ArcSwap<Snapshot>>` already uses.)

Add the import at the top of the file, alongside the existing `use std::sync::Arc;`:

```rust
use std::sync::atomic::AtomicU64;
```

In `Dataset::create`, after `let manifest = Manifest::empty();`, initialize the counter and include it in the returned `Self`:

```rust
    pub fn create(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if read_current(&dir)?.is_some() {
            return Err(TxnError::AlreadyExists(dir));
        }
        std::fs::create_dir_all(dir.join("data"))?;
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest)?;
        let graph = new_hnsw_index(0)?;
        let next_row_id_counter = Arc::new(AtomicU64::new(manifest.next_row_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(im::HashSet::new()),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            next_row_id_counter,
        })
    }
```

In `Dataset::open`, same addition after `let (graph, tombstones) = replay_index(&dir, &manifest)?;`:

```rust
        let next_row_id_counter = Arc::new(AtomicU64::new(manifest.next_row_id));
        let snapshot = Snapshot {
            dir: dir.clone(),
            version: manifest.version,
            watermark: manifest.next_row_id.saturating_sub(1),
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(tombstones),
        };
        Ok(Self {
            dir,
            current: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            next_row_id_counter,
        })
```

- [ ] **Step 4: Run tests to verify the crate still compiles and passes**

Run: `cargo test -p strata-txn`
Expected: PASS — `write_pending_batches` hasn't been touched yet in this step, so it still reads/writes `manifest.next_row_id` as before; the new field just rides along unused by the commit path so far. If `Transaction` also needs the counter (it will, next step), this step alone should still compile since `Transaction` doesn't reference it yet.

- [ ] **Step 5: Thread the counter into `Transaction` and `write_pending_batches`**

`Transaction` needs access to the same counter its `Dataset` uses. Add a field and wire it from `begin`:

```rust
pub struct Transaction {
    dir: PathBuf,
    base_manifest: Manifest,
    graph: Arc<HnswIndex>,
    tombstones: im::HashSet<u64>,
    pending: Vec<RecordBatch>,
    pending_tombstones: Vec<u64>,
    write_set: Vec<u64>,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
}
```

```rust
    pub fn begin(&self) -> Transaction {
        let snapshot = self.snapshot();
        Transaction {
            dir: self.dir.clone(),
            base_manifest: snapshot.manifest.as_ref().clone(),
            graph: Arc::clone(&snapshot.graph),
            tombstones: snapshot.tombstones.as_ref().clone(),
            pending: Vec::new(),
            pending_tombstones: Vec::new(),
            write_set: Vec::new(),
            current: Arc::clone(&self.current),
            next_row_id_counter: Arc::clone(&self.next_row_id_counter),
        }
    }
```

Change `write_pending_batches`'s signature and body to allocate from the counter instead of `manifest.next_row_id`, and to no longer take `manifest` at all — just the growing `data_files` list:

```rust
    /// `attempt_id` is a collision-free filename-uniqueness token from
    /// `Dataset.write_attempt_counter` — **not** a manifest version. It
    /// exists only so concurrent callers never write to the same path;
    /// see `Transaction::commit` (Task 6) for why it can't be derived
    /// from `base_manifest.version` instead.
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        attempt_id: u64,
        row_id_counter: &AtomicU64,
        data_files: &mut Vec<DataFileEntry>,
    ) -> Result<Vec<DeltaEntry>> {
        let mut all_deltas = Vec::new();
        for (i, batch) in pending.iter().enumerate() {
            let stats = compute_stats(batch);
            let num_rows = u64::try_from(batch.num_rows())?;
            let row_id_base = row_id_counter.fetch_add(num_rows, std::sync::atomic::Ordering::SeqCst);
            row_id_base.checked_add(num_rows).ok_or_else(|| {
                TxnError::ManifestOverflow(format!("next_row_id {row_id_base} + {num_rows}"))
            })?;

            let deltas = build_delta_entries(batch, row_id_base)?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_row_id)?;
            let file_name = format!("{attempt_id:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;

            let delta_file_name = format!("{attempt_id:020}-{i}.deltalog");
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
            all_deltas.extend(deltas);
        }
        Ok(all_deltas)
    }
```

Note the overflow check now runs *after* `fetch_add` (unlike the original `checked_add`-before-mutating version) — the counter has already advanced by the time overflow is detected. This is intentional and safe: row-ids are never reused (manifest doc comment, phase-0 spec §8), so a small abandoned gap from a failed batch is harmless, and checking-then-erroring here (rather than trying to roll the counter back, which isn't atomically expressible) keeps the fast path lock-free. Document this via a comment at the check site:

```rust
            // fetch_add already advanced the counter before this check —
            // intentional. Row-ids are never reused (see Manifest.next_row_id's
            // doc comment), so an abandoned gap from a failed batch is
            // harmless, and there is no atomic way to "undo" a fetch_add if
            // we checked first instead.
```

Update `Transaction::commit`'s call site (currently `Self::write_pending_batches(&self.pending, &data_dir, new_version, &mut manifest)?;`) to the new signature — this edit is superseded by Task 6's full rewrite of `commit`, but must compile as an intermediate step here:

```rust
        let deltas = Self::write_pending_batches(
            &self.pending,
            &data_dir,
            new_version,
            &self.next_row_id_counter,
            &mut manifest.data_files,
        )?;
        manifest.next_row_id = self.next_row_id_counter.load(std::sync::atomic::Ordering::SeqCst);
```

(Replacing the old `manifest.version = new_version;` line's neighbor — keep `manifest.version = new_version;` itself unchanged in this task; Task 6 changes how `new_version` itself is computed.)

- [ ] **Step 6: Run full crate tests**

Run: `cargo test -p strata-txn`
Expected: PASS — every existing insert/scan/commit test still passes, now allocating row-ids from the atomic counter instead of `manifest.next_row_id` directly, with equivalent single-threaded behavior (the counter starts at the same value `manifest.next_row_id` would have).

- [ ] **Step 7: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "feat(txn): decouple row-id allocation into an atomic counter"
```

---

### Task 6: `commit_lock` + real conflict-detecting `commit()` (core task — escalate model tier per Global Constraints)

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `CommitLog`/`ConflictCheck` (Task 3), `Transaction.write_set` (Task 4), `next_row_id_counter` (Task 5), `TxnError::Conflict` (Task 2).
- Produces: `Dataset.commit_lock` (private), rewritten `Transaction::commit(self) -> Result<()>` whose new failure mode is `Err(TxnError::Conflict{..})` in addition to its existing error cases.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn concurrent_delete_of_the_same_row_conflicts() {
        let dir = temp_dir("commit-lock-conflict");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(0);

        txn_a.commit().unwrap();
        let result = txn_b.commit();
        match result {
            Err(TxnError::Conflict { contested_row_ids }) => {
                assert_eq!(contested_row_ids, vec![0]);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_delete_of_disjoint_rows_both_commit() {
        let dir = temp_dir("commit-lock-no-conflict");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(
            vec![1i64, 2i64],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        );
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(1);

        txn_a.commit().unwrap();
        txn_b.commit().unwrap();

        let snapshot = ds.snapshot();
        assert!(!snapshot.is_visible(0));
        assert!(!snapshot.is_visible(1));
    }

    #[test]
    fn commit_version_is_sourced_from_latest_state_not_stale_base_manifest() {
        // Regression test for the original unconditional
        // `base_manifest.version + 1` bug: txn_a and txn_b both begin
        // against version 0; txn_a commits (version 1); txn_b's disjoint
        // write must land at version 2, not also attempt version 1.
        let dir = temp_dir("commit-version-source");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(
            vec![1i64, 2i64],
            cluster_vectors(2, [0.0, 0.0, 0.0], 0.01),
        );
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();
        assert_eq!(ds.current_version(), 1);

        let mut txn_a = ds.begin();
        txn_a.delete(0);
        let mut txn_b = ds.begin();
        txn_b.delete(1);

        txn_a.commit().unwrap();
        assert_eq!(ds.current_version(), 2);
        txn_b.commit().unwrap();
        assert_eq!(ds.current_version(), 3);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn dataset::tests::concurrent_delete_of_the_same_row_conflicts dataset::tests::concurrent_delete_of_disjoint_rows_both_commit dataset::tests::commit_version_is_sourced_from_latest_state_not_stale_base_manifest`
Expected: FAIL — `concurrent_delete_of_the_same_row_conflicts` fails because `commit()` doesn't return `Conflict` yet (both commits currently succeed silently, corrupting state); the version-source test likely fails or panics depending on exactly how the old unconditional-store code races with itself even single-threaded-sequentially (txn_b was built from the same stale version 0 base_manifest, so its own `commit()` would previously also compute `new_version = 0 + 1 = 1`, colliding with txn_a's already-written version-1 manifest file).

- [ ] **Step 3: Add `commit_lock` to `Dataset`**

Add the loom cfg-shim imports at the top of `crates/txn/src/dataset.rs`, alongside the existing `use std::sync::Arc;` and the `AtomicU64` import added in Task 5:

```rust
#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;
```

Add the field to `Dataset` and initialize it (a capacity of 256 is a reasonable default — generous enough that ordinary workloads won't hit `InsufficientHistory`, small enough to be a trivial memory cost; not a tunable exposed publicly yet, per YAGNI). Also add `write_attempt_counter: Arc<AtomicU64>` here — a second atomic counter, independent of both `next_row_id_counter` and the real manifest version, used purely to generate a collision-free filename prefix for each commit *attempt's* data/delta-log files. It exists because the pre-lock file-write phase (Step 4 below) needs a unique scratch value before it can know the real commit version, and two concurrent transactions sharing the same stale `base_manifest.version` must not compute the same value:

```rust
const COMMIT_LOG_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct Dataset {
    dir: PathBuf,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
    write_attempt_counter: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<CommitLog>>,
}
```

Add `use crate::commit_log::{CommitLog, ConflictCheck};` near the top with the other `use crate::` lines.

In both `Dataset::create` and `Dataset::open`, add to the returned `Self { .. }` literal:

```rust
            write_attempt_counter: Arc::new(AtomicU64::new(0)),
            commit_lock: Arc::new(Mutex::new(CommitLog::new(COMMIT_LOG_CAPACITY))),
```

Add the same two fields, `Arc`-cloned, to `Transaction` and to `Dataset::begin`'s construction of it:

```rust
pub struct Transaction {
    dir: PathBuf,
    base_manifest: Manifest,
    graph: Arc<HnswIndex>,
    tombstones: im::HashSet<u64>,
    pending: Vec<RecordBatch>,
    pending_tombstones: Vec<u64>,
    write_set: Vec<u64>,
    current: Arc<ArcSwap<Snapshot>>,
    next_row_id_counter: Arc<AtomicU64>,
    write_attempt_counter: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<CommitLog>>,
}
```

```rust
            write_attempt_counter: Arc::clone(&self.write_attempt_counter),
            commit_lock: Arc::clone(&self.commit_lock),
```
(added as the last two fields in `begin`'s `Transaction { .. }` literal.)

- [ ] **Step 4: Rewrite `Transaction::commit`**

Replace the entire body of `Transaction::commit` with:

```rust
    pub fn commit(self) -> Result<()> {
        let data_dir = data_subdir(&self.dir);
        std::fs::create_dir_all(&data_dir)?;

        // Data-file writes happen before the lock — they touch only
        // files unique to this transaction and never collide with a
        // concurrent transaction's own writes. The filename prefix comes
        // from write_attempt_counter, NOT base_manifest.version + 1: two
        // truly concurrent transactions can share the same stale
        // base_manifest.version, which would make them compute the same
        // "next version" and collide on the same filename before either
        // reaches commit_lock. write_attempt_counter is unique per
        // attempt regardless of version, so no such collision is
        // possible. See design doc §3 (data-file writes need no
        // conflict information to proceed) — this counter is what makes
        // that safe to do outside the lock at all.
        let attempt_id = self
            .write_attempt_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut data_files = self.base_manifest.data_files.clone();
        let deltas = Self::write_pending_batches(
            &self.pending,
            &data_dir,
            attempt_id,
            &self.next_row_id_counter,
            &mut data_files,
        )?;
        strata_storage::sync_dir(&data_dir)?;
        validate_delta_dimensions(&deltas, &self.graph)?;

        // Everything from here is the tightly-scoped critical section:
        // re-read latest state, conflict-check, apply, commit, swap. See
        // design doc §5.
        let mut commit_log = self.commit_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

        let latest_snapshot = self.current.load_full();
        let latest_version = latest_snapshot.version;

        match commit_log.conflicts_with(self.base_manifest.version, latest_version, &self.write_set) {
            ConflictCheck::Clean => {}
            ConflictCheck::Conflict(contested_row_ids) => {
                return Err(TxnError::Conflict { contested_row_ids });
            }
            ConflictCheck::InsufficientHistory => {
                return Err(TxnError::Conflict {
                    contested_row_ids: self.write_set.clone(),
                });
            }
        }

        let new_version = latest_version.checked_add(1).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("version {latest_version} + 1"))
        })?;

        let mut tombstones = latest_snapshot.tombstones.as_ref().clone();
        for delta in &deltas {
            match delta {
                DeltaEntry::Insert { row_id, vector } => self.graph.insert(*row_id, vector)?,
                DeltaEntry::Tombstone { row_id } => {
                    tombstones.insert(*row_id);
                }
            }
        }
        for row_id in &self.pending_tombstones {
            tombstones.insert(*row_id);
        }

        let mut manifest = latest_snapshot.manifest.as_ref().clone();
        manifest.version = new_version;
        manifest.data_files = data_files;
        manifest.next_row_id = self.next_row_id_counter.load(std::sync::atomic::Ordering::SeqCst);
        manifest.tombstones.extend(self.pending_tombstones.iter().copied());

        commit_manifest(&self.dir, &manifest)?;

        commit_log.push(new_version, self.write_set);

        let watermark = manifest.next_row_id.saturating_sub(1);
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            graph: self.graph,
            watermark,
            tombstones: Arc::new(tombstones),
        };
        self.current.store(Arc::new(snapshot));

        Ok(())
    }
```

Note the key structural change from the Phase 5 version: `data_files`, `tombstones`, and `manifest` are now all rebuilt from `latest_snapshot` (read *inside* the lock, after re-checking for conflicts) rather than from `self.base_manifest`/`self.tombstones` (this transaction's possibly-stale view from `begin()` time) — this is what makes a clean (non-conflicting) commit correctly layer on top of whatever else committed in between, not just on top of what existed when this transaction began. `attempt_id` (from `write_attempt_counter`, computed before the lock) is used only to name this transaction's own data/delta-log files during the pre-lock write phase, and is deliberately independent of `base_manifest.version` — two concurrent transactions sharing the same stale base version would otherwise compute the same filename and collide before either reached `commit_lock`. The real commit version (`new_version`, computed from `latest_version` inside the lock) and the filename token (`attempt_id`, computed from a separate counter before the lock) are intentionally different numbers serving different purposes; do not conflate them.

- [ ] **Step 5: Run the new tests**

Run: `cargo test -p strata-txn dataset::tests::concurrent_delete_of_the_same_row_conflicts dataset::tests::concurrent_delete_of_disjoint_rows_both_commit dataset::tests::commit_version_is_sourced_from_latest_state_not_stale_base_manifest -- --nocapture`

Expected: PASS (3 new tests). These three tests are single-threaded-sequential (no real thread contention), so they don't by themselves exercise the filename-collision scenario `attempt_id` exists to prevent — Task 7's loom tests are what genuinely stress concurrent `begin()`s sharing a stale base version. If Task 7 somehow still surfaces a collision, that means this task's `write_attempt_counter` wiring has a bug (e.g. a call site still reading `attempt_id` from somewhere version-derived) — fix it here, not by reintroducing version-based naming.

- [ ] **Step 6: Full-crate regression check and commit**

Run: `cargo test -p strata-txn && cargo clippy -p strata-txn --all-targets -- -D warnings && cargo fmt --check -p strata-txn`
Expected: PASS clean. Pay particular attention to every pre-existing test that calls `commit()` more than once against the same `Dataset` — they must all still pass with the new latest-state-sourced manifest rebuilding.

```bash
git add crates/txn/src/dataset.rs
git commit -m "feat(txn): real OCC conflict detection and atomic commit via commit_lock"
```

---

### Task 7: Loom coverage (conflict, non-conflict) + deterministic abort-leaves-no-trace regression

**Files:**
- Modify: `crates/txn/src/dataset.rs` (the existing `#[cfg(loom)] mod loom_tests` block, and the regular `#[cfg(test)] mod tests` block)

**Interfaces:**
- Consumes: everything from Tasks 4-6.
- Produces: no new public API — pure test coverage.

Locked in during planning: the design doc's §7 calls for loom coverage of "abort leaves no trace in the graph." Two problems surfaced writing it as a third loom test and fixed here: (1) proving it needs to know *which* row-id the loser transaction would have inserted, but two racing `delete()`-only transactions never touch the graph at all — `delete` produces zero `DeltaEntry::Insert` entries, so a delete-vs-delete race can't exercise the graph-mutation-ordering bug in the first place; it needs a conflict where the losing side *also* has something to insert, i.e. `update()`. (2) The natural verification (`graph.len()`) doesn't exist and can't be added minimally — `NodeTable` is a sparse row-id-addressed structure with no internal count to delegate to; adding one is a real cross-file change, not a "minimal" one. Both are avoided by making this specific regression test deterministic (single-threaded, sequential `commit()` calls — no interleaving nondeterminism to explore, so loom adds nothing here) and verifying via `vector_search`'s existing public API instead of a new length accessor: search near the loser's distinctive, never-elsewhere-used vector coordinates and assert nothing is found nearby.

- [ ] **Step 1: Write the loom tests**

Add to the existing `mod loom_tests` block (after `one_writer_store_races_safely_with_many_readers_load`), inside the same `#[cfg(loom)]` module so it shares that module's existing imports:

```rust
    #[test]
    fn two_threads_deleting_the_same_row_exactly_one_conflicts() {
        loom::model(|| {
            let dir = std::env::temp_dir().join(format!(
                "strata-loom-conflict-{}-{:?}",
                std::process::id(),
                loom::thread::current().id()
            ));
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1]))],
            )
            .unwrap();
            let mut setup = ds.begin();
            setup.insert(batch);
            setup.commit().unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();
            let thread_a = loom::thread::spawn(move || {
                let mut txn = ds_a.begin();
                txn.delete(0);
                txn.commit()
            });
            let thread_b = loom::thread::spawn(move || {
                let mut txn = ds_b.begin();
                txn.delete(0);
                txn.commit()
            });

            let result_a = thread_a.join().unwrap();
            let result_b = thread_b.join().unwrap();
            let successes = [&result_a, &result_b].iter().filter(|r| r.is_ok()).count();
            let conflicts = [&result_a, &result_b]
                .iter()
                .filter(|r| matches!(r, Err(crate::TxnError::Conflict { .. })))
                .count();
            assert_eq!(successes, 1, "exactly one commit must succeed");
            assert_eq!(conflicts, 1, "exactly one commit must report a conflict");

            std::fs::remove_dir_all(&dir).ok();
        });
    }

    #[test]
    fn two_threads_deleting_disjoint_rows_both_succeed() {
        loom::model(|| {
            let dir = std::env::temp_dir().join(format!(
                "strata-loom-disjoint-{}-{:?}",
                std::process::id(),
                loom::thread::current().id()
            ));
            let ds = crate::Dataset::create(&dir).unwrap();
            let schema = StdArc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![StdArc::new(arrow::array::Int64Array::from(vec![1, 2]))],
            )
            .unwrap();
            let mut setup = ds.begin();
            setup.insert(batch);
            setup.commit().unwrap();

            let ds_a = ds.clone();
            let ds_b = ds.clone();
            let thread_a = loom::thread::spawn(move || {
                let mut txn = ds_a.begin();
                txn.delete(0);
                txn.commit()
            });
            let thread_b = loom::thread::spawn(move || {
                let mut txn = ds_b.begin();
                txn.delete(1);
                txn.commit()
            });

            assert!(thread_a.join().unwrap().is_ok());
            assert!(thread_b.join().unwrap().is_ok());

            std::fs::remove_dir_all(&dir).ok();
        });
    }

```

- [ ] **Step 2: Run the loom tests**

Run: `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`
Then locate and run the produced test binary (`target/debug/deps/strata_txn-*`, filtered):
`./target/debug/deps/strata_txn-<hash> dataset::loom_tests --test-threads=1`
Expected: PASS for both new tests, plus the pre-existing `one_writer_store_races_safely_with_many_readers_load`. Loom's exhaustive interleaving search can take noticeably longer than a normal test run for tests with file I/O and a lock acquisition in the model closure — expect this to run longer than a typical unit test; that's expected, not a hang, but if it runs unreasonably long (many minutes), that's a signal the model closure is doing too much real I/O per interleaving (loom re-runs the closure body once per explored interleaving) — consider whether the file-system operations inside `loom::model` can be reduced (e.g., the truly minimal one/two-row datasets already used above) before concluding something is wrong.

- [ ] **Step 3: Write the deterministic abort-leaves-no-trace regression test**

Add to the regular (non-loom) `#[cfg(test)] mod tests` block in `crates/txn/src/dataset.rs`:

```rust
    #[test]
    fn losing_transactions_graph_insert_never_lands_when_it_conflicts() {
        // Deterministic, not loom: both transactions begin from the same
        // snapshot, then commit sequentially (not concurrently) so which
        // one wins is fixed by test order, not explored interleavings —
        // there is no concurrency to model here, only a specific sequence
        // to regression-test. This is what actually exercises the
        // graph-mutation-ordering bug (design doc §2): both transactions
        // use `update`, not `delete`, since a delete-only transaction has
        // nothing to insert and can't trigger this bug at all.
        let dir = temp_dir("abort-leaves-no-graph-trace");
        let ds = Dataset::create(&dir).unwrap();
        let setup_batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(setup_batch);
        setup.commit().unwrap();

        // Distinctive, far-apart, never-elsewhere-used coordinates so a
        // vector_search near either one unambiguously reveals whether
        // that specific insert ever reached the graph.
        let winner_batch = vector_batch(vec![2i64], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0));
        let loser_batch = vector_batch(vec![3i64], cluster_vectors(1, [900.0, 900.0, 900.0], 0.0));

        let mut txn_winner = ds.begin();
        txn_winner.update(0, winner_batch);
        let mut txn_loser = ds.begin();
        txn_loser.update(0, loser_batch);

        txn_winner.commit().unwrap();
        let result = txn_loser.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected the second update to conflict on row 0, got {result:?}"
        );

        // The loser's insert must never have reached the graph — search
        // near its distinctive coordinates and confirm nothing close
        // exists (a large squared_distance means the nearest match found
        // is the unrelated winner/setup data, not the loser's own point).
        let results = ds
            .snapshot()
            .vector_search(&[900.0, 900.0, 900.0], 1, None)
            .unwrap();
        assert!(
            results.is_empty() || results[0].squared_distance > 1000.0,
            "loser's vector must not be findable near its own coordinates, got {results:?}"
        );
    }
```

- [ ] **Step 4: Run the new test**

Run: `cargo test -p strata-txn dataset::tests::losing_transactions_graph_insert_never_lands_when_it_conflicts -- --nocapture`
Expected: PASS. If it fails with the loser's vector actually found near-zero distance, that means `Transaction::commit` (Task 6) is applying graph deltas before the conflict check somewhere — re-check that the `match commit_log.conflicts_with(...)` block genuinely precedes the `for delta in &deltas { ... self.graph.insert(...) ... }` loop, not just textually but in actual control flow (no early-return path should skip past the conflict check before reaching delta application).

- [ ] **Step 5: Full regression check and commit**

Run: `cargo test -p strata-txn && cargo clippy -p strata-txn --all-targets -- -D warnings`
Expected: PASS clean.

```bash
git add crates/txn/src/dataset.rs
git commit -m "test(txn): loom coverage for conflict/non-conflict, deterministic abort-leaves-no-trace regression"
```

---

### Task 8: `CommitLog` boundary regression test at the `Dataset` level

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: everything from Tasks 4-6.
- Produces: no new public API.

Task 3 already unit-tests `CommitLog::conflicts_with`'s `InsufficientHistory` branch in isolation. This task proves the same behavior end-to-end through `Dataset`/`Transaction`, with the small `COMMIT_LOG_CAPACITY` needed to make wraparound reachable in a test without committing 256 real transactions.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively() {
        // COMMIT_LOG_CAPACITY is 256 in production; committing that many
        // transactions here to force wraparound is wasteful but the most
        // direct way to exercise the real end-to-end path without adding
        // a test-only constructor parameter for capacity. Kept small
        // enough (log capacity + a few) to run quickly.
        let dir = temp_dir("commit-log-wraparound-e2e");
        let ds = Dataset::create(&dir).unwrap();
        let batch = vector_batch(vec![1i64], cluster_vectors(1, [0.0, 0.0, 0.0], 0.0));
        let mut setup = ds.begin();
        setup.insert(batch);
        setup.commit().unwrap();

        // txn begins here, before every filler commit below — its
        // base_manifest.version stays fixed at whatever ds.current_version()
        // is right now.
        let mut txn = ds.begin();
        txn.delete(0);

        // Commit enough disjoint no-op-ish filler transactions to push the
        // CommitLog's oldest retained entry past txn's read-version.
        for i in 0..(super::COMMIT_LOG_CAPACITY as i64 + 2) {
            let filler = vector_batch(
                vec![100 + i],
                cluster_vectors(1, [f32::from(i as i16), 0.0, 0.0], 0.0),
            );
            let mut filler_txn = ds.begin();
            filler_txn.insert(filler);
            filler_txn.commit().unwrap();
        }

        let result = txn.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected a conservative conflict once history aged out, got {result:?}"
        );
    }
```

`COMMIT_LOG_CAPACITY` was defined as a private `const` in Task 6 — reference it via `super::COMMIT_LOG_CAPACITY` from inside the `mod tests` block (standard Rust visibility: a private item is visible to child modules of the module that defines it, which `mod tests` already is).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-txn dataset::tests::a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively -- --nocapture`
Expected: if Task 6 is implemented correctly per its own design, this should already PASS without further changes — this task exists to prove that end-to-end, not to add new production code. If it fails, that means Task 6's wiring of `ConflictCheck::InsufficientHistory` to `TxnError::Conflict` has a bug (e.g. `since_version`/`up_to_version` passed to `conflicts_with` in the wrong order, or off-by-one against `CommitLog`'s own boundary logic) — fix `Transaction::commit` (Task 6's code), not this test.

- [ ] **Step 3: Run test to verify it passes, then commit**

Run: `cargo test -p strata-txn dataset::tests::a_transaction_whose_history_has_aged_out_of_the_commit_log_conflicts_conservatively`
Expected: PASS

```bash
git add crates/txn/src/dataset.rs
git commit -m "test(txn): end-to-end CommitLog wraparound conflict regression test"
```

---

### Task 9: Benchmark — concurrent commit throughput (exit evidence)

**Files:**
- Create: `bench/benches/concurrent_commit_bench.rs`
- Modify: `bench/Cargo.toml`

**Interfaces:**
- Consumes: `Dataset`, `Transaction` public API only.
- Produces: a runnable `cargo bench` target; no library code.

- [ ] **Step 1: Register the bench target**

In `bench/Cargo.toml`, add after the existing `[[bench]]` entries:

```toml
[[bench]]
name = "concurrent_commit_bench"
harness = false
```

- [ ] **Step 2: Write the benchmark**

Create `bench/benches/concurrent_commit_bench.rs`:

```rust
// bench/benches/concurrent_commit_bench.rs
//! Phase 6 exit-evidence benchmark: commit throughput under concurrent
//! non-conflicting writers vs. a single-writer baseline, and under a
//! high-conflict-rate workload — the number that validates (or refutes)
//! the tightly-scoped `commit_lock` design. See
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! §3 and §7.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_txn::Dataset;

const NUM_THREADS: usize = 8;
const COMMITS_PER_THREAD: i64 = 50;

/// This workspace's benches and tests use `std::env::temp_dir().join(...)`
/// directly rather than the `tempfile` crate (confirmed against
/// `bench/Cargo.toml`'s existing dependency list, which has no `tempfile`
/// entry) — matching that convention rather than introducing a new
/// dependency for this one file. `TempDataset` bundles the directory path
/// alongside the `Dataset` so its `Drop` impl can clean up, since
/// `criterion::BatchSize::LargeInput` setup closures don't get an explicit
/// teardown hook of their own.
struct TempDataset {
    dir: PathBuf,
    dataset: Dataset,
}

impl Drop for TempDataset {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn setup_dataset(row_count: i64) -> TempDataset {
    let dir = std::env::temp_dir().join(format!(
        "strata-bench-concurrent-commit-{}-{}",
        std::process::id(),
        row_count
    ));
    std::fs::remove_dir_all(&dir).ok();
    let dataset = Dataset::create(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Vec<i64> = (0..row_count).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();
    let mut txn = dataset.begin();
    txn.insert(batch);
    txn.commit().unwrap();
    TempDataset { dir, dataset }
}

/// `NUM_THREADS` threads each committing `COMMITS_PER_THREAD` inserts of
/// fresh (never-colliding) rows — zero conflicts by construction, since
/// inserts always get fresh monotonic row-ids (design doc §1). This is the
/// throughput the tightly-scoped `commit_lock` is meant to preserve close
/// to a lock-free ceiling, since the expensive data-file fsync happens
/// outside it.
fn bench_concurrent_non_conflicting_inserts(c: &mut Criterion) {
    c.bench_function("concurrent_non_conflicting_inserts", |b| {
        b.iter_batched(
            || setup_dataset(0),
            |temp_dataset| {
                std::thread::scope(|scope| {
                    for _ in 0..NUM_THREADS {
                        let ds = temp_dataset.dataset.clone();
                        scope.spawn(move || {
                            for i in 0..COMMITS_PER_THREAD {
                                let schema =
                                    Arc::new(Schema::new(vec![Field::new(
                                        "id",
                                        DataType::Int64,
                                        false,
                                    )]));
                                let batch = RecordBatch::try_new(
                                    schema,
                                    vec![Arc::new(Int64Array::from(vec![i]))],
                                )
                                .unwrap();
                                let mut txn = ds.begin();
                                txn.insert(batch);
                                txn.commit().unwrap();
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// `NUM_THREADS` threads all repeatedly deleting the *same* pre-existing
/// row-id (row 0) — maximal conflict rate, every commit but the first
/// racing to conflict. Measures how badly a saturated-contention workload
/// degrades throughput, and how much of that time is retried caller-side
/// work (this benchmark's own retry loop) vs. lock hold time.
fn bench_high_conflict_rate(c: &mut Criterion) {
    c.bench_function("high_conflict_rate_delete_retries", |b| {
        b.iter_batched(
            || setup_dataset(1),
            |temp_dataset| {
                std::thread::scope(|scope| {
                    for _ in 0..NUM_THREADS {
                        let ds = temp_dataset.dataset.clone();
                        scope.spawn(move || {
                            loop {
                                let mut txn = ds.begin();
                                txn.delete(0);
                                match txn.commit() {
                                    Ok(()) => break,
                                    Err(strata_txn::TxnError::Conflict { .. }) => {
                                        // Row 0 was already deleted by
                                        // another thread — done, not an
                                        // error for this benchmark's
                                        // purposes.
                                        if !ds.snapshot().is_visible(0) {
                                            break;
                                        }
                                    }
                                    Err(e) => panic!("unexpected error: {e}"),
                                }
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// Single-writer baseline: `NUM_THREADS * COMMITS_PER_THREAD` sequential
/// commits on one thread, same total commit count as
/// `bench_concurrent_non_conflicting_inserts` — the number that isolates
/// how much `commit_lock` contention costs from how much total commit
/// volume costs.
fn bench_single_writer_baseline(c: &mut Criterion) {
    c.bench_function("single_writer_baseline", |b| {
        b.iter_batched(
            || setup_dataset(0),
            |temp_dataset| {
                for i in 0..(NUM_THREADS as i64 * COMMITS_PER_THREAD) {
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "id",
                        DataType::Int64,
                        false,
                    )]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![i]))],
                    )
                    .unwrap();
                    let mut txn = temp_dataset.dataset.begin();
                    txn.insert(batch);
                    txn.commit().unwrap();
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    bench_concurrent_non_conflicting_inserts,
    bench_high_conflict_rate,
    bench_single_writer_baseline
);
criterion_main!(benches);
```

- [ ] **Step 3: Run the benchmark**

Run: `cargo bench -p strata-bench --bench concurrent_commit_bench`
Expected: completes and prints Criterion's timing report for all three functions. There is no pass/fail gate on the numbers themselves — per the design doc, this is exit *evidence*, not a threshold. Record all three numbers in the plan's completion notes or a short results doc, mirroring how the GROUP BY Phase A plan recorded its benchmark numbers (`docs(plan): record GROUP BY Phase A benchmark results` in this repo's git log is the precedent to follow).

- [ ] **Step 4: Commit**

```bash
git add bench/Cargo.toml bench/benches/concurrent_commit_bench.rs
git commit -m "bench(txn): add concurrent commit throughput benchmark for Phase 6"
```

---

### Task 10: Update the stale module doc comment

**Files:**
- Modify: `crates/txn/src/dataset.rs` (top-of-file doc comment only)

**Interfaces:** none — documentation only.

- [ ] **Step 1: Replace the stale doc comment**

The current top-of-file comment (lines 1-11) describes the Phase 1/5 state ("Phase 1 has exactly one writer... neither is implemented yet"), which this plan has now made false. Replace it:

```rust
//! Transaction path for `strata-txn`. Implements spec §3's commit protocol
//! in full, including real OCC conflict detection and an atomic
//! commit critical section (Phase 6) — see
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! and `.claude/rules/concurrency-txn-layer.md` before editing anything
//! here. Conflict detection is write-write only, keyed by row-id, and
//! scoped to in-process concurrency (multiple threads/tasks sharing one
//! `Dataset` handle) — see the design doc §1 for why cross-process
//! visibility and read-set tracking are explicit non-goals for this slice,
//! not gaps.
```

- [ ] **Step 2: Run full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: PASS clean — this is the project's own "What done means" gate (CLAUDE.md), required before this phase is considered complete regardless of which task most recently touched what.

- [ ] **Step 3: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "docs(txn): update dataset.rs module comment to reflect Phase 6"
```

---

## After all tasks: mandatory review

Per this project's CLAUDE.md, this phase is not done until the `reviewer` subagent (Opus 4.8) has reviewed the full branch diff — not each task in isolation, the same way Phase 2's and Phase 3's whole-branch reviews each caught something no task-scoped review could see. Do not skip this. Do not mark Phase 6 done in conversation before it happens.
