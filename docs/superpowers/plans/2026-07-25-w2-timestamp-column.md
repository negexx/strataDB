# W2 — First-Class Timestamp Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a system-populated, hidden `_timestamp: Int64` column (microseconds since the Unix epoch)
to every committed row, analogous to the existing hidden `_row_id` column, with a monotonic
commit-order high-water mark and correct predicate/pruning support.

**Architecture:** One clock read per transaction, captured at the top of `commit()` before
`write_phase` runs (so a delete-only commit still advances the high-water mark, and a clock-read
failure aborts before any row-id claim). Two independent monotonicity layers: a lock-free
`Dataset`-level `AtomicI64` gives non-decreasing *issuance* order regardless of wall-clock regression;
a new `Manifest.commit_time_high_water` field, updated as a running max inside `commit_lock`, gives the
actual non-decreasing-*across-versions* guarantee the spec requires — deliberately decoupled from any
individual row's own value, which stays an honest per-transaction capture. `_timestamp` is included in
`DataFileEntry.stats` (unlike `_row_id`), so `should_scan_file` prunes on it immediately.

**Tech Stack:** Rust 2024, `arrow` 58.3, `serde`/`serde_json` (existing manifest serialization).

## Global Constraints

- Full design, with all open questions resolved and rationale: `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` — every task below implements a specific section of it; read the relevant section before starting a task, don't re-derive the reasoning.
- `_timestamp` is `Int64` (NOT `UInt64` — `strata_storage::Value`/`Predicate` don't support unsigned), microseconds since the Unix epoch (NOT milliseconds — see design doc §2).
- `cargo build --workspace` clean, `cargo test --workspace` green, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean, before this workstream is marked done (`.claude/CLAUDE.md` "What done means").
- No `loom` test is required for the timestamp-capture mechanism itself (a single atomic RMW, no paired shared state) — but `Manifest.commit_time_high_water`'s update happens inside the already-loom-tested `commit_lock` critical section, so no *new* interleaving is introduced there (design doc §10; verified by inspection in Task 3, not by writing a new loom test).
- Every task needs an Opus-5-tier `reviewer` subagent pass before being marked done (mandatory, per `.claude/CLAUDE.md`).
- This branch (`feat/s1-w2-timestamp-column`) is rebased onto `feat/phase-s1-segmented-index` **after** W1 merged into it — `strata_query::Predicate::And`/`Or`/`columns()` and `Snapshot::row_ids_matching`'s multi-column projection already exist and must not be re-implemented; this plan builds directly on top of them.

---

## File Structure

- **Modify `crates/storage/src/manifest.rs`** — adds `Manifest.commit_time_high_water: i64`. Owns the manifest's on-disk shape; this is the only file that should define the field.
- **Modify `crates/txn/src/error.rs`** — adds `TxnError::ClockError(String)` for the one new fallible path (the system clock reporting a time before the Unix epoch).
- **Modify `crates/txn/src/dataset.rs`** — the bulk of the work: `TIMESTAMP_COLUMN` constant, `Dataset`/`Transaction`'s new `last_issued_timestamp` field, `issue_timestamp`, `append_timestamp_column`, the `commit()`/`write_phase`/`write_pending_batches` wiring, and `cast_batch_to_schema`'s hidden-column fix. This mirrors exactly how `_row_id` already lives entirely in this one file — no new file needed, matching the existing pattern.
- **Modify `crates/txn/src/lib.rs`** — re-exports `TIMESTAMP_COLUMN` at the crate root, matching `ROW_ID_COLUMN`.
- No changes needed to `crates/txn/src/snapshot.rs` — `row_ids_matching`'s projection (`predicate.columns()`) and `should_scan_file` are already generic over any column, per W1.
- No changes needed to `crates/query/src/predicate.rs` — already generic.

---

### Task 1: `Manifest.commit_time_high_water` field

**Files:**
- Modify: `crates/storage/src/manifest.rs`

**Interfaces:**
- Produces: `Manifest.commit_time_high_water: i64` (public field), defaulting to `0` both via `Manifest::empty()` and via `#[serde(default)]` on deserialization of older manifests.

Design doc reference: §4.

This task **only** adds the field and fixes every existing manual `Manifest { ... }` struct literal the
new field breaks (Rust struct literals require every field — `#[serde(default)]` only helps
*deserialization*, not construction). It does not yet update or read the field anywhere except
`Manifest::empty()`; Task 3 wires the actual commit-path update.

- [ ] **Step 1: Write the failing test**

Add to `crates/storage/src/manifest.rs`'s `#[cfg(test)] mod tests` block, immediately after the existing
`manifest_without_next_attempt_id_field_deserializes_with_default_zero` test:

```rust
    #[test]
    fn manifest_without_commit_time_high_water_field_deserializes_with_default_zero() {
        // Simulates a manifest written before `commit_time_high_water` existed
        // — must still deserialize, defaulting to 0, same as `next_attempt_id`
        // does for pre-that-field manifests.
        let old_json = serde_json::json!({
            "version": 0,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert_eq!(deserialized.commit_time_high_water, 0);
    }

    #[test]
    fn empty_manifest_starts_with_zero_commit_time_high_water() {
        assert_eq!(Manifest::empty().commit_time_high_water, 0);
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test -p strata-storage manifest_without_commit_time_high_water`
Expected: compile error — `no field 'commit_time_high_water' on type 'Manifest'`.

- [ ] **Step 3: Add the field to the struct and to `Manifest::empty()`**

In `crates/storage/src/manifest.rs`, add to the `Manifest` struct (after the existing
`next_attempt_id` field, before the closing `}` — the struct currently ends at line 71 with
`pub next_attempt_id: u64,` then `}`):

```rust
    /// The commit-order-monotone envelope of every commit's captured
    /// timestamp so far — **not** necessarily equal to the max `_timestamp`
    /// any individual row carries (see
    /// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md`
    /// §4 for why: `write_phase` runs outside `commit_lock`, so a row's own
    /// timestamp capture and its eventual commit order can diverge under
    /// concurrency). Updated as `.max()` against each commit's own captured
    /// timestamp, inside the commit lock — which is what makes *this* field
    /// non-decreasing across versions by construction, even when a specific
    /// row's own value isn't.
    ///
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones`/`next_attempt_id`.
    #[serde(default)]
    pub commit_time_high_water: i64,
```

Update `Manifest::empty()` (currently lines 74-83):

```rust
impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
        }
    }
}
```

- [ ] **Step 4: Fix every other manual `Manifest { ... }` struct literal in the codebase**

Every one of these currently ends with `next_attempt_id: 0,` (or `next_attempt_id: 0, // <-- the exact
legacy-deserialize shape` for the one at `dataset.rs:1741`) — add `commit_time_high_water: 0,` as the
next line in each. This is the complete list (confirmed by repo-wide search — do not skip any, and do
not assume there are more):

`crates/storage/src/manifest.rs`:
- Line 199 (`let m0 = Manifest { ... }`, test `commit_then_read_current_round_trips`)
- Line 211 (`let m1 = Manifest { ... }`, same test)
- Line 242 (`let m0 = Manifest { ... }`, test around "crash-sim")
- Line 319 (`let m0 = Manifest { ... }`, a stats-carrying test)
- Line 417 (`let m0 = Manifest { ... }`, test around "compact-json")

`crates/txn/src/dataset.rs`:
- Line 1732 (`let legacy_manifest = Manifest { ... }`)
- Line 1860 (`let hostile = Manifest { ... }`)
- Line 1886 (`let hostile = Manifest { ... }`)
- Line 2483 (`let hostile = Manifest { ... }`)

For each, add `commit_time_high_water: 0,` immediately after the existing `next_attempt_id: 0,` line
(or wherever that field appears in the literal — some of these list fields in the struct's declared
order, so `next_attempt_id` is last; add the new line right after it).

- [ ] **Step 5: Run the full storage and txn test suites to verify everything compiles and passes**

Run: `cargo test -p strata-storage`
Expected: PASS, including both new tests from Step 1.

Run: `cargo test -p strata-txn`
Expected: PASS — this confirms all 4 `dataset.rs` struct-literal fixes compile and nothing else broke.

- [ ] **Step 6: Commit**

```bash
git add crates/storage/src/manifest.rs crates/txn/src/dataset.rs
git commit -m "feat(storage): add Manifest.commit_time_high_water field"
```

---

### Task 2: Timestamp capture and issuance mechanism

**Files:**
- Modify: `crates/txn/src/error.rs`
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `Manifest.commit_time_high_water` (Task 1) — read on `Dataset::create`/`open` to seed the
  new atomic.
- Produces: `TxnError::ClockError(String)` (new error variant). `Dataset`/`Transaction` both gain a
  `last_issued_timestamp: Arc<std::sync::atomic::AtomicI64>` field. A new free function
  `fn issue_timestamp(last_issued: &std::sync::atomic::AtomicI64) -> Result<i64>` — Task 3 calls this at
  the top of `commit()`.

Design doc reference: §3 (Layer 1 specifically — Layer 2, the manifest-side update, is Task 3).

This task lays down the clock-issuance plumbing in isolation, without yet touching the row-append,
stats, or `cast_batch_to_schema` logic — `issue_timestamp` is fully testable on its own against a bare
`AtomicI64`, which is exactly what Step 1's test does.

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/dataset.rs`'s `#[cfg(test)] mod tests` block (anywhere reasonable — e.g. right
after the `temp_dir` helper at line 1552-1554):

```rust
    #[test]
    fn issue_timestamp_never_decreases_even_if_the_clock_would() {
        let last_issued = std::sync::atomic::AtomicI64::new(0);

        let first = issue_timestamp(&last_issued).unwrap();
        assert!(first > 0, "a real clock read must be positive microseconds-since-epoch");

        // Simulate a wall-clock regression (an NTP step backward) by seeding
        // the atomic far ahead of any real clock reading it could compete
        // against - `fetch_max` must keep the issued value at or above this,
        // never let a subsequent "now()" that's smaller win.
        last_issued.store(first + 1_000_000_000, std::sync::atomic::Ordering::SeqCst);
        let second = issue_timestamp(&last_issued).unwrap();
        assert_eq!(
            second,
            first + 1_000_000_000,
            "issuance must never go backward, even against a clock read that would be smaller"
        );

        let third = issue_timestamp(&last_issued).unwrap();
        assert!(
            third >= second,
            "a normal (non-regressing) subsequent issuance must still be non-decreasing"
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `cargo test -p strata-txn issue_timestamp_never_decreases`
Expected: compile error — `cannot find function 'issue_timestamp' in this scope`.

- [ ] **Step 3: Add `TxnError::ClockError`**

In `crates/txn/src/error.rs`, add a new variant to the `TxnError` enum (after `Conflict`, the last
existing variant):

```rust
    #[error("system clock error: {0}")]
    ClockError(String),
```

- [ ] **Step 4: Add `issue_timestamp` and wire the `AtomicI64` field into `Dataset`/`Transaction`**

In `crates/txn/src/dataset.rs`, update the import at the top from:

```rust
use std::sync::atomic::AtomicU64;
```

to:

```rust
use std::sync::atomic::{AtomicI64, AtomicU64};
```

Add a new free function, placed near `data_subdir` (around line 116-118, before `impl Dataset`):

```rust
/// Captures the current wall-clock time (microseconds since the Unix
/// epoch) and issues it through `last_issued`, guaranteeing the returned
/// value is never less than any value this `Dataset` has issued before —
/// including across a wall-clock regression (an NTP step, a manual clock
/// change), not just under concurrency. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §3.
///
/// No `Mutex` is needed here, unlike `RowIdAllocator`: there is no paired
/// state that must advance atomically together, just one independent
/// integer high-water mark — a single `fetch_max` is sufficient.
///
/// # Errors
///
/// Returns [`TxnError::ClockError`] if the system clock reports a time
/// before the Unix epoch (`SystemTime::now() < UNIX_EPOCH`), or
/// [`TxnError::TryFromInt`] if the current time in microseconds since the
/// epoch overflows `i64` (not reachable before the year 292471, but
/// checked rather than assumed).
fn issue_timestamp(last_issued: &AtomicI64) -> Result<i64> {
    let now_us = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| TxnError::ClockError(e.to_string()))?
            .as_micros(),
    )?;
    let prev = last_issued.fetch_max(now_us, std::sync::atomic::Ordering::SeqCst);
    Ok(prev.max(now_us))
}
```

Add the field to the `Dataset` struct (after `insufficient_history_conflicts`, currently the last field
before the struct's closing `}` around line 107-108):

```rust
    /// Lock-free issuance floor for `_timestamp` values — see
    /// `issue_timestamp`. Seeded from `Manifest.commit_time_high_water` on
    /// both `create` and `open`, so this floor survives a restart.
    last_issued_timestamp: Arc<AtomicI64>,
```

Add the same field to the `Transaction` struct (after `insufficient_history_conflicts`, currently the
last non-`#[cfg(...)]` field, around line 427):

```rust
    last_issued_timestamp: Arc<AtomicI64>,
```

In `create_with_commit_log_capacity` (around lines 223-255), the body currently reads (in order):
`let manifest = Manifest::empty();` then `commit_manifest(&dir, &manifest)?;` then
`let graph = new_hnsw_index(0)?;`. Insert this new line immediately after the `commit_manifest(&dir,
&manifest)?;` line and before `let graph = new_hnsw_index(0)?;`:

```rust
        let last_issued_timestamp = Arc::new(AtomicI64::new(manifest.commit_time_high_water));
```

Add `last_issued_timestamp,` to that function's final `Ok(Self { ... })` struct literal (alongside the
existing `insufficient_history_conflicts: Arc::new(AtomicU64::new(0)),` line).

In `Dataset::open` (around lines 285-336), add the equivalent line right after
`let write_attempt_counter = Arc::new(AtomicU64::new(seed_write_attempt_counter(&manifest)?));`:

```rust
        let last_issued_timestamp = Arc::new(AtomicI64::new(manifest.commit_time_high_water));
```

Add `last_issued_timestamp,` to that function's `Ok(Self { ... })` struct literal too.

In `Dataset::begin` (around lines 365-387), add to the `Transaction { ... }` struct literal (alongside
the existing `insufficient_history_conflicts: Arc::clone(&self.insufficient_history_conflicts),` line):

```rust
            last_issued_timestamp: Arc::clone(&self.last_issued_timestamp),
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p strata-txn issue_timestamp_never_decreases`
Expected: PASS.

- [ ] **Step 6: Run the full workspace build and test suite**

Run: `cargo build --workspace`
Expected: clean — this confirms every struct-literal site that constructs `Dataset`/`Transaction` was
updated (there are only the two in `create_with_commit_log_capacity`/`open` plus `begin`; unlike
`Manifest`, no test constructs a bare `Dataset`/`Transaction` literal directly — they're always built
through `Dataset::create`/`open`/`begin`).

Run: `cargo test -p strata-txn`
Expected: PASS, no regressions.

- [ ] **Step 7: Commit**

```bash
git add crates/txn/src/error.rs crates/txn/src/dataset.rs
git commit -m "feat(txn): monotonic timestamp issuance (Dataset.last_issued_timestamp)"
```

---

### Task 3: `_timestamp` column, commit-path wiring, and file-level pruning stats

**Files:**
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/lib.rs`

**Interfaces:**
- Consumes: `issue_timestamp`, `Transaction.last_issued_timestamp` (Task 2); `Manifest.commit_time_high_water` (Task 1).
- Produces: `pub const TIMESTAMP_COLUMN: &str = "_timestamp";` (re-exported from `lib.rs`, matching
  `ROW_ID_COLUMN`). `fn append_timestamp_column(batch: &RecordBatch, ts: i64, num_rows: u64) ->
  Result<RecordBatch>`. Every committed row carries `_timestamp`. `Manifest.commit_time_high_water`
  is updated on every commit, including delete-only ones. `DataFileEntry.stats` carries a
  `_timestamp` entry (`{min: ts, max: ts}`) for every file.

Design doc references: §2 (column type/placement), §3 (capture point), §4 (manifest update), §7
(`compute_stats` inclusion).

- [ ] **Step 1: Write the failing tests**

Add to `crates/txn/src/dataset.rs`'s test module, near the existing row-id tests (e.g. after
`row_id_hidden`-style tests — exact placement doesn't matter, this is a new independent test group):

```rust
    #[test]
    fn every_row_in_one_transaction_shares_the_identical_timestamp() {
        let dir = temp_dir("timestamp-shared-per-txn");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap());
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![3, 4]))]).unwrap());
        txn.commit().unwrap();

        let snapshot = ds.snapshot();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = snapshot.scan(&schema).unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let first = timestamps.value(0);
        assert_eq!(batch.num_rows(), 4);
        for i in 0..batch.num_rows() {
            assert_eq!(
                timestamps.value(i),
                first,
                "every row across both pending batches in one transaction must share one timestamp"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_delete_only_commit_still_advances_commit_time_high_water() {
        let dir = temp_dir("timestamp-delete-only-advances");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap());
        txn.commit().unwrap();
        let after_insert = ds.snapshot().manifest.commit_time_high_water;
        assert!(after_insert > 0);

        // Sleep-free: two distinct clock reads are only guaranteed distinct
        // if enough wall-clock time actually elapses, which isn't
        // guaranteed in a fast test - so this asserts non-decreasing
        // (`>=`), matching the spec's own "non-decreasing" wording, not
        // strictly increasing.
        let mut txn = ds.begin();
        txn.delete(0);
        txn.commit().unwrap();
        let after_delete = ds.snapshot().manifest.commit_time_high_water;
        assert!(
            after_delete >= after_insert,
            "a delete-only commit must still advance (or at least not regress) the high-water mark"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_time_high_water_is_non_decreasing_across_several_commits() {
        let dir = temp_dir("timestamp-high-water-monotonic");
        let ds = Dataset::create(&dir).unwrap();

        let mut last = 0i64;
        for i in 0..5 {
            let mut txn = ds.begin();
            txn.insert(
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![i]))]).unwrap(),
            );
            txn.commit().unwrap();
            let current = ds.snapshot().manifest.commit_time_high_water;
            assert!(
                current >= last,
                "commit {i}: high-water mark regressed from {last} to {current}"
            );
            last = current;
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_time_high_water_does_not_regress_when_a_smaller_timestamp_commits_later() {
        // Deterministic concurrency, not a race: constructs the exact
        // adversarial interleaving design doc §1/§3 accepts as possible - a
        // transaction that captured an EARLIER (smaller) timestamp reaches
        // commit_lock and actually commits AFTER a transaction that
        // captured a LATER (larger) one. Proves Layer 2 (the manifest's
        // commit_time_high_water) stays non-decreasing across versions even
        // though the current committer's own captured value is smaller
        // than what a concurrent commit already published. Uses this
        // file's existing `checkpoint_pair`/`pause_after_row_id_claim`
        // mechanism (see `in-flight-commit-not-visible-to-reader`-style
        // tests elsewhere in this module for the same pattern), not a
        // sleep-raced schedule - only the wall-clock *value* gap uses a
        // short sleep below, not the interleaving itself.
        let dir = temp_dir("timestamp-high-water-concurrent-non-regression");
        let ds = Dataset::create(&dir).unwrap();

        let (claim_point, claimed) = checkpoint_pair();

        // "slow": captures its timestamp first (small - issue_timestamp
        // runs at the very top of commit(), before write_phase, which is
        // what pause_after_row_id_claim pauses after), then parks before
        // acquiring commit_lock.
        let mut slow = ds.begin();
        slow.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        );
        slow.pause_after_row_id_claim(claim_point);
        let slow_thread = std::thread::spawn(move || slow.commit());

        claimed.wait();

        // Not racing an observation window (this file's other
        // Checkpoint-based tests correctly avoid that) - only guaranteeing
        // "fast"'s own clock read is strictly later in wall-clock time,
        // which real elapsed time already all but guarantees at
        // microsecond resolution; this removes any doubt.
        std::thread::sleep(std::time::Duration::from_millis(2));

        // "fast": captures a strictly later (larger) timestamp and commits
        // to completion, uncontested, while "slow" is parked.
        let mut fast = ds.begin();
        fast.insert(
            RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        );
        fast.commit().unwrap();
        let high_water_after_fast = ds.snapshot().manifest.commit_time_high_water;
        assert!(high_water_after_fast > 0);

        // Release "slow" - it builds its manifest from "fast"'s
        // already-published snapshot (commit_time_high_water ==
        // high_water_after_fast), then applies `.max(its own smaller
        // timestamp)` on top of that.
        claimed.release();
        slow_thread.join().unwrap().unwrap();

        let high_water_after_slow = ds.snapshot().manifest.commit_time_high_water;
        assert_eq!(
            high_water_after_slow, high_water_after_fast,
            "a later-committing transaction with an EARLIER captured timestamp must not \
             regress commit_time_high_water below what an already-published concurrent \
             commit established"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn should_scan_file_prunes_using_timestamp_stats() {
        let dir = temp_dir("timestamp-file-pruning");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap());
        txn.commit().unwrap();

        let files = ds.data_files();
        assert_eq!(files.len(), 1);
        let stats = files[0].stats.get(TIMESTAMP_COLUMN).expect(
            "_timestamp must have a stats entry (unlike _row_id, which deliberately has none)",
        );
        assert_eq!(stats.min, stats.max, "every row in one file shares one timestamp");

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p strata-txn every_row_in_one_transaction_shares_the_identical_timestamp`
Expected: compile error (`TIMESTAMP_COLUMN` doesn't exist yet) or, once that's stubbed, a failing
assertion — either is an acceptable "red" state to implement against next.

- [ ] **Step 3: Add `TIMESTAMP_COLUMN` and `append_timestamp_column`**

In `crates/txn/src/dataset.rs`, add right after the existing `ROW_ID_COLUMN` constant (line 52):

```rust
/// The hidden internal commit-time column every committed batch carries
/// alongside its logical columns and `_row_id` — see
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md`.
/// Every row in one transaction shares one value: microseconds since the
/// Unix epoch, captured once per commit. Unlike `_row_id`, this column
/// *does* get a `should_scan_file`-visible stats entry — see
/// `write_pending_batches`.
pub const TIMESTAMP_COLUMN: &str = "_timestamp";
```

Add `append_timestamp_column`, right after `append_row_id_column` (currently ending at line 1476, just
before the `#[cfg(test)]` that starts the test module):

```rust
/// Appends a `_timestamp: Int64` column to `batch`, every row sharing the
/// single value `ts` — microseconds since the Unix epoch, captured once per
/// transaction by `issue_timestamp`. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §2-3.
fn append_timestamp_column(batch: &RecordBatch, ts: i64, num_rows: u64) -> Result<RecordBatch> {
    let num_rows = usize::try_from(num_rows)?;
    let timestamps: Vec<i64> = vec![ts; num_rows];
    let timestamp_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(timestamps));

    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(TIMESTAMP_COLUMN, DataType::Int64, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(timestamp_array);

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}
```

- [ ] **Step 4: Wire `ts` through `commit()` → `write_phase` → `write_pending_batches`**

In `crates/txn/src/dataset.rs`, update the `strata_storage` import (currently
`use strata_storage::{DataFileEntry, Manifest, commit_manifest, compute_stats, read_current,
write_batch};`) to also bring in `ColumnStats` and `Value`:

```rust
use strata_storage::{
    ColumnStats, DataFileEntry, Manifest, Value, commit_manifest, compute_stats, read_current,
    write_batch,
};
```

In `Transaction::commit` (starts at line 818), capture `ts` as the very first line of the function body,
before `let data_dir = data_subdir(&self.dir);`:

```rust
    pub fn commit(self) -> Result<()> {
        let ts = issue_timestamp(&self.last_issued_timestamp)?;
        let data_dir = data_subdir(&self.dir);

        let (new_data_files, deltas, mut claim) = self.write_phase(&data_dir, ts)?;
```

Further down in `commit()`, find the block that persists `next_attempt_id` (currently):

```rust
        manifest.next_attempt_id = self
            .write_attempt_counter
            .load(std::sync::atomic::Ordering::SeqCst);
```

Add immediately after it:

```rust
        // Non-decreasing across versions by construction: this is a running
        // max computed under commit_lock, decoupled from any individual
        // row's own captured value — see
        // docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md
        // §4 for why that decoupling is deliberate, not a gap.
        manifest.commit_time_high_water = manifest.commit_time_high_water.max(ts);
```

Update `write_phase`'s signature (currently starting at line 1055):

```rust
    fn write_phase(
        &self,
        data_dir: &Path,
        ts: i64,
    ) -> Result<(Vec<DataFileEntry>, Vec<DeltaEntry>, Option<RowIdClaim>)> {
```

Inside `write_phase`, find the call to `Self::write_pending_batches` (currently):

```rust
        let deltas = Self::write_pending_batches(
            &self.pending,
            data_dir,
            attempt_id,
            &claim,
            &mut new_data_files,
        )?;
```

Change to:

```rust
        let deltas = Self::write_pending_batches(
            &self.pending,
            data_dir,
            attempt_id,
            &claim,
            ts,
            &mut new_data_files,
        )?;
```

Update `write_pending_batches`'s signature and body (currently starting at line 1151):

```rust
    fn write_pending_batches(
        pending: &[RecordBatch],
        data_dir: &Path,
        attempt_id: u64,
        claim: &RowIdClaim,
        ts: i64,
        data_files: &mut Vec<DataFileEntry>,
    ) -> Result<Vec<DeltaEntry>> {
        let mut all_deltas = Vec::new();
        let mut row_id_base = claim.base();
        for (i, batch) in pending.iter().enumerate() {
            // Stats computed on the original, pre-encoding, pre-hidden-column
            // batch — see .claude/docs/design/phase-3-query-refinement-spec.md
            // §1 for why (logical values, no dictionary-decode step needed
            // later). _row_id gets no stats entry (nothing predicates on it);
            // _timestamp DOES, inserted explicitly below, since every row in
            // this batch shares one value and it exists specifically to be
            // predicated on and pruned by — see design doc §7.
            let mut stats = compute_stats(batch);
            stats.insert(
                TIMESTAMP_COLUMN.to_string(),
                ColumnStats {
                    min: Value::Int64(ts),
                    max: Value::Int64(ts),
                },
            );

            let num_rows = u64::try_from(batch.num_rows())?;

            let deltas = build_delta_entries(batch, row_id_base)?;
            let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;
            let with_timestamp = append_timestamp_column(&with_row_id, ts, num_rows)?;

            let encoded = strata_storage::encode_batch(&with_timestamp)?;
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
            // Cannot overflow: `write_phase` sized the claim as the checked
            // sum of every pending batch's row count, and the claim itself
            // was bounds-checked against `u64::MAX` before it was handed
            // out.
            row_id_base += num_rows;
        }
        debug_assert_eq!(
            row_id_base,
            claim.base() + claim.len(),
            "every claimed row-id must be consumed, and none beyond them"
        );
        Ok(all_deltas)
    }
```

(Only the signature, the `stats`/`with_timestamp` lines, and the `build_delta_entries`/
`append_row_id_column` block changed from the current version — the rest is shown verbatim so the
diff is unambiguous.)

- [ ] **Step 5: Export `TIMESTAMP_COLUMN` from the crate root**

In `crates/txn/src/lib.rs`, change:

```rust
pub use dataset::{Dataset, ROW_ID_COLUMN, Transaction};
```

to:

```rust
pub use dataset::{Dataset, ROW_ID_COLUMN, TIMESTAMP_COLUMN, Transaction};
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p strata-txn`
Expected: PASS, including all 5 new tests from Step 1 — `a_delete_only_commit_still_advances_commit_time_high_water`,
`commit_time_high_water_is_non_decreasing_across_several_commits`,
`commit_time_high_water_does_not_regress_when_a_smaller_timestamp_commits_later`, and
`should_scan_file_prunes_using_timestamp_stats` do not touch `cast_batch_to_schema` at all (they read
`manifest.commit_time_high_water` or `data_files()[..].stats` directly) and must be fully green at the
end of this task. Note: `every_row_in_one_transaction_shares_the_identical_timestamp` calls
`snapshot.scan(&schema)` requesting `_timestamp` back — this exercises the *existing* (not-yet-fixed) `cast_batch_to_schema`, which today only knows how to reattach `_row_id`. Since this test's schema requests `_timestamp` but not `_row_id`, walk through `cast_batch_to_schema`'s current logic: `hidden_row_id` will be `false` (schema doesn't ask for `_row_id`), so `logical = physical` (no adjustment) — but `physical` is now 3 (`id`, `_row_id`, `_timestamp`) while the requested schema has 2 fields (`id`, `_timestamp`), so this **must fail** with `SchemaMismatch` at this point in the plan. That failure is expected and correct here — Task 4 fixes it. If you want a fully green `cargo test -p strata-txn` at the end of *this* task specifically, that's not achievable until Task 4 lands; that's fine, this task's own new tests aside from that one should still pass, and this is exactly why Task 4 exists as its own reviewable step. Do not attempt to work around this in Task 3.

Run: `cargo test -p strata-txn every_row_in_one_transaction_shares_the_identical_timestamp -- --nocapture` and confirm the failure is specifically `TxnError::SchemaMismatch`, not something else (a different failure here means a real bug, not the expected/known gap).

- [ ] **Step 7: Commit**

```bash
git add crates/txn/src/dataset.rs crates/txn/src/lib.rs
git commit -m "feat(txn): _timestamp column, commit-path wiring, and file-pruning stats"
```

---

### Task 4: Fix `cast_batch_to_schema` for two hidden columns

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `TIMESTAMP_COLUMN` (Task 3).
- Produces: `cast_batch_to_schema`'s public behavior contract changes from "matches hidden columns by
  position, supports at most one" to "matches hidden columns by name, supports any combination of
  `_row_id`/`_timestamp`" — signature unchanged (`pub(crate) fn cast_batch_to_schema(batch:
  &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch>`), so no caller elsewhere needs to change.

Design doc reference: §5 (the exact failure modes this fixes, including the silent-miscast risk).

This closes the `SchemaMismatch` left deliberately failing at the end of Task 3.

- [ ] **Step 1: Write the failing tests**

Add to `crates/txn/src/dataset.rs`'s test module, near the existing `row_id_hidden`-style tests:

```rust
    #[test]
    fn cast_batch_to_schema_reattaches_neither_hidden_column_by_default() {
        let dir = temp_dir("cast-hidden-neither");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap());
        txn.commit().unwrap();

        let batch = ds.snapshot().scan(&test_schema()).unwrap();
        assert_eq!(batch.num_columns(), 1, "requesting no hidden columns must return just 'id'");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_row_id_only() {
        let dir = temp_dir("cast-hidden-row-id-only");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap());
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ROW_ID_COLUMN, DataType::UInt64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 2);
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.values(), &[0, 1]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_timestamp_only() {
        let dir = temp_dir("cast-hidden-timestamp-only");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap());
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 2);
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(timestamps.value(0) > 0);
        assert_eq!(
            timestamps.value(0),
            timestamps.value(1),
            "this is the exact case that risked a silent miscast under the old positional logic - \
             both rows must show the SAME real timestamp, not a row-id value reinterpreted as Int64"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cast_batch_to_schema_reattaches_both_hidden_columns() {
        let dir = temp_dir("cast-hidden-both");
        let ds = Dataset::create(&dir).unwrap();
        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap());
        txn.commit().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(ROW_ID_COLUMN, DataType::UInt64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let batch = ds.snapshot().scan(&schema).unwrap();
        assert_eq!(batch.num_columns(), 3);
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.values(), &[0, 1]);
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), timestamps.value(1));

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run the tests to verify their current status**

Run: `cargo test -p strata-txn cast_batch_to_schema_reattaches`
Expected: `..._neither_hidden_column_by_default` and `..._row_id_only` PASS already (today's
position-based logic still handles zero or exactly one trailing hidden column correctly). `..._
timestamp_only` and `..._both_hidden_columns` FAIL — the first with a `SchemaMismatch` or a wrong-value
assertion, the second likely with a `SchemaMismatch` (3 requested fields vs. today's broken
`logical` computation). This mix of already-passing and failing tests is expected — it's exactly what
Task 3's step 6 predicted.

- [ ] **Step 3: Rewrite `cast_batch_to_schema`**

Replace the current function (lines 1421-1449) with:

```rust
/// Hidden columns every committed batch may carry alongside its logical
/// (user) columns — `_row_id` always, `_timestamp` always (since W2).
/// `cast_batch_to_schema` matches these by *name*, not position; every
/// other (visible) column is still matched positionally against `schema`'s
/// fields, so the caller's `schema` must still list its visible fields in
/// the same order the data was inserted in.
const HIDDEN_COLUMNS: [&str; 2] = [ROW_ID_COLUMN, TIMESTAMP_COLUMN];

/// Casts `batch`'s physical columns to `schema`'s logical field types,
/// reattaching any hidden column (`_row_id`, `_timestamp`) `schema`
/// explicitly requests. See
/// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §5
/// for why matching a *second* hidden column by position was unsound (it
/// either misfired a spurious `SchemaMismatch`, or — in the mixed-request
/// case — silently paired the wrong physical column against the wrong
/// logical field).
///
/// # Errors
///
/// Returns [`TxnError::SchemaMismatch`] if the number of *visible* columns
/// `schema` requests doesn't match the number of visible physical columns
/// in `batch`, an [`TxnError::Arrow`] if `schema` requests a hidden column
/// not present in `batch`, or an [`TxnError::Arrow`]/[`TxnError`] wrapping
/// a cast failure if a column's physical type can't convert to its
/// requested logical type.
pub(crate) fn cast_batch_to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let physical_schema = batch.schema_ref();

    let mut hidden_physical: std::collections::HashMap<&str, ArrayRef> =
        std::collections::HashMap::new();
    let mut visible_physical: Vec<ArrayRef> = Vec::new();
    for (field, column) in physical_schema.fields().iter().zip(batch.columns()) {
        if HIDDEN_COLUMNS.contains(&field.name().as_str()) {
            hidden_physical.insert(field.name().as_str(), Arc::clone(column));
        } else {
            visible_physical.push(Arc::clone(column));
        }
    }

    let visible_requested = schema
        .fields()
        .iter()
        .filter(|f| !HIDDEN_COLUMNS.contains(&f.name().as_str()))
        .count();
    if visible_requested != visible_physical.len() {
        return Err(TxnError::SchemaMismatch {
            expected: visible_requested,
            actual: visible_physical.len(),
        });
    }

    let mut visible_iter = visible_physical.into_iter();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = if HIDDEN_COLUMNS.contains(&field.name().as_str()) {
            hidden_physical
                .get(field.name().as_str())
                .cloned()
                .ok_or_else(|| {
                    TxnError::Arrow(arrow::error::ArrowError::SchemaError(format!(
                        "requested hidden column '{}' not present in this batch",
                        field.name()
                    )))
                })?
        } else {
            // `visible_requested == visible_physical.len()` was already
            // checked above, so this iterator has exactly as many elements
            // as there are non-hidden fields in `schema` — it cannot run
            // dry before this loop does.
            match visible_iter.next() {
                Some(column) => column,
                None => unreachable!(
                    "visible column count was checked equal to visible field count above"
                ),
            }
        };
        let casted = if column.data_type() == field.data_type() {
            column
        } else {
            cast(column.as_ref(), field.data_type())?
        };
        columns.push(casted);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p strata-txn cast_batch_to_schema_reattaches`
Expected: all 4 PASS.

Run: `cargo test -p strata-txn every_row_in_one_transaction_shares_the_identical_timestamp`
Expected: now PASSES (Task 3's known, deliberate failure is now fixed by this task).

Run: `cargo test -p strata-txn`
Expected: full crate PASS, no regressions — this is the first point in the plan where the whole
`strata-txn` suite is expected to be fully green again.

- [ ] **Step 5: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "fix(txn): match hidden columns by name in cast_batch_to_schema, not position"
```

---

### Task 5: Recovery, dictionary encoding, and the compound-predicate exit criterion

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: no new production code — this task is entirely tests, closing out the design doc's full
  §10 test list and the S1 spec's/W1's own literal exit-criterion example.

Design doc reference: §9 (dictionary encoding), §10 (full test list).

- [ ] **Step 1: Write the recovery round-trip test**

Add to `crates/txn/src/dataset.rs`'s test module:

```rust
    #[test]
    fn timestamps_and_the_high_water_mark_survive_reopen() {
        let dir = temp_dir("timestamp-survives-reopen");
        let ds = Dataset::create(&dir).unwrap();

        let mut txn = ds.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap());
        txn.commit().unwrap();

        let high_water_before_close = ds.snapshot().manifest.commit_time_high_water;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        ]));
        let timestamps_before_close: Vec<i64> = {
            let batch = ds.snapshot().scan(&schema).unwrap();
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|i| col.value(i)).collect()
        };
        drop(ds);

        // Reopen — this is the actual file-read path (dictionary-encoded,
        // per design doc §9), not an in-memory batch, so this is the test
        // that would catch a dictionary-encoding round-trip bug the
        // in-memory-only tests above cannot.
        let reopened = Dataset::open(&dir).unwrap();
        let timestamps_after_reopen: Vec<i64> = {
            let batch = reopened.snapshot().scan(&schema).unwrap();
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|i| col.value(i)).collect()
        };
        assert_eq!(
            timestamps_before_close, timestamps_after_reopen,
            "per-row timestamps must round-trip through the actual (dictionary-encoded) file format"
        );

        // A predicate against the reopened, dictionary-encoded column must
        // still work - the comparison kernel unwraps dictionaries
        // transparently, but this proves it end to end rather than trusting
        // that in isolation.
        let predicate = strata_query::Predicate::GtEq(
            TIMESTAMP_COLUMN.to_string(),
            strata_storage::Value::Int64(timestamps_after_reopen[0]),
        );
        let filtered = reopened.snapshot().scan_with_predicate(&schema, &predicate).unwrap();
        assert_eq!(filtered.num_rows(), 3, "all 3 rows share the same timestamp, so all must match");

        // The issuance floor must also survive: commit again post-reopen and
        // confirm the high-water mark never regressed across the restart.
        let mut txn = reopened.begin();
        txn.insert(RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vec![4]))]).unwrap());
        txn.commit().unwrap();
        assert!(
            reopened.snapshot().manifest.commit_time_high_water >= high_water_before_close,
            "commit_time_high_water must not regress across a restart"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run it**

Run: `cargo test -p strata-txn timestamps_and_the_high_water_mark_survive_reopen`
Expected: PASS. If it fails, it is a real bug in Tasks 1-4, not something to patch around here — stop
and diagnose which of the four earlier tasks' logic is wrong (per this project's
`superpowers:systematic-debugging` — a test failure here means one of the earlier tasks shipped
something subtly incorrect and its own tests didn't catch it).

- [ ] **Step 3: Write the exit-criterion compound-predicate test**

This is the literal `timestamp >= X AND category = Y` example both the S1 spec (§5.1's W1 exit
criterion) and W1's own plan named as the target instantiation, now finally possible. Add to
`crates/txn/src/dataset.rs`'s test module, next to `vector_search_with_compound_predicate_narrows_across_two_columns`
(W1's analogous test — follow its structure):

```rust
    #[test]
    fn vector_search_with_timestamp_and_category_compound_predicate() {
        use strata_query::Predicate;
        use strata_storage::Value;

        let dir = temp_dir("timestamp-compound-vector-search");
        let ds = Dataset::create(&dir).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]));

        // Two commits, two clusters: near (category "a"), far (category
        // "b"). Both share this test's helper cluster generator so recall
        // is non-flaky (matching the precedent's own justification).
        let near_cluster = cluster_vectors(10, [0.0, 0.0, 0.0], 0.01);
        let far_cluster = cluster_vectors(10, [1000.0, 0.0, 0.0], 0.01);

        let flat_near: Vec<f32> = near_cluster.iter().flatten().copied().collect();
        let item_field = Arc::new(Field::new("item", DataType::Float32, false));
        let vec_arr_near = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field.clone(),
            3,
            Arc::new(arrow::array::Float32Array::from(flat_near)),
            None,
        ));
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["a"; 10])),
                    vec_arr_near,
                ],
            )
            .unwrap(),
        );
        txn.commit().unwrap();
        let ts_after_first_commit = ds.snapshot().manifest.commit_time_high_water;

        let flat_far: Vec<f32> = far_cluster.iter().flatten().copied().collect();
        let vec_arr_far = Arc::new(arrow::array::FixedSizeListArray::new(
            item_field,
            3,
            Arc::new(arrow::array::Float32Array::from(flat_far)),
            None,
        ));
        let mut txn = ds.begin();
        txn.insert(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["b"; 10])),
                    vec_arr_far,
                ],
            )
            .unwrap(),
        );
        txn.commit().unwrap();

        let snapshot = ds.snapshot();

        // category="a" alone would match the near cluster (10 pts).
        // timestamp >= (after the first commit) alone would match only the
        // second commit's rows (the far cluster, category "b"). The AND
        // must match NEITHER in full - it needs category="a" (near
        // cluster) AND a timestamp from the first commit, which the first
        // commit's own rows satisfy trivially (>= its own timestamp).
        let predicate = Predicate::And(
            Box::new(Predicate::GtEq(
                TIMESTAMP_COLUMN.to_string(),
                Value::Int64(0),
            )),
            Box::new(Predicate::Eq("category".to_string(), Value::Utf8("a".to_string()))),
        );
        let results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate))
            .unwrap();
        assert_eq!(results.len(), 5, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id < 10),
            "timestamp>=0 AND category=a must return only the near cluster (row-ids 0..10): {results:?}"
        );

        // The sharper case: timestamp strictly after the first commit AND
        // category="a" must match NOTHING - the only category="a" rows are
        // from the first commit, whose timestamp is not >= a value taken
        // after it.
        let predicate_none = Predicate::And(
            Box::new(Predicate::GtEq(
                TIMESTAMP_COLUMN.to_string(),
                Value::Int64(ts_after_first_commit + 1),
            )),
            Box::new(Predicate::Eq("category".to_string(), Value::Utf8("a".to_string()))),
        );
        let empty_results = snapshot
            .vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate_none))
            .unwrap();
        assert!(
            empty_results.is_empty(),
            "no row can satisfy a timestamp strictly after its own commit: {empty_results:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 4: Run it**

Run: `cargo test -p strata-txn vector_search_with_timestamp_and_category_compound_predicate`
Expected: PASS. If `ts_after_first_commit` ties with the second commit's own timestamp (possible at
microsecond resolution under a very fast test run), the first assertion's exact-5 count could include
rows from the second commit too — if this test is observed to be flaky in this specific way (not
merely failing outright, which would indicate a real bug), that is useful signal, not something to
silently loosen; report it rather than weakening the assertion, since real-world commits are unlikely
to land within the same microsecond but a test loop legitimately might.

- [ ] **Step 5: Run the full verification gate**

Run: `cargo build --workspace`
Expected: clean.

Run: `cargo test --workspace`
Expected: all green.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. Pay attention to the `unreachable!()` arm added in Task 4's `cast_batch_to_schema` and
the `HashMap` usage — confirm `std::collections::HashMap` doesn't need a top-level `use` added (it's
fully qualified inline in the plan's code, so no import is required, but double-check clippy doesn't
prefer otherwise for this file's existing style — if it does, add `use std::collections::HashMap;` to
the top-of-file imports and simplify the two `std::collections::HashMap::new()` call sites accordingly).

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "test(txn): recovery round-trip, dictionary encoding, and the timestamp+category compound-predicate exit criterion"
```

---

### Task 6: Workstream verification and PR

**Files:** none (verification only).

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: success, no warnings.

- [ ] **Step 2: Full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass, including every test added in Tasks 1-5.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Format check**

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 5: Confirm no new loom interleaving was introduced**

Per the Global Constraints section: `Manifest.commit_time_high_water`'s update sits inside the
already-loom-tested `commit_lock` critical section, and `issue_timestamp` is a single independent
atomic RMW with no paired state. Read `crates/txn/src/dataset.rs`'s existing `#[cfg(loom)] mod
loom_tests` module (search for it) and confirm by inspection that none of its models construct a
`Transaction`/`Dataset` in a way that bypasses `commit()`'s new `issue_timestamp` call, and that adding
one more field write inside the existing critical section doesn't change what interleavings loom's
existing models explore (it doesn't introduce a new lock, new atomic shared across threads in a new
way, or new branching based on that atomic's value that would affect control flow loom needs to
explore — `commit_time_high_water`'s update is a straight-line `.max()` assignment, not a
conditional). Note this confirmation in the PR description; do not write a new loom test for it
(per design doc §10).

- [ ] **Step 6: Invoke the `superpowers:requesting-code-review` skill, targeting the `reviewer` subagent (Opus 5 tier)**

Per `.claude/CLAUDE.md`: mandatory, not optional. Scope the review to this workstream's diff: all
commits from Tasks 1-5. Address any findings with new commits, not amended ones.

- [ ] **Step 7: Open the PR**

Base: `feat/phase-s1-segmented-index` (now that W1 is merged into it, per the Global Constraints
note — confirm this is still the tip you rebased onto before opening the PR, since the S1 branch
could have moved again). Confirm with the user before pushing/opening, per this project's standing
instructions.
