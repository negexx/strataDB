# Phase 4 (Vector Index — HNSW) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real HNSW build + search over the vector column, wired into the commit path as an append-only delta log sharing the transaction boundary with row data, plus filtered ANN and a recall@10/QPS benchmark on a real public embedding dataset.

**Architecture:** A new global, monotonic `u64` row-id (assigned at the manifest CAS, per `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8) is written as a hidden `_row_id` column alongside every committed batch. `crates/index` gains an `HnswIndex` wrapper around `hnsw_rs` plus an append-only delta-log entry format. `crates/txn::Transaction::commit` writes one delta-log file per commit (mirroring row-data files); `Dataset::open` replays all delta-log files into a fresh in-memory index, cached on `Dataset`. `Dataset::vector_search` searches that cached index, optionally narrowed by a `strata_query::Predicate` with `ef` widened using Phase 3's file-pruning stats as a cheap selectivity signal.

**Tech Stack:** Rust, `arrow` (existing), `hnsw_rs` (already an ADR'd, already-declared dependency of `crates/index` — not previously used, verified against the installed `hnsw_rs-0.3.4`/`anndists-0.1.5` source in this plan), `serde_json` (existing, reused for delta-log serialization — no new serialization dependency), `parquet` (new dependency, `bench/` only, to read the benchmark's real embedding dataset).

## Global Constraints

- Edition 2024, workspace lints apply (`clippy::all` + `clippy::pedantic` at warn, `-D warnings`) — every public `Result`-returning function needs a `# Errors` doc section.
- `unwrap()`/`expect()` are `clippy::warn` — fine only in `#[cfg(test)]` modules, never in library code.
- Git Flow: work happens on `feature/phase-4-vector-index`, already branched from `develop` and already carrying two docs-only commits (the row-id lifecycle spec addendum and this phase's spec). Every task's "Checkpoint" step means: run the verification commands, confirm green, then commit with a Conventional Commits message.
- Verify any `hnsw_rs`/`anndists` API you're not 100% certain of against the installed source before writing code — this plan already did that for every signature it uses (see inline citations in each task), but if a task's implementer needs something not covered here, verify it the same way rather than guessing. This codebase has been burned by guessing at this exact crate's API once already (Phase 1).
- **`hnsw_rs::Hnsw::new` calls `std::process::exit(1)` — not a panic, an unconditional, uncatchable process termination — if `max_nb_connection > 256`.** `HnswIndex::new` (Task 2) must validate this bound itself and return a typed error before ever reaching the library call.
- No loom test is added in this phase for row-id assignment or delta-log writes, despite both touching `crates/txn`. This is a deliberate, documented deferral, not an oversight: Phase 4 (like Phases 1-3 before it) has exactly one writer — there is no concurrent-commit interleaving to test yet. The atomicity claim ("counter-bump + manifest CAS is one atomic step") only becomes a real concurrency question once Phase 6 introduces actual concurrent commits; a loom test before then would be testing an interleaving that cannot occur. Phase 6's plan must add this test before real conflict detection lands — noted here so it isn't forgotten, per the `llm-council` review that raised it (`.superpowers/council/council-transcript-20260716-174711.md`).

---

### Task 1: Row-id assignment (`crates/storage` + `crates/txn`)

**Files:**
- Modify: `crates/storage/src/manifest.rs` (add `next_row_id: u64`, update 5 existing test literals)
- Modify: `crates/txn/src/error.rs` (add a `TryFromInt` variant)
- Modify: `crates/txn/src/dataset.rs` (assign row-ids at commit, append a hidden `_row_id` column, export the column-name constant)
- Modify: `crates/txn/src/lib.rs` (re-export the new constant)

**Interfaces:**
- Produces: `Manifest.next_row_id: u64`; `strata_txn::ROW_ID_COLUMN: &str` (the literal `"_row_id"`, exported so any caller — including Task 6's CLI — can build a schema that requests the hidden column back through the existing `scan`/`scan_with_predicate` API, rather than a new bespoke lookup method). Task 4 consumes `manifest.next_row_id` directly when building delta-log entries; Task 6 consumes `ROW_ID_COLUMN`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/storage/src/manifest.rs`'s `mod tests` — this doesn't need a new test, but every existing `Manifest { version: N, data_files: ... }` literal must gain `next_row_id: 0` once the struct changes, or the crate won't compile. Confirm this compile failure first:

Run: `cargo check -p strata-storage`
Expected (after Step 3 below changes the struct, before Step 4 fixes the literals): FAIL — "missing field `next_row_id`" at 5 call sites in `manifest.rs`'s test module.

Add to `crates/txn/src/dataset.rs`'s `mod tests`:

```rust
#[test]
fn row_ids_are_assigned_sequentially_and_monotonically_across_commits() {
    let dir = temp_dir("row-id-sequential");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir).unwrap();

    let first = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mut txn = ds.begin();
    txn.insert(first);
    let ds = txn.commit().unwrap();

    let second =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![40, 50]))]).unwrap();
    let mut txn = ds.begin();
    txn.insert(second);
    let ds = txn.commit().unwrap();

    let data_dir = ds.data_dir();
    let first_on_disk = read_batch(&data_dir.join(&ds.data_files()[0].name)).unwrap();
    let second_on_disk = read_batch(&data_dir.join(&ds.data_files()[1].name)).unwrap();

    let row_id_col = |batch: &RecordBatch| -> Vec<u64> {
        let idx = batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        (0..arr.len()).map(|i| arr.value(i)).collect()
    };

    assert_eq!(row_id_col(&first_on_disk), vec![0, 1, 2]);
    assert_eq!(row_id_col(&second_on_disk), vec![3, 4]);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn row_id_column_never_leaks_into_scan_output() {
    let dir = temp_dir("row-id-hidden");
    let schema = test_schema(); // just "id", no _row_id
    let ds = Dataset::create(&dir).unwrap();

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
            .unwrap();
    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();

    let scanned = ds.scan(&schema).unwrap();
    assert_eq!(
        scanned.schema_ref().fields().len(),
        1,
        "_row_id must not appear in scan() output when the caller's schema doesn't ask for it"
    );
    assert!(scanned.schema_ref().index_of(ROW_ID_COLUMN).is_err());

    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn row_ids_are_assigned_sequentially_and_monotonically_across_commits row_id_column_never_leaks_into_scan_output`
Expected: FAIL to compile — `ROW_ID_COLUMN` doesn't exist, and no `_row_id` column is written yet.

- [ ] **Step 3: Add `next_row_id` to `Manifest`**

In `crates/storage/src/manifest.rs`, change:

```rust
// Old:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Accumulated across every committed version so far.
    pub data_files: Vec<DataFileEntry>,
}

impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            data_files: Vec::new(),
        }
    }
}
// New:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Accumulated across every committed version so far.
    pub data_files: Vec<DataFileEntry>,
    /// The row-id to assign to the next inserted row, dataset-wide. Never
    /// resets, never reused — see
    /// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
    pub next_row_id: u64,
}

impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            data_files: Vec::new(),
            next_row_id: 0,
        }
    }
}
```

- [ ] **Step 4: Fix the 5 existing test literals in `manifest.rs`**

Each `Manifest { version: N, data_files: ... }` literal in `mod tests` needs `next_row_id: 0` added (none of the existing tests care about its value, so `0` is correct for all of them — they're testing serde round-tripping and crash-safety, not row-id semantics). Apply to `commit_then_read_current_round_trips` (both `m0` and `m1`), `leftover_tmp_file_is_never_picked_up_as_current`'s `m0`, and `commit_then_read_current_with_populated_stats`'s `m0`. Example:

```rust
// Old:
let m0 = Manifest {
    version: 0,
    data_files: vec![DataFileEntry {
        name: "a.arrow".to_string(),
        stats: HashMap::new(),
    }],
};
// New:
let m0 = Manifest {
    version: 0,
    data_files: vec![DataFileEntry {
        name: "a.arrow".to_string(),
        stats: HashMap::new(),
    }],
    next_row_id: 0,
};
```

- [ ] **Step 5: Run `strata-storage`'s tests**

Run: `cargo test -p strata-storage`
Expected: all pass (13 tests, unchanged count — this step only fixes compilation, no new storage-level test was added).

- [ ] **Step 6: Add `TryFromInt` to `TxnError`**

In `crates/txn/src/error.rs`:

```rust
// Old:
#[derive(Debug, Error)]
pub enum TxnError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] strata_storage::StorageError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("a dataset already exists at {0} — use Dataset::open instead")]
    AlreadyExists(PathBuf),
    #[error("no dataset found at {0} — call Dataset::create first")]
    NotFound(PathBuf),
}
// New:
#[derive(Debug, Error)]
pub enum TxnError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] strata_storage::StorageError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("a dataset already exists at {0} — use Dataset::open instead")]
    AlreadyExists(PathBuf),
    #[error("no dataset found at {0} — call Dataset::create first")]
    NotFound(PathBuf),
    #[error("row count overflowed u64: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
}
```

- [ ] **Step 7: Assign row-ids and append `_row_id` in `crates/txn/src/dataset.rs`**

Update imports:

```rust
// Old:
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::SchemaRef;
// New:
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
```

Add the column-name constant near the top of the file (after the imports, before `pub struct Dataset`):

```rust
/// The hidden internal row-id column every committed batch carries
/// alongside its logical columns. Exported so callers that need it back
/// (e.g. the CLI's `search` subcommand, Task 6) can request it through the
/// existing `scan`/`scan_with_predicate` API by including it in their own
/// schema, rather than needing a bespoke lookup method. See
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
pub const ROW_ID_COLUMN: &str = "_row_id";
```

Change `Transaction::commit`'s loop:

```rust
// Old:
for (i, batch) in self.pending.iter().enumerate() {
    // Stats computed on the original, pre-encoding batch — see
    // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
    // (logical values, no dictionary-decode step needed later).
    let stats = compute_stats(batch);
    let encoded = strata_storage::encode_batch(batch)?;
    let file_name = format!("{new_version:020}-{i}.arrow");
    write_batch(&data_dir.join(&file_name), &encoded)?;
    manifest.data_files.push(DataFileEntry {
        name: file_name,
        stats,
    });
}
// New:
for (i, batch) in self.pending.iter().enumerate() {
    // Stats computed on the original, pre-encoding, pre-row-id batch — see
    // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
    // (logical values, no dictionary-decode step needed later; _row_id is
    // an internal column, not a user column subject to file-pruning stats).
    let stats = compute_stats(batch);

    let num_rows = u64::try_from(batch.num_rows())?;
    let row_id_base = manifest.next_row_id;
    manifest.next_row_id += num_rows;
    let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

    let encoded = strata_storage::encode_batch(&with_row_id)?;
    let file_name = format!("{new_version:020}-{i}.arrow");
    write_batch(&data_dir.join(&file_name), &encoded)?;
    manifest.data_files.push(DataFileEntry {
        name: file_name,
        stats,
    });
}
```

Add the helper function near `cast_batch_to_schema` (after it, at the module level):

```rust
/// Appends a `_row_id: UInt64` column to `batch`, assigning
/// `row_id_base..row_id_base + num_rows` in row order. This is what makes
/// every committed row addressable by a stable, global identity — see
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
fn append_row_id_column(batch: &RecordBatch, row_id_base: u64, num_rows: u64) -> Result<RecordBatch> {
    let row_ids: Vec<u64> = (0..num_rows).map(|i| row_id_base + i).collect();
    let row_id_array: ArrayRef = Arc::new(UInt64Array::from(row_ids));

    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(row_id_array);

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}
```

Note why `scan`/`cast_batch_to_schema` need **no changes**: `cast_batch_to_schema` zips `batch.columns()` with `schema.fields()` positionally and stops at the shorter of the two. When a caller's schema (e.g. the existing tests' `test_schema()`, just `id`) doesn't mention `_row_id`, the zip naturally produces only as many output columns as the caller's schema has fields — the trailing `_row_id` column is silently and correctly excluded from `RecordBatch::try_new`'s output. `Step 1`'s `row_id_column_never_leaks_into_scan_output` test locks this in as an explicit, intentional regression guard rather than an implicit reliance on `zip`'s behavior.

- [ ] **Step 8: Wire up `lib.rs`**

`crates/txn/src/lib.rs`:

```rust
pub use dataset::{Dataset, ROW_ID_COLUMN, Transaction};
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cargo test -p strata-txn -p strata-cli`
Expected: all pass, including the 2 new tests and every pre-existing test (in particular `insert_then_commit_then_scan_round_trips`'s `assert_eq!(scanned, batch)` — a full-batch equality check against the caller's original, `_row_id`-free schema — is unchanged and must still pass, confirming the hidden column really is invisible to existing callers).

- [ ] **Step 10: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Then `git add crates/storage/src/manifest.rs crates/txn/src/error.rs crates/txn/src/dataset.rs crates/txn/src/lib.rs && git commit -m "feat(txn): assign a stable global row-id to every committed row"`.

---

### Task 2: `HnswIndex` wrapper (`crates/index`)

**Files:**
- Create: `crates/index/src/hnsw.rs`
- Modify: `crates/index/src/lib.rs`

**Interfaces:**
- Produces: `pub struct VectorMatch { pub row_id: u64, pub squared_distance: f32 }`, `pub struct HnswIndex`, `HnswIndex::new(max_nb_connection, max_elements, max_layer, ef_construction) -> Result<Self, IndexError>`, `HnswIndex::insert(&self, row_id: u64, vector: &[f32])`, `HnswIndex::tombstone(&mut self, row_id: u64)`, `HnswIndex::search(&self, query: &[f32], k: usize, ef_search: usize) -> Result<Vec<VectorMatch>, IndexError>`, `HnswIndex::search_filtered(&self, query: &[f32], k: usize, ef_search: usize, live_ids: &[usize]) -> Result<Vec<VectorMatch>, IndexError>`. Task 4 consumes `insert`/`tombstone` (delta-log replay); Task 5 consumes `search`/`search_filtered`.

This task is self-contained — no dependency on Task 1's row-id work, since `HnswIndex` only ever receives a `row_id: u64` as a parameter; it doesn't know or care how that id was assigned.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/index/src/hnsw.rs
//! HNSW vector index wrapper. See `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-4-vector-index-spec.md` §1.

use std::collections::HashSet;

use hnsw_rs::prelude::{DistL2, FilterT, Hnsw};

/// One search result: which row-id, and its squared L2 distance to the
/// query vector. `row_id` is the persistent, global identity from
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 — not a
/// position within any particular array, unlike `brute_force::Neighbour`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorMatch {
    pub row_id: u64,
    pub squared_distance: f32,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("max_nb_connection must be <= 256 (hnsw_rs hard limit), got {0}")]
    MaxConnectionTooLarge(usize),
    #[error("query has {query_len} dimensions, but the index expects {expected}")]
    DimensionMismatch { query_len: usize, expected: usize },
}

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistL2>,
    tombstones: HashSet<u64>,
    dimension: usize,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_search_finds_the_true_nearest_neighbor() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]);
        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[10.0, 10.0, 10.0]);

        let results = index.search(&[0.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
        assert_eq!(results[1].row_id, 1);
    }

    #[test]
    fn tombstoned_row_is_never_returned_even_as_the_true_nearest_neighbor() {
        let index = {
            let mut index = HnswIndex::new(16, 100, 16, 200).unwrap();
            index.insert(0, &[0.0, 0.0, 0.0]);
            index.insert(1, &[5.0, 5.0, 5.0]);
            index.tombstone(0);
            index
        };

        let results = index.search(&[0.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(results.len(), 1, "the tombstoned row must be excluded, not just re-ranked");
        assert_eq!(results[0].row_id, 1);
    }

    #[test]
    fn search_filtered_only_returns_ids_in_the_live_set() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]);
        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[2.0, 0.0, 0.0]);

        // Only row 2 is "live" per the caller's predicate, even though rows
        // 0 and 1 are closer to the query.
        let live_ids = [2usize];
        let results = index.search_filtered(&[0.0, 0.0, 0.0], 2, 50, &live_ids).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 2);
    }

    #[test]
    fn new_rejects_max_nb_connection_above_256() {
        let result = HnswIndex::new(257, 100, 16, 200);
        assert!(matches!(result, Err(IndexError::MaxConnectionTooLarge(257))));
    }

    #[test]
    fn search_errors_on_dimension_mismatch() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]);

        let result = index.search(&[0.0, 0.0], 1, 50);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch { query_len: 2, expected: 3 })
        ));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-index hnsw`
Expected: FAIL to compile — `HnswIndex::new`/`insert`/`tombstone`/`search`/`search_filtered` not defined.

- [ ] **Step 3: Add `thiserror` as a dependency**

`crates/index/Cargo.toml`:

```toml
[dependencies]
arrow.workspace = true
hnsw_rs.workspace = true
thiserror.workspace = true
```

(`hnsw_rs.workspace = true` was already present, unused until now — see the comment already on that line, which this task makes true.)

- [ ] **Step 4: Implement `HnswIndex`**

Add above the `#[cfg(test)]` block in `crates/index/src/hnsw.rs`:

```rust
impl HnswIndex {
    /// # Errors
    ///
    /// Returns [`IndexError::MaxConnectionTooLarge`] if `max_nb_connection`
    /// exceeds 256 — checked here because the underlying `hnsw_rs::Hnsw::new`
    /// calls `std::process::exit(1)` (not a panic — an uncatchable process
    /// exit) on that condition, verified against the installed
    /// `hnsw_rs-0.3.4` source. This function must never let a bad
    /// caller-supplied value reach that call.
    pub fn new(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> Result<Self, IndexError> {
        if max_nb_connection > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection));
        }
        Ok(Self {
            hnsw: Hnsw::new(max_nb_connection, max_elements, max_layer, ef_construction, DistL2 {}),
            tombstones: HashSet::new(),
            dimension: 0,
        })
    }

    pub fn insert(&self, row_id: u64, vector: &[f32]) {
        #[allow(clippy::cast_possible_truncation)]
        let id = row_id as usize;
        self.hnsw.insert((vector, id));
    }

    pub fn tombstone(&mut self, row_id: u64) {
        self.tombstones.insert(row_id);
    }

    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `query`'s length
    /// doesn't match the dimensionality of the first vector ever inserted —
    /// checked upfront rather than silently truncating, matching
    /// `brute_force_search`'s existing Phase 1 behavior.
    pub fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        let raw = self.hnsw.search(query, k, ef_search);
        Ok(self.to_matches(raw))
    }

    /// # Errors
    ///
    /// Same as [`Self::search`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        // live_ids is caller-supplied and expected sorted (hnsw_rs's
        // impl FilterT for Vec<usize> binary-searches it) — sort defensively
        // rather than trusting every caller got this right.
        let mut live_ids = live_ids.to_vec();
        live_ids.sort_unstable();
        let raw = self.hnsw.search_filter(query, k, ef_search, Some(&live_ids as &dyn FilterT));
        Ok(self.to_matches(raw))
    }

    fn check_dimension(&self, query: &[f32]) -> Result<(), IndexError> {
        if self.dimension != 0 && query.len() != self.dimension {
            return Err(IndexError::DimensionMismatch {
                query_len: query.len(),
                expected: self.dimension,
            });
        }
        Ok(())
    }

    fn to_matches(&self, raw: Vec<hnsw_rs::prelude::Neighbour>) -> Vec<VectorMatch> {
        raw.into_iter()
            .filter(|n| !self.tombstones.contains(&(n.get_origin_id() as u64)))
            .map(|n| VectorMatch {
                row_id: n.get_origin_id() as u64,
                squared_distance: n.get_distance(),
            })
            .collect()
    }
}
```

`self.dimension` starts at `0` (meaning "unknown, not yet enforced") and should be set on the first `insert` call — add this one line inside `insert`, before the `self.hnsw.insert(...)` call, using interior mutability since `insert` takes `&self` (matching `hnsw_rs::Hnsw::insert`'s own `&self` signature, which relies on internal locking — verified against the installed source, `crates/index` does not need its own lock since it only ever assigns `dimension` from a single field write): change `dimension: usize` to `dimension: std::sync::atomic::AtomicUsize` in the struct, initialized via `AtomicUsize::new(0)`, and in `insert`:

```rust
pub fn insert(&self, row_id: u64, vector: &[f32]) {
    self.dimension
        .compare_exchange(0, vector.len(), std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
        .ok(); // only the first insert sets it; later calls leave it as-is
    #[allow(clippy::cast_possible_truncation)]
    let id = row_id as usize;
    self.hnsw.insert((vector, id));
}
```

And `check_dimension` reads `self.dimension.load(std::sync::atomic::Ordering::SeqCst)` instead of `self.dimension` directly. Update the struct definition and `new`'s constructor accordingly (`dimension: std::sync::atomic::AtomicUsize::new(0)`).

`hnsw_rs::prelude::Neighbour::get_origin_id()` returns `DataId` (verified: `pub type DataId = usize;` in the installed source), so `n.get_origin_id() as u64` is a widening cast on every platform this project targets (64-bit only, no 32-bit CI target) — safe, but still needs `#[allow(clippy::cast_possible_truncation)]` at both call sites since clippy can't see the platform assumption; add the allow on `to_matches` and the `row_id as usize` cast in `insert`.

- [ ] **Step 5: Wire up `lib.rs`**

`crates/index/src/lib.rs`:

```rust
pub mod brute_force;
pub mod hnsw;

pub use brute_force::{Neighbour, brute_force_search};
pub use hnsw::{HnswIndex, IndexError, VectorMatch};
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p strata-index`
Expected: all pass — 5 new `hnsw::tests::*` plus the 2 existing `brute_force::tests::*` unaffected.

- [ ] **Step 7: Checkpoint**

Run: `cargo clippy -p strata-index --all-targets -- -D warnings && cargo fmt -p strata-index --check`
Expected: clean. Then `git add crates/index/ && git commit -m "feat(index): add HnswIndex wrapper around hnsw_rs"`.

---

### Task 3: Delta-log entry types + I/O (`crates/index`)

**Files:**
- Create: `crates/index/src/delta_log.rs`
- Modify: `crates/index/src/lib.rs`

**Interfaces:**
- Produces: `pub enum DeltaEntry { Insert { row_id: u64, vector: Vec<f32> }, Tombstone { row_id: u64 } }`, `pub fn write_delta_log(path: &Path, entries: &[DeltaEntry]) -> Result<(), IndexError>`, `pub fn read_delta_log(path: &Path) -> Result<Vec<DeltaEntry>, IndexError>`. Task 4 consumes both functions and the `DeltaEntry` type directly.

Per `.claude/docs/architecture.md`'s module table, the delta log is `crates/index`'s responsibility ("append-only delta log for mutations"), not `crates/storage`'s — `crates/storage` only tracks filenames in the manifest (Task 4), never interprets delta-log content.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/index/src/delta_log.rs
//! Append-only delta log for vector-index mutations. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 and
//! `.claude/rules/vector-index.md`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::hnsw::IndexError;

/// One vector-index mutation. `Insert` enters a row-id's embedding into the
/// graph for the first time; `Tombstone` logically removes it (used for
/// DELETE and as half of an UPDATE — see the Phase 0 spec §8; no
/// `Tombstone` entries are produced by Phase 4's write path, but the type
/// and the read/replay path support it so Phase 5/6 don't need to touch
/// this module).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeltaEntry {
    Insert { row_id: u64, vector: Vec<f32> },
    Tombstone { row_id: u64 },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "strata-delta-log-test-{label}-{}.jsonl",
            std::process::id()
        ))
    }

    #[test]
    fn write_then_read_round_trips_insert_and_tombstone_entries() {
        let path = temp_path("round-trip");
        let entries = vec![
            DeltaEntry::Insert { row_id: 0, vector: vec![1.0, 2.0, 3.0] },
            DeltaEntry::Insert { row_id: 1, vector: vec![4.0, 5.0, 6.0] },
            DeltaEntry::Tombstone { row_id: 0 },
        ];

        write_delta_log(&path, &entries).unwrap();
        let read_back = read_delta_log(&path).unwrap();

        assert_eq!(read_back, entries);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_of_missing_file_errors_instead_of_panicking() {
        let path = temp_path("missing");
        let result = read_delta_log(&path);
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-index delta_log`
Expected: FAIL to compile — `write_delta_log`/`read_delta_log`/`DeltaEntry` not defined.

- [ ] **Step 3: Add `serde`/`serde_json` as dependencies**

`crates/index/Cargo.toml`:

```toml
[dependencies]
arrow.workspace = true
hnsw_rs.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

- [ ] **Step 4: Add an `Io`/`Serde` variant to `IndexError`**

In `crates/index/src/hnsw.rs`, extend the enum added in Task 2:

```rust
// Old:
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("max_nb_connection must be <= 256 (hnsw_rs hard limit), got {0}")]
    MaxConnectionTooLarge(usize),
    #[error("query has {query_len} dimensions, but the index expects {expected}")]
    DimensionMismatch { query_len: usize, expected: usize },
}
// New:
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("max_nb_connection must be <= 256 (hnsw_rs hard limit), got {0}")]
    MaxConnectionTooLarge(usize),
    #[error("query has {query_len} dimensions, but the index expects {expected}")]
    DimensionMismatch { query_len: usize, expected: usize },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("delta log entry serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}
```

- [ ] **Step 5: Implement `write_delta_log`/`read_delta_log`**

Add above the `#[cfg(test)]` block in `crates/index/src/delta_log.rs`:

```rust
/// Writes `entries` to `path` as newline-delimited JSON (one `DeltaEntry`
/// per line) — matches this project's existing JSON-based durable format
/// (the manifest, `crates/storage::manifest`), so no new serialization
/// dependency (e.g. bincode) is introduced for a format nothing else needs
/// to be maximally compact.
///
/// # Errors
///
/// Returns an [`IndexError::Io`] if `path` can't be written, or
/// [`IndexError::Serde`] if an entry fails to serialize.
pub fn write_delta_log(path: &Path, entries: &[DeltaEntry]) -> Result<(), IndexError> {
    use std::io::Write as _;
    let mut file = std::fs::File::create(path)?;
    for entry in entries {
        let line = serde_json::to_string(entry)?;
        writeln!(file, "{line}")?;
    }
    file.sync_all()?;
    Ok(())
}

/// Reads back every entry written by [`write_delta_log`], in order.
///
/// # Errors
///
/// Returns an [`IndexError::Io`] if `path` can't be read, or
/// [`IndexError::Serde`] if a line fails to parse.
pub fn read_delta_log(path: &Path) -> Result<Vec<DeltaEntry>, IndexError> {
    let content = std::fs::read_to_string(path)?;
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(IndexError::from))
        .collect()
}
```

- [ ] **Step 6: Wire up `lib.rs`**

`crates/index/src/lib.rs`:

```rust
pub mod brute_force;
pub mod delta_log;
pub mod hnsw;

pub use brute_force::{Neighbour, brute_force_search};
pub use delta_log::{DeltaEntry, read_delta_log, write_delta_log};
pub use hnsw::{HnswIndex, IndexError, VectorMatch};
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p strata-index`
Expected: all pass — 2 new `delta_log::tests::*` plus everything from Task 2 unaffected.

- [ ] **Step 8: Checkpoint**

Run: `cargo clippy -p strata-index --all-targets -- -D warnings && cargo fmt -p strata-index --check`
Expected: clean. Then `git add crates/index/ && git commit -m "feat(index): add append-only delta-log entry types and I/O"`.

---

### Task 4: Wire delta log into commit + replay on open (`crates/txn`)

**Files:**
- Modify: `crates/storage/src/manifest.rs` (add `delta_log: String` to `DataFileEntry`)
- Modify: `crates/txn/Cargo.toml` (promote `strata-index` to a real dependency)
- Modify: `crates/txn/src/dataset.rs` (write delta log at commit, replay at open, cache the index on `Dataset`)

**Interfaces:**
- Consumes: `strata_index::{DeltaEntry, HnswIndex, write_delta_log, read_delta_log}` (Tasks 2/3)
- Produces: `Dataset` gains a private cached `HnswIndex`. Task 5 consumes it directly (same crate, same struct).

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/dataset.rs`'s `mod tests`:

```rust
#[test]
fn reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log() {
    let dir = temp_dir("delta-log-replay");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]));
    let ds = Dataset::create(&dir).unwrap();

    let ids = Arc::new(Int64Array::from(vec![1, 2]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let values = Arc::new(arrow::array::Float32Array::from(vec![
        0.0, 0.0, 0.0, // row 0's vector
        9.0, 9.0, 9.0, // row 1's vector
    ]));
    let vectors = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    let batch = RecordBatch::try_new(schema, vec![ids, vectors]).unwrap();

    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();
    drop(ds);

    // Force a real replay from disk, not an in-memory shortcut — this is
    // the crash-recovery-equivalent test for the index (a fresh Dataset
    // struct, same process, but the index cache is definitely rebuilt from
    // the delta-log file, not carried over).
    let reopened = Dataset::open(&dir).unwrap();
    let results = reopened.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].row_id, 0, "row 0's vector [0,0,0] is the true nearest match");

    std::fs::remove_dir_all(&dir).ok();
}
```

(This test also exercises `vector_search`, added in Task 5 — write it here per TDD since it's the only way to observe that the delta log actually replayed correctly, but it will not compile until Task 5 lands. Confirm the compile failure now, then leave this test in place; Task 5's Step 2 will be the one that turns it green. Note this in the task ledger so the reviewer understands why Task 4 alone leaves one red test — matches this project's precedent for a plan that deliberately spans a red test across two tasks, same as Phase 3's `Manifest` schema change spanning Tasks 1-2.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-txn reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log`
Expected: FAIL to compile — `vector_search` doesn't exist yet (Task 5), and `DataFileEntry` doesn't have a `delta_log` field yet.

- [ ] **Step 3: Add `delta_log: String` to `DataFileEntry`**

In `crates/storage/src/manifest.rs`:

```rust
// Old:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
}
// New:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
    /// Relative to the dataset's `data/` directory. This commit's vector-
    /// index delta-log entries — see
    /// `.claude/docs/design/phase-4-vector-index-spec.md` §2.
    pub delta_log: String,
}
```

Fix the 5 `DataFileEntry { name: ..., stats: ... }` literals across `manifest.rs`'s test module (the same 5 sites touched by Task 1's Step 4, plus `commit_then_read_current_with_populated_stats`'s) to add `delta_log: "d.deltalog".to_string()` (any placeholder string works — these tests exercise serde round-tripping, not delta-log content). Also fix `crates/txn/src/dataset.rs`'s two `DataFileEntry { name, stats }` construction sites flagged in Step 5 below (not test literals — real construction in `Transaction::commit` and nowhere else, since `explain`/`scan`/`scan_with_predicate` only ever read `entry.name`/`entry.stats`, never construct a `DataFileEntry`).

- [ ] **Step 4: Promote `strata-index` to a real dependency**

`crates/txn/Cargo.toml`:

```toml
[dependencies]
arrow.workspace = true
thiserror.workspace = true
strata-storage = { path = "../storage" }
strata-query = { path = "../query" }
strata-index = { path = "../index" }
```

(Remove the now-redundant `[dev-dependencies]` block that previously held `strata-index` only for a Phase 2 test — check whether any `#[cfg(test)]` code in `crates/txn` still needs `strata-index` as a dev-only distinction; since it's now a real dependency, the dev-dependencies section can be deleted entirely if `strata-index` was its only entry.)

- [ ] **Step 5: Cache the index on `Dataset`, write the delta log at commit, replay at open**

In `crates/txn/src/dataset.rs`, update imports:

```rust
// Add:
use strata_index::{DeltaEntry, HnswIndex, read_delta_log, write_delta_log};
```

Change `Dataset`'s struct and `create`/`open`:

```rust
// Old:
pub struct Dataset {
    dir: PathBuf,
    manifest: Manifest,
}
// New:
pub struct Dataset {
    dir: PathBuf,
    manifest: Manifest,
    index: HnswIndex,
}
```

```rust
// Old (create):
pub fn create(dir: impl Into<PathBuf>) -> Result<Self> {
    let dir = dir.into();
    if read_current(&dir)?.is_some() {
        return Err(TxnError::AlreadyExists(dir));
    }
    std::fs::create_dir_all(dir.join("data"))?;
    let manifest = Manifest::empty();
    commit_manifest(&dir, &manifest)?;
    Ok(Self { dir, manifest })
}
// New (create):
pub fn create(dir: impl Into<PathBuf>) -> Result<Self> {
    let dir = dir.into();
    if read_current(&dir)?.is_some() {
        return Err(TxnError::AlreadyExists(dir));
    }
    std::fs::create_dir_all(dir.join("data"))?;
    let manifest = Manifest::empty();
    commit_manifest(&dir, &manifest)?;
    let index = new_hnsw_index(0)?;
    Ok(Self { dir, manifest, index })
}
```

```rust
// Old (open):
pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
    let dir = dir.into();
    let manifest = read_current(&dir)?.ok_or_else(|| TxnError::NotFound(dir.clone()))?;
    Ok(Self { dir, manifest })
}
```

`Tombstone` handling needs `index` to be `mut` at the point of replay (`HnswIndex::tombstone` takes `&mut self`), while `insert` takes `&self`. Since Phase 4 never produces `Tombstone` entries (§11 of the spec — no DELETE/UPDATE API ships yet), write the replay loop with a mutable `index` binding regardless, so both match arms compile and the `Tombstone` arm is real, dead-but-correct code ready for Phase 5/6:

```rust
// New (open):
pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
    let dir = dir.into();
    let manifest = read_current(&dir)?.ok_or_else(|| TxnError::NotFound(dir.clone()))?;
    let capacity = usize::try_from(manifest.next_row_id).unwrap_or(usize::MAX);
    let mut index = new_hnsw_index(capacity)?;
    let data_dir = dir.join("data");
    for entry in &manifest.data_files {
        for delta in read_delta_log(&data_dir.join(&entry.delta_log))? {
            match delta {
                DeltaEntry::Insert { row_id, vector } => index.insert(row_id, &vector),
                DeltaEntry::Tombstone { row_id } => index.tombstone(row_id),
            }
        }
    }
    Ok(Self { dir, manifest, index })
}
```

Add the shared constructor helper (module level, near `cast_batch_to_schema`):

```rust
// HNSW parameter defaults — small, correctness-only values for now.
// Task 7's benchmark is what tunes the real production defaults; see
// .claude/rules/vector-index.md ("tuned via benchmarks, not guessed").
const HNSW_MAX_NB_CONNECTION: usize = 16;
const HNSW_MAX_LAYER: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;

fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        HNSW_MAX_NB_CONNECTION,
        capacity.max(1),
        HNSW_MAX_LAYER,
        HNSW_EF_CONSTRUCTION,
    )?)
}
```

(`capacity.max(1)` since `hnsw_rs`'s `max_elements` is only a `Vec::with_capacity` hint — verified in Task 2 — `0` is legal but `1` avoids an edge case in per-layer capacity math being exercised needlessly on an empty dataset.) This requires `IndexError` to convert into `TxnError` — add a variant:

```rust
// crates/txn/src/error.rs, add:
#[error(transparent)]
Index(#[from] strata_index::IndexError),
```

Now update `Transaction::commit` to write the delta log and populate `DataFileEntry.delta_log`. The commit loop needs the vector column's data to build `DeltaEntry::Insert` entries — extract it the same way `crates/cli`'s existing `search` subcommand already does (downcast the batch's `"vector"` column to `FixedSizeListArray`, then each row's values to `Float32Array`):

```rust
// Old (inside commit(), the per-batch loop body from Task 1):
let num_rows = u64::try_from(batch.num_rows())?;
let row_id_base = manifest.next_row_id;
manifest.next_row_id += num_rows;
let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

let encoded = strata_storage::encode_batch(&with_row_id)?;
let file_name = format!("{new_version:020}-{i}.arrow");
write_batch(&data_dir.join(&file_name), &encoded)?;
manifest.data_files.push(DataFileEntry {
    name: file_name,
    stats,
});
// New:
let num_rows = u64::try_from(batch.num_rows())?;
let row_id_base = manifest.next_row_id;
manifest.next_row_id += num_rows;
let with_row_id = append_row_id_column(batch, row_id_base, num_rows)?;

let encoded = strata_storage::encode_batch(&with_row_id)?;
let file_name = format!("{new_version:020}-{i}.arrow");
write_batch(&data_dir.join(&file_name), &encoded)?;

let deltas = build_delta_entries(batch, row_id_base)?;
let delta_file_name = format!("{new_version:020}-{i}.deltalog");
write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

manifest.data_files.push(DataFileEntry {
    name: file_name,
    stats,
    delta_log: delta_file_name,
});
```

Add `build_delta_entries` (module level):

```rust
/// Builds one `Insert` delta-log entry per row in `batch` with a non-null
/// vector, keyed by the row-ids assigned starting at `row_id_base` — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §2.
///
/// # Errors
///
/// Returns an error if `batch` has no `"vector"` column, or if it isn't a
/// `FixedSizeList<Float32>`.
fn build_delta_entries(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<DeltaEntry>> {
    let vec_idx = batch.schema_ref().index_of("vector")?;
    let vectors = batch
        .column(vec_idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .ok_or_else(|| TxnError::Arrow(arrow::error::ArrowError::CastError(
            "vector column must be FixedSizeList".to_string(),
        )))?;

    let mut entries = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        if vectors.is_null(i) {
            continue;
        }
        let row = vectors.value(i);
        let row: &arrow::array::Float32Array = row
            .as_any()
            .downcast_ref()
            .ok_or_else(|| TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column's inner type must be Float32".to_string(),
            )))?;
        entries.push(DeltaEntry::Insert {
            row_id: row_id_base + u64::try_from(i)?,
            vector: row.values().to_vec(),
        });
    }
    Ok(entries)
}
```

`sync_dir(&data_dir)` (already called after the loop) covers the new `.deltalog` files the same as `.arrow` files, since it fsyncs the whole directory — no change needed there.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p strata-txn -p strata-cli`
Expected: `reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log` now passes (Task 5 landed `vector_search` in the same commit range this task's implementer produces — if executing task-by-task via subagent-driven-development, this specific test stays red until Task 5 completes; that is expected and was flagged in Step 1). Every other test passes, including all of Task 1's row-id tests and every pre-existing test.

- [ ] **Step 7: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean (the `reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log` test failing to *pass* is fine at this checkpoint if Task 5 hasn't landed yet in a task-by-task execution — but it must still *compile* once Task 5's signatures exist; if executing tasks strictly in order, expect this test to fail at runtime here and note that in the task's report rather than treating it as a blocker). Then `git add crates/storage/src/manifest.rs crates/txn/ && git commit -m "feat(txn): write per-commit delta-log files and replay them into a cached HnswIndex on open"`.

---

### Task 5: `Dataset::vector_search` + `ef` widening (`crates/txn`)

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `HnswIndex::search`/`search_filtered` (Task 2), `strata_query::{Predicate, filter}` (already a dependency), `Dataset::explain` (Phase 3, already exists)
- Produces: `Dataset::vector_search(&self, query: &[f32], k: usize, predicate: Option<&Predicate>) -> Result<Vec<VectorMatch>>`. Task 6's CLI consumes this directly.

- [ ] **Step 1: Write the failing tests**

Add to `crates/txn/src/dataset.rs`'s `mod tests`:

```rust
fn vector_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]))
}

fn vector_batch(ids: Vec<i64>, vectors: Vec<[f32; 3]>) -> RecordBatch {
    let id_arr = Arc::new(Int64Array::from(ids));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
    let values = Arc::new(arrow::array::Float32Array::from(flat));
    let vec_arr = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    RecordBatch::try_new(vector_test_schema(), vec![id_arr, vec_arr]).unwrap()
}

#[test]
fn vector_search_without_predicate_finds_the_true_nearest_neighbor() {
    let dir = temp_dir("vector-search-unfiltered");
    let ds = Dataset::create(&dir).unwrap();

    let batch = vector_batch(
        vec![1, 2, 3],
        vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [10.0, 10.0, 10.0]],
    );
    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();

    let results = ds.vector_search(&[0.0, 0.0, 0.0], 1, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].row_id, 0); // row-id 0 is the first committed row (id=1)
}

#[test]
fn vector_search_with_predicate_only_returns_matching_rows() {
    use strata_query::Predicate;
    use strata_storage::Value;

    let dir = temp_dir("vector-search-filtered");
    let ds = Dataset::create(&dir).unwrap();

    // Two vectors close to the query; only id=2's row should survive the
    // predicate `id eq 2`, even though id=1's vector is the true nearest
    // neighbor of the query.
    let batch = vector_batch(vec![1, 2], vec![[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]]);
    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();

    let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
    let results = ds.vector_search(&[0.0, 0.0, 0.0], 5, Some(&predicate)).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].row_id, 1); // row-id 1 is the second committed row (id=2)

    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn vector_search`
Expected: FAIL to compile — `vector_search` doesn't exist yet.

- [ ] **Step 3: Implement `widen_ef` and `vector_search`**

Add module-level constants and the `widen_ef` helper (near the other `HNSW_*` constants added in Task 4):

```rust
const EF_SEARCH_DEFAULT: usize = 64;
const MIN_SELECTIVITY_FLOOR: f64 = 0.01;
const MAX_EF_SCALE: f64 = 20.0;

/// Widens `base_ef` using `Dataset::explain`'s scanned/total file ratio as
/// a coarse, file-granularity *upper bound* on selectivity — see
/// `.claude/docs/design/phase-4-vector-index-spec.md` §4. Erring toward a
/// wider `ef` costs search time, never correctness, so an overestimate of
/// how many rows survive is the safe direction.
fn widen_ef(base_ef: usize, dataset: &Dataset, predicate: &Predicate) -> usize {
    let explain = dataset.explain(predicate);
    #[allow(clippy::cast_precision_loss)]
    let selectivity_upper_bound =
        explain.scanned.len() as f64 / explain.total_files.max(1) as f64;
    let scale = (1.0 / selectivity_upper_bound.max(MIN_SELECTIVITY_FLOOR)).min(MAX_EF_SCALE);
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let widened = ((base_ef as f64) * scale).round() as usize;
    widened
}
```

Add `vector_search` inside `impl Dataset`, after `scan_with_predicate`:

```rust
/// Approximate nearest-neighbor search over the vector column, optionally
/// narrowed to rows matching `predicate`. See
/// `.claude/docs/design/phase-4-vector-index-spec.md` §3-4.
///
/// # Errors
///
/// Returns an error if `predicate` is supplied and its column doesn't
/// exist or its value's type doesn't match the column's Arrow type
/// (surfaced by the same row-id resolution path `filter`/
/// `scan_with_predicate` already use), or if `query`'s dimensionality
/// doesn't match the indexed vectors'.
pub fn vector_search(
    &self,
    query: &[f32],
    k: usize,
    predicate: Option<&Predicate>,
) -> Result<Vec<strata_index::VectorMatch>> {
    let Some(predicate) = predicate else {
        return Ok(self.index.search(query, k, EF_SEARCH_DEFAULT)?);
    };

    let mut live_ids = self.row_ids_matching(predicate)?;
    live_ids.sort_unstable();
    let ef = widen_ef(EF_SEARCH_DEFAULT, self, predicate);
    Ok(self.index.search_filtered(query, k, ef, &live_ids)?)
}

/// Resolves the row-ids of every row matching `predicate`, reading each
/// surviving (per `should_scan_file`) file's raw on-disk batch directly —
/// not through the public `scan_with_predicate`, whose caller-supplied
/// logical schema never includes `ROW_ID_COLUMN` and would drop it (see
/// Task 1's note on `cast_batch_to_schema`'s positional zip).
fn row_ids_matching(&self, predicate: &Predicate) -> Result<Vec<usize>> {
    let data_dir = self.data_dir();
    let mut ids = Vec::new();
    for entry in &self.manifest.data_files {
        if !should_scan_file(&entry.stats, predicate) {
            continue;
        }
        let batch = read_batch(&data_dir.join(&entry.name))?;
        let matched = filter(&batch, predicate)?;
        let row_id_idx = matched.schema_ref().index_of(ROW_ID_COLUMN)?;
        let row_ids = matched
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| TxnError::Arrow(arrow::error::ArrowError::CastError(
                format!("{ROW_ID_COLUMN} column must be UInt64"),
            )))?;
        #[allow(clippy::cast_possible_truncation)]
        ids.extend((0..row_ids.len()).map(|i| row_ids.value(i) as usize));
    }
    Ok(ids)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-txn`
Expected: all pass — the 2 new `vector_search` tests, Task 4's `reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log` (now green), and every pre-existing test.

- [ ] **Step 5: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Then `git add crates/txn/src/dataset.rs && git commit -m "feat(txn): add Dataset::vector_search with predicate-narrowed ef widening"`.

---

### Task 6: CLI wiring (`crates/cli`)

**Files:**
- Modify: `crates/cli/src/main.rs`

**Interfaces:**
- Consumes: `Dataset::vector_search` (Task 5), `strata_txn::ROW_ID_COLUMN` (Task 1), `strata_index::brute_force_search` (existing), `strata_query::Predicate` (existing)

- [ ] **Step 1: Replace the `"search"` arm**

In `crates/cli/src/main.rs`, replace the existing `"search"` match arm:

```rust
// Old:
"search" => {
    let v0: f32 = args.get(3).ok_or("missing <v0>")?.parse()?;
    let v1: f32 = args.get(4).ok_or("missing <v1>")?.parse()?;
    let v2: f32 = args.get(5).ok_or("missing <v2>")?.parse()?;
    let k: usize = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(3);
    let ds = strata_txn::Dataset::open(dir)?;
    let batch = ds.scan(&mvp_schema())?;
    let vec_idx = batch.schema_ref().index_of("vector")?;
    let vectors = batch
        .column(vec_idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or("vector column has wrong type")?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id column has wrong type")?;
    for n in strata_index::brute_force_search(vectors, &[v0, v1, v2], k)? {
        println!(
            "id={} squared_distance={}",
            ids.value(n.row_index),
            n.squared_distance
        );
    }
}
// New:
"search" => handle_search(args, dir)?,
```

(`args` inside `run` is already `&[String]` — matching `handle_search`'s parameter type exactly, no extra `&`.)

Add `handle_search` as a new top-level function (after `run`, alongside `print_batch`) — kept as its own function rather than inline, matching the pattern Phase 3's `handle_explain` already established for the same clippy `too_many_lines` reason:

```rust
fn handle_search(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let exact = args.iter().any(|a| a == "--exact");
    let filter_idx = args.iter().position(|a| a == "--filter");

    let positional: Vec<&String> = args
        .iter()
        .skip(3)
        .take_while(|a| !a.starts_with("--"))
        .collect();
    let v0: f32 = positional.first().ok_or("missing <v0>")?.parse()?;
    let v1: f32 = positional.get(1).ok_or("missing <v1>")?.parse()?;
    let v2: f32 = positional.get(2).ok_or("missing <v2>")?.parse()?;
    let k: usize = positional.get(3).map(|s| s.parse()).transpose()?.unwrap_or(3);

    let predicate = match filter_idx {
        Some(idx) => {
            let column = args.get(idx + 1).ok_or("missing <column> after --filter")?;
            let op = args.get(idx + 2).ok_or("missing <op> after --filter")?;
            let value: i64 = args.get(idx + 3).ok_or("missing <value> after --filter")?.parse()?;
            Some(parse_predicate(column, op, value)?)
        }
        None => None,
    };

    let ds = strata_txn::Dataset::open(dir)?;

    if exact {
        let batch = ds.scan(&mvp_schema())?;
        let vec_idx = batch.schema_ref().index_of("vector")?;
        let vectors = batch
            .column(vec_idx)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or("vector column has wrong type")?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id column has wrong type")?;
        for n in strata_index::brute_force_search(vectors, &[v0, v1, v2], k)? {
            println!("id={} squared_distance={}", ids.value(n.row_index), n.squared_distance);
        }
        return Ok(());
    }

    let matches = ds.vector_search(&[v0, v1, v2], k, predicate.as_ref())?;

    // Scan once, requesting the hidden row-id column back, to translate
    // vector_search's row-ids into the user-facing id/name columns for
    // display — matches this project's "Dataset doesn't translate row-ids
    // back to column values, that's the caller's job" design (see
    // .claude/docs/design/phase-4-vector-index-spec.md §3).
    let mut display_fields = mvp_schema().fields().iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>();
    display_fields.push(Field::new(strata_txn::ROW_ID_COLUMN, DataType::UInt64, false));
    let display_schema = Arc::new(Schema::new(display_fields));
    let batch = ds.scan(&display_schema)?;
    let row_id_idx = batch.schema_ref().index_of(strata_txn::ROW_ID_COLUMN)?;
    let row_ids = batch
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .ok_or("row-id column has wrong type")?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id column has wrong type")?;

    for m in matches {
        for row in 0..batch.num_rows() {
            if row_ids.value(row) == m.row_id {
                println!("id={} squared_distance={}", ids.value(row), m.squared_distance);
                break;
            }
        }
    }
    Ok(())
}

fn parse_predicate(column: &str, op: &str, value: i64) -> Result<strata_query::Predicate, Box<dyn Error>> {
    use strata_query::Predicate;
    use strata_storage::Value;
    match op {
        "eq" => Ok(Predicate::Eq(column.to_string(), Value::Int64(value))),
        "lt" => Ok(Predicate::Lt(column.to_string(), Value::Int64(value))),
        "lteq" => Ok(Predicate::LtEq(column.to_string(), Value::Int64(value))),
        "gt" => Ok(Predicate::Gt(column.to_string(), Value::Int64(value))),
        "gteq" => Ok(Predicate::GtEq(column.to_string(), Value::Int64(value))),
        other => Err(format!("unknown op: {other} (expected eq|lt|lteq|gt|gteq)").into()),
    }
}
```

Add the new imports `main.rs` needs (`Field`/`Schema` are already imported; add `Arc` if not already — it already is, per the existing `use std::sync::Arc;`).

Update the usage line to mention the new flags:

```rust
// Old:
eprintln!(
    "usage: strata <create|insert|scan|filter|search|inspect|crash-loop> <dir> [...]"
);
// New:
eprintln!(
    "usage: strata <create|insert|scan|filter|search|explain|inspect|crash-loop> <dir> [...]"
);
eprintln!(
    "  search <dir> <v0> <v1> <v2> [k] [--exact] [--filter <column> <op> <value>]"
);
```

- [ ] **Step 2: Manually verify the CLI subcommand works end-to-end**

Build and run against a real temp dataset — do this for real, don't skip it:

```bash
cargo build -p strata-cli --quiet
DIR=./tmp-search-smoke
rm -rf "$DIR"
./target/debug/strata create "$DIR"
./target/debug/strata insert "$DIR" 1 alice 0.0 0.0 0.0
./target/debug/strata insert "$DIR" 2 bob 5.0 5.0 5.0
./target/debug/strata search "$DIR" 0.0 0.0 0.0 2
./target/debug/strata search "$DIR" 0.0 0.0 0.0 2 --exact
./target/debug/strata search "$DIR" 0.0 0.0 0.0 2 --filter id eq 2
rm -rf "$DIR"
```

Expected: the first two commands both report `id=1` as the closest match (and `id=2` as the second, unfiltered); the `--filter id eq 2` command reports only `id=2` even though `id=1`'s vector is closer to the query. Capture the real output in the task report.

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all pass, including the existing MVP checklist and crash-recovery integration tests in `crates/txn/tests/`, which don't touch `search` and should be unaffected.

- [ ] **Step 4: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Then `git add crates/cli/src/main.rs && git commit -m "feat(cli): switch search to the HNSW-backed path, add --exact and --filter"`.

---

### Task 7: Benchmark (`bench/`)

**Files:**
- Modify: `bench/Cargo.toml` (add `parquet`, `strata-txn`, `strata-index`, `strata-storage` dependencies)
- Create: `bench/benches/vector_search_bench.rs`
- Modify: `.gitignore` (exclude the downloaded dataset file)

**Interfaces:** none exported — this is the phase's exit-criterion evidence, not a library.

**Dataset:** `Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K` on Hugging Face — 100K real OpenAI `text-embedding-3-small` embeddings (512-dim) over DBpedia entity descriptions, stored as a single Parquet file with columns `_id`, `title`, `text`, `text-embedding-3-small-512-embedding`. Verified live via the HF datasets API immediately before writing this plan: `https://huggingface.co/api/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K/parquet/default/train/0.parquet` resolves to a single downloadable Parquet file, no authentication required (public dataset). This satisfies the spec's "real LLM/text embeddings, not classic CV descriptors" requirement.

- [ ] **Step 1: Download the dataset (one-time, not part of the benchmark binary)**

```bash
mkdir -p bench/data
curl -L "https://huggingface.co/api/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K/parquet/default/train/0.parquet" -o bench/data/dbpedia-openai-100k.parquet
```

This is a ~200MB file — deliberately not committed to the repo (Step 6 gitignores it) and deliberately not fetched over HTTP from inside the benchmark binary itself, since this project has no HTTP client dependency and adding one solely for a one-time dataset download isn't justified when a documented `curl` step does the same job with zero new runtime dependencies. Confirm the file downloaded successfully before continuing:

```bash
ls -la bench/data/dbpedia-openai-100k.parquet
```

Expected: a file roughly 150-250MB in size (exact size depends on Parquet compression of the float32 vectors).

- [ ] **Step 2: Add dependencies**

`bench/Cargo.toml`:

```toml
[package]
name = "strata-bench"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = false

[lints]
workspace = true

[dependencies]
arrow.workspace = true
parquet = "58"
strata-index = { path = "../crates/index" }
strata-query = { path = "../crates/query" }
strata-storage = { path = "../crates/storage" }
strata-txn = { path = "../crates/txn" }

[dev-dependencies]
criterion = { workspace = true, features = ["html_reports"] }

[[bench]]
name = "group_by_bench"
harness = false

[[bench]]
name = "vector_search_bench"
harness = false
```

(`parquet = "58"` pinned to match the workspace's `arrow = "58"` — both crates are versioned in lockstep by the arrow-rs project; verify this is still the current major version before running `cargo build` for the first time, the same live-check discipline this plan already applied to the dataset URL.)

- [ ] **Step 3: Write the benchmark**

**Verify the `parquet` crate's real API before writing this file.** Unlike every `hnsw_rs`/`anndists` signature elsewhere in this plan, the `ParquetRecordBatchReaderBuilder::try_new`/`.build()` API and the embedding column's exact Arrow type (`ListArray` vs. `FixedSizeListArray` — Parquet's own list encoding doesn't distinguish these the way Arrow does, and the HF auto-conversion's choice wasn't independently confirmed while writing this plan) below are written from general knowledge of the arrow-rs `parquet` crate's conventional API shape, not from reading the installed source, because `parquet` isn't a dependency yet at plan-writing time. After Step 2 adds it, run `cargo doc -p parquet --open` or read the installed source the same way this plan verified `hnsw_rs`, and adjust the code below to match reality before trusting it — do not assume the snippet below compiles as-is.

```rust
// bench/benches/vector_search_bench.rs
//! Phase 4 exit-criterion benchmark: recall@10 and QPS for
//! `Dataset::vector_search`, correctness-gated against
//! `strata_index::brute_force_search` before any timing is trusted — same
//! discipline as `group_by_bench.rs` (Phase 2). See
//! `.claude/docs/design/phase-4-vector-index-spec.md` §6.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strata_index::brute_force_search;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

const DATASET_PATH: &str = "bench/data/dbpedia-openai-100k.parquet";
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";
const VECTOR_DIM: usize = 512;
const NUM_QUERIES: usize = 100;
const RECALL_K: usize = 10;

/// Loads up to `limit` (vector, category) pairs from the downloaded Parquet
/// file. `category` is synthesized from the row's position (row % 10) —
/// the real dataset has no natural low-cardinality column, and Phase 4's
/// filtered-search benchmark scenario needs one to exercise `ef` widening
/// against a real (not synthetic-vector) embedding set.
fn load_vectors(limit: usize) -> Vec<(Vec<f32>, i64)> {
    let file = std::fs::File::open(DATASET_PATH).unwrap_or_else(|e| {
        panic!(
            "failed to open {DATASET_PATH}: {e}. Run the download step in \
             .claude/docs/design/phase-4-implementation-plan.md's Task 7 Step 1 first."
        )
    });
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut out = Vec::with_capacity(limit);
    for batch in reader {
        let batch = batch.unwrap();
        let col_idx = batch.schema_ref().index_of(EMBEDDING_COLUMN).unwrap();
        let list = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("embedding column must be a list type");
        for i in 0..batch.num_rows() {
            if out.len() >= limit {
                return out;
            }
            let values = list.value(i);
            let values: &Float32Array = values.as_any().downcast_ref().expect("embedding values must be f32");
            let vector: Vec<f32> = values.values().to_vec();
            let category = i64::try_from(out.len() % 10).unwrap();
            out.push((vector, category));
        }
    }
    out
}

fn build_dataset(dir: &Path, rows: &[(Vec<f32>, i64)]) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), VECTOR_DIM as i32),
            false,
        ),
    ]));
    let ds = Dataset::create(dir).unwrap();

    let ids: Vec<i64> = rows.iter().map(|(_, cat)| *cat).collect();
    let id_arr = Arc::new(Int64Array::from(ids));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let flat: Vec<f32> = rows.iter().flat_map(|(v, _)| v.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let vec_arr = Arc::new(FixedSizeListArray::new(item_field, VECTOR_DIM as i32, values, None));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_arr, vec_arr]).unwrap();

    let mut txn = ds.begin();
    txn.insert(batch);
    txn.commit().unwrap()
}

/// Ground truth via brute force, and the correctness gate: HNSW's
/// recall@10 against it must clear a floor before any QPS number is
/// trusted (mirrors group_by_bench.rs's `check_correctness`).
fn check_recall(ds: &Dataset, rows: &[(Vec<f32>, i64)], queries: &[Vec<f32>]) -> f64 {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), VECTOR_DIM as i32),
        false,
    )]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let flat: Vec<f32> = rows.iter().flat_map(|(v, _)| v.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let vec_arr = Arc::new(FixedSizeListArray::new(item_field, VECTOR_DIM as i32, values, None));
    let batch = RecordBatch::try_new(schema, vec![vec_arr]).unwrap();
    let vectors = batch.column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();

    let mut hits = 0usize;
    for query in queries {
        let exact: std::collections::HashSet<usize> = brute_force_search(vectors, query, RECALL_K)
            .unwrap()
            .into_iter()
            .map(|n| n.row_index)
            .collect();
        let approx: std::collections::HashSet<u64> = ds
            .vector_search(query, RECALL_K, None)
            .unwrap()
            .into_iter()
            .map(|m| m.row_id)
            .collect();
        hits += approx.iter().filter(|row_id| exact.contains(&(**row_id as usize))).count();
    }
    #[allow(clippy::cast_precision_loss)]
    let recall = hits as f64 / (queries.len() * RECALL_K) as f64;
    recall
}

fn bench_vector_search(c: &mut Criterion) {
    let rows = load_vectors(100_000);
    assert_eq!(rows[0].0.len(), VECTOR_DIM, "loaded vectors must match the expected dimensionality");

    let dir = std::env::temp_dir().join(format!("strata-vector-bench-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let ds = build_dataset(&dir, &rows);

    let queries: Vec<Vec<f32>> = rows.iter().take(NUM_QUERIES).map(|(v, _)| v.clone()).collect();

    let recall = check_recall(&ds, &rows, &queries);
    println!("recall@{RECALL_K} = {recall:.4} (over {NUM_QUERIES} queries, {} indexed vectors)", rows.len());
    assert!(recall > 0.8, "recall@{RECALL_K} = {recall:.4} is too low to trust the QPS numbers below it");

    let mut group = c.benchmark_group("vector_search");
    group.bench_function("unfiltered_top_10", |b| {
        b.iter(|| {
            let query = &queries[0];
            ds.vector_search(std::hint::black_box(query), RECALL_K, None).unwrap()
        });
    });

    let predicate = Predicate::Eq("id".to_string(), Value::Int64(3));
    group.bench_function("filtered_top_10_one_of_ten_categories", |b| {
        b.iter(|| {
            let query = &queries[0];
            ds.vector_search(std::hint::black_box(query), RECALL_K, Some(&predicate)).unwrap()
        });
    });
    group.finish();

    std::fs::remove_dir_all(&dir).ok();
}

criterion_group!(benches, bench_vector_search);
criterion_main!(benches);
```

- [ ] **Step 4: Run the benchmark**

Run: `cargo bench -p strata-bench --bench vector_search_bench`
Expected: prints `recall@10 = <value>` (must exceed the `0.8` floor asserted in the code — if it doesn't, this is a real correctness signal, not a benchmark-tuning problem, and must be root-caused before proceeding, not papered over by lowering the threshold), followed by Criterion's own timing report for both the `unfiltered_top_10` and `filtered_top_10_one_of_ten_categories` benchmark functions.

- [ ] **Step 5: Tune `HNSW_MAX_NB_CONNECTION`/`HNSW_EF_CONSTRUCTION`/`EF_SEARCH_DEFAULT`**

Using the benchmark from Step 4, try at least 2-3 combinations of `crates/txn/src/dataset.rs`'s `HNSW_MAX_NB_CONNECTION` (e.g. 12, 16, 24), `HNSW_EF_CONSTRUCTION` (e.g. 100, 200, 400), and `EF_SEARCH_DEFAULT` (e.g. 32, 64, 128), re-running the benchmark after each change, and pick the combination with the best QPS at recall@10 ≥ 0.9 (a stricter floor than Step 4's correctness-gate assertion of 0.8 — 0.8 is "not broken," 0.9 is "good enough to ship as the default"). Record the actual numbers observed for each combination tried.

- [ ] **Step 6: Gitignore the downloaded dataset**

`.gitignore` — add:

```
/bench/data/
```

- [ ] **Step 7: Checkpoint**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo test --workspace`
Expected: everything clean — this is the last task in the phase. Then:

```bash
git add bench/Cargo.toml bench/benches/vector_search_bench.rs .gitignore crates/txn/src/dataset.rs
git commit -m "feat(bench): add recall@10/QPS vector search benchmark on a real 100K-vector embedding dataset

Tuned HNSW defaults (max_nb_connection=<N>, ef_construction=<N>,
ef_search=<N>) via this benchmark - recall@10=<X>, unfiltered QPS=<X>,
filtered QPS=<X>. See bench run output in the task report."
```

(Fill in the actual `<N>`/`<X>` values found in Step 5 — the commit message must cite real numbers, not placeholders, per `.claude/rules/vector-index.md`'s "cite the benchmark run when changing a default.")

---

## Final Step: Dispatch the mandatory `reviewer` subagent

Per `CLAUDE.md`'s "What 'done' means" — this phase is not complete until the `reviewer` subagent (Opus) has reviewed the full branch diff, the same way Phase 2's and Phase 3's whole-branch reviews each caught something no task-scoped review could see. Do not skip this.

Pay particular attention, in that review, to:
- Whether the row-id assignment (Task 1) and delta-log write (Task 4) can ever disagree about which rows got which id — they must derive from the exact same `row_id_base`/`num_rows` computation in `Transaction::commit`'s single loop iteration; verify no future edit could let them drift apart (the same class of risk Phase 3's `explain`/`scan_with_predicate` review checked for its two independent-but-must-agree call sites).
- Whether `HnswIndex::insert`'s `row_id as usize` cast and `to_matches`'s `get_origin_id() as u64` cast are truly lossless round-trips on every platform this project actually builds for — re-confirm no 32-bit CI target exists before trusting this.
- Whether the benchmark's recall@10 floor (0.8 hard-asserted, 0.9 targeted for defaults) is itself a meaningful correctness signal or could be gamed by a benchmark bug (e.g. `check_recall` comparing against the wrong ground truth, or `row_index`/`row_id` semantics accidentally conflated the way Task 2's spec fix was specifically written to prevent).
