# Effective Rust Remediation (Items 6/7/22/24/30/32) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Task 1 must run before Tasks 2, 5, and 6** — all four touch `crates/index/src/hnsw.rs`, `crates/txn/src/dataset.rs`, or `crates/txn/src/snapshot.rs`, and Tasks 2/5/6 write doctests against the newtype-based `HnswIndex::new` signature Task 1 introduces. Tasks 3, 4, 7, 8, 9 touch none of those files and have no dependency on Task 1 or each other — run them in any order, in parallel with Task 1.

**Goal:** Close six verified gaps against *Effective Rust*/Rust API-guidelines items 6, 7, 22, 24, 30, 32 in the Strata codebase: newtype `HnswIndex::new`'s params, re-export `arrow` from `strata-txn`/`strata-storage`, add doctests + an example program + a `cargo-fuzz` target, and stand up a GitHub Actions CI workflow.

**Architecture:** See `docs/superpowers/specs/2026-07-17-effective-rust-remediation-design.md` for full rationale. One task changes a public signature (Task 1, must land first among the file-overlapping tasks); the rest are purely additive (new doc comments, new re-export lines, new files) with zero behavior change to existing logic.

**Tech Stack:** Rust (edition 2024), `arrow`, `hnsw_rs`, `thiserror`, `cargo-fuzz`/`libfuzzer-sys` (new, `fuzz/`-only, not a workspace member), GitHub Actions.

## Global Constraints

- `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check` must all stay green after every task, per `.claude/CLAUDE.md`'s "what done means" checklist.
- No changes to conflict detection, snapshot isolation, commit semantics, or any correctness-critical logic in `crates/txn/` or `crates/index/` beyond the constructor signature in Task 1 — that's a signature change, not a behavior change, so no new `loom` test is required for it.
- No new dependency without justification: doctests/examples reuse the existing hand-rolled temp-dir helper pattern already in `crates/txn/src/dataset.rs`'s test module rather than adding `tempfile`.
- Model dispatch per `.claude/CLAUDE.md`: default **Sonnet 5** for every task here (no task touches architecture, security, schema, or auth). Opus 4.8 review is mandatory for every task regardless of implementer model.
- Pre-1.0 (`version = "0.1.0"`) internal workspace — Task 1's breaking constructor-signature change needs no deprecation path.

---

### Task 1: Newtype `HnswIndex::new`'s four `usize` parameters

**Files:**
- Modify: `crates/index/src/hnsw.rs`
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/txn/src/dataset.rs:346-352`
- Modify: `crates/txn/src/snapshot.rs:234`

**Interfaces:**
- Produces: four new public types in `strata_index` — `MaxConnections(pub usize)`, `MaxElements(pub usize)`, `MaxLayers(pub usize)`, `EfConstruction(pub usize)`, each `#[derive(Debug, Clone, Copy, PartialEq, Eq)]`. `HnswIndex::new`'s signature becomes `new(max_nb_connection: MaxConnections, max_elements: MaxElements, max_layer: MaxLayers, ef_construction: EfConstruction) -> Result<Self, IndexError>`. `HnswIndex::insert`, `search`, `search_filtered`, `established_dimension` are unchanged (they never took these params).
- Consumes: nothing from other tasks — this is the first task in dependency order.

- [ ] **Step 1: Add the four newtypes to `crates/index/src/hnsw.rs`**

Insert immediately before the `pub struct HnswIndex {` definition (currently line 39):

```rust
/// Maximum number of bidirectional links per node per layer (`hnsw_rs`'s
/// `max_nb_connection`) — hard-capped at 256 by the underlying library, see
/// [`HnswIndex::new`]'s doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxConnections(pub usize);

/// Expected/reserved capacity for the graph's internal allocation
/// (`hnsw_rs`'s `max_elements`) — a sizing hint, not a hard cap on how many
/// vectors can be inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxElements(pub usize);

/// Maximum number of layers in the graph's hierarchy (`hnsw_rs`'s
/// `max_layer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxLayers(pub usize);

/// Candidate-list size used while building the graph (`hnsw_rs`'s
/// `ef_construction`) — higher values trade insert time for graph quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EfConstruction(pub usize);

```

- [ ] **Step 2: Change `HnswIndex::new`'s signature and body**

Replace (currently lines 77-96):

```rust
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
            hnsw: Hnsw::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                DistL2 {},
            ),
            dimension: AtomicUsize::new(0),
        })
    }
```

with:

```rust
    pub fn new(
        max_nb_connection: MaxConnections,
        max_elements: MaxElements,
        max_layer: MaxLayers,
        ef_construction: EfConstruction,
    ) -> Result<Self, IndexError> {
        if max_nb_connection.0 > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection.0));
        }
        Ok(Self {
            hnsw: Hnsw::new(
                max_nb_connection.0,
                max_elements.0,
                max_layer.0,
                ef_construction.0,
                DistL2 {},
            ),
            dimension: AtomicUsize::new(0),
        })
    }
```

- [ ] **Step 3: Update all 11 in-file call sites in `crates/index/src/hnsw.rs`'s test module**

Two literal patterns repeat; use find-and-replace-all for each:

Replace every occurrence of:
```rust
HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
```
with:
```rust
HnswIndex::new(
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
        )
```
(6 occurrences: the `insert_then_search_finds_the_true_nearest_neighbor`, `invisible_row_is_never_returned_even_as_the_true_nearest_neighbor`, `invisibility_of_the_single_nearest_neighbor_still_returns_k_live_results_for_small_k`, `search_filtered_only_returns_ids_in_the_live_set`, `search_filtered_excludes_invisible_rows_even_for_the_single_nearest_live_id`, and `search_reports_squared_l2_distance_not_plain_l2` tests.)

Replace every occurrence of the literal `HnswIndex::new(16, 100, 16, 200)` with `HnswIndex::new(MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200))` (4 occurrences: `search_errors_on_dimension_mismatch`, `insert_errors_on_dimension_mismatch_with_previously_inserted_vectors`, `established_dimension_is_zero_before_any_insert`, `established_dimension_reflects_the_first_inserted_vectors_length`).

Replace the one distinct literal, `HnswIndex::new(257, 100, 16, 200)` (in `new_rejects_max_nb_connection_above_256`), with `HnswIndex::new(MaxConnections(257), MaxElements(100), MaxLayers(16), EfConstruction(200))`.

- [ ] **Step 4: Re-export the new types from `crates/index/src/lib.rs`**

Replace:
```rust
pub use hnsw::{HnswIndex, IndexError, VectorMatch};
```
with:
```rust
pub use hnsw::{EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers, VectorMatch};
```

- [ ] **Step 5: Update the production call site in `crates/txn/src/dataset.rs`**

Replace (currently lines 20 and 346-352):
```rust
use strata_index::{DeltaEntry, HnswIndex, read_delta_log, write_delta_log};
```
with:
```rust
use strata_index::{
    DeltaEntry, EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers,
    read_delta_log, write_delta_log,
};
```

Replace:
```rust
fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        HNSW_MAX_NB_CONNECTION,
        capacity.max(1),
        HNSW_MAX_LAYER,
        HNSW_EF_CONSTRUCTION,
    )?)
}
```
with:
```rust
fn new_hnsw_index(capacity: usize) -> Result<HnswIndex> {
    Ok(HnswIndex::new(
        MaxConnections(HNSW_MAX_NB_CONNECTION),
        MaxElements(capacity.max(1)),
        MaxLayers(HNSW_MAX_LAYER),
        EfConstruction(HNSW_EF_CONSTRUCTION),
    )?)
}
```

- [ ] **Step 6: Update the production call site in `crates/txn/src/snapshot.rs`**

Replace (currently line 12 and line 234):
```rust
use strata_index::HnswIndex;
```
with:
```rust
use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
```

Replace:
```rust
            graph: Arc::new(HnswIndex::new(16, 100, 16, 200).unwrap()),
```
with:
```rust
            graph: Arc::new(
                HnswIndex::new(
                    MaxConnections(16),
                    MaxElements(100),
                    MaxLayers(16),
                    EfConstruction(200),
                )
                .unwrap(),
            ),
```

- [ ] **Step 7: Verify the whole workspace still builds and passes**

Run: `cargo build --workspace`
Expected: succeeds, no errors.

Run: `cargo test -p strata-index -p strata-txn`
Expected: all existing tests pass unmodified (this is a signature-only change — no new test needed, the existing suite is the regression guard, matching this project's own convention for pure refactors).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 8: Commit**

```bash
git add crates/index/src/hnsw.rs crates/index/src/lib.rs crates/txn/src/dataset.rs crates/txn/src/snapshot.rs
git commit -m "refactor(index): newtype HnswIndex::new's 4 usize params

Prevents accidental transposition of max_nb_connection/max_elements/
max_layer/ef_construction — they were structurally identical usizes
with no compiler-enforced distinction. Pre-1.0 crate, clean break."
```

---

### Task 2: Doctests for `HnswIndex`'s public API and `IndexError`

**Files:**
- Modify: `crates/index/src/hnsw.rs`

**Interfaces:**
- Consumes: `MaxConnections`, `MaxElements`, `MaxLayers`, `EfConstruction` from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a doctest to `HnswIndex::new`**

Insert immediately above the existing `/// # Errors` doc comment on `new` (after Task 1's Step 2 edit):

```rust
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16),
    ///     MaxElements(100),
    ///     MaxLayers(16),
    ///     EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
```

- [ ] **Step 2: Add a doctest to `HnswIndex::insert`**

Insert immediately above `insert`'s existing `/// # Errors` doc comment:

```rust
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[1.0, 2.0, 3.0])?;
    /// assert_eq!(index.established_dimension(), 3);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
```

- [ ] **Step 3: Add a doctest to `HnswIndex::search`**

Insert immediately above `search`'s existing `/// # Errors` doc comment:

```rust
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    /// index.insert(1, &[10.0, 10.0, 10.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
```

- [ ] **Step 4: Add a doctest to `IndexError`**

Insert immediately above the existing `#[derive(Debug, thiserror::Error)]` on `IndexError` (currently line 27):

```rust
/// # Examples
///
/// ```
/// use strata_index::IndexError;
///
/// let err = IndexError::MaxConnectionTooLarge(300);
/// assert_eq!(
///     err.to_string(),
///     "max_nb_connection must be <= 256 (hnsw_rs hard limit), got 300"
/// );
/// ```
```

- [ ] **Step 5: Verify doctests compile and pass**

Run: `cargo test -p strata-index --doc`
Expected: 4 doctests pass, 0 failed (`new`, `insert`, `search`, `IndexError`).

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/hnsw.rs
git commit -m "docs(index): add doctests to HnswIndex's public API and IndexError"
```

---

### Task 3: Re-export `arrow` from `strata-txn` and `strata-storage`

**Files:**
- Modify: `crates/txn/src/lib.rs`
- Modify: `crates/storage/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `strata_txn::arrow` and `strata_storage::arrow`, both re-exporting the workspace-pinned `arrow` crate. Not consumed by any other task in this plan (this closes Item 24 for external consumers; no in-repo call site needs it).

- [ ] **Step 1: Add the re-export to `crates/txn/src/lib.rs`**

Replace:
```rust
pub mod dataset;
pub mod error;
pub mod mvp_fixtures;
pub mod snapshot;

pub use dataset::{Dataset, ROW_ID_COLUMN, Transaction};
pub use error::{Result, TxnError};
pub use snapshot::Snapshot;
```
with:
```rust
pub mod dataset;
pub mod error;
pub mod mvp_fixtures;
pub mod snapshot;

pub use arrow;
pub use dataset::{Dataset, ROW_ID_COLUMN, Transaction};
pub use error::{Result, TxnError};
pub use snapshot::Snapshot;
```

- [ ] **Step 2: Add the re-export to `crates/storage/src/lib.rs`**

Replace:
```rust
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod stats;

pub use datafile::{read_batch, sync_dir, write_batch};
pub use encoding::encode_batch;
pub use error::{Result, StorageError};
pub use manifest::{DataFileEntry, Manifest, commit_manifest, read_current};
pub use stats::{ColumnStats, Value, compute_stats};
```
with:
```rust
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod stats;

pub use arrow;
pub use datafile::{read_batch, sync_dir, write_batch};
pub use encoding::encode_batch;
pub use error::{Result, StorageError};
pub use manifest::{DataFileEntry, Manifest, commit_manifest, read_current};
pub use stats::{ColumnStats, Value, compute_stats};
```

- [ ] **Step 3: Verify**

Run: `cargo build --workspace`
Expected: succeeds — this is a purely additive public-API change, nothing else in the workspace references `strata_txn::arrow`/`strata_storage::arrow` yet, so no other file changes.

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean.

- [ ] **Step 4: Commit**

```bash
git add crates/txn/src/lib.rs crates/storage/src/lib.rs
git commit -m "feat: re-export arrow from strata-txn and strata-storage

Both crates' public APIs take/return arrow::RecordBatch directly
(Dataset::insert, write_batch, read_batch, encode_batch,
compute_stats) without re-exporting the arrow crate itself — an
external consumer would have to independently guess and pin a
compatible arrow version to construct the types these APIs expect."
```

---

### Task 4: Doctests for `TxnError` and `StorageError`

**Files:**
- Modify: `crates/txn/src/error.rs`
- Modify: `crates/storage/src/error.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a doctest to `TxnError`**

In `crates/txn/src/error.rs`, insert immediately above the existing `#[derive(Debug, Error)]` on `TxnError` (currently line 5):

```rust
/// # Examples
///
/// ```
/// use strata_txn::TxnError;
///
/// let err = TxnError::SchemaMismatch { expected: 3, actual: 2 };
/// assert_eq!(
///     err.to_string(),
///     "schema mismatch casting a data file: expected 3 columns, found 2"
/// );
/// ```
```

- [ ] **Step 2: Add a doctest to `StorageError`**

In `crates/storage/src/error.rs`, insert immediately above the existing `#[derive(Debug, Error)]` on `StorageError` (currently line 5):

```rust
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use strata_storage::StorageError;
///
/// let err = StorageError::EmptyDataFile(PathBuf::from("data/0001.arrow"));
/// assert_eq!(
///     err.to_string(),
///     "data file at data/0001.arrow contains no record batch"
/// );
/// ```
```

- [ ] **Step 3: Verify**

Run: `cargo test -p strata-txn -p strata-storage --doc`
Expected: 2 doctests pass, 0 failed.

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean.

- [ ] **Step 4: Commit**

```bash
git add crates/txn/src/error.rs crates/storage/src/error.rs
git commit -m "docs: add doctests to TxnError and StorageError"
```

---

### Task 5: Doctests for `Dataset::create`, `Dataset::open`, `Transaction::insert`, `Transaction::commit`

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: Task 1 must be committed first (this task edits `crates/txn/src/dataset.rs`, which Task 1 also edits — sequencing avoids overlapping diffs across separate subagent runs). Does not consume any type Task 1 introduced (these doctests only use `Dataset`/`Transaction`, whose signatures Task 1 didn't touch).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a doctest to `Dataset::create`**

Insert immediately above the existing `/// # Errors` doc comment on `create` (currently line 64):

```rust
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-create-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    ///
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// txn.commit()?;
    ///
    /// assert_eq!(dataset.current_version(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
```

- [ ] **Step 2: Add a doctest to `Dataset::open`**

Insert immediately above the existing `/// # Errors` doc comment on `open` (currently line 99):

```rust
    /// # Examples
    ///
    /// ```
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-open-{}", std::process::id()));
    /// Dataset::create(&dir)?; // must exist first — `open` errors on a missing dataset
    ///
    /// let reopened = Dataset::open(&dir)?;
    /// assert_eq!(reopened.current_version(), 0);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
```

- [ ] **Step 3: Add a doctest to `Transaction::insert`**

Insert immediately above `insert`'s existing doc comment (currently: `/// Buffers a batch of rows for this transaction. ...`, before line 172):

```rust
    /// # Examples
    ///
    /// Buffered rows are invisible to every reader — including this same
    /// `Dataset` — until [`Transaction::commit`] succeeds:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-insert-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// assert_eq!(dataset.current_version(), 0, "not visible until commit");
    /// txn.commit()?;
    /// assert_eq!(dataset.current_version(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
```

- [ ] **Step 4: Add a doctest to `Transaction::commit`**

Insert immediately above `commit`'s existing `/// # Errors` doc comment (currently line 189):

```rust
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-commit-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// txn.commit()?; // durable and visible to every reader from this point on
    ///
    /// assert_eq!(dataset.data_files().len(), 1);
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
```

- [ ] **Step 5: Verify**

Run: `cargo test -p strata-txn --doc`
Expected: 4 doctests pass, 0 failed (`Dataset::create`, `Dataset::open`, `Transaction::insert`, `Transaction::commit`).

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "docs(txn): add doctests to Dataset::create/open and Transaction::insert/commit"
```

---

### Task 6: Doctest for `Snapshot::vector_search`

**Files:**
- Modify: `crates/txn/src/snapshot.rs`

**Interfaces:**
- Consumes: Task 1 must be committed first (this task edits `crates/txn/src/snapshot.rs`, which Task 1 also edits).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add a doctest to `Snapshot::vector_search`**

Insert immediately above `vector_search`'s existing `/// # Errors` doc comment (currently line 173):

```rust
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::{Float32Array, Int64Array, RecordBatch};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use strata_txn::Dataset;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir()
    ///     .join(format!("strata-doctest-vector-search-{}", std::process::id()));
    /// let dataset = Dataset::create(&dir)?;
    ///
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new(
    ///         "vector",
    ///         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
    ///         false,
    ///     ),
    /// ]));
    /// let ids = Arc::new(Int64Array::from(vec![1, 2]));
    /// let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    /// let values = Arc::new(Float32Array::from(vec![0.0, 0.0, 0.0, 9.0, 9.0, 9.0]));
    /// let vectors = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    /// let batch = RecordBatch::try_new(schema, vec![ids, vectors])?;
    ///
    /// let mut txn = dataset.begin();
    /// txn.insert(batch);
    /// txn.commit()?;
    ///
    /// let results = dataset.snapshot().vector_search(&[0.0, 0.0, 0.0], 1, None)?;
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].row_id, 0); // row-id 0 is the true nearest match
    /// # std::fs::remove_dir_all(&dir).ok();
    /// # Ok(())
    /// # }
    /// ```
    ///
```

- [ ] **Step 2: Verify**

Run: `cargo test -p strata-txn --doc`
Expected: 5 doctests pass (the 4 from Task 5 plus this one), 0 failed.

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean.

- [ ] **Step 3: Commit**

```bash
git add crates/txn/src/snapshot.rs
git commit -m "docs(txn): add a doctest to Snapshot::vector_search"
```

---

### Task 7: `examples/basic_usage.rs` for `strata-txn`

**Files:**
- Create: `crates/txn/examples/basic_usage.rs`

**Interfaces:**
- Consumes: `strata_txn::Dataset` (unchanged public API — no dependency on Task 1's newtype change, since this example never constructs an `HnswIndex` directly).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the example**

Create `crates/txn/examples/basic_usage.rs`:

```rust
//! Minimal end-to-end walkthrough: create a dataset, insert rows with a
//! vector column, commit, then scan and vector-search the result.
//!
//! Run with: `cargo run --example basic_usage -p strata-txn`

use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_txn::Dataset;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir()
        .join(format!("strata-example-basic-usage-{}", std::process::id()));
    let dataset = Dataset::create(&dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let values = Arc::new(Float32Array::from(vec![
        0.0, 0.0, 0.0, // row 0
        1.0, 0.0, 0.0, // row 1
        9.0, 9.0, 9.0, // row 2
    ]));
    let vectors = Arc::new(arrow::array::FixedSizeListArray::new(item_field, 3, values, None));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors])?;

    let mut txn = dataset.begin();
    txn.insert(batch);
    txn.commit()?;

    println!("committed version: {}", dataset.current_version());

    let scanned = dataset.snapshot().scan(&schema)?;
    println!("scanned {} row(s)", scanned.num_rows());

    let nearest = dataset.snapshot().vector_search(&[0.0, 0.0, 0.0], 1, None)?;
    println!("nearest neighbor to [0,0,0]: row-id {}", nearest[0].row_id);

    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}
```

- [ ] **Step 2: Verify it runs**

Run: `cargo run --example basic_usage -p strata-txn`
Expected: exits 0, prints:
```
committed version: 1
scanned 3 row(s)
nearest neighbor to [0,0,0]: row-id 0
```

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
Expected: both clean (examples are covered by `--all-targets`).

- [ ] **Step 3: Commit**

```bash
git add crates/txn/examples/basic_usage.rs
git commit -m "docs(txn): add a basic_usage example (create -> insert -> commit -> vector_search)"
```

---

### Task 8: `cargo-fuzz` target for manifest parsing

**Files:**
- Create: `fuzz/Cargo.toml`
- Create: `fuzz/fuzz_targets/manifest_parse.rs`

**Interfaces:**
- Consumes: `strata_storage::Manifest` (already `pub`, already `#[derive(Deserialize)]` — no source changes needed in `crates/storage`).
- Produces: nothing consumed by later tasks. Not wired into CI (Task 9) — fuzzing is deliberately excluded from the PR gate per the design doc.

**Prerequisite (environment, not part of the diff):** `cargo-fuzz` is not installed in the reference dev environment used to write this plan (`cargo fuzz --version` returns "no such command"), and only the stable toolchain is installed (`rustup toolchain list` shows only `stable-x86_64-pc-windows-msvc`). `cargo fuzz build`/`run` requires a nightly toolchain (libFuzzer's sanitizer-coverage instrumentation uses unstable `-Z` compiler flags that only nightly supports) — this is inherent to `cargo-fuzz`, not something this task's code can work around.

- [ ] **Step 1: Install `cargo-fuzz` and a nightly toolchain**

Run: `cargo install cargo-fuzz`
Expected: installs the `cargo-fuzz` subcommand (needs network access; one-time).

Run: `rustup toolchain install nightly`
Expected: installs a nightly toolchain alongside the existing stable default (does not change the active/default toolchain — `rust-toolchain.toml` from Task 9 keeps the workspace itself pinned to stable 1.90; `cargo fuzz` invokes nightly internally on its own).

If either step is not possible in the current environment (no network, no permission to install toolchains), skip to Step 4 and note in the commit message that `cargo fuzz build` could not be locally verified — the target's Rust source is still reviewable as ordinary code.

- [ ] **Step 2: Write the fuzz crate manifest**

Create `fuzz/Cargo.toml`:

```toml
[package]
name = "strata-storage-fuzz"
version = "0.0.0"
publish = false
edition = "2024"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
serde_json = "1"

[dependencies.strata-storage]
path = "../crates/storage"

[[bin]]
name = "manifest_parse"
path = "fuzz_targets/manifest_parse.rs"
test = false
doc = false
bench = false

[workspace]
```

The empty `[workspace]` table is cargo-fuzz's standard convention (also used by `cargo fuzz init`) — it makes `fuzz/` its own independent Cargo workspace, so `cargo build --workspace`/`cargo test --workspace` run from the repo root never touch it, and its nightly-only dependencies never leak into the main workspace's lockfile.

- [ ] **Step 3: Write the fuzz target**

Create `fuzz/fuzz_targets/manifest_parse.rs`:

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzzes the actual on-disk manifest deserialization step
// (`strata_storage::manifest::read_current`'s internal
// `serde_json::from_slice::<Manifest>(&bytes)` call) directly against
// arbitrary bytes — this is the real untrusted-input surface: a corrupted
// disk, a downgraded binary writing an older manifest shape, or a hostile
// actor with filesystem access could all hand a reader exactly this.
fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<strata_storage::Manifest>(data);
});
```

- [ ] **Step 4: Verify (if `cargo-fuzz` was installed in Step 1)**

Run: `cargo fuzz build` (from the `fuzz/` directory, or `cargo fuzz build --fuzz-dir fuzz` from the repo root)
Expected: builds the `manifest_parse` binary successfully — proves the target compiles against the current `strata-storage` API. Actually running it for a stretch of wall-clock time (`cargo fuzz run manifest_parse`) is optional/manual, not part of this task's done-criteria.

If `cargo-fuzz`/nightly weren't installable in this environment, note that in the commit message instead of a passing build log.

- [ ] **Step 5: Commit**

```bash
git add fuzz/Cargo.toml fuzz/fuzz_targets/manifest_parse.rs
git commit -m "test(storage): add a cargo-fuzz target for manifest JSON parsing

Not wired into CI — fuzzing is open-ended/expensive and shouldn't
gate every PR. Run manually: cargo fuzz run manifest_parse."
```

---

### Task 9: GitHub Actions CI workflow + `rust-toolchain.toml`

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `rust-toolchain.toml`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by later tasks. `rust-toolchain.toml` affects every future local `cargo`/`rustup` invocation in this repo (pins to the toolchain declared, matching the existing `rust-version = "1.90"` MSRV floor in root `Cargo.toml`).

- [ ] **Step 1: Write `rust-toolchain.toml`**

Create `rust-toolchain.toml` at the repo root:

```toml
[toolchain]
channel = "1.90"
components = ["clippy", "rustfmt"]
```

- [ ] **Step 2: Write the CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@master
        with:
          toolchain: "1.90"
          components: clippy, rustfmt

      - name: Build
        run: cargo build --workspace

      - name: Test
        run: cargo test --workspace

      - name: Clippy
        run: cargo clippy --workspace --all-targets -- -D warnings

      - name: Format check
        run: cargo fmt --check

      - name: Doc
        run: cargo doc --workspace --no-deps

      - name: Install cargo-deny
        uses: taiki-e/install-action@cargo-deny

      - name: cargo-deny (bans, sources, advisories)
        run: cargo deny check bans sources advisories
```

`dtolnay/rust-toolchain@master` does **not** read `rust-toolchain.toml` — `toolchain` is a required input with no default and the action hard-fails without it, and it only adds components passed via the `components:` input. `rust-toolchain.toml` pins local `cargo`/`rustup` invocations (which do respect it automatically); the workflow above passes `toolchain: "1.90"` and `components: clippy, rustfmt` explicitly so the Action matches what `rust-toolchain.toml` declares for local use. `cargo deny check`'s `licenses` category is deliberately omitted from the `bans sources advisories` argument list: `deny.toml`'s license allow-list is empty (an untouched `cargo deny init` template) and is already a tracked finding in the separate `audit/phase-1-2-3` dependency-lane report — silently adding `licenses` here would either fail every CI run immediately or require filling in that allow-list as an undiscussed side effect of this task.

- [ ] **Step 3: Verify the toolchain pin locally**

Run: `cargo build --workspace` (after Step 1's file exists)
Expected: succeeds. If `rustup` doesn't already have Rust 1.90 installed, this triggers an automatic one-time download (rustup respects `rust-toolchain.toml` transparently) — expected, not an error.

Review the YAML by hand for syntax correctness (indentation, step ordering) since GitHub Actions' actual trigger/run behavior can only be proven by a real push/PR against GitHub, which is outside this plan's local-verification reach — note this as a residual manual-check item when reporting the task done.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml rust-toolchain.toml
git commit -m "ci: add a GitHub Actions workflow (build/test/clippy/fmt/doc/deny) + pin rust-toolchain.toml to 1.90"
```

---

## Plan Self-Review

**Spec coverage:**
- Item 6 (newtype) → Task 1. ✓
- Item 7 (builder) → design doc §2 explicitly records "no builder, newtype is sufficient" — no task needed, already traceable. ✓
- Item 22 (minimize visibility) → design doc §3 explicitly records "verified clean, no action" — no task needed, already traceable. ✓
- Item 24 (re-export deps) → Task 3 (both `strata-txn` and `strata-storage`, per the design doc's scope addition). ✓
- Item 30 (doctests + examples + fuzz) → Tasks 2, 4, 5, 6 (doctests across `strata-index`, `strata-txn`, `strata-storage`'s and `strata-txn`'s error types), Task 7 (example), Task 8 (fuzz). ✓
- Item 32 (CI) → Task 9. ✓

**Placeholder scan:** no "TBD"/"TODO"/"handle appropriately" language; every step shows literal code or an exact command with expected output. Task 8's Step 1 install commands are the one place where actual success can't be guaranteed ahead of time (network/environment-dependent) — handled explicitly with a fallback instruction rather than silently assuming success.

**Type consistency:** `MaxConnections`/`MaxElements`/`MaxLayers`/`EfConstruction` are introduced once in Task 1 Step 1 and used identically (same names, same `.0` field access) in every later reference across Tasks 1, 2, 5's imports are unaffected since Task 5 doesn't touch `HnswIndex` directly. `Dataset`/`Transaction`/`Snapshot` method signatures referenced in Tasks 5-7 match their current, unchanged declarations in `crates/txn/src/dataset.rs` and `crates/txn/src/snapshot.rs` verified by direct file read while writing this plan.
