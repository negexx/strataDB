# Phase 3 (Predicate Pushdown & File/Chunk Pruning) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-file column statistics, a general `Predicate` type, file-pruning via those stats, and an `EXPLAIN`-style introspection surface, per `docs/design/phase-3-query-refinement-spec.md`.

**Architecture:** `crates/storage` gains `Value`/`ColumnStats`/`DataFileEntry` types and a `compute_stats` function; `Manifest.data_files` changes from `Vec<String>` to `Vec<DataFileEntry>` (a breaking manifest format change, no migration — see the spec's "Alternatives considered"). `crates/query` gains `Predicate`, a general `filter()` (with `filter_eq` becoming a thin wrapper), and the pure `should_scan_file` pruning decision. `crates/txn::Dataset` gains `explain()` and `scan_with_predicate()`, both additive — existing `scan()` is untouched. `crates/cli` gets an `explain` subcommand.

**Tech Stack:** Rust, `arrow` (already a workspace dependency — `arrow::compute::kernels::aggregate::{min,max,min_string,max_string}` and `arrow::compute::kernels::cmp::{eq,lt,lt_eq,gt,gt_eq}` are new APIs for this codebase, both verified against the installed arrow-58.3.0/arrow-arith-58.3.0/arrow-ord-58.3.0 source below — do not re-derive, trust these signatures).

## Global Constraints

- Edition 2024, workspace lints apply (`clippy::pedantic` + `clippy::all` at warn, `-D warnings`) — every public `Result`-returning function needs a `# Errors` doc section.
- `unwrap()`/`expect()` are `clippy::warn` — fine only in `#[cfg(test)]` modules (`#[allow(clippy::unwrap_used, clippy::expect_used)]` on `mod tests`), never in library code. Where a downcast could theoretically fail but the caller controls both sides (e.g. a column's `data_type()` was just matched against the exact type being downcast to), prefer `and_then`/pattern matching that treats a hypothetical mismatch as "no stats available" over `.expect(...)` — see Task 1.
- Git Flow branching: work happens on `feature/phase-3-query-refinement`, branched from `develop` (already created). Every task's "Checkpoint" step means: run the verification commands, confirm green, then `git add`/`git commit` with a Conventional Commits message. Never commit directly to `develop` or `main`.
- Verify any Arrow API you're not 100% certain of against the installed source before writing code that depends on it (`cargo metadata --format-version 1 | python -c "import json,sys; print([p['manifest_path'] for p in json.load(sys.stdin)['packages'] if p['name']=='<crate>'])"` gives the exact path under `~/.cargo/registry/src/`). This codebase has been burned twice already (Phase 1's `hnsw_rs`, and a near-miss on Phase 2's `criterion` version) by trusting something other than the actual installed source.
- `Manifest` currently derives `Eq` — this task's `Value` enum contains `f64` (`Value::Float64`), and `f64` does not implement `Eq`. `Manifest`'s derive must drop `Eq` (keep `PartialEq`) once `Value` is reachable from it. Nothing in the existing codebase relies on `Manifest: Eq` specifically (only `PartialEq`, via `assert_eq!`) — confirmed by grep before this plan was written.

---

### Task 1: `crates/storage` — `Value`, `ColumnStats`, `DataFileEntry`, `compute_stats`, manifest schema change

**Files:**
- Create: `crates/storage/src/stats.rs`
- Modify: `crates/storage/src/manifest.rs` (schema change + existing test literals)
- Modify: `crates/storage/src/lib.rs` (new exports)

**Interfaces:**
- Produces: `pub enum Value { Int64(i64), Float64(f64), Utf8(String) }`, `pub struct ColumnStats { pub min: Value, pub max: Value }`, `pub fn compute_stats(batch: &RecordBatch) -> HashMap<String, ColumnStats>`, `pub struct DataFileEntry { pub name: String, pub stats: HashMap<String, ColumnStats> }`. `Manifest.data_files` is now `Vec<DataFileEntry>`. Task 2 consumes all of this directly.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/storage/src/stats.rs
//! Per-column file statistics for pruning, per
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §1.

use std::collections::HashMap;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::kernels::aggregate::{max, max_string, min, min_string};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use serde::{Deserialize, Serialize};

/// A scalar value, shared between file statistics (this module) and
/// `strata_query::Predicate` — both need the same "which types are
/// orderable and prunable" vocabulary.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Value,
    pub max: Value,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType as DT, Field, Schema};

    use super::*;

    fn batch_with_id_and_name() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DT::Int64, false),
            Field::new("name", DT::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![30, 10, 20])),
                Arc::new(StringArray::from(vec!["banana", "apple", "cherry"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn computes_min_max_for_int64_and_utf8_columns() {
        let stats = compute_stats(&batch_with_id_and_name());

        let id_stats = stats.get("id").unwrap();
        assert_eq!(id_stats.min, Value::Int64(10));
        assert_eq!(id_stats.max, Value::Int64(30));

        let name_stats = stats.get("name").unwrap();
        assert_eq!(name_stats.min, Value::Utf8("apple".to_string()));
        assert_eq!(name_stats.max, Value::Utf8("cherry".to_string()));
    }

    #[test]
    fn non_orderable_column_gets_no_stats_entry() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DT::FixedSizeList(Arc::new(Field::new("item", DT::Float32, false)), 3),
            false,
        )]));
        let values = Arc::new(arrow::array::Float32Array::from(vec![1.0, 2.0, 3.0]));
        let item_field = Arc::new(Field::new("item", DT::Float32, false));
        let vectors =
            arrow::array::FixedSizeListArray::new(item_field, 3, values, None);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(vectors)]).unwrap();

        let stats = compute_stats(&batch);
        assert!(
            !stats.contains_key("vector"),
            "non-orderable column must get no stats entry, not a wrong one"
        );
    }

    #[test]
    fn all_null_column_gets_no_stats_entry() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DT::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None, None, None]))],
        )
        .unwrap();

        let stats = compute_stats(&batch);
        assert!(
            !stats.contains_key("id"),
            "all-null column must get no stats entry (no meaningful min/max)"
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-storage stats`
Expected: FAIL to compile — `compute_stats` is not defined.

- [ ] **Step 3: Write the implementation**

Add above the `#[cfg(test)]` block in `crates/storage/src/stats.rs`:

```rust
/// Computes per-column min/max for every orderable column in `batch`.
/// Called on the *original, pre-encoding* batch at commit time — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §1. Columns with
/// no non-null values, or a non-orderable type (e.g. a vector column), get
/// no entry — never a wrong or placeholder one.
#[must_use]
pub fn compute_stats(batch: &RecordBatch) -> HashMap<String, ColumnStats> {
    let mut stats = HashMap::new();
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let entry = match field.data_type() {
            DataType::Int64 => column.as_any().downcast_ref::<Int64Array>().and_then(|arr| {
                match (min::<Int64Type>(arr), max::<Int64Type>(arr)) {
                    (Some(min_v), Some(max_v)) => {
                        Some(ColumnStats { min: Value::Int64(min_v), max: Value::Int64(max_v) })
                    }
                    _ => None,
                }
            }),
            DataType::Float64 => {
                column.as_any().downcast_ref::<Float64Array>().and_then(|arr| {
                    match (min::<Float64Type>(arr), max::<Float64Type>(arr)) {
                        (Some(min_v), Some(max_v)) => Some(ColumnStats {
                            min: Value::Float64(min_v),
                            max: Value::Float64(max_v),
                        }),
                        _ => None,
                    }
                })
            }
            DataType::Utf8 => column.as_any().downcast_ref::<StringArray>().and_then(|arr| {
                match (min_string(arr), max_string(arr)) {
                    (Some(min_v), Some(max_v)) => Some(ColumnStats {
                        min: Value::Utf8(min_v.to_string()),
                        max: Value::Utf8(max_v.to_string()),
                    }),
                    _ => None,
                }
            }),
            _ => None, // not orderable (e.g. a vector column) - no stats
        };
        if let Some(entry) = entry {
            stats.insert(field.name().clone(), entry);
        }
    }
    stats
}
```

`min`/`max`/`min_string`/`max_string` and `Int64Type`/`Float64Type` are verified against the installed source:
- `arrow-arith-58.3.0/src/aggregate.rs:928` — `pub fn min<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>` (and `max` at line 943)
- `arrow-arith-58.3.0/src/aggregate.rs:531,521` — `pub fn min_string<T: OffsetSizeTrait>(array: &GenericStringArray<T>) -> Option<&str>` (and `max_string`)
- All four re-exported through `arrow::compute::kernels::aggregate` via `arrow-58.3.0/src/compute/kernels.rs`'s `pub use arrow_arith::{aggregate, ...}`.

- [ ] **Step 4: Change `Manifest`'s schema in `crates/storage/src/manifest.rs`**

Replace the top of the file (imports and struct definitions) — old:

```rust
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Data file names (relative to the dataset's `data/` directory),
    /// accumulated across every committed version so far.
    pub data_files: Vec<String>,
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
```

New:

```rust
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::stats::ColumnStats;

/// One committed data file's name and the per-column statistics computed
/// for it at commit time — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
}

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
```

(Note: `Eq` is dropped from the derive list — `Value::Float64(f64)` doesn't implement `Eq`, so it can no longer be derived transitively through `ColumnStats` -> `DataFileEntry` -> `Manifest`. `PartialEq` is kept and is all any existing code needs, per this plan's Global Constraints.)

- [ ] **Step 5: Update `manifest.rs`'s existing tests to the new shape**

Each of the 4 existing tests builds a `Manifest` literal with `data_files: vec!["a.arrow".to_string()]` or similar — these no longer compile. Replace every such literal, e.g.:

```rust
// Old:
let m0 = Manifest {
    version: 0,
    data_files: vec!["a.arrow".to_string()],
};
// New:
let m0 = Manifest {
    version: 0,
    data_files: vec![DataFileEntry {
        name: "a.arrow".to_string(),
        stats: HashMap::new(),
    }],
};
```

Apply the same transform to all `data_files: vec![...]` literals in `commit_then_read_current_round_trips` (which has two — `m0` and `m1`, the latter with two file names) and `leftover_tmp_file_is_never_picked_up_as_current`'s `m0`. `read_current_is_none_for_fresh_dataset` and `genuinely_corrupt_manifest_errors_instead_of_panicking` build no `Manifest` literal directly and need no changes.

- [ ] **Step 6: Wire up `lib.rs`**

`crates/storage/src/lib.rs` — add `stats` module and re-export, and re-export the new manifest type:

```rust
pub mod stats;
// (alongside the existing pub mod datafile; pub mod encoding; pub mod error; pub mod manifest;)

pub use stats::{ColumnStats, Value, compute_stats};
// (alongside the existing pub use lines)
pub use manifest::{DataFileEntry, Manifest, commit_manifest, read_current};
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p strata-storage`
Expected: all `stats::tests::*` pass (3 new), all `manifest::tests::*` pass (4, updated), all `datafile::tests::*`/`encoding::tests::*` unaffected and still passing.

- [ ] **Step 8: Checkpoint**

Run: `cargo clippy -p strata-storage --all-targets -- -D warnings && cargo fmt -p strata-storage --check`
Expected: both clean. Then `git add crates/storage/ && git commit -m "feat(storage): add per-column file statistics for pruning"`.

---

### Task 2: Wire `compute_stats` into `Transaction::commit`, update `Dataset::data_files()`

**Files:**
- Modify: `crates/txn/src/dataset.rs`

**Interfaces:**
- Consumes: `strata_storage::{DataFileEntry, compute_stats}` (Task 1)
- Produces: `Dataset::data_files(&self) -> &[DataFileEntry]` (changed return type — Task 4/5 and the two existing test call sites below depend on this)

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/dataset.rs`'s existing `#[cfg(test)] mod tests` block:

```rust
#[test]
fn commit_computes_and_stores_column_stats() {
    let dir = temp_dir("commit-stats");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir).unwrap();

    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![30, 10, 20]))])
            .unwrap();
    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();

    let entry = &ds.data_files()[0];
    let id_stats = entry.stats.get("id").unwrap();
    assert_eq!(id_stats.min, strata_storage::Value::Int64(10));
    assert_eq!(id_stats.max, strata_storage::Value::Int64(30));

    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-txn commit_computes_and_stores_column_stats`
Expected: FAIL to compile — `data_files()[0]` is currently `&String`, which has no `.stats` field, and `entry.stats` doesn't exist yet.

- [ ] **Step 3: Update imports and `data_files()`'s return type**

In `crates/txn/src/dataset.rs`, change the import line:

```rust
// Old:
use strata_storage::{Manifest, commit_manifest, read_batch, read_current, write_batch};
// New:
use strata_storage::{
    DataFileEntry, Manifest, commit_manifest, compute_stats, read_batch, read_current,
    write_batch,
};
```

Change `data_files()`:

```rust
// Old:
/// Data file names (relative to `data_dir()`) belonging to the current
/// version. Exposed for tests that need to inspect the raw on-disk
/// representation directly.
#[must_use]
pub fn data_files(&self) -> &[String] {
    &self.manifest.data_files
}
// New:
/// Data file entries (name + per-column stats) belonging to the current
/// version. Exposed for tests that need to inspect the raw on-disk
/// representation directly.
#[must_use]
pub fn data_files(&self) -> &[DataFileEntry] {
    &self.manifest.data_files
}
```

- [ ] **Step 4: Update `scan()`'s iteration over `data_files`**

```rust
// Old (inside scan()):
let batches = self
    .manifest
    .data_files
    .iter()
    .map(|name| {
        let batch = read_batch(&data_dir.join(name))?;
        cast_batch_to_schema(&batch, schema)
    })
    .collect::<std::result::Result<Vec<_>, _>>()?;
// New:
let batches = self
    .manifest
    .data_files
    .iter()
    .map(|entry| {
        let batch = read_batch(&data_dir.join(&entry.name))?;
        cast_batch_to_schema(&batch, schema)
    })
    .collect::<std::result::Result<Vec<_>, _>>()?;
```

- [ ] **Step 5: Compute stats in `Transaction::commit` and store them in the new `DataFileEntry`**

```rust
// Old (inside commit()):
for (i, batch) in self.pending.iter().enumerate() {
    let encoded = strata_storage::encode_batch(batch)?;
    let file_name = format!("{new_version:020}-{i}.arrow");
    write_batch(&data_dir.join(&file_name), &encoded)?;
    manifest.data_files.push(file_name);
}
// New:
for (i, batch) in self.pending.iter().enumerate() {
    // Stats computed on the original, pre-encoding batch — see
    // .claude/docs/design/phase-3-query-refinement-spec.md §1 for why
    // (logical values, no dictionary-decode step needed later).
    let stats = compute_stats(batch);
    let encoded = strata_storage::encode_batch(batch)?;
    let file_name = format!("{new_version:020}-{i}.arrow");
    write_batch(&data_dir.join(&file_name), &encoded)?;
    manifest.data_files.push(DataFileEntry { name: file_name, stats });
}
```

- [ ] **Step 6: Fix the two existing test call sites broken by `data_files()`'s new return type**

`scan_succeeds_on_a_dictionary_encoded_low_cardinality_column` (around line 249):

```rust
// Old:
let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0])).unwrap();
// New:
let on_disk = read_batch(&ds.data_dir().join(&ds.data_files()[0].name)).unwrap();
```

`low_cardinality_column_is_dictionary_encoded_on_commit` (around line 334):

```rust
// Old:
let data_dir = ds.data_dir();
let file_name = &ds.data_files()[0];
let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
// New:
let data_dir = ds.data_dir();
let file_name = &ds.data_files()[0].name;
let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
```

- [ ] **Step 7: Run the full `strata-txn` and `strata-cli` suites**

Run: `cargo test -p strata-txn -p strata-cli`
Expected: all passing, including the new test and both fixed call sites, plus the crash-recovery and MVP-checklist integration tests (which don't touch `data_files()` directly and should be unaffected).

- [ ] **Step 8: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean (this touches a type other crates consume, so check the whole workspace, not just `strata-txn`). Then `git add crates/txn/ && git commit -m "feat(txn): compute and store per-file column stats on commit"`.

---

### Task 3: `crates/query` — `Predicate`, general `filter`, `should_scan_file`

**Files:**
- Create: `crates/query/src/predicate.rs`
- Modify: `crates/query/src/lib.rs` (new exports, `filter_eq` becomes a thin wrapper)
- Modify: `crates/query/Cargo.toml` (add `strata-storage` dependency)

**Interfaces:**
- Consumes: `strata_storage::{ColumnStats, Value}` (Task 1)
- Produces: `pub enum Predicate { Eq(String, Value), Lt(String, Value), LtEq(String, Value), Gt(String, Value), GtEq(String, Value) }`, `pub fn filter(batch: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ArrowError>`, `pub fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool` — Task 4 consumes both `filter` and `should_scan_file` directly.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/query/src/predicate.rs
//! `Predicate` — the shared vocabulary for row-level filtering (`filter`)
//! and file-level pruning (`should_scan_file`). See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §2.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq};
use arrow::error::ArrowError;
use strata_storage::{ColumnStats, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(String, Value),
    Lt(String, Value),
    LtEq(String, Value),
    Gt(String, Value),
    GtEq(String, Value),
}

impl Predicate {
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Predicate::Eq(c, _)
            | Predicate::Lt(c, _)
            | Predicate::LtEq(c, _)
            | Predicate::Gt(c, _)
            | Predicate::GtEq(c, _) => c,
        }
    }

    #[must_use]
    pub fn value(&self) -> &Value {
        match self {
            Predicate::Eq(_, v)
            | Predicate::Lt(_, v)
            | Predicate::LtEq(_, v)
            | Predicate::Gt(_, v)
            | Predicate::GtEq(_, v) => v,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_eq_on_int64_column() {
        let result =
            filter(&sample_batch(), &Predicate::Eq("id".to_string(), Value::Int64(20))).unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 20);
    }

    #[test]
    fn filter_lt_on_int64_column() {
        let result =
            filter(&sample_batch(), &Predicate::Lt("id".to_string(), Value::Int64(25))).unwrap();
        assert_eq!(result.num_rows(), 2); // 10, 20
    }

    #[test]
    fn filter_gt_eq_on_int64_column() {
        let result = filter(&sample_batch(), &Predicate::GtEq("id".to_string(), Value::Int64(20)))
            .unwrap();
        assert_eq!(result.num_rows(), 2); // 20, 30
    }

    #[test]
    fn filter_eq_on_utf8_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Eq("name".to_string(), Value::Utf8("b".to_string())),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn should_scan_file_prunes_when_range_cannot_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats { min: Value::Int64(100), max: Value::Int64(200) },
        );
        // Eq(id, 50) can't match a file whose id range is [100, 200].
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(!should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_scans_when_range_could_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats { min: Value::Int64(100), max: Value::Int64(200) },
        );
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(150));
        assert!(should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_fails_open_when_column_has_no_stats() {
        let stats: HashMap<String, ColumnStats> = HashMap::new();
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(
            should_scan_file(&stats, &predicate),
            "a column with no stats must never be pruned - always scan"
        );
    }

    #[test]
    fn should_scan_file_fails_open_on_range_predicates() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats { min: Value::Int64(100), max: Value::Int64(200) },
        );
        // Lt(id, 100): no value in [100, 200] is < 100 -> should prune.
        assert!(!should_scan_file(&stats, &Predicate::Lt("id".to_string(), Value::Int64(100))));
        // Gt(id, 200): no value in [100, 200] is > 200 -> should prune.
        assert!(!should_scan_file(&stats, &Predicate::Gt("id".to_string(), Value::Int64(200))));
        // GtEq(id, 200): 200 itself is in range -> must scan.
        assert!(should_scan_file(&stats, &Predicate::GtEq("id".to_string(), Value::Int64(200))));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-query predicate`
Expected: FAIL to compile — `filter`/`should_scan_file` not defined.

- [ ] **Step 3: Add `strata-storage` as a dependency**

`crates/query/Cargo.toml` — add:

```toml
[dependencies]
arrow.workspace = true
strata-storage = { path = "../storage" }
```

- [ ] **Step 4: Write `filter` and `should_scan_file`**

Add above the `#[cfg(test)]` block in `crates/query/src/predicate.rs`:

```rust
/// Filters `batch` to rows matching `predicate`.
///
/// # Errors
///
/// Returns an [`ArrowError`] if `predicate`'s column doesn't exist, or if
/// its value's type doesn't match the column's actual Arrow type (the
/// underlying comparison kernel enforces this).
pub fn filter(batch: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ArrowError> {
    let idx = batch.schema_ref().index_of(predicate.column())?;
    let array = batch.column(idx);
    let mask = compare(array, predicate)?;
    filter_record_batch(batch, &mask)
}

fn compare(array: &ArrayRef, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    let cmp_fn: fn(&dyn arrow::array::Datum, &dyn arrow::array::Datum) -> Result<BooleanArray, ArrowError> =
        match predicate {
            Predicate::Eq(..) => eq,
            Predicate::Lt(..) => lt,
            Predicate::LtEq(..) => lt_eq,
            Predicate::Gt(..) => gt,
            Predicate::GtEq(..) => gt_eq,
        };
    match predicate.value() {
        Value::Int64(v) => {
            let scalar = Int64Array::new_scalar(*v);
            cmp_fn(&Arc::clone(array), &scalar)
        }
        Value::Float64(v) => {
            let scalar = Float64Array::new_scalar(*v);
            cmp_fn(&Arc::clone(array), &scalar)
        }
        Value::Utf8(v) => {
            let scalar = StringArray::new_scalar(v.as_str());
            cmp_fn(&Arc::clone(array), &scalar)
        }
    }
}

/// Decides whether a file whose column stats are `stats` could possibly
/// contain a row matching `predicate`. Fails open (returns `true`)
/// whenever it can't prove otherwise — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §2. Pure
/// function, zero I/O.
#[must_use]
pub fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool {
    let Some(col_stats) = stats.get(predicate.column()) else {
        return true; // no stats for this column - fail open, must scan
    };
    let value = predicate.value();
    // A mismatched Value variant (e.g. a Utf8 predicate value against an
    // Int64 column's stats) can't be proven to miss - fail open rather
    // than trust derived PartialOrd's cross-variant ordering, which
    // compares by declaration order, not value semantics.
    if std::mem::discriminant(value) != std::mem::discriminant(&col_stats.min) {
        return true;
    }
    match predicate {
        Predicate::Eq(..) => *value >= col_stats.min && *value <= col_stats.max,
        Predicate::Lt(..) => *value > col_stats.min,
        Predicate::LtEq(..) => *value >= col_stats.min,
        Predicate::Gt(..) => *value < col_stats.max,
        Predicate::GtEq(..) => *value <= col_stats.max,
    }
}
```

`eq`/`lt`/`lt_eq`/`gt`/`gt_eq` and `PrimitiveArray::new_scalar` are verified against the installed source:
- `arrow-ord-58.3.0/src/cmp.rs:79,113,130,147,164` — all five: `pub fn <name>(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError>`
- `arrow-array-58.3.0/src/array/primitive_array.rs:676` — `pub fn new_scalar(value: T::Native) -> Scalar<Self>`
- `StringArray::new_scalar` was already in use in this codebase before this task (`crates/query/src/lib.rs`'s existing `filter_eq`).

- [ ] **Step 5: Wire up `lib.rs` and simplify `filter_eq` to a wrapper**

`crates/query/src/lib.rs` — replace the whole file's `filter_eq` implementation and imports:

```rust
//! Expression/filter API. See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` for `Predicate`,
//! the general `filter`, and file-pruning via `should_scan_file`.

use arrow::array::RecordBatch;
use arrow::error::ArrowError;

pub mod group_by;
pub mod predicate;
pub use group_by::{AggFunc, group_by};
pub use predicate::{Predicate, filter, should_scan_file};

/// Returns the rows of `batch` where `column` equals `value`. A thin
/// convenience wrapper over [`filter`] with [`Predicate::Eq`] — kept for
/// existing callers (the CLI's `filter` subcommand, the Phase 1 MVP
/// checklist test); prefer `filter` directly for new code.
///
/// # Errors
///
/// Returns an [`ArrowError`] if `column` doesn't exist or isn't a UTF-8
/// string column.
pub fn filter_eq(batch: &RecordBatch, column: &str, value: &str) -> Result<RecordBatch, ArrowError> {
    filter(
        batch,
        &Predicate::Eq(column.to_string(), strata_storage::Value::Utf8(value.to_string())),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray as StrArr};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn filter_eq_keeps_only_matching_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StrArr::from(vec!["a", "b", "a"])),
            ],
        )
        .unwrap();

        let filtered = filter_eq(&batch, "name", "a").unwrap();
        assert_eq!(filtered.num_rows(), 2);
        let ids = filtered.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 3]);
    }
}
```

(This keeps the existing `filter_eq_keeps_only_matching_rows` test byte-for-byte as a regression guard — it must still pass unchanged, proving the wrapper is behaviorally identical to the old direct implementation.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p strata-query`
Expected: all `predicate::tests::*` pass (9 new: 4 filter variants + 5 should_scan_file cases — note `should_scan_file_fails_open_on_range_predicates` contains 3 assertions in one test), the existing `filter_eq_keeps_only_matching_rows` still passes unchanged, and all `group_by::tests::*` (8, from Phase 2) are unaffected.

- [ ] **Step 7: Checkpoint**

Run: `cargo clippy -p strata-query --all-targets -- -D warnings && cargo fmt -p strata-query --check`
Expected: clean — fix anything flagged (expect at least one `missing_errors_doc` or similar on first pass, matching every prior task's pattern in this codebase). Then `git add crates/query/ && git commit -m "feat(query): add Predicate, general filter, and should_scan_file pruning"`.

---

### Task 4: `Dataset::explain` and `Dataset::scan_with_predicate` (`crates/txn`)

**Files:**
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/Cargo.toml` (promote `strata-query` from dev-dependency to a real dependency)

**Interfaces:**
- Consumes: `strata_query::{Predicate, filter, should_scan_file}` (Task 3)
- Produces: `pub struct ExplainResult { pub total_files: usize, pub scanned: Vec<String>, pub skipped: Vec<String> }`, `Dataset::explain(&self, predicate: &Predicate) -> ExplainResult`, `Dataset::scan_with_predicate(&self, schema: &SchemaRef, predicate: &Predicate) -> Result<RecordBatch>` — Task 5's CLI subcommand and integration test consume both.

- [ ] **Step 1: Write the failing tests**

Add to `crates/txn/src/dataset.rs`'s existing `#[cfg(test)] mod tests` block:

```rust
#[test]
fn explain_reports_skipped_files_by_range() {
    use strata_query::Predicate;
    use strata_storage::Value;

    let dir = temp_dir("explain-skip");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir).unwrap();

    // Two commits, disjoint id ranges -> two files with non-overlapping stats.
    let low = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .unwrap();
    let mut txn = ds.begin();
    txn.insert(low);
    let ds = txn.commit().unwrap();

    let high =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![100, 101, 102]))])
            .unwrap();
    let mut txn = ds.begin();
    txn.insert(high);
    let ds = txn.commit().unwrap();

    let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
    let result = ds.explain(&predicate);

    assert_eq!(result.total_files, 2);
    assert_eq!(result.scanned.len(), 1, "only the [1,3] file could match id=2");
    assert_eq!(result.skipped.len(), 1, "the [100,102] file must be skipped");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_with_predicate_returns_only_matching_rows_from_unskipped_files() {
    use strata_query::Predicate;
    use strata_storage::Value;

    let dir = temp_dir("scan-with-predicate");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir).unwrap();

    let low = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .unwrap();
    let mut txn = ds.begin();
    txn.insert(low);
    let ds = txn.commit().unwrap();

    let high =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![100, 101, 102]))])
            .unwrap();
    let mut txn = ds.begin();
    txn.insert(high);
    let ds = txn.commit().unwrap();

    let predicate = Predicate::Eq("id".to_string(), Value::Int64(2));
    let result = ds.scan_with_predicate(&schema, &predicate).unwrap();

    assert_eq!(result.num_rows(), 1);
    let ids = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 2);
    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-txn explain_reports_skipped_files_by_range scan_with_predicate_returns_only_matching_rows_from_unskipped_files`
Expected: FAIL to compile — `Dataset::explain`/`scan_with_predicate` not defined, and `strata_query` isn't importable from `crates/txn`'s test module (still a dev-dependency only at this point).

- [ ] **Step 3: Promote `strata-query` to a real dependency**

`crates/txn/Cargo.toml` — move `strata-query` out of `[dev-dependencies]` into `[dependencies]`:

```toml
[dependencies]
arrow.workspace = true
thiserror.workspace = true
strata-storage = { path = "../storage" }
strata-query = { path = "../query" }

[dev-dependencies]
strata-index = { path = "../index" }
```

- [ ] **Step 4: Write `ExplainResult`, `explain`, and `scan_with_predicate`**

In `crates/txn/src/dataset.rs`, add the import:

```rust
use strata_query::{Predicate, filter, should_scan_file};
```

Add `ExplainResult` above `impl Dataset` (after the `Dataset` struct definition), and the two methods inside `impl Dataset`, after `scan`:

```rust
/// The outcome of [`Dataset::explain`] — which files a predicate would
/// touch, without actually reading any of their bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainResult {
    pub total_files: usize,
    pub scanned: Vec<String>,
    pub skipped: Vec<String>,
}
```

```rust
    /// Reports which committed files `predicate` would require scanning,
    /// without opening any file body — pure introspection over stats
    /// already loaded in the manifest. See
    /// `.claude/docs/design/phase-3-query-refinement-spec.md` §3.
    #[must_use]
    pub fn explain(&self, predicate: &Predicate) -> ExplainResult {
        let mut scanned = Vec::new();
        let mut skipped = Vec::new();
        for entry in &self.manifest.data_files {
            if should_scan_file(&entry.stats, predicate) {
                scanned.push(entry.name.clone());
            } else {
                skipped.push(entry.name.clone());
            }
        }
        ExplainResult { total_files: self.manifest.data_files.len(), scanned, skipped }
    }

    /// Like [`Dataset::scan`], but skips any file `predicate` provably
    /// can't match (per [`Dataset::explain`]'s decision) and row-filters
    /// the rest. This is the real performance path; `explain` is its
    /// introspection twin — both call the exact same
    /// `strata_query::should_scan_file`, so they can never disagree about
    /// what would be skipped.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Dataset::scan`],
    /// plus if `predicate`'s column doesn't exist or its value's type
    /// doesn't match the column's Arrow type.
    pub fn scan_with_predicate(
        &self,
        schema: &SchemaRef,
        predicate: &Predicate,
    ) -> Result<RecordBatch> {
        let data_dir = self.data_dir();
        let batches = self
            .manifest
            .data_files
            .iter()
            .filter(|entry| should_scan_file(&entry.stats, predicate))
            .map(|entry| {
                let batch = read_batch(&data_dir.join(&entry.name))?;
                cast_batch_to_schema(&batch, schema)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let scanned = concat_batches(schema, &batches)?;
        Ok(filter(&scanned, predicate)?)
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p strata-txn explain_reports_skipped_files_by_range scan_with_predicate_returns_only_matching_rows_from_unskipped_files`
Expected: both PASS.

- [ ] **Step 6: Run the full `strata-txn` and `strata-cli` suites**

Run: `cargo test -p strata-txn -p strata-cli`
Expected: all passing — nothing from Phase 1/2 should be affected, since `scan()` and `commit()`'s existing behavior for callers that never touch `explain`/`scan_with_predicate` is unchanged.

- [ ] **Step 7: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Then `git add crates/txn/ && git commit -m "feat(txn): add Dataset::explain and scan_with_predicate"`.

---

### Task 5: `strata explain` CLI subcommand + the exit-criterion integration test

**Files:**
- Modify: `crates/cli/src/main.rs`
- Create: `crates/txn/tests/phase_3_pruning.rs`

**Interfaces:**
- Consumes: `Dataset::explain`, `Dataset::scan_with_predicate`, `strata_query::Predicate`, `strata_storage::Value` (Task 4)

- [ ] **Step 1: Add the `explain` subcommand to the CLI**

In `crates/cli/src/main.rs`, add a new match arm inside `run`'s `match cmd.as_str() { ... }`, alongside the existing `"inspect"` arm:

```rust
        "explain" => {
            let column = args.get(3).ok_or("missing <column>")?;
            let op = args.get(4).ok_or("missing <op: eq|lt|lteq|gt|gteq>")?;
            let value: i64 = args.get(5).ok_or("missing <value>")?.parse()?;
            let predicate = match op.as_str() {
                "eq" => strata_query::Predicate::Eq(column.clone(), strata_storage::Value::Int64(value)),
                "lt" => strata_query::Predicate::Lt(column.clone(), strata_storage::Value::Int64(value)),
                "lteq" => {
                    strata_query::Predicate::LtEq(column.clone(), strata_storage::Value::Int64(value))
                }
                "gt" => strata_query::Predicate::Gt(column.clone(), strata_storage::Value::Int64(value)),
                "gteq" => {
                    strata_query::Predicate::GtEq(column.clone(), strata_storage::Value::Int64(value))
                }
                other => return Err(format!("unknown op: {other} (expected eq|lt|lteq|gt|gteq)").into()),
            };
            let ds = strata_txn::Dataset::open(dir)?;
            let result = ds.explain(&predicate);
            println!(
                "total_files={} scanned={} skipped={}",
                result.total_files,
                result.scanned.len(),
                result.skipped.len()
            );
            for name in &result.scanned {
                println!("  scan:  {name}");
            }
            for name in &result.skipped {
                println!("  skip:  {name}");
            }
        }
```

(Placed as its own arm; exact position among the other arms doesn't matter since `match` doesn't require ordering. Also update the usage line at the top of `run`'s `let Some(cmd) = args.get(1) else { eprintln!("usage: ..."); ... }` block to add `explain` to the listed subcommands — the exact string, find the existing `eprintln!("usage: strata <create|insert|scan|filter|search|inspect|crash-loop> ...")` and add `explain` to that list.)

- [ ] **Step 2: Manually verify the CLI subcommand works end-to-end**

Run (adjust the temp path for your platform, or reuse the pattern from earlier manual CLI smoke tests in this project's history):

```bash
cargo build -p strata-cli --quiet
TARGET=./target/debug/strata
DIR=/tmp/strata-explain-smoke-$$
$TARGET create "$DIR"
$TARGET insert "$DIR" 1 alice 1.0 2.0 3.0
$TARGET insert "$DIR" 100 bob 4.0 5.0 6.0
$TARGET explain "$DIR" id eq 1
```

Expected output shows `total_files=2`, one file in `scan:`, and (if the two single-row commits' id ranges are disjoint enough — they are, `[1,1]` vs `[100,100]`) one file in `skip:`.

- [ ] **Step 3: Write the exit-criterion integration test**

```rust
// crates/txn/tests/phase_3_pruning.rs
//! Phase 3's actual exit criterion: "EXPLAIN proves a filtered query skips
//! untouched files." See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §3's testing
//! section.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(ids: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

#[test]
fn explain_skips_files_whose_stats_cannot_match_and_scans_only_the_rest() {
    let dir = std::env::temp_dir().join(format!("strata-phase3-explain-{}", std::process::id()));
    let ds = Dataset::create(&dir).unwrap();

    // Three commits, three disjoint id ranges -> three files.
    let mut txn = ds.begin();
    txn.insert(batch(vec![1, 2, 3]));
    let ds = txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![50, 51, 52]));
    let ds = txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![100, 101, 102]));
    let ds = txn.commit().unwrap();

    // A predicate that can only match the middle file's range.
    let predicate = Predicate::Eq("id".to_string(), Value::Int64(51));
    let result = ds.explain(&predicate);

    assert_eq!(result.total_files, 3);
    assert_eq!(result.scanned.len(), 1, "only the [50,52] file could contain id=51");
    assert_eq!(result.skipped.len(), 2, "the [1,3] and [100,102] files must both be skipped");

    // scan_with_predicate must return exactly the one matching row, proving
    // the skip decision and the actual filtered result agree.
    let filtered = ds.scan_with_predicate(&schema(), &predicate).unwrap();
    assert_eq!(filtered.num_rows(), 1);
    let ids = filtered.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 51);

    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 4: Run the new integration test**

Run: `cargo test -p strata-txn --test phase_3_pruning`
Expected: PASS.

- [ ] **Step 5: Run the full workspace verification**

Run: `cargo check --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo test --workspace`
Expected: everything clean, exactly as at the end of Phase 2's whole-branch review fix, plus every new test from Tasks 1-5.

- [ ] **Step 6: Checkpoint**

`git add crates/cli/ crates/txn/tests/phase_3_pruning.rs && git commit -m "feat(cli): add explain subcommand; test the pruning exit criterion end-to-end"`.

---

## Final Step: Dispatch the mandatory `reviewer` subagent

Per `CLAUDE.md`'s "What 'done' means" — this phase is not complete until the `reviewer` subagent (`.claude/agents/reviewer.md`, Opus) has reviewed the full diff, the same way Phase 2's whole-branch review caught a Critical `Dataset::scan` regression that no task-scoped review could see (each task-scoped review only ever sees one task's diff). Do not skip this. Do not mark Phase 3 done in conversation before it happens.

Pay particular attention, in that review, to whether `scan_with_predicate`'s pruning and `explain`'s reporting can ever disagree (they must always agree, since both call the exact same `should_scan_file` — but verify no future edit could let them drift apart), and whether the manifest's breaking schema change (Task 1) is handled consistently everywhere `Manifest`/`data_files` is touched — this plan updated every known call site, but a whole-branch review, not a task-scoped one, is what caught the equivalent gap in Phase 2.
