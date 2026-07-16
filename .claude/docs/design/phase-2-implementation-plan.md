# Phase 2 (Real Encodings & Vectorized GROUP BY) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add automatic dictionary encoding to written data files and a real, hash-based, multi-column `GROUP BY` engine, per `docs/design/phase-2-encodings-and-groupby-spec.md`.

**Architecture:** Two independent additions. `crates/storage::encoding::encode_batch` runs before `write_batch` inside `Transaction::commit`, casting low-cardinality columns to `DictionaryArray`. `crates/query::group_by` hashes multi-column group keys via `arrow::row::RowConverter` into a `HashMap`-based accumulator, draining to a result `RecordBatch`. Neither touches `crates/txn`'s manifest/commit protocol beyond one new call site.

**Tech Stack:** Rust, `arrow` (already a workspace dependency — no new crates needed; `arrow::row`/`arrow::compute::kernels::cast` are re-exports already available through the existing `arrow` dependency).

## Global Constraints

- Edition 2024, workspace lints apply (`clippy::pedantic` + `clippy::all` at warn) — every public `Result`-returning function needs a `# Errors` doc section (see Phase 1's `strata-storage`/`strata-txn` for the established pattern).
- `unwrap()`/`expect()` are `clippy::warn` — fine in `#[cfg(test)]` modules (add `#[allow(clippy::unwrap_used, clippy::expect_used)]` on the `mod tests` block, matching every existing test module in this codebase), not fine in library code.
- **Git Flow branching.** Work happens on `feature/phase-2-encodings-and-groupby`, branched from `develop`. Every task's "Checkpoint" step means: run the verification commands, confirm green, then `git add` the task's files and `git commit` with a Conventional Commits message (`feat:`/`test:`/`chore:` — see `.claude/rules/git.md` for the format this project already uses). One commit per task is the floor; commit more often within a task if it helps (e.g. a separate `test:` commit for the failing test before the `feat:` commit that makes it pass) — the skill's TDD step structure already implies this rhythm. Never commit directly to `develop` or `main`.
- Verify any Arrow API you're not 100% certain of against the installed source before writing code that depends on it: `cargo metadata --format-version 1 | python -c "import json,sys; print([p['manifest_path'] for p in json.load(sys.stdin)['packages'] if p['name']=='<crate>'])"` gives the exact path under `~/.cargo/registry/src/`. This codebase has already been burned once (Phase 1, `hnsw_rs`) by trusting a library's own README example over its actual installed source.
- All new public functions in `crates/storage` and `crates/query` return `Result<_, ArrowError>` (or the crate's existing error type for `crates/storage` — see Task 1) — no new error enum needed for Phase 2.

---

### Task 1: `crates/storage::encoding` — dictionary encoding pass

**Files:**
- Create: `crates/storage/src/encoding.rs`
- Modify: `crates/storage/src/lib.rs` (add `pub mod encoding; pub use encoding::encode_batch;`)

**Interfaces:**
- Produces: `pub fn encode_batch(batch: &RecordBatch) -> crate::error::Result<RecordBatch>` — Task 2 calls this directly before `write_batch`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/storage/src/encoding.rs
//! Automatic dictionary encoding, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §1.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn low_cardinality_string_column_gets_dictionary_encoded() {
        // 100 rows, only 2 distinct values -> well below the 0.4 threshold.
        let names: Vec<&str> = (0..100).map(|i| if i % 2 == 0 { "alice" } else { "bob" }).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        let encoded_type = encoded.schema_ref().field(0).data_type();
        assert!(
            matches!(encoded_type, DataType::Dictionary(_, _)),
            "expected a Dictionary type, got {encoded_type:?}"
        );
    }

    #[test]
    fn high_cardinality_column_is_left_unencoded() {
        // 100 rows, all distinct -> well above the 0.4 threshold.
        let ids: Vec<i64> = (0..100).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.schema_ref().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn encoding_preserves_row_count_and_schema_field_names() {
        let names: Vec<&str> = vec!["x"; 10];
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.num_rows(), 10);
        assert_eq!(encoded.schema_ref().field(0).name(), "name");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-storage encoding`
Expected: FAIL to compile — `encode_batch` is not defined.

- [ ] **Step 3: Write the implementation**

Add above the `#[cfg(test)]` block in the same file:

```rust
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};

use crate::error::Result;

/// Below this distinct-value ratio (distinct / total rows), a column is
/// dictionary-encoded. Matches the range real columnar engines (Parquet)
/// default to.
const DICTIONARY_ENCODING_THRESHOLD: f64 = 0.4;

/// Casts each column of `batch` to `DictionaryArray<Int32Type>` if its
/// distinct-value ratio is below [`DICTIONARY_ENCODING_THRESHOLD`], leaving
/// higher-cardinality columns untouched. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §1.
///
/// # Errors
///
/// Returns an error if a column's distinct-value ratio can't be computed
/// (an `arrow::row` conversion failure) or if casting a low-cardinality
/// column to a dictionary type fails.
pub fn encode_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        if should_dictionary_encode(column)? {
            let dict_type =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(field.data_type().clone()));
            let encoded = cast(column.as_ref(), &dict_type)?;
            fields.push(Field::new(field.name(), dict_type, field.is_nullable()));
            columns.push(encoded);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(Arc::clone(column));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn should_dictionary_encode(column: &ArrayRef) -> Result<bool> {
    if column.is_empty() {
        return Ok(false);
    }
    let converter = RowConverter::new(vec![SortField::new(column.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(column))?;
    let distinct: HashSet<_> = rows.into_iter().map(|row| row.owned()).collect();
    #[allow(clippy::cast_precision_loss)]
    let ratio = distinct.len() as f64 / column.len() as f64;
    Ok(ratio < DICTIONARY_ENCODING_THRESHOLD)
}
```

- [ ] **Step 4: Wire up `lib.rs` and dependencies**

`crates/storage/src/lib.rs` — add to the existing `pub mod`/`pub use` lists:

```rust
pub mod encoding;
// ...
pub use encoding::encode_batch;
```

`crates/storage/Cargo.toml` needs no new entries — `arrow::row` and `arrow::compute::cast` are both part of the `arrow` crate already declared as a dependency.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p strata-storage encoding`
Expected: 3 passed (`low_cardinality_string_column_gets_dictionary_encoded`, `high_cardinality_column_is_left_unencoded`, `encoding_preserves_row_count_and_schema_field_names`)

- [ ] **Step 6: Checkpoint**

Run: `cargo clippy -p strata-storage --all-targets -- -D warnings && cargo fmt -p strata-storage --check`
Expected: both clean. Fix any `missing_errors_doc`/`float_cmp`/etc. findings the same way Phase 1's review pass did before moving to Task 2.

---

### Task 2: Wire `encode_batch` into `Transaction::commit`

**Files:**
- Modify: `crates/txn/src/dataset.rs` (the `commit` method)

**Interfaces:**
- Consumes: `strata_storage::encode_batch(&RecordBatch) -> strata_storage::Result<RecordBatch>` (Task 1)

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/dataset.rs`'s existing `#[cfg(test)] mod tests` block (it already has `#[allow(clippy::unwrap_used, clippy::expect_used)]` and the `temp_dir`/`test_schema` helpers — reuse them):

```rust
#[test]
fn low_cardinality_column_is_dictionary_encoded_on_commit() {
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    let dir = temp_dir("encode-on-commit");
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let ds = Dataset::create(&dir).unwrap();

    let names: Vec<&str> = vec!["x"; 20]; // single distinct value, well under threshold
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();
    let mut txn = ds.begin();
    txn.insert(batch);
    let ds = txn.commit().unwrap();

    // Read the raw written file back directly (bypassing Dataset::scan's
    // concat_batches, which would already show us the encoded type, but
    // reading the file directly proves the *durable* representation is
    // encoded, not just an in-memory artifact).
    let data_dir = ds.data_dir();
    let file_name = &ds_manifest_data_files(&ds)[0];
    let on_disk = strata_storage::read_batch(&data_dir.join(file_name)).unwrap();
    assert!(matches!(
        on_disk.schema_ref().field(0).data_type(),
        DataType::Dictionary(_, _)
    ));
    std::fs::remove_dir_all(&dir).ok();
}
```

This test needs a way to read the committed manifest's `data_files` list, which `Dataset` doesn't currently expose. Add this small accessor in the same step (non-test code, above `#[cfg(test)]`):

```rust
impl Dataset {
    // ... existing methods ...

    /// Data file names (relative to `data_dir()`) belonging to the current
    /// version. Exposed for tests that need to inspect the raw on-disk
    /// representation directly.
    #[must_use]
    pub fn data_files(&self) -> &[String] {
        &self.manifest.data_files
    }
}
```

Then use `ds.data_files()` instead of the placeholder `ds_manifest_data_files(&ds)` helper in the test above — replace that line with:

```rust
    let file_name = &ds.data_files()[0];
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-txn low_cardinality_column_is_dictionary_encoded_on_commit`
Expected: FAIL — the written file is still plain `Utf8`, not `Dictionary`, because `commit` doesn't call `encode_batch` yet.

- [ ] **Step 3: Implement — call `encode_batch` before `write_batch`**

In `crates/txn/src/dataset.rs`, modify `Transaction::commit`'s loop:

```rust
        for (i, batch) in self.pending.iter().enumerate() {
            let encoded = strata_storage::encode_batch(batch)?;
            let file_name = format!("{new_version:020}-{i}.arrow");
            write_batch(&data_dir.join(&file_name), &encoded)?;
            manifest.data_files.push(file_name);
        }
```

(This replaces the existing `write_batch(&data_dir.join(&file_name), batch)?;` line — `batch` becomes `&encoded`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-txn low_cardinality_column_is_dictionary_encoded_on_commit`
Expected: PASS

- [ ] **Step 5: Run the full existing `strata-txn` and `strata-cli` test suites — confirm nothing broke**

Run: `cargo test -p strata-txn -p strata-cli`
Expected: every existing test (including `crates/cli/tests/mvp_checklist_6_crash_recovery.rs` and `crates/txn/tests/mvp_checklist_1_to_5.rs`) still passes. Dictionary-encoded columns must round-trip transparently through `Dataset::scan`, `strata_query::filter_eq`, and `strata_index::brute_force_search` — if any of those fail, the bug is almost certainly a downstream consumer assuming a concrete non-Dictionary array type instead of going through the generic `Array`/`Datum` interfaces (see the spec's §1 note on why this should already work).

- [ ] **Step 6: Checkpoint**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean.

---

### Task 3: `crates/query::group_by` — core engine (single group column, `Count`)

**Files:**
- Create: `crates/query/src/group_by.rs`
- Modify: `crates/query/src/lib.rs` (add `pub mod group_by; pub use group_by::{group_by, AggFunc};`)

**Interfaces:**
- Produces: `pub enum AggFunc { Count, Sum, Min, Max, Avg }` and `pub fn group_by(batch: &RecordBatch, group_cols: &[&str], aggs: &[(&str, AggFunc)]) -> Result<RecordBatch, ArrowError>` — Task 4 extends this function's body; the signature does not change.

This task proves the `RowConverter` + `HashMap` pipeline works end-to-end with the simplest possible case. Task 4 adds the remaining `AggFunc` variants and multi-column support on top of the same structure.

- [ ] **Step 1: Write the failing test**

```rust
// crates/query/src/group_by.rs
//! Hash-based `GROUP BY`, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn single_column_count_groups_correctly() {
        let batch = sample_batch();
        let result =
            group_by(&batch, &["category"], &[("amount", AggFunc::Count)]).unwrap();

        assert_eq!(result.num_rows(), 2); // "a" and "b"

        let categories =
            result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();

        let mut got: Vec<(String, i64)> = (0..result.num_rows())
            .map(|i| (categories.value(i).to_string(), counts.value(i)))
            .collect();
        got.sort();
        assert_eq!(got, vec![("a".to_string(), 3), ("b".to_string(), 2)]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-query single_column_count_groups_correctly`
Expected: FAIL to compile — `group_by`/`AggFunc` not defined.

- [ ] **Step 3: Write the implementation**

Add above the `#[cfg(test)]` block:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::row::{OwnedRow, RowConverter, SortField};

/// Which aggregate to compute over a column within each group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// One row's running aggregate state for a single `(column, AggFunc)` pair.
#[derive(Debug, Clone, Copy)]
enum Accumulator {
    Count(u64),
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: u64 },
}

impl Accumulator {
    fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count(0),
            AggFunc::Sum => Self::Sum(0.0),
            AggFunc::Min => Self::Min(f64::INFINITY),
            AggFunc::Max => Self::Max(f64::NEG_INFINITY),
            AggFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn update(&mut self, value: f64) {
        match self {
            Self::Count(n) => *n += 1,
            Self::Sum(s) => *s += value,
            Self::Min(m) => *m = m.min(value),
            Self::Max(m) => *m = m.max(value),
            Self::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
        }
    }

    fn finish(self) -> f64 {
        match self {
            Self::Count(n) => {
                #[allow(clippy::cast_precision_loss)]
                let n = n as f64;
                n
            }
            Self::Sum(s) | Self::Min(s) | Self::Max(s) => s,
            Self::Avg { sum, count } => {
                #[allow(clippy::cast_precision_loss)]
                let count = count as f64;
                sum / count
            }
        }
    }
}

/// Groups `batch` by `group_cols` and computes `aggs` per group. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
///
/// # Errors
///
/// Returns an [`ArrowError::InvalidArgumentError`] if `group_cols` is empty,
/// if any named column doesn't exist, or if a non-numeric column is passed
/// to `Sum`/`Min`/`Max`/`Avg`.
pub fn group_by(
    batch: &RecordBatch,
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
) -> Result<RecordBatch, ArrowError> {
    if group_cols.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "group_by requires at least one group column".to_string(),
        ));
    }

    let schema = batch.schema_ref();
    let group_arrays: Vec<ArrayRef> = group_cols
        .iter()
        .map(|name| {
            let idx = schema.index_of(name)?;
            Ok(Arc::clone(batch.column(idx)))
        })
        .collect::<Result<_, ArrowError>>()?;

    // Count doesn't require a numeric column — it only null-checks, so it
    // keeps the *original* array uncast. Sum/Min/Max/Avg require numeric
    // input and get cast to Float64 up front. Casting a non-numeric column
    // (e.g. Utf8) to Float64 just to support Count would be wrong: arrow's
    // cast kernel would try to *parse* strings as numbers, erroring or
    // nulling out perfectly valid non-numeric values for no reason.
    let agg_arrays: Vec<(ArrayRef, AggFunc)> = aggs
        .iter()
        .map(|(name, func)| {
            let idx = schema.index_of(name)?;
            let arr = batch.column(idx);
            if matches!(func, AggFunc::Count) {
                return Ok((Arc::clone(arr), *func));
            }
            if !arr.data_type().is_numeric() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "column {name} is not numeric, cannot apply {func:?}"
                )));
            }
            let as_f64 = arrow::compute::cast(arr.as_ref(), &DataType::Float64)?;
            Ok((as_f64, *func))
        })
        .collect::<Result<_, ArrowError>>()?;

    let converter = RowConverter::new(
        group_arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect(),
    )?;
    let rows = converter.convert_columns(&group_arrays)?;

    let mut groups: HashMap<OwnedRow, Vec<Accumulator>> = HashMap::new();
    for i in 0..batch.num_rows() {
        let key = rows.row(i).owned();
        let accs = groups
            .entry(key)
            .or_insert_with(|| agg_arrays.iter().map(|(_, f)| Accumulator::new(*f)).collect());
        for (acc, (arr, func)) in accs.iter_mut().zip(&agg_arrays) {
            if arr.is_null(i) {
                continue;
            }
            if matches!(func, AggFunc::Count) {
                acc.update(0.0); // value is unused by Accumulator::Count
                continue;
            }
            let col = arr.as_any().downcast_ref::<arrow::array::Float64Array>().ok_or_else(
                || ArrowError::CastError("expected Float64 after cast".to_string()),
            )?;
            acc.update(col.value(i));
        }
    }

    build_result_batch(&group_arrays, group_cols, aggs, &converter, groups)
}

fn build_result_batch(
    group_arrays: &[ArrayRef],
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
    converter: &RowConverter,
    groups: HashMap<OwnedRow, Vec<Accumulator>>,
) -> Result<RecordBatch, ArrowError> {
    let owned_keys: Vec<OwnedRow> = groups.keys().cloned().collect();
    let borrowed_keys: Vec<_> = owned_keys.iter().map(OwnedRow::row).collect();
    let group_columns = converter.convert_rows(borrowed_keys)?;

    let mut agg_columns: Vec<Vec<f64>> = vec![Vec::with_capacity(groups.len()); aggs.len()];
    for key in &owned_keys {
        let accs = &groups[key];
        for (col, acc) in agg_columns.iter_mut().zip(accs) {
            col.push(acc.finish());
        }
    }

    let mut fields: Vec<Field> = group_cols
        .iter()
        .zip(group_arrays)
        .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
        .collect();
    let mut columns: Vec<ArrayRef> = group_columns;
    for ((name, func), values) in aggs.iter().zip(agg_columns) {
        fields.push(Field::new(format!("{name}_{func:?}").to_lowercase(), DataType::Float64, false));
        columns.push(Arc::new(Float64Array::from(values)));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}
```

`RowConverter::convert_rows` (used in `build_result_batch` above) is verified against arrow-row 58.3.0's installed source: `pub fn convert_rows<'a, I>(&self, rows: I) -> Result<Vec<ArrayRef>, ArrowError> where I: IntoIterator<Item = Row<'a>>` — matches the `converter.convert_rows(borrowed_keys)?` call exactly, where `borrowed_keys: Vec<Row<'_>>` comes from `owned_keys.iter().map(OwnedRow::row)`.

- [ ] **Step 4: Wire up `lib.rs`**

`crates/query/src/lib.rs` — add:

```rust
pub mod group_by;
// ...
pub use group_by::{AggFunc, group_by};
```

Also add the missing `Float64Array` import to `group_by.rs`'s implementation block: `use arrow::array::Float64Array;` (alongside the existing `arrow::array::{Array, ArrayRef, Int64Array, RecordBatch}` import — note `Int64Array` is unused by the implementation as written and should be removed from that import list; it was only needed by the test module, which imports it separately).

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p strata-query single_column_count_groups_correctly`
Expected: PASS

- [ ] **Step 6: Checkpoint**

Run: `cargo clippy -p strata-query --all-targets -- -D warnings && cargo fmt -p strata-query --check`
Expected: clean — fix any findings before Task 4 (this file is large and pedantic-heavy; expect at least a `missing_errors_doc` or two on first pass, matching the pattern from Phase 1's review).

---

### Task 4: Extend `group_by` — multi-column grouping, all `AggFunc` variants, error paths

**Files:**
- Modify: `crates/query/src/group_by.rs` (tests only — the implementation from Task 3 already supports multiple group columns and all `AggFunc` variants structurally; this task is primarily test coverage plus the error-path checks the spec requires)

**Interfaces:**
- Consumes/Produces: same signature as Task 3 — no API change.

- [ ] **Step 1: Write the failing tests**

Add to `group_by.rs`'s `mod tests` block:

```rust
    #[test]
    fn multi_column_grouping_and_multiple_agg_funcs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "east", "west", "east"])),
                Arc::new(StringArray::from(vec!["a", "a", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let result = group_by(
            &batch,
            &["region", "category"],
            &[("amount", AggFunc::Sum), ("amount", AggFunc::Max)],
        )
        .unwrap();

        assert_eq!(result.num_rows(), 3); // (east,a) (west,a) (east,b)
        assert_eq!(result.num_columns(), 4); // region, category, amount_sum, amount_max
    }

    #[test]
    fn each_agg_func_computes_correctly_for_a_single_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "x", "x"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        for (func, expected) in [
            (AggFunc::Count, 3.0),
            (AggFunc::Sum, 60.0),
            (AggFunc::Min, 10.0),
            (AggFunc::Max, 30.0),
            (AggFunc::Avg, 20.0),
        ] {
            let result = group_by(&batch, &["k"], &[("v", func)]).unwrap();
            let values =
                result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(values.value(0), expected, "AggFunc::{func:?}");
        }
    }

    #[test]
    fn empty_group_cols_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &[], &[("amount", AggFunc::Count)]);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn unknown_column_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &["not_a_column"], &[("amount", AggFunc::Count)]);
        assert!(result.is_err());
    }

    #[test]
    fn non_numeric_agg_column_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &["category"], &[("category", AggFunc::Sum)]);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn count_on_non_numeric_column_succeeds() {
        // Count must NOT require a numeric column — see Task 3's
        // Accumulator/agg_arrays design note on why casting a Utf8 column
        // to Float64 just to support Count would be wrong.
        let batch = sample_batch();
        let result = group_by(&batch, &["category"], &[("category", AggFunc::Count)]).unwrap();
        assert_eq!(result.num_rows(), 2); // "a" and "b"
    }
```

Add `use arrow::array::Float64Array;` to the test module's imports if not already present via `super::*`.

- [ ] **Step 2: Run tests to verify current state**

Run: `cargo test -p strata-query group_by`
Expected: `multi_column_grouping_and_multiple_agg_funcs`, `each_agg_func_computes_correctly_for_a_single_group`, `empty_group_cols_errors`, `non_numeric_agg_column_errors`, and `count_on_non_numeric_column_succeeds` should already PASS (Task 3's implementation already handles multi-column/all-AggFunc/empty-group-cols/non-numeric-except-Count cases structurally). `unknown_column_errors` should also PASS (`schema.index_of` already returns `Err` for an unknown name, propagated via `?`). If any of these fail, that's a real bug in Task 3's implementation to fix now, not a sign this task's tests are wrong — re-read Task 3's `group_by` body against the failing case.

- [ ] **Step 3: Fix any failures found in Step 2**

(Only if Step 2 found failures — if everything already passed, skip to Step 4.)

- [ ] **Step 4: Checkpoint**

Run: `cargo clippy -p strata-query --all-targets -- -D warnings && cargo fmt -p strata-query --check && cargo test -p strata-query`
Expected: all clean, all `group_by` tests (7 total across Tasks 3-4) passing.

---

### Task 5: `bench/` — the actual 10M+ row exit-criterion benchmark

**Files:**
- Create: `bench/Cargo.toml`
- Create: `bench/benches/group_by_bench.rs`
- Modify: root `Cargo.toml` (add `"bench"` to `[workspace] members`, add `criterion` to `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `strata_query::{group_by, AggFunc}` (Tasks 3-4)

- [ ] **Step 1: Add `criterion` to the workspace and create the bench crate**

Root `Cargo.toml`, add to `[workspace] members`:

```toml
members = [
    "crates/storage",
    "crates/txn",
    "crates/index",
    "crates/query",
    "crates/bindings",
    "crates/cli",
    "bench",
]
```

Add to `[workspace.dependencies]`:

```toml
criterion = "0.8"
```

(Verified current as of this plan's writing: 0.8.2, released 2026-02-04, supports the last three stable Rust minor releases — well within this project's MSRV floor. If significant time has passed since this plan was written, re-verify at `https://crates.io/crates/criterion` before trusting this version — do not assume it's still current, per this project's own established practice, ADR 0004/0005.)

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
strata-query = { path = "../crates/query" }

[dev-dependencies]
criterion = { workspace = true, features = ["html_reports"] }

[[bench]]
name = "group_by_bench"
harness = false
```

- [ ] **Step 2: Write the benchmark, including the correctness-against-reference check**

`bench/benches/group_by_bench.rs`:

```rust
//! `GROUP BY` benchmark — Phase 2's actual exit criterion ("GROUP BY over
//! 10M+ rows, correct, benchmarked"). Correctness against a naive,
//! obviously-right reference implementation is checked *before* the
//! throughput numbers are trusted — see
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §3.

// This file is a bench target, not a #[cfg(test)] module, but
// `cargo clippy --all-targets` still lints it under the workspace's
// `unwrap_used`/`expect_used` warn-level lints (promoted to errors via
// `-D warnings`). Benchmarks legitimately need to unwrap synthetic
// data construction — allow it here the same way every test module in
// this codebase already does.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_query::{AggFunc, group_by};

const ROW_COUNT: usize = 10_000_000;
const DISTINCT_CATEGORIES: i64 = 1000;

fn synthetic_batch() -> RecordBatch {
    let categories: Vec<String> =
        (0..ROW_COUNT).map(|i| format!("cat-{}", i as i64 % DISTINCT_CATEGORIES)).collect();
    let amounts: Vec<i64> = (0..ROW_COUNT).map(|i| i as i64 % 997).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(amounts)),
        ],
    )
    .expect("synthetic batch construction cannot fail with matching lengths")
}

/// Deliberately naive, obviously-correct reference: plain Rust `HashMap`
/// over materialized native values, no Arrow row-format tricks. Used only
/// to check `group_by`'s output, not as a performance comparison.
fn naive_reference_sum(batch: &RecordBatch) -> HashMap<String, i64> {
    let categories = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let amounts = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();

    let mut totals: HashMap<String, i64> = HashMap::new();
    for i in 0..batch.num_rows() {
        *totals.entry(categories.value(i).to_string()).or_insert(0) += amounts.value(i);
    }
    totals
}

fn check_correctness(batch: &RecordBatch) {
    let result = group_by(batch, &["category"], &[("amount", AggFunc::Sum)])
        .expect("group_by must succeed on well-formed input");
    let reference = naive_reference_sum(batch);

    assert_eq!(result.num_rows(), reference.len(), "group count mismatch");

    let categories =
        result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let sums = result.column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    for i in 0..result.num_rows() {
        let cat = categories.value(i);
        #[allow(clippy::cast_precision_loss)]
        let expected = *reference.get(cat).expect("category present in reference") as f64;
        assert!(
            (sums.value(i) - expected).abs() < 1e-6,
            "sum mismatch for category {cat}: got {}, expected {expected}",
            sums.value(i)
        );
    }
}

fn bench_group_by(c: &mut Criterion) {
    let batch = synthetic_batch();

    // Correctness gate — runs once, before any timed iteration. A fast
    // wrong answer is worse than a slow right one (see the spec).
    check_correctness(&batch);

    let mut group = c.benchmark_group("group_by_10m_rows");
    group.sample_size(10);
    group.bench_function("single_column_sum", |b| {
        b.iter(|| group_by(&batch, &["category"], &[("amount", AggFunc::Sum)]).unwrap());
    });
    group.finish();
}

criterion_group!(benches, bench_group_by);
criterion_main!(benches);
```

- [ ] **Step 3: Run the benchmark**

Run: `cargo bench -p strata-bench`
Expected: `check_correctness` passes silently (a panic there fails the whole `cargo bench` run before any timing output appears — that's intentional, it's the correctness gate), followed by Criterion's timing report for `group_by_10m_rows/single_column_sum`. Note the actual throughput number in the task completion report — this is the Phase 2 exit criterion's deliverable, not just a passing test.

- [ ] **Step 4: Checkpoint**

Run: `cargo check --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo test --workspace`
Expected: everything clean, exactly as it was at the end of Phase 1's review pass, plus every new test from Tasks 1-4.

---

## Final Step: Dispatch the mandatory `reviewer` subagent

Per `CLAUDE.md`'s "What 'done' means" — this phase is not complete until the `reviewer` subagent (`.claude/agents/reviewer.md`, Opus) has reviewed the full diff, the same way Phase 1's critical fsync-durability bug was only caught by that review, not by the test suite. Do not skip this. Do not mark Phase 2 done in conversation before it happens.
