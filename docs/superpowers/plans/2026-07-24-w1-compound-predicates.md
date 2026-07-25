# W1 — Compound Predicates (AND/OR) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `strata_query::Predicate` from a single flat leaf condition to a small boolean tree
(`And`, `Or`, plus the five existing leaf comparisons), so both row-level filtering and file-level
pruning understand compound conditions like `id >= X AND category = Y`.

**Architecture:** `Predicate` gains two recursive variants. Row-level evaluation (`mask`/`filter`)
composes per-leaf boolean masks with Arrow's `and`/`or` kernels rather than chaining
`filter_record_batch` calls (chaining would silently produce OR-only semantics for the second filter
and copy the batch twice). File-level pruning (`should_scan_file`) recurses with De Morgan-correct
polarity: an `And` can prune using *either* side's information; an `Or` only prunes if *both* sides
agree the file can't match. One existing call site (`Snapshot::row_ids_matching`'s column projection)
generalizes from "the predicate's one column" to "every column the predicate tree touches."

**Tech Stack:** Rust 2024, `arrow` 58.3 (`arrow::compute::kernels::boolean::{and, or}` for mask
composition, `arrow::compute::kernels::cmp` already used for leaf comparisons — unchanged).

## Global Constraints

- This is workstream W1 of Phase S1 (`.claude/docs/design/phase-s1-segmented-index-spec.md` §5.1):
  **additive, query-layer only** — none of the concurrency/transaction invariants in
  `.claude/rules/concurrency-txn-layer.md` apply, and no `loom` test is needed (no concurrency is
  touched).
- `cargo build --workspace` clean, `cargo test --workspace` green, `cargo clippy --workspace
  --all-targets -- -D warnings` clean, `cargo fmt --check` clean, before this is marked done
  (`.claude/CLAUDE.md` "What done means").
- No new dependencies — `and`/`or` boolean composition uses `arrow`, already a workspace dependency.
- Every task needs an Opus-5-tier `reviewer` subagent pass before being marked done (mandatory, not
  optional, per `.claude/CLAUDE.md`).
- Exit criterion (spec §5.1): `timestamp >= X AND category = Y` filters and prunes correctly. `W1`
  ships before `W2` (the timestamp column), so this plan proves the mechanism with the two existing
  orderable columns available today (`id`, and a second `Utf8`/`Int64` column added per-test) — the
  literal `timestamp`/`category` instantiation lands once W2 and W4 exist.

---

## File Structure

- **Modify `crates/query/src/predicate.rs`** — the `Predicate` enum, `mask`/`filter`/`compare`, and
  `should_scan_file`. This is the only file that owns predicate semantics; both row-filtering and
  file-pruning logic stay here, matching the module's existing "shared vocabulary" doc comment.
- **Modify `crates/txn/src/snapshot.rs`** — `Snapshot::row_ids_matching`'s column-projection list
  (currently built from a single `predicate.column()` call, which doesn't exist for a compound
  predicate spanning multiple columns).
- **Modify `crates/txn/tests/phase_3_pruning.rs`** — one new end-to-end integration test proving a
  compound predicate prunes files (`explain`) and filters rows (`scan_with_predicate`,
  `vector_search`) correctly together, in the same style as the file's two existing tests.

No new files. `crates/query/src/lib.rs` re-exports `Predicate` by name (`pub use
predicate::{Predicate, filter, mask, should_scan_file}`) — the new variants and the `columns()` method
travel with the type automatically; no edit needed there. `crates/cli/src/main.rs` only constructs leaf
`Predicate::Eq/Lt/...` values and never calls the two methods this plan removes (verified by search) —
no edit needed there either.

---

### Task 1: `Predicate::And`/`Or` variants + compound row-level filtering

**Files:**
- Modify: `crates/query/src/predicate.rs`

**Interfaces:**
- Produces: `Predicate::And(Box<Predicate>, Box<Predicate>)`, `Predicate::Or(Box<Predicate>,
  Box<Predicate>)` — new enum variants. `Predicate::columns(&self) -> Vec<&str>` — new public method,
  replaces the removed `Predicate::column(&self) -> &str` (which returns exactly one column and has no
  sensible meaning for a compound predicate). `mask`/`filter` unchanged in signature, now handle
  compound predicates. The public `Predicate::value(&self) -> &Value` method is also removed (same
  reason as `column()`; it had no external callers — see Task 3's grep note).

- [ ] **Step 1: Write the failing test**

Add to `crates/query/src/predicate.rs`'s `#[cfg(test)] mod tests` block (after the existing
`filter_eq_on_utf8_column` test):

```rust
    #[test]
    fn mask_and_combines_two_leaf_conditions() {
        let batch = sample_batch(); // id: [10, 20, 30], name: ["a", "b", "c"]
        let predicate = Predicate::And(
            Box::new(Predicate::GtEq("id".to_string(), Value::Int64(20))),
            Box::new(Predicate::Eq("name".to_string(), Value::Utf8("b".to_string()))),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 20);
    }

    #[test]
    fn mask_or_combines_two_leaf_conditions() {
        let batch = sample_batch(); // id: [10, 20, 30], name: ["a", "b", "c"]
        let predicate = Predicate::Or(
            Box::new(Predicate::Eq("id".to_string(), Value::Int64(10))),
            Box::new(Predicate::Eq("id".to_string(), Value::Int64(30))),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: Vec<i64> = (0..result.num_rows()).map(|i| ids.value(i)).collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 30]);
    }

    fn naive_eval(predicate: &Predicate, id: i64, name: &str) -> bool {
        let actual_for = |column: &str| -> Value {
            match column {
                "id" => Value::Int64(id),
                "name" => Value::Utf8(name.to_string()),
                other => panic!("naive_eval: unknown column {other}"),
            }
        };
        match predicate {
            Predicate::Eq(c, v) => actual_for(c) == *v,
            Predicate::Lt(c, v) => actual_for(c) < *v,
            Predicate::LtEq(c, v) => actual_for(c) <= *v,
            Predicate::Gt(c, v) => actual_for(c) > *v,
            Predicate::GtEq(c, v) => actual_for(c) >= *v,
            Predicate::And(l, r) => naive_eval(l, id, name) && naive_eval(r, id, name),
            Predicate::Or(l, r) => naive_eval(l, id, name) || naive_eval(r, id, name),
        }
    }

    #[test]
    fn mask_matches_naive_reference_over_compound_predicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = vec![1i64, 2, 3, 4, 5, 6];
        let names = vec!["a", "b", "a", "c", "b", "a"];
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(names.clone())),
            ],
        )
        .unwrap();

        let leaves = vec![
            Predicate::GtEq("id".to_string(), Value::Int64(3)),
            Predicate::Lt("id".to_string(), Value::Int64(5)),
            Predicate::Eq("name".to_string(), Value::Utf8("a".to_string())),
        ];

        let mut compounds = Vec::new();
        for i in 0..leaves.len() {
            for j in 0..leaves.len() {
                if i == j {
                    continue;
                }
                compounds.push(Predicate::And(
                    Box::new(leaves[i].clone()),
                    Box::new(leaves[j].clone()),
                ));
                compounds.push(Predicate::Or(
                    Box::new(leaves[i].clone()),
                    Box::new(leaves[j].clone()),
                ));
            }
        }

        for predicate in &compounds {
            let selection = mask(&batch, predicate).unwrap();
            for row in 0..ids.len() {
                let expected = naive_eval(predicate, ids[row], names[row]);
                assert_eq!(
                    selection.value(row),
                    expected,
                    "predicate {predicate:?} row {row} (id={}, name={}): mask()={} naive={}",
                    ids[row],
                    names[row],
                    selection.value(row),
                    expected
                );
            }
        }
    }

    #[test]
    fn columns_collects_every_leaf_column_in_a_compound_predicate() {
        let predicate = Predicate::And(
            Box::new(Predicate::GtEq("timestamp".to_string(), Value::Int64(100))),
            Box::new(Predicate::Eq("category".to_string(), Value::Utf8("x".to_string()))),
        );
        assert_eq!(predicate.columns(), vec!["timestamp", "category"]);
    }

    #[test]
    fn columns_on_a_leaf_predicate_returns_one_column() {
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(1));
        assert_eq!(predicate.columns(), vec!["id"]);
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test -p strata-query mask_and_combines_two_leaf_conditions`
Expected: compile error — `no variant or associated item named 'And' found for enum 'Predicate'` (and
similarly for `Or`, `columns`).

- [ ] **Step 3: Add the `And`/`Or` variants and the `columns()` method**

Replace the `Predicate` enum and its `impl` block (lines 13-44 of the current file) with:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(String, Value),
    Lt(String, Value),
    LtEq(String, Value),
    Gt(String, Value),
    GtEq(String, Value),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

impl Predicate {
    /// Every column this predicate reads from, in tree order. A leaf
    /// contributes one column; `And`/`Or` contribute their children's
    /// columns in order. May contain duplicates (e.g. `a = 1 OR a = 2`) —
    /// callers building a projection list should dedup the result.
    #[must_use]
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Predicate::Eq(c, _)
            | Predicate::Lt(c, _)
            | Predicate::LtEq(c, _)
            | Predicate::Gt(c, _)
            | Predicate::GtEq(c, _) => vec![c.as_str()],
            Predicate::And(l, r) | Predicate::Or(l, r) => {
                let mut cols = l.columns();
                cols.extend(r.columns());
                cols
            }
        }
    }
}
```

This removes the old `column(&self) -> &str` and `value(&self) -> &Value` methods. They returned
exactly one column/value each and have no sensible meaning for `And`/`Or` — `columns()` is their
compound-aware replacement, and `value()` had no callers outside this file's own `compare`/
`should_scan_file` (both rewritten below to extract the value inline instead).

- [ ] **Step 4: Rewrite `mask` and `compare` to recurse over compound predicates**

Add to the top-of-file imports (after the existing `use arrow::compute::kernels::cmp::{...}` line):

```rust
use arrow::compute::kernels::boolean::{and, or};
```

Replace `mask` (the current lines ~71-75) with:

```rust
pub fn mask(batch: &RecordBatch, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    match predicate {
        Predicate::And(l, r) => Ok(and(&mask(batch, l)?, &mask(batch, r)?)?),
        Predicate::Or(l, r) => Ok(or(&mask(batch, l)?, &mask(batch, r)?)?),
        Predicate::Eq(c, _)
        | Predicate::Lt(c, _)
        | Predicate::LtEq(c, _)
        | Predicate::Gt(c, _)
        | Predicate::GtEq(c, _) => {
            let idx = batch.schema_ref().index_of(c)?;
            let array = batch.column(idx);
            compare(array, predicate)
        }
    }
}
```

Replace `compare` (the current lines ~77-102) with:

```rust
fn compare(array: &ArrayRef, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    let cmp_fn: fn(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> Result<BooleanArray, ArrowError> = match predicate {
        Predicate::Eq(..) => eq,
        Predicate::Lt(..) => lt,
        Predicate::LtEq(..) => lt_eq,
        Predicate::Gt(..) => gt,
        Predicate::GtEq(..) => gt_eq,
        Predicate::And(..) | Predicate::Or(..) => {
            unreachable!("compare() is only reachable from mask()'s leaf arm")
        }
    };
    let value = match predicate {
        Predicate::Eq(_, v)
        | Predicate::Lt(_, v)
        | Predicate::LtEq(_, v)
        | Predicate::Gt(_, v)
        | Predicate::GtEq(_, v) => v,
        Predicate::And(..) | Predicate::Or(..) => {
            unreachable!("compare() is only reachable from mask()'s leaf arm")
        }
    };
    match value {
        Value::Int64(v) => {
            let scalar = Int64Array::new_scalar(*v);
            cmp_fn(array, &scalar)
        }
        Value::Float64(v) => {
            let scalar = Float64Array::new_scalar(*v);
            cmp_fn(array, &scalar)
        }
        Value::Utf8(v) => {
            let scalar = StringArray::new_scalar(v.as_str());
            cmp_fn(array, &scalar)
        }
    }
}
```

`filter` (which calls `mask` then `filter_record_batch`) is unchanged — it already delegates entirely
to `mask`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p strata-query`
Expected: PASS, including all pre-existing tests in this file (leaf-predicate behavior is byte-for-byte
unchanged — the leaf arm of the new `mask` does exactly what the old `mask` did).

- [ ] **Step 6: Commit**

```bash
git add crates/query/src/predicate.rs
git commit -m "feat(query): compound AND/OR predicates for row-level filtering"
```

---

### Task 2: Compound file-level pruning (`should_scan_file`)

**Files:**
- Modify: `crates/query/src/predicate.rs`

**Interfaces:**
- Consumes: `Predicate::And`/`Or` from Task 1.
- Produces: `should_scan_file`'s signature is unchanged
  (`fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool`); its
  behavior now covers compound predicates per spec §5.1: "a leaf prunes as today, an `And` prunes if
  *either* side prunes, an `Or` prunes only if *both* sides prune." (`should_scan_file` returns `true`
  = "must scan", `false` = "prune/skip" — so in code, `And` = `should_scan_file(l) && should_scan_file(r)`
  and `Or` = `should_scan_file(l) || should_scan_file(r)`; see the doc comment added below for why.)

- [ ] **Step 1: Write the failing tests**

Add to `crates/query/src/predicate.rs`'s test module (after the existing `should_scan_file_*` tests):

```rust
    #[test]
    fn should_scan_file_and_prunes_using_either_operand_even_if_the_other_could_match() {
        let mut stats = HashMap::new();
        stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(0),
                max: Value::Int64(10),
            },
        );
        stats.insert(
            "b".to_string(),
            ColumnStats {
                min: Value::Int64(20),
                max: Value::Int64(30),
            },
        );

        // a=5 is in [0,10] - a single leaf on "a" alone cannot prune.
        let leaf_a = Predicate::Eq("a".to_string(), Value::Int64(5));
        // b=999 is outside [20,30] - this leaf alone already prunes.
        let leaf_b = Predicate::Eq("b".to_string(), Value::Int64(999));
        let compound = Predicate::And(Box::new(leaf_a.clone()), Box::new(leaf_b));

        assert!(
            should_scan_file(&stats, &leaf_a),
            "a single leaf (a=5) can't prove this file has no match"
        );
        assert!(
            !should_scan_file(&stats, &compound),
            "the AND must prune using leaf_b's information, which a single leaf (a=5) alone couldn't"
        );
    }

    #[test]
    fn should_scan_file_or_requires_both_operands_to_prune() {
        let mut stats = HashMap::new();
        stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(0),
                max: Value::Int64(10),
            },
        );
        stats.insert(
            "b".to_string(),
            ColumnStats {
                min: Value::Int64(20),
                max: Value::Int64(30),
            },
        );

        let out_of_range_a = Predicate::Eq("a".to_string(), Value::Int64(999));
        let out_of_range_b = Predicate::Eq("b".to_string(), Value::Int64(999));
        let in_range_b = Predicate::Eq("b".to_string(), Value::Int64(25));

        assert!(
            !should_scan_file(
                &stats,
                &Predicate::Or(Box::new(out_of_range_a.clone()), Box::new(out_of_range_b))
            ),
            "OR of two out-of-range leaves must prune - neither side could match"
        );
        assert!(
            should_scan_file(
                &stats,
                &Predicate::Or(Box::new(out_of_range_a), Box::new(in_range_b))
            ),
            "OR must scan if either side could still match"
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p strata-query should_scan_file_and_prunes_using_either_operand`
Expected: compile error (`should_scan_file`'s current match on `predicate` isn't exhaustive once `And`/
`Or` exist — Task 1 already added the variants, so this is a real compile failure here, not just a
logic failure).

- [ ] **Step 3: Rewrite `should_scan_file`**

Replace the current `should_scan_file` function (lines ~104-130 pre-Task-1) with:

```rust
/// Decides whether a file whose column stats are `stats` could possibly
/// contain a row matching `predicate`. Fails open (returns `true`)
/// whenever it can't prove otherwise — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §2. Pure
/// function, zero I/O.
///
/// For a compound predicate: an `And` can prune the file if *either* side
/// alone proves no match is possible (both conjuncts must be satisfiable
/// for the AND to be satisfiable). An `Or` can only prune if *both* sides
/// prove no match is possible (either disjunct being satisfiable is enough
/// for the OR to be satisfiable). This is the same fail-open direction as
/// the leaf case: `should_scan_file` never says "skip" unless it can prove
/// the skip is safe.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And(l, r) => should_scan_file(stats, l) && should_scan_file(stats, r),
        Predicate::Or(l, r) => should_scan_file(stats, l) || should_scan_file(stats, r),
        Predicate::Eq(c, value)
        | Predicate::Lt(c, value)
        | Predicate::LtEq(c, value)
        | Predicate::Gt(c, value)
        | Predicate::GtEq(c, value) => {
            let Some(col_stats) = stats.get(c) else {
                return true; // no stats for this column - fail open, must scan
            };
            // A mismatched Value variant (e.g. a Utf8 predicate value
            // against an Int64 column's stats) can't be proven to miss -
            // fail open rather than trust derived PartialOrd's
            // cross-variant ordering, which compares by declaration
            // order, not value semantics.
            if std::mem::discriminant(value) != std::mem::discriminant(&col_stats.min) {
                return true;
            }
            match predicate {
                Predicate::Eq(..) => *value >= col_stats.min && *value <= col_stats.max,
                Predicate::Lt(..) => *value > col_stats.min,
                Predicate::LtEq(..) => *value >= col_stats.min,
                Predicate::Gt(..) => *value < col_stats.max,
                Predicate::GtEq(..) => *value <= col_stats.max,
                Predicate::And(..) | Predicate::Or(..) => {
                    unreachable!("compound predicates are handled by the outer match")
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p strata-query`
Expected: PASS, including all pre-existing `should_scan_file_*` tests (leaf behavior unchanged).

- [ ] **Step 5: Commit**

```bash
git add crates/query/src/predicate.rs
git commit -m "feat(query): compound AND/OR file-level pruning in should_scan_file"
```

---

### Task 3: Fix `Snapshot::row_ids_matching`'s projection for multi-column predicates + end-to-end test

**Files:**
- Modify: `crates/txn/src/snapshot.rs:296-301`
- Modify: `crates/txn/tests/phase_3_pruning.rs`

**Interfaces:**
- Consumes: `Predicate::columns() -> Vec<&str>` from Task 1.
- Produces: no new public interface; `Snapshot::vector_search`'s existing filtered-search path (which
  calls `row_ids_matching` internally) now works correctly for a compound predicate spanning more than
  one column instead of only projecting the first leaf's column.

Before this fix, `row_ids_matching` builds its column projection from `predicate.column()` — a method
that returned exactly one column name. That method no longer exists after Task 1 (removed because it
has no meaning for a compound predicate), so this call site is currently a compile error blocking the
whole crate. This task is not optional polish — without it, nothing in `crates/txn` builds once Task 1
lands.

- [ ] **Step 1: Confirm the current compile break**

Run: `cargo check -p strata-txn`
Expected: FAIL — `no method named 'column' found for reference '&Predicate'` at
`crates/txn/src/snapshot.rs:297` and `:300`.

- [ ] **Step 2: Fix the projection to use every column the predicate touches**

In `crates/txn/src/snapshot.rs`, replace lines 296-301:

```rust
        let projection: Vec<&str> = if predicate.column() == ROW_ID_COLUMN {
            vec![ROW_ID_COLUMN]
        } else {
            vec![predicate.column(), ROW_ID_COLUMN]
        };
```

with:

```rust
        let mut projection: Vec<&str> = predicate.columns();
        projection.push(ROW_ID_COLUMN);
        projection.sort_unstable();
        projection.dedup();
```

This also simplifies away the old single-column special case: a leaf predicate on `ROW_ID_COLUMN`
itself now naturally dedups to one entry, exactly as the old `if` branch handled it by hand.

- [ ] **Step 3: Run the existing txn test suite to verify nothing regressed**

Run: `cargo test -p strata-txn`
Expected: PASS. This is a pure generalization of a single-column projection to a multi-column one; every
existing caller passes a leaf predicate, for which `predicate.columns()` returns the same one-element
vector `predicate.column()` used to return.

- [ ] **Step 4: Write the end-to-end compound-predicate test**

Add to `crates/txn/tests/phase_3_pruning.rs` (after the existing two tests, before the closing of the
file):

```rust
#[test]
fn compound_and_predicate_prunes_files_and_filters_rows_together() {
    let dir = std::env::temp_dir().join(format!("strata-phase3-compound-{}", std::process::id()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let ds = Dataset::create(&dir).unwrap();

    // File A: id range [1,3] - below the id>=40 threshold, prunable by the
    // id leaf alone.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["x", "x", "x"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    // File B: id range [50,52], category all "y" - the one file that truly
    // matches both conjuncts.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![50, 51, 52])),
                Arc::new(arrow::array::StringArray::from(vec!["y", "y", "y"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    // File C: id range [60,62] - satisfies id>=40, so the id leaf ALONE
    // cannot prune this file. But category is all "x", so the category
    // leaf's own stats (min=max="x") prove no row can equal "y". Only the
    // AND, using both leaves' information together, prunes File C.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![60, 61, 62])),
                Arc::new(arrow::array::StringArray::from(vec!["x", "x", "x"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    let id_leaf_only = Predicate::GtEq("id".to_string(), Value::Int64(40));
    let predicate = Predicate::And(
        Box::new(id_leaf_only.clone()),
        Box::new(Predicate::Eq("category".to_string(), Value::Utf8("y".to_string()))),
    );
    let snapshot = ds.snapshot();

    // The id leaf alone can prune File A but not File C (60 and 62 are
    // both >= 40) - so a system that only ever pruned on the first leaf
    // would have to scan both File B and File C.
    let id_only_explain = snapshot.explain(&id_leaf_only);
    assert_eq!(
        id_only_explain.scanned.len(),
        2,
        "id>=40 alone cannot prune File C - its whole id range is >= 40"
    );

    // The AND compound prunes File C too, using the category leaf's
    // information that a single id-only leaf never had access to.
    let explain = snapshot.explain(&predicate);
    assert_eq!(explain.total_files, 3);
    assert_eq!(
        explain.scanned.len(),
        1,
        "the AND must prune File C using the category leaf, on top of File A via the id leaf"
    );
    assert_eq!(explain.scanned[0], ds.data_files()[1].name, "only File B survives");

    let filtered = snapshot.scan_with_predicate(&schema, &predicate).unwrap();
    assert_eq!(filtered.num_rows(), 3, "every row in File B matches both conjuncts");
    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut got: Vec<i64> = (0..filtered.num_rows()).map(|i| ids.value(i)).collect();
    got.sort_unstable();
    assert_eq!(got, vec![50, 51, 52]);

    std::fs::remove_dir_all(&dir).ok();
}
```

This test double-checks its own fixture arithmetic by asserting the single-leaf pruning result
(`id_only_explain`, 2 of 3 files) before asserting the compound result (1 of 3 files) — so a future
change to `compute_stats`, `should_scan_file`'s per-leaf logic, or the fixture data itself that shifts
which files get pruned will fail loudly on the first assertion instead of silently changing what the
second assertion "proves."

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p strata-txn --test phase_3_pruning compound_and_predicate_prunes_files_and_filters_rows_together`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/txn/src/snapshot.rs crates/txn/tests/phase_3_pruning.rs
git commit -m "fix(txn): multi-column projection for compound predicates in row_ids_matching"
```

---

### Task 4: Workstream verification and PR

**Files:** none (verification only).

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: success, no warnings.

- [ ] **Step 2: Full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass, including the new ones from Tasks 1-3.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. Pay particular attention to the two `unreachable!()` arms added in Task 1's `compare`
and Task 2's `should_scan_file` — `clippy::pedantic` does not flag `unreachable!` by itself, but confirm
no other pedantic lint (e.g. around the nested match in `should_scan_file`) fires; if one does, resolve
it by restructuring rather than an `#[allow]`, per this project's clippy conventions.

- [ ] **Step 4: Format check**

Run: `cargo fmt --check`
Expected: clean. If not, run `cargo fmt` and re-verify Steps 1-3 still pass, then amend the last commit
or add a formatting commit.

- [ ] **Step 5: Invoke the `superpowers:requesting-code-review` skill, targeting the `reviewer` subagent (Opus 5 tier)**

Per `.claude/CLAUDE.md`: "Review is not optional. Every task — regardless of which model implemented
it — goes through an Opus 5 review (the `reviewer` subagent) before it's marked done." Scope the review
to this workstream's diff: the three commits from Tasks 1-3 (`predicate.rs`'s new variants/`mask`/
`should_scan_file`, `snapshot.rs`'s projection fix, the new integration test). Address any findings with
new commits, not amended ones, per this project's git conventions.

- [ ] **Step 6: Open the PR**

This workstream's changes are additive and query-layer only (spec §5.1's own risk classification), so
once Steps 1-5 are green, open a PR from the current state of `feat/phase-s1-segmented-index` (or a
`feat/s1-w1-compound-predicates` branch cut from it, if the team prefers one-branch-per-workstream) into
`feat/phase-s1-segmented-index` (or wherever the S1 spec's "one PR per workstream" sequencing targets —
confirm with the user before pushing/opening, per this project's "PRs only, never push to `main`
directly" rule and the standing instruction to confirm before any action visible to others).

---

## What comes after this plan

W2 (first-class timestamp column) is the next workstream in the spec's sequencing (§5.2). Its plan
should be written fresh, after W1 has actually landed — the design doc
(`docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`) already resolves W2's
dependency on the row-id assignment pattern in `crates/txn/src/row_id.rs`, but a compound predicate
column-count-changing decision or a review finding from this plan's execution could still shift W2's
exact shape. W3 (the core segment migration) is the highest-risk workstream and should get its
implementation plan written last, immediately before it starts, against the code's actual state at that
point rather than a prediction made now — the design doc's §1-§5 already give it a concrete target to
plan against.
