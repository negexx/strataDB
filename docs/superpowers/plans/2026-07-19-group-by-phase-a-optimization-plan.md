# GROUP BY Phase A Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `crates/query/src/group_by.rs`'s `HashMap<OwnedRow, Vec<Accumulator>>` internals — which heap-allocate a fresh `OwnedRow` on every input row, not just every distinct group — with a `HashMap<Row<'_>, usize>` index over the `Rows` buffer the function already builds, plus columnar (`Vec<T>`-per-`group_idx`) accumulator state, as a pure internal swap with `group_by`'s public contract unchanged.

**Architecture:** Per `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md`. `arrow_row::Row<'a>` (confirmed against the vendored `arrow-row-58.3.0` source) already implements `Hash`/`Eq`/`Ord`/`Copy` purely over its byte slice, so a plain `std::collections::HashMap<Row<'a>, usize>` maps a borrowed row directly to a group index with zero new dependencies and zero `unsafe`. A new `ColumnarAccumulator` enum replaces the scalar `Accumulator` enum, storing each aggregate's state as one contiguous `Vec<T>` indexed by group index instead of one `Vec<Accumulator>` per group.

**Tech Stack:** Rust, `arrow`/`arrow_row` (already a workspace dependency, no version change), `criterion` (benchmarking), existing `crates/query` test harness.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md` — read it for the full reasoning behind every design choice below; this plan implements it, not re-derives it.
- `group_by`'s public signature, `ArrowError` conditions, output schema, and numeric semantics (Float64 coercion rule, null handling, identity values) must stay byte-for-byte identical — the existing `crates/query/src/group_by.rs` test suite (`mod tests`) is the correctness oracle and must pass **unmodified**. No task in this plan edits an existing test's body or assertions.
- No new dependencies. `Row<'a>: Hash + Eq + Ord + Copy` is already available via the workspace's existing `arrow` dependency (`arrow::row::Row`) — confirmed against `arrow-row-58.3.0/src/lib.rs:1441-1492`.
- Safe Rust only — no `unsafe`, no `get_unchecked`. Group-index bounds safety comes from a lockstep invariant (every `ColumnarAccumulator`'s vector is exactly `group_key_rows.len()` long at all times), not from skipping bounds checks.
- `cargo build --workspace` clean with no warnings, `cargo test --workspace` passing, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean — required before this plan is marked done (this project's standing "What done means" gate).
- **Sequencing is load-bearing, not incidental:** the cardinality-sweep benchmark must be added and run against **today's** unmodified implementation first (Task 1) to record a real baseline, before the rewrite happens (Task 3). Task 4 re-runs the identical benchmark afterward and compares against Task 1's recorded numbers — an architecturally "correct" rewrite with no measured win at the 1,000,000-group case does not satisfy this plan.
- Every task gets reviewed before being marked done, per this project's standing "Review is not optional" rule (Opus 4.8 `reviewer` subagent).

---

### Task 1: Add the cardinality-sweep benchmark and capture today's baseline

**Files:**
- Modify: `bench/benches/group_by_bench.rs`

**Interfaces:**
- Consumes: `strata_query::{AggFunc, group_by}` — existing public API, unchanged by this task.
- Produces: `synthetic_batch_with_cardinality(row_count: usize, cardinality: i64) -> RecordBatch`, `naive_reference_stats(batch: &RecordBatch) -> HashMap<String, NaiveStats>`, `check_correctness_multi_agg(batch: &RecordBatch)`, `bench_group_by_cardinality_sweep(c: &mut Criterion)` — none of these are consumed by later tasks (this file has no downstream Rust callers), but Task 4 re-runs the same benchmark function by name, so `bench_group_by_cardinality_sweep`'s benchmark group id (`"group_by_cardinality_sweep"`) and its four benchmark ids (`sum_1000_groups`, `sum_100000_groups`, `sum_1000000_groups`, `count_sum_min_max_1000000_groups`) must not be renamed in Task 4.

- [x] **Step 1: Replace the file's contents**

Replace all of `bench/benches/group_by_bench.rs` with:

```rust
//! `GROUP BY` benchmark — Phase 2's actual exit criterion ("GROUP BY over
//! 10M+ rows, correct, benchmarked"). Correctness against a naive,
//! obviously-right reference implementation is checked *before* the
//! throughput numbers are trusted — see
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §3.
//!
//! `bench_group_by_cardinality_sweep` adds the high-cardinality coverage
//! `bench_group_by` doesn't exercise (1,000 categories over 10M rows is
//! ~10,000 rows/group -- allocation-per-group cost is a tiny fraction of
//! total work there). See
//! `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md`
//! §8.

// This file is a bench target, not a #[cfg(test)] module, but
// `cargo clippy --all-targets` still lints it under the workspace's
// `unwrap_used`/`expect_used` warn-level lints (promoted to errors via
// `-D warnings`). Benchmarks legitimately need to unwrap synthetic
// data construction — allow it here the same way every test module in
// this codebase already does.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_query::{AggFunc, group_by};

const ROW_COUNT: usize = 10_000_000;
const DISTINCT_CATEGORIES: i64 = 1000;
const SWEEP_ROW_COUNT: usize = 1_000_000;

fn synthetic_batch_with_cardinality(row_count: usize, cardinality: i64) -> RecordBatch {
    // row_count is caller-controlled but always far below i64::MAX in this
    // file's own use (10M and 1M); usize -> i64 cannot wrap here.
    let categories: Vec<String> = (0..row_count)
        .map(|i| {
            #[allow(clippy::cast_possible_wrap)]
            let i = i as i64;
            format!("cat-{}", i % cardinality)
        })
        .collect();
    let amounts: Vec<i64> = (0..row_count)
        .map(|i| {
            #[allow(clippy::cast_possible_wrap)]
            let i = i as i64;
            i % 997
        })
        .collect();

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

fn synthetic_batch() -> RecordBatch {
    synthetic_batch_with_cardinality(ROW_COUNT, DISTINCT_CATEGORIES)
}

/// Deliberately naive, obviously-correct reference: plain Rust `HashMap`
/// over materialized native values, no Arrow row-format tricks. Used only
/// to check `group_by`'s output, not as a performance comparison.
fn naive_reference_sum(batch: &RecordBatch) -> HashMap<String, i64> {
    let categories = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

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

    let categories = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
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

/// Reference stats for the multi-aggregate correctness gate below --
/// `Count`/`Sum`/`Min`/`Max` computed the same deliberately-naive way as
/// `naive_reference_sum`.
struct NaiveStats {
    count: i64,
    sum: i64,
    min: i64,
    max: i64,
}

fn naive_reference_stats(batch: &RecordBatch) -> HashMap<String, NaiveStats> {
    let categories = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let mut totals: HashMap<String, NaiveStats> = HashMap::new();
    for i in 0..batch.num_rows() {
        let entry = totals
            .entry(categories.value(i).to_string())
            .or_insert(NaiveStats {
                count: 0,
                sum: 0,
                min: i64::MAX,
                max: i64::MIN,
            });
        let amount = amounts.value(i);
        entry.count += 1;
        entry.sum += amount;
        entry.min = entry.min.min(amount);
        entry.max = entry.max.max(amount);
    }
    totals
}

fn check_correctness_multi_agg(batch: &RecordBatch) {
    let result = group_by(
        batch,
        &["category"],
        &[
            ("amount", AggFunc::Count),
            ("amount", AggFunc::Sum),
            ("amount", AggFunc::Min),
            ("amount", AggFunc::Max),
        ],
    )
    .expect("group_by must succeed on well-formed input");
    let reference = naive_reference_stats(batch);

    assert_eq!(result.num_rows(), reference.len(), "group count mismatch");

    let categories = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = result
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mins = result
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let maxes = result
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    for i in 0..result.num_rows() {
        let cat = categories.value(i);
        let expected = reference.get(cat).expect("category present in reference");
        assert_eq!(counts.value(i), expected.count, "count mismatch for {cat}");
        #[allow(clippy::cast_precision_loss)]
        let expected_sum = expected.sum as f64;
        assert!(
            (sums.value(i) - expected_sum).abs() < 1e-6,
            "sum mismatch for category {cat}"
        );
        #[allow(clippy::cast_precision_loss)]
        let expected_min = expected.min as f64;
        assert!(
            (mins.value(i) - expected_min).abs() < 1e-6,
            "min mismatch for category {cat}"
        );
        #[allow(clippy::cast_precision_loss)]
        let expected_max = expected.max as f64;
        assert!(
            (maxes.value(i) - expected_max).abs() < 1e-6,
            "max mismatch for category {cat}"
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

/// High-cardinality sweep at a fixed 1,000,000 rows -- exercises the
/// allocation-heavy path `bench_group_by`'s low-cardinality (1,000
/// categories) shape doesn't reach. See
/// `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md`
/// §8.
fn bench_group_by_cardinality_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_cardinality_sweep");
    group.sample_size(10);

    for cardinality in [1_000i64, 100_000, 1_000_000] {
        let batch = synthetic_batch_with_cardinality(SWEEP_ROW_COUNT, cardinality);
        check_correctness(&batch);
        group.bench_function(format!("sum_{cardinality}_groups"), |b| {
            b.iter(|| group_by(&batch, &["category"], &[("amount", AggFunc::Sum)]).unwrap());
        });
    }

    let high_card_batch = synthetic_batch_with_cardinality(SWEEP_ROW_COUNT, 1_000_000);
    check_correctness_multi_agg(&high_card_batch);
    group.bench_function("count_sum_min_max_1000000_groups", |b| {
        b.iter(|| {
            group_by(
                &high_card_batch,
                &["category"],
                &[
                    ("amount", AggFunc::Count),
                    ("amount", AggFunc::Sum),
                    ("amount", AggFunc::Min),
                    ("amount", AggFunc::Max),
                ],
            )
            .unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_group_by, bench_group_by_cardinality_sweep);
criterion_main!(benches);
```

- [x] **Step 2: Confirm it builds and the existing 10M-row bench is untouched**

Run: `cargo check -p strata-bench --all-targets`
Expected: builds cleanly, no warnings.

Run: `cargo clippy -p strata-bench --all-targets -- -D warnings`
Expected: clean.

- [x] **Step 3: Run the sweep against today's (pre-rewrite) implementation and record the baseline**

Run: `cargo bench -p strata-bench --bench group_by_bench -- group_by_cardinality_sweep`

This runs only the new benchmark group (filtered by criterion's substring match on the group id), not the existing 10M-row bench. All four correctness gates (`check_correctness`/`check_correctness_multi_agg`, once per case) must pass silently before any timing output appears — a panic there means the fixture is wrong, fix it before proceeding.

Record the four reported times (`sum_1000_groups`, `sum_100000_groups`, `sum_1000000_groups`, `count_sum_min_max_1000000_groups`) — these are the baseline Task 4 must beat at the two 1,000,000-group cases. Append them as a note under this task's checkbox in this plan file, e.g.:

```markdown
- [x] Task 1: baseline recorded 2026-XX-XX —
      sum_1000_groups: <time>,
      sum_100000_groups: <time>,
      sum_1000000_groups: <time>,
      count_sum_min_max_1000000_groups: <time>
```

- [x] Task 1: baseline recorded 2026-07-19 —
      sum_1000_groups: 82.613 ms,
      sum_100000_groups: 279.32 ms,
      sum_1000000_groups: 1.2435 s,
      count_sum_min_max_1000000_groups: 1.2910 s

- [x] **Step 4: Commit**

```bash
git add bench/benches/group_by_bench.rs docs/superpowers/plans/2026-07-19-group-by-phase-a-optimization-plan.md
git commit -m "bench(query): add GROUP BY high-cardinality sweep, capture pre-rewrite baseline"
```

Include the four recorded numbers from Step 3 in the commit message body.

---

### Task 2: Add a high-cardinality differential test as a regression harness

**Files:**
- Modify: `crates/query/src/group_by.rs` (append to the existing `mod tests` block, after `group_by_accepts_a_dictionary_encoded_group_column_with_a_null_entry`)

**Interfaces:**
- Consumes: `group_by`, `AggFunc` — existing public API, unchanged by this task.
- Produces: nothing consumed by later tasks — this is a standalone test that must pass both before (this task) and after (Task 3) the rewrite, unmodified in between.

- [ ] **Step 1: Add the test**

Insert immediately before the final closing `}` of `mod tests` in `crates/query/src/group_by.rs` (i.e., directly after the `group_by_accepts_a_dictionary_encoded_group_column_with_a_null_entry` test's closing `}`):

```rust
    #[test]
    fn high_cardinality_grouping_matches_a_naive_reference_including_hash_collisions() {
        // 5,000 rows, 2,500 distinct groups (2 rows/group average) -- enough
        // distinct group keys to force real hash collisions in the
        // implementation's lookup structure, unlike the 2-5-group
        // hand-written tests above, which never stress it meaningfully.
        // Guards the Phase A rewrite described in
        // docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md.
        const ROW_COUNT: usize = 5_000;
        const CARDINALITY: i64 = 2_500;

        let categories: Vec<String> = (0..ROW_COUNT)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i = i as i64;
                format!("group-{}", i % CARDINALITY)
            })
            .collect();
        let amounts: Vec<i64> = (0..ROW_COUNT)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i = i as i64;
                i % 997
            })
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(categories.clone())),
                Arc::new(Int64Array::from(amounts.clone())),
            ],
        )
        .unwrap();

        let result = group_by(
            &batch,
            &["category"],
            &[
                ("amount", AggFunc::Count),
                ("amount", AggFunc::Sum),
                ("amount", AggFunc::Min),
                ("amount", AggFunc::Max),
            ],
        )
        .unwrap();

        // Independent, deliberately naive reference -- plain HashMap over
        // materialized native values, no RowConverter/Row involved, so it
        // can't share a bug with the implementation under test.
        let mut reference: std::collections::HashMap<String, (i64, i64, i64, i64)> =
            std::collections::HashMap::new();
        for i in 0..ROW_COUNT {
            let entry = reference
                .entry(categories[i].clone())
                .or_insert((0, 0, i64::MAX, i64::MIN));
            entry.0 += 1; // count
            entry.1 += amounts[i]; // sum
            entry.2 = entry.2.min(amounts[i]); // min
            entry.3 = entry.3.max(amounts[i]); // max
        }

        let result_categories = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let result_counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let result_sums = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let result_mins = result
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let result_maxes = result
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(
            result.num_rows(),
            reference.len(),
            "group count mismatch: expected {} distinct groups",
            reference.len()
        );

        // Order-independent: build both sides as sets keyed by group,
        // since group_by's output row order has never been guaranteed
        // (see the design doc) -- every amount here is a small integer
        // (i % 997), so sum/min/max are always exact integers with no
        // fractional part, making the f64 -> i64 cast below lossless.
        let mut got: std::collections::HashSet<(String, i64, i64, i64, i64)> =
            std::collections::HashSet::new();
        for i in 0..result.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            let sum = result_sums.value(i) as i64;
            #[allow(clippy::cast_possible_truncation)]
            let min = result_mins.value(i) as i64;
            #[allow(clippy::cast_possible_truncation)]
            let max = result_maxes.value(i) as i64;
            got.insert((
                result_categories.value(i).to_string(),
                result_counts.value(i),
                sum,
                min,
                max,
            ));
        }
        let expected: std::collections::HashSet<(String, i64, i64, i64, i64)> = reference
            .into_iter()
            .map(|(cat, (count, sum, min, max))| (cat, count, sum, min, max))
            .collect();

        assert_eq!(
            got, expected,
            "high-cardinality grouped output must exactly match the naive reference"
        );
    }
```

- [ ] **Step 2: Run it against today's (pre-rewrite) implementation**

Run: `cargo test -p strata-query --lib group_by::tests::high_cardinality_grouping_matches_a_naive_reference_including_hash_collisions`
Expected: PASS. This confirms two things before the risky rewrite starts: the fixture itself is correct, and today's implementation is correct at this scale — so this test is a trustworthy regression harness for Task 3, not a test that happens to only pass after the rewrite.

- [ ] **Step 3: Run the full existing suite to confirm nothing else broke**

Run: `cargo test -p strata-query --lib`
Expected: PASS — every existing test plus the new one.

- [ ] **Step 4: Commit**

```bash
git add crates/query/src/group_by.rs
git commit -m "test(query): add high-cardinality GROUP BY differential test ahead of the Phase A rewrite"
```

---

### Task 3: Implement the `HashMap<Row<'_>, usize>` + `ColumnarAccumulator` rewrite

**Files:**
- Modify: `crates/query/src/group_by.rs:1-241` (everything from the module doc comment through the end of `finish_agg_column` — the whole production-code portion of the file, not `mod tests`)

**Interfaces:**
- Consumes: nothing new — same inputs as today's `group_by`.
- Produces: `group_by(batch: &RecordBatch, group_cols: &[&str], aggs: &[(&str, AggFunc)]) -> Result<RecordBatch, ArrowError>` — **identical signature**, consumed by every existing caller (`crates/query/src/lib.rs`'s `pub use group_by::{AggFunc, group_by};`, both bench files, the full `mod tests` block) with zero changes required at any call site.

- [ ] **Step 1: Replace lines 1–241 of `crates/query/src/group_by.rs`**

Replace everything from the top of the file through the end of `finish_agg_column` (i.e., everything before `#[cfg(test)]`) with:

```rust
//! Hash-based `GROUP BY`, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
//!
//! Internals per
//! `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md`:
//! a `HashMap<Row<'_>, usize>` index over the `Rows` buffer already built
//! for the whole batch, instead of a fresh `OwnedRow` heap allocation per
//! row, plus columnar (`Vec<T>`-per-`group_idx`) accumulator state instead
//! of one small heap-allocated `Vec<Accumulator>` per group.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::row::{Row, RowConverter, SortField};

/// Which aggregate to compute over a column within each group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The result of an aggregation, preserving type information end-to-end.
#[derive(Debug, Clone, Copy)]
enum AggValue {
    Int(i64),
    Float(f64),
}

/// Dense, group-indexed aggregate state for one `(column, AggFunc)` pair:
/// all groups' running state for one aggregate lives in one contiguous,
/// type-specialized vector, indexed by `group_idx`, instead of one small
/// `Accumulator` instance existing per group. Same identity values and
/// update/finish math as the scalar accumulator this replaces.
#[derive(Debug)]
enum ColumnarAccumulator {
    Count(Vec<u64>),
    Sum(Vec<f64>),
    Min(Vec<f64>),
    Max(Vec<f64>),
    Avg { sum: Vec<f64>, count: Vec<u64> },
}

impl ColumnarAccumulator {
    fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count(Vec::new()),
            AggFunc::Sum => Self::Sum(Vec::new()),
            AggFunc::Min => Self::Min(Vec::new()),
            AggFunc::Max => Self::Max(Vec::new()),
            AggFunc::Avg => Self::Avg {
                sum: Vec::new(),
                count: Vec::new(),
            },
        }
    }

    /// Appends this variant's identity element -- called for every
    /// requested aggregate the moment a new group is discovered, so
    /// `group_idx` is always a valid index into every `ColumnarAccumulator`
    /// afterward, regardless of which columns are null on the row that
    /// discovered the group.
    fn push_identity(&mut self) {
        match self {
            Self::Count(v) => v.push(0),
            Self::Sum(v) => v.push(0.0),
            Self::Min(v) => v.push(f64::INFINITY),
            Self::Max(v) => v.push(f64::NEG_INFINITY),
            Self::Avg { sum, count } => {
                sum.push(0.0);
                count.push(0);
            }
        }
    }

    fn update(&mut self, group_idx: usize, value: f64) {
        match self {
            Self::Count(v) => v[group_idx] += 1,
            Self::Sum(v) => v[group_idx] += value,
            Self::Min(v) => v[group_idx] = v[group_idx].min(value),
            Self::Max(v) => v[group_idx] = v[group_idx].max(value),
            Self::Avg { sum, count } => {
                sum[group_idx] += value;
                count[group_idx] += 1;
            }
        }
    }

    /// Consumes the whole vector at once, mapping every group's finished
    /// value -- same per-variant math as the scalar accumulator's
    /// `finish()` this replaces.
    fn finish_all(self) -> Vec<AggValue> {
        match self {
            Self::Count(v) => v
                .into_iter()
                .map(|n| {
                    // Counts cannot realistically exceed i64::MAX in an in-memory batch.
                    #[allow(clippy::cast_possible_wrap)]
                    let n = n as i64;
                    AggValue::Int(n)
                })
                .collect(),
            Self::Sum(v) | Self::Min(v) | Self::Max(v) => {
                v.into_iter().map(AggValue::Float).collect()
            }
            Self::Avg { sum, count } => sum
                .into_iter()
                .zip(count)
                .map(|(s, c)| {
                    #[allow(clippy::cast_precision_loss)]
                    let c = c as f64;
                    AggValue::Float(s / c)
                })
                .collect(),
        }
    }
}

/// Groups `batch` by `group_cols` and computes `aggs` per group. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
///
/// Null values in an agg column are skipped, not treated as zero. **A group
/// whose agg column is entirely null is not yet handled specially** (flagged
/// by the Phase 2 whole-branch review, not fixed — out of the spec's
/// documented scope): `Min` returns `f64::INFINITY`, `Max` returns
/// `f64::NEG_INFINITY`, and `Avg` returns `NaN` for such a group, rather
/// than erroring or producing a null result cell. Callers should not rely on
/// these values being meaningful; a future revision should either emit null
/// for empty accumulations or document this as intentional.
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
        group_arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&group_arrays)?;

    // Downcast each non-Count agg array to Float64Array once, before the
    // per-row loop, instead of re-downcasting on every row of every column —
    // the concrete array type is invariant per column, only the row index
    // changes.
    let agg_float_arrays: Vec<Option<&Float64Array>> = agg_arrays
        .iter()
        .map(|(arr, func)| {
            if matches!(func, AggFunc::Count) {
                Ok(None)
            } else {
                arr.as_any()
                    .downcast_ref::<Float64Array>()
                    .map(Some)
                    .ok_or_else(|| ArrowError::CastError("expected Float64 after cast".to_string()))
            }
        })
        .collect::<Result<_, ArrowError>>()?;

    // group_index_of maps a *borrowed* row (a zero-allocation view into
    // `rows`) to its group index -- unlike a `HashMap<OwnedRow, _>`, this
    // needs no fresh heap allocation on every input row, only lazily on the
    // O(distinct groups) subset that turn out to be new. `Row<'_>` already
    // implements `Hash`/`Eq`/`Copy` purely over its byte slice (confirmed
    // against arrow-row 58.3.0's source), so no custom hashing/probing is
    // needed.
    let mut group_index_of: HashMap<Row<'_>, usize> = HashMap::new();
    let mut group_key_rows: Vec<Row<'_>> = Vec::new();
    let mut state: Vec<ColumnarAccumulator> = aggs
        .iter()
        .map(|(_, f)| ColumnarAccumulator::new(*f))
        .collect();

    for i in 0..batch.num_rows() {
        let row = rows.row(i);
        let group_idx = *group_index_of.entry(row).or_insert_with(|| {
            let idx = group_key_rows.len();
            group_key_rows.push(row);
            for acc in &mut state {
                acc.push_identity();
            }
            idx
        });
        for (agg_idx, (arr, func)) in agg_arrays.iter().enumerate() {
            if arr.is_null(i) {
                continue;
            }
            if matches!(func, AggFunc::Count) {
                state[agg_idx].update(group_idx, 0.0); // value unused by Count's update
                continue;
            }
            if let Some(col) = agg_float_arrays[agg_idx] {
                state[agg_idx].update(group_idx, col.value(i));
            }
        }
    }

    build_result_batch(group_cols, aggs, &converter, group_key_rows, state)
}

fn build_result_batch(
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
    converter: &RowConverter,
    group_key_rows: Vec<Row<'_>>,
    state: Vec<ColumnarAccumulator>,
) -> Result<RecordBatch, ArrowError> {
    let group_columns = converter.convert_rows(group_key_rows)?;

    // The output field's type comes from `group_columns` — the array
    // RowConverter::convert_rows actually produced — not from the original
    // (possibly dictionary-encoded) input array. convert_rows decodes
    // dictionary-encoded row keys back to the dictionary's plain value
    // type, so using the original array's data type here would build a
    // schema RecordBatch::try_new then rejects as a type mismatch. This
    // also means GROUP BY output on a dictionary-encoded column is
    // plain-typed (e.g. Utf8, not Dictionary(Int32, Utf8)) — matching how
    // most aggregation engines behave, since GROUP BY output rows are
    // already deduplicated and re-encoding them as a dictionary buys
    // nothing.
    let mut fields: Vec<Field> = group_cols
        .iter()
        .zip(&group_columns)
        .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
        .collect();
    let mut columns: Vec<ArrayRef> = group_columns;
    for ((name, func), acc) in aggs.iter().zip(state) {
        let (field, array) = finish_agg_column(acc.finish_all(), name, *func);
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

/// Converts one aggregate column's finished values into an Arrow field +
/// array, per [`AggFunc`]: `Count` produces `Int64`, every other `AggFunc`
/// produces `Float64` — both `unreachable!()` arms below hold because
/// `ColumnarAccumulator::finish_all()` guarantees that mapping.
fn finish_agg_column(values: Vec<AggValue>, name: &str, func: AggFunc) -> (Field, ArrayRef) {
    let field_name = format!("{name}_{func:?}").to_lowercase();
    if matches!(func, AggFunc::Count) {
        let counts: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                AggValue::Int(n) => n,
                // ColumnarAccumulator::finish_all()'s Count arm guarantees this.
                AggValue::Float(_) => {
                    unreachable!("Count aggregation should produce Int, not Float")
                }
            })
            .collect();
        (
            Field::new(field_name, DataType::Int64, false),
            Arc::new(Int64Array::from(counts)),
        )
    } else {
        let floats: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                AggValue::Float(f) => f,
                // ColumnarAccumulator::finish_all()'s non-Count arms guarantee this.
                AggValue::Int(_) => {
                    unreachable!("Non-Count aggregation should produce Float, not Int")
                }
            })
            .collect();
        (
            Field::new(field_name, DataType::Float64, false),
            Arc::new(Float64Array::from(floats)),
        )
    }
}
```

Leave `#[cfg(test)] mod tests { ... }` (everything after this point in the file) completely untouched.

- [ ] **Step 2: Build**

Run: `cargo build -p strata-query`
Expected: builds cleanly. If it doesn't, the most likely issue is a borrow-checker error around `group_index_of`/`group_key_rows`/`state` all being captured inside the `or_insert_with` closure — `Row<'_>` is `Copy` (confirmed: `#[derive(Debug, Copy, Clone)] pub struct Row<'a>` in `arrow-row-58.3.0/src/lib.rs:1441`), so `row` remains usable after being passed into `.entry(row)`; `group_key_rows` and `state` are captured by unique mutable borrow only for the closure's duration, released before the next statement borrows `state` again — this should compile as written, but if it doesn't, the fix is almost certainly reordering statements inside the closure, not restructuring the data model.

- [ ] **Step 3: Run the full existing test suite — must pass with zero test-file changes**

Run: `cargo test -p strata-query --lib`
Expected: PASS — every test in `group_by.rs`'s `mod tests`, including the Task 2 differential test, unchanged. This is the "byte-for-byte identical" gate: if any existing assertion fails, that's a real behavioral regression in this rewrite, not a test to adjust.

- [ ] **Step 4: Clippy and format**

Run: `cargo clippy -p strata-query --all-targets -- -D warnings`
Expected: clean.

Run: `cargo fmt --check -p strata-query`
Expected: clean (or run `cargo fmt -p strata-query` to fix, then re-check).

- [ ] **Step 5: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — confirms no downstream crate (there are none that call `group_by` outside `crates/query` and the bench crate, but this is the cheap way to be sure).

- [ ] **Step 6: Commit**

```bash
git add crates/query/src/group_by.rs
git commit -m "perf(query): replace GROUP BY's per-row OwnedRow allocation with a borrowed Row index and columnar accumulators"
```

---

### Task 4: Re-run the benchmark, confirm the win, record results

**Files:**
- Modify: `docs/superpowers/plans/2026-07-19-group-by-phase-a-optimization-plan.md` (this file — append the comparison note)

**Interfaces:**
- Consumes: `bench_group_by_cardinality_sweep` (Task 1), the rewritten `group_by` (Task 3).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Re-run the identical benchmark**

Run: `cargo bench -p strata-bench --bench group_by_bench -- group_by_cardinality_sweep`

Expected: all four correctness gates pass silently (proves the rewrite is correct at this scale too, independent of Task 3's unit tests), followed by Criterion's timing report for all four cases.

- [ ] **Step 2: Compare against Task 1's recorded baseline**

Read this file's Task 1 checkbox note (the four baseline numbers recorded there) and compare against this run's four numbers.

**If `sum_1000000_groups` and `count_sum_min_max_1000000_groups` both show a measured improvement** (lower time) over Task 1's baseline: proceed to Step 3.

**If either does not show improvement:** stop. Do not proceed to Step 3 or mark this plan done. Per this plan's Global Constraints, an architecturally "correct" rewrite with no measured win at the 1,000,000-group case does not satisfy this plan's success criterion — this means either the benchmark isn't actually exercising the allocation-heavy path as intended, or the rewrite has an unexpected inefficiency (e.g., `HashMap<Row<'_>, usize>`'s hashing cost is higher than expected relative to the eliminated `OwnedRow` allocation). Investigate via `cargo bench -p strata-bench --bench group_by_bench -- group_by_cardinality_sweep -- --profile-time 10` or by re-reading Task 3's implementation against §3 of the design doc before considering any further change — do not weaken this task's stop condition to force a "done" state.

- [ ] **Step 3: Record the comparison**

Append a note under this task's checkbox in this plan file, e.g.:

```markdown
- [x] Task 4: confirmed 2026-XX-XX —
      sum_1000000_groups: <baseline> -> <after>,
      count_sum_min_max_1000000_groups: <baseline> -> <after>
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-07-19-group-by-phase-a-optimization-plan.md
git commit -m "docs(plan): record GROUP BY Phase A benchmark results, confirmed win at 1M groups"
```

Include the full before/after numbers for all four cases in the commit message body.

---

### Task 5: Full workspace verification and review

**Files:** none (verification only).

**Interfaces:** none.

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: clean, no warnings.

- [ ] **Step 2: Full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 3: Full workspace clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Full workspace format check**

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 5: Dispatch the `reviewer` subagent**

Review the full diff this plan produced: `git diff <base-branch>...HEAD -- crates/query/src/group_by.rs bench/benches/group_by_bench.rs`. Per this project's standing rule, no task in this plan is done until reviewed — address any findings (fix and re-run Steps 1–4) before considering this plan complete. If the reviewer has no findings, note that explicitly rather than leaving review status ambiguous.

- [ ] **Step 6: Commit any review-driven fixes**

Only if Step 5 produced changes:

```bash
git add -A
git commit -m "fix(query): address reviewer findings on GROUP BY Phase A rewrite"
```
