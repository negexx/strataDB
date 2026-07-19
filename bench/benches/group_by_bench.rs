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
