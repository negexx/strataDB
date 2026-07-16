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
    // ROW_COUNT is a fixed 10_000_000-row constant, far below i64::MAX;
    // usize -> i64 cannot wrap here.
    let categories: Vec<String> = (0..ROW_COUNT)
        .map(|i| {
            #[allow(clippy::cast_possible_wrap)]
            let i = i as i64;
            format!("cat-{}", i % DISTINCT_CATEGORIES)
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
        .downcast_ref::<arrow::array::Float64Array>()
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
