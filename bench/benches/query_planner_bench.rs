//! Direct-versus-planned evidence for Strata's supported query primitives.
//!
//! The planner delegates to the same immutable snapshot operators as the
//! direct facade. These measurements therefore quantify planning/explain
//! overhead on a fixed synthetic fixture; they are not cost-model guarantees.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use strata_txn::{
    Aggregate, AggregateFunction, Comparison, ComparisonOperator, Dataset, FilterExpression,
    FilterLiteral, GroupByRequest, Projection, ScanRequest, VectorHydration, VectorSearchRequest,
};

const ROWS_PER_COMMIT: usize = 64;
const COMMIT_COUNT: usize = 4;
const VECTOR_DIMENSION: i32 = 2;

struct Fixture {
    dir: PathBuf,
    dataset: Dataset,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                VECTOR_DIMENSION,
            ),
            false,
        ),
    ]))
}

fn batch(schema: Arc<Schema>, start: usize) -> RecordBatch {
    let ids = (start..start + ROWS_PER_COMMIT)
        .map(|id| i64::try_from(id).unwrap())
        .collect::<Vec<_>>();
    let categories = ids
        .iter()
        .map(|id| format!("category-{}", id % 4))
        .collect::<Vec<_>>();
    let amounts = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
    let values = ids
        .iter()
        .flat_map(|id| {
            [
                f32::from(i16::try_from(*id).expect("benchmark fixture ids fit in i16")),
                0.0,
            ]
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(amounts)),
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Float32, false)),
                VECTOR_DIMENSION,
                Arc::new(Float32Array::from(values)),
                None,
            )),
        ],
    )
    .unwrap()
}

fn fixture() -> Fixture {
    let dir =
        std::env::temp_dir().join(format!("strata-query-planner-bench-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let schema = schema();
    let dataset = Dataset::create(&dir, Arc::clone(&schema)).unwrap();
    for commit in 0..COMMIT_COUNT {
        let mut transaction = dataset.begin();
        transaction
            .insert(batch(Arc::clone(&schema), commit * ROWS_PER_COMMIT))
            .unwrap();
        transaction.commit().unwrap();
    }
    eprintln!(
        "query-planner fixture: commits={COMMIT_COUNT} rows={} vector_dimension={VECTOR_DIMENSION}",
        ROWS_PER_COMMIT * COMMIT_COUNT
    );
    Fixture { dir, dataset }
}

fn selective_filter() -> FilterExpression {
    FilterExpression::Compare(Comparison {
        column: "id".into(),
        operator: ComparisonOperator::GreaterThanOrEqual,
        value: FilterLiteral::Int64(192),
    })
}

fn filtered_vector_request() -> VectorSearchRequest {
    VectorSearchRequest {
        vector_column: "vector".into(),
        query: vec![200.0, 0.0],
        k: 10,
        filter: Some(selective_filter()),
        hydration: VectorHydration::Projection(Projection::Columns(vec!["id".into()])),
    }
}

fn bench_filtered_vector_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_vector_cache");
    group.bench_function("cold", |b| {
        b.iter_batched(
            || {
                let fixture = fixture();
                let snapshot = fixture.dataset.snapshot();
                (fixture, snapshot, filtered_vector_request())
            },
            |(_fixture, snapshot, request)| {
                std::hint::black_box(snapshot.vector_search_query(&request).unwrap())
            },
            BatchSize::LargeInput,
        );
    });

    let warm_fixture = fixture();
    let warm_snapshot = warm_fixture.dataset.snapshot();
    let warm_request = filtered_vector_request();
    std::hint::black_box(warm_snapshot.vector_search_query(&warm_request).unwrap());
    let warmed = warm_snapshot.live_set_cache_accounting();
    assert_eq!(
        warmed.entry_count, 1,
        "warm benchmark setup must retain the filtered vector live set"
    );
    group.bench_function("warm", |b| {
        b.iter(|| std::hint::black_box(warm_snapshot.vector_search_query(&warm_request).unwrap()));
    });
    group.finish();
}

fn bench_query_planner(c: &mut Criterion) {
    let query_fixture = fixture();
    let snapshot = query_fixture.dataset.snapshot();
    let projection = ScanRequest {
        projection: Projection::Columns(vec!["id".into(), "category".into()]),
        filter: None,
    };
    let selective_scan = ScanRequest {
        projection: Projection::Columns(vec!["id".into(), "amount".into()]),
        filter: Some(selective_filter()),
    };
    let grouped = GroupByRequest {
        group_by: vec!["category".into()],
        aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "sum")],
        filter: Some(selective_filter()),
    };
    let vector = filtered_vector_request();

    let mut group = c.benchmark_group("query_planner");
    group.bench_function("projection_scan/direct", |b| {
        b.iter(|| std::hint::black_box(snapshot.scan_query(&projection).unwrap()));
    });
    group.bench_function("projection_scan/planned", |b| {
        b.iter(|| std::hint::black_box(snapshot.execute_planned_scan_query(&projection).unwrap()));
    });
    group.bench_function("selective_predicate_scan/direct", |b| {
        b.iter(|| std::hint::black_box(snapshot.scan_query(&selective_scan).unwrap()));
    });
    group.bench_function("selective_predicate_scan/planned", |b| {
        b.iter(|| {
            std::hint::black_box(
                snapshot
                    .execute_planned_scan_query(&selective_scan)
                    .unwrap(),
            )
        });
    });
    group.bench_function("grouped_aggregation/direct", |b| {
        b.iter(|| std::hint::black_box(snapshot.group_by_query(&grouped).unwrap()));
    });
    group.bench_function("grouped_aggregation/planned", |b| {
        b.iter(|| std::hint::black_box(snapshot.execute_planned_group_by_query(&grouped).unwrap()));
    });
    group.bench_function("vector_search/direct", |b| {
        b.iter(|| std::hint::black_box(snapshot.vector_search_query(&vector).unwrap()));
    });
    group.bench_function("vector_search/planned", |b| {
        b.iter(|| {
            std::hint::black_box(
                snapshot
                    .execute_planned_vector_search_query(&vector)
                    .unwrap(),
            )
        });
    });
    group.finish();
}

fn bench_shared_handle_transaction_commit(c: &mut Criterion) {
    c.bench_function("shared_handle_transaction_commit", |b| {
        b.iter_batched(
            fixture,
            |fixture| {
                std::thread::scope(|scope| {
                    for offset in [0_i16, 1] {
                        let dataset = fixture.dataset.clone();
                        scope.spawn(move || {
                            let mut transaction = dataset.begin();
                            transaction
                                .insert(
                                    RecordBatch::try_new(
                                        schema(),
                                        vec![
                                            Arc::new(Int64Array::from(vec![i64::from(offset)])),
                                            Arc::new(StringArray::from(vec!["commit"])),
                                            Arc::new(Int64Array::from(vec![i64::from(offset)])),
                                            Arc::new(FixedSizeListArray::new(
                                                Arc::new(Field::new(
                                                    "item",
                                                    DataType::Float32,
                                                    false,
                                                )),
                                                VECTOR_DIMENSION,
                                                Arc::new(Float32Array::from(vec![
                                                    f32::from(offset),
                                                    0.0,
                                                ])),
                                                None,
                                            )),
                                        ],
                                    )
                                    .unwrap(),
                                )
                                .unwrap();
                            transaction.commit().unwrap();
                        });
                    }
                });
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    bench_query_planner,
    bench_filtered_vector_cache,
    bench_shared_handle_transaction_commit
);
criterion_main!(benches);
