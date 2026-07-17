// bench/benches/vector_search_bench.rs
//! Phase 4 exit-criterion benchmark: recall@10 and QPS for
//! `Dataset::vector_search`, correctness-gated against
//! `strata_index::brute_force_search` before any timing is trusted — same
//! discipline as `group_by_bench.rs` (Phase 2). See
//! `.claude/docs/design/phase-4-vector-index-spec.md` §6.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strata_index::brute_force_search;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

// `cargo bench` runs the compiled binary with its working directory set to
// the package's manifest directory (`bench/`), not the workspace root, so
// this is anchored on `CARGO_MANIFEST_DIR` rather than a path relative to
// wherever the invoking shell happened to be.
const DATASET_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/dbpedia-openai-100k.parquet"
);
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";
const VECTOR_DIM: usize = 512;
const VECTOR_DIM_I32: i32 = 512;
const NUM_QUERIES: usize = 100;
const RECALL_K: usize = 10;

/// Loads up to `limit` (vector, category) pairs from the downloaded Parquet
/// file. `category` is synthesized from the row's position (row % 10) —
/// the real dataset has no natural low-cardinality column, and Phase 4's
/// filtered-search benchmark scenario needs one to exercise `ef` widening
/// against a real (not synthetic-vector) embedding set.
///
/// The embedding column is `List<Float64>` on disk (confirmed against the
/// downloaded file's actual Arrow schema via `cargo run --example
/// inspect_parquet` — the brief's assumption of `Float32` values was
/// wrong; `HuggingFace`'s parquet auto-conversion stores Python floats as
/// float64), so each value is narrowed to `f32` here to match
/// `Dataset`'s `FixedSizeList<Float32>` vector column contract.
#[allow(clippy::cast_possible_truncation)]
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
            let values: &Float64Array = values
                .as_any()
                .downcast_ref()
                .expect("embedding values must be f64");
            let vector: Vec<f32> = values.values().iter().map(|v| *v as f32).collect();
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
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                VECTOR_DIM_I32,
            ),
            false,
        ),
    ]));
    let ds = Dataset::create(dir).unwrap();

    let ids: Vec<i64> = rows.iter().map(|(_, cat)| *cat).collect();
    let id_arr = Arc::new(Int64Array::from(ids));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let flat: Vec<f32> = rows.iter().flat_map(|(v, _)| v.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let vec_arr = Arc::new(FixedSizeListArray::new(
        item_field,
        VECTOR_DIM_I32,
        values,
        None,
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_arr, vec_arr]).unwrap();

    let mut txn = ds.begin();
    txn.insert(batch);
    txn.commit().unwrap();
    ds
}

/// Ground truth via brute force, and the correctness gate: HNSW's
/// recall@10 against it must clear a floor before any QPS number is
/// trusted (mirrors `group_by_bench.rs`'s `check_correctness`).
fn check_recall(ds: &Dataset, rows: &[(Vec<f32>, i64)], queries: &[Vec<f32>]) -> f64 {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            VECTOR_DIM_I32,
        ),
        false,
    )]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let flat: Vec<f32> = rows.iter().flat_map(|(v, _)| v.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let vec_arr = Arc::new(FixedSizeListArray::new(
        item_field,
        VECTOR_DIM_I32,
        values,
        None,
    ));
    let batch = RecordBatch::try_new(schema, vec![vec_arr]).unwrap();
    let vectors = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();

    // One snapshot shared across every query in this recall check, since
    // they're all reading the same immutable, already-built dataset.
    let snapshot = ds.snapshot();
    let mut hits = 0usize;
    for query in queries {
        let exact: std::collections::HashSet<usize> = brute_force_search(vectors, query, RECALL_K)
            .unwrap()
            .into_iter()
            .map(|n| n.row_index)
            .collect();
        let approx: std::collections::HashSet<u64> = snapshot
            .vector_search(query, RECALL_K, None)
            .unwrap()
            .into_iter()
            .map(|m| m.row_id)
            .collect();
        hits += approx
            .iter()
            .filter(|row_id| usize::try_from(**row_id).is_ok_and(|id| exact.contains(&id)))
            .count();
    }
    #[allow(clippy::cast_precision_loss)]
    let recall = hits as f64 / (queries.len() * RECALL_K) as f64;
    recall
}

fn bench_vector_search(c: &mut Criterion) {
    let rows = load_vectors(100_000);
    assert_eq!(
        rows[0].0.len(),
        VECTOR_DIM,
        "loaded vectors must match the expected dimensionality"
    );

    let dir = std::env::temp_dir().join(format!("strata-vector-bench-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let ds = build_dataset(&dir, &rows);

    let queries: Vec<Vec<f32>> = rows
        .iter()
        .take(NUM_QUERIES)
        .map(|(v, _)| v.clone())
        .collect();

    let recall = check_recall(&ds, &rows, &queries);
    println!(
        "recall@{RECALL_K} = {recall:.4} (over {NUM_QUERIES} queries, {} indexed vectors)",
        rows.len()
    );
    assert!(
        recall > 0.8,
        "recall@{RECALL_K} = {recall:.4} is too low to trust the QPS numbers below it"
    );

    // One snapshot shared across both benchmark closures below — neither
    // times snapshot acquisition itself, only vector_search, and both read
    // the same never-mutated dataset.
    let snapshot = ds.snapshot();

    let mut group = c.benchmark_group("vector_search");
    group.bench_function("unfiltered_top_10", |b| {
        b.iter(|| {
            let query = &queries[0];
            snapshot
                .vector_search(std::hint::black_box(query), RECALL_K, None)
                .unwrap()
        });
    });

    let predicate = Predicate::Eq("id".to_string(), Value::Int64(3));
    group.bench_function("filtered_top_10_one_of_ten_categories", |b| {
        b.iter(|| {
            let query = &queries[0];
            snapshot
                .vector_search(std::hint::black_box(query), RECALL_K, Some(&predicate))
                .unwrap()
        });
    });
    group.finish();

    std::fs::remove_dir_all(&dir).ok();
}

criterion_group!(benches, bench_vector_search);
criterion_main!(benches);
