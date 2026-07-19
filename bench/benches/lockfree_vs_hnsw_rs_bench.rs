// bench/benches/lockfree_vs_hnsw_rs_bench.rs
//! Recall@k and QPS comparison: the new lock-free `strata_index::graph::Graph`
//! vs. `hnsw_rs` — the empirical evidence for the lock-free HNSW rewrite's
//! stated success bar (match or beat `hnsw_rs`), per
//! `docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md`.
//!
//! Both indexes are graded against `strata_index::brute_force_search` as
//! true ground truth, not against each other — this directly answers
//! whether Graph's recall@k is >= `hnsw_rs`'s, which "vs. `hnsw_rs`'s own
//! results as ground truth" alone would not (two implementations can agree
//! with each other while both being wrong, or diverge without telling you
//! which one is closer to correct).
//!
//! `load_vectors` duplicates `vector_search_bench.rs`'s loader rather than
//! sharing it — each file under `benches/` compiles as an independent
//! binary, and this project doesn't have a shared bench-helpers crate; one
//! small ~20-line loader duplicated across two files isn't worth adding one
//! for.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, FixedSizeListArray, Float32Array, Float64Array};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use hnsw_rs::prelude::{DistL2, Hnsw};
use strata_index::brute_force_search;
use strata_index::distance::{Distance, L2};
use strata_index::graph::Graph;

/// Wraps `L2` to count `eval()` calls — used to answer "how much of a
/// search's time is plausibly distance computation vs. everything else
/// (traversal, heap/hashset allocation, CAS)?" without OS-level profiler
/// tooling. This machine is Windows, where `perf`/`cargo-flamegraph` (the
/// usual answer) aren't reliably available; multiplying this count by the
/// isolated per-call cost from `l2_distance_eval_only` below gives an
/// upper-bound estimate of distance-only time per search, comparable by
/// hand against this same run's `graph_top_10` criterion result.
struct CountingL2 {
    calls: Arc<AtomicUsize>,
}

impl Distance for CountingL2 {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        self.calls.fetch_add(1, Ordering::Relaxed);
        L2.eval(a, b)
    }
}

const DATASET_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/dbpedia-openai-100k.parquet"
);
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";
const VECTOR_DIM: usize = 512;
const VECTOR_DIM_I32: i32 = 512;
const N: usize = 10_000;
const NUM_QUERIES: usize = 100;
const K: usize = 10;

/// Loads up to `limit` embedding vectors from the downloaded Parquet file.
/// Trimmed copy of `vector_search_bench.rs`'s `load_vectors` (category
/// synthesis dropped — this bench has no filtered-search scenario).
#[allow(clippy::cast_possible_truncation)]
fn load_vectors(limit: usize) -> Vec<Vec<f32>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
            out.push(values.values().iter().map(|v| *v as f32).collect());
        }
    }
    out
}

fn to_fixed_size_list(vectors: &[Vec<f32>]) -> FixedSizeListArray {
    let item_field = Arc::new(Field::new(
        "item",
        arrow::datatypes::DataType::Float32,
        false,
    ));
    let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    FixedSizeListArray::new(item_field, VECTOR_DIM_I32, values, None)
}

/// Deterministic seeded pseudo-random `unif` in `(0, 1)`, keyed by `seed` —
/// `SplitMix64` mixing, same construction as
/// `graph::tests::concurrent_inserts_are_all_findable_afterward`'s
/// `test_unif` and production `HnswIndex::insert`'s own `unif` derivation.
/// **Not decorative**: a fixed `unif` (this file's original version used a
/// constant `0.5` for every row) makes `assign_level` return `0` for every
/// single node — a flat, single-layer graph that never exercises
/// `k_nn_search`'s multi-layer descent loop, so the recall/QPS numbers
/// below would characterize a configuration production never actually
/// builds (found in final whole-branch review).
#[allow(clippy::cast_precision_loss)]
fn bench_unif(seed: u64) -> f64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    ((z >> 11) as f64 / (1u64 << 53) as f64).max(f64::EPSILON)
}

/// Recall@`K` of `found` against `truth`, both already capped to `K`.
fn recall_at_k(found: &[u64], truth: &std::collections::HashSet<u64>) -> f64 {
    let hits = found.iter().filter(|id| truth.contains(id)).count();
    #[allow(clippy::cast_precision_loss)]
    let recall = hits as f64 / truth.len().max(1) as f64;
    recall
}

/// Builds a second, identical graph (same vectors, same `bench_unif`
/// sequence, so the topology is deterministic and matches the main
/// benchmark's `graph` exactly) wired through a call-counting `Distance`,
/// then prints how many `eval()` calls one real `k_nn_search` performs on
/// average. Multiply by `l2_distance_eval_only`'s reported time for an
/// upper-bound estimate of distance-only time per search, and compare that
/// by hand against `graph_top_10`'s reported time.
fn print_distance_calls_per_search(vectors: &[Vec<f32>], queries: &[Vec<f32>], m_l: f64) {
    let call_counter = Arc::new(AtomicUsize::new(0));
    let counting_graph = Graph::new(
        CountingL2 {
            calls: Arc::clone(&call_counter),
        },
        vectors.len(),
    );
    for (i, v) in vectors.iter().enumerate() {
        counting_graph
            .insert(
                i as u64,
                v.clone(),
                16,
                32,
                16,
                200,
                m_l,
                bench_unif(i as u64),
            )
            .unwrap();
    }
    call_counter.store(0, Ordering::Relaxed);
    for query in queries {
        counting_graph.k_nn_search(query, K, 50, |_| true).unwrap();
    }
    #[allow(clippy::cast_precision_loss)]
    let avg_distance_calls_per_search =
        call_counter.load(Ordering::Relaxed) as f64 / queries.len() as f64;
    println!(
        "avg distance eval() calls per k_nn_search (ef=50, {} queries): {avg_distance_calls_per_search:.1}",
        queries.len()
    );
    println!(
        "  -> multiply by l2_distance_eval_only's reported time below for an upper-bound \
         estimate of distance-only time per search; compare that by hand against \
         graph_top_10's reported time to see what fraction it explains"
    );
}

fn bench_lockfree_vs_hnsw_rs(c: &mut Criterion) {
    let vectors = load_vectors(N);
    assert_eq!(
        vectors[0].len(),
        VECTOR_DIM,
        "loaded vectors must match the expected dimensionality"
    );
    let queries: Vec<Vec<f32>> = vectors.iter().take(NUM_QUERIES).cloned().collect();

    // --- hnsw_rs baseline ---
    let hnsw_rs_index = Hnsw::new(16, N, 16, 200, DistL2 {});
    for (i, v) in vectors.iter().enumerate() {
        hnsw_rs_index.insert((v, i));
    }
    let hnsw_rs_results: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            hnsw_rs_index
                .search(q, K, 50)
                .into_iter()
                .map(|n| n.get_origin_id() as u64)
                .collect()
        })
        .collect();

    // --- new lock-free Graph ---
    let graph = Graph::new(L2, N);
    let m_l = 1.0 / 16f64.ln();
    for (i, v) in vectors.iter().enumerate() {
        graph
            .insert(
                i as u64,
                v.clone(),
                16,
                32,
                16,
                200,
                m_l,
                bench_unif(i as u64),
            )
            .unwrap();
    }
    let graph_results: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            graph
                .k_nn_search(q, K, 50, |_| true)
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        })
        .collect();

    // --- true ground truth, via brute force -- not either HNSW index's
    // own results, so this answers "is Graph's recall >= hnsw_rs's",
    // not just "do the two agree with each other".
    let vectors_arr = to_fixed_size_list(&vectors);
    let mut hnsw_rs_recall_sum = 0.0;
    let mut graph_recall_sum = 0.0;
    for (i, query) in queries.iter().enumerate() {
        let truth: std::collections::HashSet<u64> = brute_force_search(&vectors_arr, query, K)
            .unwrap()
            .into_iter()
            .map(|n| n.row_index as u64)
            .collect();
        hnsw_rs_recall_sum += recall_at_k(&hnsw_rs_results[i], &truth);
        graph_recall_sum += recall_at_k(&graph_results[i], &truth);
    }
    #[allow(clippy::cast_precision_loss)]
    let queries_len = queries.len() as f64;
    let hnsw_rs_recall = hnsw_rs_recall_sum / queries_len;
    let graph_recall = graph_recall_sum / queries_len;

    println!(
        "recall@{K} over {NUM_QUERIES} queries against {N} indexed vectors (brute-force ground truth):"
    );
    println!("  hnsw_rs: {hnsw_rs_recall:.4}");
    println!("  Graph:   {graph_recall:.4}");
    assert!(
        hnsw_rs_recall > 0.8,
        "hnsw_rs baseline recall@{K} = {hnsw_rs_recall:.4} is too low to trust this \
         comparison's premise -- fix the fixture before trusting either QPS number"
    );

    print_distance_calls_per_search(&vectors, &queries, m_l);

    let mut group = c.benchmark_group("lockfree_vs_hnsw_rs");
    group.bench_function("l2_distance_eval_only", |b| {
        let a = &vectors[0];
        let other = &vectors[1];
        b.iter(|| L2.eval(std::hint::black_box(a), std::hint::black_box(other)));
    });
    group.bench_function("hnsw_rs_top_10", |b| {
        b.iter(|| {
            let query = &queries[0];
            hnsw_rs_index
                .search(std::hint::black_box(query), K, 50)
                .into_iter()
                .map(|n| n.get_origin_id() as u64)
                .collect::<Vec<_>>()
        });
    });
    group.bench_function("graph_top_10", |b| {
        b.iter(|| {
            let query = &queries[0];
            graph
                .k_nn_search(std::hint::black_box(query), K, 50, |_| true)
                .unwrap()
        });
    });
    group.finish();
}

criterion_group!(benches, bench_lockfree_vs_hnsw_rs);
criterion_main!(benches);
