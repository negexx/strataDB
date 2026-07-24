//! `ef_construction` sweep — pure measurement for the graph-construction-cost
//! effort (ingest+recovery performance audit, 2026-07-24). `ef_construction`
//! is the HNSW build-cost dial (`crates/txn/src/dataset.rs`'s
//! `HNSW_EF_CONSTRUCTION`, currently 200): the insert-time saturation
//! early-exit is disabled, so every build traversal runs the full beam,
//! making build cost near-linear in `ef_construction`.
//!
//! This isolates the HNSW build itself (via `strata_index::HnswIndex`
//! directly — no commit/fsync/Arrow-encode overhead) and measures recall@10
//! against exact brute-force ground truth for each candidate, at the same
//! M=16 / `max_layers`=16 production defaults
//! (`crates/txn/src/dataset.rs:739-741`). Isolating the build this way is
//! valid because the audit measured ingest+commit as ~95% HNSW graph
//! construction — everything else is single-digit-percent noise this sweep
//! doesn't need to pay for on every candidate.
//!
//! ```text
//! cargo bench --bench ef_construction_sweep_bench
//! STRATA_EF_SWEEP_ROWS=50000 STRATA_EF_SWEEP_QUERIES=200 cargo bench --bench ef_construction_sweep_bench
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::doc_markdown
)]

use std::collections::HashSet;
use std::time::Instant;

use arrow::array::Float64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};

const DATASET_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/dbpedia-openai-100k.parquet"
);
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";

// Strata production HNSW defaults other than ef_construction itself
// (crates/txn/src/dataset.rs:739-740).
const M: usize = 16;
const MAX_LAYER: usize = 16;
const EF_SEARCH: usize = 32;
const K: usize = 10;

const EF_CANDIDATES: [usize; 4] = [50, 100, 150, 200];

fn load(limit: usize) -> Vec<Vec<f32>> {
    let file = std::fs::File::open(DATASET_PATH)
        .unwrap_or_else(|e| panic!("open {DATASET_PATH}: {e}. Download the dataset first."));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::with_capacity(limit);
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.schema_ref().index_of(EMBEDDING_COLUMN).unwrap();
        let list = batch
            .column(col)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if out.len() >= limit {
                return out;
            }
            let v = list.value(i);
            let v: &Float64Array = v.as_any().downcast_ref().unwrap();
            out.push(v.values().iter().map(|x| *x as f32).collect());
        }
    }
    out
}

fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn exact_top_k(query: &[f32], vectors: &[Vec<f32>]) -> HashSet<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (sq_l2(query, v), i as u64))
        .collect();
    scored.select_nth_unstable_by(K - 1, |a, b| a.0.total_cmp(&b.0));
    scored[..K].iter().map(|(_, id)| *id).collect()
}

fn build_monolithic(vectors: &[Vec<f32>], ef_construction: usize) -> (HnswIndex, f64) {
    let idx = HnswIndex::new(
        MaxConnections(M),
        MaxElements(vectors.len().max(1)),
        MaxLayers(MAX_LAYER),
        EfConstruction(ef_construction),
    )
    .unwrap();
    let t = Instant::now();
    for (id, v) in vectors.iter().enumerate() {
        idx.insert(id as u64, v).unwrap();
    }
    (idx, t.elapsed().as_secs_f64())
}

fn main() {
    let n: usize = std::env::var("STRATA_EF_SWEEP_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25_000);
    let q_n: usize = std::env::var("STRATA_EF_SWEEP_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    eprintln!("loading {n} rows...");
    let vectors = load(n);
    let n = vectors.len();
    let queries: Vec<Vec<f32>> = vectors.iter().take(q_n).cloned().collect();
    eprintln!(
        "computing exact ground truth for {} queries...",
        queries.len()
    );
    let truth: Vec<HashSet<u64>> = queries.iter().map(|q| exact_top_k(q, &vectors)).collect();

    println!(
        "\n==== ef_construction sweep — {n} rows x 512-dim, M={M}, k={K}, ef_search={EF_SEARCH} ===="
    );
    println!("(ef_construction=200 is the current production default — dataset.rs:741)\n");
    println!(
        "{:>4}  {:>10}  {:>12}  {:>11}  {:>10}",
        "ef", "recall@10", "build s", "rows/s", "us/query"
    );
    println!("{}", "-".repeat(58));

    for &ef in &EF_CANDIDATES {
        let (idx, build_s) = build_monolithic(&vectors, ef);

        // Warm one query, then time the sweep.
        let _ = idx.search(&queries[0], K, EF_SEARCH, |_| true).unwrap();
        let t_q = Instant::now();
        let mut recall_sum = 0.0f64;
        for (qi, query) in queries.iter().enumerate() {
            let got: HashSet<u64> = idx
                .search(query, K, EF_SEARCH, |_| true)
                .unwrap()
                .into_iter()
                .map(|m| m.row_id)
                .collect();
            let hit = got.intersection(&truth[qi]).count();
            recall_sum += hit as f64 / K as f64;
        }
        let q_elapsed = t_q.elapsed().as_secs_f64();
        let recall = recall_sum / queries.len() as f64;
        let us_per = q_elapsed * 1e6 / queries.len() as f64;

        println!(
            "{:>4}  {:>10.4}  {:>10.2}s  {:>10.0}  {:>9.1}us",
            ef,
            recall,
            build_s,
            n as f64 / build_s,
            us_per
        );
    }

    println!(
        "\nNote: build isolates strata_index::HnswIndex only (no commit/fsync/Arrow-encode) —"
    );
    println!(
        "the audit measured ingest+commit as ~95% HNSW graph construction, so this is the term"
    );
    println!("that matters. Cross-check the chosen candidate end-to-end with lifecycle_bench.");
}
