//! Recall-vs-segment-count experiment — Scope Addendum v2 §7.2 / §8.2, the
//! gating de-risk for [ADR 0008](../../docs/decisions/0008-adopt-segmented-index-layout.md).
//!
//! The segmented index layout replaces one large mutable HNSW with K immutable
//! segments; a query fans out across all K and merges. The addendum names the
//! open risk: "a real recall-per-millisecond penalty versus one large graph."
//! This measures that penalty directly, against Strata's own `crates/index`
//! graph and the configured fixture or deterministic synthetic vectors.
//!
//! Method, holding Strata's production HNSW params (M=16, ef_construction=100,
//! ef_search=32, k=10) fixed:
//!   - K=1 is the one-segment comparison baseline.
//!   - For each K, the same N vectors are split into K contiguous segments
//!     (modelling K un-compacted time-ordered delta segments), each its own
//!     HNSW graph. A query searches all K for top-k, merges the K·k candidates,
//!     takes the global top-k. ef_search is held constant *per segment*, so each
//!     segment gets a full-quality search and the fan-out shows up as latency.
//!   - recall@10 is measured against exact brute-force ground truth (same for
//!     every K), so it isolates the segmentation effect from ANN error.
//!
//! Reads as: does recall hold as segments accumulate (compaction only needs to
//! bound *latency*), or does it collapse (compaction is load-bearing for
//! *correctness*, a much harder constraint)?
//!
//! ```text
//! cargo bench --bench segment_recall_bench
//! STRATA_SEG_ROWS=50000 STRATA_SEG_QUERIES=200 cargo bench --bench segment_recall_bench
//! STRATA_BENCH_SOURCE=synthetic STRATA_BENCH_SEED=20260801 \
//!   STRATA_SEG_ROWS=256 STRATA_SEG_QUERIES=16 cargo bench -p strata-bench --bench segment_recall_bench
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::doc_markdown
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use arrow::array::{FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strata_txn::Dataset;

// ---- peak-live memory tracker --------------------------------------------

struct Counting;
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

// SAFETY: pure passthrough to the system allocator plus relaxed atomic
// bookkeeping; pointers/layouts are the system allocator's own.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let live = LIVE.fetch_add(l.size() as i64, Ordering::Relaxed) + l.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}
fn peak_over_now() -> i64 {
    PEAK.load(Ordering::Relaxed) - LIVE.load(Ordering::Relaxed)
}

// ---- dataset -------------------------------------------------------------

const DATASET_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/dbpedia-openai-100k.parquet"
);
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";
const DIM: usize = 512;

// Strata production HNSW defaults (crates/txn/src/dataset.rs).
const M: usize = 16;
const EF_CONSTRUCTION: usize = 100;
const MAX_LAYER: usize = 16;
const EF_SEARCH: usize = 32;
const K: usize = 10;
const DEFAULT_SYNTHETIC_SEED: u64 = 20_260_801;

fn load_path(path: &Path, limit: usize) -> Vec<Vec<f32>> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
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

fn synthetic(limit: usize, mut state: u64) -> Vec<Vec<f32>> {
    (0..limit)
        .map(|_| {
            (0..DIM)
                .map(|dimension| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let unit = (state >> 40) as f32 / ((1_u64 << 24) - 1) as f32;
                    unit + dimension as f32 * 0.000_001
                })
                .collect()
        })
        .collect()
}

fn input_vectors(limit: usize) -> (Vec<Vec<f32>>, String) {
    let source = std::env::var("STRATA_BENCH_SOURCE").unwrap_or_else(|_| "auto".to_owned());
    let seed = std::env::var("STRATA_BENCH_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SYNTHETIC_SEED);
    let fixture = std::env::var_os("STRATA_BENCH_FIXTURE").map_or_else(
        || std::path::PathBuf::from(DATASET_PATH),
        std::path::PathBuf::from,
    );
    match source.as_str() {
        "synthetic" => (synthetic(limit, seed), format!("synthetic seed={seed}")),
        "fixture" | "auto" if fixture.is_file() => (
            load_path(&fixture, limit),
            format!("fixture {}", fixture.display()),
        ),
        "fixture" => panic!("STRATA_BENCH_SOURCE=fixture requires {}", fixture.display()),
        "auto" => (
            synthetic(limit, seed),
            format!(
                "synthetic fallback seed={seed} (fixture absent: {})",
                fixture.display()
            ),
        ),
        other => {
            panic!("unknown STRATA_BENCH_SOURCE={other:?}; expected auto, fixture, or synthetic")
        }
    }
}

fn vectors_hash(vectors: &[Vec<f32>]) -> u64 {
    vectors
        .iter()
        .flatten()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, value| {
            value
                .to_bits()
                .to_le_bytes()
                .iter()
                .fold(hash, |hash, byte| {
                    (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
                })
        })
}

fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Exact top-K row-ids for `query` over all vectors — the ground truth recall
/// is scored against.
fn exact_top_k(query: &[f32], vectors: &[Vec<f32>]) -> HashSet<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (sq_l2(query, v), i as u64))
        .collect();
    scored.select_nth_unstable_by(K - 1, |a, b| a.0.total_cmp(&b.0));
    scored[..K].iter().map(|(_, id)| *id).collect()
}

fn dataset_schema() -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector", DataType::FixedSizeList(item, DIM as i32), false),
    ]))
}

fn batch_for(vectors: &[Vec<f32>], lo: usize, hi: usize, schema: &SchemaRef) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let ids = Int64Array::from((lo..hi).map(|id| id as i64).collect::<Vec<_>>());
    let flat: Vec<f32> = vectors[lo..hi].iter().flatten().copied().collect();
    let values = Arc::new(Float32Array::from(flat));
    let vector = FixedSizeListArray::new(item, DIM as i32, values, None);
    RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vector)]).unwrap()
}

fn main() {
    let n: usize = std::env::var("STRATA_SEG_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let q_n: usize = std::env::var("STRATA_SEG_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let k_sweep = [1usize, 2, 4, 8, 16, 32, 64];

    let (vectors, source) = input_vectors(n);
    let n = vectors.len();
    eprintln!(
        "loaded {n} rows from {source}; input hash={:016x}",
        vectors_hash(&vectors)
    );
    let queries: Vec<Vec<f32>> = vectors.iter().take(q_n).cloned().collect();
    eprintln!(
        "computing exact ground truth for {} queries...",
        queries.len()
    );
    let truth: Vec<HashSet<u64>> = queries.iter().map(|q| exact_top_k(q, &vectors)).collect();

    println!(
        "\n==== recall vs segment count — {n} rows x {DIM}-dim, k={K}, ef_search={EF_SEARCH} ===="
    );
    println!(
        "production HNSW parameters: M={M}, ef_construction={EF_CONSTRUCTION}, max_layer={MAX_LAYER}"
    );
    println!("(K=1 is the one-segment comparison baseline)\n");
    println!(
        "{:>4}  {:>10}  {:>12}  {:>11}  {:>9}  {:>11}",
        "K", "recall@10", "us/query", "QPS", "build s", "peak mem"
    );
    println!("{}", "-".repeat(70));

    let mut rows: Vec<(usize, f64, f64, f64)> = Vec::new(); // K, recall, us/query, qps
    for &k_seg in &k_sweep {
        if k_seg > n {
            continue;
        }
        reset_peak();
        // Contiguous partition — models time-ordered delta segments.
        let per = n.div_ceil(k_seg);
        let dir = std::env::temp_dir().join(format!(
            "strata-segment-recall-{}-{k_seg}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = dataset_schema();
        let dataset = Dataset::create(&dir, schema.clone()).unwrap();
        let t_build = Instant::now();
        for segment in 0..k_seg {
            let lo = segment * per;
            let hi = ((segment + 1) * per).min(n);
            let mut txn = dataset.begin();
            txn.insert(batch_for(&vectors, lo, hi, &schema)).unwrap();
            txn.commit().unwrap();
        }
        drop(dataset);
        let dataset = Dataset::open(&dir).unwrap();
        let snapshot = dataset.snapshot();
        let build_s = t_build.elapsed().as_secs_f64();
        let peak = peak_over_now();

        // Warm one query, then time the sweep.
        let _ = snapshot.vector_search(&queries[0], K, None).unwrap();
        let t_q = Instant::now();
        let mut recall_sum = 0.0f64;
        for (qi, query) in queries.iter().enumerate() {
            let got: HashSet<u64> = snapshot
                .vector_search(query, K, None)
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
        let qps = queries.len() as f64 / q_elapsed;

        println!(
            "{:>4}  {:>10.4}  {:>10.1}us  {:>11.0}  {:>9.1}  {:>9.1}MB",
            k_seg,
            recall,
            us_per,
            qps,
            build_s,
            peak as f64 / (1024.0 * 1024.0)
        );
        rows.push((k_seg, recall, us_per, qps));
        drop(snapshot);
        drop(dataset);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- verdict ---------------------------------------------------------
    let base = rows.first().copied().unwrap_or((1, 0.0, 0.0, 0.0));
    let last = rows.last().copied().unwrap_or(base);
    let recall_drop = base.1 - last.1;
    let latency_mult = if base.2 > 0.0 {
        last.2 / base.2
    } else {
        f64::NAN
    };
    println!(
        "\none-segment baseline (K=1):  recall {:.4}, {:.1} us/query",
        base.1, base.2
    );
    println!(
        "at K={}:            recall {:.4} ({:+.4}), {:.1} us/query ({:.1}x)",
        last.0, last.1, -recall_drop, last.2, latency_mult
    );
    println!();
    if recall_drop <= 0.02 {
        println!(
            "VERDICT: recall holds across segmentation (drop {:.1}pp at K={}).",
            recall_drop * 100.0,
            last.0
        );
        println!("  The penalty is LATENCY, not recall — fan-out costs {latency_mult:.1}x here.");
        println!("  This bounded sample shows a latency cost without a recall drop.");
        println!("  It is evidence for the current fixture, not a universal guarantee.");
    } else {
        println!(
            "VERDICT: recall DROPS {:.1}pp by K={} — segmentation costs correctness, not just latency.",
            recall_drop * 100.0,
            last.0
        );
        println!("  This bounded sample shows a recall decrease as segments fan out.");
        println!("  It is a signal for follow-up measurement, not a universal guarantee.");
    }
    println!(
        "\nCaveats: contiguous partition (time-ordered segments); ef_search held constant per"
    );
    println!("segment, so latency is the honest fan-out cost, not a recall/latency trade. A");
    println!(
        "latency-budgeted variant (shrink ef as K grows) would trade some of this recall back"
    );
    println!("for speed — worth a follow-up if the verdict is latency-bound.");
}
