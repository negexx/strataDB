//! Recall-vs-segment-count experiment — Scope Addendum v2 §7.2 / §8.2.
//!
//! The segmented index layout replaces one large mutable HNSW with K immutable
//! segments; a query fans out across all K and merges. The addendum names the
//! open question: how recall, latency, and throughput vary with K relative to
//! one segment. This measures that K-dependent behavior directly, against
//! Strata's own `crates/index` graph and the configured fixture or
//! deterministic synthetic vectors.
//!
//! Method, holding Strata's production HNSW params (M=16, ef_construction=100,
//! ef_search=32, k=10) fixed:
//!   - K=1 is the one-segment comparison baseline.
//!   - For each K, the same N vectors are split into K contiguous segments
//!     (modelling K un-compacted time-ordered delta segments), each its own
//!     HNSW graph. A query searches all K for top-k, merges the K·k candidates,
//!     takes the global top-k. ef_search is held constant *per segment*, so each
//!     segment gets a full-quality search; the benchmark reports the resulting
//!     K-dependent recall, latency, and throughput measurements.
//!   - recall@10 is measured against exact brute-force ground truth (same for
//!     every K), so it isolates the segmentation effect from ANN error.
//!
//! Read the table as a bounded K-dependent measurement: use its rows to assess
//! the run's direction, without inferring monotonic behavior from segmentation.
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
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::{Dataset, Snapshot};

// ---- peak-live memory tracker --------------------------------------------

struct Counting;
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

// SAFETY[BENCH-SEGMENT-RECALL-ALLOC-IMPL]: The allocator forwards all allocations unchanged and only records relaxed atomics.
// bookkeeping; pointers/layouts are the system allocator's own.
unsafe impl GlobalAlloc for Counting {
    // SAFETY[BENCH-SEGMENT-RECALL-ALLOC-ALLOC]: GlobalAlloc callers provide a valid allocation layout.
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // SAFETY[BENCH-SEGMENT-RECALL-ALLOC-SYSTEM-ALLOC]: The caller-provided GlobalAlloc layout is forwarded unchanged to System.
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let live = LIVE.fetch_add(l.size() as i64, Ordering::Relaxed) + l.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    // SAFETY[BENCH-SEGMENT-RECALL-ALLOC-DEALLOC]: GlobalAlloc callers provide the matching live allocation and layout.
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY[BENCH-SEGMENT-RECALL-ALLOC-SYSTEM-DEALLOC]: The caller-provided pointer and matching layout are forwarded unchanged to System.
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

fn reset_peak() -> i64 {
    let start_live = LIVE.load(Ordering::Relaxed);
    PEAK.store(start_live, Ordering::Relaxed);
    start_live
}
fn peak_over_start(start_live: i64) -> i64 {
    PEAK.load(Ordering::Relaxed) - start_live
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
const DEFAULT_WARMUP_RUNS: usize = 1;
const DEFAULT_REPETITIONS: usize = 5;

#[derive(Debug, PartialEq)]
struct Summary {
    median: f64,
    p95: f64,
}

#[derive(Debug)]
struct SearchMeasurement {
    recall: f64,
    us_per_query: f64,
    qps: f64,
}

#[derive(Debug)]
struct SearchSummary {
    recall: Summary,
    us_per_query: Summary,
    qps: Summary,
}

#[derive(Debug)]
struct SegmentRow {
    segment_count: usize,
    unfiltered: SearchSummary,
    filtered: SearchSummary,
    build_seconds: f64,
    peak_live_bytes: i64,
}

fn positive_env(name: &str, default: usize) -> usize {
    let value = std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default);
    assert!(value > 0, "{name} must be at least one");
    value
}

fn summarize(samples: &[f64]) -> Summary {
    assert!(!samples.is_empty(), "summary requires at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[sorted.len() / 2 - 1], sorted[sorted.len() / 2])
    } else {
        sorted[sorted.len() / 2]
    };
    let p95_index = (sorted.len() * 95).div_ceil(100) - 1;
    Summary {
        median,
        p95: sorted[p95_index],
    }
}

fn summarize_search(samples: &[SearchMeasurement]) -> SearchSummary {
    SearchSummary {
        recall: summarize(
            &samples
                .iter()
                .map(|sample| sample.recall)
                .collect::<Vec<_>>(),
        ),
        us_per_query: summarize(
            &samples
                .iter()
                .map(|sample| sample.us_per_query)
                .collect::<Vec<_>>(),
        ),
        qps: summarize(&samples.iter().map(|sample| sample.qps).collect::<Vec<_>>()),
    }
}

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
    exact_top_k_where(query, vectors, |_| true)
}

fn exact_top_k_where(
    query: &[f32],
    vectors: &[Vec<f32>],
    include: impl Fn(usize) -> bool,
) -> HashSet<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .filter(|(id, _)| include(*id))
        .map(|(i, v)| (sq_l2(query, v), i as u64))
        .collect();
    assert!(
        scored.len() >= K,
        "filtered workload must contain at least k rows"
    );
    scored.select_nth_unstable_by(K - 1, |a, b| a.0.total_cmp(&b.0));
    scored[..K].iter().map(|(_, id)| *id).collect()
}

fn dataset_schema() -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("segment_cohort", DataType::Int64, false),
        Field::new("vector", DataType::FixedSizeList(item, DIM as i32), false),
    ]))
}

fn cohort_for(row_id: usize, cohort_width: usize) -> i64 {
    ((row_id / cohort_width) % 2) as i64
}

fn batch_for(
    vectors: &[Vec<f32>],
    lo: usize,
    hi: usize,
    cohort_width: usize,
    schema: &SchemaRef,
) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let ids = Int64Array::from((lo..hi).map(|id| id as i64).collect::<Vec<_>>());
    let cohorts = Int64Array::from(
        (lo..hi)
            .map(|id| cohort_for(id, cohort_width))
            .collect::<Vec<_>>(),
    );
    let flat: Vec<f32> = vectors[lo..hi].iter().flatten().copied().collect();
    let values = Arc::new(Float32Array::from(flat));
    let vector = FixedSizeListArray::new(item, DIM as i32, values, None);
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(cohorts), Arc::new(vector)],
    )
    .unwrap()
}

fn measure_search(
    snapshot: &Snapshot,
    queries: &[Vec<f32>],
    truth: &[HashSet<u64>],
    predicate: Option<&Predicate>,
    allowed_ids: Option<&HashSet<u64>>,
) -> SearchMeasurement {
    let t_q = Instant::now();
    let mut recall_sum = 0.0f64;
    for (query, expected) in queries.iter().zip(truth) {
        let got: HashSet<u64> = snapshot
            .vector_search(query, K, predicate)
            .unwrap()
            .into_iter()
            .map(|match_| match_.row_id)
            .collect();
        assert_eq!(
            got.len(),
            K,
            "the measured workload expects a full top-k result"
        );
        if let Some(allowed_ids) = allowed_ids {
            assert!(
                got.is_subset(allowed_ids),
                "filtered search returned a row outside the predicate's cohort: {got:?}"
            );
        }
        recall_sum += got.intersection(expected).count() as f64 / K as f64;
    }
    let elapsed = t_q.elapsed().as_secs_f64();
    SearchMeasurement {
        recall: recall_sum / queries.len() as f64,
        us_per_query: elapsed * 1e6 / queries.len() as f64,
        qps: queries.len() as f64 / elapsed,
    }
}

fn print_search_summary(label: &str, rows: &[SegmentRow]) {
    println!("\n{label} query results (median / p95 over measured repetitions):");
    println!(
        "{:>4}  {:>17}  {:>17}  {:>17}",
        "K", "recall@10", "us/query", "QPS"
    );
    println!("{}", "-".repeat(63));
    for row in rows {
        let summary = if label == "unfiltered" {
            &row.unfiltered
        } else {
            &row.filtered
        };
        println!(
            "{:>4}  {:>7.4} / {:>7.4}  {:>7.1} / {:>7.1}  {:>7.0} / {:>7.0}",
            row.segment_count,
            summary.recall.median,
            summary.recall.p95,
            summary.us_per_query.median,
            summary.us_per_query.p95,
            summary.qps.median,
            summary.qps.p95,
        );
    }
}

fn main() {
    if std::env::var_os("STRATA_SEGMENT_RECALL_SELF_TEST").is_some() {
        segment_recall_self_test();
        return;
    }

    let n: usize = std::env::var("STRATA_SEG_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let q_n: usize = std::env::var("STRATA_SEG_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let warmup_runs = positive_env("STRATA_SEG_WARMUP_RUNS", DEFAULT_WARMUP_RUNS);
    let repetitions = positive_env("STRATA_SEG_REPETITIONS", DEFAULT_REPETITIONS);
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
        "query policy: {warmup_runs} full unfiltered+filtered warmup sweep(s), then {repetitions} measured sweep(s) per K"
    );
    println!(
        "filtered policy: segment_cohort=0, alternating contiguous cohort per segment; K=1 contains only cohort 0"
    );

    let mut rows = Vec::new();
    for &k_seg in &k_sweep {
        if k_seg > n {
            continue;
        }
        let start_live = reset_peak();
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
            txn.insert(batch_for(&vectors, lo, hi, per, &schema))
                .unwrap();
            txn.commit().unwrap();
        }
        drop(dataset);
        let dataset = Dataset::open(&dir).unwrap();
        let snapshot = dataset.snapshot();
        let build_s = t_build.elapsed().as_secs_f64();
        let filtered_truth: Vec<HashSet<u64>> = queries
            .iter()
            .map(|query| exact_top_k_where(query, &vectors, |id| cohort_for(id, per) == 0))
            .collect();
        let filtered_ids: HashSet<u64> = (0..n)
            .filter(|id| cohort_for(*id, per) == 0)
            .map(|id| id as u64)
            .collect();
        let predicate = Predicate::Eq("segment_cohort".to_owned(), Value::Int64(0));

        for _ in 0..warmup_runs {
            let _ = measure_search(&snapshot, &queries, &truth, None, None);
            let _ = measure_search(
                &snapshot,
                &queries,
                &filtered_truth,
                Some(&predicate),
                Some(&filtered_ids),
            );
        }
        let mut unfiltered_samples = Vec::with_capacity(repetitions);
        let mut filtered_samples = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            unfiltered_samples.push(measure_search(&snapshot, &queries, &truth, None, None));
            filtered_samples.push(measure_search(
                &snapshot,
                &queries,
                &filtered_truth,
                Some(&predicate),
                Some(&filtered_ids),
            ));
        }
        rows.push(SegmentRow {
            segment_count: k_seg,
            unfiltered: summarize_search(&unfiltered_samples),
            filtered: summarize_search(&filtered_samples),
            build_seconds: build_s,
            peak_live_bytes: peak_over_start(start_live),
        });
        drop(snapshot);
        drop(dataset);
        let _ = std::fs::remove_dir_all(&dir);
    }

    print_search_summary("unfiltered", &rows);
    print_search_summary("filtered", &rows);
    println!("\nBuild/reopen and peak-live diagnostic (one dataset build per K):");
    println!(
        "{:>4}  {:>17}  {:>17}",
        "K", "build+reopen s", "peak live delta"
    );
    println!("{}", "-".repeat(42));
    for row in &rows {
        println!(
            "{:>4}  {:>17.3}  {:>14.1} MiB",
            row.segment_count,
            row.build_seconds,
            row.peak_live_bytes as f64 / (1024.0 * 1024.0)
        );
    }

    // ---- verdict ---------------------------------------------------------
    // Intentionally scoped to the unfiltered comparison baseline; the
    // filtered table above is a separate facade-path observation.
    let base = rows.first().map(|row| &row.unfiltered);
    let last = rows.last().map(|row| &row.unfiltered);
    let last_k = rows.last().map_or(0, |row| row.segment_count);
    let base_recall = base.map_or(0.0, |summary| summary.recall.median);
    let base_latency = base.map_or(0.0, |summary| summary.us_per_query.median);
    let last_recall = last.map_or(0.0, |summary| summary.recall.median);
    let last_latency = last.map_or(0.0, |summary| summary.us_per_query.median);
    let recall_drop = base_recall - last_recall;
    let latency_mult = if base_latency > 0.0 {
        last_latency / base_latency
    } else {
        f64::NAN
    };
    println!("\none-segment baseline (K=1):  recall {base_recall:.4}, {base_latency:.1} us/query");
    println!(
        "at K={}:            recall {:.4} ({:+.4}), {:.1} us/query ({:.1}x)",
        last_k, last_recall, -recall_drop, last_latency, latency_mult
    );
    println!();
    if recall_drop <= 0.02 {
        println!(
            "VERDICT: the K=1-to-K={} endpoint recall delta is {:.1}pp (within 2pp).",
            last_k,
            -recall_drop * 100.0
        );
        println!(
            "  Fan-out measures K-dependent behavior; read this run's direction from the table."
        );
        println!(
            "  This bounded sample reports recall and latency observations for the current fixture."
        );
        println!("  It is evidence for the current fixture, not a universal guarantee.");
    } else {
        println!(
            "VERDICT: the K=1-to-K={} endpoint recall delta is {:.1}pp (outside 2pp).",
            last_k,
            -recall_drop * 100.0
        );
        println!(
            "  Fan-out measures K-dependent behavior; read this run's direction from the table."
        );
        println!("  This bounded sample reports an endpoint difference for the current fixture.");
        println!("  It is a signal for follow-up measurement, not a universal guarantee.");
    }
    println!(
        "\nCaveats: contiguous partition (time-ordered segments); ef_search held constant per"
    );
    println!(
        "segment; the table reports the resulting K-dependent measurements, not a recall/latency trade. A"
    );
    println!(
        "latency-budgeted variant (shrink ef as K grows) would trade some of this recall back"
    );
    println!("for speed — worth a follow-up if the verdict is latency-bound.");
}

fn segment_recall_self_test() {
    assert_eq!(
        summarize(&[1.0, 2.0, 3.0, 4.0, 5.0]),
        Summary {
            median: 3.0,
            p95: 5.0,
        },
        "five measured repetitions use the middle sample for the median and nearest-rank p95"
    );

    // This fails if the diagnostic subtracts live bytes at the end of the K
    // run instead of the live bytes sampled when that K began.
    LIVE.store(120, Ordering::Relaxed);
    PEAK.store(180, Ordering::Relaxed);
    assert_eq!(
        peak_over_start(100),
        80,
        "peak growth must be measured above the K-run start sample"
    );
}
