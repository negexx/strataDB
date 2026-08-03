//! End-to-end lifecycle benchmark — one realistic StrataDB workload from A to
//! Z, timing and memory-profiling every phase so the slowest / CPU-heaviest /
//! memory-heaviest parts are directly comparable on one axis.
//!
//! Not a `criterion` benchmark: criterion measures one operation in a tight
//! loop. This runs the *whole* lifecycle once (ingest → recover → scan →
//! filter → group-by → vector search → concurrent writes) on the configured
//! fixture when present, or a deterministic synthetic 512-dim source when the
//! external download is absent, and prints a comparison table.
//!
//! ```text
//! cargo bench --bench lifecycle_bench
//! STRATA_LIFECYCLE_ROWS=50000 cargo bench --bench lifecycle_bench
//! STRATA_BENCH_SOURCE=synthetic STRATA_BENCH_SEED=20260801 \
//!   STRATA_LIFECYCLE_ROWS=64 STRATA_LIFECYCLE_BATCH_ROWS=8 \
//!   cargo bench -p strata-bench --bench lifecycle_bench
//! ```
//!
//! Memory is measured with a counting global allocator (below): per phase we
//! report bytes *allocated* during it (allocator churn / CPU-adjacent work)
//! and *peak live* growth (resident footprint the phase adds). Wall time on a
//! single thread is ~CPU time for the compute-bound phases; the commit and
//! recovery phases are annotated where fsync / disk I/O dominates instead.

// Benchmark harness: the numeric casts are between sizes/counts and display
// units where wrap/truncation cannot occur at realistic scales, and the
// single-char names / long main / literal prints are all local to this
// throwaway reporting binary.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::print_literal,
    clippy::doc_markdown
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strata_query::{AggFunc, Predicate};
use strata_storage::Value;
use strata_txn::{Dataset, Snapshot};

// ---- counting allocator ---------------------------------------------------

struct Counting;
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);
static TOTAL: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates every allocation to the system allocator unchanged and
// only adds atomic bookkeeping around it; the returned pointers and layouts
// are exactly the system allocator's, so all `GlobalAlloc` invariants hold.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let sz = layout.size() as i64;
            let live = LIVE.fetch_add(sz, Ordering::Relaxed) + sz;
            TOTAL.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Marks the start of a phase: snapshots cumulative allocation and resets the
/// peak-live tracker to the current live bytes so `PEAK` becomes "peak *during
/// this phase*".
fn phase_start() -> (u64, i64) {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    (TOTAL.load(Ordering::Relaxed), live)
}

struct PhaseMem {
    allocated: u64,
    peak_over_start: i64,
    live_delta: i64,
}

fn phase_end(start: (u64, i64)) -> PhaseMem {
    PhaseMem {
        allocated: TOTAL.load(Ordering::Relaxed) - start.0,
        peak_over_start: PEAK.load(Ordering::Relaxed) - start.1,
        live_delta: LIVE.load(Ordering::Relaxed) - start.1,
    }
}

// ---- dataset -------------------------------------------------------------

const DATASET_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/dbpedia-openai-100k.parquet"
);
const EMBEDDING_COLUMN: &str = "text-embedding-3-small-512-embedding";
const VECTOR_DIM: usize = 512;
const VECTOR_DIM_I32: i32 = 512;
const DEFAULT_SYNTHETIC_SEED: u64 = 20_260_801;

struct Row {
    id: i64,
    category: i64,
    amount: f64,
    vector: Vec<f32>,
}

fn load_rows(limit: usize) -> Vec<Row> {
    let file = std::fs::File::open(DATASET_PATH).unwrap_or_else(|e| {
        panic!(
            "failed to open {DATASET_PATH}: {e}. Download it first — see \
             docs/roadmap.md's Phase 1 performance-evidence work first."
        )
    });
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
            .expect("embedding column is a list");
        for i in 0..batch.num_rows() {
            if out.len() >= limit {
                return out;
            }
            let values = list.value(i);
            let values: &Float64Array = values.as_any().downcast_ref().expect("f64 values");
            let vector: Vec<f32> = values.values().iter().map(|v| *v as f32).collect();
            let id = out.len() as i64;
            out.push(Row {
                id,
                category: id % 10, // low-cardinality group/filter column
                amount: (id % 1000) as f64 + 0.5,
                vector,
            });
        }
    }
    out
}

fn load_rows_from_path(path: &Path, limit: usize) -> Vec<Row> {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
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
            .expect("embedding column is a list");
        for i in 0..batch.num_rows() {
            if out.len() >= limit {
                return out;
            }
            let values = list.value(i);
            let values: &Float64Array = values.as_any().downcast_ref().expect("f64 values");
            let vector: Vec<f32> = values.values().iter().map(|value| *value as f32).collect();
            let id = out.len() as i64;
            out.push(Row {
                id,
                category: id % 10,
                amount: (id % 1000) as f64 + 0.5,
                vector,
            });
        }
    }
    out
}

fn synthetic_rows(limit: usize, mut state: u64) -> Vec<Row> {
    (0..limit)
        .map(|id| {
            let vector = (0..VECTOR_DIM)
                .map(|dimension| {
                    // xorshift64 is deliberately specified here, avoiding a
                    // random-number dependency while keeping the fallback reproducible.
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let unit = (state >> 40) as f32 / ((1_u64 << 24) - 1) as f32;
                    unit + dimension as f32 * 0.000_001
                })
                .collect();
            Row {
                id: id as i64,
                category: (id % 10) as i64,
                amount: (id % 1000) as f64 + 0.5,
                vector,
            }
        })
        .collect()
}

fn input_rows(limit: usize) -> (Vec<Row>, String) {
    let source = std::env::var("STRATA_BENCH_SOURCE").unwrap_or_else(|_| "auto".to_owned());
    let seed = std::env::var("STRATA_BENCH_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SYNTHETIC_SEED);
    let path = std::env::var_os("STRATA_BENCH_FIXTURE")
        .map_or_else(|| PathBuf::from(DATASET_PATH), PathBuf::from);

    match source.as_str() {
        "synthetic" => (
            synthetic_rows(limit, seed),
            format!("synthetic seed={seed}"),
        ),
        "fixture" | "auto" if path == Path::new(DATASET_PATH) && path.is_file() => {
            (load_rows(limit), format!("fixture {}", path.display()))
        }
        "fixture" | "auto" if path.is_file() => (
            load_rows_from_path(&path, limit),
            format!("fixture {}", path.display()),
        ),
        "fixture" => panic!(
            "STRATA_BENCH_SOURCE=fixture requires {}; use synthetic fallback instead",
            path.display()
        ),
        "auto" => (
            synthetic_rows(limit, seed),
            format!(
                "synthetic fallback seed={seed} (fixture absent: {})",
                path.display()
            ),
        ),
        other => {
            panic!("unknown STRATA_BENCH_SOURCE={other:?}; expected auto, fixture, or synthetic")
        }
    }
}

fn rows_hash(rows: &[Row]) -> u64 {
    rows.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, row| {
        row.vector.iter().fold(hash, |hash, value| {
            value
                .to_bits()
                .to_le_bytes()
                .iter()
                .fold(hash, |hash, byte| {
                    (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
                })
        })
    })
}

fn newest_manifest_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir.join("_versions"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|meta| meta.len())
        .max()
        .unwrap_or(0)
}

fn schema() -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(item, VECTOR_DIM_I32),
            false,
        ),
    ]))
}

fn batch_of(rows: &[Row]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let ids = Int64Array::from(rows.iter().map(|r| r.id).collect::<Vec<_>>());
    let cats = Int64Array::from(rows.iter().map(|r| r.category).collect::<Vec<_>>());
    let amts = Float64Array::from(rows.iter().map(|r| r.amount).collect::<Vec<_>>());
    let flat: Vec<f32> = rows.iter().flat_map(|r| r.vector.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let vectors = FixedSizeListArray::new(item, VECTOR_DIM_I32, values, None);
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(ids),
            Arc::new(cats),
            Arc::new(amts),
            Arc::new(vectors),
        ],
    )
    .unwrap()
}

// ---- reporting -----------------------------------------------------------

struct PhaseResult {
    name: &'static str,
    kind: &'static str, // CPU / I-O / mixed
    wall: Duration,
    detail: String,
    mem: PhaseMem,
}

struct PinnedSnapshotDiagnostics {
    distinct_snapshots: usize,
    cache_entries: usize,
    cache_charged_bytes: usize,
    cache_budget_bytes: usize,
}

fn pinned_snapshot_diagnostics(pinned: &[Arc<Snapshot>]) -> PinnedSnapshotDiagnostics {
    let mut distinct: Vec<&Arc<Snapshot>> = Vec::with_capacity(pinned.len());
    for snapshot in pinned {
        if !distinct
            .iter()
            .any(|existing| Arc::ptr_eq(existing, snapshot))
        {
            distinct.push(snapshot);
        }
    }
    let mut cache_entries = 0;
    let mut cache_charged_bytes = 0;
    let mut cache_budget_bytes = 0;
    for snapshot in &distinct {
        let cache = snapshot.live_set_cache_accounting();
        cache_entries += cache.entry_count;
        cache_charged_bytes += cache.charged_bytes;
        cache_budget_bytes += cache.byte_budget;
    }
    PinnedSnapshotDiagnostics {
        distinct_snapshots: distinct.len(),
        cache_entries,
        cache_charged_bytes,
        cache_budget_bytes,
    }
}

fn mib(bytes: i64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let rows_n: usize = std::env::var("STRATA_LIFECYCLE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25_000);
    let batch_sz: usize = std::env::var("STRATA_LIFECYCLE_BATCH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000);
    let pinned_snapshots: usize = std::env::var("STRATA_PINNED_SNAPSHOTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let queries_n: usize = 50;
    let k: usize = 10;
    let threads: usize = 8;

    let (rows, source) = input_rows(rows_n);
    eprintln!(
        "loading {rows_n} rows ({VECTOR_DIM}-dim) from {source}; input hash={:016x}",
        rows_hash(&rows)
    );
    assert_eq!(rows[0].vector.len(), VECTOR_DIM);
    let n = rows.len();
    eprintln!("loaded {n} rows; running lifecycle...");

    let dir = std::env::temp_dir().join(format!("strata-lifecycle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let mut results: Vec<PhaseResult> = Vec::new();
    let mut retained_snapshots = Vec::new();

    // ---- Phase 1: ingest + commit (write path: encode data file, build +
    // serialize + fsync an index segment, publish both atomically).
    // Batched to model real ingestion. --------------------------------
    let ds = Dataset::create(&dir, schema()).unwrap();
    let m = phase_start();
    let t = Instant::now();
    let mut commits = 0;
    for chunk in rows.chunks(batch_sz) {
        let mut txn = ds.begin();
        txn.insert(batch_of(chunk)).unwrap();
        txn.commit().unwrap();
        commits += 1;
        if retained_snapshots.len() < pinned_snapshots {
            retained_snapshots.push(ds.snapshot());
        }
    }
    let wall = t.elapsed();
    results.push(PhaseResult {
        name: "ingest+commit",
        kind: "mixed (HNSW build + fsync)",
        wall,
        detail: format!(
            "{commits} commits, {:.0} rows/s, {:.2} ms/commit",
            n as f64 / wall.as_secs_f64(),
            wall.as_secs_f64() * 1000.0 / commits as f64
        ),
        mem: phase_end(m),
    });

    // ---- Phase 2: recovery — drop, reopen (load every index segment from
    // the manifest: O(bytes) per segment, no distance evaluations, no graph
    // reconstruction). ------------------------------------------------
    drop(ds);
    let m = phase_start();
    let t = Instant::now();
    let (ds, recovery_bytes) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    let wall = t.elapsed();
    results.push(PhaseResult {
        name: "recovery (reopen)",
        kind: "I/O + validation (segment load, no graph rebuild)",
        wall,
        detail: format!(
            "loaded {n} rows, {:.0} rows/s; recovery payload bytes: total={} manifest={} \
             row-data={} row-id-catalog={} segments={}",
            n as f64 / wall.as_secs_f64(),
            recovery_bytes.total_bytes(),
            recovery_bytes.manifest_bytes,
            recovery_bytes.row_data_bytes,
            recovery_bytes.row_id_catalog_bytes,
            recovery_bytes.segment_bytes,
        ),
        mem: phase_end(m),
    });

    let snapshot_start = phase_start();
    let snapshot_time = Instant::now();
    let current_snapshot = ds.snapshot();
    let mut pinned = retained_snapshots;
    while pinned.len() < pinned_snapshots {
        pinned.push(ds.snapshot());
    }
    let cache_predicate = Predicate::Eq("category".to_owned(), Value::Int64(3));
    for snapshot in &pinned {
        snapshot
            .vector_search(&rows[0].vector, k, Some(&cache_predicate))
            .unwrap();
    }
    let pinned_diagnostics = pinned_snapshot_diagnostics(&pinned);
    let snapshot_wall = snapshot_time.elapsed();
    let snapshot_mem = phase_end(snapshot_start);
    results.push(PhaseResult {
        name: "pinned snapshot/cache residency",
        kind: "memory (immutable snapshot handles)",
        wall: snapshot_wall,
        detail: format!(
            "{} retained handles ({} distinct snapshots; operational current snapshot excluded); \
             cache entries={} charged-bytes~={} budget-bytes={}; live allocator delta={} bytes",
            pinned.len(),
            pinned_diagnostics.distinct_snapshots,
            pinned_diagnostics.cache_entries,
            pinned_diagnostics.cache_charged_bytes,
            pinned_diagnostics.cache_budget_bytes,
            snapshot_mem.live_delta,
        ),
        mem: snapshot_mem,
    });
    let snap = current_snapshot;
    let sch = schema();

    // ---- Phase 3: full scan (read every file, all columns incl. vectors) --
    let m = phase_start();
    let t = Instant::now();
    let scanned = snap.scan(&sch).unwrap();
    let wall = t.elapsed();
    let scanned_rows = scanned.num_rows();
    results.push(PhaseResult {
        name: "full scan",
        kind: "I/O (read+decode all columns)",
        wall,
        detail: format!("{scanned_rows} rows materialized"),
        mem: phase_end(m),
    });

    // ---- Phase 4: filtered scan (predicate pushdown + row filter) ---------
    let pred = Predicate::Eq("category".to_string(), Value::Int64(3));
    let m = phase_start();
    let t = Instant::now();
    let filtered = snap.scan_with_predicate(&sch, &pred).unwrap();
    let wall = t.elapsed();
    let filtered_rows = filtered.num_rows();
    results.push(PhaseResult {
        name: "filtered scan",
        kind: "I/O + CPU (mask+filter)",
        wall,
        detail: format!("{filtered_rows}/{scanned_rows} rows match category=3"),
        mem: phase_end(m),
    });

    // ---- Phase 5: group-by aggregation over the scanned batch -------------
    let m = phase_start();
    let t = Instant::now();
    let grouped = strata_query::group_by(
        &scanned,
        &["category"],
        &[
            ("amount", AggFunc::Sum),
            ("amount", AggFunc::Count),
            ("amount", AggFunc::Min),
            ("amount", AggFunc::Max),
        ],
    )
    .unwrap();
    let wall = t.elapsed();
    let groups = grouped.num_rows();
    results.push(PhaseResult {
        name: "group-by (4 aggs)",
        kind: "CPU (hash aggregation)",
        wall,
        detail: format!("{groups} groups over {scanned_rows} rows"),
        mem: phase_end(m),
    });

    // ---- Phase 6: vector search, unfiltered (HNSW k-NN) -------------------
    let queries: Vec<Vec<f32>> = rows
        .iter()
        .take(queries_n)
        .map(|r| r.vector.clone())
        .collect();
    let query_count = queries.len();
    let m = phase_start();
    let t = Instant::now();
    let mut hits = 0;
    for q in &queries {
        hits += snap.vector_search(q, k, None).unwrap().len();
    }
    let wall = t.elapsed();
    results.push(PhaseResult {
        name: "vector search (unfiltered)",
        kind: "CPU (graph traversal + SIMD dist)",
        wall,
        detail: format!(
            "{query_count} queries, {:.1} us/query, {hits} hits",
            wall.as_secs_f64() * 1e6 / query_count as f64
        ),
        mem: phase_end(m),
    });

    // ---- Phase 7: vector search, filtered (resolve live-ids then k-NN) ----
    let m = phase_start();
    let t = Instant::now();
    let mut fhits = 0;
    for q in &queries {
        fhits += snap.vector_search(q, k, Some(&pred)).unwrap().len();
    }
    let wall = t.elapsed();
    results.push(PhaseResult {
        name: "vector search (filtered)",
        kind: "I/O-bound (row-id resolve) + CPU",
        wall,
        detail: format!(
            "{query_count} queries, {:.2} ms/query, {fhits} hits",
            wall.as_secs_f64() * 1000.0 / query_count as f64
        ),
        mem: phase_end(m),
    });

    // ---- Phase 7b: vector search, filtered, predicate varies per query ----
    // Same shape as Phase 7, but each query gets its own `category` predicate
    // instead of reusing one `pred` across all `queries_n` calls. Phase 7
    // alone cannot distinguish "resolving live-ids is expensive" from
    // "resolving live-ids the FIRST time for a given predicate is expensive,
    // and every later call in this loop is a free repeat" — the two have
    // very different implications for a per-snapshot cache keyed by
    // predicate. This phase is the honest cold-path baseline.
    //
    // Cycles the 9 categories OTHER than `pred`'s (category = 3), not all
    // 10 — this phase shares `snap` (and so its live-set cache) with Phase
    // 7 above, which already resolved category = 3 against it. Including 3
    // here would make one of the 10 "cold" misses actually a cache hit
    // left over from Phase 7, silently inflating this phase's hit count
    // and shrinking its true miss count from 10 to 9 without the numbers
    // saying so.
    let categories: Vec<i64> = (0..10).filter(|&c| c != 3).collect();
    let varying_preds: Vec<Predicate> = (0..queries_n)
        .map(|i| {
            Predicate::Eq(
                "category".to_string(),
                Value::Int64(categories[i % categories.len()]),
            )
        })
        .collect();
    let m = phase_start();
    let t = Instant::now();
    let mut vhits = 0;
    for (q, p) in queries.iter().zip(&varying_preds) {
        vhits += snap.vector_search(q, k, Some(p)).unwrap().len();
    }
    let wall = t.elapsed();
    results.push(PhaseResult {
        name: "vector search (filtered, varying predicate)",
        kind: "I/O-bound (row-id resolve) + CPU",
        wall,
        detail: format!(
            "{query_count} queries, {:.2} ms/query, {vhits} hits, predicate cycles 9 categories \
             disjoint from phase 7's category=3",
            wall.as_secs_f64() * 1000.0 / query_count as f64
        ),
        mem: phase_end(m),
    });

    // ---- Phase 8: concurrent non-conflicting commits ---------------------
    let per_thread = 3usize;
    let base_id = n as i64;
    let m = phase_start();
    let t = Instant::now();
    std::thread::scope(|s| {
        for th in 0..threads {
            let ds = &ds;
            let rows = &rows;
            s.spawn(move || {
                for c in 0..per_thread {
                    let idx = (th * per_thread + c) % rows.len();
                    let r = &rows[idx];
                    let one = Row {
                        id: base_id + (th * per_thread + c) as i64,
                        category: r.category,
                        amount: r.amount,
                        vector: r.vector.clone(),
                    };
                    let mut txn = ds.begin();
                    txn.insert(batch_of(std::slice::from_ref(&one))).unwrap();
                    txn.commit().unwrap();
                }
            });
        }
    });
    let wall = t.elapsed();
    let total_commits = threads * per_thread;
    results.push(PhaseResult {
        name: "concurrent commits",
        kind: "I/O-bound (serialized fsync)",
        wall,
        detail: format!(
            "{threads} threads x {per_thread} = {total_commits} commits, {:.0}/s",
            total_commits as f64 / wall.as_secs_f64()
        ),
        mem: phase_end(m),
    });

    let manifest_bytes = newest_manifest_bytes(&dir);
    drop(snap);
    drop(pinned);
    drop(ds);
    let _ = std::fs::remove_dir_all(&dir);

    // ---- report ----------------------------------------------------------
    let on_disk = dir_size_estimate(n);
    println!(
        "\n================ StrataDB lifecycle — {n} rows x {VECTOR_DIM}-dim ================\n"
    );
    println!(
        "{:<28} {:>11} {:>12} {:>12} {:>12}  {}",
        "phase", "wall", "alloc'd", "peak-live", "live-delta", "kind"
    );
    println!("{}", "-".repeat(104));
    let slowest = results
        .iter()
        .max_by_key(|r| r.wall.as_nanos())
        .map_or("", |r| r.name);
    let heaviest_mem = results
        .iter()
        .max_by_key(|r| r.mem.peak_over_start.max(0))
        .map_or("", |r| r.name);
    let most_alloc = results
        .iter()
        .max_by_key(|r| r.mem.allocated)
        .map_or("", |r| r.name);
    for r in &results {
        println!(
            "{:<28} {:>11} {:>10.1}MB {:>10.1}MB {:>10.1}MB  {}",
            r.name,
            format!("{:.2?}", r.wall),
            mib(r.mem.allocated as i64),
            mib(r.mem.peak_over_start),
            mib(r.mem.live_delta),
            r.kind
        );
        println!("{:<28} {}", "", r.detail);
    }
    println!(
        "\napprox on-disk dataset size: {:.0} MB (in-memory input held: {:.0} MB)",
        on_disk,
        mib((n * VECTOR_DIM * 4) as i64)
    );
    println!("input source           : {source}");
    println!("input hash             : {:016x}", rows_hash(&rows));
    println!("newest manifest bytes  : {manifest_bytes}");
    println!("\nSLOWEST phase        : {slowest}");
    println!("MOST CPU/alloc churn : {most_alloc}");
    println!("HEAVIEST peak memory : {heaviest_mem}");
    println!("\nNote: single-threaded wall time ~= CPU time for CPU-bound phases; commit/recovery");
    println!(
        "are dominated by fsync and disk I/O respectively, so their wall time overstates CPU."
    );
    println!(
        "Allocator columns are benchmark-process accounting deltas, not RSS or an exact resident-memory bound;"
    );
    println!(
        "cache charged bytes are approximate admission-control accounting, not allocator residency."
    );
}

fn dir_size_estimate(n: usize) -> f64 {
    // vectors dominate: n * 512 * 4 bytes on disk (Arrow IPC, uncompressed f32).
    (n * VECTOR_DIM * 4) as f64 / (1024.0 * 1024.0)
}
