//! Does per-commit cost grow with accumulated history?
//!
//! This is the measurement `concurrent_commit_bench` structurally cannot make.
//! That benchmark runs `NUM_THREADS * COMMITS_PER_THREAD` = 400 commits and
//! rebuilds the dataset in a `criterion` `iter_batched` setup closure, so the
//! manifest starts empty on every iteration and never exceeds a few hundred
//! files. It reports aggregate throughput, which cannot distinguish "every
//! commit is uniformly slow" from "commit #400 costs 400x commit #1" — and the
//! latter is the claim under test.
//!
//! `Transaction::commit` deep-clones the whole `Manifest`, appends this
//! commit's files, then `serde_json`-serializes and fsyncs the *entire*
//! accumulated file list — all inside the global commit lock. Each step is
//! O(F) in the number of data files ever committed, and F only ever grows
//! (nothing compacts it), so total work across N commits is predicted to be
//! O(N^2). This benchmark exists to confirm or refute that empirically, and to
//! locate the scale at which it starts to matter, *before* anyone invests in
//! the incremental-manifest redesign that would fix it.
//!
//! Deliberately NOT a `criterion` benchmark: criterion measures steady state
//! and resets between iterations, which is precisely the blindness being
//! corrected. This runs one long monotonic sequence and prints the curve.
//!
//! Batches carry an `id` column only — no `"vector"` column — so
//! `build_vector_inserts` produces nothing and no HNSW insert runs. That
//! isolates the manifest cost from the index's own, separately growing,
//! insert cost.
//!
//! ```text
//! cargo bench --bench manifest_growth_bench
//! STRATA_GROWTH_COMMITS=5000 cargo bench --bench manifest_growth_bench
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_txn::Dataset;

/// Enough to expose a quadratic term without making the run tedious; a clean
/// O(F) signal is already obvious well before this on a normal disk.
const DEFAULT_COMMITS: usize = 2000;
const BUCKETS: usize = 20;

fn mean_micros(samples: &[Duration]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let total: f64 = samples.iter().map(Duration::as_secs_f64).sum::<f64>() * 1e6;
    #[allow(clippy::cast_precision_loss)]
    let count = samples.len() as f64;
    total / count
}

/// Byte size of the largest file in `_versions/` — the newest manifest, since
/// each version supersedes the last and they only grow. A direct, on-disk
/// witness of the O(F) term that per-commit latency only measures indirectly.
fn newest_manifest_bytes(dir: &std::path::Path) -> u64 {
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

fn main() {
    let commits: usize = std::env::var("STRATA_GROWTH_COMMITS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_COMMITS);

    let dir = std::env::temp_dir().join(format!("strata-manifest-growth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

    let mut timings: Vec<Duration> = Vec::with_capacity(commits);
    let checkpoints = [1, commits.div_ceil(4), commits.div_ceil(2), commits];
    let mut manifest_samples = Vec::new();
    let overall = Instant::now();
    for i in 0..commits {
        let id = i64::try_from(i).unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap();
        let mut txn = ds.begin();
        txn.insert(batch).unwrap();
        let started = Instant::now();
        txn.commit().unwrap();
        timings.push(started.elapsed());
        if checkpoints.contains(&(i + 1)) {
            manifest_samples.push((i + 1, newest_manifest_bytes(&dir)));
        }
    }
    let wall = overall.elapsed();
    let manifest_bytes = newest_manifest_bytes(&dir);

    drop(ds);
    let _ = std::fs::remove_dir_all(&dir);

    println!();
    println!("manifest growth — {commits} sequential commits, one data file each");
    println!("input: deterministic id-only rows; commits={commits}; buckets={BUCKETS}");
    println!("(id column only: no vector column, so no HNSW insert is involved)");
    println!();
    println!(
        "{:>14}  {:>16}  {:>10}",
        "commit range", "mean commit", "vs first"
    );
    println!("{}", "-".repeat(46));

    let per_bucket = commits.div_ceil(BUCKETS).max(1);
    let mut first_mean = 0.0_f64;
    let mut last_mean = 0.0_f64;
    let mut bucket = 0;
    while bucket * per_bucket < commits {
        let lo = bucket * per_bucket;
        let hi = ((bucket + 1) * per_bucket).min(commits);
        let mean = mean_micros(&timings[lo..hi]);
        if bucket == 0 {
            first_mean = mean;
        }
        last_mean = mean;
        let ratio = if first_mean > 0.0 {
            mean / first_mean
        } else {
            f64::NAN
        };
        let range = format!("{lo}-{}", hi - 1);
        println!("{range:>14}  {mean:>13.1} us  {ratio:>9.2}x");
        bucket += 1;
    }

    let growth = if first_mean > 0.0 {
        last_mean / first_mean
    } else {
        f64::NAN
    };
    println!();
    println!("total wall time      : {wall:.2?}");
    println!("newest manifest bytes: {manifest_bytes}");
    println!("first->last bucket   : {growth:.2}x");
    println!("retained versions / manifest bytes:");
    for (version, bytes) in manifest_samples {
        println!("  {version:>5} / {bytes} bytes");
    }
    println!();

    if growth >= 2.0 {
        println!("VERDICT: per-commit cost grew {growth:.1}x across the run.");
        println!("  Commit work scales with accumulated history, as predicted: the");
        println!("  O(F) manifest clone + full re-serialize + fsync all sit inside");
        println!("  the global commit lock. The incremental-manifest redesign is");
        println!("  justified; use the table above to pick the scale it starts to bite.");
    } else {
        println!("VERDICT: per-commit cost changed only {growth:.2}x across the run.");
        println!("  No strong growth signal at {commits} commits. Re-run with a larger");
        println!("  STRATA_GROWTH_COMMITS before concluding the O(F) term is harmless —");
        println!("  and note the manifest byte size above still grows linearly even");
        println!("  when latency is dominated by fsync.");
    }
}
