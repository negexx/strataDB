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
//! coupled to the accumulated history. This benchmark records the observed
//! timing and manifest-byte envelope of that current path; it does not
//! establish an asymptotic or universal bound.
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
//! STRATA_GROWTH_COMMITS=160 STRATA_GROWTH_WARMUP_RUNS=1 \
//!   STRATA_GROWTH_REPETITIONS=5 cargo bench --bench manifest_growth_bench
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_txn::Dataset;

/// A bounded default suitable for a local diagnostic run.
const DEFAULT_COMMITS: usize = 2000;
const BUCKETS: usize = 20;
const DEFAULT_REPETITIONS: usize = 1;
const DEFAULT_WARMUP_RUNS: usize = 0;

struct NumericSummary {
    median: f64,
    p95: f64,
    sample_variance: f64,
}

struct Measurement {
    timings: Vec<Duration>,
    wall: Duration,
    manifest_bytes: u64,
    manifest_samples: Vec<(usize, u64)>,
}

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

#[allow(clippy::cast_precision_loss)]
fn numeric_summary(samples: &[f64]) -> NumericSummary {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let count = sorted.len();
    let middle = count / 2;
    let median = if count.is_multiple_of(2) {
        f64::midpoint(sorted[middle - 1], sorted[middle])
    } else {
        sorted[middle]
    };
    let p95 = sorted[(count * 95).div_ceil(100) - 1];
    let mean = sorted.iter().sum::<f64>() / count as f64;
    let sample_variance = if count > 1 {
        sorted
            .iter()
            .map(|sample| (sample - mean).powi(2))
            .sum::<f64>()
            / (count - 1) as f64
    } else {
        0.0
    };

    NumericSummary {
        median,
        p95,
        sample_variance,
    }
}

fn positive_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

/// Byte size of the largest file in `_versions/` — the newest manifest, since
/// each version supersedes the last and they only grow. A direct, on-disk
/// witness for the measured retained-history point.
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

fn measure(commits: usize) -> Measurement {
    let dir = std::env::temp_dir().join(format!("strata-manifest-growth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ds = Dataset::create(&dir, Arc::clone(&schema)).unwrap();

    let mut timings = Vec::with_capacity(commits);
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

    Measurement {
        timings,
        wall,
        manifest_bytes,
        manifest_samples,
    }
}

#[allow(clippy::cast_precision_loss)]
fn report_repeated() {
    let commits = positive_env("STRATA_GROWTH_COMMITS", DEFAULT_COMMITS);
    let repetitions = positive_env("STRATA_GROWTH_REPETITIONS", DEFAULT_REPETITIONS);
    let warmup_runs = std::env::var("STRATA_GROWTH_WARMUP_RUNS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_WARMUP_RUNS);

    for _ in 0..warmup_runs {
        let _ = measure(commits);
    }
    let measurements: Vec<Measurement> = (0..repetitions).map(|_| measure(commits)).collect();
    let walls: Vec<f64> = measurements
        .iter()
        .map(|measurement| measurement.wall.as_secs_f64() * 1e3)
        .collect();
    let manifest_sizes: Vec<f64> = measurements
        .iter()
        .map(|measurement| measurement.manifest_bytes as f64)
        .collect();
    let wall_summary = numeric_summary(&walls);
    let manifest_summary = numeric_summary(&manifest_sizes);

    println!();
    println!("manifest growth â€” {commits} sequential commits, one data file each");
    println!(
        "input: deterministic id-only rows; commits={commits}; buckets={BUCKETS}; warmup runs excluded={warmup_runs}; measured repetitions={repetitions}"
    );
    println!("(id column only: no vector column, so no HNSW insert is involved)");
    for (run, measurement) in measurements.iter().enumerate() {
        let per_bucket = commits.div_ceil(BUCKETS).max(1);
        let first_mean = mean_micros(&measurement.timings[..per_bucket]);
        let last_start = (commits - 1) / per_bucket * per_bucket;
        let last_mean = mean_micros(&measurement.timings[last_start..]);
        let growth = if first_mean > 0.0 {
            last_mean / first_mean
        } else {
            f64::NAN
        };
        println!(
            "run {run}: total wall={:.3} ms; newest manifest={} bytes; first->last bucket={growth:.2}x",
            measurement.wall.as_secs_f64() * 1e3,
            measurement.manifest_bytes
        );
        for (version, bytes) in &measurement.manifest_samples {
            println!("  retained version {version}: {bytes} bytes");
        }
    }
    println!(
        "median commit-sequence wall: {:.3} ms; p95: {:.3} ms; sample variance: {:.3} ms^2",
        wall_summary.median, wall_summary.p95, wall_summary.sample_variance
    );
    println!(
        "median newest manifest: {:.0} bytes; p95: {:.0} bytes; sample variance: {:.3} bytes^2",
        manifest_summary.median, manifest_summary.p95, manifest_summary.sample_variance
    );
    println!("OBSERVATION: bounded host-local evidence, not an asymptotic or universal bound.");
}

fn main() {
    if std::env::var("STRATA_GROWTH_REPETITIONS").is_ok() {
        report_repeated();
        return;
    }

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
    println!("manifest growth: {commits} sequential commits, one data file each");
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
