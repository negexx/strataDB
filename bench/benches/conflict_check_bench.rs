//! Microbenchmark for `CommitLog::conflicts_with` — deliberately touches no
//! disk.
//!
//! `concurrent_commit_bench` cannot measure this function at all. It is
//! fsync-dominated (~10 ms/commit, so it cannot resolve anything under ~20%),
//! and its insert workload has an *empty* write-set, which short-circuits
//! `conflicts_with` before any real work happens. The only benchmark there
//! with non-empty write-sets, `high_conflict_rate_delete_retries`, spends
//! essentially all its time in commit I/O.
//!
//! Two axes matter, because the two optimizations in this function have
//! opposite risk profiles:
//!
//! * **write-set size** — hashing the write-set once replaces a linear
//!   `Vec::contains` per candidate row. That is a large win for a bulk delete
//!   and a possible *loss* for a single-row delete, where one integer compare
//!   beats a `SipHash` of a `u64`. Single-row deletes are the common case, so
//!   this is the number that decides whether the rewrite is worth keeping.
//! * **range width** — binary-searching the range start replaces scanning
//!   every retained entry just to skip it by version. A transaction whose base
//!   version is recent only needs a few entries, but the old code walked all
//!   2048 regardless. This should be a pure win with no downside.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use strata_txn::commit_log::CommitLog;

/// Matches `COMMIT_LOG_CAPACITY` in `crates/txn/src/dataset.rs`.
const CAPACITY: u64 = 2048;
/// Row-ids per committed entry — a modest per-transaction delete, so the log
/// holds `CAPACITY` * `ROWS_PER_ENTRY` = 20,480 row-ids in total.
const ROWS_PER_ENTRY: u64 = 10;

/// A completely full log: every version present, disjoint row-ids per entry.
fn full_log() -> CommitLog {
    let mut log = CommitLog::new(usize::try_from(CAPACITY).unwrap());
    for version in 1..=CAPACITY {
        let base = version * ROWS_PER_ENTRY;
        log.push(version, (0..ROWS_PER_ENTRY).map(|i| base + i).collect());
    }
    log
}

/// Write-set that overlaps nothing in the log. This is the path that matters:
/// a clean check cannot early-exit, so it always pays the full scan.
fn disjoint_write_set(n: u64) -> Vec<u64> {
    (0..n).map(|i| 100_000_000 + i).collect()
}

/// Sweep write-set size over the full version range. Old cost was
/// O(rows-in-range * write-set-size); new is O(rows-in-range) hash probes plus
/// O(write-set-size) to build the set.
fn bench_write_set_size(c: &mut Criterion) {
    let log = full_log();
    let mut group = c.benchmark_group("conflicts_with_clean_by_write_set_size");
    for n in [1_u64, 4, 16, 64, 256, 1024, 10_000] {
        let write_set = disjoint_write_set(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &write_set, |b, ws| {
            b.iter(|| black_box(log.conflicts_with(black_box(0), black_box(CAPACITY), ws)));
        });
    }
    group.finish();
}

/// Sweep how much of the log the requested range actually covers, holding the
/// write-set at the common single-row case. `since_version` near the top means
/// only a handful of entries are in range — the old code still walked all
/// 2048 to skip them.
fn bench_range_width(c: &mut Criterion) {
    let log = full_log();
    let write_set = disjoint_write_set(1);
    let mut group = c.benchmark_group("conflicts_with_clean_by_range_width");
    for in_range in [1_u64, 8, 64, 512, CAPACITY] {
        let since = CAPACITY - in_range;
        group.bench_with_input(
            BenchmarkId::from_parameter(in_range),
            &since,
            |b, &since| {
                b.iter(|| {
                    black_box(log.conflicts_with(black_box(since), black_box(CAPACITY), &write_set))
                });
            },
        );
    }
    group.finish();
}

/// The pathological case the rewrite exists for: a bulk delete checked against
/// a full log. Old cost here was ~20,480 * 100,000 comparisons, held under the
/// global commit lock.
fn bench_bulk_delete(c: &mut Criterion) {
    let log = full_log();
    let write_set = disjoint_write_set(100_000);
    c.bench_function("conflicts_with_bulk_delete_100k", |b| {
        b.iter(|| black_box(log.conflicts_with(black_box(0), black_box(CAPACITY), &write_set)));
    });
}

criterion_group!(
    benches,
    bench_write_set_size,
    bench_range_width,
    bench_bulk_delete
);
criterion_main!(benches);
