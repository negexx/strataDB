// bench/benches/concurrent_commit_bench.rs
//! Phase 6 exit-evidence benchmark: commit throughput under concurrent
//! non-conflicting writers vs. a single-writer baseline, and under a
//! high-conflict-rate workload — the number that validates (or refutes)
//! the tightly-scoped `commit_lock` design. See
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! §3 and §7.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_txn::Dataset;

const NUM_THREADS: usize = 8;
const COMMITS_PER_THREAD: i64 = 50;

/// This workspace's benches and tests use `std::env::temp_dir().join(...)`
/// directly rather than the `tempfile` crate (confirmed against
/// `bench/Cargo.toml`'s existing dependency list, which has no `tempfile`
/// entry) — matching that convention rather than introducing a new
/// dependency for this one file. `TempDataset` bundles the directory path
/// alongside the `Dataset` so its `Drop` impl can clean up, since
/// `criterion::BatchSize::LargeInput` setup closures don't get an explicit
/// teardown hook of their own.
struct TempDataset {
    dir: PathBuf,
    dataset: Dataset,
}

impl Drop for TempDataset {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn setup_dataset(row_count: i64) -> TempDataset {
    let dir = std::env::temp_dir().join(format!(
        "strata-bench-concurrent-commit-{}-{}",
        std::process::id(),
        row_count
    ));
    std::fs::remove_dir_all(&dir).ok();
    let dataset = Dataset::create(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Vec<i64> = (0..row_count).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();
    let mut txn = dataset.begin();
    txn.insert(batch);
    txn.commit().unwrap();
    TempDataset { dir, dataset }
}

/// `NUM_THREADS` threads each committing `COMMITS_PER_THREAD` inserts of
/// fresh (never-colliding) rows — zero conflicts by construction, since
/// inserts always get fresh monotonic row-ids (design doc §1). This is the
/// throughput the tightly-scoped `commit_lock` is meant to preserve close
/// to a lock-free ceiling, since the expensive data-file fsync happens
/// outside it.
fn bench_concurrent_non_conflicting_inserts(c: &mut Criterion) {
    c.bench_function("concurrent_non_conflicting_inserts", |b| {
        b.iter_batched(
            || setup_dataset(0),
            |temp_dataset| {
                std::thread::scope(|scope| {
                    for _ in 0..NUM_THREADS {
                        let ds = temp_dataset.dataset.clone();
                        scope.spawn(move || {
                            for i in 0..COMMITS_PER_THREAD {
                                let schema = Arc::new(Schema::new(vec![Field::new(
                                    "id",
                                    DataType::Int64,
                                    false,
                                )]));
                                let batch = RecordBatch::try_new(
                                    schema,
                                    vec![Arc::new(Int64Array::from(vec![i]))],
                                )
                                .unwrap();
                                let mut txn = ds.begin();
                                txn.insert(batch);
                                txn.commit().unwrap();
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// `NUM_THREADS` threads all attempting to delete the *same* pre-existing
/// row-id (row 0) simultaneously — maximal conflict rate, exactly one
/// commit can succeed and every other thread gets `TxnError::Conflict` on
/// its first and only attempt. Measures how long the contended critical
/// section takes to resolve under saturated contention, not a retry loop's
/// cost (there isn't one here — see the deviation note below).
///
/// Deviation from the plan brief: the brief's version wrapped each thread's
/// attempt in a `loop` that retried on conflict, distinguishing "row 0
/// already deleted by someone else" from other conflicts via
/// `Snapshot::is_visible(0)`. That method is `pub(crate)` in `strata_txn`
/// (see `crates/txn/src/snapshot.rs`), not reachable from this external
/// bench crate. Since this workload has exactly one contested row and no
/// writer other than these `NUM_THREADS` threads, any `TxnError::Conflict`
/// here can only mean another thread already deleted row 0 — so the loser
/// is already done, with nothing left to retry. The `loop` wrapper was
/// removed (every match arm terminated on the first pass regardless, which
/// is exactly `clippy::never_loop`'s complaint) rather than kept as
/// dead structure.
fn bench_high_conflict_rate(c: &mut Criterion) {
    c.bench_function("high_conflict_rate_delete_retries", |b| {
        b.iter_batched(
            || setup_dataset(1),
            |temp_dataset| {
                std::thread::scope(|scope| {
                    for _ in 0..NUM_THREADS {
                        let ds = temp_dataset.dataset.clone();
                        scope.spawn(move || {
                            // Not a real retry loop: with exactly one
                            // contested row and no writer besides these
                            // threads, every match arm below terminates on
                            // the first attempt (clippy::never_loop agrees —
                            // a genuine retry loop would live in caller code,
                            // this benchmark only measures one contended
                            // commit attempt per thread).
                            let mut txn = ds.begin();
                            txn.delete(0);
                            match txn.commit() {
                                // Row 0 is the only contested row in this
                                // benchmark, so either this thread won the
                                // race or another thread already deleted it
                                // — both are the expected, done outcome.
                                Ok(()) | Err(strata_txn::TxnError::Conflict { .. }) => {}
                                Err(e) => panic!("unexpected error: {e}"),
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// Single-writer baseline: `NUM_THREADS * COMMITS_PER_THREAD` sequential
/// commits on one thread, same total commit count as
/// `bench_concurrent_non_conflicting_inserts` — the number that isolates
/// how much `commit_lock` contention costs from how much total commit
/// volume costs.
// `NUM_THREADS` (8) and `COMMITS_PER_THREAD` (50) are small compile-time
// constants — `NUM_THREADS as i64` can never actually wrap.
#[allow(clippy::cast_possible_wrap)]
fn bench_single_writer_baseline(c: &mut Criterion) {
    c.bench_function("single_writer_baseline", |b| {
        b.iter_batched(
            || setup_dataset(0),
            |temp_dataset| {
                for i in 0..(NUM_THREADS as i64 * COMMITS_PER_THREAD) {
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
                    let batch =
                        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![i]))])
                            .unwrap();
                    let mut txn = temp_dataset.dataset.begin();
                    txn.insert(batch);
                    txn.commit().unwrap();
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    bench_concurrent_non_conflicting_inserts,
    bench_high_conflict_rate,
    bench_single_writer_baseline
);
criterion_main!(benches);
