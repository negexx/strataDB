//! Bounded shared-handle reader evidence for Phase 2.
//!
//! Four threads share one `Dataset` handle and repeatedly scan the same
//! immutable snapshot. The benchmark reports per-thread elapsed time so a
//! future run can inspect spread/fairness; it deliberately does not encode a
//! product latency or fairness SLO.

#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

const READER_THREADS: usize = 4;
const SCANS_PER_READER: usize = 32;

struct Fixture {
    dataset: Dataset,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "strata-query-concurrency-bench-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dataset = Dataset::create(&dir, mvp_schema()).expect("fixture dataset creation works");
        let rows: Vec<(i64, &str, [f32; 3])> = (0..1024)
            .map(|id| (id, "row", [id as f32, 0.0, 1.0]))
            .collect();
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&rows).expect("fixture batch construction works"))
            .expect("fixture insert works");
        transaction.commit().expect("fixture commit works");
        Self { dataset, dir }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn run_readers(dataset: &Dataset) -> (Duration, Duration) {
    let barrier = Arc::new(Barrier::new(READER_THREADS));
    let readers: Vec<_> = (0..READER_THREADS)
        .map(|_| {
            let dataset = dataset.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let snapshot = dataset.snapshot();
                barrier.wait();
                let started = Instant::now();
                for _ in 0..SCANS_PER_READER {
                    let batch = snapshot.scan(&mvp_schema()).expect("snapshot scan works");
                    assert_eq!(batch.num_rows(), 1024);
                }
                started.elapsed()
            })
        })
        .collect();
    let durations: Vec<_> = readers
        .into_iter()
        .map(|reader| reader.join().expect("reader thread completes"))
        .collect();
    let min = durations.iter().copied().min().unwrap();
    let max = durations.iter().copied().max().unwrap();
    (min, max)
}

fn bench_shared_handle_readers(c: &mut Criterion) {
    let fixture = Fixture::new();
    let mut group = c.benchmark_group("shared_handle_query_readers");
    group.bench_function("four_readers_same_snapshot", |b| {
        b.iter(|| {
            let (min, max) = run_readers(&fixture.dataset);
            eprintln!(
                "query-reader evidence: threads={READER_THREADS} scans_per_reader={SCANS_PER_READER} min_ns={} max_ns={} spread_ns={}",
                min.as_nanos(),
                max.as_nanos(),
                max.saturating_sub(min).as_nanos()
            );
            std::hint::black_box((min, max));
        });
    });
    group.finish();
}

criterion_group!(benches, bench_shared_handle_readers);
criterion_main!(benches);
