//! Scheduled production-primitive concurrency evidence for Audit 4.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

#[test]
#[ignore = "scheduled Audit 4 production ArcSwap evidence"]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]
fn shared_dataset_publication_stress_preserves_complete_snapshots() {
    let commits = std::env::var("STRATA_CONCURRENCY_STRESS_COMMITS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(128);
    let directory = tempfile::Builder::new()
        .prefix("strata-audit4-concurrency-")
        .tempdir()
        .unwrap();
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let readers_ready = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(std::sync::Barrier::new(5));
    let post_publication_readers = Arc::new(AtomicUsize::new(0));
    let post_publication = Arc::new(std::sync::Barrier::new(5));

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader_dataset = dataset.clone();
            let reader_stop = Arc::clone(&stop);
            let reader_gate = Arc::clone(&readers_ready);
            let reader_start = Arc::clone(&start);
            let reader_post_publication_readers = Arc::clone(&post_publication_readers);
            let reader_post_publication = Arc::clone(&post_publication);
            std::thread::spawn(move || {
                reader_start.wait();
                let mut checks = 0_u64;
                let mut last_version = 0_u64;
                let mut observed_post_publication = false;
                loop {
                    let snapshot = reader_dataset.snapshot();
                    let version = snapshot.version();
                    assert!(version >= last_version, "snapshot version regressed");
                    assert_eq!(
                        snapshot.data_files().len(),
                        usize::try_from(version).expect("stress version fits usize"),
                        "row-file catalog must match the committed version"
                    );
                    assert_eq!(
                        snapshot.segment_info().len(),
                        usize::try_from(version).expect("stress version fits usize"),
                        "segment catalog must match the committed version"
                    );
                    assert_eq!(
                        snapshot.scan(&mvp_schema()).unwrap().num_rows(),
                        usize::try_from(version).expect("stress version fits usize"),
                        "a published snapshot must contain every committed row"
                    );
                    last_version = version;
                    checks += 1;
                    if checks == 1 {
                        reader_gate.fetch_add(1, Ordering::Release);
                    }
                    if version > 0 && !observed_post_publication {
                        observed_post_publication = true;
                        reader_post_publication_readers.fetch_add(1, Ordering::Release);
                        reader_post_publication.wait();
                    }
                    if reader_stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                (checks, observed_post_publication)
            })
        })
        .collect();

    let writer_dataset = dataset.clone();
    let writer_ready = Arc::clone(&readers_ready);
    let writer_post_publication_readers = Arc::clone(&post_publication_readers);
    let writer_post_publication = Arc::clone(&post_publication);
    let writer = std::thread::spawn(move || {
        start.wait();
        while writer_ready.load(Ordering::Acquire) != 4 {
            std::thread::yield_now();
        }
        let mut writer_interval_readers = 0_usize;
        for raw_id in 0..commits {
            let id = i64::try_from(raw_id).expect("stress row id fits i64");
            let mut transaction = writer_dataset.begin();
            transaction
                .insert(mvp_batch(&[(id, "stress", [id as f32, 0.0, 1.0])]).unwrap())
                .unwrap();
            transaction.commit().unwrap();
            if raw_id == 0 {
                writer_post_publication.wait();
                writer_interval_readers = writer_post_publication_readers.load(Ordering::Acquire);
            }
        }
        writer_interval_readers
    });
    let writer_interval_readers = writer.join().unwrap();
    stop.store(true, Ordering::Release);

    let total_checks: u64 = readers
        .into_iter()
        .map(|reader| {
            let (checks, observed_post_publication) = reader.join().unwrap();
            assert!(checks > 0, "reader must perform at least one check");
            assert!(
                observed_post_publication,
                "reader must validate a snapshot after the first publication"
            );
            checks
        })
        .sum();
    assert!(total_checks >= 4);
    assert_eq!(
        writer_interval_readers, 4,
        "startup-only reader evidence must not satisfy the post-publication reader interval"
    );
    assert_eq!(dataset.current_version(), commits);
    eprintln!(
        "AUDIT4_CONCURRENCY_STRESS_COMPLETE commits={commits} reader_checks={total_checks} writer_interval_readers={writer_interval_readers}"
    );
}
