//! Scheduled production-primitive concurrency evidence for Audit 4.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

struct GateState {
    arrived: usize,
    released: bool,
    failed: bool,
}

struct CancelableGate {
    state: Mutex<GateState>,
    wake: Condvar,
}

impl CancelableGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                arrived: 0,
                released: false,
                failed: false,
            }),
            wake: Condvar::new(),
        }
    }

    fn arrive_and_wait(&self, participants: usize) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.failed {
            return false;
        }
        state.arrived += 1;
        if state.arrived == participants {
            state.released = true;
            self.wake.notify_all();
            return true;
        }
        while !state.released && !state.failed {
            state = self
                .wake
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        !state.failed
    }

    fn fail(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed = true;
        self.wake.notify_all();
    }

    fn is_failed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failed
    }
}

#[test]
fn cancelable_gate_releases_waiters_when_a_peer_fails() {
    let gate = Arc::new(CancelableGate::new());
    let waiting_gate = Arc::clone(&gate);
    let waiter = std::thread::spawn(move || waiting_gate.arrive_and_wait(2));

    gate.fail();

    assert!(matches!(waiter.join(), Ok(false)));
}

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
    let post_publication = Arc::new(CancelableGate::new());

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader_dataset = dataset.clone();
            let reader_stop = Arc::clone(&stop);
            let reader_gate = Arc::clone(&readers_ready);
            let reader_start = Arc::clone(&start);
            let reader_post_publication_readers = Arc::clone(&post_publication_readers);
            let reader_post_publication = Arc::clone(&post_publication);
            std::thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
                            assert!(
                                reader_post_publication.arrive_and_wait(5),
                                "publication gate cancelled after a peer reader failed"
                            );
                        }
                        if reader_stop.load(Ordering::Acquire) {
                            break;
                        }
                    }
                    (checks, observed_post_publication)
                }));
                if result.is_err() {
                    reader_post_publication.fail();
                    reader_stop.store(true, Ordering::Release);
                }
                result.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            })
        })
        .collect();

    let writer_dataset = dataset.clone();
    let writer_ready = Arc::clone(&readers_ready);
    let writer_post_publication_readers = Arc::clone(&post_publication_readers);
    let writer_post_publication = Arc::clone(&post_publication);
    let writer = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            start.wait();
            let readiness_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while writer_ready.load(Ordering::Acquire) != 4 {
                if writer_post_publication.is_failed() {
                    return Err("a reader failed before readiness".to_owned());
                }
                if std::time::Instant::now() >= readiness_deadline {
                    writer_post_publication.fail();
                    return Err("readers did not reach readiness before the deadline".to_owned());
                }
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
                    if !writer_post_publication.arrive_and_wait(5) {
                        return Err(
                            "a reader failed during post-publication coordination".to_owned()
                        );
                    }
                    writer_interval_readers =
                        writer_post_publication_readers.load(Ordering::Acquire);
                }
            }
            Ok(writer_interval_readers)
        }));
        if result.is_err() {
            writer_post_publication.fail();
        }
        result.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
    });
    let writer_result = writer.join();
    stop.store(true, Ordering::Release);

    let reader_results: Vec<_> = readers
        .into_iter()
        .map(std::thread::JoinHandle::join)
        .collect();
    let writer_interval_readers = writer_result
        .expect("writer thread panicked")
        .expect("writer coordination failed");
    let total_checks: u64 = reader_results
        .into_iter()
        .map(|reader_result| {
            let (checks, observed_post_publication) =
                reader_result.expect("reader thread panicked");
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
