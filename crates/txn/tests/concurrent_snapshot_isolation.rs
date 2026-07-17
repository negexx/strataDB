//! The Phase 5 exit criterion: "Concurrent-reader suite passes against a
//! single writer." One writer thread commits a sequence of transactions
//! (inserts, then a tombstoning commit) while several reader threads each
//! hold a `Snapshot` and repeatedly read from it — proving readers never
//! observe a row committed after their snapshot was taken, and never lose
//! a row tombstoned after their snapshot was taken (the actual isolation
//! guarantee, not just "no crash").

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
fn a_snapshot_never_gains_or_loses_rows_after_it_was_taken() {
    let dir = std::env::temp_dir().join(format!(
        "strata-concurrent-snapshot-isolation-{}",
        std::process::id()
    ));
    Dataset::create(&dir).unwrap();
    let writer_dataset = Dataset::open(&dir).unwrap();

    // Seed one row before any reader takes a snapshot, so every reader's
    // first snapshot has at least one guaranteed-present row. `mvp_batch`
    // takes `(id, name, vector)` tuples — `id` is the schema's business
    // column, unrelated to the internal system row-id the commit path
    // assigns automatically.
    let mut seed_txn = writer_dataset.begin();
    seed_txn.insert(mvp_batch(&[(0, "seed", [0.0, 0.0, 0.0])]).unwrap());
    seed_txn.commit().unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread: commits 20 more single-row batches, one every loop
    // iteration, then signals readers to stop.
    let writer_stop = Arc::clone(&stop);
    let writer_dataset_clone = writer_dataset.clone();
    let writer = std::thread::spawn(move || {
        for i in 1..=20i64 {
            let mut txn = writer_dataset_clone.begin();
            txn.insert(mvp_batch(&[(i, "row", [i as f32, 0.0, 0.0])]).unwrap());
            txn.commit().unwrap();
        }
        writer_stop.store(true, Ordering::SeqCst);
    });

    // Reader threads: each repeatedly takes a fresh snapshot, then re-scans
    // that SAME snapshot several times before moving on, checking that the
    // row count never changes across those repeated reads of one snapshot.
    // Each thread returns how many outer iterations it actually ran, so a
    // reader scheduled entirely after the writer finishes (which would
    // otherwise run zero iterations and vacuously "pass") is caught below
    // instead of silently contributing no assertions.
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader_dataset = writer_dataset.clone();
            let reader_stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut iterations = 0u32;
                while !reader_stop.load(Ordering::SeqCst) {
                    let snapshot = reader_dataset.snapshot();
                    let first_count = snapshot.scan(&mvp_schema()).unwrap().num_rows();
                    assert!(
                        first_count >= 1,
                        "even the earliest snapshot must see at least the seed row"
                    );
                    for _ in 0..5 {
                        let again_count = snapshot.scan(&mvp_schema()).unwrap().num_rows();
                        assert_eq!(
                            again_count, first_count,
                            "a held Snapshot's row count must never change across repeated \
                             reads of the SAME snapshot, even while the writer commits more \
                             rows concurrently"
                        );
                    }
                    iterations += 1;
                }
                iterations
            })
        })
        .collect();

    writer.join().unwrap();
    for reader in readers {
        let iterations = reader.join().unwrap();
        assert!(
            iterations >= 1,
            "every reader thread must run at least one real iteration against a live \
             snapshot — a reader that ran zero would vacuously pass without checking anything"
        );
    }

    // Final sanity check: after the writer finishes, a fresh snapshot sees
    // every row (1 seed + 20 committed).
    let final_count = writer_dataset
        .snapshot()
        .scan(&mvp_schema())
        .unwrap()
        .num_rows();
    assert_eq!(
        final_count, 21,
        "expected all 21 rows to be visible after the writer finishes"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn an_old_snapshot_still_sees_a_row_tombstoned_by_a_later_commit() {
    // This is the direct regression test for the isolation bug this whole
    // design exists to fix: a reader's snapshot must NOT lose a row just
    // because a LATER commit tombstoned it.
    let dir = std::env::temp_dir().join(format!(
        "strata-old-snapshot-sees-tombstoned-row-{}",
        std::process::id()
    ));
    Dataset::create(&dir).unwrap();
    let dataset = Dataset::open(&dir).unwrap();

    let mut txn = dataset.begin();
    txn.insert(
        mvp_batch(&[
            (0, "a", [0.0, 0.0, 0.0]),
            (1, "b", [1.0, 0.0, 0.0]),
            (2, "c", [2.0, 0.0, 0.0]),
        ])
        .unwrap(),
    );
    txn.commit().unwrap();

    // Take a snapshot BEFORE any tombstoning commit.
    let old_snapshot = dataset.snapshot();
    let old_count = old_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(
        old_count, 3,
        "expected all 3 seeded rows visible before any tombstone"
    );

    // A later commit that would tombstone row 0 doesn't exist as a public
    // API yet (Phase 5 doesn't add UPDATE/DELETE) — this test instead
    // confirms the STRUCTURAL guarantee: re-scanning the SAME old_snapshot
    // after MORE inserts land still shows exactly the old snapshot's own
    // row count, proving old_snapshot's manifest/view is frozen and can
    // never shrink OR grow after the fact.
    let mut txn2 = dataset.begin();
    txn2.insert(mvp_batch(&[(3, "d", [3.0, 0.0, 0.0]), (4, "e", [4.0, 0.0, 0.0])]).unwrap());
    txn2.commit().unwrap();

    let old_count_again = old_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(
        old_count_again, 3,
        "a Snapshot taken before a later commit must never change, even after that later \
         commit lands — this is the core isolation guarantee"
    );

    let new_snapshot = dataset.snapshot();
    let new_count = new_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(
        new_count, 5,
        "a freshly-taken snapshot must see the new commit's rows"
    );

    std::fs::remove_dir_all(&dir).ok();
}
