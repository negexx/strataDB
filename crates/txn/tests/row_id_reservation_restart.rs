//! Process-level restart coverage for durable row-id reservations.
//!
//! The child process aborts after the reservation record's five durability
//! checkpoints, before it can write a row file or publish a manifest. The
//! parent then reopens the dataset and proves that the next acknowledged row
//! starts above the abandoned range.
#![cfg(feature = "chaos-injection")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

use strata_txn::{Dataset, mvp_fixtures};

const CHILD_DATASET_DIR_ENV: &str = "STRATA_ROW_ID_RESTART_CHILD_DATASET_DIR";
const CHILD_TEST_NAME: &str = "child_aborts_after_publishing_a_row_id_reservation";
const RESERVATION_PUBLICATION_CHECKPOINTS: &str = "5";

#[test]
fn child_aborts_after_publishing_a_row_id_reservation() {
    let Ok(dir) = std::env::var(CHILD_DATASET_DIR_ENV) else {
        return;
    };

    let dataset = Dataset::open(dir).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_fixtures::mvp_row(101, "abandoned", [1.0, 2.0, 3.0]).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    panic!(
        "the child must abort at the reservation's final durability checkpoint before manifest publication"
    );
}

#[test]
fn restart_after_a_published_reservation_skips_the_abandoned_range() {
    // Break caught: seeding a reopened allocator only from the manifest
    // reuses row-id 0 after the child dies with a published reservation but
    // before any manifest can name that row.
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("dataset");
    Dataset::create(&dir, mvp_fixtures::mvp_schema()).unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST_NAME, "--nocapture"])
        .env(CHILD_DATASET_DIR_ENV, &dir)
        .env("STRATA_CHAOS_ABORT_AT", RESERVATION_PUBLICATION_CHECKPOINTS)
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "the child should abort after durable reservation publication, but exited successfully: {output:?}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;

        assert_eq!(
            output.status.signal(),
            Some(6),
            "the reservation child must terminate with SIGABRT: {output:?}"
        );
    }

    // File handles from an aborted child can linger briefly on Windows.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let reopened = Dataset::open(&dir).unwrap();
    let mut transaction = reopened.begin();
    transaction
        .insert(mvp_fixtures::mvp_row(202, "committed", [4.0, 5.0, 6.0]).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    let files = reopened.data_files();
    assert_eq!(
        files.len(),
        1,
        "only the post-restart row is manifest-visible"
    );
    assert_eq!(
        files[0].row_id_range,
        Some((1, 1)),
        "the durable reservation must leave row-id 0 permanently abandoned"
    );
}
