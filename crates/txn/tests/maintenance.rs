#![allow(clippy::expect_used, clippy::unwrap_used)]

use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{Dataset, LifecycleMaintenancePolicy};

#[test]
fn maintenance_reduces_unprotected_history_and_reports_the_bound() {
    let directory = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    for (id, label, vector) in [
        (1, "first", [1.0, 0.0, 1.0]),
        (2, "second", [2.0, 0.0, 1.0]),
    ] {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(id, label, vector)]).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    std::fs::write(dataset.data_dir().join("orphan.arrow"), b"orphan").unwrap();

    let report = dataset
        .maintain(LifecycleMaintenancePolicy {
            keep_latest_versions: 1,
            max_age_us: 0,
            max_data_objects: 2,
            max_segments: 1,
        })
        .unwrap();

    assert!(report.storage_bound_met);
    assert_eq!(report.inventory.data_object_count(), 1);
    assert_eq!(report.inventory.reachable_segment_count(), 1);
    assert!(!dataset.data_dir().join("orphan.arrow").exists());
}

#[test]
fn maintenance_reports_when_an_active_snapshot_prevents_the_bound() {
    let directory = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut first = dataset.begin();
    first
        .insert(mvp_batch(&[(1, "first", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    first.commit().unwrap();
    let historical = dataset.snapshot();
    let mut second = dataset.begin();
    second
        .insert(mvp_batch(&[(2, "second", [2.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    second.commit().unwrap();

    let report = dataset
        .maintain(LifecycleMaintenancePolicy {
            keep_latest_versions: 1,
            max_age_us: 0,
            max_data_objects: 1,
            max_segments: 1,
        })
        .unwrap();

    assert!(!report.storage_bound_met);
    assert!(report.inventory.data_object_count() > 1);
    assert_eq!(historical.scan(&mvp_schema()).unwrap().num_rows(), 1);
}
