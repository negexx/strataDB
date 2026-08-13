#![allow(clippy::expect_used, clippy::unwrap_used)]

use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{AgeRetentionPolicy, Dataset};

#[test]
fn age_retention_prunes_old_manifests_but_keeps_current_and_active_snapshots() {
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
        .prune_manifests_by_age(AgeRetentionPolicy {
            keep_latest_versions: 1,
            max_age_us: 0,
        })
        .unwrap();

    assert!(report.deleted_manifest_versions.is_empty());
    assert_eq!(historical.scan(&mvp_schema()).unwrap().num_rows(), 1);
    drop(historical);
    let report = dataset
        .prune_manifests_by_age(AgeRetentionPolicy {
            keep_latest_versions: 1,
            max_age_us: 0,
        })
        .unwrap();
    assert_eq!(report.deleted_manifest_versions, vec![1]);
}
