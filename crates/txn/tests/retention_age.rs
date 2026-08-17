#![allow(clippy::expect_used, clippy::unwrap_used)]

use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{AgeRetentionPolicy, Dataset};

#[test]
fn age_retention_prunes_old_manifests_but_keeps_current_and_active_snapshots() {
    let directory = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let initial = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    assert!(
        initial.committed_at_us > 0,
        "a newly created dataset must publish a timestamped initial manifest"
    );
    let initial_snapshot = dataset.snapshot();
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
    drop(initial_snapshot);
    let report = dataset
        .prune_manifests_by_age(AgeRetentionPolicy {
            keep_latest_versions: 1,
            max_age_us: 0,
        })
        .unwrap();
    assert_eq!(report.deleted_manifest_versions, vec![0]);
}

#[test]
fn zero_timestamp_manifest_is_never_age_pruned() {
    let directory = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    drop(dataset);

    let storage = strata_storage::StorageOwner::local(directory.path());
    let initial_manifest_key = storage.manifest_object_key(0);
    std::fs::remove_file(directory.path().join(initial_manifest_key.as_str())).unwrap();
    let legacy_initial = strata_storage::Manifest::empty_with_schema(mvp_schema().as_ref());
    assert_eq!(legacy_initial.committed_at_us, 0);
    strata_storage::commit_manifest(directory.path(), &legacy_initial).unwrap();
    assert_eq!(
        strata_storage::read_current(directory.path())
            .unwrap()
            .unwrap()
            .committed_at_us,
        0
    );

    let dataset = Dataset::open(directory.path()).unwrap();
    let mut first = dataset.begin();
    first
        .insert(mvp_batch(&[(1, "first", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    first.commit().unwrap();
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

    assert!(
        !report.deleted_manifest_versions.contains(&0),
        "a legacy zero-timestamp manifest must never be reported as age-pruned"
    );
    let (retained_initial, _) = strata_storage::read_manifest_at_key_with_byte_count(
        directory.path(),
        initial_manifest_key.as_str(),
        0,
    )
    .unwrap();
    assert_eq!(retained_initial.committed_at_us, 0);
}
