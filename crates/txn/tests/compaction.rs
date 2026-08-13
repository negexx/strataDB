#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use strata_query::Predicate;
use strata_storage::Value;
#[cfg(feature = "test-fault-injection")]
use strata_txn::TxnError;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{CompactionPolicy, Dataset};

fn temp_dataset(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("strata-compaction-{label}-"))
        .tempdir()
        .expect("temporary dataset directory should be created")
}

#[test]
fn compacting_an_empty_dataset_publishes_a_new_version() {
    let directory = temp_dataset("empty");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let source_timestamp = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap()
        .committed_at_us;

    let report = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();

    assert_eq!(report.source_version, 0);
    assert_eq!(report.published_version, 1);
    assert_eq!(
        dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        0
    );
    assert!(dataset.data_files().is_empty());
    assert!(
        strata_storage::read_current(directory.path())
            .unwrap()
            .unwrap()
            .committed_at_us
            > source_timestamp,
        "compaction must issue a fresh manifest publication timestamp"
    );
}

#[test]
fn compaction_preserves_rows_vectors_and_replaces_fanout() {
    let directory = temp_dataset("rows");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    for id in 0..3_i64 {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(id, "row", [id as f32, 0.0, 1.0])]).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    assert_eq!(dataset.data_files().len(), 3);
    let segment_probe = Predicate::Eq("id".to_owned(), Value::Int64(-1));
    assert_eq!(dataset.snapshot().explain(&segment_probe).segments_total, 3);

    let before = dataset.snapshot();
    let before_hits = before.vector_search(&[0.0, 0.0, 1.0], 3, None).unwrap();
    let report = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();
    let after = dataset.snapshot();

    assert!(report.source_version < report.published_version);
    assert!(report.published_version > report.source_version);
    assert_eq!(after.scan(&mvp_schema()).unwrap().num_rows(), 3);
    assert_eq!(after.data_files().len(), 1);
    assert_eq!(after.data_files()[0].row_id_range, Some((0, 2)));
    assert_eq!(after.explain(&segment_probe).segments_total, 1);
    let after_hits = after.vector_search(&[0.0, 0.0, 1.0], 3, None).unwrap();
    assert_eq!(
        before_hits.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
        after_hits.iter().map(|hit| hit.row_id).collect::<Vec<_>>()
    );
    assert_eq!(before.scan(&mvp_schema()).unwrap().num_rows(), 3);
}

#[test]
fn compaction_reclaims_superseded_objects_after_old_snapshot_drops() {
    let directory = temp_dataset("reclaim");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    for id in 0..3_i64 {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(id, "row", [id as f32, 0.0, 1.0])]).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }

    let historical = dataset.snapshot();
    dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();
    assert_eq!(historical.scan(&mvp_schema()).unwrap().num_rows(), 3);
    assert_eq!(
        historical
            .vector_search(&[0.0, 0.0, 1.0], 3, None)
            .unwrap()
            .len(),
        3
    );
    assert!(std::fs::read_dir(dataset.data_dir()).unwrap().count() > 2);
    drop(historical);

    let report = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();
    assert!(report.objects_deleted >= 2);
    assert!(report.bytes_deleted > 0);
    assert_eq!(std::fs::read_dir(dataset.data_dir()).unwrap().count(), 2);
    assert_eq!(
        dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        3
    );
}

#[test]
fn compaction_reads_an_unpadded_active_snapshot_manifest_key_and_preserves_its_objects() {
    // Break caught: rebuilding a padded key for an active snapshot rejects a
    // recovery-recognized unpadded manifest after publication, leaving
    // compaction unable to preserve that snapshot's physical objects.
    let directory = temp_dataset("unpadded-active-snapshot");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut first = dataset.begin();
    first
        .insert(mvp_batch(&[(1, "first", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    first.commit().unwrap();
    let historical = dataset.snapshot();
    let protected_manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    let protected = protected_manifest
        .data_files
        .iter()
        .map(|entry| dataset.data_dir().join(&entry.name))
        .chain(
            protected_manifest
                .segments
                .iter()
                .map(|entry| dataset.data_dir().join(&entry.name)),
        )
        .collect::<Vec<_>>();

    let mut second = dataset.begin();
    second
        .insert(mvp_batch(&[(2, "second", [2.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    second.commit().unwrap();
    std::fs::rename(
        directory
            .path()
            .join("_versions/00000000000000000001.manifest"),
        directory.path().join("_versions/1.manifest"),
    )
    .unwrap();

    let report = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();

    assert_eq!(report.objects_deleted, 2);
    assert!(protected.iter().all(|path| path.exists()));
    assert_eq!(historical.scan(&mvp_schema()).unwrap().num_rows(), 1);
    assert_eq!(
        historical
            .vector_search(&[1.0, 0.0, 1.0], 1, None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn compaction_drops_tombstone_history_that_no_longer_has_physical_owners() {
    let directory = temp_dataset("tombstones");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    let mut delete = dataset.begin();
    delete.delete(0).unwrap();
    delete.commit().unwrap();
    dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();

    let reopened = Dataset::open(directory.path()).unwrap();
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        0
    );
}

#[test]
fn compaction_splits_noncontiguous_row_ids_into_valid_catalog_ranges() {
    let directory = temp_dataset("gapped-rows");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(
            mvp_batch(&[
                (0, "row", [0.0, 0.0, 1.0]),
                (1, "row", [1.0, 0.0, 1.0]),
                (2, "row", [2.0, 0.0, 1.0]),
            ])
            .unwrap(),
        )
        .unwrap();
    transaction.commit().unwrap();

    let mut delete = dataset.begin();
    delete.delete(1).unwrap();
    delete.commit().unwrap();

    dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();
    let reopened = Dataset::open(directory.path()).unwrap();
    let ranges = reopened
        .data_files()
        .iter()
        .map(|entry| entry.row_id_range)
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec![Some((0, 0)), Some((2, 2))]);
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        2
    );
}

#[test]
fn compaction_records_an_empty_occ_history_entry_for_preexisting_transactions() {
    let directory = temp_dataset("occ-gap");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut insert = dataset.begin();
    insert
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    insert.commit().unwrap();

    let mut pending_delete = dataset.begin();
    pending_delete.delete(0).unwrap();
    dataset
        .compact(CompactionPolicy::retain_snapshots())
        .unwrap();
    pending_delete.commit().unwrap();
    assert_eq!(
        dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        0
    );
}

#[cfg(feature = "test-fault-injection")]
#[test]
fn compaction_prepublication_directory_sync_failure_reopens_old_state_and_allows_a_unique_commit() {
    // Break caught: publishing a compacted manifest after its replacement
    // directory entry failed to sync would expose a manifest whose objects
    // are not durable. Recovery must instead retain the old manifest.
    let directory = temp_dataset("prepublication-crash-reopen");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    for id in 0..3_i64 {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(id, "row", [id as f32, 0.0, 1.0])]).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    let old_manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();

    let directory_sync_fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
        1,
        std::io::ErrorKind::Other,
    );
    let error = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .expect_err("the replacement-data directory sync must fail before manifest publication");
    assert!(matches!(error, TxnError::Storage(_)));
    drop(directory_sync_fault);
    drop(dataset);

    let reopened = Dataset::open(directory.path()).unwrap();
    let reopened_manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    assert_eq!(reopened_manifest.version, old_manifest.version);
    assert_eq!(reopened_manifest.next_row_id, old_manifest.next_row_id);
    assert_eq!(
        reopened_manifest.next_attempt_id,
        old_manifest.next_attempt_id
    );
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        3
    );
    assert_eq!(
        reopened
            .snapshot()
            .vector_search(&[0.0, 0.0, 1.0], 3, None)
            .unwrap()
            .iter()
            .map(|hit| hit.row_id)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let mut later = reopened.begin();
    later
        .insert(mvp_batch(&[(99, "later", [99.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    later.commit().unwrap();
    assert_eq!(reopened.current_version(), old_manifest.version + 1);
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        4
    );
    assert_eq!(
        reopened
            .snapshot()
            .vector_search(&[99.0, 0.0, 1.0], 4, None)
            .unwrap()
            .iter()
            .map(|hit| hit.row_id)
            .collect::<Vec<_>>(),
        vec![3, 2, 1, 0]
    );
}

#[cfg(feature = "test-fault-injection")]
#[test]
fn compaction_postpublication_fault_reopens_new_manifest_and_retains_old_objects() {
    // Break caught: doing in-memory installation or reclamation before the
    // durable manifest boundary would either lose the new state on reopen or
    // delete objects still required to recover from a post-publication crash.
    let directory = temp_dataset("postpublication-crash-reopen");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    for id in 0..3_i64 {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(id, "row", [id as f32, 0.0, 1.0])]).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    let old_manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    let old_objects = old_manifest
        .data_files
        .iter()
        .map(|entry| entry.name.as_str())
        .chain(
            old_manifest
                .segments
                .iter()
                .map(|entry| entry.name.as_str()),
        )
        .map(|name| directory.path().join("data").join(name))
        .collect::<Vec<_>>();

    let _directory_syncs = strata_storage::datafile::test_support::record_directory_syncs();
    let _post_publication_fault =
        strata_txn::test_support::fail_after_compaction_manifest_publication();
    let error = dataset
        .compact(CompactionPolicy::retain_snapshots())
        .expect_err("the test seam must stop compaction after durable manifest publication");
    assert!(matches!(error, TxnError::Io(_)));
    drop(dataset);

    for object in &old_objects {
        assert!(
            object.exists(),
            "post-publication failure must not reclaim old object {object:?}"
        );
    }
    let reopened = Dataset::open(directory.path()).unwrap();
    let reopened_manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    assert_eq!(reopened_manifest.version, old_manifest.version + 1);
    assert_eq!(reopened_manifest.next_row_id, old_manifest.next_row_id);
    assert_eq!(
        reopened_manifest.next_attempt_id,
        old_manifest.next_attempt_id + 1
    );
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        3
    );
    assert_eq!(
        reopened
            .snapshot()
            .vector_search(&[0.0, 0.0, 1.0], 3, None)
            .unwrap()
            .iter()
            .map(|hit| hit.row_id)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let mut later = reopened.begin();
    later
        .insert(mvp_batch(&[(99, "later", [99.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    later.commit().unwrap();
    assert_eq!(reopened.current_version(), old_manifest.version + 2);
    assert_eq!(
        reopened.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        4
    );
    assert_eq!(
        reopened
            .snapshot()
            .vector_search(&[99.0, 0.0, 1.0], 4, None)
            .unwrap()
            .iter()
            .map(|hit| hit.row_id)
            .collect::<Vec<_>>(),
        vec![3, 2, 1, 0]
    );
}
