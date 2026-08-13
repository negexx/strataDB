#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use strata_query::Predicate;
use strata_storage::Value;
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
