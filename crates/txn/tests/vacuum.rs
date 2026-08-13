#![allow(clippy::expect_used, clippy::unwrap_used)]

use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

fn temp_dataset(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("strata-vacuum-{label}-"))
        .tempdir()
        .expect("temporary dataset directory should be created")
}

#[test]
fn vacuum_removes_recognized_temporary_objects_and_preserves_unknown_files() {
    let directory = temp_dataset("orphans");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    transaction.commit().unwrap();

    std::fs::write(dataset.data_dir().join(".tmp-vacuum.arrow"), b"temporary").unwrap();
    std::fs::write(dataset.data_dir().join("orphan.arrow"), b"orphan row file").unwrap();
    std::fs::write(dataset.data_dir().join("orphan.seg"), b"orphan segment").unwrap();
    std::fs::write(dataset.data_dir().join("unknown.data"), b"unknown").unwrap();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 3);
    assert!(report.bytes_deleted > 0);
    assert!(!dataset.data_dir().join(".tmp-vacuum.arrow").exists());
    assert!(!dataset.data_dir().join("orphan.arrow").exists());
    assert!(!dataset.data_dir().join("orphan.seg").exists());
    assert!(dataset.data_dir().join("unknown.data").exists());
    assert_eq!(
        dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        1
    );
}

#[test]
fn vacuum_preserves_active_snapshot_objects() {
    let directory = temp_dataset("snapshot");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    transaction.commit().unwrap();
    let historical = dataset.snapshot();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 0);
    assert_eq!(historical.scan(&mvp_schema()).unwrap().num_rows(), 1);
}

#[test]
fn vacuum_fails_closed_when_the_current_manifest_object_is_missing() {
    let directory = temp_dataset("missing-protected");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    transaction.commit().unwrap();
    let file_name = dataset.data_files()[0].name.clone();
    std::fs::remove_file(dataset.data_dir().join(file_name)).unwrap();

    let error = dataset.vacuum().unwrap_err();

    assert!(matches!(
        error,
        strata_txn::TxnError::Storage(strata_storage::StorageError::Io(_))
    ));
}
