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

    let temporary = dataset
        .data_dir()
        .join(format!(".tmp-{}-0-vacuum.tmp", std::process::id()));
    std::fs::write(&temporary, b"temporary").unwrap();
    std::fs::write(dataset.data_dir().join("orphan.arrow"), b"orphan row file").unwrap();
    std::fs::write(dataset.data_dir().join("orphan.seg"), b"orphan segment").unwrap();
    std::fs::write(dataset.data_dir().join("unknown.data"), b"unknown").unwrap();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 3);
    assert!(report.bytes_deleted > 0);
    assert!(!temporary.exists());
    assert!(!dataset.data_dir().join("orphan.arrow").exists());
    assert!(!dataset.data_dir().join("orphan.seg").exists());
    assert!(dataset.data_dir().join("unknown.data").exists());
    assert_eq!(
        dataset.snapshot().scan(&mvp_schema()).unwrap().num_rows(),
        1
    );
}

#[test]
fn vacuum_preserves_unknown_dotfiles() {
    // Break caught: treating every dotfile as a Strata temporary object would
    // delete user-owned metadata that vacuum has no authority to reclaim.
    let directory = temp_dataset("unknown-dotfiles");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let keep = dataset.data_dir().join(".keep");
    let env = dataset.data_dir().join(".env");
    std::fs::write(&keep, b"keep").unwrap();
    std::fs::write(&env, b"env").unwrap();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.bytes_deleted, 0);
    assert!(keep.exists());
    assert!(env.exists());
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
        strata_txn::TxnError::Io(ref source)
            if source.kind() == std::io::ErrorKind::NotFound
    ));
}
