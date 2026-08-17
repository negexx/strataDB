#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use strata_storage::StorageError;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};
use strata_txn::{Dataset, TxnError};

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
fn vacuum_preserves_temporary_looking_names_without_canonical_numeric_prefixes() {
    // Break caught: accepting partial numeric matches grants deletion
    // authority over user files that merely resemble Strata temporaries.
    let directory = temp_dataset("malformed-temporary-names");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let non_numeric_process_id = dataset.data_dir().join(".tmp-x-0-user.data");
    let non_numeric_counter = dataset.data_dir().join(".tmp-123-x-user.data");
    let padded_process_id = dataset.data_dir().join(".tmp-00123-0-user.data");
    let padded_counter = dataset.data_dir().join(".tmp-123-000-user.data");
    std::fs::write(&non_numeric_process_id, b"user data").unwrap();
    std::fs::write(&non_numeric_counter, b"user data").unwrap();
    std::fs::write(&padded_process_id, b"user data").unwrap();
    std::fs::write(&padded_counter, b"user data").unwrap();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.bytes_deleted, 0);
    assert!(non_numeric_process_id.exists());
    assert!(non_numeric_counter.exists());
    assert!(padded_process_id.exists());
    assert!(padded_counter.exists());
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
fn vacuum_reads_an_unpadded_current_manifest_key_and_preserves_its_objects() {
    // Break caught: reconstructing the padded spelling for the current
    // manifest rejects recovery-recognized unpadded authority and prevents
    // vacuum from protecting its referenced data.
    let directory = temp_dataset("unpadded-current");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "row", [1.0, 0.0, 1.0])]).unwrap())
        .unwrap();
    transaction.commit().unwrap();
    let snapshot = dataset.snapshot();
    let manifest = strata_storage::read_current(directory.path())
        .unwrap()
        .unwrap();
    let protected = manifest
        .data_files
        .iter()
        .map(|entry| dataset.data_dir().join(&entry.name))
        .chain(
            manifest
                .segments
                .iter()
                .map(|entry| dataset.data_dir().join(&entry.name)),
        )
        .collect::<Vec<_>>();
    std::fs::rename(
        directory
            .path()
            .join("_versions/00000000000000000001.manifest"),
        directory.path().join("_versions/1.manifest"),
    )
    .unwrap();
    let orphan = dataset.data_dir().join("unprotected.arrow");
    std::fs::write(&orphan, b"orphan").unwrap();

    let report = dataset.vacuum().unwrap();

    assert_eq!(report.objects_deleted, 1);
    assert!(!orphan.exists());
    assert!(protected.iter().all(|path| path.exists()));
    assert_eq!(snapshot.scan(&mvp_schema()).unwrap().num_rows(), 1);

    drop(snapshot);
    drop(dataset);
    let reopened = Dataset::open(directory.path()).unwrap();
    let hits = reopened
        .snapshot()
        .vector_search(&[1.0, 0.0, 1.0], 1, None)
        .unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
        vec![0],
        "vacuum must preserve the unpadded current manifest's loadable vector segment and its valid physical row ID"
    );
}

#[test]
fn vacuum_fails_closed_for_duplicate_padded_and_unpadded_manifest_aliases() {
    // Break caught: accepting both spellings for one numeric version creates
    // conflicting durable authority over the objects vacuum must protect.
    let directory = temp_dataset("duplicate-manifest-alias");
    let dataset = Dataset::create(directory.path(), mvp_schema()).unwrap();
    std::fs::copy(
        directory
            .path()
            .join("_versions/00000000000000000000.manifest"),
        directory.path().join("_versions/0.manifest"),
    )
    .unwrap();

    let error = dataset.vacuum().unwrap_err();

    assert!(matches!(
        error,
        TxnError::Storage(StorageError::CorruptManifest(path, reason))
            if path == Path::new("_versions/00000000000000000000.manifest")
                && reason == "duplicate listed manifest version"
    ));
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
