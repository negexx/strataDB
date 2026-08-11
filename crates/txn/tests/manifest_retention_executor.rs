#![allow(clippy::cast_possible_wrap, clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fs;
#[cfg(feature = "chaos-injection")]
use std::process::Command;
use std::sync::Arc;

#[cfg(feature = "chaos-injection")]
use arrow::array::{FixedSizeListArray, Float32Array};
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::{Backend, LocalFs, StorageError};
use strata_txn::{Dataset, RetentionPolicy, TxnError};

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn commit_id(dataset: &Dataset, value: u64) {
    let batch = RecordBatch::try_new(
        dataset.schema(),
        vec![Arc::new(Int64Array::from(vec![value as i64]))],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
}

#[cfg(feature = "chaos-injection")]
fn vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]))
}

#[cfg(feature = "chaos-injection")]
fn commit_vector(dataset: &Dataset, id: i64) {
    let vector = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        3,
        Arc::new(Float32Array::from(vec![1.0, 0.0, 0.0])),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        dataset.schema(),
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(vector)],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
}

fn manifest_sizes(backend: &LocalFs) -> BTreeMap<String, u64> {
    backend
        .list("_versions/")
        .unwrap()
        .into_iter()
        .map(|object| (object.key, object.size))
        .collect()
}

#[test]
fn pruning_keeps_the_latest_window_and_leaves_data_objects_untouched() {
    // Break caught: deleting all old objects, or keeping only the current
    // manifest, violates a manifest-only latest-version retention policy.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("latest-window");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    commit_id(&dataset, 2);
    commit_id(&dataset, 3);
    let backend = LocalFs::new(&dir);
    backend
        .put("data/arbitrary-orphan.bin", b"must-survive")
        .unwrap();
    let data_before = backend.list("data/").unwrap();
    let sizes = manifest_sizes(&backend);
    let expected_bytes = sizes["_versions/00000000000000000000.manifest"]
        + sizes["_versions/00000000000000000001.manifest"];

    let report = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 2,
        })
        .unwrap();

    assert_eq!(report.observed_version, 3);
    assert_eq!(report.deleted_manifest_versions, vec![0, 1]);
    assert_eq!(report.deleted_manifest_bytes, expected_bytes);
    assert_eq!(
        manifest_sizes(&backend).into_keys().collect::<Vec<_>>(),
        vec![
            "_versions/00000000000000000002.manifest".to_string(),
            "_versions/00000000000000000003.manifest".to_string(),
        ]
    );
    assert_eq!(backend.list("data/").unwrap(), data_before);
}

#[test]
fn pruning_preserves_an_active_historical_snapshot_manifest() {
    // Break caught: deleting a historical manifest while its immutable
    // snapshot is still live invalidates that snapshot's retained history.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("active-snapshot");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let historical = dataset.snapshot();
    commit_id(&dataset, 2);
    let backend = LocalFs::new(&dir);

    let report = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert_eq!(report.deleted_manifest_versions, vec![0]);
    assert!(manifest_sizes(&backend).contains_key("_versions/00000000000000000001.manifest"));
    assert_eq!(historical.scan(&dataset.schema()).unwrap().num_rows(), 1);
}

#[test]
fn pruning_releases_a_historical_manifest_only_after_its_last_clone_drops() {
    // Break caught: treating one dropped Arc clone as the final snapshot
    // release can prune a version still retained by another snapshot owner.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("last-clone-release");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let first = dataset.snapshot();
    let second = Arc::clone(&first);
    commit_id(&dataset, 2);

    drop(first);
    let while_clone_is_live = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(while_clone_is_live.deleted_manifest_versions, vec![0]);

    drop(second);
    let after_last_clone = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(after_last_clone.deleted_manifest_versions, vec![1]);
}

#[test]
fn pruning_deletes_the_exact_unpadded_manifest_keys_selected_by_recovery() {
    // Break caught: rebuilding a padded key from a version fails to prune a
    // recovery-compatible unpadded manifest and can target the wrong key.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("unpadded-keys");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    commit_id(&dataset, 2);
    drop(dataset);

    let versions = dir.join("_versions");
    for version in 0..=2 {
        fs::rename(
            versions.join(format!("{version:020}.manifest")),
            versions.join(format!("{version}.manifest")),
        )
        .unwrap();
    }
    let dataset = Dataset::open(&dir).unwrap();
    let backend = LocalFs::new(&dir);
    let sizes = manifest_sizes(&backend);
    let expected_bytes = sizes["_versions/0.manifest"] + sizes["_versions/1.manifest"];

    let report = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert_eq!(report.deleted_manifest_versions, vec![0, 1]);
    assert_eq!(report.deleted_manifest_bytes, expected_bytes);
    assert_eq!(
        manifest_sizes(&backend).into_keys().collect::<Vec<_>>(),
        vec!["_versions/2.manifest".to_string()]
    );
}

#[test]
fn pruning_fails_closed_when_an_eligible_manifest_is_malformed() {
    // Break caught: treating an unreadable historical manifest as deletion
    // authority can destroy evidence that the retention policy cannot verify.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("malformed-authority");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let backend = LocalFs::new(&dir);
    backend
        .put("_versions/00000000000000000000.manifest", b"not-json")
        .unwrap();

    let result = dataset.prune_manifests(RetentionPolicy {
        keep_latest_versions: 1,
    });

    assert!(matches!(
        result,
        Err(TxnError::Storage(StorageError::CorruptManifest(path, _)))
            if path == dir.join("_versions/00000000000000000000.manifest")
    ));
    assert!(
        manifest_sizes(&backend).contains_key("_versions/00000000000000000000.manifest"),
        "a failed authority build must not delete the malformed manifest"
    );
}

#[cfg(feature = "chaos-injection")]
const MANIFEST_PRUNE_ABORT_CHILD_DIR_ENV: &str = "STRATA_MANIFEST_PRUNE_ABORT_CHILD_DIR";
#[cfg(feature = "chaos-injection")]
const MANIFEST_PRUNE_ABORT_CHILD_TEST: &str =
    "manifest_prune_abort_before_directory_sync_preserves_latest_recovery";

#[cfg(feature = "chaos-injection")]
#[test]
fn manifest_prune_abort_before_directory_sync_preserves_latest_recovery() {
    if let Ok(dir) = std::env::var(MANIFEST_PRUNE_ABORT_CHILD_DIR_ENV) {
        let dataset = Dataset::open(dir).unwrap();
        dataset
            .prune_manifests(RetentionPolicy {
                keep_latest_versions: 1,
            })
            .unwrap();
        return;
    }

    // Break caught: a crash after unlinking a historical manifest but before
    // its directory sync must not make the current row/index snapshot
    // unreadable, and a fresh prune must remain safe.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("manifest-prune-abort");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    commit_vector(&dataset, 1);
    commit_vector(&dataset, 2);
    let backend = LocalFs::new(&dir);
    let data_before = backend.list("data/").unwrap();
    drop(dataset);

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", MANIFEST_PRUNE_ABORT_CHILD_TEST, "--nocapture"])
        .env(MANIFEST_PRUNE_ABORT_CHILD_DIR_ENV, &dir)
        .env("STRATA_CHAOS_ABORT_AT", "1")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "the child must abort after manifest unlink before directory sync, but exited successfully: {output:?}"
    );
    assert!(
        !backend
            .list("_versions/")
            .unwrap()
            .into_iter()
            .any(|object| object.key == "_versions/00000000000000000000.manifest"),
        "checkpoint 1 must run after unlinking the oldest historical manifest"
    );

    let reopened = Dataset::open(&dir).unwrap();
    assert_eq!(reopened.current_version(), 2);
    let current = reopened.snapshot();
    assert_eq!(current.scan(&reopened.schema()).unwrap().num_rows(), 2);
    assert_eq!(
        current
            .vector_search(&[1.0, 0.0, 0.0], 2, None)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(backend.list("data/").unwrap(), data_before);

    let retry = reopened
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(retry.observed_version, 2);
    assert_eq!(retry.deleted_manifest_versions, vec![1]);
    assert_eq!(
        manifest_sizes(&backend).into_keys().collect::<Vec<_>>(),
        vec!["_versions/00000000000000000002.manifest".to_string()]
    );
    assert_eq!(backend.list("data/").unwrap(), data_before);
}

#[cfg(feature = "test-fault-injection")]
#[test]
fn retry_after_a_post_unlink_sync_error_is_safe_and_reports_no_failed_delete() {
    // Break caught: counting a delete whose post-unlink directory sync fails,
    // or retrying its missing key, makes a retention retry non-idempotent.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("delete-sync-retry");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let backend = LocalFs::new(&dir);
    let fault = strata_storage::datafile::test_support::fail_directory_sync_on_call(
        1,
        std::io::ErrorKind::Other,
    );

    let failed = dataset.prune_manifests(RetentionPolicy {
        keep_latest_versions: 1,
    });

    assert!(matches!(failed, Err(TxnError::Storage(_))));
    assert!(
        !manifest_sizes(&backend).contains_key("_versions/00000000000000000000.manifest"),
        "the injected error must occur after unlink"
    );
    drop(fault);

    let retry = dataset
        .prune_manifests(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(retry.observed_version, 1);
    assert!(retry.deleted_manifest_versions.is_empty());
    assert_eq!(retry.deleted_manifest_bytes, 0);
}
