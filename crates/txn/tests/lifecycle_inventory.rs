//! End-to-end coverage for read-only lifecycle inventory diagnostics.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::{Backend, LocalFs, StorageError, commit_manifest, read_current};
use strata_txn::{Dataset, TxnError};

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn vector_schema() -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector", DataType::FixedSizeList(item, 3), false),
    ]))
}

fn id_batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        id_schema(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn vector_batch(id: i64, vector: [f32; 3]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let vectors =
        FixedSizeListArray::new(item, 3, Arc::new(Float32Array::from(vector.to_vec())), None);
    RecordBatch::try_new(
        vector_schema(),
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(vectors)],
    )
    .unwrap()
}

fn commit(dataset: &Dataset, batch: RecordBatch) {
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
}

#[test]
fn fresh_dataset_inventories_its_durable_initial_manifest() {
    // Break caught: omitting the initial manifest, or treating it as data,
    // would hide a durable object or misclassify an empty dataset.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("fresh");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();

    let report = dataset.lifecycle_report().unwrap();

    assert_eq!(report.observed_version(), 0);
    assert_eq!(report.manifest_object_count(), 1);
    assert!(report.manifest_bytes() > 0);
    assert_eq!(
        report.current_manifest_bytes(),
        Some(report.manifest_bytes())
    );
    assert_eq!(report.data_object_count(), 0);
    assert_eq!(report.data_bytes(), 0);
    assert_eq!(report.reachable_data_file_count(), 0);
    assert_eq!(report.reachable_data_file_bytes(), 0);
    assert_eq!(report.reachable_segment_count(), 0);
    assert_eq!(report.reachable_segment_bytes(), 0);
    assert_eq!(report.orphan_candidate_count(), 0);
    assert_eq!(report.orphan_candidate_bytes(), 0);
    assert_eq!(report.tombstone_count(), 0);
    assert_eq!(report.physical_row_count(), 0);
}

#[test]
fn current_manifest_bytes_uses_an_unpadded_recovery_recognized_manifest_key() {
    // Break caught: reconstructing the canonical padded key would omit the
    // byte count for a valid current manifest that recovery selected by its
    // numeric version after its filename was unpadded.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("unpadded-current-manifest");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    for value in 1..=7 {
        commit(&dataset, id_batch(&[value]));
    }

    let padded = dir.join("_versions/00000000000000000007.manifest");
    let unpadded = dir.join("_versions/7.manifest");
    let expected_bytes = fs::metadata(&padded).unwrap().len();
    fs::rename(padded, &unpadded).unwrap();

    let report = dataset.lifecycle_report().unwrap();

    assert_eq!(report.observed_version(), 7);
    assert_eq!(report.current_manifest_bytes(), Some(expected_bytes));
}

#[test]
fn duplicate_padded_and_unpadded_manifest_versions_fail_closed() {
    // Break caught: choosing either filename when two listed manifest keys
    // resolve to the same numeric version would hide duplicate authority.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("duplicate-manifest-version");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    let padded = dir.join("_versions/00000000000000000000.manifest");
    let unpadded = dir.join("_versions/0.manifest");
    fs::copy(&padded, unpadded).unwrap();

    let result = dataset.lifecycle_report();

    assert!(matches!(
        result,
        Err(TxnError::Storage(StorageError::CorruptManifest(path, reason)))
            if path == Path::new("_versions/00000000000000000000.manifest")
                && reason == "duplicate listed manifest version"
    ));
}

#[test]
fn row_commit_inventories_its_reachable_file_and_physical_rows() {
    // Break caught: omitting a manifest-listed row file, or reporting its
    // metadata instead of its payload length, under-counts durable row data.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("row-commit");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit(&dataset, id_batch(&[11, 12]));

    let snapshot = dataset.snapshot();
    let data_files = snapshot.data_files();
    assert_eq!(data_files.len(), 1);
    let file = &data_files[0];
    assert_eq!(file.row_count, 2);
    assert!(file.byte_len > 0);

    let report = dataset.lifecycle_report().unwrap();
    assert_eq!(report.observed_version(), 1);
    assert_eq!(report.data_object_count(), 1);
    assert_eq!(report.data_bytes(), file.byte_len);
    assert_eq!(report.reachable_data_file_count(), 1);
    assert_eq!(report.reachable_data_file_bytes(), file.byte_len);
    assert_eq!(report.reachable_segment_count(), 0);
    assert_eq!(report.physical_row_count(), 2);
}

#[test]
fn vector_commit_counts_its_segment_separately_from_its_row_file() {
    // Break caught: folding immutable segment bytes into row-file accounting,
    // or marking either committed object as an orphan, hides lifecycle growth.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vector-commit");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    commit(&dataset, vector_batch(7, [1.0, 2.0, 3.0]));

    let row_file_bytes = dataset.snapshot().data_files()[0].byte_len;
    let report = dataset.lifecycle_report().unwrap();

    assert_eq!(report.data_object_count(), 2);
    assert_eq!(report.reachable_data_file_count(), 1);
    assert_eq!(report.reachable_data_file_bytes(), row_file_bytes);
    assert_eq!(report.reachable_segment_count(), 1);
    assert!(report.reachable_segment_bytes() > 0);
    assert_eq!(
        report.data_bytes(),
        report.reachable_data_file_bytes() + report.reachable_segment_bytes()
    );
    assert_eq!(report.orphan_candidate_count(), 0);
    assert_eq!(report.orphan_candidate_bytes(), 0);
}

#[test]
fn later_commits_retain_manifest_history_and_current_reachable_row_files() {
    // Break caught: inventorying only the current manifest object, or dropping
    // an older current row file from reachability, hides retained history.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("multi-commit");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit(&dataset, id_batch(&[1]));
    let after_first_commit = dataset.lifecycle_report().unwrap();

    commit(&dataset, id_batch(&[2]));
    let report = dataset.lifecycle_report().unwrap();
    let snapshot = dataset.snapshot();
    let current_data_files = snapshot.data_files();
    let current_data_bytes = current_data_files
        .iter()
        .try_fold(0_u64, |total, entry| total.checked_add(entry.byte_len))
        .unwrap();

    assert_eq!(after_first_commit.manifest_object_count(), 2);
    assert_eq!(report.observed_version(), 2);
    assert_eq!(report.manifest_object_count(), 3);
    assert!(report.manifest_bytes() > after_first_commit.manifest_bytes());
    assert_eq!(current_data_files.len(), 2);
    assert_eq!(report.reachable_data_file_count(), 2);
    assert_eq!(report.reachable_data_file_bytes(), current_data_bytes);
    assert_eq!(report.physical_row_count(), 2);
}

#[test]
fn unreferenced_preparation_object_is_only_an_orphan_candidate() {
    // Break caught: treating a physical object written before manifest
    // publication as reachable would make diagnostics authorize unsafe cleanup.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("orphan-candidate");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    let bytes = b"left behind before manifest publication";
    let byte_count = u64::try_from(bytes.len()).unwrap();
    LocalFs::new(&dir)
        .put("data/preparation-leftover.bin", bytes)
        .unwrap();

    let report = dataset.lifecycle_report().unwrap();

    assert_eq!(dataset.snapshot().data_files().len(), 0);
    assert_eq!(report.observed_version(), 0);
    assert_eq!(report.data_object_count(), 1);
    assert_eq!(report.data_bytes(), byte_count);
    assert_eq!(report.reachable_data_file_count(), 0);
    assert_eq!(report.reachable_data_file_bytes(), 0);
    assert_eq!(report.reachable_segment_count(), 0);
    assert_eq!(report.orphan_candidate_count(), 1);
    assert_eq!(report.orphan_candidate_bytes(), byte_count);
}

#[test]
fn missing_reachable_object_returns_a_typed_error_instead_of_an_orphan_report() {
    // Break caught: downgrading a manifest-listed missing row file to an
    // orphan candidate would conceal manifest corruption from operators.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("missing-reachable");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit(&dataset, id_batch(&[9]));
    let name = dataset.snapshot().data_files()[0].name.clone();
    LocalFs::new(&dir).delete(&format!("data/{name}")).unwrap();

    let result = dataset.lifecycle_report();

    match result {
        Err(TxnError::Storage(StorageError::CorruptManifest(path, reason))) => {
            assert_eq!(
                path,
                PathBuf::from("_versions/00000000000000000001.manifest")
            );
            assert_eq!(
                reason,
                format!("reachable object is missing from inventory: data/{name}")
            );
        }
        other => panic!("expected a typed missing-reachable-object error, got {other:?}"),
    }
}

#[test]
fn unsafe_manifest_name_is_rejected_on_open_instead_of_reclassified() {
    // Break caught: accepting a traversal-bearing manifest name could turn a
    // corrupted reachable reference into an unrelated inventory candidate.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("unsafe-name");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit(&dataset, id_batch(&[13]));
    drop(dataset);

    let mut manifest = read_current(&dir).unwrap().unwrap();
    manifest.version = 2;
    manifest.data_files[0].name = "../escaped.arrow".to_string();
    commit_manifest(&dir, &manifest).unwrap();

    let result = Dataset::open(&dir);

    assert!(matches!(
        result,
        Err(TxnError::UnsafeManifestPath(path)) if path == "../escaped.arrow"
    ));
}

#[test]
fn captured_report_remains_unchanged_after_a_later_threaded_commit() {
    // Break caught: sharing mutable report state with the live dataset would
    // let a later commit rewrite a report already returned to a caller.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("later-commit");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    let captured = dataset.lifecycle_report().unwrap();
    let expected_captured = captured.clone();

    let writer_dataset = dataset.clone();
    let writer = std::thread::spawn(move || commit(&writer_dataset, id_batch(&[21])));
    writer.join().unwrap();

    let latest = dataset.lifecycle_report().unwrap();
    assert_eq!(captured, expected_captured);
    assert_eq!(captured.observed_version(), 0);
    assert_eq!(captured.manifest_object_count(), 1);
    assert_eq!(captured.data_object_count(), 0);
    assert_eq!(captured.data_bytes(), 0);
    assert_eq!(latest.observed_version(), 1);
    assert_eq!(latest.manifest_object_count(), 2);
    assert_eq!(latest.reachable_data_file_count(), 1);
}
