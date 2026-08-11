#![allow(clippy::cast_possible_wrap, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::{Backend, LocalFs, commit_manifest, read_current};
use strata_txn::{Dataset, RetentionPolicy};

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn commit_id(dataset: &Dataset, value: u64) {
    let schema = dataset.schema();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value as i64]))]).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
}

fn commit_vector(dataset: &Dataset) {
    let schema = dataset.schema();
    let values = Arc::new(Float32Array::from(vec![1.0, 0.0, 0.0]));
    let vector = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        3,
        values,
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(vector)],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
}

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

#[test]
fn fresh_dataset_retention_plan_keeps_the_initial_manifest() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("fresh"), id_schema()).unwrap();

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert_eq!(plan.observed_version, 0);
    assert_eq!(plan.active_snapshot_versions, vec![0]);
    assert_eq!(plan.retained_manifest_versions, vec![0]);
    assert_eq!(plan.retained_data_object_count, 0);
    assert_eq!(plan.retained_data_bytes, 0);
    assert!(plan.eligible_manifest_versions.is_empty());
    assert!(plan.eligible_data_objects.is_empty());
}

#[test]
fn zero_retention_policy_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("zero"), id_schema()).unwrap();

    let error = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 0,
        })
        .unwrap_err();

    assert!(error.to_string().contains("keep_latest_versions"));
}

#[test]
fn historical_snapshot_keeps_its_manifest_until_the_last_handle_drops() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("history"), id_schema()).unwrap();
    commit_id(&dataset, 1);
    let historical = dataset.snapshot();
    commit_id(&dataset, 2);

    let held = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(held.active_snapshot_versions, vec![1, 2]);
    assert_eq!(held.retained_manifest_versions, vec![1, 2]);
    assert!(!held.eligible_manifest_versions.contains(&1));

    drop(historical);
    let released = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(released.active_snapshot_versions, vec![2]);
    assert_eq!(released.retained_manifest_versions, vec![2]);
    assert!(released.eligible_manifest_versions.contains(&1));
}

#[test]
fn cloned_historical_snapshot_stays_active_until_its_last_clone_drops() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("clones"), id_schema()).unwrap();
    commit_id(&dataset, 1);
    let first = dataset.snapshot();
    let second = Arc::clone(&first);
    commit_id(&dataset, 2);

    drop(first);
    let one_clone = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(one_clone.active_snapshot_versions, vec![1, 2]);

    drop(second);
    let no_clone = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    assert_eq!(no_clone.active_snapshot_versions, vec![2]);
}

#[test]
fn unreferenced_and_temporary_data_are_not_eligible_candidates() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("orphan");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    let backend = LocalFs::new(&dir);
    backend.put("data/orphan.bin", b"orphan-bytes").unwrap();
    backend.put("data/.tmp-orphan.bin", b"temporary").unwrap();
    let manifests_before = backend.list("_versions/").unwrap();
    let data_before = backend.list("data/").unwrap();

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert!(plan.eligible_data_objects.is_empty());
    assert_eq!(backend.list("_versions/").unwrap(), manifests_before);
    assert_eq!(backend.list("data/").unwrap(), data_before);
}

#[test]
fn older_manifest_data_is_eligible_only_when_listed_in_the_inventory() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("older-provenance");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let backend = LocalFs::new(&dir);
    let mut older_manifest = read_current(&dir).unwrap().unwrap();
    older_manifest.version = 0;
    older_manifest.data_files[0].name = "older-only.bin".to_string();
    commit_manifest(&dir, &older_manifest).unwrap();
    backend.put("data/older-only.bin", b"older-data").unwrap();

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert_eq!(
        plan.eligible_data_objects,
        vec![strata_txn::RetentionCandidate {
            key: "data/older-only.bin".to_string(),
            bytes: 10,
        }]
    );
}

#[test]
fn temporary_data_referenced_by_an_older_manifest_is_not_eligible() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("temporary-older-provenance");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let backend = LocalFs::new(&dir);
    let mut older_manifest = read_current(&dir).unwrap().unwrap();
    older_manifest.version = 0;
    older_manifest.data_files[0].name = ".tmp-older.bin".to_string();
    commit_manifest(&dir, &older_manifest).unwrap();
    backend.put("data/.tmp-older.bin", b"temporary").unwrap();

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert!(plan.eligible_data_objects.is_empty());
}

#[test]
fn missing_data_referenced_only_by_an_older_manifest_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("missing-older-data");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let mut older_manifest = read_current(&dir).unwrap().unwrap();
    older_manifest.version = 0;
    older_manifest.data_files[0].name = "missing-older.bin".to_string();
    commit_manifest(&dir, &older_manifest).unwrap();

    let error = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap_err();

    assert!(error.to_string().contains("missing"));
}

#[test]
fn missing_retained_data_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("missing-data");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let data_file = dataset.data_files().into_iter().next().unwrap().name;
    let backend = LocalFs::new(&dir);
    backend.delete(&format!("data/{data_file}")).unwrap();

    let error = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap_err();

    assert!(error.to_string().contains("missing"));
}

#[test]
fn malformed_retained_manifest_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("malformed-manifest");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    let backend = LocalFs::new(&dir);
    backend
        .put("_versions/00000000000000000000.manifest", b"not-json")
        .unwrap();

    let error = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap_err();

    assert!(error.to_string().contains("manifest"));
}

#[test]
fn captured_plan_exposes_staleness_after_a_later_commit() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("stale"), id_schema()).unwrap();
    let first = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();
    commit_id(&dataset, 1);

    assert_eq!(first.observed_version, 0);
    assert_eq!(
        dataset
            .retention_plan(RetentionPolicy {
                keep_latest_versions: 1,
            })
            .unwrap()
            .observed_version,
        1
    );
}

#[test]
fn latest_version_window_retains_only_the_newest_manifests() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("window"), id_schema()).unwrap();
    commit_id(&dataset, 1);
    commit_id(&dataset, 2);
    commit_id(&dataset, 3);

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 2,
        })
        .unwrap();

    assert_eq!(plan.observed_version, 3);
    assert_eq!(plan.retained_manifest_versions, vec![2, 3]);
    assert_eq!(plan.eligible_manifest_versions, vec![0, 1]);
}

#[test]
fn retained_vector_segments_are_counted_as_reachable_data() {
    let root = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(root.path().join("segments"), vector_schema()).unwrap();
    commit_vector(&dataset);

    let plan = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap();

    assert_eq!(plan.retained_data_object_count, 2);
    assert!(plan.retained_data_bytes > 0);
    assert!(plan.eligible_data_objects.is_empty());
}

#[test]
fn malformed_older_manifest_is_not_silently_classified_as_eligible() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("malformed-old");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    commit_id(&dataset, 1);
    let backend = LocalFs::new(&dir);
    backend
        .put("_versions/00000000000000000000.manifest", b"not-json")
        .unwrap();

    let error = dataset
        .retention_plan(RetentionPolicy {
            keep_latest_versions: 1,
        })
        .unwrap_err();

    assert!(error.to_string().contains("manifest"));
}
