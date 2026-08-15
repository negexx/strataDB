#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_storage::{SchemaMigration, StorageError};
use strata_txn::{Dataset, TxnError};

fn vector_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]))
}

fn vector_batch() -> RecordBatch {
    let ids = Arc::new(Int64Array::from(vec![7_i64, 8, 9]));
    let values = Arc::new(Float32Array::from(vec![
        1.0_f32, 2.0, 3.0, 20.0, 21.0, 22.0, 40.0, 41.0, 42.0,
    ]));
    let vectors = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        3,
        values,
        None,
    ));
    RecordBatch::try_new(vector_schema(), vec![ids, vectors]).unwrap()
}

fn add_nullable_tag(source_version: u32, target_version: u32) -> SchemaMigration {
    SchemaMigration::add_nullable_column(
        source_version,
        target_version,
        Field::new("tag", DataType::Utf8, true),
    )
}

fn assert_reserved_column_migration_fails_before_replacement_writes(name: &str) {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("dataset");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(vector_batch()).unwrap();
    transaction.commit().unwrap();
    let manifest_before =
        std::fs::read(dir.join("_versions").join("00000000000000000001.manifest")).unwrap();
    let objects_before = std::fs::read_dir(dir.join("data"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();

    let result = dataset.migrate_schema(&SchemaMigration::add_nullable_column(
        1,
        2,
        Field::new(name, DataType::Utf8, true),
    ));

    assert!(
        matches!(result, Err(TxnError::ReservedColumnName(ref rejected)) if rejected == name),
        "reserved columns must fail validation before migration writes: {result:?}"
    );
    assert_eq!(dataset.current_version(), 1);
    assert_eq!(
        std::fs::read(dir.join("_versions").join("00000000000000000001.manifest")).unwrap(),
        manifest_before,
        "a rejected migration must leave the current manifest bytes unchanged"
    );
    assert_eq!(
        std::fs::read_dir(dir.join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>(),
        objects_before,
        "a rejected migration must not create replacement row or segment objects"
    );
}

#[test]
fn migration_rejects_reserved_row_id_before_creating_replacement_objects() {
    assert_reserved_column_migration_fails_before_replacement_writes("_row_id");
}

#[test]
fn migration_rejects_reserved_timestamp_before_creating_replacement_objects() {
    assert_reserved_column_migration_fails_before_replacement_writes("_timestamp");
}

#[test]
fn migration_rewrites_data_and_segments_then_preserves_old_snapshot_and_reopen() {
    // Break caught: publishing only a changed schema would leave the new
    // manifest pointing at row files that still carry the old physical schema.
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("dataset");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(vector_batch()).unwrap();
    transaction.commit().unwrap();

    assert_eq!(
        dataset
            .snapshot()
            .vector_search(&[1.0, 2.0, 3.0], 1, None)
            .unwrap()
            .first()
            .map(|hit| hit.row_id),
        Some(0),
        "the pre-migration segment must be searchable so the post-migration assertion is discriminating"
    );

    let old_snapshot = dataset.snapshot();
    let old_data_names: Vec<_> = dataset
        .data_files()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    let old_segment_names: Vec<_> = dataset
        .segment_info()
        .into_iter()
        .map(|entry| entry.name)
        .collect();

    let result = dataset.migrate_schema(&add_nullable_tag(1, 2)).unwrap();

    assert_eq!(result.name, "add_nullable_column");
    assert_eq!(result.source_schema_version, 1);
    assert_eq!(result.target_schema_version, 2);
    assert_eq!(dataset.schema_version(), 2);
    assert_eq!(dataset.schema().field(2).name(), "tag");
    assert!(dataset.schema().field(2).is_nullable());
    assert!(
        dataset
            .data_files()
            .iter()
            .all(|entry| !old_data_names.contains(&entry.name)),
        "migration must publish rewritten row objects at new durable locations"
    );
    assert!(
        dataset
            .segment_info()
            .iter()
            .all(|entry| !old_segment_names.contains(&entry.name)),
        "migration must publish copied immutable segments at new durable locations"
    );
    assert_eq!(
        dataset
            .snapshot()
            .vector_search(&[1.0, 2.0, 3.0], 1, None)
            .unwrap()
            .first()
            .map(|hit| hit.row_id),
        Some(0)
    );
    assert_eq!(old_snapshot.schema().fields().len(), 2);
    assert_eq!(old_snapshot.scan(&vector_schema()).unwrap().num_rows(), 3);

    drop(dataset);
    let reopened = Dataset::open(&dir).unwrap();
    assert_eq!(reopened.schema_version(), 2);
    assert_eq!(reopened.schema().fields().len(), 3);
    assert_eq!(
        reopened
            .snapshot()
            .vector_search(&[1.0, 2.0, 3.0], 1, None)
            .unwrap()
            .first()
            .map(|hit| hit.row_id),
        Some(0)
    );
}

#[test]
fn migration_rejects_wrong_source_reverse_and_lossy_requests_without_publication() {
    // Break caught: accepting a stale, reverse, or lossy request could
    // publish a manifest whose catalog and physical rows disagree.
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("dataset");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let before = dataset.current_version();

    let wrong_source = dataset.migrate_schema(&add_nullable_tag(0, 2));
    assert!(matches!(
        wrong_source,
        Err(TxnError::Storage(
            StorageError::MigrationSourceVersion { .. }
        ))
    ));
    let reverse = dataset.migrate_schema(&add_nullable_tag(1, 0));
    assert!(matches!(
        reverse,
        Err(TxnError::Storage(
            StorageError::MigrationUnsupportedDirection { .. }
        ))
    ));
    let lossy = dataset.migrate_schema(&SchemaMigration::change_column_type(
        1,
        2,
        "id",
        DataType::Utf8,
    ));
    assert!(matches!(
        lossy,
        Err(TxnError::Storage(
            StorageError::MigrationLossyConversion { .. }
        ))
    ));
    let incompatible = dataset.migrate_schema(&SchemaMigration::add_nullable_column(
        1,
        2,
        Field::new("required", DataType::Int64, false),
    ));
    assert!(matches!(
        incompatible,
        Err(TxnError::Storage(
            StorageError::MigrationIncompatibleType { .. }
        ))
    ));
    assert_eq!(dataset.current_version(), before);
    assert_eq!(dataset.schema_version(), 1);
}

#[test]
fn transaction_captured_before_migration_cannot_publish_old_schema_rows() {
    // Break caught: a pre-migration transaction that appends its v1 row file
    // to a v2 manifest would make recovery reject the newly published state.
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("dataset");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let mut stale = dataset.begin();

    dataset.migrate_schema(&add_nullable_tag(1, 2)).unwrap();
    stale.insert(vector_batch()).unwrap();
    let result = stale.commit();
    assert!(matches!(
        result,
        Err(TxnError::Storage(StorageError::SchemaVersionChanged {
            expected: 1,
            actual: 2,
        }))
    ));
    assert_eq!(dataset.current_version(), 1);
}

#[cfg(feature = "test-fault-injection")]
#[test]
fn migration_failure_before_publication_reopens_the_prior_complete_manifest() {
    // Break caught: a migration that returns an error after writing replacement
    // objects but before manifest publication must not replace the prior
    // complete manifest selected by recovery.
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("dataset");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(vector_batch()).unwrap();
    transaction.commit().unwrap();
    assert_eq!(dataset.current_version(), 1);

    let _fault = strata_txn::dataset::test_support::fail_before_migration_manifest_publication();
    let result = dataset.migrate_schema(&add_nullable_tag(1, 2));
    assert!(matches!(result, Err(TxnError::Io(_))), "{result:?}");
    assert_eq!(dataset.current_version(), 1);
    assert_eq!(dataset.schema_version(), 1);
    drop(dataset);

    let reopened = Dataset::open(&dir).unwrap();
    assert_eq!(reopened.current_version(), 1);
    assert_eq!(reopened.schema_version(), 1);
    assert_eq!(
        reopened
            .snapshot()
            .scan(&vector_schema())
            .unwrap()
            .num_rows(),
        3,
        "recovery must select the prior complete manifest"
    );
}
