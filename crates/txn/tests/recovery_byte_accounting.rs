#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::read_current;
use strata_txn::Dataset;

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

fn id_batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(id_schema(), vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

fn vector_batch() -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let vectors = FixedSizeListArray::new(
        item,
        3,
        Arc::new(Float32Array::from(vec![7.0, 0.0, 1.0])),
        None,
    );
    RecordBatch::try_new(
        vector_schema(),
        vec![Arc::new(Int64Array::from(vec![7])), Arc::new(vectors)],
    )
    .unwrap()
}

fn listed_bytes(dir: &Path, names: impl IntoIterator<Item = String>) -> u64 {
    names
        .into_iter()
        .map(|name| {
            std::fs::metadata(dir.join("data").join(name))
                .unwrap()
                .len()
        })
        .sum()
}

fn row_id_catalog_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir.join("_meta").join("row-id-high-water"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum()
}

fn assert_accounting_matches_manifest_listed_files(dir: &Path) {
    let manifest = read_current(dir).unwrap().unwrap();
    let (reopened, accounting) = Dataset::open_with_recovery_accounting(dir).unwrap();
    let manifest_path = dir
        .join("_versions")
        .join(format!("{:020}.manifest", manifest.version));

    assert_eq!(
        accounting.manifest_bytes,
        u128::from(std::fs::metadata(manifest_path).unwrap().len())
    );
    assert_eq!(
        accounting.row_data_bytes,
        u128::from(listed_bytes(
            dir,
            manifest.data_files.into_iter().map(|entry| entry.name)
        ))
    );
    assert_eq!(
        accounting.row_id_catalog_bytes,
        u128::from(row_id_catalog_bytes(dir))
    );
    assert_eq!(
        accounting.segment_bytes,
        u128::from(listed_bytes(
            dir,
            manifest.segments.into_iter().map(|entry| entry.name)
        ))
    );
    assert_eq!(
        accounting.total_bytes(),
        accounting.manifest_bytes
            + accounting.row_data_bytes
            + accounting.row_id_catalog_bytes
            + accounting.segment_bytes
    );
    drop(reopened);
}

#[test]
fn recovery_accounting_for_an_empty_dataset_is_deterministic_and_exact() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("empty");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    drop(dataset);

    let (_, first) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    let (_, second) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    assert_eq!(
        first, second,
        "the same on-disk recovery state must account identically"
    );
    assert_eq!(first.row_data_bytes, 0);
    assert_eq!(first.segment_bytes, 0);
    assert_accounting_matches_manifest_listed_files(&dir);
}

#[test]
fn recovery_accounting_includes_manifest_listed_row_and_segment_files() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("small-vector");
    let dataset = Dataset::create(&dir, vector_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(vector_batch()).unwrap();
    transaction.commit().unwrap();
    drop(dataset);

    let (_, accounting) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    assert!(
        accounting.row_data_bytes > 0,
        "the committed Arrow row file must be counted"
    );
    assert!(
        accounting.segment_bytes > 0,
        "the manifest-listed immutable segment must be counted"
    );
    assert_accounting_matches_manifest_listed_files(&dir);
}

#[test]
fn recovery_accounting_covers_retained_history_and_grows_with_currently_listed_files() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("retained-history");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    for value in 0..4 {
        let mut transaction = dataset.begin();
        transaction.insert(id_batch(value)).unwrap();
        transaction.commit().unwrap();
    }
    drop(dataset);

    let (_, accounting) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    assert!(accounting.row_data_bytes > 0);
    assert!(
        accounting.row_id_catalog_bytes >= 5 * 12,
        "creation plus four commits retain immutable row-ID reservations"
    );
    assert_accounting_matches_manifest_listed_files(&dir);
}

#[test]
fn recovery_accounting_covers_a_bounded_larger_dataset() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("larger");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    for value in 0..16 {
        let mut transaction = dataset.begin();
        transaction.insert(id_batch(value)).unwrap();
        transaction.commit().unwrap();
    }
    drop(dataset);

    let (_, accounting) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    assert!(
        accounting.row_data_bytes > 16,
        "the bounded larger dataset must inspect every listed row file"
    );
    assert_accounting_matches_manifest_listed_files(&dir);
}
