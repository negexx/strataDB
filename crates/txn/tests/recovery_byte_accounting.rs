#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::{
    persist_row_id_high_water_at_least, read_current, read_row_id_high_water,
    read_row_id_high_water_with_byte_count, set_after_row_id_read_hook,
};
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

fn assert_accounting_matches_manifest_listed_files(dir: &Path) {
    let manifest = read_current(dir).unwrap().unwrap();
    let row_id_load = read_row_id_high_water_with_byte_count(dir).unwrap();
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
        accounting.row_id_catalog_bytes, row_id_load.bytes_read,
        "diagnostic accounting must use the reservation bytes decoded by the loader"
    );
    assert_eq!(
        read_row_id_high_water(dir).unwrap(),
        row_id_load.high_water,
        "the existing high-water API must retain its decoded result"
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
fn recovery_accounting_uses_the_row_id_payload_loaded_before_catalog_mutation() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("row-id-read-boundary");
    let dataset = Dataset::create(&dir, id_schema()).unwrap();
    drop(dataset);

    let loaded_payload_bytes = Arc::new(AtomicU64::new(0));
    let loaded_payload_bytes_from_hook = Arc::clone(&loaded_payload_bytes);
    let hook_dir = dir.clone();
    set_after_row_id_read_hook(move |bytes| {
        loaded_payload_bytes_from_hook.store(bytes.len().try_into().unwrap(), Ordering::Relaxed);
        assert_eq!(persist_row_id_high_water_at_least(&hook_dir, 1).unwrap(), 1);
    });

    let (_, accounting) = Dataset::open_with_recovery_accounting(&dir).unwrap();
    let mutated_catalog = read_row_id_high_water_with_byte_count(&dir).unwrap();

    assert_eq!(
        accounting.row_id_catalog_bytes,
        u128::from(loaded_payload_bytes.load(Ordering::Relaxed)),
        "diagnostic accounting must report the payload that recovery loaded"
    );
    assert!(
        mutated_catalog.bytes_read > accounting.row_id_catalog_bytes,
        "the catalog mutation after the loader read must not change diagnostic accounting"
    );
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
