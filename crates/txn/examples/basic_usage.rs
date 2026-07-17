//! Minimal end-to-end walkthrough: create a dataset, insert rows with a
//! vector column, commit, then scan and vector-search the result.
//!
//! Run with: `cargo run --example basic_usage -p strata-txn`

use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_txn::Dataset;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::temp_dir().join(format!("strata-example-basic-usage-{}", std::process::id()));
    let dataset = Dataset::create(&dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let values = Arc::new(Float32Array::from(vec![
        0.0, 0.0, 0.0, // row 0
        1.0, 0.0, 0.0, // row 1
        9.0, 9.0, 9.0, // row 2
    ]));
    let vectors = Arc::new(arrow::array::FixedSizeListArray::new(
        item_field, 3, values, None,
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, vectors])?;

    let mut txn = dataset.begin();
    txn.insert(batch);
    txn.commit()?;

    println!("committed version: {}", dataset.current_version());

    let scanned = dataset.snapshot().scan(&schema)?;
    println!("scanned {} row(s)", scanned.num_rows());

    let nearest = dataset
        .snapshot()
        .vector_search(&[0.0, 0.0, 0.0], 1, None)?;
    println!("nearest neighbor to [0,0,0]: row-id {}", nearest[0].row_id);

    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}
