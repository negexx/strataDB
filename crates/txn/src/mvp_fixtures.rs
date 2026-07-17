//! Shared fixtures for the Phase 1 MVP demo schema (id: `Int64`, name: `Utf8`,
//! vector: `FixedSizeList<Float32, 3>`) — used by the CLI binary, the MVP
//! checklist integration test, and this crate's own unit tests, so the
//! schema shape has exactly one definition instead of independent copies
//! that could silently drift apart.

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

#[must_use]
pub fn mvp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]))
}

/// Builds a one-row batch in [`mvp_schema`]'s shape.
///
/// # Errors
///
/// Returns an error if Arrow's `RecordBatch::try_new` rejects the
/// constructed columns (should not happen for well-formed inputs).
pub fn mvp_row(id: i64, name: &str, vector: [f32; 3]) -> Result<RecordBatch, ArrowError> {
    mvp_batch(&[(id, name, vector)])
}

/// Builds a multi-row batch in [`mvp_schema`]'s shape.
///
/// # Errors
///
/// Returns an error if Arrow's `RecordBatch::try_new` rejects the
/// constructed columns (should not happen for well-formed inputs).
pub fn mvp_batch(rows: &[(i64, &str, [f32; 3])]) -> Result<RecordBatch, ArrowError> {
    let ids = Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
    let names = StringArray::from(rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>());
    let flat: Vec<f32> = rows.iter().flat_map(|r| r.2).collect();
    let values = Arc::new(Float32Array::from(flat));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let vectors = FixedSizeListArray::new(item_field, 3, values, None);
    RecordBatch::try_new(
        mvp_schema(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(vectors)],
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn mvp_row_and_mvp_batch_agree_on_a_single_row() {
        let single = mvp_row(1, "alice", [1.0, 2.0, 3.0]).unwrap();
        let via_batch = mvp_batch(&[(1, "alice", [1.0, 2.0, 3.0])]).unwrap();
        assert_eq!(single, via_batch);
    }

    #[test]
    fn mvp_batch_builds_multiple_rows_in_order() {
        let batch =
            mvp_batch(&[(1, "alice", [1.0, 2.0, 3.0]), (2, "bob", [4.0, 5.0, 6.0])]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }
}
