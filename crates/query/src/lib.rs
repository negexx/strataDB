//! Expression/filter API. Phase 1 only needs a full scan (that's
//! `strata_txn::Dataset::scan`) and an equality predicate on a string
//! column — real vectorized scan/filter/agg and predicate pushdown are
//! Phase 2/3 work, see `.claude/docs/architecture.md`'s roadmap.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::eq;
use arrow::error::ArrowError;

/// Returns the rows of `batch` where `column` equals `value`.
///
/// # Errors
///
/// Returns an [`ArrowError`] if `column` doesn't exist or isn't a UTF-8
/// string column.
pub fn filter_eq(
    batch: &RecordBatch,
    column: &str,
    value: &str,
) -> Result<RecordBatch, ArrowError> {
    let idx = batch.schema_ref().index_of(column)?;
    let array = batch.column(idx);
    let scalar = StringArray::new_scalar(value);
    let predicate = eq(&Arc::clone(array), &scalar)?;
    filter_record_batch(batch, &predicate)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use arrow::array::{Int64Array, StringArray as StrArr};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn filter_eq_keeps_only_matching_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StrArr::from(vec!["a", "b", "a"])),
            ],
        )
        .unwrap();

        let filtered = filter_eq(&batch, "name", "a").unwrap();
        assert_eq!(filtered.num_rows(), 2);
        let ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3]);
    }
}
