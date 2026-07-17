//! Expression/filter API. See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` for `Predicate`,
//! the general `filter`, and file-pruning via `should_scan_file`.

use arrow::array::RecordBatch;
use arrow::error::ArrowError;

pub mod group_by;
pub mod predicate;
pub use group_by::{AggFunc, group_by};
pub use predicate::{Predicate, filter, should_scan_file};

/// Returns the rows of `batch` where `column` equals `value`. A thin
/// convenience wrapper over [`filter`] with [`Predicate::Eq`] — kept for
/// existing callers (the CLI's `filter` subcommand, the Phase 1 MVP
/// checklist test); prefer `filter` directly for new code.
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
    filter(
        batch,
        &Predicate::Eq(
            column.to_string(),
            strata_storage::Value::Utf8(value.to_string()),
        ),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

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
