//! Per-column file statistics for pruning, per
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §1.

use std::collections::HashMap;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::kernels::aggregate::{max, max_string, min, min_string};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use serde::{Deserialize, Serialize};

/// A scalar value, shared between file statistics (this module) and
/// `strata_query::Predicate` — both need the same "which types are
/// orderable and prunable" vocabulary.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Value,
    pub max: Value,
}

/// Computes per-column min/max for every orderable column in `batch`.
/// Called on the *original, pre-encoding* batch at commit time — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §1. Columns with
/// no non-null values, or a non-orderable type (e.g. a vector column), get
/// no entry — never a wrong or placeholder one.
#[must_use]
pub fn compute_stats(batch: &RecordBatch) -> HashMap<String, ColumnStats> {
    let mut stats = HashMap::new();
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let entry = match field.data_type() {
            DataType::Int64 => column
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|arr| match (min::<Int64Type>(arr), max::<Int64Type>(arr)) {
                    (Some(min_v), Some(max_v)) => Some(ColumnStats {
                        min: Value::Int64(min_v),
                        max: Value::Int64(max_v),
                    }),
                    _ => None,
                }),
            DataType::Float64 => column
                .as_any()
                .downcast_ref::<Float64Array>()
                .and_then(
                    |arr| match (min::<Float64Type>(arr), max::<Float64Type>(arr)) {
                        (Some(min_v), Some(max_v)) => Some(ColumnStats {
                            min: Value::Float64(min_v),
                            max: Value::Float64(max_v),
                        }),
                        _ => None,
                    },
                ),
            DataType::Utf8 => column
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|arr| match (min_string(arr), max_string(arr)) {
                    (Some(min_v), Some(max_v)) => Some(ColumnStats {
                        min: Value::Utf8(min_v.to_string()),
                        max: Value::Utf8(max_v.to_string()),
                    }),
                    _ => None,
                }),
            _ => None, // not orderable (e.g. a vector column) - no stats
        };
        if let Some(entry) = entry {
            stats.insert(field.name().clone(), entry);
        }
    }
    stats
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType as DT, Field, Schema};

    use super::*;

    fn batch_with_id_and_name() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DT::Int64, false),
            Field::new("name", DT::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![30, 10, 20])),
                Arc::new(StringArray::from(vec!["banana", "apple", "cherry"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn computes_min_max_for_int64_and_utf8_columns() {
        let stats = compute_stats(&batch_with_id_and_name());

        let id_stats = stats.get("id").unwrap();
        assert_eq!(id_stats.min, Value::Int64(10));
        assert_eq!(id_stats.max, Value::Int64(30));

        let name_stats = stats.get("name").unwrap();
        assert_eq!(name_stats.min, Value::Utf8("apple".to_string()));
        assert_eq!(name_stats.max, Value::Utf8("cherry".to_string()));
    }

    #[test]
    fn non_orderable_column_gets_no_stats_entry() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DT::FixedSizeList(Arc::new(Field::new("item", DT::Float32, false)), 3),
            false,
        )]));
        let values = Arc::new(arrow::array::Float32Array::from(vec![1.0, 2.0, 3.0]));
        let item_field = Arc::new(Field::new("item", DT::Float32, false));
        let vectors = arrow::array::FixedSizeListArray::new(item_field, 3, values, None);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(vectors)]).unwrap();

        let stats = compute_stats(&batch);
        assert!(
            !stats.contains_key("vector"),
            "non-orderable column must get no stats entry, not a wrong one"
        );
    }

    #[test]
    fn all_null_column_gets_no_stats_entry() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DT::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None, None, None]))],
        )
        .unwrap();

        let stats = compute_stats(&batch);
        assert!(
            !stats.contains_key("id"),
            "all-null column must get no stats entry (no meaningful min/max)"
        );
    }
}
