//! `Predicate` — the shared vocabulary for row-level filtering (`filter`)
//! and file-level pruning (`should_scan_file`). See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §2.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq};
use arrow::error::ArrowError;
use strata_storage::{ColumnStats, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(String, Value),
    Lt(String, Value),
    LtEq(String, Value),
    Gt(String, Value),
    GtEq(String, Value),
}

impl Predicate {
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Predicate::Eq(c, _)
            | Predicate::Lt(c, _)
            | Predicate::LtEq(c, _)
            | Predicate::Gt(c, _)
            | Predicate::GtEq(c, _) => c,
        }
    }

    #[must_use]
    pub fn value(&self) -> &Value {
        match self {
            Predicate::Eq(_, v)
            | Predicate::Lt(_, v)
            | Predicate::LtEq(_, v)
            | Predicate::Gt(_, v)
            | Predicate::GtEq(_, v) => v,
        }
    }
}

/// Filters `batch` to rows matching `predicate`.
///
/// # Errors
///
/// Returns an [`ArrowError`] if `predicate`'s column doesn't exist, or if
/// its value's type doesn't match the column's actual Arrow type (the
/// underlying comparison kernel enforces this).
pub fn filter(batch: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ArrowError> {
    let idx = batch.schema_ref().index_of(predicate.column())?;
    let array = batch.column(idx);
    let mask = compare(array, predicate)?;
    filter_record_batch(batch, &mask)
}

fn compare(array: &ArrayRef, predicate: &Predicate) -> Result<BooleanArray, ArrowError> {
    let cmp_fn: fn(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> Result<BooleanArray, ArrowError> = match predicate {
        Predicate::Eq(..) => eq,
        Predicate::Lt(..) => lt,
        Predicate::LtEq(..) => lt_eq,
        Predicate::Gt(..) => gt,
        Predicate::GtEq(..) => gt_eq,
    };
    match predicate.value() {
        Value::Int64(v) => {
            let scalar = Int64Array::new_scalar(*v);
            cmp_fn(&Arc::clone(array), &scalar)
        }
        Value::Float64(v) => {
            let scalar = Float64Array::new_scalar(*v);
            cmp_fn(&Arc::clone(array), &scalar)
        }
        Value::Utf8(v) => {
            let scalar = StringArray::new_scalar(v.as_str());
            cmp_fn(&Arc::clone(array), &scalar)
        }
    }
}

/// Decides whether a file whose column stats are `stats` could possibly
/// contain a row matching `predicate`. Fails open (returns `true`)
/// whenever it can't prove otherwise — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §2. Pure
/// function, zero I/O.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn should_scan_file(stats: &HashMap<String, ColumnStats>, predicate: &Predicate) -> bool {
    let Some(col_stats) = stats.get(predicate.column()) else {
        return true; // no stats for this column - fail open, must scan
    };
    let value = predicate.value();
    // A mismatched Value variant (e.g. a Utf8 predicate value against an
    // Int64 column's stats) can't be proven to miss - fail open rather
    // than trust derived PartialOrd's cross-variant ordering, which
    // compares by declaration order, not value semantics.
    if std::mem::discriminant(value) != std::mem::discriminant(&col_stats.min) {
        return true;
    }
    match predicate {
        Predicate::Eq(..) => *value >= col_stats.min && *value <= col_stats.max,
        Predicate::Lt(..) => *value > col_stats.min,
        Predicate::LtEq(..) => *value >= col_stats.min,
        Predicate::Gt(..) => *value < col_stats.max,
        Predicate::GtEq(..) => *value <= col_stats.max,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_eq_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Eq("id".to_string(), Value::Int64(20)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 20);
    }

    #[test]
    fn filter_lt_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Lt("id".to_string(), Value::Int64(25)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 2); // 10, 20
    }

    #[test]
    fn filter_gt_eq_on_int64_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::GtEq("id".to_string(), Value::Int64(20)),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 2); // 20, 30
    }

    #[test]
    fn filter_eq_on_utf8_column() {
        let result = filter(
            &sample_batch(),
            &Predicate::Eq("name".to_string(), Value::Utf8("b".to_string())),
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn should_scan_file_prunes_when_range_cannot_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Eq(id, 50) can't match a file whose id range is [100, 200].
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(!should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_scans_when_range_could_overlap() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(150));
        assert!(should_scan_file(&stats, &predicate));
    }

    #[test]
    fn should_scan_file_fails_open_when_column_has_no_stats() {
        let stats: HashMap<String, ColumnStats> = HashMap::new();
        let predicate = Predicate::Eq("id".to_string(), Value::Int64(50));
        assert!(
            should_scan_file(&stats, &predicate),
            "a column with no stats must never be pruned - always scan"
        );
    }

    #[test]
    fn should_scan_file_fails_open_on_range_predicates() {
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(200),
            },
        );
        // Lt(id, 100): no value in [100, 200] is < 100 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::Lt("id".to_string(), Value::Int64(100))
        ));
        // Gt(id, 200): no value in [100, 200] is > 200 -> should prune.
        assert!(!should_scan_file(
            &stats,
            &Predicate::Gt("id".to_string(), Value::Int64(200))
        ));
        // GtEq(id, 200): 200 itself is in range -> must scan.
        assert!(should_scan_file(
            &stats,
            &Predicate::GtEq("id".to_string(), Value::Int64(200))
        ));
    }
}
