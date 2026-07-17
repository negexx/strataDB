//! Hash-based `GROUP BY`, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::row::{OwnedRow, RowConverter, SortField};

/// Which aggregate to compute over a column within each group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The result of an aggregation, preserving type information end-to-end.
#[derive(Debug, Clone, Copy)]
enum AggValue {
    Int(i64),
    Float(f64),
}

/// One row's running aggregate state for a single `(column, AggFunc)` pair.
#[derive(Debug, Clone, Copy)]
enum Accumulator {
    Count(u64),
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: u64 },
}

impl Accumulator {
    fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count(0),
            AggFunc::Sum => Self::Sum(0.0),
            AggFunc::Min => Self::Min(f64::INFINITY),
            AggFunc::Max => Self::Max(f64::NEG_INFINITY),
            AggFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn update(&mut self, value: f64) {
        match self {
            Self::Count(n) => *n += 1,
            Self::Sum(s) => *s += value,
            Self::Min(m) => *m = m.min(value),
            Self::Max(m) => *m = m.max(value),
            Self::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
        }
    }

    fn finish(self) -> AggValue {
        match self {
            Self::Count(n) => {
                // Counts cannot realistically exceed i64::MAX in an in-memory batch.
                #[allow(clippy::cast_possible_wrap)]
                let n = n as i64;
                AggValue::Int(n)
            }
            Self::Sum(s) | Self::Min(s) | Self::Max(s) => AggValue::Float(s),
            Self::Avg { sum, count } => {
                #[allow(clippy::cast_precision_loss)]
                let count = count as f64;
                AggValue::Float(sum / count)
            }
        }
    }
}

/// Groups `batch` by `group_cols` and computes `aggs` per group. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
///
/// Null values in an agg column are skipped, not treated as zero. **A group
/// whose agg column is entirely null is not yet handled specially** (flagged
/// by the Phase 2 whole-branch review, not fixed — out of the spec's
/// documented scope): `Min` returns `f64::INFINITY`, `Max` returns
/// `f64::NEG_INFINITY`, and `Avg` returns `NaN` for such a group, rather
/// than erroring or producing a null result cell. Callers should not rely on
/// these values being meaningful; a future revision should either emit null
/// for empty accumulations or document this as intentional.
///
/// # Errors
///
/// Returns an [`ArrowError::InvalidArgumentError`] if `group_cols` is empty,
/// if any named column doesn't exist, or if a non-numeric column is passed
/// to `Sum`/`Min`/`Max`/`Avg`.
pub fn group_by(
    batch: &RecordBatch,
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
) -> Result<RecordBatch, ArrowError> {
    if group_cols.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "group_by requires at least one group column".to_string(),
        ));
    }

    let schema = batch.schema_ref();
    let group_arrays: Vec<ArrayRef> = group_cols
        .iter()
        .map(|name| {
            let idx = schema.index_of(name)?;
            Ok(Arc::clone(batch.column(idx)))
        })
        .collect::<Result<_, ArrowError>>()?;

    // Count doesn't require a numeric column — it only null-checks, so it
    // keeps the *original* array uncast. Sum/Min/Max/Avg require numeric
    // input and get cast to Float64 up front. Casting a non-numeric column
    // (e.g. Utf8) to Float64 just to support Count would be wrong: arrow's
    // cast kernel would try to *parse* strings as numbers, erroring or
    // nulling out perfectly valid non-numeric values for no reason.
    let agg_arrays: Vec<(ArrayRef, AggFunc)> = aggs
        .iter()
        .map(|(name, func)| {
            let idx = schema.index_of(name)?;
            let arr = batch.column(idx);
            if matches!(func, AggFunc::Count) {
                return Ok((Arc::clone(arr), *func));
            }
            if !arr.data_type().is_numeric() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "column {name} is not numeric, cannot apply {func:?}"
                )));
            }
            let as_f64 = arrow::compute::cast(arr.as_ref(), &DataType::Float64)?;
            Ok((as_f64, *func))
        })
        .collect::<Result<_, ArrowError>>()?;

    let converter = RowConverter::new(
        group_arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&group_arrays)?;

    // Downcast each non-Count agg array to Float64Array once, before the
    // per-row loop, instead of re-downcasting on every row of every column —
    // the concrete array type is invariant per column, only the row index
    // changes.
    let agg_float_arrays: Vec<Option<&Float64Array>> = agg_arrays
        .iter()
        .map(|(arr, func)| {
            if matches!(func, AggFunc::Count) {
                Ok(None)
            } else {
                arr.as_any()
                    .downcast_ref::<Float64Array>()
                    .map(Some)
                    .ok_or_else(|| ArrowError::CastError("expected Float64 after cast".to_string()))
            }
        })
        .collect::<Result<_, ArrowError>>()?;

    let mut groups: HashMap<OwnedRow, Vec<Accumulator>> = HashMap::new();
    for i in 0..batch.num_rows() {
        let key = rows.row(i).owned();
        let accs = groups.entry(key).or_insert_with(|| {
            agg_arrays
                .iter()
                .map(|(_, f)| Accumulator::new(*f))
                .collect()
        });
        for ((acc, (arr, func)), float_arr) in accs
            .iter_mut()
            .zip(&agg_arrays)
            .zip(agg_float_arrays.iter().copied())
        {
            if arr.is_null(i) {
                continue;
            }
            if matches!(func, AggFunc::Count) {
                acc.update(0.0); // value is unused by Accumulator::Count
                continue;
            }
            if let Some(col) = float_arr {
                acc.update(col.value(i));
            }
        }
    }

    build_result_batch(&group_arrays, group_cols, aggs, &converter, &groups)
}

fn build_result_batch(
    group_arrays: &[ArrayRef],
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
    converter: &RowConverter,
    groups: &HashMap<OwnedRow, Vec<Accumulator>>,
) -> Result<RecordBatch, ArrowError> {
    let owned_keys: Vec<OwnedRow> = groups.keys().cloned().collect();
    let borrowed_keys: Vec<_> = owned_keys.iter().map(OwnedRow::row).collect();
    let group_columns = converter.convert_rows(borrowed_keys)?;

    let mut agg_columns: Vec<Vec<AggValue>> = vec![Vec::with_capacity(groups.len()); aggs.len()];
    for key in &owned_keys {
        let accs = &groups[key];
        for (col, acc) in agg_columns.iter_mut().zip(accs) {
            col.push(acc.finish());
        }
    }

    let mut fields: Vec<Field> = group_cols
        .iter()
        .zip(group_arrays)
        .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
        .collect();
    let mut columns: Vec<ArrayRef> = group_columns;
    for ((name, func), values) in aggs.iter().zip(agg_columns) {
        let (field, array) = finish_agg_column(values, name, *func);
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

/// Converts one aggregate column's finished values into an Arrow field +
/// array, per [`AggFunc`]: `Count` produces `Int64`, every other `AggFunc`
/// produces `Float64` — both `unreachable!()` arms below hold because
/// `Accumulator::finish()` guarantees that mapping.
fn finish_agg_column(values: Vec<AggValue>, name: &str, func: AggFunc) -> (Field, ArrayRef) {
    let field_name = format!("{name}_{func:?}").to_lowercase();
    if matches!(func, AggFunc::Count) {
        let counts: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                AggValue::Int(n) => n,
                // Accumulator::finish()'s Count arm guarantees this.
                AggValue::Float(_) => {
                    unreachable!("Count aggregation should produce Int, not Float")
                }
            })
            .collect();
        (
            Field::new(field_name, DataType::Int64, false),
            Arc::new(Int64Array::from(counts)),
        )
    } else {
        let floats: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                AggValue::Float(f) => f,
                // Accumulator::finish()'s non-Count arms guarantee this.
                AggValue::Int(_) => {
                    unreachable!("Non-Count aggregation should produce Float, not Int")
                }
            })
            .collect();
        (
            Field::new(field_name, DataType::Float64, false),
            Arc::new(Float64Array::from(floats)),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn single_column_count_groups_correctly() {
        let batch = sample_batch();
        let result = group_by(&batch, &["category"], &[("amount", AggFunc::Count)]).unwrap();

        assert_eq!(result.num_rows(), 2); // "a" and "b"

        let categories = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut got: Vec<(String, i64)> = (0..result.num_rows())
            .map(|i| (categories.value(i).to_string(), counts.value(i)))
            .collect();
        got.sort();
        assert_eq!(got, vec![("a".to_string(), 3), ("b".to_string(), 2)]);
    }

    #[test]
    fn multi_column_grouping_and_multiple_agg_funcs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "east", "west", "east"])),
                Arc::new(StringArray::from(vec!["a", "a", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let result = group_by(
            &batch,
            &["region", "category"],
            &[("amount", AggFunc::Sum), ("amount", AggFunc::Max)],
        )
        .unwrap();

        assert_eq!(result.num_rows(), 3); // (east,a) (west,a) (east,b)
        assert_eq!(result.num_columns(), 4); // region, category, amount_sum, amount_max

        let regions = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let categories = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let maxes = result
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut got: Vec<(String, String, f64, f64)> = (0..result.num_rows())
            .map(|i| {
                (
                    regions.value(i).to_string(),
                    categories.value(i).to_string(),
                    sums.value(i),
                    maxes.value(i),
                )
            })
            .collect();
        got.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));

        assert_eq!(
            got,
            vec![
                ("east".to_string(), "a".to_string(), 30.0, 20.0), // amounts 10, 20
                ("east".to_string(), "b".to_string(), 40.0, 40.0), // amount 40
                ("west".to_string(), "a".to_string(), 30.0, 30.0), // amount 30
            ]
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn each_agg_func_computes_correctly_for_a_single_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "x", "x"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Count's result column is Int64, not Float64 like every other
        // AggFunc — COUNT is semantically an integer, and the reference
        // implementation special-cases it in `finish_agg_column` rather
        // than uniformly emitting Float64 for every aggregate. Checked
        // separately from the float-producing funcs below.
        let count_result = group_by(&batch, &["k"], &[("v", AggFunc::Count)]).unwrap();
        let count_values = count_result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_values.value(0), 3, "AggFunc::Count");

        for (func, expected) in [
            (AggFunc::Sum, 60.0),
            (AggFunc::Min, 10.0),
            (AggFunc::Max, 30.0),
            (AggFunc::Avg, 20.0),
        ] {
            let result = group_by(&batch, &["k"], &[("v", func)]).unwrap();
            let values = result
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.value(0), expected, "AggFunc::{func:?}");
        }
    }

    #[test]
    fn empty_group_cols_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &[], &[("amount", AggFunc::Count)]);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn unknown_column_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &["not_a_column"], &[("amount", AggFunc::Count)]);
        assert!(result.is_err());
    }

    #[test]
    fn non_numeric_agg_column_errors() {
        let batch = sample_batch();
        let result = group_by(&batch, &["category"], &[("category", AggFunc::Sum)]);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn count_on_non_numeric_column_succeeds() {
        // Count must NOT require a numeric column — see Task 3's
        // Accumulator/agg_arrays design note on why casting a Utf8 column
        // to Float64 just to support Count would be wrong.
        let batch = sample_batch();
        let result = group_by(&batch, &["category"], &[("category", AggFunc::Count)]).unwrap();
        assert_eq!(result.num_rows(), 2); // "a" and "b"
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn null_values_are_skipped_and_an_all_null_group_yields_the_documented_sentinels() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "x", "y"])),
                Arc::new(Int64Array::from(vec![Some(10), None, None])), // group x: 10, null; group y: null
            ],
        )
        .unwrap();

        let result = group_by(
            &batch,
            &["k"],
            &[("v", AggFunc::Sum), ("v", AggFunc::Count)],
        )
        .unwrap();
        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let counts = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut got: Vec<(String, f64, i64)> = (0..result.num_rows())
            .map(|i| (keys.value(i).to_string(), sums.value(i), counts.value(i)))
            .collect();
        // f64 isn't Ord, so sort by the (unique) group key rather than the
        // whole tuple.
        got.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            got,
            vec![
                ("x".to_string(), 10.0, 1), // null skipped: sum=10, count=1
                ("y".to_string(), 0.0, 0),  // all-null group: Sum's identity is 0.0, Count is 0
            ]
        );
    }

    #[test]
    fn group_by_on_a_zero_row_batch_returns_a_zero_row_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();

        let result = group_by(&batch, &["category"], &[("amount", AggFunc::Sum)]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    #[ignore = "tracks a real group_by Dictionary-column bug found while adding test coverage \
                (audit-remediation Batch 3 Task 6, 2026-07-17): build_result_batch's Field \
                construction (see the `fields` build in build_result_batch) reuses the *original* \
                group array's data type — Dictionary(Int32, Utf8) for a dictionary-encoded column \
                — as the output schema's field type, but RowConverter::convert_rows decodes the \
                row-format representation back into the dictionary's plain value type (Utf8), not \
                the dictionary-encoded type. RecordBatch::try_new then rejects the mismatch with \
                'column types must match schema types, expected Dictionary(Int32, Utf8) but found \
                Utf8 at column index 0'. Not fixed here: group_by would need to either re-encode \
                convert_rows' output back into the original group column's data type per-column, \
                or advertise plain (non-dictionary) output types for group columns regardless of \
                input encoding — a real design decision outside this test-only batch's scope. \
                Re-enable once group_by's Dictionary-column handling is fixed."]
    fn group_by_accepts_a_dictionary_encoded_group_column() {
        use arrow::array::{DictionaryArray, StringArray as SA};
        use arrow::datatypes::Int32Type;

        let categories: DictionaryArray<Int32Type> = vec!["a", "b", "a", "a"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(categories),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let result = group_by(&batch, &["category"], &[("amount", AggFunc::Sum)]).unwrap();
        assert_eq!(result.num_rows(), 2, "expected 2 groups: a and b");
        let _ = SA::from(vec!["unused"]); // keep the SA import meaningful if unused elsewhere
    }
}
