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

    fn finish(self) -> f64 {
        match self {
            Self::Count(n) => {
                #[allow(clippy::cast_precision_loss)]
                let n = n as f64;
                n
            }
            Self::Sum(s) | Self::Min(s) | Self::Max(s) => s,
            Self::Avg { sum, count } => {
                #[allow(clippy::cast_precision_loss)]
                let count = count as f64;
                sum / count
            }
        }
    }
}

/// Groups `batch` by `group_cols` and computes `aggs` per group. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
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

    let mut groups: HashMap<OwnedRow, Vec<Accumulator>> = HashMap::new();
    for i in 0..batch.num_rows() {
        let key = rows.row(i).owned();
        let accs = groups.entry(key).or_insert_with(|| {
            agg_arrays
                .iter()
                .map(|(_, f)| Accumulator::new(*f))
                .collect()
        });
        for (acc, (arr, func)) in accs.iter_mut().zip(&agg_arrays) {
            if arr.is_null(i) {
                continue;
            }
            if matches!(func, AggFunc::Count) {
                acc.update(0.0); // value is unused by Accumulator::Count
                continue;
            }
            let col = arr
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| ArrowError::CastError("expected Float64 after cast".to_string()))?;
            acc.update(col.value(i));
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

    let mut agg_columns: Vec<Vec<f64>> = vec![Vec::with_capacity(groups.len()); aggs.len()];
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
        if matches!(func, AggFunc::Count) {
            fields.push(Field::new(
                format!("{name}_{func:?}").to_lowercase(),
                DataType::Int64,
                false,
            ));
            #[allow(clippy::cast_possible_truncation)]
            let counts: Vec<i64> = values.iter().map(|v| *v as i64).collect();
            columns.push(Arc::new(Int64Array::from(counts)));
        } else {
            fields.push(Field::new(
                format!("{name}_{func:?}").to_lowercase(),
                DataType::Float64,
                false,
            ));
            columns.push(Arc::new(Float64Array::from(values)));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
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
}
