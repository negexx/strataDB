//! Hash-based `GROUP BY`, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §2.
//!
//! Internals per
//! `docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md`:
//! a `HashMap<Row<'_>, usize>` index over the `Rows` buffer already built
//! for the whole batch, instead of a fresh `OwnedRow` heap allocation per
//! row, plus columnar (`Vec<T>`-per-`group_idx`) accumulator state instead
//! of one small heap-allocated `Vec<Accumulator>` per group.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::row::{Row, RowConverter, SortField};

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

/// Dense, group-indexed aggregate state for one `(column, AggFunc)` pair:
/// all groups' running state for one aggregate lives in one contiguous,
/// type-specialized vector, indexed by `group_idx`, instead of one small
/// `Accumulator` instance existing per group. Same identity values and
/// update/finish math as the scalar accumulator this replaces.
#[derive(Debug)]
enum ColumnarAccumulator {
    Count(Vec<u64>),
    Sum(Vec<f64>),
    Min(Vec<f64>),
    Max(Vec<f64>),
    Avg { sum: Vec<f64>, count: Vec<u64> },
}

impl ColumnarAccumulator {
    fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count(Vec::new()),
            AggFunc::Sum => Self::Sum(Vec::new()),
            AggFunc::Min => Self::Min(Vec::new()),
            AggFunc::Max => Self::Max(Vec::new()),
            AggFunc::Avg => Self::Avg {
                sum: Vec::new(),
                count: Vec::new(),
            },
        }
    }

    /// Appends this variant's identity element -- called for every
    /// requested aggregate the moment a new group is discovered, so
    /// `group_idx` is always a valid index into every `ColumnarAccumulator`
    /// afterward, regardless of which columns are null on the row that
    /// discovered the group.
    fn push_identity(&mut self) {
        match self {
            Self::Count(v) => v.push(0),
            Self::Sum(v) => v.push(0.0),
            Self::Min(v) => v.push(f64::INFINITY),
            Self::Max(v) => v.push(f64::NEG_INFINITY),
            Self::Avg { sum, count } => {
                sum.push(0.0);
                count.push(0);
            }
        }
    }

    fn update(&mut self, group_idx: usize, value: f64) {
        match self {
            Self::Count(v) => v[group_idx] += 1,
            Self::Sum(v) => v[group_idx] += value,
            Self::Min(v) => v[group_idx] = v[group_idx].min(value),
            Self::Max(v) => v[group_idx] = v[group_idx].max(value),
            Self::Avg { sum, count } => {
                sum[group_idx] += value;
                count[group_idx] += 1;
            }
        }
    }

    /// Consumes the whole vector at once, mapping every group's finished
    /// value -- same per-variant math as the scalar accumulator's
    /// `finish()` this replaces.
    fn finish_all(self) -> Vec<AggValue> {
        match self {
            Self::Count(v) => v
                .into_iter()
                .map(|n| {
                    // Counts cannot realistically exceed i64::MAX in an in-memory batch.
                    #[allow(clippy::cast_possible_wrap)]
                    let n = n as i64;
                    AggValue::Int(n)
                })
                .collect(),
            Self::Sum(v) | Self::Min(v) | Self::Max(v) => {
                v.into_iter().map(AggValue::Float).collect()
            }
            Self::Avg { sum, count } => sum
                .into_iter()
                .zip(count)
                .map(|(s, c)| {
                    #[allow(clippy::cast_precision_loss)]
                    let c = c as f64;
                    AggValue::Float(s / c)
                })
                .collect(),
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

    // group_index_of maps a *borrowed* row (a zero-allocation view into
    // `rows`) to its group index -- unlike a `HashMap<OwnedRow, _>`, this
    // needs no fresh heap allocation on every input row, only lazily on the
    // O(distinct groups) subset that turn out to be new. `Row<'_>` already
    // implements `Hash`/`Eq`/`Copy` purely over its byte slice (confirmed
    // against arrow-row 58.3.0's source), so no custom hashing/probing is
    // needed.
    let mut group_index_of: HashMap<Row<'_>, usize> = HashMap::new();
    let mut group_key_rows: Vec<Row<'_>> = Vec::new();
    let mut state: Vec<ColumnarAccumulator> = aggs
        .iter()
        .map(|(_, f)| ColumnarAccumulator::new(*f))
        .collect();

    for i in 0..batch.num_rows() {
        let row = rows.row(i);
        let group_idx = *group_index_of.entry(row).or_insert_with(|| {
            let idx = group_key_rows.len();
            group_key_rows.push(row);
            for acc in &mut state {
                acc.push_identity();
            }
            idx
        });
        for (agg_idx, (arr, func)) in agg_arrays.iter().enumerate() {
            if arr.is_null(i) {
                continue;
            }
            if matches!(func, AggFunc::Count) {
                state[agg_idx].update(group_idx, 0.0); // value unused by Count's update
                continue;
            }
            if let Some(col) = agg_float_arrays[agg_idx] {
                state[agg_idx].update(group_idx, col.value(i));
            }
        }
    }

    build_result_batch(group_cols, aggs, &converter, group_key_rows, state)
}

fn build_result_batch(
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
    converter: &RowConverter,
    group_key_rows: Vec<Row<'_>>,
    state: Vec<ColumnarAccumulator>,
) -> Result<RecordBatch, ArrowError> {
    let group_columns = converter.convert_rows(group_key_rows)?;

    // The output field's type comes from `group_columns` — the array
    // RowConverter::convert_rows actually produced — not from the original
    // (possibly dictionary-encoded) input array. convert_rows decodes
    // dictionary-encoded row keys back to the dictionary's plain value
    // type, so using the original array's data type here would build a
    // schema RecordBatch::try_new then rejects as a type mismatch. This
    // also means GROUP BY output on a dictionary-encoded column is
    // plain-typed (e.g. Utf8, not Dictionary(Int32, Utf8)) — matching how
    // most aggregation engines behave, since GROUP BY output rows are
    // already deduplicated and re-encoding them as a dictionary buys
    // nothing.
    let mut fields: Vec<Field> = group_cols
        .iter()
        .zip(&group_columns)
        .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
        .collect();
    let mut columns: Vec<ArrayRef> = group_columns;
    for ((name, func), acc) in aggs.iter().zip(state) {
        let (field, array) = finish_agg_column(acc.finish_all(), name, *func);
        fields.push(field);
        columns.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
}

/// Converts one aggregate column's finished values into an Arrow field +
/// array, per [`AggFunc`]: `Count` produces `Int64`, every other `AggFunc`
/// produces `Float64` — both `unreachable!()` arms below hold because
/// `ColumnarAccumulator::finish_all()` guarantees that mapping.
fn finish_agg_column(values: Vec<AggValue>, name: &str, func: AggFunc) -> (Field, ArrayRef) {
    let field_name = format!("{name}_{func:?}").to_lowercase();
    if matches!(func, AggFunc::Count) {
        let counts: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                AggValue::Int(n) => n,
                // ColumnarAccumulator::finish_all()'s Count arm guarantees this.
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
                // ColumnarAccumulator::finish_all()'s non-Count arms guarantee this.
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
    fn group_by_accepts_a_dictionary_encoded_group_column() {
        use arrow::array::DictionaryArray;
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
        assert_eq!(
            result.schema_ref().field(0).data_type(),
            &DataType::Utf8,
            "GROUP BY output for a dictionary-encoded input column must be plain-typed, \
             not re-encoded as a dictionary"
        );
    }

    #[test]
    fn group_by_accepts_a_dictionary_encoded_group_column_with_a_null_entry() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Covers the composition of this fix with should_dictionary_encode's
        // null-handling fix (crates/storage/src/encoding.rs): a nullable,
        // dictionary-encoded column being grouped, which neither fix's own
        // test exercised on its own.
        let categories: DictionaryArray<Int32Type> = vec![Some("a"), None, Some("a"), Some("b")]
            .into_iter()
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
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
        assert_eq!(result.num_rows(), 3, "expected 3 groups: a, b, and null");
        assert_eq!(
            result.schema_ref().field(0).data_type(),
            &DataType::Utf8,
            "output must stay plain-typed even for a nullable dictionary-encoded input"
        );
        assert_eq!(
            result.column(0).null_count(),
            1,
            "the null value must form its own group, not be dropped or merged"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn high_cardinality_grouping_matches_a_naive_reference_including_hash_collisions() {
        // 5,000 rows, 2,500 distinct groups (2 rows/group average) -- enough
        // distinct group keys to force real hash collisions in the
        // implementation's lookup structure, unlike the 2-5-group
        // hand-written tests above, which never stress it meaningfully.
        // Guards the Phase A rewrite described in
        // docs/superpowers/specs/2026-07-19-group-by-phase-a-optimization-design.md.
        const ROW_COUNT: usize = 5_000;
        const CARDINALITY: i64 = 2_500;

        let categories: Vec<String> = (0..ROW_COUNT)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i = i as i64;
                format!("group-{}", i % CARDINALITY)
            })
            .collect();
        let amounts: Vec<i64> = (0..ROW_COUNT)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i = i as i64;
                i % 997
            })
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(categories.clone())),
                Arc::new(Int64Array::from(amounts.clone())),
            ],
        )
        .unwrap();

        let result = group_by(
            &batch,
            &["category"],
            &[
                ("amount", AggFunc::Count),
                ("amount", AggFunc::Sum),
                ("amount", AggFunc::Min),
                ("amount", AggFunc::Max),
            ],
        )
        .unwrap();

        // Independent, deliberately naive reference -- plain HashMap over
        // materialized native values, no RowConverter/Row involved, so it
        // can't share a bug with the implementation under test.
        let mut reference: std::collections::HashMap<String, (i64, i64, i64, i64)> =
            std::collections::HashMap::new();
        for i in 0..ROW_COUNT {
            let entry =
                reference
                    .entry(categories[i].clone())
                    .or_insert((0, 0, i64::MAX, i64::MIN));
            entry.0 += 1; // count
            entry.1 += amounts[i]; // sum
            entry.2 = entry.2.min(amounts[i]); // min
            entry.3 = entry.3.max(amounts[i]); // max
        }

        let result_categories = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let result_counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let result_sums = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let result_mins = result
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let result_maxes = result
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(
            result.num_rows(),
            reference.len(),
            "group count mismatch: expected {} distinct groups",
            reference.len()
        );

        // Order-independent: build both sides as sets keyed by group,
        // since group_by's output row order has never been guaranteed
        // (see the design doc) -- every amount here is a small integer
        // (i % 997), so sum/min/max are always exact integers with no
        // fractional part, making the f64 -> i64 cast below lossless.
        let mut got: std::collections::HashSet<(String, i64, i64, i64, i64)> =
            std::collections::HashSet::new();
        for i in 0..result.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            let sum = result_sums.value(i) as i64;
            #[allow(clippy::cast_possible_truncation)]
            let min = result_mins.value(i) as i64;
            #[allow(clippy::cast_possible_truncation)]
            let max = result_maxes.value(i) as i64;
            got.insert((
                result_categories.value(i).to_string(),
                result_counts.value(i),
                sum,
                min,
                max,
            ));
        }
        let expected: std::collections::HashSet<(String, i64, i64, i64, i64)> = reference
            .into_iter()
            .map(|(cat, (count, sum, min, max))| (cat, count, sum, min, max))
            .collect();

        assert_eq!(
            got, expected,
            "high-cardinality grouped output must exactly match the naive reference"
        );
    }
}
