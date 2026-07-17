//! Automatic dictionary encoding, per
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §1.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};

use crate::error::Result;

/// Below this distinct-value ratio (distinct / total rows), a column is
/// dictionary-encoded. Matches the range real columnar engines (Parquet)
/// default to.
const DICTIONARY_ENCODING_THRESHOLD: f64 = 0.4;

/// Casts each column of `batch` to `DictionaryArray<Int32Type>` if its
/// distinct-value ratio is below [`DICTIONARY_ENCODING_THRESHOLD`], leaving
/// higher-cardinality columns untouched. See
/// `.claude/docs/design/phase-2-encodings-and-groupby-spec.md` §1.
///
/// # Errors
///
/// Returns an error if a column's distinct-value ratio can't be computed
/// (an `arrow::row` conversion failure) or if casting a low-cardinality
/// column to a dictionary type fails.
pub fn encode_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        if should_dictionary_encode(column)? {
            let dict_type = DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(field.data_type().clone()),
            );
            let encoded = cast(column.as_ref(), &dict_type)?;
            fields.push(Field::new(field.name(), dict_type, field.is_nullable()));
            columns.push(encoded);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(Arc::clone(column));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn should_dictionary_encode(column: &ArrayRef) -> Result<bool> {
    if column.is_empty() {
        return Ok(false);
    }
    let non_null_count = column.len() - column.null_count();
    if non_null_count == 0 {
        return Ok(false);
    }
    let converter = RowConverter::new(vec![SortField::new(column.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(column))?;

    #[allow(clippy::cast_precision_loss)]
    let non_null_len = non_null_count as f64;
    // Once the running distinct count reaches this many, the ratio can only
    // ever be >= DICTIONARY_ENCODING_THRESHOLD (distinct count never
    // decreases as more rows are added) — bail out without owning/hashing
    // the remaining rows, instead of always materializing the full column.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    let bail_out_at = (DICTIONARY_ENCODING_THRESHOLD * non_null_len).ceil() as usize;

    let mut distinct: HashSet<_> = HashSet::new();
    for (i, row) in (&rows).into_iter().enumerate() {
        // Nulls are tracked via Arrow's null buffer, not as dictionary
        // entries — matching real Parquet, which excludes nulls from
        // dictionary cardinality entirely. Counting a null as "one more
        // distinct value" would make an otherwise low-cardinality nullable
        // column look artificially high-cardinality.
        if column.is_null(i) {
            continue;
        }
        distinct.insert(row.owned());
        if distinct.len() >= bail_out_at {
            return Ok(false);
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let ratio = distinct.len() as f64 / non_null_len;
    Ok(ratio < DICTIONARY_ENCODING_THRESHOLD)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn low_cardinality_string_column_gets_dictionary_encoded() {
        // 100 rows, only 2 distinct values -> well below the 0.4 threshold.
        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        let encoded_type = encoded.schema_ref().field(0).data_type();
        assert!(
            matches!(encoded_type, DataType::Dictionary(_, _)),
            "expected a Dictionary type, got {encoded_type:?}"
        );
    }

    #[test]
    fn high_cardinality_column_is_left_unencoded() {
        // 100 rows, all distinct -> well above the 0.4 threshold.
        let ids: Vec<i64> = (0..100).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.schema_ref().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn encoding_preserves_row_count_and_schema_field_names() {
        let names: Vec<&str> = vec!["x"; 10];
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.num_rows(), 10);
        assert_eq!(encoded.schema_ref().field(0).name(), "name");
    }

    #[test]
    fn dictionary_encoding_round_trip_preserves_data() {
        // Create a low-cardinality batch (100 rows, 2 distinct values)
        let names: Vec<&str> = (0..100)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let original_batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        // Encode the batch
        let encoded = encode_batch(&original_batch).unwrap();

        // Write to file and read back
        let dir = std::env::temp_dir().join(format!("strata-encoding-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.arrow");

        crate::datafile::write_batch(&path, &encoded).unwrap();
        let read_back = crate::datafile::read_batch(&path).unwrap();

        // Cast the read-back column back to the original type for comparison
        let read_column = read_back.column(0);
        let cast_back = cast(read_column.as_ref(), &DataType::Utf8).unwrap();

        // Compare with the original data
        let original_array = original_batch.column(0);
        assert_eq!(cast_back.as_ref(), original_array.as_ref());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_column_is_left_unencoded() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.schema_ref().field(0).data_type(), &DataType::Utf8);
        assert_eq!(encoded.num_rows(), 0);
    }

    #[test]
    fn all_null_column_is_left_unencoded_without_panicking() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let values: Vec<Option<&str>> = vec![None, None, None];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(encoded.schema_ref().field(0).data_type(), &DataType::Utf8);
        assert_eq!(encoded.column(0).null_count(), 3);
    }

    #[test]
    fn column_exactly_at_the_threshold_ratio_is_left_unencoded() {
        // 40 distinct values over 100 rows = ratio 0.4, exactly at
        // DICTIONARY_ENCODING_THRESHOLD. The comparison is strict (`<`), so
        // this must NOT be encoded.
        let names: Vec<String> = (0..100).map(|i| format!("v{}", i % 40)).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert_eq!(
            encoded.schema_ref().field(0).data_type(),
            &DataType::Utf8,
            "ratio exactly at the threshold must not be encoded"
        );
    }

    #[test]
    fn low_cardinality_numeric_column_gets_dictionary_encoded() {
        // 100 rows, 2 distinct Int64 values - the encode path has so far
        // only ever been exercised with Utf8 columns.
        let ids: Vec<i64> = (0..100).map(|i| i % 2).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        let encoded_type = encoded.schema_ref().field(0).data_type();
        assert!(
            matches!(encoded_type, DataType::Dictionary(_, _)),
            "expected a Dictionary type, got {encoded_type:?}"
        );
    }

    #[test]
    fn nullable_low_cardinality_column_preserves_nulls_through_encoding() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let values = vec![Some("alice"), None, Some("alice"), Some("alice")];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap();

        let encoded = encode_batch(&batch).unwrap();
        assert!(matches!(
            encoded.schema_ref().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(
            encoded.column(0).null_count(),
            1,
            "the null value must survive the cast to Dictionary"
        );
    }
}
