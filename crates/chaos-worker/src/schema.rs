//! `mvp_schema()`'s fields plus the hidden row-id column — needed to read
//! back the internal system row-id `Transaction::commit()` itself never
//! returns. See
//! `docs/audit/phase-1/audit.md`; this is a read-back rather than a prediction
//! from the row-id allocator's own
//! claim-order semantics.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

pub(crate) fn schema_with_row_id() -> Arc<Schema> {
    let mvp = strata_txn::mvp_fixtures::mvp_schema();
    let mut fields: Vec<Field> = mvp.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(
        strata_txn::ROW_ID_COLUMN,
        DataType::UInt64,
        false,
    ));
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn includes_every_mvp_field_plus_the_row_id_column() {
        let schema = schema_with_row_id();
        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("vector").is_ok());
        let row_id_field = schema.field_with_name(strata_txn::ROW_ID_COLUMN).unwrap();
        assert_eq!(
            *row_id_field.data_type(),
            DataType::UInt64,
            "must be UInt64 -- cast_batch_to_schema silently casts on a type \
             mismatch instead of erroring, which would turn a wrong DataType \
             here into a runtime downcast panic in commit_ops.rs instead of \
             a caught-at-the-source bug"
        );
    }
}
