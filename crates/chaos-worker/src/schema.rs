//! `mvp_schema()`'s fields plus the hidden row-id column — needed to read
//! back the internal system row-id `Transaction::commit()` itself never
//! returns. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! Global Constraint 6 in the implementation plan for why this is a
//! read-back rather than a prediction from the row-id allocator's own
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
mod tests {
    use super::*;

    #[test]
    fn includes_every_mvp_field_plus_the_row_id_column() {
        let schema = schema_with_row_id();
        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("vector").is_ok());
        assert!(schema.field_with_name(strata_txn::ROW_ID_COLUMN).is_ok());
    }
}
