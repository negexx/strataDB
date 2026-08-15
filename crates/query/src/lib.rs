//! Expression/filter API. See
//! `docs/design.md` for `Predicate`,
//! the general `filter`, and file-pruning via `should_scan_file`.
//!
//! Query primitives are internal building blocks. The supported engine
//! facade is `strata-txn`'s `Dataset`/`Snapshot`/`Transaction` surface.

use arrow::array::RecordBatch;
use arrow::error::ArrowError;

mod group_by;
mod plan;
mod predicate;
mod predicate_key;
pub use group_by::{AggFunc, group_by};
pub use plan::{
    LogicalOperator, LogicalPlan, PhysicalOperator, PhysicalPlan, PlanError, PlanObservations,
    Planner,
};
pub use predicate::{Predicate, filter, mask, should_scan_file};
pub use predicate_key::PredicateKey;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod planner_tests {
    use super::{
        LogicalOperator, LogicalPlan, PhysicalOperator, PlanError, PlanObservations, Planner,
    };

    #[test]
    fn planner_selects_zone_map_and_tombstone_operators_for_a_filtered_projection() {
        let plan = Planner::plan(
            LogicalPlan::new(vec![
                LogicalOperator::Source,
                LogicalOperator::Predicate {
                    zone_map_eligible: true,
                },
                LogicalOperator::Projection {
                    columns: vec!["name".into()],
                },
                LogicalOperator::Materialize,
            ])
            .unwrap(),
            PlanObservations {
                data_files_total: 3,
                data_files_scanned: 1,
                data_files_pruned: 2,
                index_segments_total: 0,
                index_segments_scanned: 0,
                index_segments_pruned: 0,
                transaction_overlay: false,
            },
        )
        .unwrap();

        assert_eq!(
            plan.physical_operators,
            vec![
                PhysicalOperator::ManifestSnapshotSource,
                PhysicalOperator::ZoneMapPruning,
                PhysicalOperator::TombstoneFilter,
                PhysicalOperator::RowFilter,
                PhysicalOperator::ColumnProjection,
                PhysicalOperator::Materialize,
            ]
        );
        assert_eq!(plan.observations.data_files_pruned, 2);
        assert!(!plan.observations.transaction_overlay);
    }

    #[test]
    fn planner_rejects_a_logical_plan_that_mixes_grouping_and_vector_search() {
        let error = LogicalPlan::new(vec![
            LogicalOperator::Source,
            LogicalOperator::Grouping {
                keys: vec!["category".into()],
                aggregate_count: 1,
            },
            LogicalOperator::VectorSearch {
                vector_column: "vector".into(),
                k: 3,
                has_filter: false,
                hydration: false,
            },
            LogicalOperator::Materialize,
        ])
        .expect_err("a plan cannot group and vector-search the same result stream");

        assert_eq!(error, PlanError::ConflictingTerminalOperators);
    }

    #[test]
    fn planner_rejects_a_predicate_after_a_result_operator() {
        let error = LogicalPlan::new(vec![
            LogicalOperator::Source,
            LogicalOperator::Projection {
                columns: vec!["name".into()],
            },
            LogicalOperator::Predicate {
                zone_map_eligible: true,
            },
            LogicalOperator::Materialize,
        ])
        .expect_err("a predicate cannot follow the result operator it must filter");

        assert_eq!(error, PlanError::InvalidLogicalShape);
    }
}

/// Returns the rows of `batch` where `column` equals `value`. A thin
/// convenience wrapper over [`filter`] with [`Predicate::Eq`] — kept for
/// existing compatibility callers (the CLI's `filter` subcommand and its
/// legacy checklist test); prefer `filter` directly for new code.
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
