#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use strata_query::{LogicalOperator, PhysicalOperator};
use strata_txn::{
    Aggregate, AggregateFunction, Comparison, ComparisonOperator, Dataset, FilterExpression,
    FilterLiteral, GroupByRequest, Projection, QueryError, QueryValidationError, RowId,
    ScanRequest, VectorHydration, VectorSearchRequest,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
            false,
        ),
    ]))
}

fn batch(
    categories: Vec<Option<&str>>,
    amounts: Vec<Option<i64>>,
    vectors: Vec<[f32; 2]>,
) -> RecordBatch {
    let flat = vectors.into_iter().flatten().collect::<Vec<_>>();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(amounts)),
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Float32, false)),
                2,
                Arc::new(Float32Array::from(flat)),
                None,
            )),
        ],
    )
    .unwrap()
}

#[test]
fn planned_queries_match_direct_snapshot_operators_and_report_selection_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), schema()).unwrap();
    for rows in [
        batch(
            vec![Some("discard"), None],
            vec![Some(1), None],
            vec![[0.0, 0.0], [1.0, 1.0]],
        ),
        batch(
            vec![Some("keep"), Some("keep")],
            vec![Some(10), Some(20)],
            vec![[2.0, 2.0], [3.0, 3.0]],
        ),
    ] {
        let mut transaction = dataset.begin();
        transaction.insert(rows).unwrap();
        transaction.commit().unwrap();
    }
    let snapshot = dataset.snapshot();
    let filter = FilterExpression::Compare(Comparison {
        column: "amount".into(),
        operator: ComparisonOperator::GreaterThan,
        value: FilterLiteral::Int64(5),
    });
    let scan = ScanRequest {
        projection: Projection::Columns(vec!["category".into()]),
        filter: Some(filter.clone()),
    };
    let scan_plan = snapshot.explain_scan_query(&scan).unwrap();
    assert_eq!(scan_plan.observations.data_files_total, 2);
    assert_eq!(scan_plan.observations.data_files_pruned, 1);
    assert!(!scan_plan.observations.transaction_overlay);
    assert!(
        scan_plan
            .physical_operators
            .contains(&PhysicalOperator::ZoneMapPruning)
    );
    assert_eq!(
        snapshot.execute_planned_scan_query(&scan).unwrap(),
        snapshot.scan_query(&scan).unwrap()
    );

    let group = GroupByRequest {
        group_by: vec!["category".into()],
        aggregates: vec![Aggregate::new("amount", AggregateFunction::Sum, "sum")],
        filter: Some(filter.clone()),
    };
    assert_eq!(
        snapshot.execute_planned_group_by_query(&group).unwrap(),
        snapshot.group_by_query(&group).unwrap()
    );

    let vector = VectorSearchRequest {
        vector_column: "vector".into(),
        query: vec![2.0, 2.0],
        k: 2,
        filter: Some(filter),
        hydration: VectorHydration::Projection(Projection::Columns(vec!["category".into()])),
    };
    let vector_plan = snapshot.explain_vector_search_query(&vector).unwrap();
    assert!(
        vector_plan
            .physical_operators
            .contains(&PhysicalOperator::ImmutableSegmentVectorSearch)
    );
    assert_eq!(
        snapshot
            .execute_planned_vector_search_query(&vector)
            .unwrap(),
        snapshot.vector_search_query(&vector).unwrap()
    );
}

#[test]
fn unfiltered_plans_do_not_claim_a_row_filter_or_zone_map_path() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(batch(vec![Some("keep")], vec![Some(10)], vec![[2.0, 2.0]]))
        .unwrap();
    transaction.commit().unwrap();

    let plan = dataset
        .snapshot()
        .explain_scan_query(&ScanRequest {
            projection: Projection::All,
            filter: None,
        })
        .unwrap();

    assert!(
        !plan
            .physical_operators
            .contains(&PhysicalOperator::RowFilter)
    );
    assert!(
        !plan
            .physical_operators
            .contains(&PhysicalOperator::ZoneMapPruning)
    );
    assert!(
        plan.logical_operators
            .iter()
            .all(|operator| !matches!(operator, LogicalOperator::Predicate { .. }))
    );
}

#[test]
fn unprunable_filters_still_select_the_row_filter_without_claiming_zone_map_pruning() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(batch(vec![Some("keep")], vec![Some(10)], vec![[2.0, 2.0]]))
        .unwrap();
    transaction.commit().unwrap();

    let plan = dataset
        .snapshot()
        .explain_scan_query(&ScanRequest {
            projection: Projection::All,
            filter: Some(FilterExpression::Not(Box::new(FilterExpression::Compare(
                Comparison {
                    column: "amount".into(),
                    operator: ComparisonOperator::GreaterThan,
                    value: FilterLiteral::Int64(100),
                },
            )))),
        })
        .unwrap();

    assert!(
        plan.physical_operators
            .contains(&PhysicalOperator::RowFilter)
    );
    assert!(
        !plan
            .physical_operators
            .contains(&PhysicalOperator::ZoneMapPruning)
    );
}

#[test]
fn planned_paths_preserve_tombstones_nulls_projection_order_and_invalid_request_errors() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(batch(
            vec![None, Some("keep")],
            vec![None, Some(20)],
            vec![[0.0, 0.0], [2.0, 2.0]],
        ))
        .unwrap();
    transaction.commit().unwrap();
    let mut transaction = dataset.begin();
    transaction.delete(RowId(1).0).unwrap();
    transaction.commit().unwrap();

    let snapshot = dataset.snapshot();
    let scan = ScanRequest {
        projection: Projection::Columns(vec!["amount".into(), "category".into()]),
        filter: None,
    };
    let direct_scan = snapshot.scan_query(&scan).unwrap();
    assert_eq!(
        direct_scan
            .rows
            .iter()
            .flat_map(|row| row.fields.iter().map(|field| field.name.as_str()))
            .collect::<Vec<_>>(),
        vec!["amount", "category"]
    );
    assert_eq!(
        snapshot.execute_planned_scan_query(&scan).unwrap(),
        direct_scan
    );

    let group = GroupByRequest {
        group_by: vec!["category".into()],
        aggregates: vec![Aggregate::new("amount", AggregateFunction::Count, "count")],
        filter: None,
    };
    assert_eq!(
        snapshot.execute_planned_group_by_query(&group).unwrap(),
        snapshot.group_by_query(&group).unwrap()
    );

    let vector = VectorSearchRequest {
        vector_column: "vector".into(),
        query: vec![0.0, 0.0],
        k: 2,
        filter: None,
        hydration: VectorHydration::Projection(Projection::Columns(vec!["category".into()])),
    };
    assert_eq!(
        snapshot
            .execute_planned_vector_search_query(&vector)
            .unwrap(),
        snapshot.vector_search_query(&vector).unwrap()
    );

    let invalid = ScanRequest {
        projection: Projection::Columns(vec!["missing".into()]),
        filter: None,
    };
    for result in [
        snapshot.scan_query(&invalid),
        snapshot.execute_planned_scan_query(&invalid),
    ] {
        assert!(matches!(
            result,
            Err(QueryError::Validation(QueryValidationError::UnknownColumn { name })) if name == "missing"
        ));
    }
}
