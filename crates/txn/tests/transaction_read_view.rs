#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use strata_txn::mvp_fixtures::{mvp_batch, mvp_row, mvp_schema};
use strata_txn::{
    Aggregate, AggregateFunction, Comparison, ComparisonOperator, Dataset, FilterExpression,
    FilterLiteral, GroupByRequest, ProjectedField, ProjectedRow, Projection, QueryError,
    QueryExecutionError, ResultValue, RowId, RowLookupOutcome, RowLookupRequest, ScanRequest,
    VectorHydration, VectorSearchRequest,
};

fn scan_request(filter: Option<FilterExpression>) -> ScanRequest {
    ScanRequest {
        projection: Projection::Columns(vec!["id".into(), "name".into()]),
        filter,
    }
}

#[test]
fn transaction_scan_predicate_and_group_merge_staged_inserts_replacements_and_deletes() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();

    let mut seed = dataset.begin();
    seed.insert(
        mvp_batch(&[
            (1, "base", [1.0, 0.0, 0.0]),
            (2, "deleted", [2.0, 0.0, 0.0]),
            (3, "old", [3.0, 0.0, 0.0]),
        ])
        .unwrap(),
    )
    .unwrap();
    seed.commit().unwrap();

    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(4, "inserted", [4.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    transaction.delete(1).unwrap();
    transaction
        .update(2, mvp_row(30, "replacement", [30.0, 0.0, 0.0]).unwrap())
        .unwrap();

    let scan = transaction.scan_query(&scan_request(None)).unwrap();
    assert_eq!(
        scan.rows,
        vec![
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(1)),
                    ProjectedField::new("name", ResultValue::Utf8("base".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(4)),
                    ProjectedField::new("name", ResultValue::Utf8("inserted".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(30)),
                    ProjectedField::new("name", ResultValue::Utf8("replacement".into())),
                ],
            },
        ],
        "removing staged rows from the overlay would reintroduce the stale base row or omit staged writes"
    );

    let predicate = transaction
        .scan_query(&scan_request(Some(FilterExpression::Compare(Comparison {
            column: "id".into(),
            operator: ComparisonOperator::GreaterThan,
            value: FilterLiteral::Int64(3),
        }))))
        .unwrap();
    assert_eq!(
        predicate.rows,
        vec![
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(4)),
                    ProjectedField::new("name", ResultValue::Utf8("inserted".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(30)),
                    ProjectedField::new("name", ResultValue::Utf8("replacement".into())),
                ],
            },
        ]
    );

    let groups = transaction
        .group_by_query(&GroupByRequest {
            group_by: vec!["name".into()],
            aggregates: vec![Aggregate::new("id", AggregateFunction::Count, "count")],
            filter: None,
        })
        .unwrap();
    assert_eq!(
        groups.rows(),
        vec![
            strata_txn::GroupedRow {
                keys: vec![ResultValue::Utf8("base".into())],
                aggregates: vec![ResultValue::UInt64(1)],
            },
            strata_txn::GroupedRow {
                keys: vec![ResultValue::Utf8("inserted".into())],
                aggregates: vec![ResultValue::UInt64(1)],
            },
            strata_txn::GroupedRow {
                keys: vec![ResultValue::Utf8("replacement".into())],
                aggregates: vec![ResultValue::UInt64(1)],
            },
        ]
    );

    assert_eq!(
        dataset
            .snapshot()
            .scan_query(&scan_request(None))
            .unwrap()
            .rows,
        vec![
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(1)),
                    ProjectedField::new("name", ResultValue::Utf8("base".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(2)),
                    ProjectedField::new("name", ResultValue::Utf8("deleted".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(3)),
                    ProjectedField::new("name", ResultValue::Utf8("old".into())),
                ],
            },
        ],
        "a separate transaction must not observe uncommitted staged state"
    );
}

#[test]
fn transaction_lookup_uses_staged_replacement_and_delete_for_existing_row_ids() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let mut seed = dataset.begin();
    seed.insert(
        mvp_batch(&[(1, "deleted", [1.0, 0.0, 0.0]), (2, "old", [2.0, 0.0, 0.0])]).unwrap(),
    )
    .unwrap();
    seed.commit().unwrap();

    let mut transaction = dataset.begin();
    transaction.delete(0).unwrap();
    transaction
        .update(1, mvp_row(20, "replacement", [20.0, 0.0, 0.0]).unwrap())
        .unwrap();

    let deleted = transaction
        .lookup_row(&RowLookupRequest {
            row_id: RowId(0),
            projection: Projection::Columns(vec!["name".into()]),
        })
        .unwrap();
    assert_eq!(deleted.outcome, RowLookupOutcome::Tombstoned);

    let replacement = transaction
        .lookup_row(&RowLookupRequest {
            row_id: RowId(1),
            projection: Projection::Columns(vec!["id".into(), "name".into()]),
        })
        .unwrap();
    assert_eq!(
        replacement.outcome,
        RowLookupOutcome::Live(ProjectedRow {
            fields: vec![
                ProjectedField::new("id", ResultValue::Int64(20)),
                ProjectedField::new("name", ResultValue::Utf8("replacement".into())),
            ],
        })
    );
}

#[test]
fn transaction_vector_query_with_staged_writes_is_typed_unsupported() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(1, "staged", [1.0, 0.0, 0.0])]).unwrap())
        .unwrap();

    let error = transaction
        .vector_search_query(&VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            filter: None,
            hydration: VectorHydration::NotRequested,
        })
        .expect_err("staged vector values must not return stale base-snapshot results");
    assert!(matches!(
        error,
        QueryError::Execution(QueryExecutionError::UnsupportedTransactionRead { operation })
            if operation == "vector search"
    ));
}

#[test]
fn transaction_vector_query_with_staged_delete_is_typed_unsupported() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let mut seed = dataset.begin();
    seed.insert(mvp_batch(&[(1, "committed", [1.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    seed.commit().unwrap();

    let mut transaction = dataset.begin();
    transaction.delete(0).unwrap();

    let error = transaction
        .vector_search_query(&VectorSearchRequest {
            vector_column: "vector".into(),
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            filter: None,
            hydration: VectorHydration::NotRequested,
        })
        .expect_err("a base-only vector result would include a locally deleted row");
    assert!(matches!(
        error,
        QueryError::Execution(QueryExecutionError::UnsupportedTransactionRead { operation })
            if operation == "vector search"
    ));
}

#[test]
fn transaction_read_view_stays_bound_to_its_base_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    let mut seed = dataset.begin();
    seed.insert(mvp_batch(&[(1, "base", [1.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    seed.commit().unwrap();

    let mut transaction = dataset.begin();
    transaction
        .insert(mvp_batch(&[(2, "local", [2.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    let mut concurrent = dataset.begin();
    concurrent
        .insert(mvp_batch(&[(3, "later", [3.0, 0.0, 0.0])]).unwrap())
        .unwrap();
    concurrent.commit().unwrap();

    assert_eq!(
        transaction.scan_query(&scan_request(None)).unwrap().rows,
        vec![
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(1)),
                    ProjectedField::new("name", ResultValue::Utf8("base".into())),
                ],
            },
            ProjectedRow {
                fields: vec![
                    ProjectedField::new("id", ResultValue::Int64(2)),
                    ProjectedField::new("name", ResultValue::Utf8("local".into())),
                ],
            },
        ],
        "reading the current dataset snapshot here would leak a post-begin commit"
    );
}

#[test]
fn dropping_a_transaction_discards_its_read_overlay_without_publication() {
    let temp = tempfile::tempdir().unwrap();
    let dataset = Dataset::create(temp.path().join("dataset"), mvp_schema()).unwrap();
    {
        let mut transaction = dataset.begin();
        transaction
            .insert(mvp_batch(&[(1, "discarded", [1.0, 0.0, 0.0])]).unwrap())
            .unwrap();
        assert_eq!(
            transaction
                .scan_query(&scan_request(None))
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    assert!(
        dataset
            .snapshot()
            .scan_query(&scan_request(None))
            .unwrap()
            .rows
            .is_empty()
    );
}
