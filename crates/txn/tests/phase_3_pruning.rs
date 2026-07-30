//! Phase 3's actual exit criterion: "EXPLAIN proves a filtered query skips
//! untouched files." See
//! `docs/design/phase-3-query-refinement-spec.md` §3's testing
//! section.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(ids: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

#[test]
fn explain_skips_files_whose_stats_cannot_match_and_scans_only_the_rest() {
    let dir = tempfile::Builder::new()
        .prefix("strata-phase3-explain-")
        .tempdir()
        .unwrap()
        .keep();
    let ds = Dataset::create(&dir).unwrap();

    // Three commits, three disjoint id ranges -> three files.
    let mut txn = ds.begin();
    txn.insert(batch(vec![1, 2, 3]));
    txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![50, 51, 52]));
    txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![100, 101, 102]));
    txn.commit().unwrap();

    // A predicate that can only match the middle file's range. Both reads
    // below share a single snapshot, so they observe exactly the same
    // committed state.
    let predicate = Predicate::Eq("id".to_string(), Value::Int64(51));
    let snapshot = ds.snapshot();
    let result = snapshot.explain(&predicate);

    assert_eq!(result.total_files, 3);
    assert_eq!(
        result.scanned.len(),
        1,
        "only the [50,52] file could contain id=51"
    );
    assert_eq!(
        result.skipped.len(),
        2,
        "the [1,3] and [100,102] files must both be skipped"
    );
    // Identify the surviving file by its manifest entry, not by parsing
    // the filename: as of Phase 6, filenames embed an opaque per-attempt
    // counter (see `Transaction::commit`'s `write_attempt_counter`), not
    // the commit version, so the name prefix is an implementation detail
    // no test should decode.
    assert_eq!(
        result.scanned[0],
        ds.data_files()[1].name,
        "the scanned file must be the second commit (the [50,52] one): {:?}",
        result.scanned
    );

    // scan_with_predicate must return exactly the one matching row, proving
    // the skip decision and the actual filtered result agree.
    let filtered = snapshot.scan_with_predicate(&schema(), &predicate).unwrap();
    assert_eq!(filtered.num_rows(), 1);
    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 51);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn explain_and_scan_with_predicate_agree_on_a_range_predicate_not_just_equality() {
    let dir = tempfile::Builder::new()
        .prefix("strata-phase3-range-")
        .tempdir()
        .unwrap()
        .keep();
    let ds = Dataset::create(&dir).unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![1, 2, 3]));
    txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![50, 51, 52]));
    txn.commit().unwrap();

    let mut txn = ds.begin();
    txn.insert(batch(vec![100, 101, 102]));
    txn.commit().unwrap();

    // Gt(id, 60): only the [100,102] file can possibly satisfy this. Both
    // reads below share a single snapshot, so they observe exactly the same
    // committed state.
    let predicate = Predicate::Gt("id".to_string(), Value::Int64(60));
    let snapshot = ds.snapshot();
    let result = snapshot.explain(&predicate);

    assert_eq!(result.total_files, 3);
    assert_eq!(
        result.scanned.len(),
        1,
        "only the [100,102] file could match id>60"
    );
    assert_eq!(result.skipped.len(), 2);

    let filtered = snapshot.scan_with_predicate(&schema(), &predicate).unwrap();
    assert_eq!(filtered.num_rows(), 3);
    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut got: Vec<i64> = (0..filtered.num_rows()).map(|i| ids.value(i)).collect();
    got.sort_unstable();
    assert_eq!(got, vec![100, 101, 102]);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compound_and_predicate_prunes_files_and_filters_rows_together() {
    let dir = std::env::temp_dir().join(format!("strata-phase3-compound-{}", std::process::id()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let ds = Dataset::create(&dir).unwrap();

    // File A: id range [1,3] - below the id>=40 threshold, prunable by the
    // id leaf alone.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["x", "x", "x"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    // File B: id range [50,52], category all "y" - the one file that truly
    // matches both conjuncts.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![50, 51, 52])),
                Arc::new(arrow::array::StringArray::from(vec!["y", "y", "y"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    // File C: id range [60,62] - satisfies id>=40, so the id leaf ALONE
    // cannot prune this file. But category is all "x", so the category
    // leaf's own stats (min=max="x") prove no row can equal "y". Only the
    // AND, using both leaves' information together, prunes File C.
    let mut txn = ds.begin();
    txn.insert(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![60, 61, 62])),
                Arc::new(arrow::array::StringArray::from(vec!["x", "x", "x"])),
            ],
        )
        .unwrap(),
    );
    txn.commit().unwrap();

    let id_leaf_only = Predicate::GtEq("id".to_string(), Value::Int64(40));
    let predicate = Predicate::And(
        Box::new(id_leaf_only.clone()),
        Box::new(Predicate::Eq(
            "category".to_string(),
            Value::Utf8("y".to_string()),
        )),
    );
    let snapshot = ds.snapshot();

    // The id leaf alone can prune File A but not File C (60 and 62 are
    // both >= 40) - so a system that only ever pruned on the first leaf
    // would have to scan both File B and File C.
    let id_only_explain = snapshot.explain(&id_leaf_only);
    assert_eq!(
        id_only_explain.scanned.len(),
        2,
        "id>=40 alone cannot prune File C - its whole id range is >= 40"
    );

    // The AND compound prunes File C too, using the category leaf's
    // information that a single id-only leaf never had access to.
    let explain = snapshot.explain(&predicate);
    assert_eq!(explain.total_files, 3);
    assert_eq!(
        explain.scanned.len(),
        1,
        "the AND must prune File C using the category leaf, on top of File A via the id leaf"
    );
    assert_eq!(
        explain.scanned[0],
        ds.data_files()[1].name,
        "only File B survives"
    );

    let filtered = snapshot.scan_with_predicate(&schema, &predicate).unwrap();
    assert_eq!(
        filtered.num_rows(),
        3,
        "every row in File B matches both conjuncts"
    );
    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut got: Vec<i64> = (0..filtered.num_rows()).map(|i| ids.value(i)).collect();
    got.sort_unstable();
    assert_eq!(got, vec![50, 51, 52]);

    std::fs::remove_dir_all(&dir).ok();
}
