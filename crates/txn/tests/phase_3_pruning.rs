//! Phase 3's actual exit criterion: "EXPLAIN proves a filtered query skips
//! untouched files." See
//! `.claude/docs/design/phase-3-query-refinement-spec.md` §3's testing
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
    let dir = std::env::temp_dir().join(format!("strata-phase3-explain-{}", std::process::id()));
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
    let dir = std::env::temp_dir().join(format!("strata-phase3-range-{}", std::process::id()));
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
