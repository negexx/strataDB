//! Phase 1 MVP checklist, steps 1-5. Step 6 (kill -9 mid-write, restart,
//! recover) lives in `crates/cli/tests/` since it needs the real `strata`
//! binary as a killable subprocess — nothing in-process can exercise actual
//! crash safety. See the MVP definition in `.claude/docs/architecture.md`'s
//! roadmap.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use arrow::array::{FixedSizeListArray, Int64Array};

use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

#[test]
fn mvp_checklist_steps_1_through_5() {
    let dir = std::env::temp_dir().join(format!("strata-mvp-1to5-{}", std::process::id()));

    // 1. Create a new dataset.
    let ds = Dataset::create(&dir).unwrap();
    assert_eq!(ds.current_version(), 0);

    // 2. Insert a batch of rows with a numeric column, a string column, and
    //    a fixed-length vector column.
    let batch = mvp_batch(&[
        (1, "alice", [1.0, 2.0, 3.0]),
        (2, "bob", [4.0, 5.0, 6.0]),
        (3, "alice", [7.0, 8.0, 9.0]),
    ])
    .unwrap();
    let mut txn = ds.begin();
    txn.insert(batch.clone());
    txn.commit().unwrap();
    assert_eq!(ds.current_version(), 1);

    // 3. Read the data back via a full scan.
    let scanned = ds.snapshot().scan(&mvp_schema()).unwrap();
    assert_eq!(scanned.num_rows(), 3);
    assert_eq!(scanned, batch);

    // 4. Filter by an equality predicate on the string column.
    let filtered = strata_query::filter_eq(&scanned, "name", "alice").unwrap();
    assert_eq!(filtered.num_rows(), 2);
    let filtered_ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut got: Vec<i64> = (0..filtered.num_rows())
        .map(|i| filtered_ids.value(i))
        .collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 3],
        "must be exactly the two 'alice' rows (ids 1 and 3), not just any 2 rows"
    );

    // 5. Run a brute-force nearest-neighbor search on the vector column,
    //    correctly.
    let vec_idx = scanned.schema_ref().index_of("vector").unwrap();
    let vectors = scanned
        .column(vec_idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    let results = strata_index::brute_force_search(vectors, &[1.0, 2.0, 3.0], 1).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].row_index, 0, "row 0 (id=1) is the exact match");
    // Exact zero, not approximate: identical points sum to exactly 0.0.
    #[allow(clippy::float_cmp)]
    {
        assert_eq!(results[0].squared_distance, 0.0);
    }

    std::fs::remove_dir_all(&dir).ok();
}
