//! Commit atomicity: a transaction that fails must leave neither its rows
//! nor its vectors behind, and must never leave durable state that cannot be
//! reopened. See `.claude/docs/analysis/2026-07-23-occ-proposal-review.md`.
//!
//! `.claude/rules/concurrency-txn-layer.md`: "A transaction that writes a row
//! and updates the vector index commits both atomically. A crash or conflict
//! mid-transaction must leave neither behind."
//!
//! The shared HNSW graph is append-only with no node-removal API, so any
//! insert applied before the durability point can never be taken back. Both
//! tests here pin down one half of that:
//!
//! * `a_failed_commit_leaves_no_searchable_residue_in_the_vector_index`
//!   covers applying deltas *too early* — before `commit_manifest`.
//! * `concurrent_inserts_with_different_dimensions_never_leave_an_unopenable_dataset`
//!   covers validating *too early* — before the commit lock, where the check
//!   is racy and cannot carry the weight of applying deltas after durability.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use strata_storage::{commit_manifest, read_current};
use strata_txn::mvp_fixtures::{mvp_row, mvp_schema};
use strata_txn::{Dataset, ROW_ID_COLUMN, TxnError};

/// [`mvp_schema`] plus the hidden row-id column, so a scan can report which
/// row-ids the dataset actually contains.
fn mvp_schema_with_row_id() -> SchemaRef {
    let mut fields: Vec<Field> = mvp_schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// A one-row batch whose `"vector"` column has exactly `vector.len()`
/// dimensions — `mvp_row` is fixed at 3, and the dimension is the variable
/// under test in the concurrency case.
fn vector_batch(id: i64, vector: &[f32]) -> RecordBatch {
    let dim = i32::try_from(vector.len()).unwrap();
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::clone(&item), dim),
            false,
        ),
    ]));
    let values = Arc::new(Float32Array::from(vector.to_vec()));
    let vectors = FixedSizeListArray::new(item, dim, values, None);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(vectors)],
    )
    .unwrap()
}

#[test]
fn a_failed_commit_leaves_no_searchable_residue_in_the_vector_index() {
    let dir = std::env::temp_dir().join(format!(
        "strata-failed-commit-residue-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    let ds = Dataset::create(&dir).unwrap();

    let versions = dir.join("_versions");
    let stashed = dir.join("_versions_stashed");

    // Deterministic, cross-platform commit failure with no chaos feature:
    // `commit_manifest` opens with `fs::create_dir_all(_versions)`, which
    // fails when a *regular file* occupies that path. Stash the real
    // directory so the later commit can succeed.
    fs::rename(&versions, &stashed).unwrap();
    fs::write(&versions, b"not a directory").unwrap();

    let mut ghost = ds.begin();
    ghost.insert(mvp_row(1, "ghost", [9.0, 9.0, 9.0]).unwrap());
    let ghost_result = ghost.commit();

    // Un-sabotage before asserting, so a failure here can't leave a stray
    // regular file where the version directory belongs.
    fs::remove_file(&versions).unwrap();
    fs::rename(&stashed, &versions).unwrap();
    assert!(
        ghost_result.is_err(),
        "setup is invalid unless the manifest write actually fails"
    );

    // A later successful commit advances the watermark past the ghost's
    // row-id, which is what would make any residue visible.
    let mut real = ds.begin();
    real.insert(mvp_row(2, "real", [1.0, 1.0, 1.0]).unwrap());
    real.commit().unwrap();

    let snap = ds.snapshot();
    let scanned = snap.scan(&mvp_schema_with_row_id()).unwrap();
    // Query the ghost's own vector so it ranks first if it is present at all.
    let matches = snap.vector_search(&[9.0, 9.0, 9.0], 10, None).unwrap();

    let row_id_idx = scanned.schema_ref().index_of(ROW_ID_COLUMN).unwrap();
    let scanned_ids: HashSet<u64> = scanned
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .iter()
        .copied()
        .collect();
    let searched_ids: HashSet<u64> = matches.iter().map(|m| m.row_id).collect();

    let _ = fs::remove_dir_all(&dir);

    assert_eq!(
        scanned.num_rows(),
        1,
        "only the successfully committed row belongs to the dataset"
    );
    assert_eq!(
        searched_ids, scanned_ids,
        "vector_search returned row-ids the dataset does not contain — the \
         failed commit's vectors are still searchable (searched: {searched_ids:?}, \
         scanned: {scanned_ids:?})"
    );
}

#[test]
fn concurrent_inserts_with_different_dimensions_never_leave_an_unopenable_dataset() {
    // Two insert-only transactions with different vector dimensions race for
    // the commit lock. They never conflict with each other (an empty
    // write-set short-circuits to `Clean`), and a dimension check taken
    // before the lock passes for *both* of them, because a graph with no
    // established dimension adopts whichever batch it validates first.
    //
    // So if that pre-lock check were the only one, the loser would discover
    // the mismatch only after `commit_manifest` had already made its version
    // durable: a manifest listing two data files whose delta logs cannot
    // both replay into one graph, which no future `Dataset::open` can ever
    // load. The in-lock re-validation is what prevents that.
    // How many iterations genuinely overlapped. Asserted non-zero at the end:
    // without it this test can pass while exercising nothing (see below).
    let mut raced = 0_usize;

    for iteration in 0..25 {
        let dir = std::env::temp_dir().join(format!(
            "strata-dim-race-{}-{iteration}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let ds = Dataset::create(&dir).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [(1_i64, 3_usize), (2, 5)]
            .into_iter()
            .map(|(id, dim)| {
                let ds = ds.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut txn = ds.begin();
                    txn.insert(vector_batch(id, &vec![1.0_f32; dim]));
                    // Both threads reach `commit` together, so both run their
                    // pre-lock validation against a still-empty graph.
                    barrier.wait();
                    txn.commit()
                })
            })
            .collect();

        let mut committed = 0_usize;
        let mut rejections = Vec::new();
        for handle in handles {
            match handle.join().unwrap() {
                Ok(()) => committed += 1,
                Err(err) => rejections.push(err.to_string()),
            }
        }

        // Reopen from disk: this is what a durable-but-unpublished manifest
        // breaks, permanently and unrecoverably.
        drop(ds);
        let reopen_err = Dataset::open(&dir).err().map(|e| e.to_string());

        // Did this iteration actually overlap? The winner records the row-id
        // counter's value from inside the lock, so a durable `next_row_id` of
        // 2 means the loser's pre-lock `fetch_add` landed before the winner
        // sealed its manifest. That is necessary — not sufficient — for having
        // reached the in-lock re-check, but it rules out a fully serialized
        // run, which is the way this test could otherwise pass having
        // exercised nothing.
        if read_current(&dir).ok().flatten().map(|m| m.next_row_id) == Some(2) {
            raced += 1;
        }
        let _ = fs::remove_dir_all(&dir);

        // NOTE: this assertion does *not* discriminate fixed from broken —
        // exactly one commit succeeds either way, including in every
        // corrupting interleaving. All the discriminating power lives in the
        // `reopen_err` assertion below, and only when the race occurred.
        assert_eq!(
            committed, 1,
            "iteration {iteration}: exactly one of two mismatched-dimension \
             inserts may commit (rejections: {rejections:?})"
        );
        assert!(
            reopen_err.is_none(),
            "iteration {iteration}: the rejected transaction left durable state \
             that cannot be replayed — Dataset::open failed with {reopen_err:?}"
        );
    }

    assert!(
        raced > 0,
        "every iteration ran serialized, so this test never reached the race \
         it exists to guard — it would pass even with the in-lock dimension \
         re-check deleted. Raise the iteration count for this machine rather \
         than trusting the green result."
    );
}

#[test]
fn committing_past_the_row_id_ceiling_is_rejected_before_reaching_the_index() {
    // `NodeTable` sizes its chunk directory from this ceiling and indexes it
    // with a plain slice index, so row-ids beyond its addressable range panic
    // *inside the index* rather than returning an error. Since deltas are now
    // applied after `commit_manifest`, such a panic would unwind after the
    // commit was already durable — so the write path has to reject first,
    // typed and pre-durability, the same way `replay_index` does on open.
    //
    // This asserts the *contract*, not the panic: at exactly the ceiling the
    // directory's round-up-to-a-whole-chunk still has slack, so without the
    // check this commit would quietly succeed rather than panic. Either way
    // the test discriminates — `unwrap_err` fails if the rejection is absent.
    const CEILING: u64 = 1_000_000_000;

    let dir = std::env::temp_dir().join(format!("strata-rowid-ceiling-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    drop(Dataset::create(&dir).unwrap());

    // Fast-forward the durable row-id counter to exactly the ceiling. `open`
    // still accepts this (its check is `>`), so the very next commit is the
    // one that would cross the line.
    let mut manifest = read_current(&dir).unwrap().unwrap();
    manifest.version += 1;
    manifest.next_row_id = CEILING;
    commit_manifest(&dir, &manifest).unwrap();

    let ds = Dataset::open(&dir).unwrap();
    let mut txn = ds.begin();
    txn.insert(mvp_row(1, "over-the-line", [1.0, 1.0, 1.0]).unwrap());
    let err = txn.commit().unwrap_err();

    let _ = fs::remove_dir_all(&dir);

    assert!(
        matches!(err, TxnError::UnreasonableCapacity(_, CEILING)),
        "expected a typed capacity rejection before the index is touched, \
         got: {err:?}"
    );
}
