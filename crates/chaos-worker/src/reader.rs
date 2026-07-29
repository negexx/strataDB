//! The live predicate-pruning correctness check — see design doc §3.3.
//! Runs on its own thread for the whole worker process lifetime,
//! concurrently with the main commit loop, comparing a zone-map-pruned
//! predicate query against an unpruned reference on the SAME snapshot.
//!
//! Only checks pruned-result-is-a-subset-of-reference (§3.3's first
//! direction) — this does NOT verify zone-map-merge correctness (design
//! doc §1's gap 5), despite §3.3's original claim that it would.
//! `Snapshot::vector_search`'s pruned path applies `live_set` as an exact
//! per-row membership filter after pruning, so pruned ⊆ reference holds
//! BY CONSTRUCTION regardless of whether the zone map itself is correct.
//! The realistic zone-map-merge bug is a merged range too narrow, wrongly
//! pruning a segment OUT — producing missing rows, which only the
//! unimplemented reverse direction ("every reference row with a
//! genuinely nearest vector must be findable through the pruned path
//! too") could detect. Even that reverse direction would additionally
//! need `execute_multi_batch_insert` to give its two rows DISTINCT `name`
//! values (today both rows in one multi-batch commit share the same
//! `name`, so the merged zone map is identical to either batch's alone —
//! a degenerate merge for exactly the column this reader predicates on).
//! Neither gap is closed by real chaos-run volume alone; see the design
//! doc's own post-implementation correction in §3.3 for the full
//! reasoning. What this module DOES genuinely verify: gap 2 (delete/
//! update visibility) and gap 4 (predicate-filtered search is actually
//! exercised under pruning, not just present).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, StringArray, UInt64Array};
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

use crate::schema::schema_with_row_id;

const READER_PREDICATE_NAME: &str = "agent0";
const READER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);
/// Generous upper bound on rows any single chaos run will ever produce.
/// This bounds only the post-merge truncation across parts
/// (`SegmentSet::search_filtered_pruned_live`'s final `k`-truncation) --
/// per-part recall is governed by `ef` (`EF_SEARCH_DEFAULT`, widened by
/// `widen_ef`), not by this constant. Fine for this one-directional
/// pruned-subset-of-reference check; a future reverse-direction check
/// (reference implies prunable) would need to reason about `ef` instead.
const READER_SEARCH_K: usize = 100_000;

/// One iteration's check, as a pure function over already-fetched
/// row-id sets so it's unit-testable without a real `Dataset`. Returns
/// the pruned-but-not-in-reference row-ids, if any (a disagreement).
fn disagreement(pruned_row_ids: &[u64], reference_row_ids: &HashSet<u64>) -> Vec<u64> {
    pruned_row_ids
        .iter()
        .copied()
        .filter(|id| !reference_row_ids.contains(id))
        .collect()
}

/// Spawns the reader thread. The caller must set the returned
/// `Arc<AtomicBool>` (via `Ordering::SeqCst`) once every agent has
/// finished, then join the handle — the thread has no other stop signal
/// (a genuine chaos-induced crash kills it along with the whole process,
/// which needs no explicit signaling at all).
pub(crate) fn spawn(dataset: Arc<Dataset>) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let done = Arc::new(AtomicBool::new(false));
    let done_for_thread = Arc::clone(&done);
    let handle = std::thread::spawn(move || {
        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        while !done_for_thread.load(Ordering::SeqCst) {
            check_once(&dataset, &predicate);
            std::thread::sleep(READER_POLL_INTERVAL);
        }
        // One final check so the last batch of commits (landed between
        // the reader's last loop iteration and the writer setting
        // `done`) is still checked at least once.
        check_once(&dataset, &predicate);
    });
    (handle, done)
}

fn check_once(dataset: &Dataset, predicate: &Predicate) {
    let snapshot = dataset.snapshot();
    let schema = schema_with_row_id();

    // Neither call legitimately errors here: an empty snapshot (zero
    // committed files) resolves to Ok(vec![]) on both paths, and this
    // codebase has no compaction/GC that could make a manifest-listed
    // file vanish out from under a live snapshot. A real Err is therefore
    // always a genuine bug (a dimension mismatch, a predicate/column type
    // mismatch, a cast failure) — exactly the class of failure this
    // reader thread exists to surface via the global panic hook (design
    // doc §3.4), so it must panic here, not silently skip the check.
    let pruned = snapshot
        .vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(predicate))
        .expect("vector_search must succeed against a live snapshot");
    let pruned_row_ids: Vec<u64> = pruned.into_iter().map(|m| m.row_id).collect();

    let all_rows = snapshot
        .scan(&schema)
        .expect("scan must succeed against a live snapshot");
    let name_col = all_rows
        .column(
            all_rows
                .schema()
                .index_of("name")
                .expect("schema_with_row_id always includes a name column"),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column must be Utf8");
    let row_id_col = all_rows
        .column(
            all_rows
                .schema()
                .index_of(strata_txn::ROW_ID_COLUMN)
                .expect("schema_with_row_id always includes the row-id column"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("_row_id column must be UInt64");

    let reference_row_ids: HashSet<u64> = (0..all_rows.num_rows())
        .filter(|&i| name_col.value(i) == READER_PREDICATE_NAME)
        .map(|i| row_id_col.value(i))
        .collect();

    let bad = disagreement(&pruned_row_ids, &reference_row_ids);
    assert!(
        bad.is_empty(),
        "predicate-pruning disagreement: vector_search with Eq(name, {READER_PREDICATE_NAME:?}) \
         returned row-ids {bad:?}, which the unpruned reference scan does not have tagged \
         name={READER_PREDICATE_NAME:?} — zone-map pruning (or its merge across multi-batch \
         commits) returned a wrong result"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-worker-reader-test-{label}-{}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn disagreement_is_empty_when_every_pruned_id_is_in_the_reference() {
        let reference: HashSet<u64> = [1, 2, 3].into_iter().collect();
        assert_eq!(disagreement(&[1, 2], &reference), Vec::<u64>::new());
    }

    #[test]
    fn disagreement_reports_a_pruned_id_missing_from_the_reference() {
        let reference: HashSet<u64> = [1, 2].into_iter().collect();
        assert_eq!(disagreement(&[1, 2, 99], &reference), vec![99]);
    }

    #[test]
    fn spawn_and_stop_against_a_real_but_empty_dataset_does_not_panic() {
        let dir = temp_dir("spawn-empty");
        let dataset = Arc::new(Dataset::create(&dir).unwrap());

        let (handle, done) = spawn(Arc::clone(&dataset));
        std::thread::sleep(std::time::Duration::from_millis(20));
        done.store(true, Ordering::SeqCst);
        handle.join().unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_once_passes_against_real_committed_rows() {
        // The empty-dataset spawn test above never exercises check_once's
        // actual comparison logic: num_rows() == 0 means the reference-set
        // construction, both `expect` downcasts, and the assertion never
        // run against real data anywhere else in this module's tests. Two
        // agents' worth of rows makes the "name" predicate load-bearing --
        // a check_once that ignored the predicate and returned every
        // row's id, or that matched the wrong column, would still pass a
        // single-agent version of this test. Both rows land in one commit
        // (one segment, one merged zone map spanning both names), so this
        // does NOT exercise segment-level zone-map pruning -- only the
        // resolve_live_set filter path. Real pruning coverage needs
        // multiple segments, which only Task 8's chaos runs produce.
        let dir = temp_dir("check-once-real-data");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "agent1", [0.0, 1.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        // Must not panic: the pruned agent0-only search result must be a
        // subset of the unpruned reference scan's agent0 rows.
        check_once(&dataset, &predicate);

        std::fs::remove_dir_all(&dir).ok();
    }
}
