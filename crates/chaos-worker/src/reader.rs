//! The live predicate-pruning correctness check — see design doc §3.3.
//! Runs on its own thread for the whole worker process lifetime,
//! concurrently with the main commit loop, comparing a zone-map-pruned
//! predicate query against an unpruned reference on the SAME snapshot.

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
/// Generous upper bound on rows any single chaos run will ever produce —
/// large enough that `vector_search` never truncates a genuine match, not
/// a performance-sensitive value.
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

    let Ok(pruned) = snapshot.vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(predicate))
    else {
        return; // e.g. no vector committed yet — nothing to compare.
    };
    let pruned_row_ids: Vec<u64> = pruned.into_iter().map(|m| m.row_id).collect();

    let Ok(all_rows) = snapshot.scan(&schema) else {
        return;
    };
    let name_col = all_rows
        .column(all_rows.schema().index_of("name").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column must be Utf8");
    let row_id_col = all_rows
        .column(
            all_rows
                .schema()
                .index_of(strata_txn::ROW_ID_COLUMN)
                .unwrap(),
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
mod tests {
    use super::*;

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
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-worker-reader-test-{}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        let dataset = Arc::new(Dataset::create(&dir).unwrap());

        let (handle, done) = spawn(Arc::clone(&dataset));
        std::thread::sleep(std::time::Duration::from_millis(20));
        done.store(true, Ordering::SeqCst);
        handle.join().unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }
}
