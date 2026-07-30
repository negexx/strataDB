//! The live predicate-pruning correctness check — see design doc §3.3 and
//! `2026-07-30-chaos-worker-real-concurrency-and-zonemap-verification-design.md`
//! Part 2. Runs on its own thread for the whole worker process lifetime,
//! concurrently with the agent threads, comparing zone-map-pruned
//! predicate queries against unpruned references on the SAME snapshot.
//!
//! Three checks run every poll: (1) the original pruned-subset-of-reference
//! check for `Eq(name, "agent0")` (§3.3's first direction — pruned ⊆
//! reference holds by construction regardless of zone-map correctness,
//! since `vector_search`'s pruned path applies `live_set` as an exact
//! per-row membership filter after pruning); (2) a reverse-direction check
//! (one pseudo-randomly chosen name-scoped reference row per poll, queried
//! by its own vector) that CAN catch a bad live-set resolution or a
//! wrongly-narrow zone map on the `name` column itself — but every commit
//! path gives all of a multi-batch insert's rows the SAME `name`
//! (`commit_ops::execute_multi_batch_insert`), so the `name` zone map's
//! merge is degenerate (min == max, a single value) and this check alone
//! does NOT exercise real cross-batch min/max merge arithmetic; (3) [not
//! yet implemented — see Task 5] an `id`-range compound-predicate subset
//! check, which does exercise that arithmetic, since `id` genuinely varies
//! per row unlike `name`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt64Array,
};
use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;
use strata_index::VectorMatch;
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
/// `widen_ef`), not by this constant. Fine for the pruned-subset-of-
/// reference check's large `k`. The reverse-direction check below queries
/// with `k=1` instead and does NOT reuse this constant -- it currently
/// gets away with an exact top-1 match without reasoning about `ef`
/// because this workload's segments are tiny (1-2 rows per commit), so
/// the target's own part is searched exhaustively regardless of `ef`;
/// this would need revisiting if segment size ever grows enough for
/// approximate search to plausibly miss the exact match.
const READER_SEARCH_K: usize = 100_000;
/// Distinct RNG stream for the reverse-direction check's per-poll
/// reference-row pick — see design doc Part 2 §2a. Seeded once from the
/// worker's own `seed` at thread-spawn time, then threaded through every
/// poll iteration so the pick sequence is deterministic across runs even
/// though which rows exist at poll time isn't.
const READER_REVERSE_STREAM: u64 = 0xB33F_ACE5_0000_0003;

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

/// Asserts every id in `pruned_row_ids` is present in `reference_row_ids`
/// — shared by every predicate this reader checks (`name`, and later the
/// `id`-range compound predicate). `predicate_description` names the
/// predicate for the panic message only.
fn assert_pruned_is_subset_of_reference(
    pruned_row_ids: &[u64],
    reference_row_ids: &HashSet<u64>,
    predicate_description: &str,
) {
    let bad = disagreement(pruned_row_ids, reference_row_ids);
    assert!(
        bad.is_empty(),
        "predicate-pruning disagreement: vector_search with {predicate_description} returned \
         row-ids {bad:?}, which the unpruned reference scan does not have — zone-map pruning \
         (or its merge across multi-batch commits) returned a wrong result"
    );
}

/// One reverse-direction check's own-vector round-trip, as a pure function
/// over an already-fetched hit list so it's unit-testable without a real
/// `Dataset` -- mirrors [`disagreement`]'s own rationale. `hits` is the
/// top-1 result of querying by `expected_row_id`'s own vector; correct
/// means that row itself came back as the (only) hit, not a farther point
/// or nothing at all.
fn reverse_hit_is_correct(hits: &[VectorMatch], expected_row_id: u64) -> bool {
    hits.first()
        .is_some_and(|h| h.row_id == expected_row_id && h.squared_distance < 1.0)
}

/// Downcasts a named column from an unpruned reference scan to its
/// expected arrow array type — every column this reader reads
/// (`schema_with_row_id`'s fields) is required, so a missing column or a
/// type mismatch is a genuine bug, not a recoverable condition.
fn required_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    let column = batch.column(
        batch
            .schema()
            .index_of(name)
            .unwrap_or_else(|_| panic!("schema_with_row_id must include column {name}")),
    );
    column.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "column {name} has arrow type {:?}, expected {}",
            column.data_type(),
            std::any::type_name::<T>()
        )
    })
}

/// Spawns the reader thread. The caller must set the returned
/// `Arc<AtomicBool>` (via `Ordering::SeqCst`) once every agent has
/// finished, then join the handle — the thread has no other stop signal
/// (a genuine chaos-induced crash kills it along with the whole process,
/// which needs no explicit signaling at all). `seed` seeds the
/// reverse-direction check's reference-row pick (see
/// `READER_REVERSE_STREAM`) — the same `seed` the worker itself was
/// invoked with.
pub(crate) fn spawn(
    dataset: Arc<Dataset>,
    seed: u64,
) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let done = Arc::new(AtomicBool::new(false));
    let done_for_thread = Arc::clone(&done);
    let handle = std::thread::spawn(move || {
        let name_predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(seed ^ READER_REVERSE_STREAM);
        while !done_for_thread.load(Ordering::SeqCst) {
            check_once(&dataset, &name_predicate, &mut reverse_rng);
            std::thread::sleep(READER_POLL_INTERVAL);
        }
        // One final check so the last batch of commits (landed between
        // the reader's last loop iteration and the writer setting `done`)
        // is still checked at least once.
        check_once(&dataset, &name_predicate, &mut reverse_rng);
    });
    (handle, done)
}

fn check_once(dataset: &Dataset, name_predicate: &Predicate, reverse_rng: &mut ChaCha8Rng) {
    let snapshot = dataset.snapshot();
    let schema = schema_with_row_id();

    // Neither call legitimately errors here: an empty snapshot (zero
    // committed files) resolves to Ok(vec![]) on both paths, and this
    // codebase has no compaction/GC that could make a manifest-listed
    // file vanish out from under a live snapshot. A real Err is therefore
    // always a genuine bug -- exactly the class of failure this reader
    // thread exists to surface via the global panic hook (design doc
    // §3.4), so it must panic here, not silently skip the check.
    let pruned = snapshot
        .vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(name_predicate))
        .expect("vector_search must succeed against a live snapshot");
    let pruned_row_ids: Vec<u64> = pruned.into_iter().map(|m| m.row_id).collect();

    let all_rows = snapshot
        .scan(&schema)
        .expect("scan must succeed against a live snapshot");
    let name_col = required_column::<StringArray>(&all_rows, "name");
    let row_id_col = required_column::<UInt64Array>(&all_rows, strata_txn::ROW_ID_COLUMN);
    let vector_col = required_column::<FixedSizeListArray>(&all_rows, "vector");

    let name_reference_indices: Vec<usize> = (0..all_rows.num_rows())
        .filter(|&i| name_col.value(i) == READER_PREDICATE_NAME)
        .collect();
    let name_reference: HashSet<u64> = name_reference_indices
        .iter()
        .map(|&i| row_id_col.value(i))
        .collect();
    assert_pruned_is_subset_of_reference(
        &pruned_row_ids,
        &name_reference,
        &format!("Eq(name, {READER_PREDICATE_NAME:?})"),
    );

    // Reverse-direction check (design doc Part 2 §2a): one pseudo-randomly
    // chosen name-scoped reference row per poll, queried by its OWN
    // vector under the same predicate. If zone-map pruning ever wrongly
    // excludes the segment holding that row, it would come back missing
    // or replaced by a farther point -- the failure mode the subset-only
    // check above structurally cannot see. Skipped when there's no
    // reference row yet.
    if !name_reference_indices.is_empty() {
        let idx = name_reference_indices[reverse_rng.random_range(0..name_reference_indices.len())];
        let expected_row_id = row_id_col.value(idx);
        let vector_value = vector_col.value(idx);
        let vector_values: &Float32Array = vector_value
            .as_any()
            .downcast_ref()
            .expect("vector column elements must be Float32");
        let query: Vec<f32> = (0..vector_values.len())
            .map(|i| vector_values.value(i))
            .collect();
        let hits = snapshot
            .vector_search(&query, 1, Some(name_predicate))
            .expect("vector_search must succeed against a live snapshot");
        assert!(
            reverse_hit_is_correct(&hits, expected_row_id),
            "reverse-direction disagreement: row {expected_row_id}'s own vector, queried under \
             Eq(name, {READER_PREDICATE_NAME:?}), did not come back as the top-1 pruned hit \
             (got {hits:?}) — zone-map pruning wrongly excluded the segment holding this row"
        );
    }
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
    fn reverse_hit_is_correct_is_false_on_an_empty_hit_list() {
        assert!(!reverse_hit_is_correct(&[], 5));
    }

    #[test]
    fn reverse_hit_is_correct_is_false_when_the_top_hit_is_a_different_row() {
        let hits = [VectorMatch {
            row_id: 99,
            squared_distance: 0.0,
        }];
        assert!(!reverse_hit_is_correct(&hits, 5));
    }

    #[test]
    fn reverse_hit_is_correct_is_true_when_the_top_hit_is_the_expected_row_at_zero_distance() {
        let hits = [VectorMatch {
            row_id: 5,
            squared_distance: 0.0,
        }];
        assert!(reverse_hit_is_correct(&hits, 5));
    }

    #[test]
    fn spawn_and_stop_against_a_real_but_empty_dataset_does_not_panic() {
        let dir = temp_dir("spawn-empty");
        let dataset = Arc::new(Dataset::create(&dir).unwrap());

        let (handle, done) = spawn(Arc::clone(&dataset), 42);
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
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        // Must not panic: the pruned agent0-only search result must be a
        // subset of the unpruned reference scan's agent0 rows.
        check_once(&dataset, &predicate, &mut reverse_rng);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_once_passes_against_a_real_multi_batch_commit() {
        // Two rows from the SAME multi-batch commit (one segment, one
        // merged zone map spanning both) -- exercises the
        // reverse-direction check's own-vector round-trip against a real
        // committed row, not just the subset check. Real cross-SEGMENT
        // merge coverage still needs multiple segments, which only a real
        // chaos run produces (see the module doc).
        let dir = temp_dir("check-once-multi-batch");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "agent0", [0.0, 1.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        // Run several times: the reverse check picks a pseudo-random
        // reference row each call. This does not ASSERT both of the 2
        // candidate rows get picked across these 10 calls (a seed that
        // picked the same index every time would still pass) -- it's
        // exercising check_once against real multi-batch data without
        // panicking, not proving reverse_hit_is_correct's own logic
        // (that's covered directly by its own unit tests above).
        for _ in 0..10 {
            check_once(&dataset, &predicate, &mut reverse_rng);
        }

        std::fs::remove_dir_all(&dir).ok();
    }
}
