//! The Phase 5 exit criterion: "Concurrent-reader suite passes against a
//! single writer." One writer thread commits a sequence of insert-only
//! transactions while several reader threads each hold a `Snapshot` and
//! repeatedly read from it — proving readers never observe a row committed
//! after their snapshot was taken. The tombstone half of the isolation
//! guarantee (a reader never loses a row tombstoned after their snapshot
//! was taken) needs a specific before/after ordering rather than a race,
//! so it's covered by a single-threaded test below instead --
//! `an_old_snapshots_vector_search_still_sees_a_row_a_later_commit_tombstones`.
//! `scan`'s tombstone half is currently untestable: `Snapshot::scan` doesn't
//! consult tombstones at all yet (see the note in
//! `an_old_snapshots_scan_never_gains_a_later_commits_rows` below, and the
//! tracked follow-up task to fix it).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use strata_txn::Dataset;
use strata_txn::mvp_fixtures::{mvp_batch, mvp_schema};

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
fn a_snapshot_never_gains_or_loses_rows_after_it_was_taken() {
    let dir = tempfile::Builder::new()
        .prefix("strata-concurrent-snapshot-isolation-")
        .tempdir()
        .unwrap()
        .keep();
    Dataset::create(&dir).unwrap();
    let writer_dataset = Dataset::open(&dir).unwrap();

    // Seed one row before any reader takes a snapshot, so every reader's
    // first snapshot has at least one guaranteed-present row. `mvp_batch`
    // takes `(id, name, vector)` tuples — `id` is the schema's business
    // column, unrelated to the internal system row-id the commit path
    // assigns automatically.
    let mut seed_txn = writer_dataset.begin();
    seed_txn.insert(mvp_batch(&[(0, "seed", [0.0, 0.0, 0.0])]).unwrap());
    seed_txn.commit().unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread: commits 20 more single-row batches, one every loop
    // iteration, then signals readers to stop.
    let writer_stop = Arc::clone(&stop);
    let writer_dataset_clone = writer_dataset.clone();
    let writer = std::thread::spawn(move || {
        for i in 1..=20i64 {
            let mut txn = writer_dataset_clone.begin();
            txn.insert(mvp_batch(&[(i, "row", [i as f32, 0.0, 0.0])]).unwrap());
            txn.commit().unwrap();
        }
        writer_stop.store(true, Ordering::SeqCst);
    });

    // Reader threads: each repeatedly takes a fresh snapshot, then re-scans
    // that SAME snapshot several times before moving on, checking that the
    // row count never changes across those repeated reads of one snapshot.
    // Each thread returns how many outer iterations it actually ran, so a
    // reader scheduled entirely after the writer finishes (which would
    // otherwise run zero iterations and vacuously "pass") is caught below
    // instead of silently contributing no assertions.
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader_dataset = writer_dataset.clone();
            let reader_stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut iterations = 0u32;
                while !reader_stop.load(Ordering::SeqCst) {
                    let snapshot = reader_dataset.snapshot();
                    let first_count = snapshot.scan(&mvp_schema()).unwrap().num_rows();
                    assert!(
                        first_count >= 1,
                        "even the earliest snapshot must see at least the seed row"
                    );
                    for _ in 0..5 {
                        let again_count = snapshot.scan(&mvp_schema()).unwrap().num_rows();
                        assert_eq!(
                            again_count, first_count,
                            "a held Snapshot's row count must never change across repeated \
                             reads of the SAME snapshot, even while the writer commits more \
                             rows concurrently"
                        );
                    }
                    iterations += 1;
                }
                iterations
            })
        })
        .collect();

    writer.join().unwrap();
    for reader in readers {
        let iterations = reader.join().unwrap();
        assert!(
            iterations >= 1,
            "every reader thread must run at least one real iteration against a live \
             snapshot — a reader that ran zero would vacuously pass without checking anything"
        );
    }

    // Final sanity check: after the writer finishes, a fresh snapshot sees
    // every row (1 seed + 20 committed).
    let final_count = writer_dataset
        .snapshot()
        .scan(&mvp_schema())
        .unwrap()
        .num_rows();
    assert_eq!(
        final_count, 21,
        "expected all 21 rows to be visible after the writer finishes"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn an_old_snapshots_scan_never_gains_a_later_commits_rows() {
    // The direct regression test for the isolation bug this whole design
    // exists to fix: a reader's snapshot must NOT gain a row committed
    // after it was taken.
    let dir = tempfile::Builder::new()
        .prefix("strata-old-snapshot-scan-isolation-")
        .tempdir()
        .unwrap()
        .keep();
    Dataset::create(&dir).unwrap();
    let dataset = Dataset::open(&dir).unwrap();

    let mut txn = dataset.begin();
    txn.insert(
        mvp_batch(&[
            (0, "a", [0.0, 0.0, 0.0]),
            (1, "b", [1.0, 0.0, 0.0]),
            (2, "c", [2.0, 0.0, 0.0]),
        ])
        .unwrap(),
    );
    txn.commit().unwrap();

    // Take a snapshot BEFORE the later commit.
    let old_snapshot = dataset.snapshot();
    let old_count = old_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(old_count, 3, "expected all 3 seeded rows visible");

    // `Transaction::delete` is now a real public API, unlike when this test
    // was first written — but `Snapshot::scan`/`scan_with_predicate` do not
    // currently consult `Snapshot::tombstones` at all (only
    // `Snapshot::vector_search`'s HNSW traversal does, via `is_visible`;
    // see `crates/txn/src/snapshot.rs`). That is a real gap against
    // `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8's
    // "Tombstone GC" paragraph, which requires "`scan`, `search`, and
    // (later) conflict detection must all treat a tombstoned row-id as
    // dead" — confirmed empirically while writing this test (deleting a row
    // and observing a fresh snapshot's `scan()` still returns it), and
    // tracked as its own follow-up task ("Fix Snapshot::scan() ignoring
    // tombstones entirely") rather than fixed as a drive-by change in an
    // unrelated test-accuracy PR. Until that's resolved, this test can only
    // exercise the STRUCTURAL guarantee: re-scanning the SAME old_snapshot
    // after MORE inserts land still shows exactly the old snapshot's own
    // row count, proving old_snapshot's manifest/view is frozen and can
    // never grow after the fact.
    let mut txn2 = dataset.begin();
    txn2.insert(mvp_batch(&[(3, "d", [3.0, 0.0, 0.0]), (4, "e", [4.0, 0.0, 0.0])]).unwrap());
    txn2.commit().unwrap();

    let old_count_again = old_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(
        old_count_again, 3,
        "a Snapshot taken before a later commit must never change, even after that later \
         commit lands — this is the core isolation guarantee"
    );

    let new_snapshot = dataset.snapshot();
    let new_count = new_snapshot.scan(&mvp_schema()).unwrap().num_rows();
    assert_eq!(
        new_count, 5,
        "a freshly-taken snapshot must see the new commit's rows"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// Generates `count` points scattered within a small cube of side `spacing`
/// around `center`. Mirrors `crates/txn/src/dataset.rs`'s own
/// `cluster_vectors` test helper (itself mirroring
/// `crates/index/src/hnsw.rs`'s `insert_cluster`, see commit `733579f`):
/// `crates/index`'s layer assignment (a deterministic, counter-derived hash
/// draw — see `crates/index/src/hnsw.rs` — not a seeded RNG) can still land
/// tiny (2-3 point) fixtures on a graph shape where greedy search misses the
/// true nearest neighbor. Many points spread across well-separated clusters
/// makes "which cluster is nearest" unambiguous regardless of layer-
/// assignment luck. Offsets come from an irrational-multiplier
/// equidistribution sequence rather than a line/grid, since collinear
/// near-duplicate points let the neighbor-diversification heuristic prune
/// almost all direct links between them.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn cluster_vectors(count: usize, center: [f32; 3], spacing: f32) -> Vec<[f32; 3]> {
    const PHI: f64 = 0.618_033_988_749_895; // fractional part of the golden ratio
    const SQRT2: f64 = 0.414_213_562_373_095; // fractional part of sqrt(2)
    const SQRT3: f64 = 0.732_050_807_568_877; // fractional part of sqrt(3)
    (0..count)
        .map(|i| {
            let n = i as f64;
            let frac = |mult: f64| (n * mult).fract();
            let dx = (frac(PHI) as f32) * spacing;
            let dy = (frac(SQRT2) as f32) * spacing;
            let dz = (frac(SQRT3) as f32) * spacing;
            [center[0] + dx, center[1] + dy, center[2] + dz]
        })
        .collect()
}

#[test]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn an_old_snapshots_vector_search_never_leaks_a_later_commits_rows() {
    // A later commit's INSERTS never leak into an old snapshot's
    // `vector_search` results. Since S1 (`crates/index`'s segmented
    // immutable index — see `.claude/rules/vector-index.md`), a
    // `Snapshot`'s index is its OWN `SegmentSet`: an immutable list of
    // segments fixed at the instant that snapshot was published
    // (`Snapshot.index`, `crates/txn/src/snapshot.rs`). A later commit
    // builds a brand-new segment and appends it to a NEW `SegmentSet` via
    // `SegmentSet::with_appended` (`crates/index/src/segment_set.rs`),
    // which never mutates the old snapshot's own `SegmentSet` — there is
    // no shared, ever-growing graph object for a later commit's vectors to
    // become "physically present" in from an old snapshot's point of view.
    // This is what the near/far-cluster checks below prove: the far
    // cluster's segment simply never exists in `old_snapshot.index`.
    //
    // This property holds regardless of what `Snapshot::is_visible` does —
    // it would pass even if `is_visible` were hardcoded to `true`. The
    // complementary property — that a later commit's DELETE (tombstone) is
    // scoped to snapshots taken after it, which IS `is_visible`'s actual
    // remaining job — is a sibling test, since it needs a different
    // mechanism to discriminate:
    // `an_old_snapshots_vector_search_still_sees_a_row_a_later_commit_tombstones`.
    //
    // `scan()`'s analogous insert-isolation is covered by the two tests
    // above; this test is the `vector_search` analog, going through the
    // real `Dataset`/`Transaction`/`commit` path (not
    // `HnswIndex`/`SegmentSet` directly), and therefore through
    // `Snapshot::vector_search`'s no-predicate branch's PRODUCTION HNSW
    // parameters
    // (`HNSW_MAX_NB_CONNECTION=16`, `HNSW_EF_CONSTRUCTION`,
    // `EF_SEARCH_DEFAULT=32` in `crates/txn/src/dataset.rs` /
    // `crates/txn/src/snapshot.rs`) — weaker than the elevated test-only
    // parameters `crates/index/src/hnsw.rs`'s own unit tests use.
    //
    // IMPORTANT, learned the hard way while writing this test: requesting
    // `k` equal to a cluster's full point count is NOT safe even with a
    // well-separated cluster — `ef_search=32` occasionally fails to
    // discover literally every point in a same-sized cluster (a genuine
    // recall gap of approximate greedy search over a small graph, not an
    // isolation bug; see 9/10 and 10/10-with-one-foreign-point failures observed
    // during manual repeated-run validation of an earlier k=cluster_size
    // version of this test). The fix, matching this codebase's own proven
    // pattern in `vector_search_with_predicate_only_returns_matching_rows`
    // (`crates/txn/src/dataset.rs`): use a cluster noticeably LARGER than
    // `k`, so a missed connection or two doesn't starve the result set.
    // Cluster separation (100,000 units) is still far beyond the 1,000
    // units that test uses, to compensate for the weaker production
    // ef_search default.
    const CLUSTER_SIZE: usize = 20;
    const K: usize = 5;

    let dir = tempfile::Builder::new()
        .prefix("strata-old-snapshot-vector-search-isolation-")
        .tempdir()
        .unwrap()
        .keep();
    Dataset::create(&dir).unwrap();
    let dataset = Dataset::open(&dir).unwrap();

    // First commit: a 20-point cluster near the origin, row-ids 0..19.
    let near_cluster = cluster_vectors(CLUSTER_SIZE, [0.0, 0.0, 0.0], 0.01);
    let near_rows: Vec<(i64, &str, [f32; 3])> = (0..CLUSTER_SIZE)
        .map(|i| (i as i64, "near", near_cluster[i]))
        .collect();
    let mut txn = dataset.begin();
    txn.insert(mvp_batch(&near_rows).unwrap());
    txn.commit().unwrap();

    // Take a snapshot BEFORE the second (far) cluster is committed.
    let old_snapshot = dataset.snapshot();
    let old_results = old_snapshot
        .vector_search(&[0.0, 0.0, 0.0], K, None)
        .unwrap();
    assert_eq!(
        old_results.len(),
        K,
        "expected {K} near-cluster rows visible before the far cluster is committed: \
         {old_results:?}"
    );
    assert!(
        old_results.iter().all(|r| r.row_id < CLUSTER_SIZE as u64),
        "every result must come from the near cluster (row_id < {CLUSTER_SIZE}): {old_results:?}"
    );

    // Second commit: a 20-point cluster centered 100,000 units away, with
    // DIFFERENT row-ids (20..39). This builds a brand-new segment and
    // appends it to a NEW `SegmentSet` (`with_appended`) — `old_snapshot`'s
    // own `SegmentSet`, taken before this commit, is untouched by it.
    let far_center = [100_000.0, 0.0, 0.0];
    let far_cluster = cluster_vectors(CLUSTER_SIZE, far_center, 0.01);
    let far_rows: Vec<(i64, &str, [f32; 3])> = (0..CLUSTER_SIZE)
        .map(|i| (CLUSTER_SIZE as i64 + i as i64, "far", far_cluster[i]))
        .collect();
    let mut txn2 = dataset.begin();
    txn2.insert(mvp_batch(&far_rows).unwrap());
    txn2.commit().unwrap();

    // Re-run vector_search on the SAME old_snapshot. The far cluster's
    // segment was appended only to the NEW SegmentSet the second commit
    // published — old_snapshot's own SegmentSet, fixed when it was taken,
    // never gained it.
    let old_results_again = old_snapshot
        .vector_search(&[0.0, 0.0, 0.0], K, None)
        .unwrap();
    assert_eq!(
        old_results_again.len(),
        K,
        "an old snapshot's vector_search must still return {K} near-cluster rows after a later \
         commit publishes a new segment: {old_results_again:?}"
    );
    assert!(
        old_results_again
            .iter()
            .all(|r| r.row_id < CLUSTER_SIZE as u64),
        "an old snapshot's vector_search must NEVER return a row from a later commit — its own \
         SegmentSet never gained that commit's segment in the first place: {old_results_again:?}"
    );

    // The two checks above query near the origin, where the far cluster
    // (100,000 units away) is never a plausible nearest-neighbor candidate
    // regardless of whether segment-set isolation works — so they can't
    // actually distinguish "isolation enforced" from "isolation broken."
    // This is the check that can: query old_snapshot AT the far cluster's
    // own center. If old_snapshot's SegmentSet somehow DID gain the far
    // cluster's segment, the far cluster's rows are genuinely nearest here
    // and WOULD be returned. With segment-set isolation correctly enforced,
    // old_snapshot must fall back to the near cluster instead — proving
    // its SegmentSet is doing real (unfavorable-geometry) work, not merely
    // reflecting cluster distance.
    let old_results_at_far_center = old_snapshot.vector_search(&far_center, K, None).unwrap();
    assert_eq!(
        old_results_at_far_center.len(),
        K,
        "an old snapshot querying AT the far cluster's own center must still fall back to \
         returning {K} near-cluster rows (the far cluster's segment is absent from its \
         SegmentSet): {old_results_at_far_center:?}"
    );
    assert!(
        old_results_at_far_center
            .iter()
            .all(|r| r.row_id < CLUSTER_SIZE as u64),
        "an old snapshot querying AT the far cluster's own center must NEVER return a \
         far-cluster row (row_id >= {CLUSTER_SIZE}), even though those rows are the genuine \
         nearest neighbors there — if this fails, the far cluster's segment leaked into \
         old_snapshot's SegmentSet: {old_results_at_far_center:?}"
    );

    // A freshly-taken snapshot, in contrast, must see the far cluster: query
    // near the far cluster's own center and confirm only far-cluster rows
    // (row_id >= CLUSTER_SIZE) come back.
    let new_snapshot = dataset.snapshot();
    let new_results = new_snapshot.vector_search(&far_center, K, None).unwrap();
    assert_eq!(
        new_results.len(),
        K,
        "a fresh snapshot must see {K} far-cluster rows: {new_results:?}"
    );
    assert!(
        new_results.iter().all(|r| r.row_id >= CLUSTER_SIZE as u64),
        "a fresh snapshot's vector_search near the far cluster's center must return only \
         far-cluster rows (row_id >= {CLUSTER_SIZE}): {new_results:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
fn an_old_snapshots_vector_search_still_sees_a_row_a_later_commit_tombstones() {
    // The `vector_search` analog of `an_old_snapshots_scan_never_gains_a_later_commits_rows`
    // above, and the complementary property to
    // `an_old_snapshots_vector_search_never_leaks_a_later_commits_rows`: that test proves a later
    // commit's INSERTS never leak into an old snapshot (segment-set isolation, which holds
    // regardless of what `Snapshot::is_visible` does). This test proves the other direction — a
    // later commit's DELETE (tombstone) is scoped to snapshots taken after it, never applied
    // retroactively to a snapshot taken before it — which is `Snapshot::is_visible`'s actual
    // remaining job: `is_visible(row_id)` is exactly `!self.tombstones.contains(&row_id)`
    // (`crates/txn/src/snapshot.rs`), where `tombstones` is captured from `Manifest.tombstones` as
    // of THAT snapshot's own version.
    //
    // Deletion never rewrites a segment (`.claude/rules/vector-index.md`) — a deleted row's vector
    // stays physically present in its segment forever; only the manifest's tombstone set (and
    // therefore what `is_visible` filters) changes.
    let dir = tempfile::Builder::new()
        .prefix("strata-old-snapshot-vector-search-tombstone-")
        .tempdir()
        .unwrap()
        .keep();
    Dataset::create(&dir).unwrap();
    let dataset = Dataset::open(&dir).unwrap();

    let row_0_vector = [0.0f32, 0.0, 0.0];
    let mut txn = dataset.begin();
    txn.insert(
        mvp_batch(&[
            (0, "a", row_0_vector),
            (1, "b", [1.0, 0.0, 0.0]),
            (2, "c", [2.0, 0.0, 0.0]),
        ])
        .unwrap(),
    );
    txn.commit().unwrap();

    // Take a snapshot BEFORE the tombstoning commit.
    let old_snapshot = dataset.snapshot();

    let mut delete_txn = dataset.begin();
    delete_txn.delete(0);
    delete_txn.commit().unwrap();

    // Query at row 0's OWN exact coordinates with k=1, rather than a larger k against the whole
    // (here, tiny) point set. Three collinear points don't build a complete layer-0 graph — the
    // neighbor-diversification heuristic prunes the direct 0-2 edge here exactly as
    // `crates/index/src/graph.rs`'s `search_layer_traverses_through_an_excluded_node_to_reach_a_node_beyond_it`
    // documents for the identical geometry — but at 3 nodes, `EF_SEARCH_DEFAULT=32`
    // (`crates/txn/src/snapshot.rs`) far exceeds the graph size, so the layer-0 beam never evicts a
    // candidate (and the saturation early-exit can't fire either -- it's measured as a fraction of
    // `ef`, so 3 results out of 32 never reaches its threshold) -- search is exhaustive over the
    // (connected-by-construction) graph regardless of which edges the heuristic kept: an
    // exact-coordinate query cannot miss its own point. This is
    // simpler than picking a `k` and reasoning about which other rows would also qualify: a k=1
    // exact-coordinate self-match (trivially nearest to itself, distance ~0) is an unambiguous
    // yes/no on "is this exact row still visible" — the same technique
    // `crates/txn/src/dataset.rs`'s loom "Model 3" uses (`found_own_point`).
    //
    // old_snapshot's own tombstone set was fixed before this delete committed, so it must still
    // return row 0 here.
    let old_self_match_after_delete = old_snapshot.vector_search(&row_0_vector, 1, None).unwrap();
    assert_eq!(
        old_self_match_after_delete.first().map(|m| m.row_id),
        Some(0),
        "a Snapshot taken before a later delete must still see the deleted row — tombstones are \
         scoped to the snapshot that observes them, never applied retroactively to a snapshot \
         taken earlier: {old_self_match_after_delete:?}"
    );

    // A freshly-taken snapshot, in contrast, must never return row 0: its own tombstone set
    // (captured from the manifest as of ITS version) includes it, and `Snapshot::is_visible`
    // filters it out during traversal — even though row 0's vector is still physically present in
    // the segment.
    let post_delete_snapshot = dataset.snapshot();
    let post_delete_self_match = post_delete_snapshot
        .vector_search(&row_0_vector, 1, None)
        .unwrap();
    assert!(
        !post_delete_self_match.is_empty(),
        "sanity: rows 1 and 2 are still live after deleting row 0, so a k=1 query at row 0's old \
         coordinates must still return something, or this test isn't exercising the hazard: \
         {post_delete_self_match:?}"
    );
    assert_ne!(
        post_delete_self_match[0].row_id, 0,
        "a snapshot taken after a delete must never return the deleted row, even querying at its \
         own exact coordinates — if this fails, Snapshot::is_visible's tombstone check is not \
         actually being applied: {post_delete_self_match:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
