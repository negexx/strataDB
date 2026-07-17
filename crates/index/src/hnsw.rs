//! HNSW vector index wrapper. See `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-4-vector-index-spec.md` §1.

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

use hnsw_rs::prelude::{DistL2, FilterT, Hnsw};

/// One search result: which row-id, and its squared L2 distance to the
/// query vector. `row_id` is the persistent, global identity from
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 — not a
/// position within any particular array, unlike `brute_force::Neighbor`.
///
/// `squared_distance` is the sum of squared per-dimension differences (no
/// square root), the same units as `brute_force::Neighbor::squared_distance`
/// — `hnsw_rs`'s underlying `anndists::DistL2` returns true (non-squared)
/// Euclidean distance, so `to_matches` squares it before constructing this
/// struct.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorMatch {
    pub row_id: u64,
    pub squared_distance: f32,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("max_nb_connection must be <= 256 (hnsw_rs hard limit), got {0}")]
    MaxConnectionTooLarge(usize),
    #[error("query has {query_len} dimensions, but the index expects {expected}")]
    DimensionMismatch { query_len: usize, expected: usize },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("delta log entry serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistL2>,
    dimension: AtomicUsize,
}

/// Atomically establishes the vector dimension on the first call (via
/// `compare_exchange`), or validates `len` against the already-established
/// dimension on every subsequent call — including the losing side of a
/// concurrent race to be "first", which must validate against whichever
/// length actually won, never silently succeed with a different one.
///
/// # Errors
///
/// Returns [`IndexError::DimensionMismatch`] if `len` doesn't match the
/// dimension this call just established or a prior call already established.
fn establish_or_check_dimension(dimension: &AtomicUsize, len: usize) -> Result<(), IndexError> {
    dimension
        .compare_exchange(0, len, Ordering::SeqCst, Ordering::SeqCst)
        .ok();
    let established = dimension.load(Ordering::SeqCst);
    if established != 0 && len != established {
        return Err(IndexError::DimensionMismatch {
            query_len: len,
            expected: established,
        });
    }
    Ok(())
}

impl HnswIndex {
    /// # Errors
    ///
    /// Returns [`IndexError::MaxConnectionTooLarge`] if `max_nb_connection`
    /// exceeds 256 — checked here because the underlying `hnsw_rs::Hnsw::new`
    /// calls `std::process::exit(1)` (not a panic — an uncatchable process
    /// exit) on that condition, verified against the installed
    /// `hnsw_rs-0.3.4` source. This function must never let a bad
    /// caller-supplied value reach that call.
    pub fn new(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> Result<Self, IndexError> {
        if max_nb_connection > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection));
        }
        Ok(Self {
            hnsw: Hnsw::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                DistL2 {},
            ),
            dimension: AtomicUsize::new(0),
        })
    }

    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `vector`'s length
    /// doesn't match the dimensionality of the first vector ever inserted.
    /// Checked upfront so a corrupted delta-log entry with a wrong-length
    /// vector can never reach `hnsw_rs`'s underlying distance function,
    /// which does not itself validate vector lengths for `f32`/`DistL2` —
    /// verified against the installed `anndists-0.1.5` source, where the
    /// dedicated `impl Distance<f32> for DistL2` has no length assertion
    /// and would otherwise silently zip-truncate to the shorter vector,
    /// producing a wrong distance instead of an error.
    pub fn insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError> {
        establish_or_check_dimension(&self.dimension, vector.len())?;
        #[allow(clippy::cast_possible_truncation)]
        let id = row_id as usize;
        self.hnsw.insert((vector, id));
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `query`'s length
    /// doesn't match the dimensionality of the first vector ever inserted —
    /// checked upfront rather than silently truncating, matching
    /// `brute_force_search`'s existing Phase 1 behavior.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        // Visibility (tombstone exclusion, and — as of Phase 5 — snapshot
        // watermark filtering) is pushed into hnsw_rs's own `FilterT`
        // mechanism, not applied as a post-filter on an already-capped
        // top-k, so an invisible candidate can't silently shrink the
        // result set below `k` — see
        // `.claude/docs/design/phase-4-vector-index-spec.md` §1 and
        // `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`.
        let filter = move |id: &hnsw_rs::prelude::DataId| is_visible(Self::to_row_id(*id));
        let raw = self
            .hnsw
            .search_filter(query, k, ef_search, Some(&filter as &dyn FilterT));
        Ok(Self::to_matches(raw))
    }

    /// # Errors
    ///
    /// Same as [`Self::search`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        // live_ids is caller-supplied and expected sorted (binary-searched
        // below) — sort defensively rather than trusting every caller got
        // this right.
        let mut live_ids = live_ids.to_vec();
        live_ids.sort_unstable();
        // Membership in `live_ids` and visibility are composed into a
        // single `FilterT` predicate, so both are applied during
        // hnsw_rs's own traversal/candidate-heap construction rather than
        // as a post-filter on an already-capped top-k.
        let filter = move |id: &hnsw_rs::prelude::DataId| {
            live_ids.binary_search(id).is_ok() && is_visible(Self::to_row_id(*id))
        };
        let raw = self
            .hnsw
            .search_filter(query, k, ef_search, Some(&filter as &dyn FilterT));
        Ok(Self::to_matches(raw))
    }

    fn check_dimension(&self, query: &[f32]) -> Result<(), IndexError> {
        let dimension = self.dimension.load(Ordering::SeqCst);
        if dimension != 0 && query.len() != dimension {
            return Err(IndexError::DimensionMismatch {
                query_len: query.len(),
                expected: dimension,
            });
        }
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn to_row_id(id: hnsw_rs::prelude::DataId) -> u64 {
        id as u64
    }

    fn to_matches(raw: Vec<hnsw_rs::prelude::Neighbour>) -> Vec<VectorMatch> {
        raw.into_iter()
            .map(|n| {
                // `anndists::DistL2::eval` (the distance fn passed to
                // `Hnsw::new`) computes true (non-squared) Euclidean L2
                // distance for f32 — verified against the installed
                // `anndists-0.1.5` source, where `eval` ends in
                // `norm.sqrt()`. Squaring here restores the squared-L2
                // units `VectorMatch::squared_distance` promises, matching
                // `brute_force::Neighbor::squared_distance`'s units.
                // Squaring is monotonic for non-negative inputs, so it
                // can't change which neighbors were found or their
                // relative order — only the reported magnitude.
                let distance = n.get_distance();
                VectorMatch {
                    row_id: Self::to_row_id(n.get_origin_id()),
                    squared_distance: distance * distance,
                }
            })
            .collect()
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    // Empirically validated (10000-trial repro, zero failures) against a
    // 30-point, two-cluster fixture: enough connections/candidate budget
    // that `hnsw_rs`'s neighbor-diversification pruning can't leave any
    // near-cluster member unreachable during search. See `insert_cluster`'s
    // doc comment for why a lower `max_nb_connection` was still measurably
    // (if rarely) flaky on this same fixture.
    const TEST_MAX_NB_CONNECTION: usize = 200;
    const TEST_MAX_LAYER: usize = 16;
    const TEST_EF_CONSTRUCTION: usize = 1600;
    const TEST_EF_SEARCH: usize = 500;

    /// Inserts `count` points scattered within a small cube of side
    /// `spacing` around `center`, with row-ids `start_id..start_id + count`.
    ///
    /// `hnsw_rs::Hnsw::new` seeds its RNG from OS entropy with no seed
    /// exposed anywhere in the public API (verified against the installed
    /// `hnsw_rs-0.3.4` source), so unlucky random layer assignment can make
    /// greedy search miss the true nearest neighbor on tiny (2-3 point)
    /// fixtures. Using many points arranged in clusters that are far apart
    /// relative to their own radius makes "which cluster is nearest"
    /// unambiguous regardless of layer-assignment luck, without needing the
    /// library to expose a seed.
    ///
    /// Offsets come from an irrational-multiplier equidistribution
    /// sequence (fractional parts of `i * golden ratio`, etc.) rather than
    /// a regular line or grid. A 2000-trial repro showed that a line or
    /// axis-aligned grid of near-duplicate points lets `hnsw_rs`'s
    /// neighbor-diversification heuristic prune almost all direct links
    /// between them (they all point the same direction from any given
    /// node), occasionally leaving parts of the near cluster unreachable
    /// during search even with `ef_search` well above the point count.
    /// Quasi-random, non-collinear offsets avoid that degenerate case.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn insert_cluster(
        index: &HnswIndex,
        start_id: u64,
        count: u64,
        center: [f32; 3],
        spacing: f32,
    ) {
        const PHI: f64 = 0.618_033_988_749_895; // fractional part of the golden ratio
        const SQRT2: f64 = 0.414_213_562_373_095; // fractional part of sqrt(2)
        const SQRT3: f64 = 0.732_050_807_568_877; // fractional part of sqrt(3)
        for i in 0..count {
            let n = i as f64;
            let frac = |mult: f64| (n * mult).fract();
            let dx = (frac(PHI) as f32) * spacing;
            let dy = (frac(SQRT2) as f32) * spacing;
            let dz = (frac(SQRT3) as f32) * spacing;
            index
                .insert(
                    start_id + i,
                    &[center[0] + dx, center[1] + dy, center[2] + dz],
                )
                .unwrap();
        }
    }

    #[test]
    fn insert_then_search_finds_the_true_nearest_neighbor() {
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide cube
        // around (1000,0,0). Clusters are ~100000x farther apart than
        // their own radius, so which cluster is nearest is unambiguous
        // even under hnsw_rs's approximate search.
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);

        // Row 0 is an exact match for the query (offset 0 in the near
        // cluster) — the unambiguous true nearest neighbor.
        let results = index
            .search(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, |_| true)
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
        assert!(
            results[1].row_id < 15 && results[2].row_id < 15,
            "the next-nearest neighbors must come from the near cluster, not the far one: {results:?}"
        );
        assert!(
            results[0].squared_distance <= results[1].squared_distance
                && results[1].squared_distance <= results[2].squared_distance,
            "results must be ranked by increasing distance: {results:?}"
        );
    }

    #[test]
    fn invisible_row_is_never_returned_even_as_the_true_nearest_neighbor() {
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor; mark it invisible
        // (the caller-side equivalent of tombstoning it).
        let invisible: HashSet<u64> = HashSet::from([0]);

        // Visibility exclusion happens inside hnsw_rs's own traversal-level
        // filter (not a Rust-side post-filter on an already-capped top-k),
        // so asking for exactly 5 candidates is enough to get 5 live
        // results even though the true nearest neighbor is invisible — no
        // "ask for one extra" compensation needed.
        let results = index
            .search(&[0.0, 0.0, 0.0], 5, TEST_EF_SEARCH, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            5,
            "the near cluster has 14 live rows left after excluding row 0, all vastly \
             closer than the far cluster, so the top 5 must still be fully populated: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id != 0),
            "the invisible row must be excluded, not just re-ranked: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id < 15),
            "every returned row must still be a genuine near-cluster neighbor, \
             not a fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn invisibility_of_the_single_nearest_neighbor_still_returns_k_live_results_for_small_k() {
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor; mark it
        // invisible. Under the old design, `hnsw_rs`'s unfiltered
        // `Hnsw::search(query, 1, ef)` would return exactly one raw
        // candidate — row 0, the unambiguous nearest — and post-filtering
        // it out afterward would leave *zero* results even though 14 live
        // near-cluster rows exist. Pushing the exclusion into hnsw_rs's own
        // traversal-level filter (via `search_filter`) means row 0 is never
        // considered a candidate in the first place, so the true
        // next-nearest *live* neighbor is found instead.
        let invisible: HashSet<u64> = HashSet::from([0]);

        let results = index
            .search(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "an invisible true-nearest-neighbor must not shrink the result \
             count below k when enough live candidates exist deeper in the \
             graph: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the invisible row must never be returned: {results:?}"
        );
        assert!(
            results[0].row_id < 15,
            "the returned row must be a genuine near-cluster neighbor, not a \
             fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn search_filtered_only_returns_ids_in_the_live_set() {
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0) — much closer to the query than the far cluster, but
        // excluded from the live set below.
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        // Far cluster: row-ids 15..30, within a 0.01-wide cube around
        // (1000,0,0).
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);

        // Only the far cluster is "live" per the caller's predicate, even
        // though every near-cluster row is far closer to the query.
        let live_ids: Vec<usize> = (15..30).collect();
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, &live_ids, |_| true)
            .unwrap();
        assert_eq!(results.len(), 3, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 15),
            "search_filtered must only return ids from the live set, even when \
             closer points exist outside it: {results:?}"
        );
    }

    #[test]
    fn search_filtered_excludes_invisible_rows_even_for_the_single_nearest_live_id() {
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        // Near cluster: row-ids 0..15, within a 0.01-wide cube around
        // (0,0,0). Far cluster: row-ids 15..30, within a 0.01-wide
        // cube around (1000,0,0).
        insert_cluster(&index, 0, 15, [0.0, 0.0, 0.0], 0.01);
        insert_cluster(&index, 15, 15, [1000.0, 0.0, 0.0], 0.01);
        // Row 0 is the exact-match true nearest neighbor among the
        // near-cluster live set; mark it invisible. Visibility exclusion
        // is composed into the same `FilterT` predicate as the `live_ids`
        // membership check, so both are applied during hnsw_rs's own
        // traversal — not as a Rust-side post-filter that could silently
        // return fewer than k live results.
        let invisible: HashSet<u64> = HashSet::from([0]);

        // Every near-cluster row is "live" per the caller's predicate;
        // only the invisibility marker should exclude row 0.
        let live_ids: Vec<usize> = (0..15).collect();
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, &live_ids, |id| {
                !invisible.contains(&id)
            })
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "an invisible true-nearest live id must not shrink the result \
             count below k when enough other live candidates exist: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the invisible row must never be returned, even though it is \
             in the live set: {results:?}"
        );
        assert!(
            results[0].row_id < 15,
            "the returned row must be a genuine near-cluster neighbor, not a \
             fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn search_reports_squared_l2_distance_not_plain_l2() {
        // `anndists::DistL2::eval` returns true (non-squared) Euclidean
        // distance for f32 — verified against the installed
        // `anndists-0.1.5` source. A single point lets us hand-compute the
        // exact expected value and catch a regression to plain L2, which a
        // relative-ordering-only test (as above) cannot: a 3-4-5 triangle
        // gives distance 5.0 but squared distance 25.0, and those two
        // values are different enough that a `sqrt` vs. no-`sqrt` bug
        // can't accidentally pass.
        let index = HnswIndex::new(
            TEST_MAX_NB_CONNECTION,
            100,
            TEST_MAX_LAYER,
            TEST_EF_CONSTRUCTION,
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let results = index
            .search(&[3.0, 4.0, 0.0], 1, TEST_EF_SEARCH, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(
                results[0].squared_distance, 25.0,
                "expected squared L2 distance (3^2 + 4^2 = 25), not plain L2 \
                 distance (sqrt(25) = 5): {results:?}"
            );
        }
    }

    #[test]
    fn new_rejects_max_nb_connection_above_256() {
        let result = HnswIndex::new(257, 100, 16, 200);
        assert!(matches!(
            result,
            Err(IndexError::MaxConnectionTooLarge(257))
        ));
    }

    #[test]
    fn search_errors_on_dimension_mismatch() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let result = index.search(&[0.0, 0.0], 1, 50, |_| true);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }

    #[test]
    fn insert_errors_on_dimension_mismatch_with_previously_inserted_vectors() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();

        let result = index.insert(1, &[0.0, 0.0]);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }
}

/// Run with: `RUSTFLAGS="--cfg loom" cargo test -p strata-index --lib`
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    #[test]
    fn concurrent_first_inserts_race_safely_on_dimension() {
        loom::model(|| {
            let dimension = loom::sync::Arc::new(AtomicUsize::new(0));
            let d1 = loom::sync::Arc::clone(&dimension);
            let d2 = loom::sync::Arc::clone(&dimension);

            let t1 = loom::thread::spawn(move || establish_or_check_dimension(&d1, 3));
            let t2 = loom::thread::spawn(move || establish_or_check_dimension(&d2, 2));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one of the two racing "first" calls wins and
            // establishes the dimension; the other must observe a
            // DimensionMismatch against whichever length actually won —
            // never silently succeed with a different length, never leave
            // `dimension` at an intermediate/torn value.
            let established = dimension.load(Ordering::SeqCst);
            match established {
                3 => {
                    assert!(r1.is_ok());
                    assert!(matches!(
                        r2,
                        Err(IndexError::DimensionMismatch {
                            query_len: 2,
                            expected: 3
                        })
                    ));
                }
                2 => {
                    assert!(r2.is_ok());
                    assert!(matches!(
                        r1,
                        Err(IndexError::DimensionMismatch {
                            query_len: 3,
                            expected: 2
                        })
                    ));
                }
                other => panic!("dimension must be established as 2 or 3, got {other}"),
            }
        });
    }
}
