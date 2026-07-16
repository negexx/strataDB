//! HNSW vector index wrapper. See `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-4-vector-index-spec.md` §1.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use hnsw_rs::prelude::{DistL2, FilterT, Hnsw};

/// One search result: which row-id, and its squared L2 distance to the
/// query vector. `row_id` is the persistent, global identity from
/// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 — not a
/// position within any particular array, unlike `brute_force::Neighbor`.
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
    tombstones: HashSet<u64>,
    dimension: AtomicUsize,
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
            tombstones: HashSet::new(),
            dimension: AtomicUsize::new(0),
        })
    }

    pub fn insert(&self, row_id: u64, vector: &[f32]) {
        self.dimension
            .compare_exchange(0, vector.len(), Ordering::SeqCst, Ordering::SeqCst)
            .ok(); // only the first insert sets it; later calls leave it as-is
        #[allow(clippy::cast_possible_truncation)]
        let id = row_id as usize;
        self.hnsw.insert((vector, id));
    }

    pub fn tombstone(&mut self, row_id: u64) {
        self.tombstones.insert(row_id);
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
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        // Tombstone exclusion is pushed into hnsw_rs's own `FilterT`
        // mechanism, not applied as a post-filter on an already-capped
        // top-k, so a tombstoned candidate can't silently shrink the
        // result set below `k` — see
        // `.claude/docs/design/phase-4-vector-index-spec.md` §1.
        let filter = self.not_tombstoned_filter();
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
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.check_dimension(query)?;
        // live_ids is caller-supplied and expected sorted (binary-searched
        // below) — sort defensively rather than trusting every caller got
        // this right.
        let mut live_ids = live_ids.to_vec();
        live_ids.sort_unstable();
        let tombstones = &self.tombstones;
        // Membership in `live_ids` and tombstone-exclusion are composed
        // into a single `FilterT` predicate, so both are applied during
        // hnsw_rs's own traversal/candidate-heap construction rather than
        // as a post-filter on an already-capped top-k.
        let filter = move |id: &hnsw_rs::prelude::DataId| {
            live_ids.binary_search(id).is_ok() && !tombstones.contains(&Self::to_row_id(*id))
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

    fn not_tombstoned_filter(&self) -> impl Fn(&hnsw_rs::prelude::DataId) -> bool + '_ {
        move |id: &hnsw_rs::prelude::DataId| !self.tombstones.contains(&Self::to_row_id(*id))
    }

    #[allow(clippy::cast_possible_truncation)]
    fn to_row_id(id: hnsw_rs::prelude::DataId) -> u64 {
        id as u64
    }

    fn to_matches(raw: Vec<hnsw_rs::prelude::Neighbour>) -> Vec<VectorMatch> {
        raw.into_iter()
            .map(|n| VectorMatch {
                row_id: Self::to_row_id(n.get_origin_id()),
                squared_distance: n.get_distance(),
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
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
            index.insert(
                start_id + i,
                &[center[0] + dx, center[1] + dy, center[2] + dz],
            );
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
        let results = index.search(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH).unwrap();
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
    fn tombstoned_row_is_never_returned_even_as_the_true_nearest_neighbor() {
        let index = {
            let mut index = HnswIndex::new(
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
            // Row 0 is the exact-match true nearest neighbor; tombstone it.
            index.tombstone(0);
            index
        };

        // Tombstone exclusion happens inside hnsw_rs's own traversal-level
        // filter now (not a Rust-side post-filter on an already-capped
        // top-k), so asking for exactly 5 candidates is enough to get 5
        // live results even though the true nearest neighbor is
        // tombstoned — no "ask for one extra" compensation needed.
        let results = index.search(&[0.0, 0.0, 0.0], 5, TEST_EF_SEARCH).unwrap();
        assert_eq!(
            results.len(),
            5,
            "the near cluster has 14 live rows left after the tombstone, all vastly \
             closer than the far cluster, so the top 5 must still be fully populated: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id != 0),
            "the tombstoned row must be excluded, not just re-ranked: {results:?}"
        );
        assert!(
            results.iter().all(|r| r.row_id < 15),
            "every returned row must still be a genuine near-cluster neighbor, \
             not a fallback to the far cluster: {results:?}"
        );
    }

    #[test]
    fn tombstone_of_the_single_nearest_neighbor_still_returns_k_live_results_for_small_k() {
        let index = {
            let mut index = HnswIndex::new(
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
            // Row 0 is the exact-match true nearest neighbor; tombstone
            // it. Under the old design, `hnsw_rs`'s unfiltered
            // `Hnsw::search(query, 1, ef)` would return exactly one raw
            // candidate — row 0, the unambiguous nearest — and
            // post-filtering it out afterward would leave *zero* results
            // even though 14 live near-cluster rows exist. Pushing the
            // tombstone into hnsw_rs's own traversal-level filter (via
            // `search_filter`) means row 0 is never considered a
            // candidate in the first place, so the true next-nearest
            // *live* neighbor is found instead.
            index.tombstone(0);
            index
        };

        let results = index.search(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH).unwrap();
        assert_eq!(
            results.len(),
            1,
            "a tombstoned true-nearest-neighbor must not shrink the result \
             count below k when enough live candidates exist deeper in the \
             graph: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the tombstoned row must never be returned: {results:?}"
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
            .search_filtered(&[0.0, 0.0, 0.0], 3, TEST_EF_SEARCH, &live_ids)
            .unwrap();
        assert_eq!(results.len(), 3, "unexpected results: {results:?}");
        assert!(
            results.iter().all(|r| r.row_id >= 15),
            "search_filtered must only return ids from the live set, even when \
             closer points exist outside it: {results:?}"
        );
    }

    #[test]
    fn search_filtered_excludes_tombstones_even_for_the_single_nearest_live_id() {
        let index = {
            let mut index = HnswIndex::new(
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
            // near-cluster live set; tombstone it. Tombstone-exclusion is
            // composed into the same `FilterT` predicate as the
            // `live_ids` membership check, so both are applied during
            // hnsw_rs's own traversal — not as a Rust-side post-filter
            // that could silently return fewer than k live results.
            index.tombstone(0);
            index
        };

        // Every near-cluster row is "live" per the caller's predicate;
        // only the tombstone should exclude row 0.
        let live_ids: Vec<usize> = (0..15).collect();
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 1, TEST_EF_SEARCH, &live_ids)
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "a tombstoned true-nearest live id must not shrink the result \
             count below k when enough other live candidates exist: {results:?}"
        );
        assert_ne!(
            results[0].row_id, 0,
            "the tombstoned row must never be returned, even though it is \
             in the live set: {results:?}"
        );
        assert!(
            results[0].row_id < 15,
            "the returned row must be a genuine near-cluster neighbor, not a \
             fallback to the far cluster: {results:?}"
        );
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
        index.insert(0, &[0.0, 0.0, 0.0]);

        let result = index.search(&[0.0, 0.0], 1, 50);
        assert!(matches!(
            result,
            Err(IndexError::DimensionMismatch {
                query_len: 2,
                expected: 3
            })
        ));
    }
}
