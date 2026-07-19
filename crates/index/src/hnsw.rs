//! HNSW vector index — lock-free, from-scratch implementation (replacing
//! `hnsw_rs` as of this rewrite). See `.claude/rules/vector-index.md` and
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

use crate::distance::L2;
use crate::graph::Graph;

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

/// # Examples
///
/// ```
/// use strata_index::IndexError;
///
/// let err = IndexError::MaxConnectionTooLarge(300);
/// assert_eq!(
///     err.to_string(),
///     "max_nb_connection must be <= 256 (hnsw_rs hard limit), got 300"
/// );
/// ```
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

/// Maximum number of bidirectional links per node per layer (`hnsw_rs`'s
/// `max_nb_connection`) — hard-capped at 256 by the underlying library, see
/// [`HnswIndex::new`]'s doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxConnections(pub usize);

/// Expected/reserved capacity for the graph's internal allocation
/// (`hnsw_rs`'s `max_elements`) — a sizing hint, not a hard cap on how many
/// vectors can be inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxElements(pub usize);

/// Maximum number of layers in the graph's hierarchy (`hnsw_rs`'s
/// `max_layer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxLayers(pub usize);

/// Candidate-list size used while building the graph (`hnsw_rs`'s
/// `ef_construction`) — higher values trade insert time for graph quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EfConstruction(pub usize);

pub struct HnswIndex {
    graph: Graph<L2>,
    m: usize,
    mmax0: usize,
    mmax: usize,
    ef_construction: usize,
    m_l: f64,
    row_counter: std::sync::atomic::AtomicU64, // supplies a deterministic unif draw per insert; see Self::insert's note below
}

impl HnswIndex {
    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16),
    ///     MaxElements(100),
    ///     MaxLayers(16),
    ///     EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::MaxConnectionTooLarge`] if `max_nb_connection`
    /// exceeds 256 — this validation predates the lock-free rewrite (it
    /// used to guard against `hnsw_rs::Hnsw::new`'s uncatchable
    /// `std::process::exit(1)` on that condition) and is retained
    /// unconditionally: 256 remains this crate's own documented connection
    /// ceiling regardless of backing implementation.
    pub fn new(
        max_nb_connection: MaxConnections,
        max_elements: MaxElements,
        max_layer: MaxLayers,
        ef_construction: EfConstruction,
    ) -> Result<Self, IndexError> {
        if max_nb_connection.0 > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection.0));
        }
        let _ = max_layer; // MaxLayers is retained in the public signature for API compatibility; the new design derives level count from mL/unif rather than a hard layer cap — see design doc §2/§3.
        let m = max_nb_connection.0.max(1);
        // `m` is capped at 256 by the `max_nb_connection.0 > 256` check
        // above, so this cast is always exact — never a real precision
        // loss, just a lint that can't see the bound.
        #[allow(clippy::cast_precision_loss)]
        let m_l = 1.0 / (m as f64).ln();
        Ok(Self {
            graph: Graph::new(L2, max_elements.0.max(1)),
            m,
            mmax0: m * 2,
            mmax: m,
            ef_construction: ef_construction.0,
            m_l,
            row_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[1.0, 2.0, 3.0])?;
    /// assert_eq!(index.established_dimension(), 3);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `vector`'s length
    /// doesn't match the dimensionality of the first vector ever inserted.
    /// Checked upfront (inside `Graph::insert`'s own
    /// `check_or_establish_dimension` call) so a corrupted delta-log entry
    /// with a wrong-length vector can never reach the distance function.
    pub fn insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError> {
        // A deterministic-but-varying draw per insert, avoiding a new RNG
        // dependency: derived from a monotonically-advancing counter run
        // through a fixed hash, mapped into (0, 1). This is NOT
        // cryptographic or high-quality randomness — HNSW's level
        // assignment only needs a source that varies across inserts to
        // achieve the paper's expected layer-count distribution, and this
        // project's own established precedent (this file's *old* tests)
        // already tolerates non-reproducible layer assignment (see the
        // existing `insert_cluster` test helper's doc comment on
        // hnsw_rs's own unseeded RNG). If a real `rand`-crate dependency
        // is preferred instead, swap this for one — flagged here as an
        // explicit, deliberate choice for the implementer/reviewer to
        // confirm, not a silent placeholder.
        let n = self
            .row_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 33;
        #[allow(clippy::cast_precision_loss)]
        let unif = ((x >> 11) as f64 / (1u64 << 53) as f64).clamp(f64::EPSILON, 1.0 - f64::EPSILON);

        self.graph.insert(
            row_id,
            vector.to_vec(),
            self.m,
            self.mmax0,
            self.mmax,
            self.ef_construction,
            self.m_l,
            1.0,
            unif,
        )
    }

    /// The vector dimension established by the first-ever [`Self::insert`]
    /// call, or `0` if no vector has been inserted yet. Read-only — never
    /// establishes a dimension itself. Exposed so callers (e.g.
    /// `crates/txn`'s `Transaction::commit`) can pre-validate a batch of
    /// pending inserts' dimensions against this index *before* applying
    /// any of them, rather than discovering a mismatch mid-application.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.graph.established_dimension()
    }

    /// # Examples
    ///
    /// ```
    /// use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    ///
    /// let index = HnswIndex::new(
    ///     MaxConnections(16), MaxElements(100), MaxLayers(16), EfConstruction(200),
    /// )?;
    /// index.insert(0, &[0.0, 0.0, 0.0])?;
    /// index.insert(1, &[10.0, 10.0, 10.0])?;
    ///
    /// let results = index.search(&[0.0, 0.0, 0.0], 1, 50, |_| true)?;
    /// assert_eq!(results[0].row_id, 0);
    /// # Ok::<(), strata_index::IndexError>(())
    /// ```
    ///
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
        let raw = self.graph.k_nn_search(query, k, ef_search, is_visible)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
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
        // live_ids membership and is_visible are composed into ONE
        // predicate passed straight into k_nn_search -> search_layer,
        // applied during traversal-time result-set construction — not a
        // post-filter over an already-capped top-k. This matches
        // hnsw_rs's original FilterT-based behavior exactly: a live_ids
        // row deep in the graph is never missed just because it fell
        // outside some pre-guessed widened candidate window, since the
        // predicate is evaluated as part of the same search, not after it.
        let live: std::collections::HashSet<u64> = live_ids.iter().map(|&id| id as u64).collect();
        let filter = move |id: u64| live.contains(&id) && is_visible(id);
        let raw = self.graph.k_nn_search(query, k, ef_search, filter)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
            MaxConnections(TEST_MAX_NB_CONNECTION),
            MaxElements(100),
            MaxLayers(TEST_MAX_LAYER),
            EfConstruction(TEST_EF_CONSTRUCTION),
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
        let result = HnswIndex::new(
            MaxConnections(257),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        );
        assert!(matches!(
            result,
            Err(IndexError::MaxConnectionTooLarge(257))
        ));
    }

    #[test]
    fn search_errors_on_dimension_mismatch() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
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
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
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

    #[test]
    fn established_dimension_is_zero_before_any_insert() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        assert_eq!(index.established_dimension(), 0);
    }

    #[test]
    fn established_dimension_reflects_the_first_inserted_vectors_length() {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(200),
        )
        .unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]).unwrap();
        assert_eq!(index.established_dimension(), 3);
    }
}
