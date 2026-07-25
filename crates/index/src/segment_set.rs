//! The set of index parts a snapshot searches over. Today (S1 W3.1) this is
//! always exactly one `Live` part — the existing shared, mutable `HnswIndex`
//! — wrapped so `crates/txn` searches through a `SegmentSet` instead of a
//! bare `Arc<HnswIndex>`. This is a pure abstraction step: W3.2 (a later
//! plan) adds an `IndexPart::Sealed(Arc<SegmentReader>)` variant and a
//! per-commit build/publish path; W3.3 adds real multi-part fan-out to
//! `search`/`search_filtered` below. See
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §4 (W3.1) and its 2026-07-25 amendment for why `Sealed` is deliberately
//! absent from this file.

use std::sync::Arc;

use crate::graph::k_nn_search_generic;
use crate::hnsw::{HnswIndex, IndexError, VectorMatch, build_live_filter};

/// One part of a segment set. Only `Live` exists as of S1 W3.1 — see this
/// module's doc comment.
pub enum IndexPart {
    Live(Arc<HnswIndex>),
}

/// The set of index parts a snapshot searches. Cheap to clone (`Arc<[_]>`).
#[derive(Clone)]
pub struct SegmentSet {
    parts: Arc<[IndexPart]>,
}

impl SegmentSet {
    /// Builds a segment set of exactly one live part, wrapping today's
    /// shared mutable index. The only constructor until W3.2 adds a second.
    #[must_use]
    pub fn from_live(index: Arc<HnswIndex>) -> Self {
        Self {
            parts: Arc::from(vec![IndexPart::Live(index)]),
        }
    }

    /// Every call site that needs the sole `Live` part goes through this
    /// one exhaustive `match`, rather than each repeating its own refutable
    /// `let [IndexPart::Live(index)] = ... else { unreachable!() }` slice
    /// pattern. The difference matters once W3.2 adds `IndexPart::Sealed`:
    /// a refutable-pattern `else` arm compiles unchanged (silently becoming
    /// a *runtime* panic on the new variant/second part), whereas the
    /// exhaustive `match` inside here becomes a *compile error* — forcing
    /// every one of this type's methods to be revisited, which is the
    /// entire reason `IndexPart` is an enum rather than a bare
    /// `Arc<HnswIndex>` (design doc §4). Panics (rather than returning a
    /// `Result`) only on a `parts` slice of the wrong length, which no
    /// constructor in this module can produce today.
    fn sole_live(&self) -> &Arc<HnswIndex> {
        let [part] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one part until W3.2")
        };
        match part {
            IndexPart::Live(index) => index,
        }
    }

    /// Mirrors [`HnswIndex::search`] — delegates to the single `Live` part
    /// via the `NodeSource`-generic traversal (Task 2/3 of this plan), by
    /// calling `k_nn_search_generic` directly rather than going through
    /// `HnswIndex::search`, so this genuinely proves the refactor's
    /// equivalence rather than routing around it.
    ///
    /// # Errors
    ///
    /// Same as [`HnswIndex::search`].
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let index = self.sole_live();
        let raw = k_nn_search_generic(
            &index.graph,
            &crate::distance::L2,
            query,
            k,
            ef_search,
            is_visible,
        )?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// Mirrors [`HnswIndex::search_filtered`] — builds the same
    /// live-ids/visibility predicate via `build_live_filter` (shared with
    /// `HnswIndex::search_filtered` so both call sites compose the filter
    /// identically) and passes it straight to `k_nn_search_generic`
    /// directly, rather than delegating to `HnswIndex::search_filtered`
    /// itself — see [`Self::search`]'s doc comment for why that distinction
    /// matters for what this module's equivalence tests actually prove.
    ///
    /// # Errors
    ///
    /// Same as [`HnswIndex::search_filtered`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let index = self.sole_live();
        let filter = build_live_filter(live_ids, is_visible);
        let raw = k_nn_search_generic(
            &index.graph,
            &crate::distance::L2,
            query,
            k,
            ef_search,
            filter,
        )?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// The established vector dimension of this set's parts. Used by
    /// `crates/txn` in place of `HnswIndex::established_dimension` now that
    /// `Snapshot` no longer holds a bare `Arc<HnswIndex>`.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.sole_live().established_dimension()
    }

    /// Recovers the underlying live index, for callers that still need a bare
    /// `Arc<HnswIndex>` — today, `Dataset::begin()` seeds a new `Transaction`'s
    /// own `graph: Arc<HnswIndex>` field from the current snapshot's index this
    /// way. Will need to change once a `SegmentSet` can hold more than one live
    /// part or any `Sealed` part (not yet possible — see this file's module doc).
    #[must_use]
    pub fn live_arc(&self) -> Arc<HnswIndex> {
        Arc::clone(self.sole_live())
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
mod tests {
    use super::*;
    use crate::{EfConstruction, MaxConnections, MaxElements, MaxLayers};

    // Quasi-random (non-collinear) offsets -- same rationale as
    // `hnsw.rs`'s own `insert_cluster` doc comment: collinear points let
    // the build-time diversity heuristic prune down to a near-complete
    // graph, which makes layer-0 search exact for *any* `ef_search`,
    // masking a `k`/`ef_search` argument-order bug (see the doc comment
    // on `build_index` below).
    const PHI: f64 = 0.618_033_988_749_895;
    const SQRT2: f64 = 0.414_213_562_373_095;
    const SQRT3: f64 = 0.732_050_807_568_877;

    /// `MaxConnections(2)` (`mmax0 = 4`) with a generously large `n` keeps
    /// layer 0 genuinely sparse (unlike an earlier version of this fixture,
    /// `MaxConnections(16)`/`n = 20`, where `mmax0 = 32 > n - 1 = 19` made
    /// every node reachable from any entry point in a single hop --
    /// layer-0 search was then mathematically exact regardless of
    /// `ef_search`, so the two equivalence tests below could not have
    /// caught a `k`/`ef_search` argument swap in the code under test: for
    /// an exact search, the final result is always the true top-
    /// `min(k, ef_search, n)`, which is symmetric in `(k, ef_search)`. With
    /// a genuinely approximate (sparse) graph, a small `ef_search` can miss
    /// true nearest neighbors that a larger one would find, breaking that
    /// symmetry -- see the two tests' own comments for the specific
    /// `k`/`ef_search` values this was verified against.
    #[allow(clippy::cast_possible_truncation, clippy::many_single_char_names)]
    fn build_index(n: usize) -> Arc<HnswIndex> {
        let index = HnswIndex::new(
            MaxConnections(2),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(5),
        )
        .unwrap();
        for i in 0..n as u64 {
            let f = i as f64;
            let x = ((f * PHI).fract() * 1000.0) as f32;
            let y = ((f * SQRT2).fract() * 1000.0) as f32;
            let z = ((f * SQRT3).fract() * 1000.0) as f32;
            index.insert_owned(i, vec![x, y, z]).unwrap();
        }
        Arc::new(index)
    }

    #[test]
    fn search_over_one_live_part_matches_hnsw_index_search_directly() {
        let index = build_index(500);
        let set = SegmentSet::from_live(Arc::clone(&index));
        let query = [500.0, 500.0, 500.0];

        // k=40, ef_search=5 verified (empirically, against this exact
        // fixture) to make a `k`/`ef_search` argument swap in the code
        // under test observable: calling `k_nn_search_generic` with these
        // two arguments swapped returns a different row-id set than
        // calling it in the correct order, because ef_search=5 caps the
        // beam width low enough that this sparse graph's search is
        // genuinely approximate. `direct`/`via_set` below are both called
        // with the SAME (correct) argument order -- this doesn't test a
        // swap directly, it only confirms the two wrappers still agree
        // with each other using values that would have caught it if either
        // one got the order wrong.
        let direct = index.search(&query, 40, 5, |_| true).unwrap();
        let via_set = set.search(&query, 40, 5, |_| true).unwrap();

        assert_eq!(
            via_set.len(),
            direct.len(),
            "SegmentSet::search must return the same number of matches as HnswIndex::search"
        );
        for (a, b) in via_set.iter().zip(direct.iter()) {
            assert_eq!(a.row_id, b.row_id, "row-id order must match exactly");
            assert!(
                (a.squared_distance - b.squared_distance).abs() < f32::EPSILON,
                "distances must match exactly: {} vs {}",
                a.squared_distance,
                b.squared_distance
            );
        }
    }

    #[test]
    fn search_filtered_over_one_live_part_matches_hnsw_index_search_filtered_directly() {
        let index = build_index(500);
        let set = SegmentSet::from_live(Arc::clone(&index));
        let query = [500.0, 500.0, 500.0];
        let live_ids: Vec<usize> = (0..500).step_by(2).collect(); // only even row-ids

        // Same k=40/ef_search=5 choice, re-verified with a `live_ids`
        // filter engaged (see `search_over_one_live_part_...`'s comment) --
        // filtering changes which candidates survive into the result set,
        // so the swap-detecting property was checked again under filtering
        // rather than assumed to carry over unchanged.
        let direct = index
            .search_filtered(&query, 40, 5, &live_ids, |_| true)
            .unwrap();
        let via_set = set
            .search_filtered(&query, 40, 5, &live_ids, |_| true)
            .unwrap();

        assert_eq!(via_set.len(), direct.len());
        for (a, b) in via_set.iter().zip(direct.iter()) {
            assert_eq!(a.row_id, b.row_id);
            assert!(
                (a.squared_distance - b.squared_distance).abs() < f32::EPSILON,
                "distances must match exactly: {} vs {}",
                a.squared_distance,
                b.squared_distance
            );
        }
        assert!(
            via_set.iter().all(|m| m.row_id % 2 == 0),
            "only even row-ids were in live_ids: {via_set:?}"
        );
    }

    #[test]
    fn established_dimension_matches_the_underlying_index() {
        let index = build_index(3);
        let set = SegmentSet::from_live(Arc::clone(&index));
        assert_eq!(set.established_dimension(), index.established_dimension());
        assert_eq!(set.established_dimension(), 3);
    }
}
