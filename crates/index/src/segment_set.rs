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
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
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
    /// live-ids/visibility predicate via [`build_live_filter`] (shared with
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
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
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
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
        index.established_dimension()
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
mod tests {
    use super::*;
    use crate::{EfConstruction, MaxConnections, MaxElements, MaxLayers};

    fn build_index(n: usize) -> Arc<HnswIndex> {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(100),
        )
        .unwrap();
        for i in 0..n as u64 {
            index.insert_owned(i, vec![i as f32, 0.0, 0.0]).unwrap();
        }
        Arc::new(index)
    }

    #[test]
    fn search_over_one_live_part_matches_hnsw_index_search_directly() {
        let index = build_index(20);
        let set = SegmentSet::from_live(Arc::clone(&index));

        let direct = index.search(&[0.0, 0.0, 0.0], 5, 32, |_| true).unwrap();
        let via_set = set.search(&[0.0, 0.0, 0.0], 5, 32, |_| true).unwrap();

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
        let index = build_index(20);
        let set = SegmentSet::from_live(Arc::clone(&index));
        let live_ids: Vec<usize> = (0..20).step_by(2).collect(); // only even row-ids

        let direct = index
            .search_filtered(&[0.0, 0.0, 0.0], 5, 32, &live_ids, |_| true)
            .unwrap();
        let via_set = set
            .search_filtered(&[0.0, 0.0, 0.0], 5, 32, &live_ids, |_| true)
            .unwrap();

        assert_eq!(via_set.len(), direct.len());
        for (a, b) in via_set.iter().zip(direct.iter()) {
            assert_eq!(a.row_id, b.row_id);
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
