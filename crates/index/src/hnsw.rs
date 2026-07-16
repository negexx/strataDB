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
        let raw = self.hnsw.search(query, k, ef_search);
        Ok(self.to_matches(raw))
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
        // live_ids is caller-supplied and expected sorted (hnsw_rs's
        // impl FilterT for Vec<usize> binary-searches it) — sort defensively
        // rather than trusting every caller got this right.
        let mut live_ids = live_ids.to_vec();
        live_ids.sort_unstable();
        let raw = self
            .hnsw
            .search_filter(query, k, ef_search, Some(&live_ids as &dyn FilterT));
        Ok(self.to_matches(raw))
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
    fn to_matches(&self, raw: Vec<hnsw_rs::prelude::Neighbour>) -> Vec<VectorMatch> {
        raw.into_iter()
            .filter(|n| !self.tombstones.contains(&(n.get_origin_id() as u64)))
            .map(|n| VectorMatch {
                row_id: n.get_origin_id() as u64,
                squared_distance: n.get_distance(),
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_search_finds_the_true_nearest_neighbor() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]);
        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[10.0, 10.0, 10.0]);

        let results = index.search(&[0.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 0);
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(results[0].squared_distance, 0.0);
        }
        assert_eq!(results[1].row_id, 1);
    }

    #[test]
    fn tombstoned_row_is_never_returned_even_as_the_true_nearest_neighbor() {
        let index = {
            let mut index = HnswIndex::new(16, 100, 16, 200).unwrap();
            index.insert(0, &[0.0, 0.0, 0.0]);
            index.insert(1, &[5.0, 5.0, 5.0]);
            index.tombstone(0);
            index
        };

        let results = index.search(&[0.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(
            results.len(),
            1,
            "the tombstoned row must be excluded, not just re-ranked"
        );
        assert_eq!(results[0].row_id, 1);
    }

    #[test]
    fn search_filtered_only_returns_ids_in_the_live_set() {
        let index = HnswIndex::new(16, 100, 16, 200).unwrap();
        index.insert(0, &[0.0, 0.0, 0.0]);
        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[2.0, 0.0, 0.0]);

        // Only row 2 is "live" per the caller's predicate, even though rows
        // 0 and 1 are closer to the query.
        let live_ids = [2usize];
        let results = index
            .search_filtered(&[0.0, 0.0, 0.0], 2, 50, &live_ids)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_id, 2);
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
