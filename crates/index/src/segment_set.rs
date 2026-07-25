//! The set of index parts a snapshot searches over: an immutable,
//! cheaply-clonable list of sealed on-disk segments, one per vector-carrying
//! commit. A snapshot's segment set is exactly its manifest's
//! `segments` list, which is what makes a published snapshot's index view
//! and its row view the same atomic fact (base design doc §4/§5).
//!
//! [`SegmentSet::search`]/[`SegmentSet::search_filtered`] query **every**
//! part for its own top-`k` at the caller's full per-part `ef_search`, map
//! each part's local ordinals back to global row-ids, merge by ascending
//! distance, dedup by row-id, and truncate to `k`. The over-fetch is
//! deliberate: it is *why* recall rises with segment count (ADR 0008), not
//! an accident to tune away. Zone-map-based pruning of parts that provably
//! cannot match is W4's job; nothing here prunes.
//!
//! Dedup by row-id is a no-op in S1 (each row-id lives in exactly one
//! segment, since there is no compaction yet) and is implemented now so
//! S2's compaction — where a row transiently exists in both a source
//! segment and its compacted output — does not require reopening this
//! merge logic.
//!
//! [`IndexPart::Live`] is a **transient** variant, present only while
//! `crates/txn` is being cut over to the segment write path; it is deleted
//! in the same workstream. See
//! `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §1 for
//! why every method below iterates length-independently and matches
//! exhaustively rather than destructuring a fixed-arity slice.

use std::sync::Arc;

use crate::graph::k_nn_search_generic;
use crate::hnsw::{HnswIndex, IndexError, VectorMatch, build_live_filter};
use crate::segment_reader::SegmentReader;

/// One part of a segment set.
pub enum IndexPart {
    /// The legacy shared, mutable in-memory graph. **Transient** — exists
    /// only until `crates/txn`'s write path is fully cut over to segments,
    /// and is deleted in this same workstream. No code may rely on it.
    Live(Arc<HnswIndex>),
    /// One immutable on-disk segment, loaded once and shared by every
    /// snapshot whose manifest lists it.
    Sealed(Arc<SegmentReader>),
}

/// The set of index parts a snapshot searches. Cheap to clone (`Arc<[_]>`).
#[derive(Clone)]
pub struct SegmentSet {
    parts: Arc<[IndexPart]>,
}

impl SegmentSet {
    /// A set with no parts — a freshly created dataset, or one whose
    /// commits have all been vector-less. Searches to an empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            parts: Arc::from(Vec::new()),
        }
    }

    /// Builds a segment set of exactly one live part, wrapping the legacy
    /// shared mutable index. **Transient** — see [`IndexPart::Live`].
    #[must_use]
    pub fn from_live(index: Arc<HnswIndex>) -> Self {
        Self {
            parts: Arc::from(vec![IndexPart::Live(index)]),
        }
    }

    /// Builds a segment set over already-loaded sealed segments, in
    /// manifest order. `Dataset::open`'s constructor.
    #[must_use]
    pub fn from_segments(parts: Vec<Arc<SegmentReader>>) -> Self {
        Self {
            parts: parts.into_iter().map(IndexPart::Sealed).collect(),
        }
    }

    /// A new set holding this set's parts plus `reader`, in that order.
    /// `self` is untouched — an already-published snapshot must never see
    /// a segment committed after it was taken.
    ///
    /// This clones the parts slice, so it is O(parts) per commit and
    /// O(parts²) across a session. Accepted for S1, which explicitly
    /// tolerates one segment per commit; S2's compaction is what bounds the
    /// part count. Do not "fix" it by deferring or batching segment
    /// publication — that would break the no-silent-buffering invariant.
    #[must_use]
    pub fn with_appended(&self, reader: Arc<SegmentReader>) -> Self {
        let mut parts: Vec<IndexPart> = Vec::with_capacity(self.parts.len() + 1);
        for part in self.parts.iter() {
            match part {
                IndexPart::Live(index) => parts.push(IndexPart::Live(Arc::clone(index))),
                IndexPart::Sealed(sealed) => parts.push(IndexPart::Sealed(Arc::clone(sealed))),
            }
        }
        parts.push(IndexPart::Sealed(reader));
        Self {
            parts: Arc::from(parts),
        }
    }

    /// How many parts this set searches. Exposed so `crates/txn`'s tests
    /// can assert the manifest's segment list and the snapshot's in-memory
    /// view never disagree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Queries every part and merges the results — see this module's doc
    /// comment for the merge contract.
    fn fan_out(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let mut merged: Vec<(u64, f32)> = Vec::new();
        for part in self.parts.iter() {
            match part {
                IndexPart::Live(index) => {
                    // A live graph's local id IS its row-id, so no mapping.
                    let raw = k_nn_search_generic(
                        &index.graph,
                        &crate::distance::L2,
                        query,
                        k,
                        ef_search,
                        filter,
                    )?;
                    merged.extend(raw);
                }
                IndexPart::Sealed(reader) => {
                    let raw = k_nn_search_generic(
                        reader.as_ref(),
                        &crate::distance::L2,
                        query,
                        k,
                        ef_search,
                        filter,
                    )?;
                    // A segment's local id is its ordinal within THIS
                    // segment. Returning it unmapped would hand the caller
                    // a plausible-looking wrong row-id -- see this task's
                    // `search_returns_global_row_ids_not_segment_local_ordinals`.
                    merged.extend(
                        raw.into_iter()
                            .filter_map(|(local, dist)| Some((reader.row_id_at(local)?, dist))),
                    );
                }
            }
        }
        merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        // Nearest-first order means the retained occurrence of a duplicated
        // row-id is always its nearest one.
        let mut seen = std::collections::HashSet::with_capacity(merged.len());
        merged.retain(|&(row_id, _)| seen.insert(row_id));
        merged.truncate(k);
        Ok(merged
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// Approximate nearest-neighbor search across every part, gated by
    /// `is_visible` during traversal (never as a post-filter over an
    /// already-capped top-k).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `query`'s length
    /// doesn't match a part's established dimension.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.fan_out(query, k, ef_search, &is_visible)
    }

    /// As [`Self::search`], additionally restricted to `live_ids`.
    /// `live_ids` membership and `is_visible` are composed into ONE
    /// predicate by [`build_live_filter`] — built **once here** and shared
    /// by every part, never rebuilt per part.
    ///
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
        let filter = build_live_filter(live_ids, is_visible);
        self.fan_out(query, k, ef_search, &filter)
    }

    /// The vector dimension this set's parts were built at, or `0` if the
    /// set is empty (no vector has ever been committed). `crates/txn` uses
    /// this to pre-validate a commit's vector dimensions before building
    /// anything — see
    /// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §2.
    ///
    /// Every part necessarily agrees (that pre-validation is what enforces
    /// it), so the first non-empty part's dimension is the set's.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.parts
            .iter()
            .map(|part| match part {
                IndexPart::Live(index) => index.established_dimension(),
                IndexPart::Sealed(reader) => reader.dimension(),
            })
            .find(|&dim| dim != 0)
            .unwrap_or(0)
    }

    /// Recovers the underlying live index. **Transient** — see
    /// [`IndexPart::Live`]; deleted with that variant.
    ///
    /// # Panics
    ///
    /// Panics if this set does not hold exactly one `Live` part, which no
    /// caller can produce once `crates/txn` is cut over.
    #[must_use]
    pub fn live_arc(&self) -> Arc<HnswIndex> {
        for part in self.parts.iter() {
            if let IndexPart::Live(index) = part {
                return Arc::clone(index);
            }
        }
        unreachable!("live_arc called on a set with no Live part")
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

    /// Builds one sealed segment over `n` quasi-random 3-d points whose
    /// global row-ids start at `row_id_base` — the exact shape
    /// `crates/txn`'s per-commit builder produces: a fresh index keyed by
    /// segment-local ordinals `0..n`, serialized, and loaded back.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn build_sealed(n: usize, row_id_base: u64, offset: f32) -> Arc<crate::SegmentReader> {
        let index = HnswIndex::new(
            MaxConnections(4),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(20),
        )
        .unwrap();
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        offset + ((f * PHI).fract() * 100.0) as f32,
                        offset + ((f * SQRT2).fract() * 100.0) as f32,
                        offset + ((f * SQRT3).fract() * 100.0) as f32,
                    ],
                )
                .unwrap();
        }
        let row_ids: Vec<u64> = (row_id_base..row_id_base + n as u64).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();
        Arc::new(crate::SegmentReader::from_bytes(&bytes).unwrap())
    }

    #[test]
    fn an_empty_segment_set_searches_to_no_results_instead_of_erroring() {
        // A freshly created dataset has no segments at all. This must be a
        // clean empty result, not an error and not a panic.
        let set = SegmentSet::empty();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(
            set.search(&[0.0, 0.0, 0.0], 5, 32, |_| true)
                .unwrap()
                .is_empty()
        );
        assert!(
            set.search_filtered(&[0.0, 0.0, 0.0], 5, 32, &[0, 1, 2], |_| true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(set.established_dimension(), 0);
    }

    #[test]
    fn search_returns_global_row_ids_not_segment_local_ordinals() {
        // The single most dangerous bug this layer can have: every sealed
        // part is keyed 0..n, so a missing row_id_at() map returns
        // ordinals that look exactly like plausible row-ids. Row-id base
        // 1_000_000 makes the two impossible to confuse.
        let set = SegmentSet::from_segments(vec![build_sealed(30, 1_000_000, 0.0)]);
        let hits = set.search(&[50.0, 50.0, 50.0], 5, 32, |_| true).unwrap();
        assert!(!hits.is_empty());
        assert!(
            hits.iter().all(|m| m.row_id >= 1_000_000),
            "results must be global row-ids, not local ordinals: {hits:?}"
        );
    }

    #[test]
    fn search_fans_out_across_every_part_and_finds_rows_in_all_of_them() {
        // The recall property this plan's Scope decision exists to
        // protect: two segments, two well-separated clusters, and a query
        // near each one must find that cluster's own rows. A
        // consult-one-part implementation passes for one query and fails
        // for the other.
        let near = build_sealed(30, 0, 0.0); // row-ids 0..30 around the origin
        let far = build_sealed(30, 500, 10_000.0); // row-ids 500..530, far away
        let set = SegmentSet::from_segments(vec![near, far]);
        assert_eq!(set.len(), 2);

        let near_hits = set.search(&[50.0, 50.0, 50.0], 3, 32, |_| true).unwrap();
        assert_eq!(near_hits.len(), 3, "{near_hits:?}");
        assert!(
            near_hits.iter().all(|m| m.row_id < 30),
            "a query near segment 0 must return segment 0's rows: {near_hits:?}"
        );

        let far_hits = set
            .search(&[10_050.0, 10_050.0, 10_050.0], 3, 32, |_| true)
            .unwrap();
        assert_eq!(far_hits.len(), 3, "{far_hits:?}");
        assert!(
            far_hits.iter().all(|m| (500..530).contains(&m.row_id)),
            "a query near segment 1 must return segment 1's rows -- this is the \
             assertion a consult-one-part implementation fails: {far_hits:?}"
        );
    }

    #[test]
    fn merged_results_are_ordered_by_ascending_distance_and_capped_at_k() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
            build_sealed(30, 900, 50_000.0),
        ]);
        let hits = set.search(&[50.0, 50.0, 50.0], 4, 32, |_| true).unwrap();
        assert_eq!(hits.len(), 4, "k must cap the merged set, not each part");
        for pair in hits.windows(2) {
            assert!(
                pair[0].squared_distance <= pair[1].squared_distance,
                "merged results must be nearest-first across parts: {hits:?}"
            );
        }
    }

    #[test]
    fn a_visibility_predicate_is_applied_uniformly_across_every_part() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
        ]);
        // Hide every row-id below 500 -- i.e. the whole first segment.
        let hits = set
            .search(&[50.0, 50.0, 50.0], 5, 32, |id| id >= 500)
            .unwrap();
        assert!(
            !hits.is_empty(),
            "the second segment's rows are still visible"
        );
        assert!(
            hits.iter().all(|m| m.row_id >= 500),
            "the predicate must gate every part, not only the first: {hits:?}"
        );
    }

    #[test]
    fn search_filtered_applies_its_live_id_set_across_every_part() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
        ]);
        // Only even row-ids from the far segment are live.
        let live_ids: Vec<usize> = (500..530).step_by(2).collect();
        let hits = set
            .search_filtered(&[50.0, 50.0, 50.0], 5, 32, &live_ids, |_| true)
            .unwrap();
        assert!(!hits.is_empty());
        assert!(
            hits.iter().all(|m| m.row_id >= 500 && m.row_id % 2 == 0),
            "only live ids may come back, from any part: {hits:?}"
        );
    }

    #[test]
    fn with_appended_leaves_the_original_set_untouched() {
        // Snapshots are immutable and share their parts; publishing a new
        // segment must never mutate an already-published snapshot's view.
        let base = SegmentSet::from_segments(vec![build_sealed(10, 0, 0.0)]);
        let grown = base.with_appended(build_sealed(10, 500, 10_000.0));

        assert_eq!(base.len(), 1, "the original set must not have grown");
        assert_eq!(grown.len(), 2);

        let from_base = base
            .search(&[10_050.0, 10_050.0, 10_050.0], 1, 32, |_| true)
            .unwrap();
        assert!(
            from_base.iter().all(|m| m.row_id < 10),
            "the pre-append set must not see the appended segment: {from_base:?}"
        );
        let from_grown = grown
            .search(&[10_050.0, 10_050.0, 10_050.0], 1, 32, |_| true)
            .unwrap();
        assert_eq!(from_grown.first().map(|m| m.row_id >= 500), Some(true));
    }

    #[test]
    fn established_dimension_reads_the_first_non_empty_part() {
        let set = SegmentSet::from_segments(vec![build_sealed(5, 0, 0.0)]);
        assert_eq!(set.established_dimension(), 3);
        assert_eq!(SegmentSet::empty().established_dimension(), 0);
    }

    #[test]
    fn duplicate_row_id_across_two_parts_is_deduped_keeping_the_nearer_occurrence() {
        // Deliberately give both segments the SAME row-id range (0..10) so
        // every row-id exists in both parts at once. S1 has no compaction
        // yet, so this exact overlap cannot occur naturally today -- it is
        // constructed here on purpose, because S2's compaction (a row
        // transiently existing in both a source segment and its compacted
        // output) is exactly this shape, and the merge logic must already
        // be correct for it.
        let near = build_sealed(10, 0, 0.0); // row-ids 0..10, clustered near the origin
        let far = build_sealed(10, 0, 10_000.0); // SAME row-ids 0..10, clustered far away
        let set = SegmentSet::from_segments(vec![near, far]);

        // k=15 is deliberately larger than the 10 *unique* row-ids that
        // exist: only 10 rows exist in total, so the merged result can
        // never legitimately have more than 10 entries. A broken
        // implementation that skips dedup would return up to 15 raw
        // (row-id, distance) pairs -- the near occurrences of every row-id
        // plus the 5 nearest far occurrences -- reintroducing row-ids
        // already present from the near part. k=10 would truncate away
        // every far entry regardless of dedup (since all 10 near entries
        // are already nearer than any far one), which would make this test
        // pass vacuously even with dedup removed; k=15 is what forces a
        // duplicate to actually survive truncation if dedup is missing.
        let hits = set.search(&[50.0, 50.0, 50.0], 15, 32, |_| true).unwrap();

        assert_eq!(
            hits.len(),
            10,
            "only 10 unique row-ids exist across both parts; a result longer \
             than that means a duplicate survived dedup: {hits:?}"
        );

        // Every duplicated row-id must appear exactly once in the merged
        // result -- not once per part.
        let mut seen = std::collections::HashSet::new();
        for m in &hits {
            assert!(
                seen.insert(m.row_id),
                "row-id {} appeared more than once in the merged result: {hits:?}",
                m.row_id
            );
        }

        // And the surviving occurrence must be the NEAR one: a squared
        // distance computed against the far segment's ~10_000-offset
        // cluster would be on the order of 10_000^2 * 3, many orders of
        // magnitude larger than anything the near cluster can produce for
        // a query at [50, 50, 50].
        for m in &hits {
            assert!(
                m.squared_distance < 1_000_000.0,
                "row-id {} kept the far occurrence instead of the near one -- \
                 dedup did not retain the nearest duplicate: {m:?}",
                m.row_id
            );
        }
    }
}
