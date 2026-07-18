//! The lock-free HNSW graph: entry point, `SEARCH-LAYER`,
//! `SELECT-NEIGHBORS-*`, `INSERT`, `K-NN-SEARCH`. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;

use crate::distance::Distance;
use crate::node::{Node, assign_level};
use crate::node_table::NodeTable;

/// Sentinel for "the graph has no nodes yet" — see `EntryPoint::new`.
const NO_ENTRY: u64 = u64::MAX;

/// `(row_id, level)` are packed into the low/high bits of a single
/// `AtomicU64` and updated with ONE compare-exchange, not two separate
/// atomics. This is not stylistic: an earlier version of this design used
/// two separate atomics (`row_id: AtomicU64, level: AtomicUsize`) updated
/// as two sequential operations, and loom found a genuine torn-state race
/// — a thread can win the `row_id` CAS with a lower level, get preempted
/// before its own `level.store`, let a higher-level thread complete its
/// *entire* update, and then blindly overwrite the winner's correct level
/// with its own stale one, producing a `(row_id, level)` pair neither
/// thread ever proposed. Packing both fields into one atomic makes that
/// class of bug structurally impossible: there is only ever one
/// consistent `(row_id, level)` pair in existence at a time, because there
/// is only one atomic word holding it.
///
/// `LEVEL_BITS = 8` (max representable level 255) is enormously generous:
/// per the paper's own formula, expected max level for N nodes is roughly
/// `mL * ln(N)` — for N at `crates/txn`'s own row-id ceiling of
/// 1,000,000,000 and `mL = 1/ln(16) ≈ 0.36`, that's `0.36 * ln(1e9) ≈ 7.5`,
/// vastly under 255 even accounting for statistical outliers.
const LEVEL_BITS: u32 = 8;
const LEVEL_MASK: u64 = (1 << LEVEL_BITS) - 1;

/// Packs `(row_id, level)` into a single `u64`. If `level` exceeds what
/// `LEVEL_BITS` can represent, it is **clamped** to the maximum
/// representable value (`LEVEL_MASK`) rather than silently truncated via
/// the bitmask. A `debug_assert!` alone is not sufficient here: it
/// compiles to a no-op in release builds, and `crate::node::assign_level`'s
/// contract permits `unif == 0.0`, which makes `-unif.ln()` evaluate to
/// `f64::INFINITY` and (via Rust's saturating float-to-int cast)
/// `usize::MAX` — a real, reachable input once a later task wires
/// `assign_level`'s output into `advance_if_higher`, not a hypothetical
/// one. Clamping is a safe degradation: an out-of-range level clamped to
/// the max representable value can never cause memory unsafety and can
/// never produce an incorrect *lower* level than intended, just a
/// possibly-suboptimal (but still valid) entry point. Silently truncating
/// via the bitmask instead could wrap to an arbitrary, even lower, value —
/// exactly the "never silently resolved" failure mode this project's
/// conventions forbid for correctness-relevant state.
fn pack(row_id: u64, level: usize) -> u64 {
    let level = (level as u64).min(LEVEL_MASK);
    (row_id << LEVEL_BITS) | level
}

fn unpack(packed: u64) -> (u64, usize) {
    (packed >> LEVEL_BITS, (packed & LEVEL_MASK) as usize)
}

/// The graph's current top-layer entry point: which node, at which level.
pub(crate) struct EntryPoint {
    packed: AtomicU64,
}

impl EntryPoint {
    pub(crate) fn new() -> Self {
        Self {
            packed: AtomicU64::new(NO_ENTRY),
        }
    }

    /// Returns `Some((row_id, level))`, or `None` if the graph is empty.
    pub(crate) fn get(&self) -> Option<(u64, usize)> {
        let packed = self.packed.load(Ordering::SeqCst);
        if packed == NO_ENTRY {
            return None;
        }
        Some(unpack(packed))
    }

    /// Advances the entry point to `(row_id, level)` if the graph is
    /// currently empty, or if `level` exceeds the current entry point's
    /// level — matching Algorithm 1 step 18-19 ("if l > L, set enter
    /// point for hnsw to q"). A losing race here just means some other
    /// node's insert already advanced (or is concurrently advancing) to
    /// an equal-or-higher level — never retried beyond re-checking against
    /// the fresh value, self-resolving like every other CAS in this
    /// design.
    pub(crate) fn advance_if_higher(&self, row_id: u64, level: usize) {
        let new_packed = pack(row_id, level);
        loop {
            let current = self.packed.load(Ordering::SeqCst);
            if current != NO_ENTRY {
                let (_, current_level) = unpack(current);
                if level <= current_level {
                    return;
                }
            }
            if self
                .packed
                .compare_exchange(current, new_packed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
            // Lost the race — loop and re-check against the fresh value:
            // the winner may or may not have advanced to a level high
            // enough that we no longer need to advance at all.
        }
    }
}

pub(crate) struct Graph<D: Distance> {
    nodes: NodeTable<Node>,
    entry_point: EntryPoint,
    distance: D,
    dimension: AtomicUsize,
    #[cfg(test)]
    next_test_row_id: AtomicU64, // test-only bookkeeping; removed in Task 8's cleanup along with insert_for_test
}

/// A `(row_id, distance)` pair ordered so a `BinaryHeap` behaves as a
/// min-heap by distance (nearest first) when wrapped in `Reverse`, or as a
/// max-heap (farthest first, for evicting the worst candidate from a
/// capped result set) when used directly — see `search_layer`'s two heaps.
#[derive(Clone, Copy, PartialEq)]
struct Candidate {
    row_id: u64,
    dist: f32,
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(CmpOrdering::Equal)
    }
}

impl<D: Distance> Graph<D> {
    pub(crate) fn new(distance: D, expected_capacity: usize) -> Self {
        Self {
            nodes: NodeTable::new(expected_capacity),
            entry_point: EntryPoint::new(),
            distance,
            dimension: AtomicUsize::new(0),
            #[cfg(test)]
            next_test_row_id: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn insert_for_test(&self, row_id: u64, vector: Vec<f32>) {
        let node = Node::new(row_id, vector, 0, 32, 16);
        self.nodes.insert(row_id, node);
        self.entry_point.advance_if_higher(row_id, 0);
    }

    /// Algorithm 2, `SEARCH-LAYER`. Returns up to `ef` `(row_id, distance)`
    /// pairs, nearest-first, found by greedy traversal from `entry` at
    /// layer `lc`. `filter` and the deleted-flag check both gate entry
    /// into the returned result set `W`, never `neighbourhood(c)`
    /// traversal — a node excluded by `filter` (or tombstoned) still
    /// serves as a live waypoint for reaching other nodes, exactly
    /// mirroring `hnsw_rs`'s own `FilterT` behavior (see the original
    /// `crates/index/src/hnsw.rs`'s `search_filtered` doc comment: "both
    /// are applied during `hnsw_rs`'s own traversal... not as a post-filter
    /// on an already-capped top-k"). This is what lets a caller's
    /// `live_ids` membership push all the way into traversal-time
    /// filtering, not just the deleted flag. See design doc §3.
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        lc: usize,
        filter: &impl Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(entry);

        let entry_dist = self.distance_to(query, entry);
        // Min-heap of candidates still to explore (nearest first via `Reverse`).
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        candidates.push(std::cmp::Reverse(Candidate {
            row_id: entry,
            dist: entry_dist,
        }));
        // Max-heap of the best `ef` results found so far (farthest first, for cheap eviction).
        let mut result: BinaryHeap<Candidate> = BinaryHeap::new();
        if let Some(node) = self.nodes.get(entry)
            && !node.is_deleted()
            && filter(entry)
        {
            result.push(Candidate {
                row_id: entry,
                dist: entry_dist,
            });
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            if let Some(furthest) = result.peek()
                && c.dist > furthest.dist
                && result.len() >= ef
            {
                break; // Algorithm 2 line 7-8: all of W is settled.
            }
            let Some(node) = self.nodes.get(c.row_id) else {
                continue;
            };
            // A node's layer-lc slot array only exists for lc <= node.level().
            if lc > node.level() {
                continue;
            }
            for neighbor_id in node.layer(lc).occupied() {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                let neighbor_dist = self.distance_to(query, neighbor_id);
                let should_add = match result.peek() {
                    Some(furthest) => neighbor_dist < furthest.dist || result.len() < ef,
                    None => true,
                };
                if should_add {
                    candidates.push(std::cmp::Reverse(Candidate {
                        row_id: neighbor_id,
                        dist: neighbor_dist,
                    }));
                    if let Some(neighbor_node) = self.nodes.get(neighbor_id)
                        && !neighbor_node.is_deleted()
                        && filter(neighbor_id)
                    {
                        result.push(Candidate {
                            row_id: neighbor_id,
                            dist: neighbor_dist,
                        });
                        if result.len() > ef {
                            result.pop(); // evict the current furthest
                        }
                    }
                }
            }
        }

        let mut out: Vec<(u64, f32)> = result.into_iter().map(|c| (c.row_id, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
        out
    }

    fn distance_to(&self, query: &[f32], row_id: u64) -> f32 {
        self.nodes
            .get(row_id)
            .map_or(f32::INFINITY, |n| self.distance.eval(query, n.vector()))
    }
}

/// Algorithm 3, `SELECT-NEIGHBORS-SIMPLE`: the `m` nearest candidates,
/// nearest-first. `candidates` need not be pre-sorted.
fn select_neighbors_simple(candidates: &[(u64, f32)], m: usize) -> Vec<u64> {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
    sorted.into_iter().take(m).map(|(id, _)| id).collect()
}

/// Algorithm 4, `SELECT-NEIGHBORS-HEURISTIC`, with `extendCandidates` fixed
/// to `false` (the paper's own default — "useful only for extremely
/// clustered data") and `keepPrunedConnections` fixed to `false` (this
/// design always has more true candidates available from `SEARCH-LAYER`
/// than any single call needs, so backfilling from discarded candidates
/// isn't necessary here the way the paper's more general setting
/// anticipates). `pairwise_dist(a, b)` evaluates the same distance metric
/// as `candidates`' own distances, between two candidate row-ids — needed
/// for line 11's diversity check, which compares a candidate against
/// *other candidates*, not just against the query.
fn select_neighbors_heuristic(
    candidates: &[(u64, f32)],
    m: usize,
    pairwise_dist: impl Fn(u64, u64) -> f32,
) -> Vec<u64> {
    let mut working: Vec<(u64, f32)> = candidates.to_vec();
    working.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));

    let mut result: Vec<u64> = Vec::new();
    for (candidate_id, query_dist) in working {
        if result.len() >= m {
            break;
        }
        // Algorithm 4 line 11's diversity check: keep `candidate_id` only
        // if it is NOT dominated — i.e. no already-picked neighbor is
        // closer to this candidate than the candidate itself is to the
        // query. A dominated candidate is redundant with an existing pick
        // (same direction, no new information); a non-dominated one
        // represents a genuinely different direction.
        let dominated = result
            .iter()
            .any(|&picked| pairwise_dist(candidate_id, picked) < query_dist);
        if !dominated {
            result.push(candidate_id);
        }
    }
    result
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn new_entry_point_is_empty() {
        let ep = EntryPoint::new();
        assert_eq!(ep.get(), None);
    }

    #[test]
    fn advance_if_higher_sets_the_first_entry_point() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 2);
        assert_eq!(ep.get(), Some((5, 2)));
    }

    #[test]
    fn advance_if_higher_replaces_a_lower_level() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 1);
        ep.advance_if_higher(9, 3);
        assert_eq!(ep.get(), Some((9, 3)));
    }

    #[test]
    fn advance_if_higher_ignores_an_equal_or_lower_level() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 3);
        ep.advance_if_higher(9, 3);
        ep.advance_if_higher(1, 1);
        assert_eq!(
            ep.get(),
            Some((5, 3)),
            "neither an equal nor a lower level may replace the current entry point"
        );
    }

    #[test]
    fn advance_if_higher_clamps_an_out_of_range_level_instead_of_wrapping() {
        let ep = EntryPoint::new();
        // 1000 exceeds LEVEL_MASK (255). A bitmask truncation (1000 & 0xFF)
        // would silently wrap to 232 — still a plausible-looking level,
        // which is exactly the dangerous case: pack() must clamp to 255
        // instead, never truncate.
        ep.advance_if_higher(7, 1000);
        assert_eq!(
            ep.get(),
            Some((7, 255)),
            "an out-of-range level must clamp to the max representable value (255), \
             not silently wrap via the bitmask"
        );

        // usize::MAX is the real reachable input this guards against (see
        // pack()'s doc comment: assign_level(m_l, 0.0) produces exactly
        // this via a saturating float-to-int cast on f64::INFINITY).
        let ep2 = EntryPoint::new();
        ep2.advance_if_higher(11, usize::MAX);
        assert_eq!(
            ep2.get(),
            Some((11, 255)),
            "usize::MAX must clamp to 255, not truncate to something else"
        );
    }

    #[test]
    fn search_layer_finds_the_true_nearest_neighbor_in_a_small_graph() {
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![10.0, 0.0, 0.0]);
        graph.insert_for_test(2, vec![20.0, 0.0, 0.0]);
        // insert_for_test alone doesn't create edges (see the comment on
        // search_layer_excludes_a_deleted_node_from_results below) — wire a
        // 0 <-> 1 <-> 2 chain so greedy traversal from entry 0 can actually
        // reach rows 1 and 2, exactly like INSERT will once Task 8 exists.
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
            node1.layer(0).claim(2);
        }
        if let Some(node2) = graph.nodes.get(2) {
            node2.layer(0).claim(1);
        }

        let results = graph.search_layer(&[0.5, 0.0, 0.0], 0, 3, 0, &|_| true);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0, "row 0 must be nearest");
        assert!(
            results[0].1 <= results[1].1 && results[1].1 <= results[2].1,
            "results must be nearest-first: {results:?}"
        );
    }

    #[test]
    fn search_layer_excludes_a_deleted_node_from_results() {
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![10.0, 0.0, 0.0]);
        // Manually wire an edge 0 <-> 1 at layer 0, mirroring what INSERT
        // will do once Task 8 exists — insert_for_test alone doesn't
        // create edges.
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
        }
        if let Some(node0) = graph.nodes.get(0) {
            node0.mark_deleted();
        }

        let results = graph.search_layer(&[0.0, 0.0, 0.0], 1, 5, 0, &|_| true);
        assert!(
            results.iter().all(|(id, _)| *id != 0),
            "a deleted node must never appear in results: {results:?}"
        );
        assert!(
            results.iter().any(|(id, _)| *id == 1),
            "the live node must still be found: {results:?}"
        );
    }

    #[test]
    fn search_layer_filter_excludes_a_live_node_from_results_but_not_from_traversal() {
        // The direct test for the new membership-predicate parameter:
        // node 0 fails an external `filter`, but a query routed through 0
        // must still be able to reach node 1 via 0's edge.
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![1000.0, 0.0, 0.0]);
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
        }

        let results = graph.search_layer(&[0.0, 0.0, 0.0], 1, 5, 0, &|id| id != 0);
        assert!(
            results.iter().all(|(id, _)| *id != 0),
            "a filtered-out node must never appear in results: {results:?}"
        );
        assert!(
            results.iter().any(|(id, _)| *id == 1),
            "the filter must not have blocked traversal through node 0 to reach node 1: {results:?}"
        );
    }

    #[test]
    fn select_neighbors_simple_returns_the_m_nearest() {
        let candidates = vec![(1, 5.0), (2, 1.0), (3, 3.0), (4, 2.0)];
        let selected = select_neighbors_simple(&candidates, 2);
        assert_eq!(
            selected,
            vec![2, 4],
            "must return the 2 nearest, in nearest-first order"
        );
    }

    #[test]
    fn select_neighbors_simple_returns_everything_if_m_exceeds_candidate_count() {
        let candidates = vec![(1, 5.0), (2, 1.0)];
        let selected = select_neighbors_simple(&candidates, 5);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn select_neighbors_heuristic_prunes_a_candidate_dominated_by_an_already_picked_neighbor() {
        // Candidate 2: dist-to-query 1.0. Candidate 3: dist-to-query 3.0,
        // but dist(3, 2) = 0.1 — candidate 3 is nearly redundant with
        // already-picked candidate 2, so the heuristic should skip it in
        // favor of a more diverse pick (candidate 4) if one exists.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let pairwise = |a: u64, b: u64| -> f32 {
            match (a, b) {
                (3, 2) | (2, 3) => 0.1, // 3 is nearly redundant with 2
                (4, 2) | (2, 4) => 5.0, // 4 is genuinely distinct from 2
                _ => 0.0,
            }
        };
        let selected = select_neighbors_heuristic(&candidates, 2, pairwise);
        assert_eq!(
            selected,
            vec![2, 4],
            "must prefer the diverse candidate (4) over the redundant one (3), unlike SIMPLE: {selected:?}"
        );
    }

    #[test]
    fn search_layer_traverses_through_an_excluded_node_to_reach_a_node_beyond_it() {
        // The core property this whole design fix exists to guarantee,
        // proven on a THREE-node chain (unlike the two tests above, which
        // only prove a filtered/deleted node is itself discovered — with
        // nothing beyond it, they can't tell a real "traversal skips
        // through it" from a coincidental "it just happens not to matter").
        // Here A (entry, live) -- B (excluded via filter) -- C (live) are
        // chained with NO direct A<->C edge, so C is reachable ONLY by
        // routing through B's own edges. If `filter` ever leaked into the
        // traversal/expansion path (instead of gating only `result`
        // entry), B would never be expanded and C would never be found.
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]); // A: entry, live
        graph.insert_for_test(1, vec![5.0, 0.0, 0.0]); // B: excluded by filter
        graph.insert_for_test(2, vec![10.0, 0.0, 0.0]); // C: live, target
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
            node1.layer(0).claim(2);
        }
        if let Some(node2) = graph.nodes.get(2) {
            node2.layer(0).claim(1);
        }

        let results = graph.search_layer(&[10.0, 0.0, 0.0], 0, 5, 0, &|id| id != 1);
        assert!(
            results.iter().all(|(id, _)| *id != 1),
            "the excluded middle node must never appear in results: {results:?}"
        );
        assert!(
            results.iter().any(|(id, _)| *id == 2),
            "traversal must reach row 2 through row 1's edges despite row 1 \
             being excluded from results — a filtered node must still act \
             as a live waypoint: {results:?}"
        );
    }
}

/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// Mirrors the shape of `crates/txn/src/dataset.rs`'s
    /// `one_writer_store_races_safely_with_many_readers_load` loom test:
    /// multiple threads racing to advance the entry point to different
    /// levels — proves the final state is always the highest level any
    /// thread proposed, and a reader never observes a torn
    /// (row_id, level) pair (i.e. never observes a level that doesn't
    /// belong to the row_id currently stored).
    #[test]
    fn concurrent_advances_always_settle_on_the_highest_level() {
        loom::model(|| {
            let ep = loom::sync::Arc::new(EntryPoint::new());

            let ep1 = loom::sync::Arc::clone(&ep);
            let t1 = loom::thread::spawn(move || ep1.advance_if_higher(1, 1));

            let ep2 = loom::sync::Arc::clone(&ep);
            let t2 = loom::thread::spawn(move || ep2.advance_if_higher(2, 2));

            t1.join().unwrap();
            t2.join().unwrap();

            // Whichever thread's advance ran last among equals could win,
            // but the FINAL level must be 2 (the higher of the two
            // proposals) regardless of interleaving, and it must be
            // row_id 2's — never a torn pairing of row_id 1 with level 2
            // or vice versa.
            assert_eq!(
                ep.get(),
                Some((2, 2)),
                "the entry point must settle on the higher-level proposal, \
                 with row_id and level always paired consistently"
            );
        });
    }
}
