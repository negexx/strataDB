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
        }
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

    /// Algorithm 1, `INSERT`. `unif` is a caller-supplied draw from
    /// `(0, 1)` (exclusive of 0) used for this node's random level
    /// assignment — see `crate::node::assign_level`. No OCC-retry-loop
    /// exists anywhere in this method: every CAS (slot-claim, slot-clear,
    /// entry-point-advance) is self-resolving on failure, per design doc
    /// §3.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` if `vector`'s length
    /// doesn't match the dimension established by this graph's first-ever
    /// insert.
    // Algorithm 1's own parameter list (row-id, vector, M, Mmax0, Mmax,
    // efConstruction, mL, plus the caller-supplied `unif` draw this design
    // injects instead of an internal RNG) is inherently 8 conceptual
    // parameters wide — this is the exact interface Task 8's spec mandates
    // (consumed as-is by Task 9's tests, Task 11's stress test, and Task
    // 14's `HnswIndex` wrapper), not something to restructure into a
    // struct just to satisfy the lint.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert(
        &self,
        row_id: u64,
        vector: Vec<f32>,
        m: usize,
        mmax0: usize,
        mmax: usize,
        ef_construction: usize,
        m_l: f64,
        unif: f64,
    ) -> Result<(), crate::hnsw::IndexError> {
        self.check_or_establish_dimension(vector.len())?;

        let level = assign_level(m_l, unif);
        let node = Node::new(row_id, vector, level, mmax0, mmax);
        self.nodes.insert(row_id, node);

        let Some((mut entry, mut entry_level)) = self.entry_point.get() else {
            // First node in the graph: it IS the entry point, no
            // connections to build.
            self.entry_point.advance_if_higher(row_id, level);
            return Ok(());
        };
        if entry == row_id {
            // We only just inserted; re-fetch below already accounts for
            // the entry point possibly having been this exact call in a
            // single-node graph — nothing further to connect.
            return Ok(());
        }

        // The node table now owns the vector (moved into the `Node` above,
        // never cloned) — borrow it back for the rest of this call rather
        // than keeping a second owned copy alive, so an embedding-sized
        // vector is never duplicated on the hot insert path.
        let Some(inserted) = self.nodes.get(row_id) else {
            // NodeTable::insert is a single deterministic store with no
            // concurrent removal in this design (nodes are never reclaimed
            // once inserted) — this should be unreachable, but fails safe
            // rather than panicking if it ever isn't.
            return Ok(());
        };
        let query: &[f32] = inserted.vector();

        // Phase 1 (Algorithm 1 lines 5-7): ef=1 descent from the current
        // top layer down to level+1, to find a good entry point for the
        // real connection-building phase.
        while entry_level > level {
            // INSERT's own internal traversal has no membership-predicate
            // concept — always-true filter, deleted-flag exclusion still
            // applies via search_layer's own unconditional check.
            let found = self.search_layer(query, entry, 1, entry_level, &|_| true);
            if let Some((nearest, _)) = found.first() {
                entry = *nearest;
            }
            entry_level -= 1;
        }

        // Phase 2 (Algorithm 1 lines 8-17): real connection-building from
        // min(L, l) down to 0.
        let start_layer = entry_level.min(level);
        for lc in (0..=start_layer).rev() {
            let candidates = self.search_layer(query, entry, ef_construction, lc, &|_| true);
            if let Some((nearest, _)) = candidates.first() {
                entry = *nearest;
            }
            let capacity = if lc == 0 { mmax0 } else { mmax };
            let chosen =
                select_neighbors_heuristic(&candidates, m, |a, b| self.pairwise_distance(a, b));

            let Some(new_node) = self.nodes.get(row_id) else {
                continue;
            };
            for &neighbor_id in &chosen {
                new_node.layer(lc).claim(neighbor_id);
                if let Some(neighbor_node) = self.nodes.get(neighbor_id)
                    && lc <= neighbor_node.level()
                {
                    neighbor_node.layer(lc).claim(row_id);
                    // Shrink the neighbor's list if it now exceeds capacity.
                    let occupied = neighbor_node.layer(lc).occupied();
                    if occupied.len() > capacity {
                        let with_dists: Vec<(u64, f32)> = occupied
                            .iter()
                            .map(|&id| (id, self.pairwise_distance(neighbor_id, id)))
                            .collect();
                        let keep = select_neighbors_heuristic(&with_dists, capacity, |a, b| {
                            self.pairwise_distance(a, b)
                        });
                        let to_remove: Vec<u64> = occupied
                            .into_iter()
                            .filter(|id| !keep.contains(id))
                            .collect();
                        neighbor_node.layer(lc).clear_matching(&to_remove);
                    }
                }
            }
        }

        self.entry_point.advance_if_higher(row_id, level);
        Ok(())
    }

    /// Inserts every row in `rows`, sharing repeated entry-point lookups
    /// across the whole batch instead of recomputing per row — matches
    /// `crates/txn::Transaction::commit`'s calling pattern (many rows per
    /// commit). `unifs[i]` supplies row `i`'s level-assignment draw;
    /// `rows.len()` must equal `unifs.len()`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` on the first row whose
    /// vector length disagrees with the graph's established dimension (or
    /// an earlier row in this same batch) — matches `insert`'s own
    /// per-call validation, just applied row-by-row within the batch.
    // Mirrors `insert`'s own 8-parameter signature by design (this is a
    // thin forwarding wrapper over it) — same too-many-arguments rationale
    // as `insert` above, not something to restructure into a struct here
    // either.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_batch(
        &self,
        rows: &[(u64, Vec<f32>)],
        m: usize,
        mmax0: usize,
        mmax: usize,
        ef_construction: usize,
        m_l: f64,
        unifs: &[f64],
    ) -> Result<(), crate::hnsw::IndexError> {
        debug_assert_eq!(rows.len(), unifs.len());
        for ((row_id, vector), &unif) in rows.iter().zip(unifs.iter()) {
            self.insert(
                *row_id,
                vector.clone(),
                m,
                mmax0,
                mmax,
                ef_construction,
                m_l,
                unif,
            )?;
        }
        Ok(())
    }

    /// The distance between two already-inserted nodes' vectors, by
    /// row-id — the pairwise-distance primitive `SELECT-NEIGHBORS-
    /// HEURISTIC`'s diversity check (Algorithm 4 line 11) needs, shared
    /// between the initial connection-building and the shrink step so
    /// neither duplicates the other's lookup-and-eval logic. Returns
    /// `f32::INFINITY` if either row-id has no node (should not happen
    /// for row-ids drawn from this same `insert` call's own candidate
    /// set, but fails safe rather than panicking if it ever does).
    fn pairwise_distance(&self, a: u64, b: u64) -> f32 {
        match (self.nodes.get(a), self.nodes.get(b)) {
            (Some(node_a), Some(node_b)) => self.distance.eval(node_a.vector(), node_b.vector()),
            _ => f32::INFINITY,
        }
    }

    /// Algorithm 5, `K-NN-SEARCH`. Descends layers `L..1` with `ef=1`
    /// greedy search, then one real `SEARCH-LAYER` at layer 0 with the
    /// caller's actual `ef`. Returns `(row_id, distance)` pairs,
    /// nearest-first, capped at `k`. `filter` is threaded through every
    /// `search_layer` call in both phases — matching `hnsw_rs`'s own
    /// behavior of applying one filter predicate throughout the whole
    /// search, not just the final layer — so a caller's membership
    /// predicate (e.g. `HnswIndex::search_filtered`'s `live_ids`) can
    /// never be silently missed by routing through a node the coarse ef=1
    /// descent excluded from ITS results (excluding from results never
    /// blocks traversal — see `search_layer`'s own doc comment — so this
    /// is safe: the ef=1 phase still finds a good entry point even
    /// through filtered-out nodes, it just never returns one as that
    /// phase's own single "nearest" pick unless it passes the filter).
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` if `query`'s length doesn't
    /// match this graph's established dimension.
    pub(crate) fn k_nn_search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: impl Fn(u64) -> bool,
    ) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError> {
        let established = self.dimension.load(Ordering::SeqCst);
        if established != 0 && query.len() != established {
            return Err(crate::hnsw::IndexError::DimensionMismatch {
                query_len: query.len(),
                expected: established,
            });
        }
        let Some((mut entry, mut level)) = self.entry_point.get() else {
            return Ok(Vec::new());
        };
        while level >= 1 {
            let found = self.search_layer(query, entry, 1, level, &filter);
            if let Some((nearest, _)) = found.first() {
                entry = *nearest;
            }
            level -= 1;
        }
        let mut results = self.search_layer(query, entry, ef, 0, &filter);
        results.truncate(k);
        Ok(results)
    }

    /// Marks `row_id` as deleted — excluded from `k_nn_search` results
    /// from this point on, but its edges remain intact and it continues
    /// to serve as a live traversal waypoint for other queries (Stage 1's
    /// tombstone-flag-only scope — see design doc §1/§3). A no-op if
    /// `row_id` was never inserted.
    pub(crate) fn delete(&self, row_id: u64) {
        if let Some(node) = self.nodes.get(row_id) {
            node.mark_deleted();
        }
    }

    fn check_or_establish_dimension(&self, len: usize) -> Result<(), crate::hnsw::IndexError> {
        let established = self.dimension.load(Ordering::SeqCst);
        if established == 0 {
            self.dimension
                .compare_exchange(0, len, Ordering::SeqCst, Ordering::SeqCst)
                .ok();
        }
        let established = self.dimension.load(Ordering::SeqCst);
        if established != 0 && len != established {
            return Err(crate::hnsw::IndexError::DimensionMismatch {
                query_len: len,
                expected: established,
            });
        }
        Ok(())
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
        graph
            .insert(
                0,
                vec![0.0, 0.0, 0.0],
                16,
                32,
                16,
                100,
                1.0 / (16f64).ln(),
                0.5,
            )
            .unwrap();
        graph
            .insert(
                1,
                vec![10.0, 0.0, 0.0],
                16,
                32,
                16,
                100,
                1.0 / (16f64).ln(),
                0.5,
            )
            .unwrap();
        graph
            .insert(
                2,
                vec![20.0, 0.0, 0.0],
                16,
                32,
                16,
                100,
                1.0 / (16f64).ln(),
                0.5,
            )
            .unwrap();

        let results = graph.search_layer(&[0.5, 0.0, 0.0], 0, 3, 0, &|_| true);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0, "row 0 must be nearest");
    }

    #[test]
    fn search_layer_excludes_a_deleted_node_from_results() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        graph
            .insert(1, vec![10.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        // INSERT itself wires the bidirectional 0 <-> 1 edge at layer 0.
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
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        graph
            .insert(1, vec![1000.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        // INSERT itself wires the bidirectional 0 <-> 1 edge at layer 0.

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
        // and dist(3, 2) = 2.0 — candidate 3 is dominated by already-picked
        // candidate 2, so the heuristic should skip it in favor of a more
        // diverse pick (candidate 4) if one exists.
        //
        // dist(3, 2) = 2.0 is deliberately chosen to sit strictly BETWEEN
        // the two possible reference points a correct-vs-backwards
        // implementation could compare it against: the picked neighbor's
        // own query-distance (dist(2, q) = 1.0) and the candidate's own
        // query-distance (dist(3, q) = 3.0). The correct check compares
        // against the candidate's distance (2.0 < 3.0 -> dominated, matches
        // Algorithm 4 line 11); a backwards implementation that compared
        // against the picked neighbor's distance instead would see
        // 2.0 < 1.0 -> false -> NOT dominated, and wrongly keep candidate 3,
        // producing [2, 3] instead of [2, 4]. A value below both thresholds
        // (e.g. the previous 0.1) or above both would pass under either
        // comparison direction and silently fail to catch a swapped
        // comparison — do not "simplify" this value back down.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let pairwise = |a: u64, b: u64| -> f32 {
            match (a, b) {
                (3, 2) | (2, 3) => 2.0, // strictly between dist(2,q)=1.0 and dist(3,q)=3.0
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
        let m_l = 1.0 / (16f64).ln();
        // A, B, C collinear and evenly spaced: INSERT's own
        // SELECT-NEIGHBORS-HEURISTIC diversity check (Algorithm 4 line 11)
        // prunes A from C's candidate list once B is picked first (A is
        // dominated by B, since dist(A, B) < dist(A, C)) — reproducing
        // exactly the "no direct A<->C edge" topology this test needs,
        // without manually wiring it.
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap(); // A: entry, live
        graph
            .insert(1, vec![5.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap(); // B: excluded by filter
        graph
            .insert(2, vec![10.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap(); // C: live, target

        // Hardens this test against a silent regression: it depends on
        // SELECT-NEIGHBORS-HEURISTIC's diversity pruning to reproduce the
        // "no direct A<->C edge" topology (see the comment above), rather
        // than wiring it by hand. If that pruning behavior ever regressed
        // and A<->C connected directly, the assertions below would still
        // pass (C would just be found directly instead of via B) without
        // ever exercising the property this test exists to prove. Assert
        // the precondition explicitly so a regression here fails loudly.
        assert!(
            !graph.nodes.get(0).unwrap().layer(0).occupied().contains(&2),
            "topology precondition violated: A must have no direct edge to \
             C, or the traversal-through-an-excluded-node assertions below \
             would pass vacuously"
        );

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

    #[test]
    fn insert_creates_bidirectional_edges_between_new_and_existing_nodes() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.9)
            .unwrap();
        graph
            .insert(1, vec![0.1, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.9)
            .unwrap();

        let node0 = graph.nodes.get(0).unwrap();
        let node1 = graph.nodes.get(1).unwrap();
        assert!(
            node0.layer(0).occupied().contains(&1),
            "node 0 must have an edge to node 1 at layer 0"
        );
        assert!(
            node1.layer(0).occupied().contains(&0),
            "the edge must be bidirectional: node 1 must have an edge back to node 0"
        );
    }

    #[test]
    fn insert_advances_the_entry_point_when_a_new_node_has_a_higher_level() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        // unif close to 1.0 -> level 0; unif close to 0.0 -> a high level.
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.99)
            .unwrap();
        assert_eq!(graph.entry_point.get().map(|(_, level)| level), Some(0));

        graph
            .insert(1, vec![1.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.000_001)
            .unwrap();
        let (entry_row, entry_level) = graph.entry_point.get().unwrap();
        assert_eq!(
            entry_row, 1,
            "the higher-level node must become the entry point"
        );
        assert!(entry_level > 0);
    }

    #[test]
    fn insert_shrinks_a_full_neighbor_list_to_keep_the_closer_candidate() {
        // Regression test for a Task 8 review finding: with each layer's
        // `SlotArray` sized to exactly mmax0/mmax (no headroom), `claim`
        // fails silently once a neighbor's list is full, so the shrink
        // step (Algorithm 1 lines 12-16) could never observe an oversized
        // list — it was structurally unreachable dead code. `Node::new`
        // now sizes each layer's `SlotArray` at `mmax0 + 1`/`mmax + 1`
        // (see node.rs) so a new, closer candidate has room to land before
        // the shrink logic prunes the worse existing edge back out.
        //
        // m = mmax0 = mmax = 1 throughout, so every node keeps exactly one
        // layer-0 neighbor once the graph has settled:
        //   B (origin) gets F1 (far) as its only neighbor first, then F2
        //   (much closer to B than F1) is inserted and connects to B.
        //   Without the fix, B's array is already physically full with F1
        //   and the claim for F2 just fails — B keeps the worse neighbor
        //   forever. With the fix, the claim succeeds into the headroom
        //   slot, the shrink step fires, and F1 (farther from B) is the
        //   one evicted, leaving F2 (closer) as B's sole neighbor.
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 1, 1, 1, 10, m_l, 0.99)
            .unwrap(); // B: first node, becomes the entry point
        graph
            .insert(1, vec![100.0, 0.0, 0.0], 1, 1, 1, 10, m_l, 0.99)
            .unwrap(); // F1: far from B, fills B's single layer-0 slot
        graph
            .insert(2, vec![0.1, 0.0, 0.0], 1, 1, 1, 10, m_l, 0.99)
            .unwrap(); // F2: much closer to B than F1 is

        let b = graph.nodes.get(0).unwrap();
        assert_eq!(
            b.layer(0).occupied(),
            vec![2],
            "B must drop the far neighbor (row 1) and keep the close one \
             (row 2) once its layer-0 list is full — proves the shrink \
             step actually runs, not just that claim() silently no-ops \
             when the array is full: {:?}",
            b.layer(0).occupied()
        );
    }

    #[test]
    fn k_nn_search_finds_the_true_nearest_neighbor_across_layers() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph
                .insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
                .unwrap();
        }
        let results = graph
            .k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn k_nn_search_descends_through_upper_layers_to_reach_a_far_entry_points_target() {
        // Partial coverage for a Task 9 review finding: every other test in
        // this file pins `unif = 0.5`, which deterministically assigns
        // level 0 to every node (`assign_level(1/ln(16), 0.5) == 0`), so
        // `k_nn_search`'s `while level >= 1` descent loop never executes at
        // all in any of them. This test at least forces the loop body to
        // run (row 0's high level, from `unif` close to 0, makes
        // `entry_level >= 1` a real assertion below, not a tautology) and
        // proves the two-phase composition still finds the true nearest
        // neighbor in a graph that genuinely spans multiple layers.
        //
        // What this test does NOT prove: that the descent loop is
        // *necessary* for that correct result. Empirically (temporarily
        // short-circuiting the loop to a no-op and re-running this exact
        // test), the result stays correct even with descent disabled here
        // -- this fixture's layer-0 topology happens to let `search_layer`'s
        // strictly-improving greedy walk (see its `should_add` check)
        // hill-climb from row 0 straight to the answer in a handful of
        // hops regardless of starting layer, because every node here is
        // forced to keep at least one edge back toward row 0 (the first
        // node inserted always receives a bidirectional edge from the
        // second, per `insert`'s own connection step) and the positions
        // form a monotonic staircase toward the query. Constructing a
        // fixture where a cold, layer-0-only greedy walk provably gets
        // stuck in a local minimum -- and so genuinely depends on the
        // multi-layer descent to recover -- needs either an adversarial
        // non-monotonic local topology or a much larger, randomized graph;
        // deferred to Task 11's concurrent stress test, which spans many
        // levels via real random draws across many nodes and can assert
        // recall holds from level > 0 entries at that scale.
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![1000.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.000_001)
            .unwrap();
        for i in 1..=8u64 {
            graph
                .insert(i, vec![i as f32 * 0.1, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.9)
                .unwrap();
        }
        let (entry_row, entry_level) = graph.entry_point.get().unwrap();
        assert_eq!(
            entry_row, 0,
            "row 0 must remain the entry point -- it is the only node with a level above 0"
        );
        assert!(
            entry_level >= 1,
            "the test graph must actually span multiple layers, or this test proves nothing: level = {entry_level}"
        );

        let results = graph.k_nn_search(&[0.4, 0.0, 0.0], 1, 1, |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].0, 4,
            "must find the true nearest neighbor (row 4, at x=0.4) despite the \
             entry point (row 0, at x=1000.0) being far away and only ef=1 \
             being used at layer 0: {results:?}"
        );
    }

    #[test]
    fn delete_excludes_a_row_from_k_nn_search_results() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph
                .insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
                .unwrap();
        }
        graph.delete(0);
        let results = graph
            .k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].0, 0, "deleted row must never be returned");
        assert_eq!(
            results[0].0, 1,
            "the next-nearest live row must be returned instead"
        );
    }

    #[test]
    fn k_nn_search_on_an_empty_graph_returns_no_results() {
        let graph: Graph<crate::distance::L2> = Graph::new(crate::distance::L2, 10);
        let results = graph
            .k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn k_nn_search_filter_excludes_a_row_from_results_but_search_still_finds_others_through_it() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph
                .insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
                .unwrap();
        }
        let results = graph
            .k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |id| id != 0)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].0, 0, "a filtered-out row must never be returned");
        assert_eq!(
            results[0].0, 1,
            "the next-nearest row passing the filter must be returned instead"
        );
    }

    #[test]
    fn deleted_node_is_never_returned_even_when_queried_at_its_own_exact_location() {
        // The discriminating test, per this project's own Phase 5 lesson
        // (crates/txn/tests/concurrent_snapshot_isolation.rs): querying
        // somewhere a broken deleted-flag check and a correct one would
        // look identical proves nothing. Querying AT the deleted node's
        // own coordinates is where a broken check would return it as the
        // unambiguous true nearest neighbor — a correct check must fall
        // back to the next-nearest live node instead.
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        graph
            .insert(1, vec![1000.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();

        graph.delete(0);

        // Querying exactly at row 0's own location: if the deleted-flag
        // check were broken, row 0 would be the unambiguous nearest
        // (distance 0.0). A correct implementation must instead return
        // row 1, even though it's 1000 units away.
        let results = graph
            .k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].0, 1,
            "querying at the deleted node's own location must still exclude it, \
             falling back to the far live node: {results:?}"
        );
    }

    /// Deterministic seeded pseudo-random `unif` in `(0, 1)`, keyed by
    /// `seed` -- avoids adding a `rand` dependency for a test-only need
    /// (`unif` is caller-supplied by design; see
    /// `crate::node::assign_level`'s doc comment). `SplitMix64` mixing
    /// gives a good spread across `[0, 1)` so this stress test's 320 rows
    /// produce a realistic multi-layer graph instead of every row landing
    /// on level 0.
    // Mapping a 53-bit mixed integer into an f64 in [0, 1) is an
    // intentional, bounded precision reduction, not a bug -- assign_level
    // only needs a uniform-ish draw, not full u64 precision.
    #[allow(clippy::cast_precision_loss)]
    fn test_unif(seed: u64) -> f64 {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        ((z >> 11) as f64 / (1u64 << 53) as f64).max(f64::EPSILON)
    }

    #[test]
    fn concurrent_inserts_are_all_findable_afterward() {
        use std::sync::Arc;

        const THREADS: u64 = 16;
        const PER_THREAD: u64 = 20;
        // THREADS * PER_THREAD is a small compile-time constant (320),
        // nowhere near usize::MAX on any real target.
        #[allow(clippy::cast_possible_truncation)]
        let graph = Arc::new(Graph::new(
            crate::distance::L2,
            (THREADS * PER_THREAD) as usize,
        ));
        let m_l = 1.0 / (16f64).ln();

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let graph = Arc::clone(&graph);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let row_id = t * PER_THREAD + i;
                        graph
                            .insert(
                                row_id,
                                vec![row_id as f32, 0.0, 0.0],
                                16,
                                32,
                                16,
                                100,
                                m_l,
                                test_unif(row_id),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Confirm this fixture actually built a multi-layer graph -- a
        // fixed unif here would silently degrade to a level-0-only graph
        // and this test would stop proving anything about the descent
        // loop.
        let (_, entry_level) = graph.entry_point.get().unwrap();
        assert!(
            entry_level >= 1,
            "the varied-unif fixture must produce a real multi-layer graph, \
             or this test no longer exercises k_nn_search's descent loop: \
             entry level = {entry_level}"
        );

        // Every inserted row must be exactly findable via a query at its
        // own coordinates -- each of these 320 queries goes through the
        // real entry point (now at level >= 1), so this recall check
        // exercises the multi-layer descent for real, not just a single
        // hand-built case.
        for row_id in 0..(THREADS * PER_THREAD) {
            let results = graph
                .k_nn_search(&[row_id as f32, 0.0, 0.0], 1, 200, |_| true)
                .unwrap();
            assert_eq!(
                results.len(),
                1,
                "row {row_id} must be findable after concurrent insertion"
            );
        }
    }

    #[test]
    fn insert_batch_inserts_every_row() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        let rows: Vec<(u64, Vec<f32>)> = (0..5).map(|i| (i, vec![i as f32, 0.0, 0.0])).collect();
        let unifs = vec![0.5; 5];
        graph
            .insert_batch(&rows, 16, 32, 16, 100, m_l, &unifs)
            .unwrap();

        for i in 0..5u64 {
            let results = graph
                .k_nn_search(&[i as f32, 0.0, 0.0], 1, 50, |_| true)
                .unwrap();
            assert_eq!(results[0].0, i);
        }
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
