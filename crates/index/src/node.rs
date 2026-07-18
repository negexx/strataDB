//! A graph node: its vector, one `SlotArray` per layer it participates in
//! (packed into a single `Vec`, not one allocation per layer — see design
//! doc §2/§4), and its deleted flag. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, Ordering};

use crate::slot_array::SlotArray;

pub(crate) struct Node {
    row_id: u64,
    vector: Vec<f32>,
    /// One `SlotArray` per layer `0..=level`: index 0 has capacity
    /// `mmax0`, every other index has capacity `mmax`.
    layers: Vec<SlotArray>,
    deleted: AtomicBool,
}

impl Node {
    pub(crate) fn new(
        row_id: u64,
        vector: Vec<f32>,
        level: usize,
        mmax0: usize,
        mmax: usize,
    ) -> Self {
        let layers = (0..=level)
            .map(|lc| SlotArray::new(if lc == 0 { mmax0 } else { mmax }))
            .collect();
        Self {
            row_id,
            vector,
            layers,
            deleted: AtomicBool::new(false),
        }
    }

    pub(crate) fn row_id(&self) -> u64 {
        self.row_id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        &self.vector
    }

    /// This node's highest layer — it participates in layers `0..=level()`.
    pub(crate) fn level(&self) -> usize {
        self.layers.len() - 1
    }

    /// The `SlotArray` for layer `lc`. Panics if `lc > self.level()` —
    /// callers must never traverse a node at a layer it doesn't
    /// participate in (checked by `Graph`'s traversal logic, not here).
    pub(crate) fn layer(&self, lc: usize) -> &SlotArray {
        &self.layers[lc]
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }
}

/// Random level assignment per the paper: `l = floor(-ln(unif(0,1)) * mL)`.
/// `mL = 1/ln(M)` is "a simple choice for the optimal mL" per the paper —
/// callers pass `1.0 / (m as f64).ln()`.
pub(crate) fn assign_level(m_l: f64, unif: f64) -> usize {
    debug_assert!((0.0..1.0).contains(&unif), "unif must be in [0, 1)");
    // unif == 0.0 would make -ln(unif) infinite; callers must supply a
    // value strictly greater than 0 (e.g. `rand`'s `gen_range(f64::EPSILON..1.0)`).
    (-unif.ln() * m_l).floor() as usize
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn new_node_participates_in_layers_zero_through_level() {
        let node = Node::new(0, vec![1.0, 2.0, 3.0], 2, 32, 16);
        assert_eq!(node.level(), 2);
        assert_eq!(node.layer(0).capacity(), 32, "layer 0 uses mmax0");
        assert_eq!(node.layer(1).capacity(), 16, "layer 1 uses mmax");
        assert_eq!(node.layer(2).capacity(), 16, "layer 2 uses mmax");
    }

    #[test]
    fn level_zero_node_has_exactly_one_layer() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert_eq!(node.level(), 0);
        assert_eq!(node.layer(0).capacity(), 32);
    }

    #[test]
    fn new_node_is_not_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert!(!node.is_deleted());
    }

    #[test]
    fn mark_deleted_is_observed_by_is_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        node.mark_deleted();
        assert!(node.is_deleted());
    }

    #[test]
    fn vector_and_row_id_are_preserved() {
        let node = Node::new(7, vec![1.0, 2.0, 3.0], 0, 32, 16);
        assert_eq!(node.row_id(), 7);
        assert_eq!(node.vector(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn assign_level_is_zero_at_unif_close_to_one() {
        // -ln(x) -> 0 as x -> 1, so floor(-ln(x) * mL) -> 0.
        let level = assign_level(1.0 / (16f64).ln(), 0.999_999);
        assert_eq!(level, 0);
    }

    #[test]
    fn assign_level_grows_as_unif_shrinks_toward_zero() {
        let m_l = 1.0 / (16f64).ln();
        let small_unif_level = assign_level(m_l, 0.000_001);
        let large_unif_level = assign_level(m_l, 0.5);
        assert!(
            small_unif_level > large_unif_level,
            "a smaller unif must produce a level at least as high: {small_unif_level} vs {large_unif_level}"
        );
    }
}
