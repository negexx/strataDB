//! Distance metrics, generic over the graph type — see design doc §4.
//! Backed by `anndists`, the same SIMD-accelerated distance crate
//! `hnsw_rs` itself already uses internally.

use anndists::dist::{
    DistCosine, DistDot, DistL2 as AnnDistsL2, Distance as AnnDistance, l2_normalize,
};

/// A distance metric usable by `Graph<D>`. `eval` must return a value
/// where smaller means "more similar" — callers needing true similarity
/// (e.g. cosine similarity, where larger is more similar) should return
/// its negation or complement, matching `anndists`' own convention.
pub(crate) trait Distance: Send + Sync {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32;
}

/// Euclidean (L2) distance — Strata's existing default, matching today's
/// `hnsw_rs`-backed `DistL2` usage exactly.
pub(crate) struct L2;

impl Distance for L2 {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        AnnDistsL2.eval(a, b)
    }
}

/// Cosine distance (`1 - cosine_similarity`) — smaller means more similar,
/// matching this trait's convention. Verified against the installed
/// `anndists-0.1.5` source (`impl Distance<f32> for DistCosine` in
/// `dist/distances.rs`): it computes true cosine similarity from the raw
/// vectors (via their own norms), so no pre-normalization or extra
/// transform is needed here — unlike `DistDot` below.
pub(crate) struct Cosine;

impl Distance for Cosine {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        DistCosine.eval(a, b)
    }
}

/// Dot-product-based distance, for pre-normalized (unit) embedding
/// vectors — smaller means more similar, matching this trait's convention.
///
/// The brief this was implemented from assumed `anndists::DistDot::eval`
/// returns a raw dot product that this wrapper would need to negate.
/// Verified against the installed `anndists-0.1.5` source
/// (`dist/distances.rs`), that assumption doesn't hold on two counts:
///
/// 1. `DistDot::eval`'s `f32` impl (`scalar_dot_f32`, used by default —
///    `simdeez_f`/`stdsimd` are off by default and not enabled anywhere in
///    this workspace) already returns `1.0 - dot_product`, i.e. it is
///    *already* a smaller-is-closer distance, not a raw similarity score.
///    Negating it (as the brief assumed) would invert the convention this
///    trait promises: an aligned vector would come out *farther* than an
///    orthogonal one, backwards from what `Graph<D>` needs.
/// 2. `scalar_dot_f32` asserts `1.0 - dot_product >= 0.0`, i.e. it assumes
///    the inputs are already unit-normalized (documented in `DistDot`'s
///    own doc comment: "we suppose all vectors ... have been l2 normalized
///    to unity BEFORE INSERTING"). Confirmed empirically: calling it on
///    arbitrary (non-unit) vectors, e.g. `[1.0, 0.0]` vs. `[2.0, 0.0]`,
///    panics with `assertion failed: dot >= 0.` rather than returning a
///    value.
///
/// To keep this metric usable for arbitrary input vectors (not just ones
/// the caller has remembered to pre-normalize) without panicking, `eval`
/// L2-normalizes both operands via `anndists::dist::l2_normalize` before
/// delegating to `DistDot::eval`, and returns its result unmodified (no
/// negation) since it's already smaller-is-closer.
pub(crate) struct Dot;

impl Distance for Dot {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        let mut a = a.to_vec();
        let mut b = b.to_vec();
        l2_normalize(&mut a);
        l2_normalize(&mut b);
        DistDot.eval(&a, &b)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn l2_distance_of_identical_vectors_is_zero() {
        assert_eq!(L2.eval(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn l2_distance_matches_hand_computed_value() {
        // A 3-4-5 triangle: sqrt(3^2 + 4^2) = 5.
        let d = L2.eval(&[0.0, 0.0], &[3.0, 4.0]);
        assert!((d - 5.0).abs() < 1e-5, "expected 5.0, got {d}");
    }

    #[test]
    fn cosine_distance_of_identical_direction_vectors_is_zero() {
        let d = Cosine.eval(&[1.0, 0.0], &[2.0, 0.0]);
        assert!(
            d.abs() < 1e-5,
            "same-direction vectors must have ~0 cosine distance, got {d}"
        );
    }

    #[test]
    fn cosine_distance_of_orthogonal_vectors_is_one() {
        let d = Cosine.eval(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(
            (d - 1.0).abs() < 1e-5,
            "orthogonal vectors must have cosine distance 1.0, got {d}"
        );
    }

    #[test]
    fn dot_orders_a_more_aligned_vector_as_closer() {
        let query = [1.0, 0.0];
        let aligned = Dot.eval(&query, &[2.0, 0.0]);
        let orthogonal = Dot.eval(&query, &[0.0, 2.0]);
        assert!(
            aligned < orthogonal,
            "a more-aligned vector must be reported as closer (smaller): {aligned} vs {orthogonal}"
        );
    }
}
