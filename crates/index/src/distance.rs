//! Distance metrics, generic over the graph type — see design doc §4.
//! Backed by `anndists`, the same SIMD-accelerated distance crate
//! `hnsw_rs` itself already uses internally.

use anndists::dist::{DistCosine, DistL2 as AnnDistsL2, Distance as AnnDistance};

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

/// Negative raw dot product — smaller means more similar, matching this
/// trait's convention (a larger raw dot product means more similar, so
/// this negates it). Deliberately does NOT delegate to `anndists::DistDot`
/// — see below for why — so unlike `Cosine`, this metric is not
/// scale-invariant: it's a distinct metric that also reflects vector
/// magnitude, which is the actual point of offering it as a separate
/// option from `Cosine`.
///
/// The brief this was implemented from assumed `anndists::DistDot::eval`
/// returns a raw dot product that this wrapper would need to negate. An
/// initial implementation instead L2-normalized both operands via
/// `anndists::dist::l2_normalize` and delegated to `DistDot::eval`
/// unmodified — but that reintroduced a panic, just on a different
/// trigger. Verified against the installed `anndists-0.1.5` source
/// (`dist/distances.rs`) and confirmed empirically (a throwaway probe
/// crate run at dim=768 across 200k trials): `DistDot::eval`'s `f32` impl
/// (`scalar_dot_f32`, the default non-SIMD path — `simdeez_f`/`stdsimd`
/// are off by default and not enabled anywhere in this workspace)
/// computes `1.0 - dot_product` and then `assert!(dot >= 0.0)` with *zero*
/// tolerance (unlike `DistCosine`'s analogous internal assert, which has a
/// `-0.00002` margin). Floating-point rounding during L2 normalization
/// routinely pushes the post-normalization dot product of two
/// identical/near-identical vectors marginally above `1.0` — at dim=768
/// this happened in ~45% of trials — which trips that zero-tolerance
/// assert and panics. It also made the metric bit-identical to `Cosine`'s
/// output on the (majority of) inputs that didn't panic, since
/// L2-normalizing before an inner product is exactly what cosine
/// similarity is — i.e. that implementation wasn't a genuinely distinct
/// metric, just a strictly worse, panic-prone `Cosine`.
///
/// This version instead computes the raw dot product directly and negates
/// it — no normalization, no allocation, and no call into any
/// `assert!`-guarded `anndists` internals, so it cannot panic on any
/// finite `f32` input (including exact duplicates, at any dimension).
pub(crate) struct Dot;

impl Distance for Dot {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
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

    /// Regression test for a panic in an earlier implementation of `Dot`
    /// that L2-normalized operands and delegated to
    /// `anndists::DistDot::eval`: at realistic embedding dimensions,
    /// floating-point rounding during normalization routinely pushed the
    /// post-normalization dot product of near-identical vectors marginally
    /// above `1.0`, tripping `anndists`' zero-tolerance internal
    /// `assert!(dot >= 0.0)`. At dim=768 this reproduced in ~45% of trials
    /// against exact duplicates. `Dot::eval` must not call into any
    /// `assert!`-guarded `anndists` internals for this reason — this test
    /// uses a realistic dimension (768, a common embedding size) and exact
    /// duplicate vectors, the case most likely to trigger that failure
    /// mode, and asserts it neither panics nor reports non-negative
    /// "distance" for a vector compared to itself.
    #[test]
    #[allow(clippy::cast_precision_loss)] // DIM=768 is exactly representable in f32.
    fn dot_of_identical_high_dimensional_vectors_does_not_panic() {
        const DIM: usize = 768;
        // Deterministic pseudo-random-looking values, no external RNG
        // dependency needed: varied magnitudes/signs are what stress
        // floating-point summation, not true randomness.
        let v: Vec<f32> = (0..DIM).map(|i| ((i as f32) * 0.017).sin() * 3.0).collect();

        let d = Dot.eval(&v, &v);

        // eval(x, x) = -sum(x_i * x_i) = -||x||^2, which is <= 0 for any
        // real vector, and strictly negative for this non-zero vector.
        assert!(
            d < 0.0,
            "a vector's dot-distance to itself must be negative (self dot product is positive), got {d}"
        );

        // Comparing against a near-duplicate (tiny per-element noise, the
        // other condition the original bug report called out) must also
        // not panic and must still be close to the exact-duplicate value.
        let noisy: Vec<f32> = v.iter().map(|x| x + 1e-6).collect();
        let d_noisy = Dot.eval(&v, &noisy);
        assert!(
            (d_noisy - d).abs() < 1.0,
            "near-duplicate dot-distance should be close to the exact-duplicate value: {d_noisy} vs {d}"
        );
    }
}
