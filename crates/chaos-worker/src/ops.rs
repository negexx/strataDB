//! Per-agent operation-verb generation and just-in-time target resolution
//! for the chaos workload. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! §3.1.

use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;

/// One agent's chosen action for a single op slot. Drawn up front per
/// agent (see [`generate_verb_sequence`]) from a fixed weighted
/// distribution — see the design doc §3.1 for why these specific
/// percentages are starting defaults, not load-bearing constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpVerb {
    Insert,
    Delete,
    Update,
    MultiBatchInsert,
}

fn verb_for_fraction(u: f64) -> OpVerb {
    if u < 0.40 {
        OpVerb::Insert
    } else if u < 0.60 {
        OpVerb::Delete
    } else if u < 0.80 {
        OpVerb::Update
    } else {
        OpVerb::MultiBatchInsert
    }
}

/// Draws one [`OpVerb`]: 40% Insert, 20% Delete, 20% Update, 20%
/// `MultiBatchInsert`.
fn draw_verb(rng: &mut ChaCha8Rng) -> OpVerb {
    verb_for_fraction(rng.random())
}

/// Distinct RNG stream from the existing per-op vector generation (itself
/// seeded `seed ^ agent`), so consuming it for verbs doesn't perturb the
/// vector sequence.
const VERB_STREAM: u64 = 0xC0DE_A62B_005E_1234;

/// Generates one agent's full verb sequence up front, independent of
/// scheduling — same seeding discipline as the existing per-op vector
/// generation.
pub(crate) fn generate_verb_sequence(seed: u64, agent: u64, ops_per_agent: u64) -> Vec<OpVerb> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ agent ^ VERB_STREAM);
    (0..ops_per_agent).map(|_| draw_verb(&mut rng)).collect()
}

/// Resolves a Delete/Update op's target row-id at scheduling time: 50% a
/// random row from `pool_rows`, 50% a random row from `own_rows` (this
/// agent's own live prior inserts), falling back to whichever of the two
/// is non-empty, or `None` (the caller must downgrade this op slot to
/// Insert) if both are empty.
pub(crate) fn resolve_target(
    target_rng: &mut ChaCha8Rng,
    pool_rows: &[u64],
    own_rows: &[u64],
) -> Option<u64> {
    let prefer_pool = target_rng.random_bool(0.5);
    let (primary, secondary) = if prefer_pool {
        (pool_rows, own_rows)
    } else {
        (own_rows, pool_rows)
    };
    let source = if !primary.is_empty() {
        primary
    } else if !secondary.is_empty() {
        secondary
    } else {
        return None;
    };
    Some(source[target_rng.random_range(0..source.len())])
}

/// Given the verb drawn for this agent's current op slot and how many
/// slots remain (including the current one — so `slots_remaining >= 1` is
/// always expected; the caller only ever invokes this for an agent it has
/// already filtered to have at least one op left), decides how many slots
/// this op actually consumes and what verb to execute. `MultiBatchInsert`
/// needs 2 slots; if only 1 remains, it downgrades to a plain `Insert`
/// consuming just that 1 slot.
pub(crate) fn resolve_slot_consumption(verb: OpVerb, slots_remaining: u64) -> (OpVerb, u64) {
    debug_assert!(
        slots_remaining >= 1,
        "caller must never invoke this for an exhausted agent"
    );
    match verb {
        OpVerb::MultiBatchInsert if slots_remaining < 2 => (OpVerb::Insert, 1),
        OpVerb::MultiBatchInsert => (OpVerb::MultiBatchInsert, 2),
        other => (other, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_for_fraction_respects_the_documented_boundaries() {
        assert_eq!(verb_for_fraction(0.0), OpVerb::Insert);
        assert_eq!(verb_for_fraction(0.399), OpVerb::Insert);
        assert_eq!(verb_for_fraction(0.4), OpVerb::Delete);
        assert_eq!(verb_for_fraction(0.599), OpVerb::Delete);
        assert_eq!(verb_for_fraction(0.6), OpVerb::Update);
        assert_eq!(verb_for_fraction(0.799), OpVerb::Update);
        assert_eq!(verb_for_fraction(0.8), OpVerb::MultiBatchInsert);
        assert_eq!(verb_for_fraction(0.999), OpVerb::MultiBatchInsert);
    }

    #[test]
    fn generate_verb_sequence_matches_the_documented_distribution_over_many_draws() {
        // Exercises draw_verb/generate_verb_sequence end-to-end through the
        // real RNG, not just the pure verb_for_fraction boundary function
        // above -- if draw_verb's rng.random() call ever changed range (a
        // silent drift verb_for_fraction's own test cannot catch), this is
        // what would notice. Design doc §6 names precisely this failure
        // mode: "a distribution that's too insert-heavy would silently
        // under-exercise the very thing this design exists to add."
        let sequence = generate_verb_sequence(42, 1, 10_000);
        assert_eq!(sequence.len(), 10_000);
        let count = |verb: OpVerb| sequence.iter().filter(|&&v| v == verb).count();
        let insert = count(OpVerb::Insert);
        let delete = count(OpVerb::Delete);
        let update = count(OpVerb::Update);
        let multi = count(OpVerb::MultiBatchInsert);
        assert_eq!(insert + delete + update + multi, 10_000);
        // Generous tolerance (+/- 300 of the expected count, ~7.5% relative)
        // so this isn't flaky, while still catching a distribution that
        // silently drifted to e.g. 50/50.
        assert!(
            (3700..=4300).contains(&insert),
            "insert count {insert} outside expected range"
        );
        assert!(
            (1700..=2300).contains(&delete),
            "delete count {delete} outside expected range"
        );
        assert!(
            (1700..=2300).contains(&update),
            "update count {update} outside expected range"
        );
        assert!(
            (1700..=2300).contains(&multi),
            "multibatch count {multi} outside expected range"
        );
    }

    #[test]
    fn the_same_seed_and_agent_always_produce_the_same_sequence() {
        let a = generate_verb_sequence(42, 1, 20);
        let b = generate_verb_sequence(42, 1, 20);
        assert_eq!(a, b);
        assert_eq!(a.len(), 20, "must produce exactly ops_per_agent verbs");
    }

    #[test]
    fn generate_verb_sequence_has_a_pinned_golden_output_for_seed_42_agent_1() {
        // Pins the actual seeding discipline (seed ^ agent ^ VERB_STREAM,
        // ChaCha8Rng, draw order), not just "calling it twice gives the
        // same answer" (true of any pure function, including a badly
        // seeded one) -- the chaos harness depends on cross-run,
        // cross-version reproducibility (CLAUDE.md's Phase 7 bullet:
        // "seed-reproducible scenarios"), which only a literal golden
        // vector actually tests. Captured by running this exact call once
        // and pasting its real output -- do not hand-derive or guess this
        // value.
        let sequence = generate_verb_sequence(42, 1, 8);
        assert_eq!(
            sequence,
            vec![
                OpVerb::Insert,
                OpVerb::Insert,
                OpVerb::Delete,
                OpVerb::Delete,
                OpVerb::Insert,
                OpVerb::Update,
                OpVerb::Insert,
                OpVerb::Insert,
            ]
        );
    }

    #[test]
    fn different_agents_produce_different_sequences() {
        let a = generate_verb_sequence(42, 1, 20);
        let b = generate_verb_sequence(42, 2, 20);
        assert_ne!(
            a, b,
            "two different agents drawing identical 20-op sequences by chance is \
             astronomically unlikely and would indicate the per-agent XOR isn't \
             actually varying the stream"
        );
    }

    #[test]
    fn resolve_target_returns_none_when_both_sources_are_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        assert_eq!(resolve_target(&mut rng, &[], &[]), None);
    }

    #[test]
    fn resolve_target_falls_back_to_pool_when_own_rows_is_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        for _ in 0..20 {
            let target = resolve_target(&mut rng, &[7, 8, 9], &[]);
            assert!(matches!(target, Some(7..=9)));
        }
    }

    #[test]
    fn resolve_target_falls_back_to_own_rows_when_pool_is_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        for _ in 0..20 {
            let target = resolve_target(&mut rng, &[], &[1, 2, 3]);
            assert!(matches!(target, Some(1..=3)));
        }
    }

    #[test]
    fn resolve_target_draws_from_both_sources_in_roughly_equal_proportion() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let mut pool_count = 0;
        let mut own_count = 0;
        for _ in 0..200 {
            match resolve_target(&mut rng, &[100], &[200]) {
                Some(100) => pool_count += 1,
                Some(200) => own_count += 1,
                other => panic!("unexpected target: {other:?}"),
            }
        }
        assert_eq!(pool_count + own_count, 200);
        // A real balance check on the stated 50/50 policy, not just "both
        // were reachable" -- a 199/1 split would satisfy reachability
        // alone but clearly isn't 50/50. Generous bound (60..140 of 200,
        // i.e. 30%-70%) so this isn't flaky.
        assert!(
            (60..=140).contains(&pool_count),
            "pool_count {pool_count}/200 outside the expected ~50/50 balance"
        );
        assert!(
            (60..=140).contains(&own_count),
            "own_count {own_count}/200 outside the expected ~50/50 balance"
        );
    }

    #[test]
    fn resolve_target_is_deterministic_for_identically_seeded_rngs() {
        let mut rng_a = ChaCha8Rng::seed_from_u64(99);
        let mut rng_b = ChaCha8Rng::seed_from_u64(99);
        for _ in 0..20 {
            let a = resolve_target(&mut rng_a, &[1, 2, 3], &[4, 5, 6]);
            let b = resolve_target(&mut rng_b, &[1, 2, 3], &[4, 5, 6]);
            assert_eq!(
                a, b,
                "identically-seeded RNGs must draw the identical target sequence"
            );
        }
    }

    #[test]
    fn multi_batch_insert_consumes_two_slots_when_available() {
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 2),
            (OpVerb::MultiBatchInsert, 2)
        );
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 5),
            (OpVerb::MultiBatchInsert, 2)
        );
    }

    #[test]
    fn multi_batch_insert_downgrades_to_a_single_insert_on_the_last_slot() {
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 1),
            (OpVerb::Insert, 1)
        );
    }

    #[test]
    fn other_verbs_always_consume_exactly_one_slot() {
        for verb in [OpVerb::Insert, OpVerb::Delete, OpVerb::Update] {
            assert_eq!(resolve_slot_consumption(verb, 5), (verb, 1));
            assert_eq!(resolve_slot_consumption(verb, 1), (verb, 1));
        }
    }
}
