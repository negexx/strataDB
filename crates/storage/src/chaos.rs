//! Deterministic crash injection for Phase 7's correctness harness. Entirely
//! inert unless compiled with the `chaos-injection` feature AND the
//! `STRATA_CHAOS_ABORT_AT` env var is set — zero cost, zero behavior change
//! for the real `strata` binary or any other consumer. See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md` §3.

#[cfg(feature = "chaos-injection")]
mod real {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CHECKPOINT_COUNT: AtomicU64 = AtomicU64::new(0);

    fn abort_at() -> Option<u64> {
        static ABORT_AT: OnceLock<Option<u64>> = OnceLock::new();
        *ABORT_AT.get_or_init(|| {
            std::env::var("STRATA_CHAOS_ABORT_AT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
    }

    /// Call at each durability-boundary point in the commit protocol.
    /// Increments a process-global counter; if `STRATA_CHAOS_ABORT_AT` is
    /// set and this call is exactly the Nth checkpoint since process start,
    /// aborts immediately — no unwinding, no destructors, exactly what a
    /// real crash at this instant would leave on disk.
    pub fn chaos_checkpoint() {
        let n = CHECKPOINT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        if abort_at() == Some(n) {
            std::process::abort();
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn checkpoint_count_increments_each_call() {
            // CHECKPOINT_COUNT is process-global and shared across tests
            // run in the same binary, so assert on the *delta*, not an
            // absolute value.
            let before = CHECKPOINT_COUNT.load(Ordering::SeqCst);
            chaos_checkpoint();
            chaos_checkpoint();
            let after = CHECKPOINT_COUNT.load(Ordering::SeqCst);
            assert_eq!(after - before, 2);
        }

        #[test]
        fn no_abort_when_env_var_unset() {
            // Absence of STRATA_CHAOS_ABORT_AT (the default, and true in
            // this test process) must never abort no matter how many
            // checkpoints pass.
            for _ in 0..50 {
                chaos_checkpoint();
            }
            // Reaching this line at all is the assertion — an abort would
            // have killed the test process.
        }
    }
}

#[cfg(feature = "chaos-injection")]
pub use real::chaos_checkpoint;

/// No-op, inlined away entirely, when the crate isn't built with
/// `chaos-injection` — the real `strata` binary and every other consumer
/// never pays for this at all.
#[cfg(not(feature = "chaos-injection"))]
#[inline(always)]
pub fn chaos_checkpoint() {}
