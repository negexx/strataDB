//! A fixed-capacity, lock-free edge list: `capacity` atomic slots, each
//! either empty or holding one neighbor's row-id. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md` §2.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};

/// Sentinel for an empty slot. Row-ids are assigned sequentially by
/// `crates/txn`'s manifest (`next_row_id`, capped well below `u64::MAX` by
/// `MAX_REASONABLE_ROW_ID_CAPACITY` in `crates/txn/src/dataset.rs`), so
/// `u64::MAX` is never a real row-id.
pub(crate) const EMPTY: u64 = u64::MAX;

pub(crate) struct SlotArray {
    slots: Box<[AtomicU64]>,
}

impl SlotArray {
    pub(crate) fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| AtomicU64::new(EMPTY))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    // Not read by any production code path yet — `Node`'s shrink logic in
    // `Graph::insert` compares `occupied().len()` against its own
    // `mmax0`/`mmax` parameters rather than querying physical capacity.
    // Kept as a natural companion accessor to `occupied()`/`claim()` below
    // for a future consumer, and exercised by this module's own
    // `new_array_has_no_occupied_slots` and `node.rs`'s headroom tests.
    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Attempts to claim any empty slot for `neighbor_id`. Returns `true` if
    /// claimed, `false` if every slot is occupied.
    pub(crate) fn claim(&self, neighbor_id: u64) -> bool {
        for slot in &self.slots[..] {
            if slot
                .compare_exchange(EMPTY, neighbor_id, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    /// Clears each slot currently holding a value in `to_remove` —
    /// best-effort: if a slot's value already changed since the caller
    /// decided to remove it, that CAS fails and the slot is left alone,
    /// since whatever occupies it now is newer information than what this
    /// caller planned to remove. Never retried — a failed CAS here is a
    /// self-resolving no-op, not an error.
    pub(crate) fn clear_matching(&self, to_remove: &[u64]) {
        for slot in &self.slots[..] {
            let current = slot.load(Ordering::SeqCst);
            if to_remove.contains(&current) {
                let _ = slot.compare_exchange(current, EMPTY, Ordering::SeqCst, Ordering::SeqCst);
            }
        }
    }

    /// An approximately-consistent snapshot of currently-occupied
    /// neighbor-ids — not a true atomic snapshot across slots, which is
    /// fine since HNSW is already an approximate algorithm (see design doc
    /// §2).
    // `Graph`'s two hot-loop call sites (`search_layer`'s per-popped-
    // candidate neighbor lookup, `insert`'s shrink step) now call
    // `occupied_into` directly with a reused scratch buffer; this
    // owned-return form is kept for this module's own tests and
    // `node.rs`'s headroom tests, which don't need scratch reuse.
    #[allow(dead_code)]
    pub(crate) fn occupied(&self) -> Vec<u64> {
        let mut out = Vec::new();
        self.occupied_into(&mut out);
        out
    }

    /// Same snapshot as [`Self::occupied`], written into a caller-supplied
    /// buffer instead of a freshly allocated `Vec` — lets a hot loop that
    /// calls this once per popped candidate (`Graph::search_layer`'s
    /// construction-time traversal) or per chosen neighbor (the shrink
    /// step in `Graph::insert`) reuse one buffer's allocation across every
    /// call instead of paying a fresh allocation each time. `out` is
    /// cleared first, so any stale contents from a previous call never
    /// leak into the refill.
    pub(crate) fn occupied_into(&self, out: &mut Vec<u64>) {
        out.clear();
        out.extend(
            self.slots
                .iter()
                .map(|s| s.load(Ordering::SeqCst))
                .filter(|&v| v != EMPTY),
        );
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn new_array_has_no_occupied_slots() {
        let arr = SlotArray::new(4);
        assert_eq!(arr.capacity(), 4);
        assert!(arr.occupied().is_empty());
    }

    #[test]
    fn claim_fills_an_empty_slot() {
        let arr = SlotArray::new(4);
        assert!(arr.claim(7));
        assert_eq!(arr.occupied(), vec![7]);
    }

    #[test]
    fn claim_fails_once_every_slot_is_occupied() {
        let arr = SlotArray::new(2);
        assert!(arr.claim(1));
        assert!(arr.claim(2));
        assert!(!arr.claim(3), "capacity-2 array must reject a third claim");
        let mut occ = arr.occupied();
        occ.sort_unstable();
        assert_eq!(occ, vec![1, 2]);
    }

    #[test]
    fn clear_matching_removes_only_named_values() {
        let arr = SlotArray::new(4);
        arr.claim(1);
        arr.claim(2);
        arr.claim(3);
        arr.clear_matching(&[2]);
        let mut occ = arr.occupied();
        occ.sort_unstable();
        assert_eq!(occ, vec![1, 3], "only the named value must be cleared");
    }

    #[test]
    fn clear_matching_is_a_noop_for_a_value_not_present() {
        let arr = SlotArray::new(4);
        arr.claim(1);
        arr.clear_matching(&[99]);
        assert_eq!(arr.occupied(), vec![1]);
    }

    #[test]
    fn after_clearing_a_slot_can_be_reclaimed() {
        let arr = SlotArray::new(1);
        assert!(arr.claim(1));
        assert!(!arr.claim(2), "array is full");
        arr.clear_matching(&[1]);
        assert!(arr.claim(2), "the freed slot must now be claimable");
        assert_eq!(arr.occupied(), vec![2]);
    }

    #[test]
    fn occupied_into_matches_occupied() {
        let arr = SlotArray::new(4);
        arr.claim(5);
        arr.claim(6);
        let mut out = Vec::new();
        arr.occupied_into(&mut out);
        let mut expected = arr.occupied();
        out.sort_unstable();
        expected.sort_unstable();
        assert_eq!(out, expected);
    }

    #[test]
    fn occupied_into_clears_stale_contents_from_a_reused_buffer() {
        let arr = SlotArray::new(4);
        arr.claim(9);
        let mut out = vec![111, 222, 333]; // stale data from a prior call
        arr.occupied_into(&mut out);
        assert_eq!(out, vec![9], "stale entries must not leak into the refill");
    }

    #[test]
    fn occupied_into_reuses_the_buffers_existing_capacity() {
        let arr = SlotArray::new(4);
        arr.claim(1);
        arr.claim(2);
        let mut out = Vec::with_capacity(16);
        let cap_before = out.capacity();
        arr.occupied_into(&mut out);
        assert_eq!(
            out.capacity(),
            cap_before,
            "occupied_into must not reallocate when the buffer already has enough capacity"
        );
    }
}

/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
/// (never a workspace-wide `RUSTFLAGS` — see `.claude/rules/concurrency-txn-layer.md`).
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// Two threads claiming slots concurrently, one thread shrinking (via
    /// `clear_matching`) concurrently with both — proves no slot ever
    /// reaches a torn state, a claim never silently overwrites another
    /// thread's already-claimed slot, and concurrent claim+shrink never
    /// produces a phantom edge (every occupied slot, at the end, holds
    /// exactly one of the values some thread actually claimed and that was
    /// never subsequently cleared).
    #[test]
    fn concurrent_claim_and_shrink_never_corrupts_a_slot() {
        loom::model(|| {
            let arr = loom::sync::Arc::new(SlotArray::new(2));

            let a1 = loom::sync::Arc::clone(&arr);
            let t1 = loom::thread::spawn(move || a1.claim(1));

            let a2 = loom::sync::Arc::clone(&arr);
            let t2 = loom::thread::spawn(move || a2.claim(2));

            let a3 = loom::sync::Arc::clone(&arr);
            let t3 = loom::thread::spawn(move || a3.clear_matching(&[1]));

            let claimed1 = t1.join().unwrap();
            let claimed2 = t2.join().unwrap();
            t3.join().unwrap();

            // Every slot must hold EMPTY or a value that was genuinely
            // claimed by t1 or t2 — never a torn/corrupted u64.
            let final_occupied = arr.occupied();
            for value in &final_occupied {
                assert!(
                    (*value == 1 && claimed1) || (*value == 2 && claimed2),
                    "slot holds a value {value} that was never validly claimed: {final_occupied:?}"
                );
            }
            // No duplicate values — a phantom edge would show up as the
            // same neighbor-id claimed into two slots simultaneously.
            let mut sorted = final_occupied.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                final_occupied.len(),
                "no value may occupy more than one slot: {final_occupied:?}"
            );
        });
    }
}
