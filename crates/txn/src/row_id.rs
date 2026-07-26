//! Row-id allocation: hands out contiguous, session-durable ranges from a
//! single global counter.
//!
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 defines
//! the contract: a commit writing N rows atomically claims the contiguous
//! range `[next_row_id, next_row_id + N)`, and ids are never reused —
//! "gaps are safe, reuse is forbidden." One claim per *transaction*, not
//! per pending batch, so a transaction's own rows stay contiguous even
//! when another transaction's claim interleaves with its own.
//!
//! Claimed *before* `commit_lock` is acquired, in
//! `Transaction::write_phase` — the row-id is baked into both the data
//! file's `_row_id` column and the index segment built from it
//! (segment-local ordinals map back to these exact row-ids), and those
//! files are written and fsynced outside the lock on purpose. Pulling
//! allocation back inside the lock would drag every data-file and segment
//! fsync in with it.
//!
//! # No in-flight exclusion set (removed — see the S1 segmented-index spec §6)
//!
//! An earlier version of this module paired the counter with a registry of
//! not-yet-committed claims, and `Snapshot::is_visible` excluded any
//! row-id whose claim was still outstanding, on top of a plain `row_id <=
//! watermark` bound. That machinery existed to close a hazard specific to
//! the OLD shared-mutable-HNSW-graph design: a transaction's vector was
//! applied to ONE graph object shared by every snapshot, live, before that
//! transaction's own commit was durable — so a watermark published by a
//! *different*, unrelated commit (reading the same global counter this
//! transaction had already advanced by claiming) could numerically cover
//! this transaction's row-id while it was *already* physically findable
//! in that shared graph.
//!
//! S1 W3.2 removed that hazard structurally: a snapshot's index is exactly
//! its own manifest's segment list, built fresh per commit and published
//! by the same atomic manifest swap as its row data — there is no shared,
//! eagerly-mutated structure for an in-flight transaction to leak into. A
//! row-id can only ever be found via `scan`/`vector_search` if its owning
//! transaction's data file/segment already appears in the snapshot's OWN
//! manifest, which is only ever true after that transaction's
//! `commit_manifest` succeeded — a watermark numerically covering an
//! uncommitted row-id is therefore harmless, since there is nothing for a
//! reader to find at that id regardless of what the watermark says.
//! Separately, the `row_id <= watermark` bound was always redundant for
//! anything `is_visible` is actually called with: every candidate it ever
//! receives comes from the calling snapshot's own segments/data files,
//! which cannot contain a row-id that snapshot itself didn't allocate.
//! `Snapshot::is_visible` reduces to the tombstone check alone; this
//! module keeps only the counter.
//!
//! Proven, not assumed: `dataset::loom_tests::
//! a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_watermark`
//! ("Model 3") passed against the OLD in-flight-tracking implementation as
//! a baseline, then again, completely unmodified, after this module's
//! simplification — see the S1 segmented-index spec §6 for the "migrate
//! the guarantee, then remove the mechanism" plan this followed.
//!
//! # Locking
//!
//! The counter still advances under a lock: two concurrent `claim` calls
//! must never observe or return overlapping ranges. A `Mutex` guarding a
//! bare `u64` is kept here deliberately, rather than replaced with
//! `AtomicU64::fetch_add` — `claim` needs a *checked* add (reject a claim
//! that would overflow `u64::MAX` before consuming any ids), which a bare
//! `fetch_add` cannot express atomically, and swapping in a
//! compare-and-swap retry loop instead is a separate simplification with
//! its own `loom` obligations, not bundled into this change.

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;

use crate::error::{Result, TxnError};

/// A half-open range of row-ids, `[base, base + len)`, claimed for one
/// transaction. There is nothing to release: once granted, a range is
/// permanently consumed (spec §8's "gaps are safe, reuse is forbidden"),
/// so the range itself is the whole of what a claim ever was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowIdRange {
    pub(crate) base: u64,
    pub(crate) len: u64,
}

impl RowIdRange {
    /// First row-id of this range.
    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    /// How many row-ids this range covers.
    pub(crate) fn len(&self) -> u64 {
        self.len
    }
}

struct AllocatorState {
    next_row_id: u64,
}

/// Hands out contiguous row-id ranges from a single global counter.
pub(crate) struct RowIdAllocator {
    state: Mutex<AllocatorState>,
}

impl RowIdAllocator {
    /// Starts allocating at `next_row_id` — `Manifest::next_row_id` on
    /// `Dataset::create`/`open`, so ids are never reused across sessions.
    pub(crate) fn new(next_row_id: u64) -> Self {
        Self {
            state: Mutex::new(AllocatorState { next_row_id }),
        }
    }

    /// A poisoned allocator lock is recovered rather than propagated, for
    /// the same reason `Dataset.commit_lock` does: the guarded state is
    /// only ever mutated by a single whole-integer assignment, so a
    /// panicking holder cannot leave it half-updated.
    ///
    /// Defined twice rather than once with a `cfg` inside, because loom's
    /// `MutexGuard` is a distinct type from `std`'s and the return type
    /// has to vary with it.
    #[cfg(not(loom))]
    fn lock(&self) -> std::sync::MutexGuard<'_, AllocatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn lock(&self) -> loom::sync::MutexGuard<'_, AllocatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Claims `count` contiguous row-ids for one transaction — spec §8's
    /// "a commit writing N rows atomically claims the contiguous range
    /// `[next_row_id, next_row_id + N)`".
    ///
    /// # Errors
    ///
    /// [`TxnError::ManifestOverflow`] if the range would run past
    /// `u64::MAX`. Checked *before* the counter moves, so a rejected claim
    /// consumes no row-ids at all.
    pub(crate) fn claim(&self, count: u64) -> Result<RowIdRange> {
        let mut state = self.lock();
        let base = state.next_row_id;
        let end = base
            .checked_add(count)
            .ok_or_else(|| TxnError::ManifestOverflow(format!("next_row_id {base} + {count}")))?;
        state.next_row_id = end;
        Ok(RowIdRange { base, len: count })
    }

    /// The current allocation high-water mark — one past the highest
    /// row-id handed out so far, committed or not. Read under the same
    /// lock `claim` uses, so it always reflects a real, fully-applied
    /// `claim` call, never a torn intermediate state.
    pub(crate) fn next_row_id(&self) -> u64 {
        self.lock().next_row_id
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn successive_claims_hand_out_contiguous_non_overlapping_ranges() {
        let allocator = RowIdAllocator::new(0);
        let first = allocator.claim(3).unwrap();
        let second = allocator.claim(2).unwrap();
        assert_eq!(first.base(), 0);
        assert_eq!(second.base(), 3);
        assert_eq!(
            allocator.next_row_id(),
            5,
            "the counter must cover every id handed out, committed or not"
        );
    }

    #[test]
    fn a_claim_that_would_overflow_is_rejected_without_consuming_row_ids() {
        let allocator = RowIdAllocator::new(u64::MAX - 1);
        assert!(matches!(
            allocator.claim(2),
            Err(TxnError::ManifestOverflow(_))
        ));
        assert_eq!(
            allocator.next_row_id(),
            u64::MAX - 1,
            "a rejected claim must not advance the counter"
        );
        assert!(allocator.claim(1).is_ok(), "the last id is still available");
    }
}
