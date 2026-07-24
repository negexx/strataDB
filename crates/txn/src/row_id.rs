//! Row-id allocation, and the in-flight claim registry that keeps
//! not-yet-committed row-ids invisible.
//!
//! # Why this exists
//!
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8
//! *originally* specified that a commit "claims the contiguous range
//! `[next_row_id, next_row_id + N)`" as part of §3 step 4's atomic manifest
//! CAS — §8 now records the design below instead, so don't read the current
//! text expecting the older rule. Phase 6 moved the claim *before* the
//! commit lock, because the row-id is baked into both
//! the data file's `_row_id` column and the delta-log entries, and those
//! files are written (and fsynced) outside the lock on purpose — see
//! `Transaction::commit`. Pulling allocation back inside the lock would drag
//! every data-file fsync in with it.
//!
//! That divergence broke a property §8 was silently relying on. Visibility
//! used to be a single scalar high-water mark, `watermark = next_row_id - 1`,
//! published by whichever transaction happened to be committing. Because
//! `next_row_id` is *global*, that watermark covered row-ids other,
//! still-in-flight transactions had claimed but not committed — so a reader
//! (which takes no lock) could observe a row between another transaction's
//! `graph.insert` and its `commit_manifest`. Spec §2 rules that out
//! outright: a transaction's writes are "never visible to any other
//! transaction until commit succeeds."
//!
//! A scalar cannot express "committed" once allocation order can diverge
//! from commit order, and it cannot be repaired by lowering it: clamping the
//! watermark to the lowest outstanding claim would hide the *committing*
//! transaction's own rows, breaking the equally load-bearing invariant that
//! an acknowledged write is immediately visible.
//!
//! # The shape
//!
//! So visibility is a bound *plus an exclusion set*, which is `PostgreSQL`'s
//! snapshot in miniature — `xmin`/`xmax` plus the `xip_list` of in-progress
//! transactions. A row-id is visible when it is at or below the bound and
//! not in the exclusion set. The exclusion set is the set of claims
//! outstanding at the instant the snapshot was published, so it is bounded
//! by the number of *concurrently committing* transactions, not by dataset
//! size or history length — typically empty, and a handful at most. That
//! matters because `Snapshot::is_visible` runs per candidate during HNSW
//! graph traversal.
//!
//! Row-ids abandoned by a failed commit are deliberately *not* tracked. They
//! become permanent gaps (spec §8: "gaps are safe, reuse is forbidden"), and
//! a gap needs no exclusion entry because nothing exists at it — the data
//! file never entered a manifest, so `scan` cannot see it, and
//! `GraphResidueGuard` removes it from the shared graph, so `vector_search`
//! cannot either. Only *pending* claims need hiding, which is what keeps the
//! exclusion set from growing without bound.
//!
//! # Locking
//!
//! The counter advance and the claim registration must be one atomic step:
//! if a claim could be observed after its row-ids were reflected in
//! `next_row_id` but before it appeared in the registry, a publisher reading
//! that instant would produce exactly the bound-without-exclusion pair this
//! module exists to prevent. A `Mutex` around both is the simplest thing
//! that gives that, and its critical section is an integer add plus a push
//! onto a tiny `Vec` — no I/O, unlike `Dataset.commit_lock`.
//!
//! **Lock order: `Dataset.commit_lock` -> `RowIdAllocator.state`, never the
//! reverse.** Claims are taken with no lock held; every other access
//! (`visibility_bound_excluding`, `RowIdClaim::release`, and the `Drop` that
//! backs it) happens either with `commit_lock` held or with no lock held.
//! Nothing acquires `commit_lock` while holding `state`, so the two cannot
//! deadlock.

use std::sync::Arc;

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;

use crate::error::{Result, TxnError};

/// A half-open range of row-ids, `[base, base + len)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowIdRange {
    pub(crate) base: u64,
    pub(crate) len: u64,
}

impl RowIdRange {
    /// Whether `row_id` falls in this range. Written as a subtraction
    /// rather than `row_id < self.base + self.len` so it stays correct
    /// without relying on the caller's overflow check having run.
    pub(crate) fn contains(self, row_id: u64) -> bool {
        row_id >= self.base && row_id - self.base < self.len
    }
}

/// The `(bound, exclusion set)` pair a committing transaction stamps into
/// the `Snapshot` it publishes. Read as one unit under the allocator lock,
/// so the two halves can never disagree.
pub(crate) struct VisibilityBound {
    /// One past the highest row-id handed out as of this read — the source
    /// of both `Manifest::next_row_id` and `Snapshot::watermark`.
    pub(crate) next_row_id: u64,
    /// Claims outstanding as of the same instant, excluding the committing
    /// transaction's own. Sorted ascending by `base`, as a by-product of
    /// claims being registered in allocation order.
    pub(crate) in_flight: Arc<[RowIdRange]>,
}

struct AllocatorState {
    next_row_id: u64,
    /// Claims handed out and not yet released, in allocation order.
    /// Bounded by the number of transactions concurrently inside
    /// `Transaction::commit`, so a linear scan is the right data structure.
    active: Vec<RowIdRange>,
}

/// Hands out row-id ranges and remembers which of them are still in flight.
pub(crate) struct RowIdAllocator {
    state: Mutex<AllocatorState>,
}

impl RowIdAllocator {
    /// Starts allocating at `next_row_id` — `Manifest::next_row_id` on
    /// `Dataset::create`/`open`, so ids are never reused across sessions.
    pub(crate) fn new(next_row_id: u64) -> Self {
        Self {
            state: Mutex::new(AllocatorState {
                next_row_id,
                active: Vec::new(),
            }),
        }
    }

    /// A poisoned allocator lock is recovered rather than propagated, for
    /// the same reason `commit_lock` recovers: the guarded state is only
    /// ever mutated by whole `push`/`remove`/integer-assignment steps, so a
    /// panicking holder cannot leave it half-updated.
    ///
    /// Defined twice rather than once with a `cfg` inside, because loom's
    /// `MutexGuard` is a distinct type from `std`'s and the return type has
    /// to vary with it.
    #[cfg(not(loom))]
    fn lock(&self) -> std::sync::MutexGuard<'_, AllocatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // loom's `Mutex::lock` returns a `LockResult` carrying `std`'s own
    // `PoisonError`, so the recovery expression is identical to the one
    // above; only the guard type differs.
    #[cfg(loom)]
    fn lock(&self) -> loom::sync::MutexGuard<'_, AllocatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Claims `count` contiguous row-ids for one transaction, registering
    /// them as in-flight for as long as the returned [`RowIdClaim`] lives.
    ///
    /// One claim per *transaction*, not per pending batch — this is spec
    /// §8's "a commit writing N rows atomically claims the contiguous range
    /// `[next_row_id, next_row_id + N)`", and it keeps the exclusion set to
    /// one entry per in-flight transaction.
    ///
    /// # Errors
    ///
    /// [`TxnError::ManifestOverflow`] if the range would run past
    /// `u64::MAX`. Checked *before* the counter moves — unlike the
    /// `fetch_add`-then-check this replaces, which had no way to undo the
    /// advance — so a rejected claim consumes no row-ids at all.
    pub(crate) fn claim(self: &Arc<Self>, count: u64) -> Result<RowIdClaim> {
        let mut state = self.lock();
        let base = state.next_row_id;
        let end = base
            .checked_add(count)
            .ok_or_else(|| TxnError::ManifestOverflow(format!("next_row_id {base} + {count}")))?;
        let range = RowIdRange { base, len: count };
        state.next_row_id = end;
        // An empty claim is not registered. It names no row-ids, so it has
        // nothing to hide, and leaving it out is what keeps every entry in
        // `active` a *distinct* range: a zero-length claim does not advance
        // `next_row_id`, so two concurrent ones would otherwise be
        // indistinguishable duplicates. (Reachable: a transaction whose
        // pending batches all have zero rows.)
        if count > 0 {
            state.active.push(range);
        }
        drop(state);
        Ok(RowIdClaim {
            allocator: Arc::clone(self),
            range,
            released: false,
        })
    }

    /// Reads the current bound together with every claim outstanding
    /// *except* `committing`, whose rows are about to become durable and
    /// must be visible in the snapshot this pair goes on to stamp.
    ///
    /// `committing` is `None` for a transaction that inserted nothing (a
    /// delete-only commit claims no row-ids).
    pub(crate) fn visibility_bound_excluding(
        &self,
        committing: Option<&RowIdClaim>,
    ) -> VisibilityBound {
        let excluded = committing.map(|claim| claim.range);
        let state = self.lock();
        let in_flight: Vec<RowIdRange> = state
            .active
            .iter()
            .copied()
            .filter(|range| Some(*range) != excluded)
            .collect();
        VisibilityBound {
            next_row_id: state.next_row_id,
            in_flight: in_flight.into(),
        }
    }

    fn release(&self, range: RowIdRange) {
        let mut state = self.lock();
        // Every registered range is distinct — each non-empty claim
        // advances `next_row_id` past the last, and empty ones are never
        // registered — so this removes exactly this claim's entry. Finding
        // nothing is normal (an empty claim, or a second `release`).
        if let Some(index) = state.active.iter().position(|active| *active == range) {
            state.active.remove(index);
        }
    }
}

/// A transaction's outstanding claim on a contiguous row-id range. While it
/// lives, those row-ids are excluded from every snapshot published by any
/// *other* transaction — which is what makes an in-flight write invisible
/// per spec §2.
///
/// Dropping it releases the claim, so an abandoned commit (an early `?`, or
/// a panic unwinding out of `commit`) can never strand an entry in the
/// registry and permanently blind readers to a stretch of row-ids. Those
/// ids then become permanent gaps, which is exactly spec §8's stated
/// behavior for a failed attempt.
pub(crate) struct RowIdClaim {
    allocator: Arc<RowIdAllocator>,
    range: RowIdRange,
    released: bool,
}

impl RowIdClaim {
    /// First row-id of the claimed range.
    pub(crate) fn base(&self) -> u64 {
        self.range.base
    }

    /// How many row-ids were claimed.
    pub(crate) fn len(&self) -> u64 {
        self.range.len
    }

    /// Releases the claim early, at the instant this commit becomes durable
    /// — before the `Drop` that would otherwise do it. Idempotent, so the
    /// drop that follows is a no-op.
    ///
    /// Calling this is not what makes the committing transaction's own rows
    /// visible (`visibility_bound_excluding` already filters them out); it
    /// is what stops them being hidden from *later* commits' snapshots.
    pub(crate) fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.allocator.release(self.range);
        }
    }
}

impl Drop for RowIdClaim {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn bases(bound: &VisibilityBound) -> Vec<u64> {
        bound.in_flight.iter().map(|range| range.base).collect()
    }

    #[test]
    fn successive_claims_hand_out_contiguous_non_overlapping_ranges() {
        let allocator = Arc::new(RowIdAllocator::new(0));
        let first = allocator.claim(3).unwrap();
        let second = allocator.claim(2).unwrap();
        assert_eq!(first.base(), 0);
        assert_eq!(second.base(), 3);
        assert_eq!(
            allocator.visibility_bound_excluding(None).next_row_id,
            5,
            "the bound must cover every id handed out, committed or not"
        );
    }

    #[test]
    fn an_outstanding_claim_is_excluded_from_another_transactions_bound() {
        let allocator = Arc::new(RowIdAllocator::new(0));
        let in_flight = allocator.claim(1).unwrap();
        let mut committing = allocator.claim(1).unwrap();

        // What the committing transaction stamps into its snapshot: its own
        // claim is visible, the concurrent one is not.
        let bound = allocator.visibility_bound_excluding(Some(&committing));
        assert_eq!(bound.next_row_id, 2);
        assert_eq!(bases(&bound), vec![0], "only the other claim is in flight");
        assert!(bound.in_flight[0].contains(0));
        assert!(!bound.in_flight[0].contains(1));

        committing.release();
        drop(in_flight);
        assert!(
            allocator
                .visibility_bound_excluding(None)
                .in_flight
                .is_empty(),
            "releasing every claim must leave nothing excluded"
        );
    }

    #[test]
    fn dropping_a_claim_without_releasing_it_still_clears_the_registry() {
        let allocator = Arc::new(RowIdAllocator::new(0));
        {
            let _abandoned = allocator.claim(4).unwrap();
            assert_eq!(bases(&allocator.visibility_bound_excluding(None)), vec![0]);
        }
        let bound = allocator.visibility_bound_excluding(None);
        assert!(
            bound.in_flight.is_empty(),
            "an abandoned commit must not blind readers to its row-ids forever"
        );
        assert_eq!(
            bound.next_row_id, 4,
            "its ids stay consumed — gaps are safe, reuse is forbidden (spec §8)"
        );
    }

    #[test]
    fn a_claim_that_would_overflow_is_rejected_without_consuming_row_ids() {
        let allocator = Arc::new(RowIdAllocator::new(u64::MAX - 1));
        assert!(matches!(
            allocator.claim(2),
            Err(TxnError::ManifestOverflow(_))
        ));
        let bound = allocator.visibility_bound_excluding(None);
        assert_eq!(
            bound.next_row_id,
            u64::MAX - 1,
            "a rejected claim must not advance the counter"
        );
        assert!(
            bound.in_flight.is_empty(),
            "a rejected claim must not register"
        );
        assert!(allocator.claim(1).is_ok(), "the last id is still available");
    }

    #[test]
    fn range_contains_is_half_open_and_overflow_safe() {
        let range = RowIdRange { base: 5, len: 3 };
        assert!(!range.contains(4));
        assert!(range.contains(5));
        assert!(range.contains(7));
        assert!(!range.contains(8));

        // `base + len` overflows here; `contains` must not.
        let saturating = RowIdRange {
            base: u64::MAX - 1,
            len: 2,
        };
        assert!(saturating.contains(u64::MAX));
        assert!(!saturating.contains(0));
    }
}
