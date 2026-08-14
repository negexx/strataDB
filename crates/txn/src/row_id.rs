//! Row-id allocation: hands out contiguous, session-durable ranges from a
//! single global counter.
//!
//! `docs/design.md` defines
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
//! The named `dataset::loom_tests::
//! a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`
//! ("Model 3") is the regression gate for this simplification. Its required
//! post-change run is recorded separately because the full model can exceed
//! local resource limits; source comments do not imply completion.
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
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::Mutex;

use crate::error::{Result, TxnError};
#[cfg(not(loom))]
use strata_storage::{StorageOwner, persist_row_id_high_water_at_least_with};

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
    #[cfg(not(loom))]
    storage: Arc<StorageOwner>,
}

impl RowIdAllocator {
    /// Starts allocating at `next_row_id` — `Manifest::next_row_id` on
    /// `Dataset::create`/`open`, so ids are never reused across sessions.
    #[allow(dead_code)]
    pub(crate) fn new(dataset_dir: impl Into<PathBuf>, next_row_id: u64) -> Self {
        #[cfg(not(loom))]
        let dataset_dir = dataset_dir.into();
        #[cfg(loom)]
        let _ = dataset_dir;
        Self {
            state: Mutex::new(AllocatorState { next_row_id }),
            #[cfg(not(loom))]
            storage: Arc::new(StorageOwner::local(dataset_dir)),
        }
    }

    /// Starts an allocator using an already-owned dataset storage capability.
    #[cfg(not(loom))]
    pub(crate) fn new_with_storage(storage: Arc<StorageOwner>, next_row_id: u64) -> Self {
        Self {
            state: Mutex::new(AllocatorState { next_row_id }),
            storage,
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
    #[cfg(not(loom))]
    pub(crate) fn claim(&self, count: u64) -> Result<RowIdRange> {
        let mut state = self.lock();
        loop {
            let base = state.next_row_id;
            let end = base.checked_add(count).ok_or_else(|| {
                TxnError::ManifestOverflow(format!("next_row_id {base} + {count}"))
            })?;
            match persist_row_id_high_water_at_least_with(&self.storage, end) {
                Ok(persisted_end) if persisted_end == end => {
                    state.next_row_id = end;
                    return Ok(RowIdRange { base, len: count });
                }
                Ok(persisted_end) => {
                    // A record discovered above the in-memory seed is a
                    // durable floor, never a range this allocator may use.
                    state.next_row_id = state.next_row_id.max(persisted_end);
                }
                Err(error) => {
                    if let Some(possibly_published_end) = error.possibly_published_end() {
                        // The immutable record became visible before a
                        // directory-sync failure. Return the error without
                        // exposing this range, but retain the gap forever.
                        state.next_row_id = state.next_row_id.max(possibly_published_end);
                        return Err(TxnError::RowIdReservationDurability {
                            end: possibly_published_end,
                            source: error.into_storage_error(),
                        });
                    }
                    return Err(TxnError::Storage(error.into_storage_error()));
                }
            }
        }
    }

    /// Filesystem publication is modeled separately under loom. The real
    /// claim path stays disk-free in existing dataset loom models.
    #[cfg(loom)]
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

    fn allocator_dir(label: &str) -> std::path::PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-row-id-allocator-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    #[test]
    fn successive_claims_hand_out_contiguous_non_overlapping_ranges() {
        let dir = allocator_dir("successive");
        let allocator = RowIdAllocator::new(&dir, 0);
        let first = allocator.claim(3).unwrap();
        let second = allocator.claim(2).unwrap();
        assert_eq!(first.base(), 0);
        assert_eq!(second.base(), 3);
        assert_eq!(
            allocator.next_row_id(),
            5,
            "the counter must cover every id handed out, committed or not"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_claim_that_would_overflow_is_rejected_without_consuming_row_ids() {
        let dir = allocator_dir("overflow");
        let allocator = RowIdAllocator::new(&dir, u64::MAX - 1);
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
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "test-fault-injection")]
    #[test]
    fn a_pre_publish_failure_does_not_advance_the_allocator() {
        let dir = allocator_dir("pre-publish-failure");
        let allocator = RowIdAllocator::new(&dir, 0);
        let _fault =
            strata_storage::row_id_high_water::test_support::fail_reservation_before_publish(
                std::io::ErrorKind::Other,
            );

        let result = allocator.claim(1);

        assert!(matches!(result, Err(TxnError::Storage(_))));
        assert_eq!(allocator.next_row_id(), 0);
        assert_eq!(strata_storage::read_row_id_high_water(&dir).unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(loom)]
pub(crate) mod loom_tests {
    use loom::sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering},
    };

    use super::RowIdRange;

    #[derive(Clone, Copy, Debug)]
    enum Publication {
        Durable,
        FailsAfterPublish,
    }

    struct AllocatorState {
        next: u64,
    }

    /// Models the durable collection independently from allocator
    /// linearization. `published` means the immutable record became
    /// observable; `durable` means its directory-sync confirmation returned;
    /// `exposed` means a caller received a successful range. They are three
    /// distinct transitions in the real protocol, not aliases of one scalar.
    struct PublicationState {
        published: AtomicU64,
        durable: AtomicU64,
        exposed: AtomicU64,
    }

    // Every claim moves through a bounded, one-way state machine. The
    // post-publication failure is intentionally the only failure branch:
    // it is the branch that must retain a gap and is the durability boundary
    // the production allocator cannot weaken.
    const ALLOCATED: u8 = 1;
    const PUBLISHED: u8 = 2;
    const DURABLE: u8 = 3;
    const EXPOSED: u8 = 4;
    const FAILED_AFTER_PUBLISH: u8 = 5;

    fn claim(
        allocator: &Mutex<AllocatorState>,
        publication: &PublicationState,
        phase: &AtomicU8,
        outcome: Publication,
    ) -> (RowIdRange, bool) {
        let mut allocator = allocator.lock().unwrap();
        let base = allocator.next;
        let end = base + 1;
        let range = RowIdRange { base, len: 1 };

        phase.store(ALLOCATED, Ordering::SeqCst);
        publication.published.store(end, Ordering::SeqCst);
        phase.store(PUBLISHED, Ordering::SeqCst);
        allocator.next = end;

        if matches!(outcome, Publication::FailsAfterPublish) {
            // The immutable record may survive the failed confirmation, so
            // its range is consumed but is never returned to the caller.
            phase.store(FAILED_AFTER_PUBLISH, Ordering::SeqCst);
            return (range, false);
        }

        publication.durable.store(end, Ordering::SeqCst);
        phase.store(DURABLE, Ordering::SeqCst);
        assert!(
            publication.published.load(Ordering::SeqCst) >= end,
            "a high-water record must be published before its durable confirmation"
        );
        publication.exposed.store(end, Ordering::SeqCst);
        phase.store(EXPOSED, Ordering::SeqCst);
        (range, true)
    }

    fn ranges_do_not_overlap(left: RowIdRange, right: RowIdRange) -> bool {
        let left_end = left.base + left.len;
        let right_end = right.base + right.len;
        left_end <= right.base || right_end <= left.base
    }

    #[test]
    fn concurrent_claims_publish_monotonic_high_water() {
        loom::model(|| {
            let allocator = Arc::new(Mutex::new(AllocatorState { next: 0 }));
            let publication = Arc::new(PublicationState {
                published: AtomicU64::new(0),
                durable: AtomicU64::new(0),
                exposed: AtomicU64::new(0),
            });
            let first_phase = Arc::new(AtomicU8::new(0));
            let second_phase = Arc::new(AtomicU8::new(0));

            let first_allocator = Arc::clone(&allocator);
            let first_publication = Arc::clone(&publication);
            let first_phase_for_claim = Arc::clone(&first_phase);
            let first = loom::thread::spawn(move || {
                claim(
                    &first_allocator,
                    &first_publication,
                    &first_phase_for_claim,
                    Publication::FailsAfterPublish,
                )
            });
            let second_allocator = Arc::clone(&allocator);
            let second_publication = Arc::clone(&publication);
            let second_phase_for_claim = Arc::clone(&second_phase);
            let second = loom::thread::spawn(move || {
                claim(
                    &second_allocator,
                    &second_publication,
                    &second_phase_for_claim,
                    Publication::Durable,
                )
            });

            let (failed_range, failed_exposed) = first.join().unwrap();
            let (successful_range, successful_exposed) = second.join().unwrap();

            assert!(
                ranges_do_not_overlap(failed_range, successful_range),
                "two concurrently-started claims must consume non-overlapping ranges"
            );
            assert!(
                !failed_exposed,
                "a post-publication failure must leave its range unexposed"
            );
            assert!(successful_exposed, "a durable claim must expose its range");
            assert_eq!(
                first_phase.load(Ordering::SeqCst),
                FAILED_AFTER_PUBLISH,
                "the failed claim must stop after publication"
            );
            assert_eq!(
                second_phase.load(Ordering::SeqCst),
                EXPOSED,
                "the successful claim must reach API exposure"
            );

            let published = publication.published.load(Ordering::SeqCst);
            let durable = publication.durable.load(Ordering::SeqCst);
            let exposed = publication.exposed.load(Ordering::SeqCst);
            assert!(
                published >= durable && durable >= exposed,
                "high-water transitions must remain monotonic: published={published}, durable={durable}, exposed={exposed}"
            );
            assert!(
                durable >= successful_range.base + successful_range.len,
                "a successful range must have crossed durable high-water before exposure"
            );
            assert!(
                published >= failed_range.base + failed_range.len,
                "a failed post-publication claim must retain its published high-water floor"
            );
            assert_eq!(
                allocator.lock().unwrap().next,
                failed_range.base.max(successful_range.base) + 1,
                "the allocator must retain every consumed range"
            );
        });
    }
}
