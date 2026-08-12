//! A bounded, per-[`Snapshot`](crate::snapshot::Snapshot) cache from a
//! predicate's identity to its resolved [`LiveSet`]. See
//! `docs/phase-1-performance.md`
//! for why this exists: `Snapshot::row_ids_matching` re-reads a whole data
//! file's Arrow IPC body per query to resolve which rows match a predicate,
//! and `HnswIndex::search_filtered` rebuilds a bitset from that result on
//! every call — both are pure functions of `(Snapshot, Predicate)`, so a
//! live snapshot can serve every later call for a predicate it has already
//! resolved from one cached `LiveSet`, computed once.
//!
//! **Soundness — read before changing anything here.** This cache is sound
//! *only* because a `Snapshot` is fully immutable and is discarded whole
//! (never invalidated incrementally) when the `Dataset` it came from
//! commits again. Do not turn this into an incrementally-updated cache
//! (e.g. patching entries on commit) without re-deriving that argument from
//! scratch.
//!
//! **Bounding.** New entries stop being created once `byte_budget` has been
//! reached — no LRU/eviction machinery. Each entry charges, at slot-creation
//! time (before `compute` runs, win or lose): a fixed [`ENTRY_OVERHEAD_BYTES`],
//! the key's own [`PredicateKey::variable_byte_size`] (its column name, and
//! a `Utf8` value's own bytes — otherwise a long string predicate value
//! could slip past the budget uncounted), and — once `compute` succeeds —
//! the resulting `LiveSet`'s own [`LiveSet::byte_size`]. So a near-empty
//! result, a *failing* predicate, and a predicate carrying a long string
//! value all still spend real budget — see `ENTRY_OVERHEAD_BYTES`'s doc
//! comment for why charging only the `LiveSet` payload bytes would leave
//! this unbounded in the first two cases. This keeps the cache bounded
//! rather than merely deferred (an unbounded `HashMap<PredicateKey, _>`
//! would grow for as long as a caller holds this `Snapshot` across many
//! distinct ad-hoc predicates) — bounded *per live `Snapshot`*: N
//! long-lived readers cost up to N × `byte_budget`, not one shared
//! ceiling. `charged_bytes` is atomically hard-capped at `byte_budget`, even
//! across concurrent misses. The charge remains an approximation of resident
//! memory: it excludes `HashMap` buckets, mutexes, `Arc` headers, allocator
//! metadata, and other implementation overhead.
//!
//! **Lock discipline.** The outer `slots` map lock is held only to look up
//! or insert a per-key slot — never across `compute`, which does the actual
//! (potentially ~50 MB) file read. This is why misses on two different keys
//! never block each other. The inner per-key lock IS held across `compute`,
//! by design: it is what makes two concurrent misses on the *same* key
//! compute once instead of racing (the second caller blocks on the first's
//! slot lock, then observes the now-filled `Some`, rather than duplicating
//! the read).

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::Mutex;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

use strata_index::LiveSet;
use strata_query::PredicateKey;

/// One cache entry's storage, including an eviction marker so callers never
/// compute into a slot that has been removed from the outer map.
type Slot = Arc<Mutex<SlotState>>;

enum SlotState {
    Vacant,
    Cached(Arc<LiveSet>),
    Evicted,
}

/// Fixed per-entry charge against `byte_budget`, applied at slot-creation
/// time regardless of whether `compute` later succeeds. Two reasons this
/// exists rather than charging only a filled `LiveSet::byte_size()`:
///
/// 1. A `LiveSet` for a predicate matching zero (or few) rows is tiny
///    (~8 bytes for an empty bitset), but the real resident cost of one
///    entry is the `PredicateKey` (a `String` column name, and possibly a
///    `String` value), the `HashMap` bucket, and the `Arc<Mutex<Option<_>>>>`
///    slot allocation itself — charging only the payload would let far more
///    entries accumulate than the budget's name implies.
/// 2. A **failed** `compute` never reaches the payload-charging step at
///    all (see `get_or_try_compute`) — if overhead were only charged on a
///    successful fill, a caller issuing many distinct *failing* predicates
///    against one long-lived snapshot (e.g. an unknown column, or a
///    type-mismatched value — both surface as an `ArrowError` from
///    `strata_query::mask`) would grow the slot map without bound, because
///    the budget gate would never see it as spent.
///
/// This is a deliberately rough estimate, not a precise accounting —
/// getting it exactly right would need `std::mem::size_of` on types this
/// module doesn't own (`PredicateKey`'s internals) plus allocator overhead,
/// which isn't worth it for this bounded cache.
const ENTRY_OVERHEAD_BYTES: usize = 256;

pub(crate) struct LiveSetCache {
    slots: Mutex<HashMap<PredicateKey, Slot>>,
    bytes: AtomicUsize,
    byte_budget: usize,
}

/// A best-effort observation of one snapshot's live-set cache.
///
/// `charged_bytes` is atomically hard-capped at `byte_budget`, but is not an
/// allocator measurement: it excludes `HashMap` buckets, mutexes, `Arc`
/// headers, and allocator metadata. Concurrent misses may make the fields
/// describe slightly different instants; benchmark callers sample while no
/// query is concurrently populating the cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveSetCacheAccounting {
    /// Number of predicate slots retained by this snapshot.
    pub entry_count: usize,
    /// Approximate bytes charged to retained slots and successful live sets.
    pub charged_bytes: usize,
    /// The per-snapshot hard cap on `charged_bytes`.
    pub byte_budget: usize,
}

/// Whether a lookup found (or was allowed to create) a slot to compute
/// into, or the cache is at its byte budget and this call should bypass
/// caching entirely for this one predicate.
enum Lookup {
    Slot(Slot),
    OverBudget,
}

impl LiveSetCache {
    pub(crate) fn new(byte_budget: usize) -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            bytes: AtomicUsize::new(0),
            byte_budget,
        }
    }

    #[cfg(not(loom))]
    fn lock_slots(&self) -> std::sync::MutexGuard<'_, HashMap<PredicateKey, Slot>> {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn lock_slots(&self) -> loom::sync::MutexGuard<'_, HashMap<PredicateKey, Slot>> {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(not(loom))]
    fn lock_slot(slot: &Slot) -> std::sync::MutexGuard<'_, SlotState> {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn lock_slot(slot: &Slot) -> loom::sync::MutexGuard<'_, SlotState> {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn try_reserve_bytes(&self, bytes: usize) -> bool {
        let mut charged_bytes = self.bytes.load(Ordering::Relaxed);
        loop {
            let Some(next_charged_bytes) = charged_bytes.checked_add(bytes) else {
                return false;
            };
            if next_charged_bytes > self.byte_budget {
                return false;
            }
            match self.bytes.compare_exchange(
                charged_bytes,
                next_charged_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual_charged_bytes) => charged_bytes = actual_charged_bytes,
            }
        }
    }

    fn remove_evicted_slot(&self, slot: &Slot, entry_charge: usize) {
        let removed = {
            let mut slots = self.lock_slots();
            let entry_count = slots.len();
            slots.retain(|_, current_slot| !Arc::ptr_eq(current_slot, slot));
            slots.len() != entry_count
        };
        if removed {
            self.bytes.fetch_sub(entry_charge, Ordering::Relaxed);
        }
    }

    /// Returns the cached `LiveSet` for `key`, calling `compute` on a miss.
    /// `compute`'s error type is a generic parameter (not this crate's own
    /// error type) so this cache stays reusable without a dependency on any
    /// particular caller's error enum; `Snapshot` supplies its own
    /// `Result`-returning closure. A `compute` error is propagated and never
    /// stored — the next call for the same key retries from scratch, so a
    /// transient I/O failure doesn't poison the cache for the snapshot's
    /// remaining lifetime.
    ///
    /// `compute` must never call back into `get_or_try_compute` on `self`:
    /// with the same key that's a self-deadlock on the (non-reentrant)
    /// per-key `Mutex`; with a different key it's a slot-before-`slots`
    /// lock-order inversion against this method's own `slots`-then-slot
    /// order below (see `docs/design.md`'s
    /// requirement to document lock order at the acquisition site).
    pub(crate) fn get_or_try_compute<E>(
        &self,
        key: PredicateKey,
        compute: impl FnOnce() -> Result<LiveSet, E>,
    ) -> Result<Arc<LiveSet>, E> {
        let entry_charge = ENTRY_OVERHEAD_BYTES + key.variable_byte_size();
        // Lock order: `slots` (this block) then, if needed, one slot
        // (below) — never the reverse, and never both held at once past
        // this block's end. This block's only job is to decide which slot
        // (if any) to use, and it must end — releasing `slots`' lock —
        // before `compute` ever runs. See the module doc's "Lock
        // discipline" section.
        let lookup = {
            let mut slots = self.lock_slots();
            if let Some(slot) = slots.get(&key) {
                Lookup::Slot(Arc::clone(slot))
            } else {
                // `Relaxed`: compare-exchange makes this charged-byte
                // reservation atomic, while the per-slot `Mutex` below
                // provides the ordering for publishing a computed `LiveSet`.
                //
                // Charged now, before `compute` runs and win or lose — see
                // `ENTRY_OVERHEAD_BYTES`'s doc comment for why a failed
                // `compute` must still spend budget. `key.variable_byte_size()`
                // (the column name, plus a string value's own bytes) is
                // added on top of the fixed overhead so a predicate with a
                // long column name or a long `Utf8` value can't slip past
                // the budget uncounted — see `PredicateKey::variable_byte_size`'s
                // doc comment.
                if self.try_reserve_bytes(entry_charge) {
                    let slot: Slot = Arc::new(Mutex::new(SlotState::Vacant));
                    slots.insert(key, Arc::clone(&slot));
                    Lookup::Slot(slot)
                } else {
                    Lookup::OverBudget
                }
            }
        };

        let slot = match lookup {
            Lookup::Slot(slot) => slot,
            Lookup::OverBudget => return compute().map(Arc::new),
        };

        let mut guard = Self::lock_slot(&slot);
        match &*guard {
            SlotState::Cached(live_set) => return Ok(Arc::clone(live_set)),
            SlotState::Evicted => return compute().map(Arc::new),
            SlotState::Vacant => {}
        }
        let live_set = Arc::new(compute()?);
        let live_set_bytes = live_set.byte_size();
        if !self.try_reserve_bytes(live_set_bytes) {
            *guard = SlotState::Evicted;
            drop(guard);
            self.remove_evicted_slot(&slot, entry_charge);
            return Ok(live_set);
        }
        // The payload charge was atomically reserved above; `guard` (the
        // per-slot `Mutex`, locked for this whole scope) publishes `live_set`
        // to later readers.
        *guard = SlotState::Cached(Arc::clone(&live_set));
        Ok(live_set)
    }

    pub(crate) fn accounting(&self) -> LiveSetCacheAccounting {
        let entry_count = self.lock_slots().len();
        LiveSetCacheAccounting {
            entry_count,
            charged_bytes: self.bytes.load(Ordering::Relaxed),
            byte_budget: self.byte_budget,
        }
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use strata_storage::Value;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Unreachable;

    fn key(n: i64) -> PredicateKey {
        PredicateKey::from(&strata_query::Predicate::Eq(
            "category".to_string(),
            Value::Int64(n),
        ))
    }

    fn key_with_string_value(value: &str) -> PredicateKey {
        PredicateKey::from(&strata_query::Predicate::Eq(
            "category".to_string(),
            Value::Utf8(value.to_string()),
        ))
    }

    #[test]
    fn a_long_string_predicate_values_bytes_count_against_the_budget() {
        // Budget covers the fixed per-entry overhead plus a little slack,
        // nowhere near enough extra for a 1000-byte string value on top of
        // it. If the budget only charged `ENTRY_OVERHEAD_BYTES` and ignored
        // the key's own variable-length bytes, there would still be room
        // for a second, unrelated entry — there must not be.
        let long_value = "x".repeat(1000);
        let cache = LiveSetCache::new(ENTRY_OVERHEAD_BYTES + 10);
        let _ = cache.get_or_try_compute(
            key_with_string_value(&long_value),
            || -> Result<LiveSet, Unreachable> { Ok(LiveSet::from_row_ids(&[1])) },
        );

        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[2]))
        };
        cache.get_or_try_compute(key(99), compute).unwrap();
        cache.get_or_try_compute(key(99), compute).unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "a long string predicate value must count its own bytes against \
             the budget, not just the fixed per-entry overhead"
        );
    }

    #[test]
    fn a_deep_compound_predicates_tree_shape_counts_against_the_budget() {
        // Same shape of proof as the string-value test above, but for
        // PredicateKey's recursive And/Or case: a deeply-nested compound
        // predicate's own Box<Node> allocations must count against the
        // budget via PredicateKey::variable_byte_size's per-interior-node
        // charge, not just its leaves' column/value bytes -- otherwise a
        // caller issuing many distinct deep trees against one long-lived
        // Snapshot could grow this cache's slot map unboundedly while the
        // budget accounting saw it as nearly free.
        let mut deep = strata_query::Predicate::Eq("category".to_string(), Value::Int64(0));
        for i in 1..50 {
            deep = strata_query::Predicate::And(
                Box::new(deep),
                Box::new(strata_query::Predicate::Eq(
                    "category".to_string(),
                    Value::Int64(i),
                )),
            );
        }
        let deep_key = PredicateKey::from(&deep);
        let cache = LiveSetCache::new(ENTRY_OVERHEAD_BYTES + 10);
        let _ = cache.get_or_try_compute(deep_key, || -> Result<LiveSet, Unreachable> {
            Ok(LiveSet::from_row_ids(&[1]))
        });

        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[2]))
        };
        cache.get_or_try_compute(key(99), compute).unwrap();
        cache.get_or_try_compute(key(99), compute).unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "a deep compound predicate's tree shape must count its own bytes against \
             the budget, not just the fixed per-entry overhead"
        );
    }

    #[test]
    fn computes_once_and_reuses_on_second_call_with_the_same_key() {
        let cache = LiveSetCache::new(64 * 1024 * 1024);
        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[1, 2, 3]))
        };

        let first = cache.get_or_try_compute(key(3), compute).unwrap();
        let second = cache.get_or_try_compute(key(3), compute).unwrap();

        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "compute must run only once"
        );
        assert!(
            Arc::ptr_eq(&first, &second),
            "second call must reuse the cached Arc"
        );
    }

    #[test]
    fn different_keys_each_compute_independently() {
        let cache = LiveSetCache::new(64 * 1024 * 1024);
        let calls = AtomicUsize::new(0);

        cache
            .get_or_try_compute(key(1), || -> Result<LiveSet, Unreachable> {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(LiveSet::from_row_ids(&[1]))
            })
            .unwrap();
        cache
            .get_or_try_compute(key(2), || -> Result<LiveSet, Unreachable> {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(LiveSet::from_row_ids(&[2]))
            })
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn over_budget_computes_every_call_instead_of_growing_the_cache() {
        let cache = LiveSetCache::new(0);
        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[1, 2, 3]))
        };

        cache.get_or_try_compute(key(3), compute).unwrap();
        cache.get_or_try_compute(key(3), compute).unwrap();

        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "a zero-byte budget must never cache, so both calls recompute"
        );
    }

    #[test]
    fn an_oversized_live_set_is_returned_without_being_retained() {
        let cache = LiveSetCache::new(ENTRY_OVERHEAD_BYTES + "category".len());
        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[1_000_000]))
        };

        let first = cache.get_or_try_compute(key(3), compute).unwrap();
        assert!(first.contains(1_000_000));

        let accounting = cache.accounting();
        assert!(
            accounting.charged_bytes <= accounting.byte_budget,
            "an oversized live set must not leave cache accounting over budget"
        );
        assert_eq!(
            accounting.entry_count, 0,
            "an oversized live set must not retain an empty predicate slot"
        );
        assert_eq!(
            accounting.charged_bytes, 0,
            "an oversized live set must release its entry-overhead reservation"
        );

        let second = cache.get_or_try_compute(key(3), compute).unwrap();
        assert!(second.contains(1_000_000));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "an oversized live set must be returned but recomputed instead of retained"
        );
    }

    #[test]
    fn a_compute_error_is_not_cached_and_is_retried_next_call() {
        let cache = LiveSetCache::new(64 * 1024 * 1024);
        let calls = AtomicUsize::new(0);

        let first = cache.get_or_try_compute(key(3), || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err::<LiveSet, _>(Unreachable)
        });
        assert!(first.is_err());

        let second = cache.get_or_try_compute(key(3), || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[1]))
        });
        assert!(second.is_ok());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "a failed compute must not poison the slot for the next call"
        );
    }

    #[test]
    fn a_failed_computes_overhead_still_counts_against_the_budget() {
        // Budget for exactly two entries' fixed per-entry overhead, with no
        // headroom left for any LiveSet payload bytes at all -- proves the
        // overhead is charged at slot-creation time, not only on a
        // successful fill, so a caller issuing many distinct FAILING
        // predicates against one snapshot can't grow the cache without
        // bound (a failed compute never adds a LiveSet's own bytes, so if
        // only payload bytes were charged, failing predicates would be
        // free and the map would grow forever).
        let cache = LiveSetCache::new(ENTRY_OVERHEAD_BYTES * 2);
        for n in 0..2 {
            let _ = cache.get_or_try_compute(key(n), || -> Result<LiveSet, Unreachable> {
                Err(Unreachable)
            });
        }

        // Budget is now exhausted by two dead (failed, never-filled) slots.
        // A brand-new key must bypass caching entirely from here on
        // (compute runs every call) rather than being allowed to grow the
        // map further.
        let calls = AtomicUsize::new(0);
        let compute = || -> Result<LiveSet, Unreachable> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(LiveSet::from_row_ids(&[1]))
        };
        cache.get_or_try_compute(key(99), compute).unwrap();
        cache.get_or_try_compute(key(99), compute).unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "budget already exhausted by dead slots must still block new entries"
        );
    }
}

/// Loom interleaving coverage — concurrent misses on the same key must
/// compute exactly once, per the module doc's "Lock discipline" section.
/// Run with:
/// `cargo rustc -p strata-txn --lib --profile test -- --cfg loom` then the
/// resulting test binary directly (never a workspace-wide
/// `RUSTFLAGS=--cfg loom`) — see `AGENTS.md`.
#[cfg(all(test, loom))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use std::sync::Arc as StdArc;

    use loom::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn two_concurrent_misses_on_the_same_key_compute_exactly_once() {
        loom::model(|| {
            let cache = StdArc::new(LiveSetCache::new(64 * 1024 * 1024));
            let calls = StdArc::new(AtomicUsize::new(0));

            let mut handles = Vec::new();
            for _ in 0..2 {
                let cache = StdArc::clone(&cache);
                let calls = StdArc::clone(&calls);
                handles.push(loom::thread::spawn(move || {
                    let k = key(3);
                    cache
                        .get_or_try_compute(k, || -> Result<LiveSet, Unreachable> {
                            calls.fetch_add(1, Ordering::Relaxed);
                            Ok(LiveSet::from_row_ids(&[1, 2, 3]))
                        })
                        .unwrap()
                }));
            }

            let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

            assert_eq!(
                calls.load(Ordering::Relaxed),
                1,
                "two concurrent misses on the same key must compute exactly once"
            );
            assert!(
                StdArc::ptr_eq(&results[0], &results[1]),
                "both threads must observe the same cached Arc"
            );
        });
    }

    #[test]
    fn concurrent_different_key_payloads_do_not_exceed_the_budget() {
        loom::model(|| {
            let payload_bytes = LiveSet::from_row_ids(&[63]).byte_size();
            let entry_charge = ENTRY_OVERHEAD_BYTES + "category".len();
            let cache = StdArc::new(LiveSetCache::new(entry_charge * 2 + payload_bytes));

            let first_cache = StdArc::clone(&cache);
            let first = loom::thread::spawn(move || {
                first_cache
                    .get_or_try_compute(key(1), || -> Result<LiveSet, Unreachable> {
                        Ok(LiveSet::from_row_ids(&[63]))
                    })
                    .unwrap()
            });
            let second_cache = StdArc::clone(&cache);
            let second = loom::thread::spawn(move || {
                second_cache
                    .get_or_try_compute(key(2), || -> Result<LiveSet, Unreachable> {
                        Ok(LiveSet::from_row_ids(&[63]))
                    })
                    .unwrap()
            });

            assert!(first.join().unwrap().contains(63));
            assert!(second.join().unwrap().contains(63));
            let accounting = cache.accounting();
            assert!(
                accounting.charged_bytes <= accounting.byte_budget,
                "concurrent payload admission must not exceed the cache budget"
            );
        });
    }

    #[test]
    fn concurrent_different_key_entry_charges_do_not_exceed_the_budget() {
        loom::model(|| {
            let entry_charge = ENTRY_OVERHEAD_BYTES + "category".len();
            let cache = StdArc::new(LiveSetCache::new(entry_charge * 2 - 1));

            let first_cache = StdArc::clone(&cache);
            let first = loom::thread::spawn(move || {
                first_cache
                    .get_or_try_compute(key(1), || -> Result<LiveSet, Unreachable> {
                        Ok(LiveSet::from_row_ids(&[1]))
                    })
                    .unwrap()
            });
            let second_cache = StdArc::clone(&cache);
            let second = loom::thread::spawn(move || {
                second_cache
                    .get_or_try_compute(key(2), || -> Result<LiveSet, Unreachable> {
                        Ok(LiveSet::from_row_ids(&[2]))
                    })
                    .unwrap()
            });

            assert!(first.join().unwrap().contains(1));
            assert!(second.join().unwrap().contains(2));
            let accounting = cache.accounting();
            assert!(
                accounting.charged_bytes <= accounting.byte_budget,
                "concurrent entry admission must not exceed the cache budget"
            );
        });
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Unreachable;

    fn key(n: i64) -> PredicateKey {
        PredicateKey::from(&strata_query::Predicate::Eq(
            "category".to_string(),
            strata_storage::Value::Int64(n),
        ))
    }
}
