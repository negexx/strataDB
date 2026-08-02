//! A bounded, per-[`Snapshot`](crate::snapshot::Snapshot) cache from a
//! predicate's identity to its resolved [`LiveSet`]. See
//! `docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md`
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
//! ceiling. The accounting is still an approximation, not exact (it
//! doesn't count `HashMap` bucket overhead or allocator metadata), and the
//! budget is soft in one more way: concurrent misses that both observe
//! "under budget" can push the total slightly over it before either
//! finishes; this is intentional (an atomic read-then-conditionally-insert
//! under the same lock closes that gap, but isn't worth the extra
//! complexity for a soft memory cap).
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

/// One cache entry's storage: `None` until the first caller for this key
/// fills it.
type Slot = Arc<Mutex<Option<Arc<LiveSet>>>>;

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
/// which isn't worth it for a soft admission-control budget.
const ENTRY_OVERHEAD_BYTES: usize = 256;

pub(crate) struct LiveSetCache {
    slots: Mutex<HashMap<PredicateKey, Slot>>,
    bytes: AtomicUsize,
    byte_budget: usize,
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
    fn lock_slot(slot: &Slot) -> std::sync::MutexGuard<'_, Option<Arc<LiveSet>>> {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(loom)]
    fn lock_slot(slot: &Slot) -> loom::sync::MutexGuard<'_, Option<Arc<LiveSet>>> {
        slot.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            } else if self.bytes.load(Ordering::Relaxed) < self.byte_budget {
                // `Relaxed`: `bytes` is an approximate admission-control
                // counter, not a value anything synchronizes with — the
                // actual publication of a computed `LiveSet` happens
                // through the per-slot `Mutex` below, which provides all
                // the ordering this cache needs.
                //
                // Charged now, before `compute` runs and win or lose — see
                // `ENTRY_OVERHEAD_BYTES`'s doc comment for why a failed
                // `compute` must still spend budget. `key.variable_byte_size()`
                // (the column name, plus a string value's own bytes) is
                // added on top of the fixed overhead so a predicate with a
                // long column name or a long `Utf8` value can't slip past
                // the budget uncounted — see `PredicateKey::variable_byte_size`'s
                // doc comment.
                let charge = ENTRY_OVERHEAD_BYTES + key.variable_byte_size();
                self.bytes.fetch_add(charge, Ordering::Relaxed);
                let slot: Slot = Arc::new(Mutex::new(None));
                slots.insert(key, Arc::clone(&slot));
                Lookup::Slot(slot)
            } else {
                Lookup::OverBudget
            }
        };

        let slot = match lookup {
            Lookup::Slot(slot) => slot,
            Lookup::OverBudget => return compute().map(Arc::new),
        };

        let mut guard = Self::lock_slot(&slot);
        if let Some(live_set) = guard.as_ref() {
            return Ok(Arc::clone(live_set));
        }
        let live_set = Arc::new(compute()?);
        // `Relaxed`: same reasoning as the overhead charge above — this is
        // an approximate admission-control counter, and `guard` (the
        // per-slot `Mutex`, locked for this whole scope) is what actually
        // publishes `live_set` to later readers, not this counter.
        self.bytes
            .fetch_add(live_set.byte_size(), Ordering::Relaxed);
        *guard = Some(Arc::clone(&live_set));
        Ok(live_set)
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

    #[derive(Debug, PartialEq, Eq)]
    struct Unreachable;

    fn key(n: i64) -> PredicateKey {
        PredicateKey::from(&strata_query::Predicate::Eq(
            "category".to_string(),
            strata_storage::Value::Int64(n),
        ))
    }
}
