//! Row-id-indexed, demand-allocated, chunked storage — no hashing, since
//! `crates/txn`'s row-ids are dense, monotonic `u64`s. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md` §2.

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, Ordering};

/// Rows per chunk. Sized so the top-level chunk-pointer directory (always
/// sized for `MAX_ROW_ID_CAPACITY`, never the caller's `expected_capacity`
/// hint — see `NodeTable::new`) stays small even at that ceiling:
/// `1_000_000_000 / 65536 ≈ 15259` chunk pointers, ~122KB — cheap
/// regardless of how many chunks are ever actually allocated, since
/// chunks themselves are demand-allocated.
const CHUNK_SIZE: usize = 65536;

/// Absolute ceiling on row-ids this table can address — matches
/// `crates/txn`'s own enforced limit (`MAX_REASONABLE_ROW_ID_CAPACITY` in
/// `crates/txn/src/dataset.rs`). The directory is always sized for this
/// ceiling rather than `NodeTable::new`'s `expected_capacity` hint: sizing
/// from the hint instead would panic on out-of-bounds directory access
/// for any row-id beyond it, directly contradicting the "never a hard
/// cap" contract `HnswIndex::new`'s existing `MaxElements` doc comment
/// already promises. Since chunks are demand-allocated, sizing the
/// (pointer-only) directory for the full ceiling costs a fixed ~122KB no
/// matter how small the actual graph is.
const MAX_ROW_ID_CAPACITY: usize = 1_000_000_000;

struct Chunk<T> {
    slots: Box<[AtomicPtr<T>]>,
}

impl<T> Chunk<T> {
    fn new() -> Self {
        let slots = (0..CHUNK_SIZE)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }
}

pub(crate) struct NodeTable<T> {
    chunks: Box<[AtomicPtr<Chunk<T>>]>,
}

impl<T> NodeTable<T> {
    /// `expected_capacity` is accepted for API symmetry with
    /// `HnswIndex::new`'s existing `MaxElements` sizing hint but is
    /// otherwise unused — the chunk-pointer directory is always sized for
    /// `MAX_ROW_ID_CAPACITY` (see that constant's doc comment for why).
    pub(crate) fn new(expected_capacity: usize) -> Self {
        let _ = expected_capacity;
        let num_chunks = MAX_ROW_ID_CAPACITY.div_ceil(CHUNK_SIZE).max(1);
        let chunks = (0..num_chunks)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { chunks }
    }

    fn chunk_index(row_id: u64) -> (usize, usize) {
        #[allow(clippy::cast_possible_truncation)]
        let row_id = row_id as usize;
        (row_id / CHUNK_SIZE, row_id % CHUNK_SIZE)
    }

    /// Returns the chunk at `chunk_idx`, allocating and publishing a new
    /// one if it doesn't exist yet. If two threads race to allocate the
    /// same chunk index, exactly one wins the compare-exchange; the
    /// loser's allocation was never visible to any other thread, so it's
    /// safe to drop synchronously — no epoch/hazard-pointer reclamation
    /// needed for this race (see design doc §2/§5).
    fn get_or_create_chunk(&self, chunk_idx: usize) -> &Chunk<T> {
        let existing = self.chunks[chunk_idx].load(Ordering::SeqCst);
        if !existing.is_null() {
            // SAFETY: a non-null pointer in `self.chunks[chunk_idx]` was
            // published by a successful compare_exchange below and is
            // never freed or moved afterward (Stage 1 never reclaims
            // chunks), so dereferencing it for the table's own lifetime is
            // sound.
            return unsafe { &*existing };
        }
        let new_chunk = Box::into_raw(Box::new(Chunk::new()));
        match self.chunks[chunk_idx].compare_exchange(
            ptr::null_mut(),
            new_chunk,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            // SAFETY: `new_chunk` was just published by this successful
            // compare_exchange and is never freed or moved afterward.
            Ok(_) => unsafe { &*new_chunk },
            Err(actual) => {
                // Lost the race: `new_chunk` was never observed by any
                // other thread (the compare_exchange that would have
                // published it failed), so no other thread can hold a
                // reference to it — safe to drop synchronously, no
                // reclamation scheme needed.
                // SAFETY: `new_chunk` came from `Box::into_raw` on the line
                // above in this same function and has not been shared with
                // any other thread (the publish attempt failed).
                unsafe {
                    drop(Box::from_raw(new_chunk));
                }
                // SAFETY: `actual` is the pointer that won the race — by
                // the same invariant as the `existing` branch above, it's
                // published and never freed/moved for the table's
                // lifetime.
                unsafe { &*actual }
            }
        }
    }

    /// Registers `value` at `row_id`. Must only be called once per
    /// `row_id` — `crates/txn`'s calling convention assigns each row-id
    /// exactly once and never re-inserts it, so this is a single `store`,
    /// not a CAS (there is no per-node contention to resolve, only the
    /// chunk-allocation race above).
    pub(crate) fn insert(&self, row_id: u64, value: T) {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        let chunk = self.get_or_create_chunk(chunk_idx);
        let value_ptr = Box::into_raw(Box::new(value));
        chunk.slots[offset].store(value_ptr, Ordering::SeqCst);
    }

    /// Looks up the value at `row_id`. Returns `None` if `row_id` has
    /// never been inserted (including if its chunk hasn't been allocated
    /// yet at all).
    pub(crate) fn get(&self, row_id: u64) -> Option<&T> {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        let chunk_ptr = self.chunks[chunk_idx].load(Ordering::SeqCst);
        if chunk_ptr.is_null() {
            return None;
        }
        // SAFETY: `chunk_ptr` is non-null, so it was published by
        // `get_or_create_chunk`'s successful compare_exchange and is never
        // freed or moved afterward.
        let chunk = unsafe { &*chunk_ptr };
        let value_ptr = chunk.slots[offset].load(Ordering::SeqCst);
        if value_ptr.is_null() {
            return None;
        }
        // SAFETY: `value_ptr` is non-null, so it was published by
        // `insert`'s `store` and is never freed or moved afterward (Stage
        // 1 never removes a node once inserted).
        Some(unsafe { &*value_ptr })
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn get_on_an_empty_table_returns_none() {
        let table: NodeTable<u32> = NodeTable::new(100);
        assert!(table.get(0).is_none());
    }

    #[test]
    fn insert_then_get_round_trips() {
        let table = NodeTable::new(100);
        table.insert(5, 42u32);
        assert_eq!(table.get(5), Some(&42));
    }

    #[test]
    fn get_for_an_uninserted_row_id_in_an_otherwise_populated_chunk_returns_none() {
        let table = NodeTable::new(100);
        table.insert(5, 42u32);
        assert!(table.get(6).is_none(), "row 6 was never inserted");
    }

    #[test]
    fn row_ids_spanning_multiple_chunks_all_round_trip() {
        let table = NodeTable::new(10); // small expected_capacity — still handles row-ids beyond it
        table.insert(0, 1u64);
        table.insert(CHUNK_SIZE as u64, 2u64); // forces a second chunk
        table.insert((CHUNK_SIZE * 2) as u64, 3u64); // forces a third chunk
        assert_eq!(table.get(0), Some(&1));
        assert_eq!(table.get(CHUNK_SIZE as u64), Some(&2));
        assert_eq!(table.get((CHUNK_SIZE * 2) as u64), Some(&3));
    }
}

/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// Two threads racing to allocate the same not-yet-existing chunk
    /// (both call `insert` for row-ids that land in the same chunk index)
    /// — proves exactly one chunk is ever published, the loser's
    /// allocation is safely discarded, and both inserts' values are
    /// readable afterward through the single winning chunk.
    #[test]
    fn concurrent_chunk_allocation_publishes_exactly_one_chunk() {
        loom::model(|| {
            let table = loom::sync::Arc::new(NodeTable::<u64>::new(1));

            let t1_table = loom::sync::Arc::clone(&table);
            let t1 = loom::thread::spawn(move || t1_table.insert(0, 100));

            let t2_table = loom::sync::Arc::clone(&table);
            let t2 = loom::thread::spawn(move || t2_table.insert(1, 200));

            t1.join().unwrap();
            t2.join().unwrap();

            // Both row-ids land in chunk 0 (CHUNK_SIZE is far larger than
            // 2) — both must be readable through whichever chunk won the
            // allocation race.
            assert_eq!(table.get(0), Some(&100));
            assert_eq!(table.get(1), Some(&200));
        });
    }
}
