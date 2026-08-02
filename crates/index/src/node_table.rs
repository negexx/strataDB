//! Row-id-indexed, demand-allocated, chunked storage — no hashing, since
//! `crates/txn`'s row IDs are monotonically allocated `u64`s; gaps are valid. See
//! `docs/design.md`.

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
///
/// Under `--cfg loom`, both this and `MAX_ROW_ID_CAPACITY` are far
/// smaller (still comfortably larger than any row-id this crate's loom
/// tests use — see e.g. `graph.rs`'s `concurrent_inserts_of_distinct_rows_
/// are_all_findable_and_uncorrupted`, which only reaches row-id 2).
/// `NodeTable`'s `Drop` scans every directory slot, and every slot of
/// every *allocated* chunk, to find what to reclaim — at the real
/// production sizes that's ~80,000 atomic loads on every single dropped
/// table, and loom accounts for every atomic operation in its
/// exhaustive-interleaving budget even when (as here) the scan runs
/// single-threaded after every spawned thread has already joined. Confirmed
/// empirically: two loom tests that passed before `Drop` existed started
/// failing with "Model exceeded maximum number of branches" once it did,
/// and shrinking these two constants under loom alone (with the real
/// production values completely unchanged) fixed both without touching
/// any test's logic or assertions.
#[cfg(loom)]
const CHUNK_SIZE: usize = 64;
#[cfg(not(loom))]
const CHUNK_SIZE: usize = 65536;

/// Absolute ceiling on row-ids this table can address — matches
/// `crates/txn`'s own enforced limit (`MAX_REASONABLE_ROW_ID_CAPACITY` in
/// `crates/txn/src/dataset.rs`) in non-loom builds. The directory is
/// always sized for this ceiling rather than `NodeTable::new`'s
/// `expected_capacity` hint: sizing from the hint instead would panic on
/// out-of-bounds directory access for any row-id beyond it, directly
/// contradicting the "never a hard cap" contract `HnswIndex::new`'s
/// existing `MaxElements` doc comment already promises. Since chunks are
/// demand-allocated, sizing the (pointer-only) directory for the full
/// ceiling costs a fixed ~122KB no matter how small the actual graph is.
/// See `CHUNK_SIZE`'s doc comment for why this is far smaller under loom.
#[cfg(loom)]
const MAX_ROW_ID_CAPACITY: usize = CHUNK_SIZE * 4;
#[cfg(not(loom))]
const MAX_ROW_ID_CAPACITY: usize = 1_000_000_000;

/// Types stored in a [`NodeTable`] that own memory *beyond* the `Box`
/// wrapper `NodeTable` itself manages (e.g. `crate::node::Node`'s separate
/// `alloc_node`-allocated block) must implement this so `NodeTable`'s
/// `Drop` can free it. `Node` cannot implement `Drop` directly -- it
/// derives `Copy`, and Rust forbids a type being both `Copy` and `Drop`
/// (a `Copy` value can be implicitly duplicated, so a `Drop` impl on it
/// would risk freeing the same out-of-band memory more than once) -- so
/// this trait is the seam `NodeTable` uses instead.
pub(crate) trait Reclaim {
    /// Frees any memory owned by `self` beyond its own in-place bytes (a
    /// no-op for a type with nothing extra to free, such as a plain
    /// integer).
    ///
    /// # Safety
    /// Must be called at most once per value, with no other reference to
    /// it outstanding. `NodeTable`'s `Drop` is the only caller in this
    /// crate, which satisfies this via `&mut self`'s exclusive access.
    unsafe fn reclaim(self);
}

// No out-of-band memory to free -- plain integers, used only by this
// module's own generic tests (production code only ever instantiates
// `NodeTable<crate::node::Node>`). Gated out of production builds so the
// only `Reclaim` impl a non-test build ever sees is `Node`'s.
#[cfg(any(test, loom))]
impl Reclaim for u32 {
    unsafe fn reclaim(self) {}
}
#[cfg(any(test, loom))]
impl Reclaim for u64 {
    unsafe fn reclaim(self) {}
}

struct Chunk<T: Reclaim> {
    slots: Box<[AtomicPtr<T>]>,
}

impl<T: Reclaim> Chunk<T> {
    fn new() -> Self {
        let slots = (0..CHUNK_SIZE)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }
}

impl<T: Reclaim> Drop for Chunk<T> {
    fn drop(&mut self) {
        // `Ordering::Relaxed`, not `.get_mut()`: `&mut self` already gives
        // exclusive access (no concurrent reader/writer can exist), so no
        // ordering stronger than Relaxed buys anything here -- and unlike
        // `.get_mut()`, `.load()` exists on both `std`'s and `loom`'s
        // `AtomicPtr`, so this compiles under `--cfg loom` too.
        for slot in &self.slots {
            let value_ptr = slot.load(Ordering::Relaxed);
            if value_ptr.is_null() {
                continue;
            }
            // SAFETY: a non-null slot was published by `NodeTable::insert`'s
            // `Box::into_raw(Box::new(value))` (or `insert_ptr`, whose own
            // safety contract requires the same eventual single reclaim)
            // and never freed or moved since -- `&mut self` here means no
            // other reference to this `Chunk` (or anything it points to)
            // exists, so reconstructing and dropping the box exactly once
            // is sound.
            let boxed: Box<T> = unsafe { Box::from_raw(value_ptr) };
            let value: T = *boxed;
            // SAFETY: this value has never had `reclaim` called on it
            // before (it was only ever stored, never read out until this
            // drop), and nothing else can reference it now.
            unsafe {
                value.reclaim();
            }
        }
    }
}

pub(crate) struct NodeTable<T: Reclaim> {
    chunks: Box<[AtomicPtr<Chunk<T>>]>,
}

impl<T: Reclaim> Drop for NodeTable<T> {
    fn drop(&mut self) {
        // Same `Relaxed`-load-not-`get_mut()` reasoning as `Chunk::drop`.
        for slot in &self.chunks {
            let chunk_ptr = slot.load(Ordering::Relaxed);
            if chunk_ptr.is_null() {
                continue;
            }
            // SAFETY: a non-null directory slot was published by
            // `get_or_create_chunk`'s successful compare_exchange and
            // never freed or moved since -- `&mut self` here means no
            // other reference to this table (or any chunk it points to)
            // exists, so reconstructing and dropping the box exactly once
            // is sound. Dropping `Box<Chunk<T>>` runs `Chunk`'s own `Drop`,
            // which reclaims every value the chunk holds.
            drop(unsafe { Box::from_raw(chunk_ptr) });
        }
    }
}

/// Returned by [`NodeTable::insert`] when `row_id` lies beyond the table's
/// addressable range (`MAX_ROW_ID_CAPACITY`).
///
/// The value is never stored, and `insert` explicitly reclaims it (see its
/// own doc comment) before returning this error — nothing is leaked. The
/// caller turns this into a typed error. `crates/txn` already rejects such
/// row-ids before commit (its own `MAX_REASONABLE_ROW_ID_CAPACITY` check),
/// so through the normal commit path this is unreachable; it exists so the
/// `pub` `HnswIndex::insert` is self-defending against any other caller
/// instead of relying on that cross-crate convention to avoid a panic.
#[derive(Debug)]
pub(crate) struct CapacityExceeded {
    pub(crate) capacity: u64,
}

impl<T: Reclaim> NodeTable<T> {
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

    /// The exclusive upper bound on addressable row-ids: `chunks.len() *
    /// CHUNK_SIZE`. Rounds up past `MAX_ROW_ID_CAPACITY` because the
    /// directory is sized with `div_ceil`.
    fn addressable_capacity(&self) -> u64 {
        (self.chunks.len() as u64) * (CHUNK_SIZE as u64)
    }

    fn chunk_index(row_id: u64) -> (usize, usize) {
        #[allow(clippy::cast_possible_truncation)]
        let row_id = row_id as usize;
        (row_id / CHUNK_SIZE, row_id % CHUNK_SIZE)
    }

    /// Returns the chunk at `chunk_idx`, allocating and publishing a new
    /// one if it doesn't exist yet, or `None` if `chunk_idx` is past the end
    /// of the directory (a `row_id` beyond `MAX_ROW_ID_CAPACITY`). If two
    /// threads race to allocate the same chunk index, exactly one wins the
    /// compare-exchange; the loser's allocation was never visible to any
    /// other thread, so it's safe to drop synchronously — no
    /// epoch/hazard-pointer reclamation needed for this race (see design doc
    /// §2/§5).
    fn get_or_create_chunk(&self, chunk_idx: usize) -> Option<&Chunk<T>> {
        // Bound the directory access rather than indexing `self.chunks`
        // directly: an out-of-range `chunk_idx` would otherwise panic here.
        // `chunks` has a fixed length set once at construction, so this
        // length check races with nothing and adds no atomic operation.
        let slot = self.chunks.get(chunk_idx)?;
        let existing = slot.load(Ordering::SeqCst);
        if !existing.is_null() {
            // SAFETY: a non-null pointer in this directory slot was
            // published by a successful compare_exchange below and is
            // never freed or moved afterward (Stage 1 never reclaims
            // chunks), so dereferencing it for the table's own lifetime is
            // sound.
            return Some(unsafe { &*existing });
        }
        let new_chunk = Box::into_raw(Box::new(Chunk::new()));
        match slot.compare_exchange(
            ptr::null_mut(),
            new_chunk,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            // SAFETY: `new_chunk` was just published by this successful
            // compare_exchange and is never freed or moved afterward.
            Ok(_) => Some(unsafe { &*new_chunk }),
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
                Some(unsafe { &*actual })
            }
        }
    }

    /// Registers `value` at `row_id`. Must only be called once per
    /// `row_id` — `crates/txn`'s calling convention assigns each row-id
    /// exactly once and never re-inserts it, so this is a single `store`,
    /// not a CAS (there is no per-node contention to resolve, only the
    /// chunk-allocation race above).
    ///
    /// **Load-bearing beyond that:** each distinct `value` may be passed to
    /// `insert` (across *any* row-id, on *any* table) at most once. This
    /// method is safe to call, but `T: Reclaim` means `NodeTable`'s `Drop`
    /// will call `reclaim()` on every stored copy independently — for
    /// `crate::node::Node`, which is `Copy`, inserting the same value twice
    /// (e.g. `table.insert(a, node)` then `table.insert(b, node)`) stores
    /// two handles to the *same* `alloc_node` block and reclaims it twice,
    /// which is a double-free. Nothing in the type system prevents this;
    /// every current caller (`Graph::insert`) constructs a fresh `Node` per
    /// call and inserts it exactly once, so the invariant holds today by
    /// construction, not by enforcement — keep it that way if `Node`
    /// construction sites ever change.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityExceeded`] if `row_id` is beyond the table's
    /// addressable range. The bound is checked *before* the value is boxed,
    /// so nothing is ever boxed on this path — but `value` may still own
    /// out-of-band memory of its own (e.g. `crate::node::Node`'s
    /// `alloc_node` block), so this reclaims it explicitly before
    /// returning, rather than relying on `value`'s own drop glue (which,
    /// for a `Copy` type like `Node`, does nothing).
    ///
    /// **On `Err`, `value` has already been reclaimed by this call.** For a
    /// `Copy` type like `Node`, `insert` taking `value` by value does *not*
    /// stop the caller's own binding from still being in scope after this
    /// returns — that binding is now a dangling handle. Do not read
    /// through, or pass onward, `value` after an `Err` return; treat it the
    /// same as a value that was moved out and dropped.
    pub(crate) fn insert(&self, row_id: u64, value: T) -> Result<(), CapacityExceeded> {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        let Some(chunk) = self.get_or_create_chunk(chunk_idx) else {
            // The true addressable ceiling is the directory's exclusive upper
            // bound, `chunks.len() * CHUNK_SIZE`, which rounds up past
            // `MAX_ROW_ID_CAPACITY` — report that exact number rather than the
            // soft constant so the error means precisely what it says. (Any
            // id that reaches here is `>= this`, hence also `> 1e9`, so it is
            // genuinely beyond `crates/txn`'s own upstream limit too.)
            // SAFETY: `value` was never stored anywhere (this returns before
            // any `Box`/slot is created), so this is the only handle to it
            // and reclaiming it here is a single, final reclaim.
            unsafe {
                value.reclaim();
            }
            return Err(CapacityExceeded {
                capacity: self.addressable_capacity(),
            });
        };
        let value_ptr = Box::into_raw(Box::new(value));
        // `offset` is `row_id % CHUNK_SIZE`, always in `0..CHUNK_SIZE`, which
        // is exactly `slots.len()` — so this index can never be out of
        // bounds once the chunk exists. Only the directory access above is
        // bounded by the caller's `row_id`.
        chunk.slots[offset].store(value_ptr, Ordering::SeqCst);
        Ok(())
    }

    /// Registers an already-allocated `value` at `row_id`, storing the
    /// given pointer directly instead of boxing a fresh copy the way
    /// [`Self::insert`] does. Same single-registration contract as
    /// `insert` (must only be called once per `row_id`). Exists so a
    /// caller that already produced a raw, fully-initialized pointer
    /// (e.g. `crate::node_layout::alloc_node`) doesn't pay for a second,
    /// redundant allocation just to hand it to this table.
    ///
    /// # Safety
    /// `ptr` must be non-null, must point to a validly initialized `T`,
    /// and must never be freed or mutated through any other handle for
    /// the table's lifetime. **`ptr` must also have come from
    /// `Box::into_raw(Box::new(value))`** (exactly what [`Self::insert`]
    /// does internally) — `NodeTable`'s `Drop` reconstructs every stored
    /// pointer via `Box::from_raw` to reclaim it, so a pointer from any
    /// other allocation (e.g. a future bump/arena allocator, which is
    /// this method's intended eventual caller) would be unsound to free
    /// that way. Whoever wires up an arena-backed caller for this method
    /// must revisit `NodeTable`'s `Drop` at the same time — it cannot stay
    /// a blanket `Box::from_raw` once a non-`Box` source exists.
    // Not called by any production code path yet — reserved for Stage B's
    // `NodeArena` (Task 10 of the single-allocation-node plan), where it
    // removes a real, large per-node allocation. Exercised today by this
    // module's own `insert_ptr_then_get_round_trips` test (same pattern as
    // the not-yet-consumed accessors in `node.rs`/`slot_array.rs`) — that
    // test's pointer *does* come from `Box::new`, so it round-trips
    // through `Drop` soundly today.
    #[allow(dead_code)]
    ///
    /// # Errors
    ///
    /// Returns [`CapacityExceeded`] if `row_id` is beyond the table's
    /// addressable range, mirroring [`Self::insert`]. On that path nothing is
    /// stored, so the caller retains ownership of `ptr` (it must free it) —
    /// unreachable in practice, since `crates/txn` bounds row-ids upstream;
    /// it exists so this path cannot panic on an out-of-range directory index.
    pub(crate) unsafe fn insert_ptr(
        &self,
        row_id: u64,
        ptr: *mut T,
    ) -> Result<(), CapacityExceeded> {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        let Some(chunk) = self.get_or_create_chunk(chunk_idx) else {
            return Err(CapacityExceeded {
                capacity: self.addressable_capacity(),
            });
        };
        chunk.slots[offset].store(ptr, Ordering::SeqCst);
        Ok(())
    }

    /// Looks up the value at `row_id`. Returns `None` if `row_id` has
    /// never been inserted (including if its chunk hasn't been allocated
    /// yet at all).
    pub(crate) fn get(&self, row_id: u64) -> Option<&T> {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        // `.get` rather than `self.chunks[chunk_idx]`: a `row_id` past
        // `MAX_ROW_ID_CAPACITY` was necessarily never inserted, so `None`
        // (its natural meaning here) is correct and no panic is possible.
        let chunk_ptr = self.chunks.get(chunk_idx)?.load(Ordering::SeqCst);
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
        // SAFETY: `value_ptr` is non-null, so it was published by the
        // `store` in `insert` (or `insert_ptr`, its currently-unused
        // raw-pointer sibling — same publication contract) and is never
        // freed or moved afterward (Stage 1 never removes a node once
        // inserted).
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
        table.insert(5, 42u32).unwrap();
        assert_eq!(table.get(5), Some(&42));
    }

    #[test]
    fn insert_ptr_then_get_round_trips() {
        let table: NodeTable<u32> = NodeTable::new(100);
        let boxed: *mut u32 = Box::into_raw(Box::new(42u32));
        // SAFETY: `boxed` is non-null, points to a validly initialized
        // u32, and is never freed elsewhere in this test.
        unsafe { table.insert_ptr(5, boxed) }.unwrap();
        assert_eq!(table.get(5), Some(&42));
    }

    #[test]
    fn get_for_an_uninserted_row_id_in_an_otherwise_populated_chunk_returns_none() {
        let table = NodeTable::new(100);
        table.insert(5, 42u32).unwrap();
        assert!(table.get(6).is_none(), "row 6 was never inserted");
    }

    #[test]
    fn row_ids_spanning_multiple_chunks_all_round_trip() {
        let table = NodeTable::new(10); // small expected_capacity — still handles row-ids beyond it
        table.insert(0, 1u64).unwrap();
        table.insert(CHUNK_SIZE as u64, 2u64).unwrap(); // forces a second chunk
        table.insert((CHUNK_SIZE * 2) as u64, 3u64).unwrap(); // forces a third chunk
        assert_eq!(table.get(0), Some(&1));
        assert_eq!(table.get(CHUNK_SIZE as u64), Some(&2));
        assert_eq!(table.get((CHUNK_SIZE * 2) as u64), Some(&3));
    }

    #[test]
    fn a_row_id_past_capacity_is_rejected_without_panicking() {
        let table: NodeTable<u32> = NodeTable::new(100);
        // Far beyond MAX_ROW_ID_CAPACITY, so its chunk index is past the end
        // of the directory. Previously this panicked on an out-of-bounds
        // index; now both paths fail gracefully.
        let past_capacity = 10_000_000_000_u64;
        let err = table
            .insert(past_capacity, 7)
            .expect_err("insert past capacity must be rejected");
        // The reported ceiling is the directory's true exclusive bound, which
        // rounds up just past the 1e9 soft constant.
        assert!(err.capacity >= MAX_ROW_ID_CAPACITY as u64);
        assert_eq!(
            err.capacity,
            (MAX_ROW_ID_CAPACITY.div_ceil(CHUNK_SIZE) * CHUNK_SIZE) as u64
        );
        assert!(
            table.get(past_capacity).is_none(),
            "a row-id past capacity was never stored, so get must return None"
        );
        // A normal row-id still works on the same table.
        table.insert(5, 99).unwrap();
        assert_eq!(table.get(5), Some(&99));
    }

    /// Same scenario as `a_row_id_past_capacity_is_rejected_without_panicking`
    /// above, but with a real `Node` instead of a `u32` -- `u32::reclaim` is
    /// a no-op, so that test cannot exercise (or catch a regression in)
    /// `insert`'s error-path `value.reclaim()` call at all. This one can:
    /// under `cargo miri test`, a real `Node`'s `alloc_node` block would
    /// show up as a leaked allocation if that reclaim call were ever
    /// removed or skipped.
    #[test]
    fn a_row_id_past_capacity_reclaims_a_real_node_instead_of_leaking_it() {
        let table: NodeTable<crate::node::Node> = NodeTable::new(1);
        let past_capacity = 10_000_000_000_u64;
        let node = crate::node::Node::new(0, vec![1.0, 2.0, 3.0], 0, 32, 16);
        let err = table
            .insert(past_capacity, node)
            .expect_err("insert past capacity must be rejected");
        assert!(err.capacity >= MAX_ROW_ID_CAPACITY as u64);
        assert!(
            table.get(past_capacity).is_none(),
            "a row-id past capacity was never stored, so get must return None"
        );
        // A normal insert still works on the same table afterward.
        let ok_node = crate::node::Node::new(5, vec![4.0, 5.0, 6.0], 0, 32, 16);
        table.insert(5, ok_node).unwrap();
        assert_eq!(table.get(5).unwrap().vector(), &[4.0, 5.0, 6.0]);
    }

    /// Not a leak-detector by itself under `cargo test` -- the point of
    /// this test is `cargo miri test -p strata-index --lib
    /// node_table::tests::dropping_a_table_of_real_nodes_frees_every_node
    /// --exact`, which fails with a "memory leaked" diagnostic on the
    /// pre-fix code (no `Drop` for `NodeTable`/`Chunk`, `Node`'s
    /// `alloc_node` block never freed) and passes cleanly once `Reclaim`
    /// is wired through. Spans multiple chunks and multiple layers per
    /// node so both the chunk-directory free path and the multi-layer
    /// `dealloc_node` layout recomputation are exercised, not just the
    /// single-slot/single-layer case.
    #[test]
    fn dropping_a_table_of_real_nodes_frees_every_node() {
        let table: NodeTable<crate::node::Node> = NodeTable::new(3);
        for i in 0..3u64 {
            let row_id = i * CHUNK_SIZE as u64; // forces 3 distinct chunks
            let node = crate::node::Node::new(row_id, vec![1.0, 2.0, 3.0], 2, 32, 16);
            table.insert(row_id, node).unwrap();
        }
        assert_eq!(table.get(0).unwrap().vector(), &[1.0, 2.0, 3.0]);
        drop(table);
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
            let t1 = loom::thread::spawn(move || t1_table.insert(0, 100).unwrap());

            let t2_table = loom::sync::Arc::clone(&table);
            let t2 = loom::thread::spawn(move || t2_table.insert(1, 200).unwrap());

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
