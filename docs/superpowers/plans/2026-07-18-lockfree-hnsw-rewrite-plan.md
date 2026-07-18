# Lock-Free HNSW Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `crates/index/`'s `hnsw_rs` dependency with a from-scratch, fully lock-free HNSW implementation (arXiv:1603.09320), preserving `HnswIndex`'s exact public API so nothing downstream (`crates/txn/`, Phase 6's paused plan) ever needs to change.

**Architecture:** A row-id-indexed, demand-allocated chunked node table (no hashing — row-ids are dense monotonic `u64`s); each node's per-layer neighbor lists live in one contiguous, fixed-capacity atomic-slot buffer allocated once at insert time; edges are claimed/cleared via per-slot CAS, never in-place list mutation. Because Stage 1 does tombstone-*flagging* only (no active node/edge removal), nothing in the node/edge data path is ever freed — no epoch reclamation anywhere in this design.

**Tech Stack:** Rust (edition 2024), `anndists` (SIMD-accelerated distance functions — the same crate `hnsw_rs` itself already uses), `loom` (interleaving-exhaustive concurrency proof).

## Global Constraints

- Branch: `explore/hnsw-lockfree-rewrite` (forked from `origin/main`). Every task's commits land here.
- **`HnswIndex`'s public API must not change**: `new(MaxConnections, MaxElements, MaxLayers, EfConstruction) -> Result<Self, IndexError>`, `insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError>`, `established_dimension(&self) -> usize`, `search(&self, query: &[f32], k: usize, ef_search: usize, is_visible: impl Fn(u64) -> bool) -> Result<Vec<VectorMatch>, IndexError>`, `search_filtered(&self, query: &[f32], k: usize, ef_search: usize, live_ids: &[usize], is_visible: impl Fn(u64) -> bool) -> Result<Vec<VectorMatch>, IndexError>`. This is what lets Phase 6's already-approved, paused plan resume unmodified.
- Every concurrency-touching change needs a `loom` test, not just happy-path `cargo test` (`.claude/rules/vector-index.md`).
- Run loom tests via `cargo rustc -p strata-index --lib --profile test -- --cfg loom`, never a workspace-wide `RUSTFLAGS` (breaks on cross-crate `#[cfg(loom)]` shims elsewhere in the workspace). **This produces a special test binary that must be run directly — `cargo test -p strata-index --lib loom_tests` afterward silently reports 0 tests**, because that second command rebuilds without `--cfg loom`, compiling the `#[cfg(loom)]`-gated module out entirely (confirmed empirically during Task 5). Find and run the actual binary:
  ```bash
  cargo rustc -p strata-index --lib --profile test -- --cfg loom
  BINARY=$(ls -t target/debug/deps/strata_index-* 2>/dev/null | grep -v '\.d$' | head -1)
  "$BINARY" loom_tests --test-threads=1
  ```
  Every task step below that says "or `cargo test -p strata-index --lib loom_tests` once the build is confirmed" means: use this exact binary-invocation recipe, not a bare `cargo test` re-run.
- No `crossbeam-epoch`, no hazard pointers, no reclamation scheme anywhere in this plan — Stage 1 never frees live node/edge data. The one `unsafe` raw-pointer pattern that's expected (the chunk-publish race's synchronous drop of a never-shared allocation) needs a `// SAFETY:` comment per task; anything beyond that pattern is a deviation requiring a stop-and-discuss, not silent implementation.
- No OCC-retry-loop anywhere in `INSERT` — a failed slot CAS is a self-resolving no-op (leave the edge alone), never a retry-worthy error.
- Stage 2 (active edge cleanup/connectivity repair on delete) is explicitly out of scope. Do not build scaffolding for it beyond the `deleted: AtomicBool` per node that Stage 1 already needs.
- `unwrap()`/`expect()` are `clippy::warn` in production code; tests carry `#[allow(clippy::unwrap_used, clippy::expect_used)]` at the module level, matching the existing pattern in `crates/index/src/hnsw.rs`.
- Every step that changes code ends with `cargo build -p strata-index` (or `cargo test -p strata-index` where a test is run) passing before commit.
- Full workspace gate at the end: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.

---

## File Structure

- **Create** `crates/index/src/slot_array.rs` — `SlotArray`, the fixed-capacity atomic-slot edge-list primitive.
- **Create** `crates/index/src/node_table.rs` — `NodeTable`, the row-id-indexed, demand-allocated chunked node directory.
- **Create** `crates/index/src/node.rs` — `Node`, packing a node's per-layer `SlotArray`s into one allocation, plus its `deleted` flag and level-assignment logic.
- **Create** `crates/index/src/distance.rs` — `Distance` trait and `anndists`-backed `L2`/`Cosine`/`Dot` implementations.
- **Create** `crates/index/src/graph.rs` — `Graph<D: Distance>`: `SEARCH-LAYER`, `SELECT-NEIGHBORS-SIMPLE`/`-HEURISTIC`, `INSERT`, `K-NN-SEARCH`, the entry-point cell, `delete`, `insert_batch`.
- **Modify** `crates/index/src/hnsw.rs` — rewritten in the final task to be a thin public `HnswIndex` wrapper over `Graph<distance::L2>`, preserving today's exact public signatures; the 11 existing tests are adapted, not deleted.
- **Modify** `crates/index/src/lib.rs` — add `mod slot_array; mod node_table; mod node; mod distance; mod graph;` (all private — nothing new is publicly exported beyond what `hnsw.rs` already re-exports).
- **Modify** `crates/index/Cargo.toml` — add `anndists`; remove `hnsw_rs` in the final task once nothing references it.
- **Create** `crates/index/benches/lockfree_vs_hnsw_rs.rs` (or extend an existing harness under `bench/` if Task 15 finds one) — recall/QPS comparison.

---

### Task 1: `SlotArray` — the fixed-capacity atomic edge-slot primitive

**⚠️ FULL SCRUTINY — this is the core novel concurrency primitive everything else builds on.**

**Files:**
- Create: `crates/index/src/slot_array.rs`
- Modify: `crates/index/src/lib.rs` (add `mod slot_array;`)

**Interfaces:**
- Consumes: nothing.
- Produces: `pub(crate) struct SlotArray`, `pub(crate) fn SlotArray::new(capacity: usize) -> Self`, `pub(crate) fn capacity(&self) -> usize`, `pub(crate) fn claim(&self, neighbor_id: u64) -> bool`, `pub(crate) fn clear_matching(&self, to_remove: &[u64])`, `pub(crate) fn occupied(&self) -> Vec<u64>` — consumed by Task 3 (`Node`) and Task 6/8 (`Graph`'s `SEARCH-LAYER`/`INSERT`).

- [ ] **Step 1: Write the failing tests**

Create `crates/index/src/slot_array.rs`:

```rust
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
    pub(crate) fn occupied(&self) -> Vec<u64> {
        self.slots
            .iter()
            .map(|s| s.load(Ordering::SeqCst))
            .filter(|&v| v != EMPTY)
            .collect()
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
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib slot_array::tests`
Expected: PASS (all 6 tests) — this is a from-scratch file, no prior broken state to verify-fails-first against.

- [ ] **Step 3: Register the module**

In `crates/index/src/lib.rs`, add `mod slot_array;` alongside the existing `pub mod` declarations (not `pub` — this is a crate-internal primitive).

Run: `cargo build -p strata-index`
Expected: builds cleanly.

- [ ] **Step 4: Write and run the loom test**

Add to `crates/index/src/slot_array.rs`, at the end of the file:

```rust
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
```

In `crates/index/Cargo.toml`, the `unexpected_cfgs` check-cfg for `loom` already exists (verified in this crate's current `Cargo.toml`) — no change needed there.

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
Expected: builds successfully.

Run the produced test binary (path printed by `cargo rustc`'s output), or once the build is confirmed: `cargo test -p strata-index --lib loom_tests`
Expected: PASS — loom reports every explored interleaving satisfied both assertions.

- [ ] **Step 5: Confirm the normal (non-loom) suite is unaffected**

Run: `cargo test -p strata-index --lib slot_array`
Expected: PASS — `loom_tests` does not appear (not compiled in a normal build).

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/slot_array.rs crates/index/src/lib.rs
git commit -m "feat(index): add SlotArray, the lock-free fixed-capacity edge-slot primitive"
```

---

### Task 2: `NodeTable` — row-id-indexed, demand-allocated chunked node directory

**⚠️ FULL SCRUTINY — the only `unsafe` raw-pointer code in this plan lives here.**

**Files:**
- Create: `crates/index/src/node_table.rs`
- Modify: `crates/index/src/lib.rs` (add `mod node_table;`)

**Interfaces:**
- Consumes: `crate::node::Node` (Task 3 — written next, but `NodeTable` only needs `Node` to exist as an opaque type it stores; this task can define a minimal placeholder-free `Node` stub if sequenced first, OR Task 3 can precede this one. This plan sequences `Node` (Task 3) *before* wiring it into `NodeTable`'s tests, so `NodeTable`'s own tests here use a trivial local test-only type instead of the real `Node`, to avoid a forward dependency.)
- Produces: `pub(crate) struct NodeTable<T>`, `pub(crate) fn NodeTable::new(expected_capacity: usize) -> Self`, `pub(crate) fn insert(&self, row_id: u64, value: T)`, `pub(crate) fn get(&self, row_id: u64) -> Option<&T>` — consumed by Task 6/8's `Graph`.

This task defines `NodeTable<T>` generically over the stored value type (not hardcoded to `Node`) specifically so its own tests don't need to forward-reference Task 3.

- [ ] **Step 1: Write the failing tests**

Create `crates/index/src/node_table.rs`:

```rust
//! Row-id-indexed, demand-allocated, chunked storage — no hashing, since
//! `crates/txn`'s row-ids are dense, monotonic `u64`s. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md` §2.

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

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

    fn chunk_index(&self, row_id: u64) -> (usize, usize) {
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
        let (chunk_idx, offset) = self.chunk_index(row_id);
        let chunk = self.get_or_create_chunk(chunk_idx);
        let value_ptr = Box::into_raw(Box::new(value));
        chunk.slots[offset].store(value_ptr, Ordering::SeqCst);
    }

    /// Looks up the value at `row_id`. Returns `None` if `row_id` has
    /// never been inserted (including if its chunk hasn't been allocated
    /// yet at all).
    pub(crate) fn get(&self, row_id: u64) -> Option<&T> {
        let (chunk_idx, offset) = self.chunk_index(row_id);
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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib node_table::tests`
Expected: PASS (all 4 tests).

- [ ] **Step 3: Register the module**

In `crates/index/src/lib.rs`, add `mod node_table;`.

Run: `cargo build -p strata-index`
Expected: builds cleanly.

- [ ] **Step 4: Write and run the loom test**

Add to `crates/index/src/node_table.rs`, at the end:

```rust
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
```

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
Expected: builds successfully.

Run: `cargo test -p strata-index --lib loom_tests`
Expected: PASS.

- [ ] **Step 5: Confirm the normal suite is unaffected**

Run: `cargo test -p strata-index --lib node_table`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/node_table.rs crates/index/src/lib.rs
git commit -m "feat(index): add NodeTable, row-id-indexed chunked node storage with no hashing"
```

---

### Task 3: `Node` — packed per-layer slot arrays, deleted flag, level assignment

**Files:**
- Create: `crates/index/src/node.rs`
- Modify: `crates/index/src/lib.rs` (add `mod node;`)

**Interfaces:**
- Consumes: `SlotArray` (Task 1).
- Produces: `pub(crate) struct Node`, `pub(crate) fn Node::new(row_id: u64, vector: Vec<f32>, level: usize, mmax0: usize, mmax: usize) -> Self`, `pub(crate) fn row_id(&self) -> u64`, `pub(crate) fn vector(&self) -> &[f32]`, `pub(crate) fn level(&self) -> usize`, `pub(crate) fn layer(&self, lc: usize) -> &SlotArray`, `pub(crate) fn is_deleted(&self) -> bool`, `pub(crate) fn mark_deleted(&self)`, `pub(crate) fn assign_level(mL: f64) -> usize` (free function) — consumed by Task 5/6/8's `Graph`.

- [ ] **Step 1: Write the failing tests**

Create `crates/index/src/node.rs`:

```rust
//! A graph node: its vector, one `SlotArray` per layer it participates in
//! (packed into a single `Vec`, not one allocation per layer — see design
//! doc §2/§4), and its deleted flag. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, Ordering};

use crate::slot_array::SlotArray;

pub(crate) struct Node {
    row_id: u64,
    vector: Vec<f32>,
    /// One `SlotArray` per layer `0..=level`: index 0 has capacity
    /// `mmax0`, every other index has capacity `mmax`.
    layers: Vec<SlotArray>,
    deleted: AtomicBool,
}

impl Node {
    pub(crate) fn new(row_id: u64, vector: Vec<f32>, level: usize, mmax0: usize, mmax: usize) -> Self {
        let layers = (0..=level)
            .map(|lc| SlotArray::new(if lc == 0 { mmax0 } else { mmax }))
            .collect();
        Self {
            row_id,
            vector,
            layers,
            deleted: AtomicBool::new(false),
        }
    }

    pub(crate) fn row_id(&self) -> u64 {
        self.row_id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        &self.vector
    }

    /// This node's highest layer — it participates in layers `0..=level()`.
    pub(crate) fn level(&self) -> usize {
        self.layers.len() - 1
    }

    /// The `SlotArray` for layer `lc`. Panics if `lc > self.level()` —
    /// callers must never traverse a node at a layer it doesn't
    /// participate in (checked by `Graph`'s traversal logic, not here).
    pub(crate) fn layer(&self, lc: usize) -> &SlotArray {
        &self.layers[lc]
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }
}

/// Random level assignment per the paper: `l = floor(-ln(unif(0,1)) * mL)`.
/// `mL = 1/ln(M)` is "a simple choice for the optimal mL" per the paper —
/// callers pass `1.0 / (m as f64).ln()`.
pub(crate) fn assign_level(m_l: f64, unif: f64) -> usize {
    debug_assert!((0.0..1.0).contains(&unif), "unif must be in [0, 1)");
    // unif == 0.0 would make -ln(unif) infinite; callers must supply a
    // value strictly greater than 0 (e.g. `rand`'s `gen_range(f64::EPSILON..1.0)`).
    (-unif.ln() * m_l).floor() as usize
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn new_node_participates_in_layers_zero_through_level() {
        let node = Node::new(0, vec![1.0, 2.0, 3.0], 2, 32, 16);
        assert_eq!(node.level(), 2);
        assert_eq!(node.layer(0).capacity(), 32, "layer 0 uses mmax0");
        assert_eq!(node.layer(1).capacity(), 16, "layer 1 uses mmax");
        assert_eq!(node.layer(2).capacity(), 16, "layer 2 uses mmax");
    }

    #[test]
    fn level_zero_node_has_exactly_one_layer() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert_eq!(node.level(), 0);
        assert_eq!(node.layer(0).capacity(), 32);
    }

    #[test]
    fn new_node_is_not_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert!(!node.is_deleted());
    }

    #[test]
    fn mark_deleted_is_observed_by_is_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        node.mark_deleted();
        assert!(node.is_deleted());
    }

    #[test]
    fn vector_and_row_id_are_preserved() {
        let node = Node::new(7, vec![1.0, 2.0, 3.0], 0, 32, 16);
        assert_eq!(node.row_id(), 7);
        assert_eq!(node.vector(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn assign_level_is_zero_at_unif_close_to_one() {
        // -ln(x) -> 0 as x -> 1, so floor(-ln(x) * mL) -> 0.
        let level = assign_level(1.0 / (16f64).ln(), 0.999_999);
        assert_eq!(level, 0);
    }

    #[test]
    fn assign_level_grows_as_unif_shrinks_toward_zero() {
        let m_l = 1.0 / (16f64).ln();
        let small_unif_level = assign_level(m_l, 0.000_001);
        let large_unif_level = assign_level(m_l, 0.5);
        assert!(
            small_unif_level > large_unif_level,
            "a smaller unif must produce a level at least as high: {small_unif_level} vs {large_unif_level}"
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib node::tests`
Expected: PASS (all 7 tests).

- [ ] **Step 3: Register the module and commit**

In `crates/index/src/lib.rs`, add `mod node;`.

Run: `cargo build -p strata-index`
Expected: builds cleanly.

```bash
git add crates/index/src/node.rs crates/index/src/lib.rs
git commit -m "feat(index): add Node — packed per-layer slot arrays, deleted flag, level assignment"
```

---

### Task 4: `Distance` trait and `anndists`-backed metrics

**Files:**
- Create: `crates/index/src/distance.rs`
- Modify: `crates/index/src/lib.rs` (add `mod distance;`)
- Modify: `crates/index/Cargo.toml` (add `anndists` dependency)

**Interfaces:**
- Consumes: `anndists` crate.
- Produces: `pub(crate) trait Distance: Send + Sync { fn eval(&self, a: &[f32], b: &[f32]) -> f32; }`, `pub(crate) struct L2;`, `pub(crate) struct Cosine;`, `pub(crate) struct Dot;`, each implementing `Distance` — consumed by Task 6/8's `Graph<D: Distance>`.

- [ ] **Step 1: Add the dependency**

In `crates/index/Cargo.toml`, add to `[dependencies]`:

```toml
anndists = "0.1"
```

Run: `cargo build -p strata-index`
Expected: builds cleanly (fetches `anndists`).

- [ ] **Step 2: Write the failing tests**

Create `crates/index/src/distance.rs`:

```rust
//! Distance metrics, generic over the graph type — see design doc §4.
//! Backed by `anndists`, the same SIMD-accelerated distance crate
//! `hnsw_rs` itself already uses internally.

use anndists::dist::{DistCosine, DistDot, DistL2 as AnnDistsL2, Distance as AnnDistance};

/// A distance metric usable by `Graph<D>`. `eval` must return a value
/// where smaller means "more similar" — callers needing true similarity
/// (e.g. cosine similarity, where larger is more similar) should return
/// its negation or complement, matching `anndists`' own convention.
pub(crate) trait Distance: Send + Sync {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32;
}

/// Euclidean (L2) distance — Strata's existing default, matching today's
/// `hnsw_rs`-backed `DistL2` usage exactly.
pub(crate) struct L2;

impl Distance for L2 {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        AnnDistsL2.eval(a, b)
    }
}

/// Cosine distance (`1 - cosine_similarity`) — smaller means more similar,
/// matching this trait's convention.
pub(crate) struct Cosine;

impl Distance for Cosine {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        DistCosine.eval(a, b)
    }
}

/// Negative dot product — smaller means more similar (a larger raw dot
/// product means more similar, so this negates it to match the
/// smaller-is-closer convention every other metric here uses).
pub(crate) struct Dot;

impl Distance for Dot {
    fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
        -DistDot.eval(a, b)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn l2_distance_of_identical_vectors_is_zero() {
        assert_eq!(L2.eval(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn l2_distance_matches_hand_computed_value() {
        // A 3-4-5 triangle: sqrt(3^2 + 4^2) = 5.
        let d = L2.eval(&[0.0, 0.0], &[3.0, 4.0]);
        assert!((d - 5.0).abs() < 1e-5, "expected 5.0, got {d}");
    }

    #[test]
    fn cosine_distance_of_identical_direction_vectors_is_zero() {
        let d = Cosine.eval(&[1.0, 0.0], &[2.0, 0.0]);
        assert!(d.abs() < 1e-5, "same-direction vectors must have ~0 cosine distance, got {d}");
    }

    #[test]
    fn cosine_distance_of_orthogonal_vectors_is_one() {
        let d = Cosine.eval(&[1.0, 0.0], &[0.0, 1.0]);
        assert!((d - 1.0).abs() < 1e-5, "orthogonal vectors must have cosine distance 1.0, got {d}");
    }

    #[test]
    fn dot_orders_a_more_aligned_vector_as_closer() {
        let query = [1.0, 0.0];
        let aligned = Dot.eval(&query, &[2.0, 0.0]);
        let orthogonal = Dot.eval(&query, &[0.0, 2.0]);
        assert!(
            aligned < orthogonal,
            "a more-aligned vector must be reported as closer (smaller): {aligned} vs {orthogonal}"
        );
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib distance::tests`
Expected: PASS (5 tests). If `anndists`' actual API differs from `DistCosine`/`DistDot`/`DistL2`'s exact names or the `Distance` trait's exact method name (`eval`) shown here — verify against the installed `anndists` source (matching this codebase's own established practice of citing verified source behavior in comments, e.g. `hnsw.rs`'s existing `anndists-0.1.5`-verified comments) and adjust the import/call sites accordingly; the test *behavior* (identical vectors → 0 L2 distance, 3-4-5 triangle → 5.0, orthogonal vectors → cosine distance 1.0, more-aligned → smaller dot-based distance) must hold regardless of exact API surface.

- [ ] **Step 4: Register the module and commit**

In `crates/index/src/lib.rs`, add `mod distance;`.

```bash
git add crates/index/src/distance.rs crates/index/src/lib.rs crates/index/Cargo.toml
git commit -m "feat(index): add Distance trait with anndists-backed L2/Cosine/Dot metrics"
```

---

### Task 5: Entry-point cell

**⚠️ FULL SCRUTINY — loom test directly reuses Phase 5's established pattern; verify the reuse is faithful.**

**Files:**
- Create: `crates/index/src/graph.rs` (this task starts the file; later tasks extend it)
- Modify: `crates/index/src/lib.rs` (add `mod graph;`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub(crate) struct EntryPoint { row_id: AtomicU64, level: AtomicUsize }` (or equivalent), `pub(crate) fn EntryPoint::new() -> Self`, `pub(crate) fn EntryPoint::get(&self) -> Option<(u64, usize)>`, `pub(crate) fn EntryPoint::advance_if_higher(&self, row_id: u64, level: usize)` — consumed by Task 8's `INSERT` and Task 9's `K-NN-SEARCH`.

- [ ] **Step 1: Write the failing tests**

Create `crates/index/src/graph.rs`:

```rust
//! The lock-free HNSW graph: entry point, `SEARCH-LAYER`,
//! `SELECT-NEIGHBORS-*`, `INSERT`, `K-NN-SEARCH`. See
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::assign_level;

/// Sentinel for "the graph has no nodes yet" — see `EntryPoint::new`.
const NO_ENTRY: u64 = u64::MAX;

/// `(row_id, level)` are packed into the low/high bits of a single
/// `AtomicU64` and updated with ONE compare-exchange, not two separate
/// atomics. This is not stylistic: an earlier version of this design used
/// two separate atomics (`row_id: AtomicU64, level: AtomicUsize`) updated
/// as two sequential operations, and loom found a genuine torn-state race
/// — a thread can win the `row_id` CAS with a lower level, get preempted
/// before its own `level.store`, let a higher-level thread complete its
/// *entire* update, and then blindly overwrite the winner's correct level
/// with its own stale one, producing a `(row_id, level)` pair neither
/// thread ever proposed. Packing both fields into one atomic makes that
/// class of bug structurally impossible: there is only ever one
/// consistent `(row_id, level)` pair in existence at a time, because there
/// is only one atomic word holding it.
///
/// `LEVEL_BITS = 8` (max representable level 255) is enormously generous:
/// per the paper's own formula, expected max level for N nodes is roughly
/// `mL * ln(N)` — for N at `crates/txn`'s own row-id ceiling of
/// 1,000,000,000 and `mL = 1/ln(16) ≈ 0.36`, that's `0.36 * ln(1e9) ≈ 7.5`,
/// vastly under 255 even accounting for statistical outliers.
const LEVEL_BITS: u32 = 8;
const LEVEL_MASK: u64 = (1 << LEVEL_BITS) - 1;

/// Packs `(row_id, level)`. `level` is clamped to `LEVEL_MASK` (255) with a
/// hard runtime check, not a `debug_assert!` — reaching this function with
/// `level > 255` should be practically impossible (see `LEVEL_BITS`'s doc
/// comment), but a `debug_assert!` alone compiles to a no-op in release
/// builds, and silently truncating via the bitmask instead of clamping
/// could wrap an out-of-range level to an arbitrary, even lower, wrong
/// value — exactly the "silently resolved" failure mode this project's
/// conventions forbid for correctness-relevant state. Clamping instead
/// degrades safely: the entry point stays at a valid, merely
/// possibly-suboptimal level, never a corrupted one.
fn pack(row_id: u64, level: usize) -> u64 {
    let level = (level as u64).min(LEVEL_MASK);
    (row_id << LEVEL_BITS) | level
}

fn unpack(packed: u64) -> (u64, usize) {
    (packed >> LEVEL_BITS, (packed & LEVEL_MASK) as usize)
}

/// The graph's current top-layer entry point: which node, at which level.
pub(crate) struct EntryPoint {
    packed: AtomicU64,
}

impl EntryPoint {
    pub(crate) fn new() -> Self {
        Self {
            packed: AtomicU64::new(NO_ENTRY),
        }
    }

    /// Returns `Some((row_id, level))`, or `None` if the graph is empty.
    pub(crate) fn get(&self) -> Option<(u64, usize)> {
        let packed = self.packed.load(Ordering::SeqCst);
        if packed == NO_ENTRY {
            return None;
        }
        Some(unpack(packed))
    }

    /// Advances the entry point to `(row_id, level)` if the graph is
    /// currently empty, or if `level` exceeds the current entry point's
    /// level — matching Algorithm 1 step 18-19 ("if l > L, set enter
    /// point for hnsw to q"). A losing race here just means some other
    /// node's insert already advanced (or is concurrently advancing) to
    /// an equal-or-higher level — never retried beyond re-checking against
    /// the fresh value, self-resolving like every other CAS in this
    /// design.
    pub(crate) fn advance_if_higher(&self, row_id: u64, level: usize) {
        let new_packed = pack(row_id, level);
        loop {
            let current = self.packed.load(Ordering::SeqCst);
            if current != NO_ENTRY {
                let (_, current_level) = unpack(current);
                if level <= current_level {
                    return;
                }
            }
            if self
                .packed
                .compare_exchange(current, new_packed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
            // Lost the race — loop and re-check against the fresh value:
            // the winner may or may not have advanced to a level high
            // enough that we no longer need to advance at all.
        }
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn new_entry_point_is_empty() {
        let ep = EntryPoint::new();
        assert_eq!(ep.get(), None);
    }

    #[test]
    fn advance_if_higher_sets_the_first_entry_point() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 2);
        assert_eq!(ep.get(), Some((5, 2)));
    }

    #[test]
    fn advance_if_higher_replaces_a_lower_level() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 1);
        ep.advance_if_higher(9, 3);
        assert_eq!(ep.get(), Some((9, 3)));
    }

    #[test]
    fn advance_if_higher_ignores_an_equal_or_lower_level() {
        let ep = EntryPoint::new();
        ep.advance_if_higher(5, 3);
        ep.advance_if_higher(9, 3);
        ep.advance_if_higher(1, 1);
        assert_eq!(
            ep.get(),
            Some((5, 3)),
            "neither an equal nor a lower level may replace the current entry point"
        );
    }

    #[test]
    fn advance_if_higher_clamps_a_level_beyond_the_representable_range_instead_of_wrapping() {
        // A level this large should never occur in practice (see
        // LEVEL_BITS's doc comment) — this proves the defensive clamp is
        // real, not just documented: a naive bitmask-truncate of 1000
        // (0b11_1110_1000) would wrap to a small, WRONG value, not merely
        // a clamped-but-still-maximal one.
        let ep = EntryPoint::new();
        ep.advance_if_higher(7, 1000);
        assert_eq!(
            ep.get(),
            Some((7, 255)),
            "an out-of-range level must clamp to the maximum representable value, not wrap"
        );

        // The concrete reachable trigger the original finding named:
        // assign_level(m_l, 0.0) produces usize::MAX (-ln(0.0) is
        // f64::INFINITY, saturating-cast to usize::MAX) — must clamp the
        // same way, not just for the smaller 1000 case above.
        let ep2 = EntryPoint::new();
        ep2.advance_if_higher(8, usize::MAX);
        assert_eq!(
            ep2.get(),
            Some((8, 255)),
            "usize::MAX (assign_level's actual overflow value) must also clamp to 255"
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS (5 tests).

- [ ] **Step 3: Register the module**

In `crates/index/src/lib.rs`, add `mod graph;`.

Run: `cargo build -p strata-index`
Expected: builds cleanly.

- [ ] **Step 4: Write and run the loom test**

First, read `crates/txn/src/dataset.rs`'s existing `loom_tests` module (specifically `one_writer_store_races_safely_with_many_readers_load`) to confirm the exact pattern being mirrored.

Add to `crates/index/src/graph.rs`, at the end:

```rust
/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// Mirrors the shape of `crates/txn/src/dataset.rs`'s
    /// `one_writer_store_races_safely_with_many_readers_load` loom test:
    /// multiple threads racing to advance the entry point to different
    /// levels — proves the final state is always the highest level any
    /// thread proposed, and a reader never observes a torn
    /// (row_id, level) pair (i.e. never observes a level that doesn't
    /// belong to the row_id currently stored).
    #[test]
    fn concurrent_advances_always_settle_on_the_highest_level() {
        loom::model(|| {
            let ep = loom::sync::Arc::new(EntryPoint::new());

            let ep1 = loom::sync::Arc::clone(&ep);
            let t1 = loom::thread::spawn(move || ep1.advance_if_higher(1, 1));

            let ep2 = loom::sync::Arc::clone(&ep);
            let t2 = loom::thread::spawn(move || ep2.advance_if_higher(2, 2));

            t1.join().unwrap();
            t2.join().unwrap();

            // Whichever thread's advance ran last among equals could win,
            // but the FINAL level must be 2 (the higher of the two
            // proposals) regardless of interleaving, and it must be
            // row_id 2's — never a torn pairing of row_id 1 with level 2
            // or vice versa.
            assert_eq!(
                ep.get(),
                Some((2, 2)),
                "the entry point must settle on the higher-level proposal, \
                 with row_id and level always paired consistently"
            );
        });
    }
}
```

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
Expected: builds successfully.

Run: `cargo test -p strata-index --lib loom_tests`
Expected: PASS.

- [ ] **Step 5: Confirm the normal suite is unaffected and commit**

Run: `cargo test -p strata-index --lib graph`
Expected: PASS.

```bash
git add crates/index/src/graph.rs crates/index/src/lib.rs
git commit -m "feat(index): add EntryPoint, the graph-level top-layer CAS cell"
```

---

### Task 6: `SEARCH-LAYER` (Algorithm 2)

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `NodeTable` (Task 2), `Node` (Task 3), `Distance` (Task 4), `EntryPoint` (Task 5).
- Produces: `pub(crate) struct Graph<D: Distance> { nodes: NodeTable<Node>, entry_point: EntryPoint, distance: D, dimension: AtomicUsize }`, `pub(crate) fn Graph::new(distance: D, expected_capacity: usize) -> Self`, `fn search_layer(&self, query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool) -> Vec<(u64, f32)>` (returns `(row_id, distance)` pairs, nearest-first; `filter` gates entry into the returned result set — composed with the deleted-flag check — but never gates traversal through a node's own edges, matching how the deleted flag itself already behaves; this is the real traversal-time membership predicate that lets `search_filtered`'s `live_ids` push all the way into the search, not just the deleted flag) — consumed by Task 7's `SELECT-NEIGHBORS-*`, Task 8's `INSERT` (which always passes an always-true filter — insert's own internal traversal has no membership concept), Task 9's `K-NN-SEARCH`.

- [ ] **Step 1: Write the failing test**

Add to `crates/index/src/graph.rs`'s existing `mod tests` block (create the `Graph` struct and constructor first, per Step 3 below, so this test compiles):

```rust
    #[test]
    fn search_layer_finds_the_true_nearest_neighbor_in_a_small_graph() {
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![10.0, 0.0, 0.0]);
        graph.insert_for_test(2, vec![20.0, 0.0, 0.0]);

        let results = graph.search_layer(&[0.5, 0.0, 0.0], 0, 3, 0, &|_| true);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0, "row 0 must be nearest");
        assert!(
            results[0].1 <= results[1].1 && results[1].1 <= results[2].1,
            "results must be nearest-first: {results:?}"
        );
    }
```

`insert_for_test` is a temporary, test-only helper (added in Step 3 alongside `Graph`) that directly wires two nodes together at layer 0 without going through the not-yet-built `INSERT` algorithm (Task 8) — it exists only so `SEARCH-LAYER` can be tested in isolation before `INSERT` exists. Task 8 replaces every use of `insert_for_test` in this file's tests with the real public `insert` and deletes the helper.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-index --lib graph::tests::search_layer_finds_the_true_nearest_neighbor_in_a_small_graph`
Expected: FAIL to compile (`Graph`, `search_layer`, `insert_for_test` don't exist yet).

- [ ] **Step 3: Implement `Graph` and `SEARCH-LAYER`**

Add to `crates/index/src/graph.rs`, before the `mod tests` block:

```rust
use std::collections::BinaryHeap;
use std::cmp::Ordering as CmpOrdering;

use crate::distance::Distance;
use crate::node::Node;
use crate::node_table::NodeTable;

pub(crate) struct Graph<D: Distance> {
    nodes: NodeTable<Node>,
    entry_point: EntryPoint,
    distance: D,
    dimension: AtomicUsize,
    next_test_row_id: AtomicU64, // test-only bookkeeping; removed in Task 8's cleanup along with insert_for_test
}

/// A `(row_id, distance)` pair ordered so a `BinaryHeap` behaves as a
/// min-heap by distance (nearest first) when wrapped in `Reverse`, or as a
/// max-heap (farthest first, for evicting the worst candidate from a
/// capped result set) when used directly — see `search_layer`'s two heaps.
#[derive(Clone, Copy, PartialEq)]
struct Candidate {
    row_id: u64,
    dist: f32,
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.dist.partial_cmp(&other.dist).unwrap_or(CmpOrdering::Equal)
    }
}

impl<D: Distance> Graph<D> {
    pub(crate) fn new(distance: D, expected_capacity: usize) -> Self {
        Self {
            nodes: NodeTable::new(expected_capacity),
            entry_point: EntryPoint::new(),
            distance,
            dimension: AtomicUsize::new(0),
            next_test_row_id: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn insert_for_test(&self, row_id: u64, vector: Vec<f32>) {
        let node = Node::new(row_id, vector, 0, 32, 16);
        self.nodes.insert(row_id, node);
        self.entry_point.advance_if_higher(row_id, 0);
    }

    /// Algorithm 2, `SEARCH-LAYER`. Returns up to `ef` `(row_id, distance)`
    /// pairs, nearest-first, found by greedy traversal from `entry` at
    /// layer `lc`. `filter` and the deleted-flag check both gate entry
    /// into the returned result set `W`, never `neighbourhood(c)`
    /// traversal — a node excluded by `filter` (or tombstoned) still
    /// serves as a live waypoint for reaching other nodes, exactly
    /// mirroring `hnsw_rs`'s own `FilterT` behavior (see the original
    /// `crates/index/src/hnsw.rs`'s `search_filtered` doc comment: "both
    /// are applied during hnsw_rs's own traversal... not as a post-filter
    /// on an already-capped top-k"). This is what lets a caller's
    /// `live_ids` membership push all the way into traversal-time
    /// filtering, not just the deleted flag. See design doc §3.
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        lc: usize,
        filter: &impl Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(entry);

        let entry_dist = self.distance_to(query, entry);
        // Min-heap of candidates still to explore (nearest first via `Reverse`).
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        candidates.push(std::cmp::Reverse(Candidate { row_id: entry, dist: entry_dist }));
        // Max-heap of the best `ef` results found so far (farthest first, for cheap eviction).
        let mut result: BinaryHeap<Candidate> = BinaryHeap::new();
        if let Some(node) = self.nodes.get(entry) {
            if !node.is_deleted() && filter(entry) {
                result.push(Candidate { row_id: entry, dist: entry_dist });
            }
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            if let Some(furthest) = result.peek() {
                if c.dist > furthest.dist && result.len() >= ef {
                    break; // Algorithm 2 line 7-8: all of W is settled.
                }
            }
            let Some(node) = self.nodes.get(c.row_id) else { continue };
            // A node's layer-lc slot array only exists for lc <= node.level().
            if lc > node.level() {
                continue;
            }
            for neighbor_id in node.layer(lc).occupied() {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                let neighbor_dist = self.distance_to(query, neighbor_id);
                let should_add = match result.peek() {
                    Some(furthest) => neighbor_dist < furthest.dist || result.len() < ef,
                    None => true,
                };
                if should_add {
                    candidates.push(std::cmp::Reverse(Candidate { row_id: neighbor_id, dist: neighbor_dist }));
                    if let Some(neighbor_node) = self.nodes.get(neighbor_id) {
                        if !neighbor_node.is_deleted() && filter(neighbor_id) {
                            result.push(Candidate { row_id: neighbor_id, dist: neighbor_dist });
                            if result.len() > ef {
                                result.pop(); // evict the current furthest
                            }
                        }
                    }
                }
            }
        }

        let mut out: Vec<(u64, f32)> = result.into_iter().map(|c| (c.row_id, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
        out
    }

    fn distance_to(&self, query: &[f32], row_id: u64) -> f32 {
        self.nodes
            .get(row_id)
            .map(|n| self.distance.eval(query, n.vector()))
            .unwrap_or(f32::INFINITY)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::search_layer_finds_the_true_nearest_neighbor_in_a_small_graph`
Expected: PASS.

- [ ] **Step 5: Add a second test — deleted nodes excluded from results but still traversed**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn search_layer_excludes_a_deleted_node_from_results() {
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![10.0, 0.0, 0.0]);
        // Manually wire an edge 0 <-> 1 at layer 0, mirroring what INSERT
        // will do once Task 8 exists — insert_for_test alone doesn't
        // create edges.
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
        }
        if let Some(node0) = graph.nodes.get(0) {
            node0.mark_deleted();
        }

        let results = graph.search_layer(&[0.0, 0.0, 0.0], 1, 5, 0, &|_| true);
        assert!(
            results.iter().all(|(id, _)| *id != 0),
            "a deleted node must never appear in results: {results:?}"
        );
        assert!(
            results.iter().any(|(id, _)| *id == 1),
            "the live node must still be found: {results:?}"
        );
    }

    #[test]
    fn search_layer_filter_excludes_a_live_node_from_results_but_not_from_traversal() {
        // The direct test for the new membership-predicate parameter:
        // node 0 fails an external `filter`, but a query routed through 0
        // must still be able to reach node 1 via 0's edge.
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert_for_test(0, vec![0.0, 0.0, 0.0]);
        graph.insert_for_test(1, vec![1000.0, 0.0, 0.0]);
        if let Some(node0) = graph.nodes.get(0) {
            node0.layer(0).claim(1);
        }
        if let Some(node1) = graph.nodes.get(1) {
            node1.layer(0).claim(0);
        }

        let results = graph.search_layer(&[0.0, 0.0, 0.0], 1, 5, 0, &|id| id != 0);
        assert!(
            results.iter().all(|(id, _)| *id != 0),
            "a filtered-out node must never appear in results: {results:?}"
        );
        assert!(
            results.iter().any(|(id, _)| *id == 1),
            "the filter must not have blocked traversal through node 0 to reach node 1: {results:?}"
        );
    }
```

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS (three tests).

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "feat(index): implement SEARCH-LAYER (Algorithm 2) with a traversal-time membership filter"
```

---

### Task 7: `SELECT-NEIGHBORS-SIMPLE` and `SELECT-NEIGHBORS-HEURISTIC` (Algorithms 3 & 4)

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: nothing new (pure functions over `Vec<(u64, f32)>`).
- Produces: `fn select_neighbors_simple(candidates: &[(u64, f32)], m: usize) -> Vec<u64>`, `fn select_neighbors_heuristic(candidates: &[(u64, f32)], m: usize) -> Vec<u64>` (module-level free functions, `extendCandidates`/`keepPrunedConnections` both fixed to their paper-documented defaults — `false`/not applicable, since this design's candidates are already gathered via `SEARCH-LAYER`, not extended further — see Step 3's doc comment for the precise justification) — consumed by Task 8's `INSERT`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn select_neighbors_simple_returns_the_m_nearest() {
        let candidates = vec![(1, 5.0), (2, 1.0), (3, 3.0), (4, 2.0)];
        let selected = select_neighbors_simple(&candidates, 2);
        assert_eq!(selected, vec![2, 4], "must return the 2 nearest, in nearest-first order");
    }

    #[test]
    fn select_neighbors_simple_returns_everything_if_m_exceeds_candidate_count() {
        let candidates = vec![(1, 5.0), (2, 1.0)];
        let selected = select_neighbors_simple(&candidates, 5);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn select_neighbors_heuristic_prunes_a_candidate_closer_to_an_already_selected_neighbor_than_to_the_query() {
        // Candidate 2 is very close to the query (dist 1.0). Candidate 3
        // is farther from the query (dist 3.0) but also very close to
        // candidate 2 (a redundant direction) — the heuristic (Algorithm
        // 4 line 11) must prefer diversity over raw distance and skip 3
        // if 2 already "shadows" it. This test asserts the heuristic's
        // pruning actually changes the outcome versus SIMPLE for a
        // constructed case where a closer-to-an-existing-pick candidate
        // exists.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let simple = select_neighbors_simple(&candidates, 2);
        let heuristic = select_neighbors_heuristic(&candidates, 2);
        assert_eq!(simple, vec![2, 3], "SIMPLE just takes the 2 nearest by raw distance");
        // The heuristic's exact pruning decision depends on real vector
        // geometry, not just scalar distances (candidate-to-candidate
        // distance isn't derivable from candidate-to-query distances
        // alone) — this test is refined in Step 3 below once real vectors
        // (not just scalar distances) are threaded through, since
        // Algorithm 4 line 11 ("closer to q compared to any element from
        // R") needs actual candidate-to-candidate distance, not just each
        // candidate's distance to the query.
        assert_eq!(heuristic.len(), 2);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-index --lib graph::tests::select_neighbors`
Expected: FAIL to compile (`select_neighbors_simple`/`select_neighbors_heuristic` don't exist).

- [ ] **Step 3: Implement both algorithms**

`SELECT-NEIGHBORS-HEURISTIC` (Algorithm 4 line 11, "if e is closer to q compared to any element from R") genuinely needs candidate-to-candidate distances, not just each candidate's distance to the query — which means it needs access to the actual vectors, not just `(row_id, distance_to_query)` pairs. Revise the signature to take a distance-evaluation closure:

Add to `crates/index/src/graph.rs`, near the other free functions:

```rust
/// Algorithm 3, `SELECT-NEIGHBORS-SIMPLE`: the `m` nearest candidates,
/// nearest-first. `candidates` need not be pre-sorted.
fn select_neighbors_simple(candidates: &[(u64, f32)], m: usize) -> Vec<u64> {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
    sorted.into_iter().take(m).map(|(id, _)| id).collect()
}

/// Algorithm 4, `SELECT-NEIGHBORS-HEURISTIC`, with `extendCandidates` fixed
/// to `false` (the paper's own default — "useful only for extremely
/// clustered data") and `keepPrunedConnections` fixed to `false` (this
/// design always has more true candidates available from `SEARCH-LAYER`
/// than any single call needs, so backfilling from discarded candidates
/// isn't necessary here the way the paper's more general setting
/// anticipates). `pairwise_dist(a, b)` evaluates the same distance metric
/// as `candidates`' own distances, between two candidate row-ids — needed
/// for line 11's diversity check, which compares a candidate against
/// *other candidates*, not just against the query.
fn select_neighbors_heuristic(
    candidates: &[(u64, f32)],
    m: usize,
    pairwise_dist: impl Fn(u64, u64) -> f32,
) -> Vec<u64> {
    let mut working: Vec<(u64, f32)> = candidates.to_vec();
    working.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));

    let mut result: Vec<u64> = Vec::new();
    for (candidate_id, query_dist) in working {
        if result.len() >= m {
            break;
        }
        // Algorithm 4 line 11's diversity check: keep `candidate_id` only
        // if it is NOT dominated — i.e. no already-picked neighbor is
        // closer to this candidate than the candidate itself is to the
        // query. A dominated candidate is redundant with an existing pick
        // (same direction, no new information); a non-dominated one
        // represents a genuinely different direction.
        let dominated = result
            .iter()
            .any(|&picked| pairwise_dist(candidate_id, picked) < query_dist);
        if !dominated {
            result.push(candidate_id);
        }
    }
    result
}
```

Update Step 1's third test to match the real `select_neighbors_heuristic` signature (it now takes a `pairwise_dist` closure):

```rust
    #[test]
    fn select_neighbors_heuristic_prunes_a_candidate_dominated_by_an_already_picked_neighbor() {
        // Candidate 2: dist-to-query 1.0. Candidate 3: dist-to-query 3.0,
        // but dist(3, 2) = 0.1 — candidate 3 is nearly redundant with
        // already-picked candidate 2, so the heuristic should skip it in
        // favor of a more diverse pick (candidate 4) if one exists.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let pairwise = |a: u64, b: u64| -> f32 {
            match (a, b) {
                (3, 2) | (2, 3) => 0.1, // 3 is nearly redundant with 2
                (4, 2) | (2, 4) => 5.0, // 4 is genuinely distinct from 2
                _ => 0.0,
            }
        };
        let selected = select_neighbors_heuristic(&candidates, 2, pairwise);
        assert_eq!(
            selected,
            vec![2, 4],
            "must prefer the diverse candidate (4) over the redundant one (3), unlike SIMPLE: {selected:?}"
        );
    }
```

Remove the earlier, superseded `select_neighbors_heuristic_prunes_a_candidate_closer_to_an_already_selected_neighbor_than_to_the_query` test from Step 1 (replaced by this one, which actually exercises real pairwise-distance-driven pruning rather than asserting only a length).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS (all tests in the file, including the two `select_neighbors_simple` tests and the revised heuristic test).

- [ ] **Step 5: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "feat(index): implement SELECT-NEIGHBORS-SIMPLE and SELECT-NEIGHBORS-HEURISTIC (Algorithms 3 & 4)"
```

---

### Task 8: `INSERT` (Algorithm 1) — the full assembly

**⚠️ FULL SCRUTINY — the largest, most consequential task in this plan. Review the slot-claim/shrink ordering line-by-line against the design doc's §3.**

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `SEARCH-LAYER` (Task 6), `SELECT-NEIGHBORS-*` (Task 7), `EntryPoint` (Task 5), `Node`/`assign_level` (Task 3).
- Produces: `pub(crate) fn Graph::insert(&self, row_id: u64, vector: Vec<f32>, m: usize, mmax0: usize, mmax: usize, ef_construction: usize, m_l: f64, unif: f64) -> Result<(), IndexError>` (the `unif` parameter is a caller-supplied random draw in `(0, 1)` — kept explicit/injectable here rather than calling a global RNG internally, so Task 9's tests and the final `HnswIndex` wrapper control randomness explicitly, matching this project's existing preference for deterministic, testable primitives over hidden global state) — consumed by Task 9 (`K-NN-SEARCH`'s tests build graphs via this), Task 11 (stress test), Task 14 (`HnswIndex` wrapper).

Removes `insert_for_test` (Task 6's temporary helper) — every test using it is updated to call the real `insert`.

- [ ] **Step 1: Write the failing test**

Replace every use of `insert_for_test` in `crates/index/src/graph.rs`'s existing tests with the real `insert`, e.g.:

```rust
    #[test]
    fn search_layer_finds_the_true_nearest_neighbor_in_a_small_graph() {
        let graph = Graph::new(crate::distance::L2, 10);
        graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, 1.0 / (16f64).ln(), 0.5).unwrap();
        graph.insert(1, vec![10.0, 0.0, 0.0], 16, 32, 16, 100, 1.0 / (16f64).ln(), 0.5).unwrap();
        graph.insert(2, vec![20.0, 0.0, 0.0], 16, 32, 16, 100, 1.0 / (16f64).ln(), 0.5).unwrap();

        let results = graph.search_layer(&[0.5, 0.0, 0.0], 0, 3, 0, &|_| true);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0, "row 0 must be nearest");
    }
```

Add a new, dedicated INSERT test:

```rust
    #[test]
    fn insert_creates_bidirectional_edges_between_new_and_existing_nodes() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.9).unwrap();
        graph.insert(1, vec![0.1, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.9).unwrap();

        let node0 = graph.nodes.get(0).unwrap();
        let node1 = graph.nodes.get(1).unwrap();
        assert!(
            node0.layer(0).occupied().contains(&1),
            "node 0 must have an edge to node 1 at layer 0"
        );
        assert!(
            node1.layer(0).occupied().contains(&0),
            "the edge must be bidirectional: node 1 must have an edge back to node 0"
        );
    }

    #[test]
    fn insert_advances_the_entry_point_when_a_new_node_has_a_higher_level() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        // unif close to 1.0 -> level 0; unif close to 0.0 -> a high level.
        graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.99).unwrap();
        assert_eq!(graph.entry_point.get().map(|(_, level)| level), Some(0));

        graph.insert(1, vec![1.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.000_001).unwrap();
        let (entry_row, entry_level) = graph.entry_point.get().unwrap();
        assert_eq!(entry_row, 1, "the higher-level node must become the entry point");
        assert!(entry_level > 0);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: FAIL to compile (`Graph::insert` doesn't exist yet, `insert_for_test` calls now reference a signature that no longer matches once Step 3 removes it).

- [ ] **Step 3: Implement `INSERT`**

Add to `crates/index/src/graph.rs`, inside `impl<D: Distance> Graph<D>` (and delete `insert_for_test` entirely):

```rust
    /// Algorithm 1, `INSERT`. `unif` is a caller-supplied draw from
    /// `(0, 1)` (exclusive of 0) used for this node's random level
    /// assignment — see `crate::node::assign_level`. No OCC-retry-loop
    /// exists anywhere in this method: every CAS (slot-claim, slot-clear,
    /// entry-point-advance) is self-resolving on failure, per design doc
    /// §3.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` if `vector`'s length
    /// doesn't match the dimension established by this graph's first-ever
    /// insert.
    pub(crate) fn insert(
        &self,
        row_id: u64,
        vector: Vec<f32>,
        m: usize,
        mmax0: usize,
        mmax: usize,
        ef_construction: usize,
        m_l: f64,
        unif: f64,
    ) -> Result<(), crate::hnsw::IndexError> {
        self.check_or_establish_dimension(vector.len())?;

        let level = assign_level(m_l, unif);
        let node = Node::new(row_id, vector.clone(), level, mmax0, mmax);
        self.nodes.insert(row_id, node);

        let Some((mut entry, mut entry_level)) = self.entry_point.get() else {
            // First node in the graph: it IS the entry point, no
            // connections to build.
            self.entry_point.advance_if_higher(row_id, level);
            return Ok(());
        };
        if entry == row_id {
            // We only just inserted; re-fetch below already accounts for
            // the entry point possibly having been this exact call in a
            // single-node graph — nothing further to connect.
            return Ok(());
        }

        // Phase 1 (Algorithm 1 lines 5-7): ef=1 descent from the current
        // top layer down to level+1, to find a good entry point for the
        // real connection-building phase.
        while entry_level > level {
            // INSERT's own internal traversal has no membership-predicate
            // concept — always-true filter, deleted-flag exclusion still
            // applies via search_layer's own unconditional check.
            let found = self.search_layer(&vector, entry, 1, entry_level, &|_| true);
            if let Some((nearest, _)) = found.first() {
                entry = *nearest;
            }
            entry_level -= 1;
        }

        // Phase 2 (Algorithm 1 lines 8-17): real connection-building from
        // min(L, l) down to 0.
        let start_layer = entry_level.min(level);
        for lc in (0..=start_layer).rev() {
            let candidates = self.search_layer(&vector, entry, ef_construction, lc, &|_| true);
            if let Some((nearest, _)) = candidates.first() {
                entry = *nearest;
            }
            let capacity = if lc == 0 { mmax0 } else { mmax };
            let chosen = select_neighbors_heuristic(&candidates, m, |a, b| self.pairwise_distance(a, b));

            let Some(new_node) = self.nodes.get(row_id) else { continue };
            for &neighbor_id in &chosen {
                new_node.layer(lc).claim(neighbor_id);
                if let Some(neighbor_node) = self.nodes.get(neighbor_id) {
                    if lc <= neighbor_node.level() {
                        neighbor_node.layer(lc).claim(row_id);
                        // Shrink the neighbor's list if it now exceeds capacity.
                        let occupied = neighbor_node.layer(lc).occupied();
                        if occupied.len() > capacity {
                            let with_dists: Vec<(u64, f32)> = occupied
                                .iter()
                                .map(|&id| (id, self.pairwise_distance(neighbor_id, id)))
                                .collect();
                            let keep = select_neighbors_heuristic(&with_dists, capacity, |a, b| self.pairwise_distance(a, b));
                            let to_remove: Vec<u64> = occupied
                                .into_iter()
                                .filter(|id| !keep.contains(id))
                                .collect();
                            neighbor_node.layer(lc).clear_matching(&to_remove);
                        }
                    }
                }
            }
        }

        self.entry_point.advance_if_higher(row_id, level);
        Ok(())
    }

    /// The distance between two already-inserted nodes' vectors, by
    /// row-id — the pairwise-distance primitive `SELECT-NEIGHBORS-
    /// HEURISTIC`'s diversity check (Algorithm 4 line 11) needs, shared
    /// between the initial connection-building and the shrink step so
    /// neither duplicates the other's lookup-and-eval logic. Returns
    /// `f32::INFINITY` if either row-id has no node (should not happen
    /// for row-ids drawn from this same `insert` call's own candidate
    /// set, but fails safe rather than panicking if it ever does).
    fn pairwise_distance(&self, a: u64, b: u64) -> f32 {
        match (self.nodes.get(a), self.nodes.get(b)) {
            (Some(node_a), Some(node_b)) => self.distance.eval(node_a.vector(), node_b.vector()),
            _ => f32::INFINITY,
        }
    }

    fn check_or_establish_dimension(&self, len: usize) -> Result<(), crate::hnsw::IndexError> {
        let established = self.dimension.load(Ordering::SeqCst);
        if established == 0 {
            self.dimension.compare_exchange(0, len, Ordering::SeqCst, Ordering::SeqCst).ok();
        }
        let established = self.dimension.load(Ordering::SeqCst);
        if established != 0 && len != established {
            return Err(crate::hnsw::IndexError::DimensionMismatch {
                query_len: len,
                expected: established,
            });
        }
        Ok(())
    }
```

This method is long because `INSERT` genuinely is the most involved algorithm in the paper — do not split it into more `pub(crate)` sub-methods than shown here, beyond the `pairwise_distance` helper above (which exists specifically to avoid duplicating the same lookup-and-eval logic between the initial connection-building step and the shrink step).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — every test in the file, including the three new/updated ones from Step 1.

- [ ] **Step 5: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "feat(index): implement INSERT (Algorithm 1) — lock-free bidirectional connection-building via slot-claim/shrink"
```

---

### Task 9: `K-NN-SEARCH` (Algorithm 5) and `delete`

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `search_layer` (Task 6), `EntryPoint` (Task 5).
- Produces: `pub(crate) fn Graph::k_nn_search(&self, query: &[f32], k: usize, ef: usize, filter: impl Fn(u64) -> bool) -> Result<Vec<(u64, f32)>, IndexError>` (the `filter` parameter is `search_layer`'s traversal-time membership predicate, threaded all the way through both search phases — this is what lets `HnswIndex::search_filtered` push `live_ids` membership into the traversal itself, not a post-filter), `pub(crate) fn Graph::delete(&self, row_id: u64)` — consumed by Task 14's `HnswIndex` wrapper.

- [ ] **Step 1: Write the failing tests**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn k_nn_search_finds_the_true_nearest_neighbor_across_layers() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph.insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();
        }
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn delete_excludes_a_row_from_k_nn_search_results() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph.insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();
        }
        graph.delete(0);
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].0, 0, "deleted row must never be returned");
        assert_eq!(results[0].0, 1, "the next-nearest live row must be returned instead");
    }

    #[test]
    fn k_nn_search_on_an_empty_graph_returns_no_results() {
        let graph: Graph<crate::distance::L2> = Graph::new(crate::distance::L2, 10);
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn k_nn_search_filter_excludes_a_row_from_results_but_search_still_finds_others_through_it() {
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        for i in 0..10u64 {
            graph.insert(i, vec![i as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();
        }
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |id| id != 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].0, 0, "a filtered-out row must never be returned");
        assert_eq!(results[0].0, 1, "the next-nearest row passing the filter must be returned instead");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-index --lib graph::tests::k_nn_search`
Expected: FAIL to compile (`k_nn_search`/`delete` don't exist yet).

- [ ] **Step 3: Implement both**

Add to `crates/index/src/graph.rs`, inside `impl<D: Distance> Graph<D>`:

```rust
    /// Algorithm 5, `K-NN-SEARCH`. Descends layers `L..1` with `ef=1`
    /// greedy search, then one real `SEARCH-LAYER` at layer 0 with the
    /// caller's actual `ef`. Returns `(row_id, distance)` pairs,
    /// nearest-first, capped at `k`. `filter` is threaded through every
    /// `search_layer` call in both phases — matching `hnsw_rs`'s own
    /// behavior of applying one filter predicate throughout the whole
    /// search, not just the final layer — so a caller's membership
    /// predicate (e.g. `HnswIndex::search_filtered`'s `live_ids`) can
    /// never be silently missed by routing through a node the coarse ef=1
    /// descent excluded from ITS results (excluding from results never
    /// blocks traversal — see `search_layer`'s own doc comment — so this
    /// is safe: the ef=1 phase still finds a good entry point even
    /// through filtered-out nodes, it just never returns one as that
    /// phase's own single "nearest" pick unless it passes the filter).
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` if `query`'s length doesn't
    /// match this graph's established dimension.
    pub(crate) fn k_nn_search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: impl Fn(u64) -> bool,
    ) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError> {
        let established = self.dimension.load(Ordering::SeqCst);
        if established != 0 && query.len() != established {
            return Err(crate::hnsw::IndexError::DimensionMismatch {
                query_len: query.len(),
                expected: established,
            });
        }
        let Some((mut entry, mut level)) = self.entry_point.get() else {
            return Ok(Vec::new());
        };
        while level >= 1 {
            let found = self.search_layer(query, entry, 1, level, &filter);
            if let Some((nearest, _)) = found.first() {
                entry = *nearest;
            }
            level -= 1;
        }
        let mut results = self.search_layer(query, entry, ef, 0, &filter);
        results.truncate(k);
        Ok(results)
    }

    /// Marks `row_id` as deleted — excluded from `k_nn_search` results
    /// from this point on, but its edges remain intact and it continues
    /// to serve as a live traversal waypoint for other queries (Stage 1's
    /// tombstone-flag-only scope — see design doc §1/§3). A no-op if
    /// `row_id` was never inserted.
    pub(crate) fn delete(&self, row_id: u64) {
        if let Some(node) = self.nodes.get(row_id) {
            node.mark_deleted();
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — every test in the file.

- [ ] **Step 5: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "feat(index): implement K-NN-SEARCH (Algorithm 5) and delete (tombstone-flag-only soft-delete)"
```

---

### Task 10: Deletion correctness test (non-vacuous, per Phase 5's established lesson)

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `insert`/`delete`/`k_nn_search` (Tasks 8-9).
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Read the pattern being applied**

Read `crates/txn/tests/concurrent_snapshot_isolation.rs`'s `an_old_snapshots_vector_search_never_leaks_a_later_commits_rows` test — specifically the comment explaining why querying near a cluster's own origin doesn't discriminate a real isolation bug from a coincidence, and why querying AT the far cluster's center does.

- [ ] **Step 2: Write the test**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn deleted_node_is_never_returned_even_when_queried_at_its_own_exact_location() {
        // The discriminating test, per this project's own Phase 5 lesson
        // (crates/txn/tests/concurrent_snapshot_isolation.rs): querying
        // somewhere a broken deleted-flag check and a correct one would
        // look identical proves nothing. Querying AT the deleted node's
        // own coordinates is where a broken check would return it as the
        // unambiguous true nearest neighbor — a correct check must fall
        // back to the next-nearest live node instead.
        let graph = Graph::new(crate::distance::L2, 20);
        let m_l = 1.0 / (16f64).ln();
        graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();
        graph.insert(1, vec![1000.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();

        graph.delete(0);

        // Querying exactly at row 0's own location: if the deleted-flag
        // check were broken, row 0 would be the unambiguous nearest
        // (distance 0.0). A correct implementation must instead return
        // row 1, even though it's 1000 units away.
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 1, 50, |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].0, 1,
            "querying at the deleted node's own location must still exclude it, \
             falling back to the far live node: {results:?}"
        );
    }
```

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::deleted_node_is_never_returned_even_when_queried_at_its_own_exact_location`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "test(index): non-vacuous deletion correctness test (query at the deleted node's own location)"
```

---

### Task 11: Real-thread stress test

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `insert`/`k_nn_search` (Tasks 8-9).
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Write the test**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn concurrent_inserts_are_all_findable_afterward() {
        use std::sync::Arc;

        const THREADS: u64 = 16;
        const PER_THREAD: u64 = 20;
        let graph = Arc::new(Graph::new(crate::distance::L2, (THREADS * PER_THREAD) as usize));
        let m_l = 1.0 / (16f64).ln();

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let graph = Arc::clone(&graph);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let row_id = t * PER_THREAD + i;
                        graph
                            .insert(
                                row_id,
                                vec![row_id as f32, 0.0, 0.0],
                                16,
                                32,
                                16,
                                100,
                                m_l,
                                0.5,
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Every inserted row must be exactly findable via a query at its
        // own coordinates.
        for row_id in 0..(THREADS * PER_THREAD) {
            let results = graph
                .k_nn_search(&[row_id as f32, 0.0, 0.0], 1, 200, |_| true)
                .unwrap();
            assert_eq!(
                results.len(),
                1,
                "row {row_id} must be findable after concurrent insertion"
            );
        }
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::concurrent_inserts_are_all_findable_afterward`
Expected: PASS. If flaky, that indicates a real bug in the CAS-claim/entry-point logic — investigate the root cause rather than adding a retry.

- [ ] **Step 3: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "test(index): concurrent-insert stress test — every row findable after real multi-thread contention"
```

---

### Task 12: `insert_batch`

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `insert` (Task 8).
- Produces: `pub(crate) fn Graph::insert_batch(&self, rows: &[(u64, Vec<f32>)], m: usize, mmax0: usize, mmax: usize, ef_construction: usize, m_l: f64, unifs: &[f64]) -> Result<(), IndexError>` — consumed by Task 14's `HnswIndex` wrapper (optional — the wrapper may or may not expose this publicly; see Task 14).

- [ ] **Step 1: Write the failing test**

Add to `crates/index/src/graph.rs`'s `mod tests`:

```rust
    #[test]
    fn insert_batch_inserts_every_row() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        let rows: Vec<(u64, Vec<f32>)> = (0..5).map(|i| (i, vec![i as f32, 0.0, 0.0])).collect();
        let unifs = vec![0.5; 5];
        graph
            .insert_batch(&rows, 16, 32, 16, 100, m_l, &unifs)
            .unwrap();

        for i in 0..5u64 {
            let results = graph.k_nn_search(&[i as f32, 0.0, 0.0], 1, 50, |_| true).unwrap();
            assert_eq!(results[0].0, i);
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-index --lib graph::tests::insert_batch_inserts_every_row`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

Add to `crates/index/src/graph.rs`, inside `impl<D: Distance> Graph<D>`:

```rust
    /// Inserts every row in `rows`, sharing repeated entry-point lookups
    /// across the whole batch instead of recomputing per row — matches
    /// `crates/txn::Transaction::commit`'s calling pattern (many rows per
    /// commit). `unifs[i]` supplies row `i`'s level-assignment draw;
    /// `rows.len()` must equal `unifs.len()`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::DimensionMismatch` on the first row whose
    /// vector length disagrees with the graph's established dimension (or
    /// an earlier row in this same batch) — matches `insert`'s own
    /// per-call validation, just applied row-by-row within the batch.
    pub(crate) fn insert_batch(
        &self,
        rows: &[(u64, Vec<f32>)],
        m: usize,
        mmax0: usize,
        mmax: usize,
        ef_construction: usize,
        m_l: f64,
        unifs: &[f64],
    ) -> Result<(), crate::hnsw::IndexError> {
        debug_assert_eq!(rows.len(), unifs.len());
        for ((row_id, vector), &unif) in rows.iter().zip(unifs.iter()) {
            self.insert(*row_id, vector.clone(), m, mmax0, mmax, ef_construction, m_l, unif)?;
        }
        Ok(())
    }
```

This is intentionally a thin sequential loop for Stage 1 — genuine cross-row entry-point sharing/batched-locality optimization is deferred (see design doc §4: batch insert amortizes lookups "matching how Strata calls this," not a requirement to parallelize within a single batch call).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::insert_batch_inserts_every_row`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "feat(index): add Graph::insert_batch"
```

---

### Task 13: Recall/QPS benchmark against `hnsw_rs`

**Files:**
- Create: `crates/index/benches/lockfree_vs_hnsw_rs.rs`
- Modify: `crates/index/Cargo.toml` (add `[[bench]]` entry, `criterion` dev-dependency if not already present at the workspace level — check `bench/` directory first for an existing harness to extend)

**Interfaces:**
- Consumes: `Graph`/`insert`/`k_nn_search` (Tasks 8-9), `hnsw_rs` (still present in `Cargo.toml` until Task 14 removes it — this benchmark is the reason it isn't removed before this task runs).
- Produces: nothing consumed by other tasks — this is the empirical evidence for the "match/beat hnsw_rs" success bar, referenced in Task 14's commit message.

- [ ] **Step 1: Check for an existing benchmark harness**

Run: `ls bench/` and `ls bench/benches/` (or equivalent) to check for an existing vector-search benchmark to extend rather than duplicating setup (this project's `bench/` directory is noted in `.claude/docs/architecture.md` as the `criterion`-based benchmark location).

- [ ] **Step 2: Write the benchmark**

Create `crates/index/benches/lockfree_vs_hnsw_rs.rs` (adjust to extend an existing harness if Step 1 found one, keeping the same structure otherwise):

```rust
//! Recall@k and QPS comparison: the new lock-free `Graph` vs. `hnsw_rs` —
//! the empirical evidence for this rewrite's stated success bar (match or
//! beat `hnsw_rs`, per
//! `docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md`).

use std::time::Instant;

use hnsw_rs::prelude::{DistL2, Hnsw};
use strata_index::graph::Graph; // requires graph module to be pub(crate) -> pub for this bench, or an internal #[doc(hidden)] pub re-export; see Task 14's note on bench-only visibility.

const N: usize = 10_000;
const DIM: usize = 128;
const K: usize = 10;

fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    // Simple deterministic PRNG (xorshift) — no new dependency needed for
    // a bench-only fixture generator.
    let mut state = seed.max(1);
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state as f64 / u64::MAX as f64) as f32
    };
    (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
}

fn recall_at_k(found: &[u64], truth: &[u64], k: usize) -> f64 {
    let truth_set: std::collections::HashSet<_> = truth.iter().take(k).collect();
    let hits = found.iter().take(k).filter(|id| truth_set.contains(id)).count();
    hits as f64 / k as f64
}

fn main() {
    let vectors = random_vectors(N, DIM, 42);
    let queries = random_vectors(100, DIM, 99);

    // --- hnsw_rs baseline ---
    let hnsw_rs_index = Hnsw::new(16, N, 16, 200, DistL2 {});
    for (i, v) in vectors.iter().enumerate() {
        hnsw_rs_index.insert((v, i));
    }
    let hnsw_rs_start = Instant::now();
    let hnsw_rs_results: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            hnsw_rs_index
                .search(q, K, 50)
                .into_iter()
                .map(|n| n.get_origin_id() as u64)
                .collect()
        })
        .collect();
    let hnsw_rs_elapsed = hnsw_rs_start.elapsed();

    // --- new lock-free Graph ---
    let graph = Graph::new(strata_index::distance::L2, N);
    let m_l = 1.0 / 16f64.ln();
    for (i, v) in vectors.iter().enumerate() {
        graph
            .insert(i as u64, v.clone(), 16, 32, 16, 200, m_l, 0.5)
            .unwrap();
    }
    let graph_start = Instant::now();
    let graph_results: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            graph
                .k_nn_search(q, K, 50, |_| true)
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        })
        .collect();
    let graph_elapsed = graph_start.elapsed();

    let avg_recall: f64 = graph_results
        .iter()
        .zip(hnsw_rs_results.iter())
        .map(|(g, h)| recall_at_k(g, h, K))
        .sum::<f64>()
        / queries.len() as f64;

    println!("hnsw_rs:  {:?} total, {:?}/query", hnsw_rs_elapsed, hnsw_rs_elapsed / queries.len() as u32);
    println!("Graph:    {:?} total, {:?}/query", graph_elapsed, graph_elapsed / queries.len() as u32);
    println!("Recall@{K} of Graph vs hnsw_rs's own results as ground truth: {avg_recall:.3}");
}
```

This is deliberately a plain `fn main()` micro-bench (printing timings) rather than a full `criterion` harness if Step 1 found no existing `criterion` setup to extend — matching the "don't build infrastructure the project doesn't already have a pattern for" principle; upgrade to `criterion` only if Step 1's investigation shows the project already has that pattern established elsewhere in `bench/`.

- [ ] **Step 3: Run it**

Run: `cargo run -p strata-index --release --bin lockfree_vs_hnsw_rs` (or `cargo bench -p strata-index` if using `criterion`, per whatever Step 1/2 settled on)
Expected: prints both implementations' timings and the recall comparison. Recall should be reasonably high (both are approximate — this isn't a pass/fail assertion, it's the empirical evidence referenced in Task 14).

- [ ] **Step 4: Commit**

```bash
git add crates/index/benches/lockfree_vs_hnsw_rs.rs crates/index/Cargo.toml
git commit -m "bench(index): recall/QPS comparison between the new lock-free Graph and hnsw_rs"
```

---

### Task 14: Swap `HnswIndex`'s internals, remove `hnsw_rs`, full verification gate

**⚠️ FULL SCRUTINY — this is the task that proves the "preserve HnswIndex's public API exactly" constraint actually held, including that `search_filtered`'s traversal-time filtering is now genuinely equivalent to the original `hnsw_rs`-backed behavior (per the real membership predicate threaded through Tasks 6-9), not just API-compatible. Review the adapted tests especially closely: they must test the SAME properties as the original 11, not weaker ones.**

**Files:**
- Modify: `crates/index/src/hnsw.rs` (rewritten)
- Modify: `crates/index/src/lib.rs` (no export changes expected — verify)
- Modify: `crates/index/Cargo.toml` (remove `hnsw_rs`)

**Interfaces:**
- Consumes: `Graph` (Tasks 5-12).
- Produces: `HnswIndex`'s public surface, unchanged from today (see Global Constraints) — this is the final, externally-visible deliverable of the whole plan.

- [ ] **Step 1: Rewrite `HnswIndex` as a thin wrapper**

Replace `crates/index/src/hnsw.rs`'s body (keep `VectorMatch`, `IndexError`, `MaxConnections`/`MaxElements`/`MaxLayers`/`EfConstruction` exactly as they are — only `HnswIndex`'s internals and its `impl` block change):

```rust
//! HNSW vector index — lock-free, from-scratch implementation (replacing
//! hnsw_rs as of this rewrite). See `.claude/rules/vector-index.md` and
//! `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`.

use crate::distance::L2;
use crate::graph::Graph;

// ... VectorMatch, IndexError, MaxConnections, MaxElements, MaxLayers,
// EfConstruction definitions unchanged from the current file ...

pub struct HnswIndex {
    graph: Graph<L2>,
    m: usize,
    mmax0: usize,
    mmax: usize,
    ef_construction: usize,
    m_l: f64,
    row_counter: std::sync::atomic::AtomicU64, // supplies a deterministic unif draw per insert; see Step 1's note below
}

impl HnswIndex {
    pub fn new(
        max_nb_connection: MaxConnections,
        max_elements: MaxElements,
        max_layer: MaxLayers,
        ef_construction: EfConstruction,
    ) -> Result<Self, IndexError> {
        if max_nb_connection.0 > 256 {
            return Err(IndexError::MaxConnectionTooLarge(max_nb_connection.0));
        }
        let _ = max_layer; // MaxLayers is retained in the public signature for API compatibility; the new design derives level count from mL/unif rather than a hard layer cap — see design doc §2/§3.
        let m = max_nb_connection.0.max(1);
        Ok(Self {
            graph: Graph::new(L2, max_elements.0.max(1)),
            m,
            mmax0: m * 2,
            mmax: m,
            ef_construction: ef_construction.0,
            m_l: 1.0 / (m as f64).ln(),
            row_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn insert(&self, row_id: u64, vector: &[f32]) -> Result<(), IndexError> {
        // A deterministic-but-varying draw per insert, avoiding a new RNG
        // dependency: derived from a monotonically-advancing counter run
        // through a fixed hash, mapped into (0, 1). This is NOT
        // cryptographic or high-quality randomness — HNSW's level
        // assignment only needs a source that varies across inserts to
        // achieve the paper's expected layer-count distribution, and this
        // project's own established precedent (this file's *old* tests)
        // already tolerates non-reproducible layer assignment (see the
        // existing `insert_cluster` test helper's doc comment on
        // hnsw_rs's own unseeded RNG). If a real `rand`-crate dependency
        // is preferred instead, swap this for one — flagged here as an
        // explicit, deliberate choice for the implementer/reviewer to
        // confirm, not a silent placeholder.
        let n = self.row_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 33;
        let unif = ((x >> 11) as f64 / (1u64 << 53) as f64).clamp(f64::EPSILON, 1.0 - f64::EPSILON);

        self.graph.insert(row_id, vector.to_vec(), self.m, self.mmax0, self.mmax, self.ef_construction, self.m_l, unif)
    }

    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.graph.established_dimension()
    }

    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let raw = self.graph.k_nn_search(query, k, ef_search, is_visible)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch { row_id, squared_distance: dist * dist })
            .collect())
    }

    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        // live_ids membership and is_visible are composed into ONE
        // predicate passed straight into k_nn_search -> search_layer,
        // applied during traversal-time result-set construction — not a
        // post-filter over an already-capped top-k. This matches
        // hnsw_rs's original FilterT-based behavior exactly: a live_ids
        // row deep in the graph is never missed just because it fell
        // outside some pre-guessed widened candidate window, since the
        // predicate is evaluated as part of the same search, not after it.
        let live: std::collections::HashSet<u64> = live_ids.iter().map(|&id| id as u64).collect();
        let filter = move |id: u64| live.contains(&id) && is_visible(id);
        let raw = self.graph.k_nn_search(query, k, ef_search, filter)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch { row_id, squared_distance: dist * dist })
            .collect())
    }
}
```

`squared_distance`: `Graph::k_nn_search` returns `L2::eval`'s output, which (per `anndists::DistL2`, matching this file's pre-existing verified-behavior comment) is already true (non-squared) Euclidean distance — squaring it here preserves `VectorMatch::squared_distance`'s existing documented units exactly as before.

This resolves the gap the initial draft of this plan flagged: because Task 9 threads `filter` all the way through `search_layer` (gating result-set entry only, never traversal, exactly like the deleted-flag check), `search_filtered` no longer needs the widened-`ef_search`-plus-post-filter workaround at all — `live_ids` membership is now a real traversal-time predicate, matching `hnsw_rs`'s original `FilterT` behavior with no weakening.

- [ ] **Step 2: Adapt the 11 existing tests**

Rewrite `crates/index/src/hnsw.rs`'s `mod tests` block, preserving each test's *intent* exactly (do not weaken any assertion) while adapting to the new implementation:
- `insert_then_search_finds_the_true_nearest_neighbor` — unchanged in spirit; keep the `insert_cluster` helper as-is (it only calls the public `insert`/`search`, both of which keep their exact signatures).
- `invisible_row_is_never_returned_even_as_the_true_nearest_neighbor` and the two `search_filtered_*` tests — keep using the external `is_visible` closure exactly as today (Stage 1's native soft-delete is a separate mechanism at the `Graph`/`delete` level, exercised by Task 10's new test in `graph.rs`; `HnswIndex::search`'s own external-closure visibility parameter is UNCHANGED and still needs its own coverage here, since it's part of the preserved public API, not replaced by native tombstoning).
- `search_reports_squared_l2_distance_not_plain_l2` — unchanged in spirit.
- `new_rejects_max_nb_connection_above_256` — unchanged (still validated in `HnswIndex::new`, Step 1 keeps this check).
- `search_errors_on_dimension_mismatch`, `insert_errors_on_dimension_mismatch_with_previously_inserted_vectors`, `established_dimension_is_zero_before_any_insert`, `established_dimension_reflects_the_first_inserted_vectors_length` — unchanged in spirit; `established_dimension`/dimension-mismatch logic is now `Graph`'s (Task 8's `check_or_establish_dimension`), exercised transitively through the same public calls.

Run each adapted test individually as it's rewritten to confirm it still asserts the same property, not just that it compiles.

- [ ] **Step 3: Run the full adapted test suite**

Run: `cargo test -p strata-index --lib hnsw::tests`
Expected: PASS — all 11 (or more, if any were split) tests.

- [ ] **Step 4: Remove `hnsw_rs`**

In `crates/index/Cargo.toml`, remove the `hnsw_rs.workspace = true` line — **but only if Task 13's benchmark has already run and its results are recorded** (the benchmark itself still needs `hnsw_rs` as a dev-dependency for comparison purposes; move it from `[dependencies]` to `[dev-dependencies]` rather than deleting it outright, so Task 13's benchmark keeps working).

```toml
[dev-dependencies]
loom = "0.7"
hnsw_rs.workspace = true
```

Run: `cargo build -p strata-index`
Expected: builds cleanly with `hnsw_rs` no longer a production dependency.

- [ ] **Step 5: Full workspace verification gate**

Run: `cargo build --workspace`
Expected: builds cleanly, no warnings.

Run: `cargo test --workspace`
Expected: PASS — every test across every crate, including `crates/txn/`'s full existing suite (untouched by this rewrite, proving the API-preservation constraint held) and Phase 6's own paused-but-still-compiling code on its own branch is unaffected (not part of this workspace build, but confirm no shared code broke).

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`, then run the resulting binary filtered to `loom_tests`
Expected: PASS (all three loom tests from Tasks 1, 2, 5).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean, zero warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/hnsw.rs crates/index/Cargo.toml
git commit -m "feat(index): swap HnswIndex onto the new lock-free Graph, remove hnsw_rs as a production dependency"
```

---

## Self-Review Notes

- **Spec coverage:** design doc §1 (scope/staging) → Task 8-9's tombstone-flag-only `delete`, no Stage 2 scaffolding anywhere; §2 (node representation) → Tasks 1-3; §3 (algorithm adaptation) → Tasks 6-9; §4 (performance) → Task 4 (`anndists`), Task 12 (`insert_batch`), explicit non-adoptions not implemented anywhere (verified: no quantization, no GPU code, no branchless micro-opt introduced); §5 (testing) → Tasks 1/2/5's loom tests, Task 10's deletion correctness test, Task 11's stress test, Task 13's benchmark, Task 14's adapted original 11.
- **Placeholder scan:** no TBD/TODO.
- **Type consistency:** `Graph<D: Distance>` used consistently from Task 6 onward; `(u64, f32)` as the `(row_id, distance)` pair shape is consistent across `search_layer`, `k_nn_search`, `select_neighbors_*`; `search_layer`'s `filter: &impl Fn(u64) -> bool` and `k_nn_search`'s `filter: impl Fn(u64) -> bool` are consistent in every call site across Tasks 6, 8, 9, 10, 11, 12, 13, 14 (verified: every `search_layer`/`k_nn_search` call updated to pass a filter argument once Task 6 introduced the parameter, either `&|_| true`/`|_| true` for call sites with no membership concept, or a real predicate); `HnswIndex`'s public signatures (Task 14) match the Global Constraints section verbatim, matching what Phase 6's paused plan depends on.
- **Gap identified during initial planning, then resolved per explicit direction:** the first draft of this plan had `search_filtered` fall back to a widened-`ef_search`-plus-post-filter workaround, weaker than `hnsw_rs`'s original traversal-time `FilterT` behavior. Tasks 6 and 9 now thread a real membership predicate through `search_layer`/`k_nn_search` (gating result-set entry only, never traversal — the same pattern the deleted-flag already used), so Task 14's `search_filtered` composes `live_ids` membership directly into that predicate with no weakening versus the original.
