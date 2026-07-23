# Single-Allocation HNSW Node Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Model dispatch — read before starting:** This plan touches `unsafe`,
> lock-free, correctness-critical code (`crates/index/`). Per this
> project's `CLAUDE.md` model-dispatch table, do NOT execute this on
> Sonnet 5 by default — escalate to Fable 5 (Opus 4.8 as fallback) for
> Tasks 2, 3, 8, 9, 10 specifically (anything allocating/laying out raw
> memory or introducing new `unsafe`). Tasks 1, 4, 5, 11 are safe-Rust
> refactors/tests and may stay on Sonnet 5. Every task, regardless of
> which model implements it, still needs the standard Opus 4.8 reviewer
> pass before being marked done — this is not optional and is separate
> from the extra soundness review in Task 6.

**Goal:** Collapse strataDB's HNSW node storage from L+4 heap allocations
per node (L = the node's layer level) down to one raw block per node
(Stage A), then remove even that per-insert allocator call via a
chunk-owned bump arena (Stage B).

**Architecture:** `SlotArray` changes from owning `Box<[AtomicU64]>` to
borrowing `&[AtomicU64]` (Task 1, pure safe Rust, zero behavior change).
`Node` becomes a thin, `Copy`, 8-byte pointer wrapper around a manually
laid-out raw block (header + vector + all layers' edges), built via
`std::alloc::Layout` composition (Task 2). `NodeTable<T>` gets exactly one
small, purely additive method (`insert_ptr`) so inserting a pre-allocated
`Node` doesn't cost a second, redundant box (Task 3) — every existing
`NodeTable<T>` method, field, and test is untouched. Stage B (Tasks 8-12)
replaces `Graph`'s `NodeTable<Node>` with a new, purpose-built `NodeArena`
that reuses the same chunk-directory shape but bump-allocates node storage
out of chunk-owned, growable blocks instead of calling the global allocator
per insert.

**Tech Stack:** Rust (this workspace's existing `std::sync::atomic`/`loom`
dual-cfg pattern), no new dependencies.

## Global Constraints

- `unsafe_op_in_unsafe_fn = "deny"` is set workspace-wide — every `unsafe`
  block needs a `// SAFETY:` comment stating the invariant it relies on.
- `cargo build --workspace` clean, `cargo test --workspace` passes,
  `cargo clippy --workspace --all-targets -- -D warnings` clean, and a
  standard Opus 4.8 reviewer pass are required before ANY task is marked
  done (this project's "what done means" gate, `.claude/CLAUDE.md`).
- Every concurrency-touching change needs a `loom` interleaving test, not
  just a happy-path unit test. Run `loom` tests via `cargo rustc -p
  strata-index --lib --profile test -- --cfg loom` then run the resulting
  test binary directly — **never** a workspace-wide `RUSTFLAGS="--cfg
  loom"`, which would break other crates' non-test builds
  (`.claude/rules/concurrency-txn-layer.md`).
- `HnswIndex`'s public method signatures (`insert`, `search`,
  `search_filtered`, `established_dimension`) must never change across
  this entire plan.
- `graph.rs`'s algorithm (`search_layer`, `insert`, `k_nn_search`,
  `select_neighbors_heuristic`) must need zero logic changes — only
  `Node`'s and `NodeTable`'s internals change.
- `SlotArray::EMPTY` is `u64::MAX`, not `0` — never use `alloc_zeroed` for
  slot storage; every slot must be explicitly initialized.
- Nodes are never freed or moved once published — this plan preserves that
  invariant exactly at every task; no `Drop` impl is introduced anywhere.
- Stage B (Tasks 8-12) must not start until Stage A (Tasks 1-7) is fully
  merged and shippable on its own.

---

## Design refinement found during planning (read before Task 3)

The design doc (`docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md`)
states `NodeTable<T>` is "left completely untouched." Planning surfaced one
necessary refinement: `NodeTable::insert(row_id, value: T)` does
`Box::into_raw(Box::new(value))` internally — if `Node` becomes a thin
pointer wrapper around an already-allocated raw block, calling the
existing `insert` would allocate a *second*, redundant box just to hold
that 8-byte wrapper, landing at 2 allocations per node instead of 1. Task 3
adds one new, purely additive method, `NodeTable::insert_ptr`, that stores
an already-obtained raw pointer directly into a chunk slot, skipping the
internal boxing step. This does not modify `insert`, `get`,
`get_or_create_chunk`, `Chunk`, or any existing test — it is a sibling
method. It turns out Stage A itself doesn't need to call it: a `Node`
handle is only 8 bytes once it's a thin pointer wrapper (Task 3), so
`NodeTable::insert`'s existing boxing of that 8-byte value is already
cheap and not worth bypassing. `insert_ptr` is added in Task 3 but first
actually *used* in Task 10 (Stage B), where `NodeArena` uses it to
register a real, large, arena-claimed block without a second box. Every
existing `NodeTable<T>` consumer (including its own `u32`/`u64` tests) is
unaffected either way.

---

# Stage A: Single-Block Node

## Task 1: `SlotArray` becomes a borrowed view

**Files:**
- Modify: `crates/index/src/slot_array.rs`
- Modify: `crates/index/src/node.rs`
- Test: existing tests in both files, adapted (not rewritten)

**Interfaces:**
- Consumes: nothing from other tasks (this is the first task).
- Produces: `SlotArray<'a>` (was `SlotArray`), borrowing `&'a [AtomicU64]`
  instead of owning `Box<[AtomicU64]>`. `Node::layer(&self, lc: usize) ->
  SlotArray<'_>` — same signature shape as today, callers in `graph.rs`
  need zero changes. `Node` internally holds ONE `Vec<AtomicU64>` (all
  layers' slots concatenated) plus a `layer_offsets: Vec<usize>` (one
  offset per layer, computed at construction) instead of `Vec<SlotArray>`.

This task is deliberately still 100% safe Rust — no raw allocation yet.
It's the de-risking step: prove `SlotArray`-as-borrowed-view works and
every existing test still passes before adding any `unsafe`.

- [ ] **Step 1: Change `SlotArray` to borrow a slice**

In `crates/index/src/slot_array.rs`, change:

```rust
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
```

to:

```rust
pub(crate) struct SlotArray<'a> {
    slots: &'a [AtomicU64],
}

impl<'a> SlotArray<'a> {
    pub(crate) fn new(slots: &'a [AtomicU64]) -> Self {
        Self { slots }
    }
```

Every method below `new` (`capacity`, `claim`, `clear_matching`,
`occupied`) is unchanged — they already only read `&self.slots[..]`, which
works identically whether `slots` is a `Box<[AtomicU64]>` or a `&[AtomicU64]`.

- [ ] **Step 2: Update `slot_array.rs`'s own tests to construct via an owned backing `Vec`**

`SlotArray::new` now takes a slice, not a capacity. Update each test to
own its backing storage locally and borrow from it:

```rust
#[test]
fn new_array_has_no_occupied_slots() {
    let backing: Vec<AtomicU64> = (0..4).map(|_| AtomicU64::new(EMPTY)).collect();
    let arr = SlotArray::new(&backing);
    assert_eq!(arr.capacity(), 4);
    assert!(arr.occupied().is_empty());
}
```

Apply the same pattern (`let backing: Vec<AtomicU64> = ...; let arr =
SlotArray::new(&backing);`) to every test in `slot_array.rs`'s `mod tests`
(`claim_fills_an_empty_slot`, `claim_fails_once_every_slot_is_occupied`,
`clear_matching_removes_only_named_values`,
`clear_matching_is_a_noop_for_a_value_not_present`,
`after_clearing_a_slot_can_be_reclaimed`) and the `loom_tests` module's
`concurrent_claim_and_shrink_never_corrupts_a_slot` (there, wrap the
backing `Vec<AtomicU64>` in `loom::sync::Arc` alongside the `SlotArray`
reference, or restructure to have each thread hold an `Arc<Vec<AtomicU64>>`
and construct its own `SlotArray::new(&backing)` view from it before
calling `claim`/`clear_matching`).

- [ ] **Step 3: Run the slot_array tests to confirm they pass**

Run: `cargo test -p strata-index --lib slot_array`
Expected: all tests pass (behavior unchanged, only construction changed).

- [ ] **Step 4: Change `Node` to own one concatenated `Vec<AtomicU64>` instead of `Vec<SlotArray>`**

In `crates/index/src/node.rs`, change:

```rust
pub(crate) struct Node {
    row_id: u64,
    vector: Vec<f32>,
    layers: Vec<SlotArray>,
    deleted: AtomicBool,
}

impl Node {
    pub(crate) fn new(
        row_id: u64,
        vector: Vec<f32>,
        level: usize,
        mmax0: usize,
        mmax: usize,
    ) -> Self {
        let layers = (0..=level)
            .map(|lc| SlotArray::new(if lc == 0 { mmax0 + 1 } else { mmax + 1 }))
            .collect();
        Self {
            row_id,
            vector,
            layers,
            deleted: AtomicBool::new(false),
        }
    }
```

to:

```rust
pub(crate) struct Node {
    row_id: u64,
    vector: Vec<f32>,
    /// All layers' slots concatenated: layer 0's `mmax0+1` slots, then
    /// layer 1's `mmax+1` slots, etc. `layer_offsets[lc]` is where layer
    /// `lc`'s slots start within this Vec.
    slots: Vec<AtomicU64>,
    layer_offsets: Vec<usize>,
    deleted: AtomicBool,
}

impl Node {
    pub(crate) fn new(
        row_id: u64,
        vector: Vec<f32>,
        level: usize,
        mmax0: usize,
        mmax: usize,
    ) -> Self {
        let mut slots = Vec::new();
        let mut layer_offsets = Vec::with_capacity(level + 1);
        for lc in 0..=level {
            layer_offsets.push(slots.len());
            let capacity = if lc == 0 { mmax0 + 1 } else { mmax + 1 };
            slots.extend((0..capacity).map(|_| AtomicU64::new(EMPTY)));
        }
        Self {
            row_id,
            vector,
            slots,
            layer_offsets,
            deleted: AtomicBool::new(false),
        }
    }
```

Add `use crate::slot_array::EMPTY;` to `node.rs`'s imports (`EMPTY` is
already `pub(crate)` in `slot_array.rs`).

- [ ] **Step 5: Update `Node::level` and `Node::layer` for the new representation**

```rust
    pub(crate) fn level(&self) -> usize {
        self.layer_offsets.len() - 1
    }

    /// The `SlotArray` view for layer `lc`. Panics if `lc > self.level()`.
    pub(crate) fn layer(&self, lc: usize) -> SlotArray<'_> {
        let start = self.layer_offsets[lc];
        let end = self
            .layer_offsets
            .get(lc + 1)
            .copied()
            .unwrap_or(self.slots.len());
        SlotArray::new(&self.slots[start..end])
    }
```

- [ ] **Step 6: Run node.rs's tests to confirm they still pass**

Run: `cargo test -p strata-index --lib node::`
Expected: all pass, including `new_node_participates_in_layers_zero_through_level`
(which asserts `node.layer(0).capacity()` etc. — same assertions, same
values, since the slot COUNT per layer is unchanged, only how the slots
are stored changed).

- [ ] **Step 7: Run the full strata-index test suite and the graph.rs suite specifically**

Run: `cargo test -p strata-index --lib`
Expected: PASS, including every `graph::tests` test — `graph.rs` calls
`node.layer(lc).claim(...)` etc. and never constructs a `SlotArray`
itself, so it needs no changes and no test in it should need any edit.

Run: `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add crates/index/src/slot_array.rs crates/index/src/node.rs
git commit -m "refactor(index): make SlotArray a borrowed view instead of an owning box

Prerequisite for the single-allocation node layout: SlotArray's methods
are all &self, so it never needed ownership. Node now stores all its
layers' slots as one concatenated Vec<AtomicU64> instead of N separate
Box<[AtomicU64]>s, cutting per-node allocations from N+3 to 3 as a safe
intermediate step before Task 2's raw single-block layout."
```

---

## Task 2: Compute the raw block `Layout` and write the allocation/initialization function

**Files:**
- Create: `crates/index/src/node_layout.rs`
- Modify: `crates/index/src/lib.rs` (add `mod node_layout;`)
- Test: `crates/index/src/node_layout.rs`'s own `#[cfg(test)]` module

**Interfaces:**
- Consumes: nothing new from Task 1 (this is a standalone layout/alloc
  module; Task 3 wires it into `Node`/`NodeTable`).
- Produces: `pub(crate) struct NodeHeader { row_id: u64, level: u8, mmax0:
  u16, mmax: u16, deleted: AtomicU8 }`; `pub(crate) fn compute_node_layout(dim:
  usize, level: usize, mmax0: usize, mmax: usize) -> (std::alloc::Layout,
  NodeLayoutOffsets)` where `NodeLayoutOffsets { vector_offset: usize,
  layer_offsets: Vec<usize> }`; `pub(crate) unsafe fn alloc_node(row_id:
  u64, vector: &[f32], level: usize, mmax0: usize, mmax: usize) -> *mut
  u8` — allocates, fully initializes, and returns a pointer to the raw
  block. Task 3 consumes `alloc_node`, `NodeHeader`, and
  `NodeLayoutOffsets` directly.

- [ ] **Step 1: Write the failing layout test**

Create `crates/index/src/node_layout.rs`:

```rust
//! Raw single-block layout for a graph node: header, vector, and every
//! layer's edge slots packed into one allocation. See
//! `docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md`.

use std::alloc::Layout;
use std::sync::atomic::AtomicU64;

use crate::slot_array::EMPTY;

#[repr(C)]
pub(crate) struct NodeHeader {
    pub(crate) row_id: u64,
    pub(crate) level: u8,
    pub(crate) mmax0: u16,
    pub(crate) mmax: u16,
    pub(crate) deleted: std::sync::atomic::AtomicU8,
}

pub(crate) struct NodeLayoutOffsets {
    pub(crate) vector_offset: usize,
    /// `layer_offsets[lc]` is the byte offset of layer `lc`'s first slot.
    pub(crate) layer_offsets: Vec<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_node_layout_places_header_then_vector_then_layers_in_order() {
        let (layout, offsets) = compute_node_layout(3, 1, 32, 16);
        // Header must come first (offset 0 by construction of Layout::new::<NodeHeader>()).
        assert!(offsets.vector_offset >= std::mem::size_of::<NodeHeader>());
        // Vector (3 x f32 = 12 bytes) must end at or before layer 0's offset.
        assert!(offsets.layer_offsets[0] >= offsets.vector_offset + 3 * 4);
        // Layer 1 must start after layer 0's 33 slots (mmax0+1 = 33) x 8 bytes.
        assert_eq!(offsets.layer_offsets.len(), 2);
        assert!(offsets.layer_offsets[1] >= offsets.layer_offsets[0] + 33 * 8);
        // The whole layout must be 8-byte aligned (AtomicU64's requirement) at minimum.
        assert_eq!(layout.align() % 8, 0);
    }
}
```

- [ ] **Step 2: Run to confirm it fails (compute_node_layout doesn't exist yet)**

Run: `cargo test -p strata-index --lib node_layout`
Expected: FAIL with "cannot find function `compute_node_layout`"

- [ ] **Step 3: Implement `compute_node_layout` using `Layout` composition**

Add to `crates/index/src/node_layout.rs`, above the `#[cfg(test)]` module:

```rust
pub(crate) fn compute_node_layout(
    dim: usize,
    level: usize,
    mmax0: usize,
    mmax: usize,
) -> (Layout, NodeLayoutOffsets) {
    let mut layout = Layout::new::<NodeHeader>();
    let mut layer_offsets = Vec::with_capacity(level + 1);

    let (extended, vector_offset) = layout
        .extend(Layout::array::<f32>(dim).expect("dim*4 bytes must not overflow isize"))
        .expect("header+vector layout must not overflow isize");
    layout = extended;

    for lc in 0..=level {
        let capacity = if lc == 0 { mmax0 + 1 } else { mmax + 1 };
        let (extended, layer_offset) = layout
            .extend(Layout::array::<AtomicU64>(capacity).expect("slot array bytes must not overflow isize"))
            .expect("layer layout must not overflow isize");
        layout = extended;
        layer_offsets.push(layer_offset);
    }

    (layout.pad_to_align(), NodeLayoutOffsets { vector_offset, layer_offsets })
}
```

- [ ] **Step 4: Run to confirm the test passes**

Run: `cargo test -p strata-index --lib node_layout`
Expected: PASS

- [ ] **Step 5: Write the failing allocation/initialization test**

Add to `node_layout.rs`'s test module:

```rust
    #[test]
    fn alloc_node_initializes_header_vector_and_every_slot_to_empty() {
        let vector = vec![1.0f32, 2.0, 3.0];
        // SAFETY: test-only call, immediately read back and never shared
        // across threads before the read.
        let ptr = unsafe { alloc_node(7, &vector, 1, 32, 16) };
        // SAFETY: `ptr` was just returned by `alloc_node` above and is
        // fully initialized per its own contract.
        unsafe {
            let header = &*ptr.cast::<NodeHeader>();
            assert_eq!(header.row_id, 7);
            assert_eq!(header.level, 1);
            assert_eq!(header.mmax0, 32);
            assert_eq!(header.mmax, 16);
            assert_eq!(header.deleted.load(std::sync::atomic::Ordering::SeqCst), 0);

            let (_, offsets) = compute_node_layout(3, 1, 32, 16);
            let vector_ptr = ptr.add(offsets.vector_offset).cast::<f32>();
            assert_eq!(std::slice::from_raw_parts(vector_ptr, 3), &[1.0, 2.0, 3.0]);

            for &layer_offset in &offsets.layer_offsets {
                let slot_ptr = ptr.add(layer_offset).cast::<AtomicU64>();
                assert_eq!((*slot_ptr).load(std::sync::atomic::Ordering::SeqCst), EMPTY);
            }
        }
        // Deliberately leaked -- matches this crate's existing "nodes are
        // never freed" invariant; see the module doc comment on Drop.
    }
```

- [ ] **Step 6: Run to confirm it fails (alloc_node doesn't exist yet)**

Run: `cargo test -p strata-index --lib node_layout`
Expected: FAIL with "cannot find function `alloc_node`"

- [ ] **Step 7: Implement `alloc_node`**

Add to `node_layout.rs`, above the test module:

```rust
/// Allocates and fully initializes a single raw block for a node: header,
/// vector, and every layer's slots preset to `EMPTY`. The returned
/// pointer is never freed or moved for the caller's process lifetime,
/// matching this crate's existing node/chunk-storage invariant -- there
/// is deliberately no matching `dealloc`/`Drop` anywhere in this crate.
///
/// # Safety
/// The caller must ensure the returned pointer is eventually published
/// (made reachable to other threads) via a `Release`-or-stronger store,
/// since this function performs no synchronization itself -- see
/// `NodeTable::insert_ptr` (Task 3), which is this function's only
/// production caller.
pub(crate) unsafe fn alloc_node(
    row_id: u64,
    vector: &[f32],
    level: usize,
    mmax0: usize,
    mmax: usize,
) -> *mut u8 {
    let (layout, offsets) = compute_node_layout(vector.len(), level, mmax0, mmax);
    // SAFETY: `layout` has non-zero size (NodeHeader alone is non-zero-sized).
    let ptr = unsafe { std::alloc::alloc(layout) };
    assert!(!ptr.is_null(), "node allocation failed (layout: {layout:?})");

    // SAFETY: `ptr` was just allocated with exactly `layout`, which
    // reserves space for one `NodeHeader` at offset 0 with correct
    // alignment (`Layout::new::<NodeHeader>()` is `compute_node_layout`'s
    // first component) -- writing one `NodeHeader` here does not exceed
    // the allocation and does not read the uninitialized memory it
    // overwrites.
    unsafe {
        std::ptr::write(
            ptr.cast::<NodeHeader>(),
            NodeHeader {
                row_id,
                level: u8::try_from(level).expect("level must fit in u8 (see graph.rs LEVEL_MASK)"),
                mmax0: u16::try_from(mmax0).expect("mmax0 must fit in u16"),
                mmax: u16::try_from(mmax).expect("mmax must fit in u16"),
                deleted: std::sync::atomic::AtomicU8::new(0),
            },
        );
    }

    // SAFETY: `offsets.vector_offset` plus `vector.len() * 4` bytes was
    // reserved by `compute_node_layout`'s `Layout::array::<f32>(dim)`
    // extension using this exact `vector.len()`, and the target region
    // does not overlap `vector`'s own backing memory (freshly allocated).
    unsafe {
        std::ptr::copy_nonoverlapping(
            vector.as_ptr(),
            ptr.add(offsets.vector_offset).cast::<f32>(),
            vector.len(),
        );
    }

    // NOT alloc_zeroed: EMPTY is u64::MAX, not 0 -- every slot must be
    // explicitly written, or a zeroed slot would be silently misread as
    // an edge to row-id 0.
    for &layer_offset in &offsets.layer_offsets {
        let capacity_bytes_start = layer_offset;
        let slot_count = if layer_offset == offsets.layer_offsets[0] { mmax0 + 1 } else { mmax + 1 };
        for i in 0..slot_count {
            // SAFETY: each slot's address (`capacity_bytes_start + i *
            // size_of::<AtomicU64>()`) is within the region
            // `compute_node_layout` reserved for this layer via
            // `Layout::array::<AtomicU64>(capacity)`, and no other write
            // targets this address.
            unsafe {
                let slot_ptr = ptr.add(capacity_bytes_start).cast::<AtomicU64>().add(i);
                std::ptr::write(slot_ptr, AtomicU64::new(EMPTY));
            }
        }
    }

    ptr
}
```

- [ ] **Step 8: Run to confirm the test passes**

Run: `cargo test -p strata-index --lib node_layout`
Expected: PASS (both tests)

- [ ] **Step 9: Register the module and run the full crate test suite**

In `crates/index/src/lib.rs`, add `mod node_layout;` alongside the existing
`mod node; mod node_table; mod slot_array;` lines.

Run: `cargo test -p strata-index --lib`
Expected: PASS (no other test should be affected -- this module isn't
wired into `Node`/`Graph` yet, that's Task 3).

Run: `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add crates/index/src/node_layout.rs crates/index/src/lib.rs
git commit -m "feat(index): add raw single-block node layout and allocation

compute_node_layout uses std::alloc::Layout composition to place a
node's header, vector, and every layer's edge slots contiguously.
alloc_node allocates and fully initializes the block -- every slot
explicitly set to EMPTY (u64::MAX), never alloc_zeroed. Not yet wired
into Node/NodeTable/Graph; that's the next task."
```

---

## Task 3: Add `NodeTable::insert_ptr` and rewire `Node` into a thin pointer wrapper

**Files:**
- Modify: `crates/index/src/node_table.rs`
- Modify: `crates/index/src/node.rs`
- Modify: `crates/index/src/graph.rs` (only the two `Node::new(...)` call
  sites and the `NodeTable::insert(...)` call site, if any signature
  shape changed -- see Step 4)

**Interfaces:**
- Consumes: `NodeHeader`, `compute_node_layout`, `alloc_node` from Task 2.
- Produces: `NodeTable::<T>::insert_ptr(&self, row_id: u64, ptr: *mut T)`
  (new, additive, sibling to the existing `insert`). `Node` becomes
  `pub(crate) struct Node(std::ptr::NonNull<u8>)` (a thin, `Copy`, 8-byte
  wrapper). `Node::new(row_id, vector, level, mmax0, mmax) -> Node` keeps
  its exact existing signature and now calls `alloc_node` internally.
  `Node::layer`, `Node::level`, `Node::is_deleted`, `Node::mark_deleted`,
  `Node::vector`, `Node::row_id` keep their exact existing signatures.

- [ ] **Step 1: Write the failing test for `NodeTable::insert_ptr`**

Add to `crates/index/src/node_table.rs`'s `mod tests`:

```rust
    #[test]
    fn insert_ptr_then_get_round_trips() {
        let table: NodeTable<u32> = NodeTable::new(100);
        let boxed: *mut u32 = Box::into_raw(Box::new(42u32));
        table.insert_ptr(5, boxed);
        assert_eq!(table.get(5), Some(&42));
    }
```

- [ ] **Step 2: Run to confirm it fails**

Run: `cargo test -p strata-index --lib node_table::tests::insert_ptr_then_get_round_trips`
Expected: FAIL with "no method named `insert_ptr`"

- [ ] **Step 3: Implement `insert_ptr`**

In `crates/index/src/node_table.rs`, add this method to `impl<T>
NodeTable<T>`, right after the existing `insert`:

```rust
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
    /// the table's lifetime (this table never reclaims it, matching
    /// every other pointer this table stores).
    pub(crate) unsafe fn insert_ptr(&self, row_id: u64, ptr: *mut T) {
        let (chunk_idx, offset) = Self::chunk_index(row_id);
        let chunk = self.get_or_create_chunk(chunk_idx);
        chunk.slots[offset].store(ptr, Ordering::SeqCst);
    }
```

Note this makes `insert_ptr` `unsafe` (unlike safe `insert`, since the
caller — not this table — is responsible for the pointer's validity).
Update the Step 1 test call site accordingly:

```rust
    #[test]
    fn insert_ptr_then_get_round_trips() {
        let table: NodeTable<u32> = NodeTable::new(100);
        let boxed: *mut u32 = Box::into_raw(Box::new(42u32));
        // SAFETY: `boxed` is non-null, points to a validly initialized
        // u32, and is never freed elsewhere in this test.
        unsafe { table.insert_ptr(5, boxed) };
        assert_eq!(table.get(5), Some(&42));
    }
```

- [ ] **Step 4: Run to confirm the test passes, and existing NodeTable tests are unaffected**

Run: `cargo test -p strata-index --lib node_table::`
Expected: PASS, including every pre-existing test (`get_on_an_empty_table_
returns_none`, `insert_then_get_round_trips`,
`row_ids_spanning_multiple_chunks_all_round_trip`, and the loom test) —
none of them are touched by this addition.

- [ ] **Step 5: Rewrite `Node` as a thin pointer wrapper**

In `crates/index/src/node.rs`, replace the whole struct/impl with:

```rust
//! A graph node: its vector, one slot region per layer it participates
//! in, and its deleted flag, all packed into a single raw allocation. See
//! `docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md`.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::node_layout::{NodeHeader, alloc_node, compute_node_layout};
use crate::slot_array::SlotArray;

/// A thin, `Copy` handle to a single raw-allocated node block. Never
/// freed or moved once constructed -- see `alloc_node`'s doc comment.
#[derive(Clone, Copy)]
pub(crate) struct Node(std::ptr::NonNull<u8>);

// SAFETY: every field this type's methods read is either immutable after
// construction (header's row_id/level/mmax0/mmax, the vector bytes) or an
// atomic (deleted flag, edge slots) -- concurrent shared access through
// `&Node`/`Node` (it's `Copy`) is exactly what those atomics are for.
unsafe impl Send for Node {}
// SAFETY: same reasoning as the `Send` impl above.
unsafe impl Sync for Node {}

impl Node {
    pub(crate) fn new(row_id: u64, vector: Vec<f32>, level: usize, mmax0: usize, mmax: usize) -> Self {
        // SAFETY: `alloc_node`'s only contract is that its result is
        // published via a synchronizing store before any other thread
        // reads it -- the caller (`NodeTable::insert_ptr`, via
        // `Graph::insert`) does so through `AtomicPtr::store`.
        let ptr = unsafe { alloc_node(row_id, &vector, level, mmax0, mmax) };
        Self(std::ptr::NonNull::new(ptr).expect("alloc_node never returns null (it asserts internally)"))
    }

    fn header(&self) -> &NodeHeader {
        // SAFETY: `self.0` was produced by `alloc_node`, which reserves
        // and initializes a `NodeHeader` at offset 0 (see
        // `compute_node_layout`) before returning; this `Node` is never
        // constructed from any other pointer.
        unsafe { &*self.0.as_ptr().cast::<NodeHeader>() }
    }

    #[allow(dead_code)]
    pub(crate) fn row_id(&self) -> u64 {
        self.header().row_id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        let header = self.header();
        let (_, offsets) = compute_node_layout(0, 0, 0, 0); // placeholder, replaced below
        let _ = offsets;
        // Vector length isn't stored in the header (it equals the
        // graph's established dimension, a Graph-level constant) -- see
        // Step 6 below for how callers supply it.
        unimplemented!("see Step 6: vector() takes dim as a parameter")
    }

    pub(crate) fn level(&self) -> usize {
        self.header().level as usize
    }

    pub(crate) fn layer(&self, lc: usize) -> SlotArray<'_> {
        let header = self.header();
        assert!(lc <= header.level as usize, "layer {lc} requested but node's level is {}", header.level);
        let (_, offsets) = compute_node_layout(0, header.level as usize, header.mmax0 as usize, header.mmax as usize);
        let start = offsets.layer_offsets[lc];
        let end = offsets
            .layer_offsets
            .get(lc + 1)
            .copied()
            .unwrap_or_else(|| start + Self::layer_capacity(header, lc) * std::mem::size_of::<AtomicU64>());
        // SAFETY: [start, end) was reserved for layer `lc`'s slots by
        // `alloc_node`'s call to the same `compute_node_layout`, and
        // every slot in it was explicitly initialized to `EMPTY`.
        let slots = unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().add(start).cast::<AtomicU64>(), (end - start) / std::mem::size_of::<AtomicU64>())
        };
        SlotArray::new(slots)
    }

    fn layer_capacity(header: &NodeHeader, lc: usize) -> usize {
        if lc == 0 { header.mmax0 as usize + 1 } else { header.mmax as usize + 1 }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.header().deleted.load(Ordering::SeqCst) != 0
    }

    #[allow(dead_code)]
    pub(crate) fn mark_deleted(&self) {
        self.header().deleted.store(1, Ordering::SeqCst);
    }
}
```

This step is intentionally left with a placeholder `unimplemented!()` in
`vector()` -- Step 6 resolves it. `compute_node_layout`'s `dim` parameter
being needed at every `layer()`/`vector()` call, but not stored in the
header, is the specific problem Step 6 fixes.

- [ ] **Step 6: Store `dim` in the header too, and finish `vector()`**

Storing `dim` in the header avoids threading it as a parameter through
every `Node` method (mirroring why `mmax0`/`mmax` are stored there — see
the design doc's §3.1 note). Update `crates/index/src/node_layout.rs`'s
`NodeHeader`:

```rust
#[repr(C)]
pub(crate) struct NodeHeader {
    pub(crate) row_id: u64,
    pub(crate) dim: u32,
    pub(crate) level: u8,
    pub(crate) mmax0: u16,
    pub(crate) mmax: u16,
    pub(crate) deleted: std::sync::atomic::AtomicU8,
}
```

Update `alloc_node` to write `dim: u32::try_from(vector.len()).expect("dim
must fit in u32")` into the header. Update `node_layout.rs`'s two tests
(Task 2, Steps 1 and 5) to add `dim: 3,`/assert `header.dim == 3` matching
their existing 3-element vectors.

Then in `node.rs`, replace `vector()`, `layer()`, and `layer_capacity`
with versions that read `dim` from the header instead of taking a
placeholder:

```rust
    pub(crate) fn vector(&self) -> &[f32] {
        let header = self.header();
        let (_, offsets) = compute_node_layout(header.dim as usize, header.level as usize, header.mmax0 as usize, header.mmax as usize);
        // SAFETY: [vector_offset, vector_offset + dim*4) was reserved and
        // initialized by `alloc_node` using this same `header.dim`.
        unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().add(offsets.vector_offset).cast::<f32>(), header.dim as usize)
        }
    }

    pub(crate) fn layer(&self, lc: usize) -> SlotArray<'_> {
        let header = self.header();
        assert!(lc <= header.level as usize, "layer {lc} requested but node's level is {}", header.level);
        let (_, offsets) = compute_node_layout(header.dim as usize, header.level as usize, header.mmax0 as usize, header.mmax as usize);
        let start = offsets.layer_offsets[lc];
        let capacity = Self::layer_capacity(header, lc);
        let end = start + capacity * std::mem::size_of::<AtomicU64>();
        // SAFETY: [start, end) is exactly the byte range `alloc_node`
        // reserved and initialized (every slot set to EMPTY) for layer
        // `lc`, per the same `compute_node_layout` call.
        let slots = unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().add(start).cast::<AtomicU64>(), capacity)
        };
        SlotArray::new(slots)
    }
```

Delete the placeholder body from Step 5 entirely (no `unimplemented!()`
should remain).

- [ ] **Step 7: Remove Task 1's now-superseded `Vec<AtomicU64>`/`layer_offsets` fields**

Task 1 gave `Node` a safe intermediate representation
(`slots: Vec<AtomicU64>` + `layer_offsets: Vec<usize>`). This task's Step 5
already replaced the whole struct with the thin pointer wrapper, so this
step is a checkpoint, not new work: confirm no leftover references to the
Task-1-era fields remain anywhere in `node.rs` or its tests.

Run: `cargo build -p strata-index --lib 2>&1 | grep -i "slots\|layer_offsets"`
Expected: no matches referencing the old `Node` fields (matches inside
`node_layout.rs`'s `NodeLayoutOffsets.layer_offsets` are fine — that's a
different, still-current type).

- [ ] **Step 8: Update `node.rs`'s own tests for the new construction/assertions**

`new_node_participates_in_layers_zero_through_level`,
`level_zero_node_has_exactly_one_layer`, `new_node_is_not_deleted`,
`mark_deleted_is_observed_by_is_deleted`, `vector_and_row_id_are_preserved`
should all still compile and pass against the new `Node` unchanged in
their assertions (only `Node`'s internals changed, not its public
`pub(crate)` method behavior). `assign_level`'s tests are untouched (that
free function doesn't touch `Node` at all).

Run: `cargo test -p strata-index --lib node::tests`
Expected: PASS, no test body edits needed beyond confirming they compile.

- [ ] **Step 9: Confirm `graph.rs` needs no call-site changes**

`Graph::nodes` stays `NodeTable<Node>`, still using the existing, safe
`insert(row_id, node)` — **not** `insert_ptr`. `Node` is now just an
8-byte `Copy` pointer wrapper (Step 5), so `NodeTable::insert`'s one
`Box::into_raw(Box::new(node))` call boxes a single pointer-sized value:
cheap, fixed-size, and not the allocation this task exists to eliminate.
The expensive, variable-size, scattered allocations (vector, layers,
per-layer slot arrays) already live in the ONE block `Node::new`
allocates via `alloc_node` (Step 5) — `insert`'s remaining box is just a
handle to that block, not a second copy of its contents. `insert_ptr`
(Task 3, Step 3) stays reserved for Task 10's `NodeArena`, where it
removes a real, large allocation instead of an 8-byte one.

Confirm by inspection: `crates/index/src/graph.rs`'s `Graph::insert`
still reads `self.nodes.insert(row_id, node);` exactly as it does before
this task — no diff expected in `graph.rs` from this task at all.

- [ ] **Step 10: Run the full workspace gate**

Run: `cargo build --workspace`
Expected: clean.

Run: `cargo test -p strata-index --lib`
Expected: all pass, including every `graph::tests` test (no changes
needed there per Step 9's resolution) and every `node::tests`/
`node_layout::tests`/`slot_array` test.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add crates/index/src/node.rs crates/index/src/node_layout.rs crates/index/src/node_table.rs
git commit -m "feat(index): back Node with a single raw allocation

Node is now a thin, Copy, 8-byte pointer wrapper around a single
alloc_node()-produced block containing its header (including dim,
needed since vector()/layer() must locate fields without a separate
parameter), vector, and every layer's edge slots. Per-node allocation
count drops from N+3 (post-Task-1 baseline) to 2: the block itself,
plus NodeTable::insert's existing box of the 8-byte Node handle --
unavoidable without changing NodeTable's insert contract, and
negligible next to the eliminated variable-size allocations.
NodeTable::insert_ptr (added but not yet used here) is reserved for
Stage B, where it removes a real, large allocation instead of a
fixed 8-byte one."
```

**Note on the design doc's "L+4 to 1" framing:** this task reaches 2
allocations per node (the raw block + `NodeTable::insert`'s own boxing of
the 8-byte `Node` handle), not literally 1, because changing
`NodeTable<T>::insert`'s internal contract was ruled out to keep
`NodeTable<T>` genuinely untouched for its other consumers. This still
fully achieves the design's actual goal (eliminating the fragmented,
variable-size, hot-path-adjacent allocations) — `search_layer`'s hot loop
never touches the small fixed-size wrapper box, only the one big
contiguous block. Stage B's `NodeArena` (Task 10) is where true
single-allocation-per-node is reached, using `insert_ptr` for real.

---

## Task 4: Loom test — full-node publish visibility

**Files:**
- Modify: `crates/index/src/node.rs` (add a `#[cfg(loom)] mod loom_tests`)

**Interfaces:**
- Consumes: `Node::new`, `Node::layer`, `Node::vector`, `Node::level` from
  Task 3.
- Produces: nothing new (test-only).

- [ ] **Step 1: Write the failing loom test**

Add to `crates/index/src/node.rs`:

```rust
/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
/// (never a workspace-wide `RUSTFLAGS`).
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// A thread that only observes a node through a published `AtomicPtr`
    /// must see the WHOLE node fully initialized -- header, vector, and
    /// every slot preset to EMPTY -- never a partially-constructed node.
    /// This is the test most likely to catch a release/acquire ordering
    /// mistake in alloc_node's publication path.
    #[test]
    fn full_node_publish_is_completely_visible_to_a_reader() {
        loom::model(|| {
            let published: loom::sync::Arc<loom::sync::atomic::AtomicPtr<Node>> =
                loom::sync::Arc::new(loom::sync::atomic::AtomicPtr::new(std::ptr::null_mut()));

            let writer_published = loom::sync::Arc::clone(&published);
            let writer = loom::thread::spawn(move || {
                let node = Node::new(7, vec![1.0, 2.0, 3.0], 1, 32, 16);
                let boxed = Box::into_raw(Box::new(node));
                writer_published.store(boxed, loom::sync::atomic::Ordering::SeqCst);
            });

            let reader_published = loom::sync::Arc::clone(&published);
            let reader = loom::thread::spawn(move || {
                let ptr = reader_published.load(loom::sync::atomic::Ordering::SeqCst);
                if !ptr.is_null() {
                    // SAFETY: a non-null `ptr` was published by the
                    // writer's store above, after Node::new's alloc_node
                    // call fully returned.
                    let node = unsafe { &*ptr };
                    assert_eq!(node.vector(), &[1.0, 2.0, 3.0]);
                    assert_eq!(node.level(), 1);
                    assert!(!node.is_deleted());
                    for lc in 0..=node.level() {
                        assert!(node.layer(lc).occupied().is_empty());
                    }
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
}
```

- [ ] **Step 2: Run under loom to confirm it currently passes**

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
then run the produced test binary (path printed by `rustc`, typically
`target/debug/deps/strata_index-<hash>.exe`) filtered to
`node::loom_tests::full_node_publish_is_completely_visible_to_a_reader`.

Expected: PASS. (This test is expected to already pass given `SeqCst` is
used throughout — its value is as a regression guard for any *future*
ordering relaxation, not as a currently-failing repro. If it fails, that
is a real bug in Task 2/3's implementation and must be fixed before
proceeding — do not weaken the test.)

- [ ] **Step 3: Commit**

```bash
git add crates/index/src/node.rs
git commit -m "test(index): loom coverage for full-node publish visibility

Proves a reader observing a Node only through a published AtomicPtr
always sees the whole node fully initialized, never partial -- the
regression guard for alloc_node's publication path."
```

---

## Task 5: Real-thread stress test against the new storage

**Files:**
- Modify: `crates/index/src/graph.rs` (add one test to the existing
  `#[cfg(all(test, not(loom)))] mod tests`)

**Interfaces:**
- Consumes: `Graph::insert`, `Graph::k_nn_search` (unchanged signatures).
- Produces: nothing new (test-only).

- [ ] **Step 1: Add a concurrent-insert stress test mirroring the existing one**

`graph.rs` already has `concurrent_inserts_are_all_findable_afterward`
(16 threads × 20 inserts each, then every row queried back). Add a second
version that also exercises re-reading vectors, since Task 3 changed how
`vector()` locates its data (via the header's `dim` field).

**Scope note (recorded during execution):** the brief's own draft title
below mentioned deletion, but its body and this design's actual testing
requirement (`docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md`,
testing strategy section) only ask for post-establishment concurrent
insert + vector-content read-back, not concurrent deletion — single-
threaded deletion is already covered elsewhere in `graph.rs`
(`search_layer_excludes_a_deleted_node_from_results` etc.). Task 5's
implementer correctly built to the actual requirement and renamed the
test to `concurrent_inserts_after_dimension_established_are_findable_and_vector_readable`
rather than the mismatched draft title below — treat the code sample
below as illustrative of the ORIGINAL (flawed) sketch, superseded by
what actually shipped:

```rust
    #[test]
    fn concurrent_inserts_of_varying_dimension_vectors_error_without_corrupting_existing_nodes() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new(crate::distance::L2, 100));
        let m_l = 1.0 / (16f64).ln();

        // Establish dimension 3 first, single-threaded, so the assertion
        // below is deterministic.
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 1.0, test_unif(0))
            .unwrap();

        let handles: Vec<_> = (1..17)
            .map(|row_id| {
                let graph = Arc::clone(&graph);
                std::thread::spawn(move || {
                    graph.insert(row_id, vec![row_id as f32, 0.0, 0.0], 16, 32, 16, 100, m_l, 1.0, test_unif(row_id))
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        for row_id in 0..17u64 {
            let results = graph.k_nn_search(&[row_id as f32, 0.0, 0.0], 1, 200, |_| true).unwrap();
            assert_eq!(results.len(), 1, "row {row_id} must be findable after concurrent insertion into single-block storage");
            assert_eq!(results[0].0, row_id);
        }
    }
```

(Use whatever this file's existing `test_unif` helper's signature already
is — it's referenced by the pre-existing `concurrent_inserts_are_all_
findable_afterward` test in this same file.)

- [ ] **Step 2: Run to confirm it passes**

Run: `cargo test -p strata-index --lib graph::tests::concurrent_inserts_of_varying_dimension_vectors_error_without_corrupting_existing_nodes`
Expected: PASS

- [ ] **Step 3: Run the pre-existing stress test too, to confirm no regression**

Run: `cargo test -p strata-index --lib graph::tests::concurrent_inserts_are_all_findable_afterward`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "test(index): concurrent stress test against single-block node storage"
```

---

## Task 6: Dedicated soundness review of the unsafe allocation/initialization code

**Files:** none (review-only task, no code changes unless the review
finds a real issue, in which case fix it and re-run Task 2-5's tests).

This is the extra gate the design doc's §3.3 calls for, separate from the
standard Task 7 reviewer pass — specifically focused on `unsafe`
correctness in `node_layout.rs` and `node.rs`, not general code quality.

- [ ] **Step 1: Generate a diff package for Tasks 1-5**

```bash
git log --oneline main..HEAD
```

Identify the commit range covering Tasks 1-5 (the `SlotArray`-as-view
refactor through the stress test).

- [ ] **Step 2: Dispatch a Fable-5 (or Opus 4.8 if unavailable) soundness review**

Prompt must ask the reviewer to verify, independently, against the actual
diff (not just read the plan):
- Every `unsafe` block's `// SAFETY:` comment states a real invariant, and
  that invariant is actually upheld by the surrounding code (not just
  plausible-sounding).
- `compute_node_layout`'s `Layout` composition can never produce an
  under-sized allocation for any `(dim, level, mmax0, mmax)` combination
  `alloc_node` is ever called with in production (cross-check against
  `graph.rs`'s actual call sites' value ranges).
- Every byte range `Node::layer`/`Node::vector` compute via
  `compute_node_layout` exactly matches what `alloc_node` reserved and
  initialized for the same `(dim, level, mmax0, mmax)` — no off-by-one,
  no reliance on two separate `compute_node_layout` calls (one at alloc
  time, one at access time) silently producing different offsets if
  header fields are ever read incorrectly.
- The `EMPTY`-slot initialization loop in `alloc_node` covers every slot
  in every layer with no gap and no double-write.
- `Node`'s `Send`/`Sync` unsafe impls are justified by what the type
  actually guarantees (every field access goes through an atomic or is
  immutable post-construction) — not merely asserted.
- No path exists where a `Node` handle becomes reachable to a second
  thread before `alloc_node` has finished every write (the actual
  publication-safety property Task 4's loom test targets).

- [ ] **Step 3: Address every finding**

Fix any confirmed issue; do not mark this task done with an open,
confirmed-real finding.

---

## Task 7: Stage A completion gate

**Files:** none (verification-only).

- [ ] **Step 1: Full workspace gate**

Run: `cargo build --workspace` — expect clean.
Run: `cargo test --workspace` — expect all pass.
Run: `cargo clippy --workspace --all-targets -- -D warnings` — expect clean.
Run: `cargo fmt --check` — expect clean.

- [ ] **Step 2: Standard Opus 4.8 reviewer pass**

Dispatch the `reviewer` subagent per this project's standard "what done
means" gate, covering the full Tasks 1-6 diff. This is in addition to
Task 6's soundness-specific review, not a replacement for it.

- [ ] **Step 3: Confirm Stage A is a complete, shippable milestone**

Verify: `HnswIndex`'s public API is unchanged (no diff outside
`crates/index/src/`), every pre-existing test in `crates/index/` and
`crates/txn/` (which consumes `HnswIndex`) passes unmodified, and the
per-node allocation count is now 2 (down from L+4) with zero remaining
references to `SlotArray` owning its own storage.

- [ ] **Step 4: Open the PR / mark Stage A done**

Per this project's convention (`.claude/CLAUDE.md`: "Don't push to `main`
directly — PRs only"), open a PR for Tasks 1-6's commits. **Do not start
Stage B (Task 8 onward) until this PR is merged.**

---

# Stage B: Chunk-Owned Bump Arena

**Do not begin any task in this section until Stage A (Task 7) is merged.**

## Task 8: `ArenaBlock` with lock-free bump-pointer claiming

**Files:**
- Create: `crates/index/src/arena_block.rs`
- Modify: `crates/index/src/lib.rs` (add `mod arena_block;`)

**Interfaces:**
- Consumes: nothing from Stage A directly (standalone primitive).
- Produces: `pub(crate) struct ArenaBlock { data: Box<[u8]>, bump_offset:
  AtomicUsize, next: AtomicPtr<ArenaBlock> }`; `impl ArenaBlock { fn
  new(capacity: usize) -> Self; fn try_claim(&self, size: usize, align:
  usize) -> Option<*mut u8>; }`. Task 9 consumes `try_claim`'s `None`
  return to trigger block-chain growth.

- [ ] **Step 1: Write the failing claim test**

Create `crates/index/src/arena_block.rs`:

```rust
//! A fixed-capacity, lock-free bump allocator block. See
//! `docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md` §4.

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::ptr;

pub(crate) struct ArenaBlock {
    data: Box<[u8]>,
    bump_offset: AtomicUsize,
    pub(crate) next: AtomicPtr<ArenaBlock>,
}

impl ArenaBlock {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity].into_boxed_slice(),
            bump_offset: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn try_claim_returns_a_pointer_within_bounds() {
        let block = ArenaBlock::new(1024);
        let ptr = block.try_claim(64, 8).unwrap();
        let base = block_base_ptr(&block);
        let offset = unsafe { ptr.offset_from(base) };
        assert!(offset >= 0 && (offset as usize) + 64 <= 1024);
    }

    #[test]
    fn try_claim_returns_none_once_capacity_is_exhausted() {
        let block = ArenaBlock::new(64);
        assert!(block.try_claim(64, 8).is_some());
        assert!(block.try_claim(1, 1).is_none(), "block is full, must not claim beyond capacity");
    }

    #[test]
    fn concurrent_claims_never_overlap() {
        use std::sync::Arc;
        let block = Arc::new(ArenaBlock::new(1024));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let block = Arc::clone(&block);
                std::thread::spawn(move || block.try_claim(32, 8))
            })
            .collect();
        let mut claims: Vec<usize> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .map(|ptr| unsafe { ptr.offset_from(block_base_ptr(&block)) as usize })
            .collect();
        claims.sort_unstable();
        for pair in claims.windows(2) {
            assert!(pair[1] >= pair[0] + 32, "claims must not overlap: {claims:?}");
        }
    }

    fn block_base_ptr(block: &ArenaBlock) -> *const u8 {
        block.data.as_ptr()
    }
}
```

- [ ] **Step 2: Run to confirm it fails**

Run: `cargo test -p strata-index --lib arena_block`
Expected: FAIL with "no method named `try_claim`"

- [ ] **Step 3: Implement `try_claim`**

Add to `ArenaBlock`'s `impl` block:

```rust
    /// Attempts to claim `size` bytes, aligned to `align`, from this
    /// block. Returns `None` if the block doesn't have enough remaining
    /// capacity (including any padding needed for alignment) -- callers
    /// (Task 9) must fall back to a new block on `None`, never retry this
    /// same block.
    pub(crate) fn try_claim(&self, size: usize, align: usize) -> Option<*mut u8> {
        loop {
            let current = self.bump_offset.load(Ordering::SeqCst);
            let base_addr = self.data.as_ptr() as usize;
            let unaligned = base_addr + current;
            let aligned = unaligned.next_multiple_of(align);
            let padding = aligned - unaligned;
            let new_offset = current + padding + size;
            if new_offset > self.data.len() {
                return None;
            }
            if self
                .bump_offset
                .compare_exchange(current, new_offset, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // SAFETY: `current + padding` is within `self.data`'s
                // bounds (checked by `new_offset <= self.data.len()`
                // above, and `new_offset >= current + padding`), and this
                // CAS succeeding means no other thread's claim overlaps
                // `[current + padding, new_offset)` -- every other
                // claim either happened fully before (`bump_offset` was
                // `current` before this CAS) or will happen fully after
                // (starts at `new_offset` or later).
                return Some(unsafe { self.data.as_ptr().add(current + padding).cast_mut() });
            }
            // Lost the race -- another thread advanced bump_offset; retry
            // against the fresh value (self-resolving, no backoff needed:
            // this mirrors SlotArray::claim's retry-the-loop-not-the-caller shape).
        }
    }
```

- [ ] **Step 4: Run to confirm all three tests pass**

Run: `cargo test -p strata-index --lib arena_block`
Expected: PASS (all three tests)

- [ ] **Step 5: Write and run the loom test for claim races**

Add to `arena_block.rs`:

```rust
/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// Two threads claiming space concurrently in the same block -- proves
    /// claimed byte ranges never overlap, matching the real-thread test
    /// above but exhaustively over loom's interleavings.
    #[test]
    fn concurrent_claims_never_overlap() {
        loom::model(|| {
            let block = loom::sync::Arc::new(ArenaBlock::new(256));

            let b1 = loom::sync::Arc::clone(&block);
            let t1 = loom::thread::spawn(move || b1.try_claim(32, 8));

            let b2 = loom::sync::Arc::clone(&block);
            let t2 = loom::thread::spawn(move || b2.try_claim(32, 8));

            let c1 = t1.join().unwrap();
            let c2 = t2.join().unwrap();

            if let (Some(p1), Some(p2)) = (c1, c2) {
                assert_ne!(p1, p2, "two successful claims must never return the same address");
            }
        });
    }
}
```

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom` then
run the produced binary filtered to
`arena_block::loom_tests::concurrent_claims_never_overlap`.
Expected: PASS

- [ ] **Step 6: Register the module and run the full gate**

In `crates/index/src/lib.rs`, add `mod arena_block;`.

Run: `cargo test -p strata-index --lib`
Expected: PASS.
Run: `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/index/src/arena_block.rs crates/index/src/lib.rs
git commit -m "feat(index): add lock-free bump-allocator ArenaBlock

try_claim atomically reserves aligned byte ranges via fetch_add-style
CAS retry, self-resolving on failure (same shape as SlotArray::claim).
Returns None when the block is full -- callers chain a new block on
that signal (Task 9)."
```

---

## Task 9: Block-chain growth via CAS-publish-or-discard

**Files:**
- Modify: `crates/index/src/arena_block.rs`

**Interfaces:**
- Consumes: `ArenaBlock::try_claim`, `ArenaBlock::next` from Task 8.
- Produces: `pub(crate) fn claim_in_chain(head: &AtomicPtr<ArenaBlock>,
  size: usize, align: usize, new_block_capacity: usize) -> *mut u8` — walks
  the chain from `head`, claiming from the last block, growing the chain
  if needed.

- [ ] **Step 1: Write the failing chain-growth test**

Add to `arena_block.rs`'s test module:

```rust
    #[test]
    fn claim_in_chain_grows_a_new_block_when_the_current_one_is_full() {
        let first = Box::into_raw(Box::new(ArenaBlock::new(32)));
        let head = AtomicPtr::new(first);

        let p1 = claim_in_chain(&head, 32, 8, 1024); // fills the first block exactly
        let p2 = claim_in_chain(&head, 32, 8, 1024); // must grow a new block

        assert_ne!(p1, p2);
        // SAFETY: both blocks are leaked deliberately for this test,
        // matching this crate's never-freed invariant.
        unsafe {
            let first_ref = &*first;
            assert!(!first_ref.next.load(Ordering::SeqCst).is_null(), "a second block must have been published");
        }
    }
```

- [ ] **Step 2: Run to confirm it fails**

Run: `cargo test -p strata-index --lib arena_block::tests::claim_in_chain_grows_a_new_block_when_the_current_one_is_full`
Expected: FAIL with "cannot find function `claim_in_chain`"

- [ ] **Step 3: Implement `claim_in_chain`**

Add to `arena_block.rs`, above the test module:

```rust
/// Claims `size` bytes (aligned to `align`) from the block chain starting
/// at `head`, growing the chain with a fresh block of `new_block_capacity`
/// bytes if every existing block is full. Mirrors
/// `NodeTable::get_or_create_chunk`'s publish-or-discard race handling:
/// if two threads race to grow the chain from the same tail, exactly one
/// wins the compare_exchange; the loser's block was never visible to any
/// other thread, so it's safe to drop synchronously.
pub(crate) fn claim_in_chain(
    head: &AtomicPtr<ArenaBlock>,
    size: usize,
    align: usize,
    new_block_capacity: usize,
) -> *mut u8 {
    loop {
        // SAFETY: `head` always points at a validly-constructed,
        // never-freed `ArenaBlock` once non-null (established by this
        // function's own publish step below and by whatever constructed
        // the initial block before calling this function).
        let current = head.load(Ordering::SeqCst);
        let current_ref = unsafe { &*current };
        if let Some(ptr) = current_ref.try_claim(size, align) {
            return ptr;
        }
        // Current block is full -- walk to its `next`, or grow one.
        let next_ptr = current_ref.next.load(Ordering::SeqCst);
        if !next_ptr.is_null() {
            // Another thread already grew the chain -- try claiming from
            // there next iteration by advancing `head`'s effective search
            // start. Since `head` itself is the chain's fixed root (not
            // rewritten to point at the tail), retry via the same `head`
            // but this time `current_ref.next` will be non-null so this
            // branch is taken again -- to avoid re-walking from the root
            // every time under heavy contention, callers should pass a
            // `head` that already refers to a per-chunk "last known
            // tail" cell; see Task 10's `NodeArena` for that wrapper.
            if let Some(ptr) = unsafe { &*next_ptr }.try_claim(size, align) {
                return ptr;
            }
            continue;
        }
        let capacity = new_block_capacity.max(size + align);
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(capacity)));
        match current_ref.next.compare_exchange(
            ptr::null_mut(),
            new_block,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => continue, // retry the loop; the new block will be found via `next` above
            Err(_) => {
                // Lost the race -- `new_block` was never observed by any
                // other thread, safe to drop synchronously.
                // SAFETY: `new_block` came from `Box::into_raw` on the
                // line above in this same call and was never shared.
                unsafe { drop(Box::from_raw(new_block)) };
                continue;
            }
        }
    }
}
```

- [ ] **Step 4: Run to confirm the test passes**

Run: `cargo test -p strata-index --lib arena_block::tests::claim_in_chain_grows_a_new_block_when_the_current_one_is_full`
Expected: PASS

- [ ] **Step 5: Write and run the loom test for the block-publish race**

```rust
#[cfg(loom)]
mod loom_tests {
    // ... (existing content from Task 8 Step 5) ...

    /// Two threads racing to grow the chain when the current block is
    /// full -- proves exactly one new block is ever published, the
    /// loser's allocation is safely discarded, and both threads' claims
    /// land in valid, non-overlapping memory afterward.
    #[test]
    fn concurrent_chain_growth_publishes_exactly_one_block() {
        loom::model(|| {
            let first = loom::sync::Arc::new(ArenaBlock::new(8)); // tiny, forces growth immediately
            let first_ptr = loom::sync::Arc::into_raw(first) as *mut ArenaBlock;
            let head = loom::sync::Arc::new(AtomicPtr::new(first_ptr));

            let h1 = loom::sync::Arc::clone(&head);
            let t1 = loom::thread::spawn(move || claim_in_chain(&h1, 8, 8, 64));

            let h2 = loom::sync::Arc::clone(&head);
            let t2 = loom::thread::spawn(move || claim_in_chain(&h2, 8, 8, 64));

            let p1 = t1.join().unwrap();
            let p2 = t2.join().unwrap();
            assert_ne!(p1, p2, "two successful claims must never return the same address");
        });
    }
}
```

Run: `cargo rustc -p strata-index --lib --profile test -- --cfg loom` then
run the produced binary filtered to
`arena_block::loom_tests::concurrent_chain_growth_publishes_exactly_one_block`.
Expected: PASS

- [ ] **Step 6: Full gate and commit**

Run: `cargo test -p strata-index --lib`, `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: both clean.

```bash
git add crates/index/src/arena_block.rs
git commit -m "feat(index): chain ArenaBlocks with publish-or-discard growth

claim_in_chain walks the block chain, growing it via the same
CAS-publish-or-discard pattern NodeTable::get_or_create_chunk already
uses for chunk directories, applied one layer deeper."
```

---

## Task 10: `NodeArena` — replace `Graph`'s `NodeTable<Node>`

**Files:**
- Create: `crates/index/src/node_arena.rs`
- Modify: `crates/index/src/lib.rs` (add `mod node_arena;`)
- Modify: `crates/index/src/graph.rs` (`Graph::nodes`'s type and every
  call site that touches it)

**Interfaces:**
- Consumes: `ArenaBlock`, `claim_in_chain` (Task 8-9); `alloc_node`,
  `compute_node_layout`, `NodeHeader` (Task 2); `NodeTable::insert_ptr`
  (Task 3, now used for real).
- Produces: `pub(crate) struct NodeArena { ... }` with `fn new(expected_capacity:
  usize) -> Self`, `fn insert(&self, row_id: u64, node: Node)`, `fn
  get(&self, row_id: u64) -> Option<&Node>` — same method shapes as
  `NodeTable<Node>` had, so `Graph`'s call sites (`self.nodes.insert(...)`,
  `self.nodes.get(...)`) need only a field-type change, not call-site
  rewrites.

- [ ] **Step 1: Write the failing round-trip test**

Create `crates/index/src/node_arena.rs`:

```rust
//! Row-id-indexed node storage, chunk directory reused from NodeTable's
//! shape, but each chunk bump-allocates node blocks out of an owned
//! ArenaBlock chain instead of calling the global allocator per node. See
//! `docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md` §4.

use std::ptr;
#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::arena_block::{ArenaBlock, claim_in_chain};
use crate::node::Node;
use crate::node_layout::{alloc_node, compute_node_layout};
use crate::node_table::NodeTable;

/// Default size for a chunk's first arena block: enough for roughly 4096
/// "average" nodes at a typical 512-dim, mostly-level-0 workload (a level
/// 0-only 512-dim node is ~2116 bytes: header ~16B + vector 2048B + layer
/// 0's 33 slots * 8B = 264B); tunable, not load-bearing for correctness --
/// too small just means more chain growth, not a bug.
const INITIAL_ARENA_BLOCK_BYTES: usize = 4096 * 2200;

struct ArenaChunk {
    arena_head: AtomicPtr<ArenaBlock>,
}

pub(crate) struct NodeArena {
    slots: NodeTable<Node>, // reuses NodeTable purely for its chunk-directory/pointer-slot machinery
    chunks_arena: Box<[AtomicPtr<ArenaChunk>]>, // parallel directory: one ArenaChunk per NodeTable chunk index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_get_round_trips() {
        let arena = NodeArena::new(100);
        arena.insert(5, 3, vec![1.0, 2.0, 3.0], 0, 32, 16);
        let node = arena.get(5).unwrap();
        assert_eq!(node.vector(), &[1.0, 2.0, 3.0]);
    }
}
```

(This first draft's `insert` signature takes raw construction params
rather than a pre-built `Node`, because Task 10's whole point is that
`NodeArena` — not `Node::new` — must own the allocation call, so it can
route it through the arena instead of the global allocator. Step 4 below
implements it for real.)

- [ ] **Step 2: Run to confirm it fails**

Run: `cargo test -p strata-index --lib node_arena`
Expected: FAIL (missing `NodeArena::new`/`insert`/`get`)

- [ ] **Step 3: Expose the two constants and add `alloc_node`'s split-out initializer**

`node_table.rs` currently has `CHUNK_SIZE` and `MAX_ROW_ID_CAPACITY` as
private `const` items. Change both to `pub(crate) const`.

`node_layout.rs`'s `alloc_node` currently allocates AND initializes in one
call; `NodeArena::insert` (Step 4 below) needs the initialization logic
applied to memory it already claimed from the arena (not freshly
`alloc`'d). Split `alloc_node` into `alloc_node` (unchanged, still used by
any future direct/non-arena caller and by Task 2's own tests) plus a new
`pub(crate) unsafe fn init_node_at(ptr: *mut u8, row_id: u64, vector: &[f32],
dim: usize, level: usize, mmax0: usize, mmax: usize)` containing exactly
the header/vector/slot-initialization logic `alloc_node` already has
(Task 2 Step 7's body from `std::ptr::write(ptr.cast::<NodeHeader>(), ...)`
onward), with the same `// SAFETY:` comments carried over unchanged (they
already only assume "target memory is allocated with the matching
layout and not yet observed by any other thread," which is exactly what
an arena-claimed block also satisfies). `alloc_node` itself becomes a thin
wrapper: `std::alloc::alloc` the layout, then call `init_node_at`.

Add `pub(crate) fn from_raw(ptr: std::ptr::NonNull<u8>) -> Self { Self(ptr) }`
to `Node` in `node.rs` (currently only constructible via `Node::new`,
which always calls `alloc_node` itself — `NodeArena` needs to construct a
`Node` around a pointer it obtained from the arena instead).

- [ ] **Step 4: Implement `NodeArena`**

Replace the placeholder struct/test module in `node_arena.rs` with:

```rust
use crate::node_table::CHUNK_SIZE; // now pub(crate), per Step 3

pub(crate) struct NodeArena {
    directory: Box<[AtomicPtr<ArenaChunk>]>, // one entry per NodeTable-style chunk index
    slots: NodeTable<Node>,                  // reused purely for its row-id -> pointer-slot machinery
}

impl NodeArena {
    pub(crate) fn new(expected_capacity: usize) -> Self {
        let num_chunks = crate::node_table::MAX_ROW_ID_CAPACITY.div_ceil(CHUNK_SIZE).max(1);
        let directory = (0..num_chunks)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { directory, slots: NodeTable::new(expected_capacity) }
    }

    fn get_or_create_arena_chunk(&self, chunk_idx: usize) -> &ArenaChunk {
        let existing = self.directory[chunk_idx].load(Ordering::SeqCst);
        if !existing.is_null() {
            // SAFETY: a non-null pointer here was published by a
            // successful compare_exchange below and is never freed or
            // moved afterward.
            return unsafe { &*existing };
        }
        let first_block = Box::into_raw(Box::new(ArenaBlock::new(INITIAL_ARENA_BLOCK_BYTES)));
        let new_chunk = Box::into_raw(Box::new(ArenaChunk { arena_head: AtomicPtr::new(first_block) }));
        match self.directory[chunk_idx].compare_exchange(
            ptr::null_mut(), new_chunk, Ordering::SeqCst, Ordering::SeqCst,
        ) {
            // SAFETY: `new_chunk` was just published by this successful compare_exchange.
            Ok(_) => unsafe { &*new_chunk },
            Err(actual) => {
                // SAFETY: `new_chunk` (and its `first_block`) were never
                // observed by any other thread -- safe to drop
                // synchronously, same reasoning as
                // NodeTable::get_or_create_chunk's loser-cleanup path.
                unsafe {
                    let chunk = Box::from_raw(new_chunk);
                    drop(Box::from_raw(chunk.arena_head.load(Ordering::SeqCst)));
                }
                // SAFETY: `actual` won the race and is published/never freed.
                unsafe { &*actual }
            }
        }
    }

    /// Allocates a node's block out of its chunk's arena (never touching
    /// the global allocator on this path) and registers it at `row_id`.
    pub(crate) fn insert(&self, row_id: u64, dim: usize, vector: Vec<f32>, level: usize, mmax0: usize, mmax: usize) {
        let (layout, _offsets) = compute_node_layout(dim, level, mmax0, mmax);
        let chunk_idx = row_id as usize / CHUNK_SIZE;
        let chunk = self.get_or_create_arena_chunk(chunk_idx);
        let block_ptr = claim_in_chain(&chunk.arena_head, layout.size(), layout.align(), INITIAL_ARENA_BLOCK_BYTES);
        // SAFETY: `block_ptr` was just claimed with exactly `layout`'s
        // size/alignment from arena memory that is never freed or moved,
        // and is not yet reachable by any other thread (only this
        // function holds it until the insert_ptr publish below).
        unsafe { crate::node_layout::init_node_at(block_ptr, row_id, &vector, dim, level, mmax0, mmax) };
        let node = Node::from_raw(unsafe { std::ptr::NonNull::new_unchecked(block_ptr) });
        // SAFETY: `node` wraps `block_ptr`, which is fully initialized by
        // `init_node_at` above and never freed/moved for this arena's
        // lifetime -- this is `insert_ptr`'s exact intended use (Task 3),
        // now finally exercised: no second, redundant box of `node`.
        unsafe { self.slots.insert_ptr(row_id, Box::into_raw(Box::new(node))) };
    }

    pub(crate) fn get(&self, row_id: u64) -> Option<&Node> {
        self.slots.get(row_id)
    }
}
```

- [ ] **Step 5: Run to confirm the test passes**

Run: `cargo test -p strata-index --lib node_arena`
Expected: PASS

- [ ] **Step 6: Wire `Graph` to use `NodeArena` instead of `NodeTable<Node>`**

In `crates/index/src/graph.rs`, change:

```rust
pub struct Graph<D: Distance> {
    nodes: NodeTable<Node>,
    // ...
}
```

to:

```rust
pub struct Graph<D: Distance> {
    nodes: NodeArena,
    // ...
}
```

Update `Graph::new` (`NodeTable::new(expected_capacity)` → `NodeArena::new(expected_capacity)`).

In `Graph::insert`, replace:

```rust
        let node = Node::new(row_id, vector, level, mmax0, mmax);
        self.nodes.insert(row_id, node);
```

with:

```rust
        let dim = vector.len();
        self.nodes.insert(row_id, dim, vector, level, mmax0, mmax);
```

Every other `self.nodes.get(...)` call site is unchanged (`NodeArena::get`
has the identical signature to `NodeTable::get`).

Add `use crate::node_arena::NodeArena;` and remove the now-unused
`use crate::node_table::NodeTable;` from `graph.rs` if nothing else in
that file uses it directly.

- [ ] **Step 7: Register the module and run the full workspace gate**

In `crates/index/src/lib.rs`, add `mod node_arena;`.

Run: `cargo build --workspace` — expect clean.
Run: `cargo test --workspace` — expect all pass, including every
`graph::tests` test (Task 5's stress test included) now running against
`NodeArena` instead of `NodeTable<Node>`.
Run: `cargo clippy --workspace --all-targets -- -D warnings` — expect clean.

- [ ] **Step 8: Commit**

```bash
git add crates/index/src/node_arena.rs crates/index/src/node.rs crates/index/src/node_layout.rs crates/index/src/node_table.rs crates/index/src/graph.rs crates/index/src/lib.rs
git commit -m "feat(index): route node storage through NodeArena's bump allocator

Graph::nodes is now NodeArena instead of NodeTable<Node>. Each node's
block is claimed from its chunk's arena via claim_in_chain (Task 9)
instead of a fresh std::alloc::alloc call, reaching true
single-allocation-per-node (the arena's own block allocations are
amortized across many nodes, not one per insert). NodeTable<T> itself
is untouched -- NodeArena reuses it only for its row-id -> pointer-slot
directory machinery via the existing generic insert_ptr/get."
```

---

## Task 11: Concurrent-insert-throughput benchmark

**Files:**
- Create: `bench/benches/node_arena_bench.rs`
- Modify: `bench/Cargo.toml` (register the new bench target)

**Interfaces:**
- Consumes: `Graph::insert` (public within the workspace via
  `strata_index::graph::Graph`, already `#[doc(hidden)] pub` per
  `lib.rs`'s existing convention for bench access).

This is Stage B's landing requirement per the design doc's own calibration
— its motivating problem (allocator contention under concurrent insert)
was found unmeasured, so this benchmark is what turns "plausible" into
"decided."

- [ ] **Step 1: Write the benchmark**

Create `bench/benches/node_arena_bench.rs`:

```rust
//! Concurrent insert throughput: validates (or refutes) Stage B's
//! motivating claim that removing per-node global-allocator calls
//! improves throughput under concurrent writers. See
//! docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md §1.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use strata_index::distance::L2;
use strata_index::graph::Graph;

const THREADS: usize = 8;
const INSERTS_PER_THREAD: usize = 2000;
const DIM: usize = 512;

fn concurrent_insert_throughput(c: &mut Criterion) {
    c.bench_function("concurrent_insert_8_threads_2000_each_512dim", |b| {
        b.iter(|| {
            let graph = Arc::new(Graph::new(L2, THREADS * INSERTS_PER_THREAD));
            let m_l = 1.0 / (16f64).ln();
            let handles: Vec<_> = (0..THREADS)
                .map(|t| {
                    let graph = Arc::clone(&graph);
                    std::thread::spawn(move || {
                        for i in 0..INSERTS_PER_THREAD {
                            let row_id = (t * INSERTS_PER_THREAD + i) as u64;
                            let vector: Vec<f32> = (0..DIM).map(|d| (row_id as f32) + d as f32 * 0.001).collect();
                            let unif = deterministic_unif(row_id);
                            graph.insert(row_id, vector, 16, 32, 16, 200, m_l, 1.0, unif).unwrap();
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

fn deterministic_unif(seed: u64) -> f64 {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 33;
    ((x >> 11) as f64 / (1u64 << 53) as f64).clamp(f64::EPSILON, 1.0 - f64::EPSILON)
}

criterion_group!(benches, concurrent_insert_throughput);
criterion_main!(benches);
```

- [ ] **Step 2: Register the bench target**

In `bench/Cargo.toml`, add alongside the existing `[[bench]]` entries:

```toml
[[bench]]
name = "node_arena_bench"
harness = false
```

- [ ] **Step 3: Run it against the current (post-Task-10) arena-backed code**

Run: `cargo bench -p strata-bench --bench node_arena_bench`
Record the reported mean time for `concurrent_insert_8_threads_2000_each_512dim`.

- [ ] **Step 4: Temporarily revert to the pre-arena path for an A/B comparison**

Check out Task 7's Stage-A-complete commit into a scratch state (or use
`git stash`/a throwaway branch — do not disturb the actual Stage B commits
on the working branch) and re-run the same benchmark command against
`Graph::nodes: NodeTable<Node>` (Stage A only, still 2 allocations/node
but no arena). Record its mean time.

Return to the Stage B branch afterward (`git checkout -` or equivalent) —
this step is measurement-only, it does not produce a commit itself.

- [ ] **Step 5: Record the comparison result in the plan's tracking**

Add a short note (a follow-up commit touching only this plan file, or the
PR description) stating both measured numbers and the computed percentage
difference. This is the evidence Task 12's landing decision is based on —
do not skip recording it even if the result is "no significant
difference."

- [ ] **Step 6: Commit the benchmark itself**

```bash
git add bench/benches/node_arena_bench.rs bench/Cargo.toml
git commit -m "perf(index): add concurrent-insert-throughput benchmark for NodeArena

A/B measurement (arena vs. plain NodeTable<Node>) is Stage B's landing
requirement per its own design doc calibration -- allocator contention
under concurrent insert was flagged as a plausible-but-unmeasured claim
before this benchmark existed."
```

---

## Task 12: Stage B completion gate and landing decision

**Files:** none (verification/decision-only).

- [ ] **Step 1: Full workspace gate**

Run: `cargo build --workspace`, `cargo test --workspace`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean.

- [ ] **Step 2: Standard Opus 4.8 reviewer pass**

Covering the full Task 8-11 diff.

- [ ] **Step 3: Apply the landing decision from Task 11's benchmark**

If the benchmark shows a real, meaningful improvement: proceed to Step 4.
If it shows no measurable difference (within noise) or a regression: per
this plan's own Global Constraints, **Stage A alone remains a complete,
shippable milestone** — document the negative/null result plainly (do not
bury it), and either (a) don't merge Stage B's PR at all, reverting to
Stage A as the final state, or (b) merge it anyway only if there's a
separate, explicit justification beyond throughput (e.g., a real
allocator-pressure issue observed elsewhere) — do not merge Stage B on the
strength of "it's already built," that is exactly the outcome this
project's calibration discipline exists to prevent.

- [ ] **Step 4: Open the PR / mark Stage B done**

Per this project's convention, PRs only, no direct push to `main`.
