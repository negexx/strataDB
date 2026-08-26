//! Raw single-block layout for a graph node: header, vector, and every
//! layer's edge slots packed into one allocation. See
//! `docs/design.md`.

use std::alloc::Layout;

#[cfg(loom)]
use loom::sync::atomic::{AtomicU8, AtomicU64};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU8, AtomicU64};

use crate::slot_array::EMPTY;

#[cfg(test)]
thread_local! {
    static RAW_NODE_BLOCK_ALLOCATION_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn alloc_node_block(layout: Layout) -> *mut u8 {
    #[cfg(test)]
    RAW_NODE_BLOCK_ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));

    // SAFETY[IDX-NODE-LAYOUT-ALLOC-BLOCK]: The non-zero NodeHeader-containing layout is valid for allocation.
    // node block contains a `NodeHeader`.
    unsafe { std::alloc::alloc(layout) }
}

#[cfg(test)]
fn reset_raw_node_block_allocation_count() {
    RAW_NODE_BLOCK_ALLOCATION_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn raw_node_block_allocation_count() -> usize {
    RAW_NODE_BLOCK_ALLOCATION_COUNT.with(std::cell::Cell::get)
}

#[repr(C)]
pub(crate) struct NodeHeader {
    pub(crate) row_id: u64,
    /// The vector's element count, stored here (mirroring `mmax0`/`mmax`)
    /// so `Node::vector`/`Node::layer` can recompute this block's layout
    /// without threading `dim` as a parameter through every accessor.
    pub(crate) dim: u32,
    pub(crate) level: u8,
    pub(crate) mmax0: u16,
    pub(crate) mmax: u16,
    pub(crate) deleted: AtomicU8,
    /// `0` from construction until `Graph::insert`'s connection-building
    /// for this row completes, then `1` forever after (never reset --
    /// there's no "un-publish"). See `Node::is_published`'s doc comment
    /// for the race this closes: a node visible in `NodeTable` but not
    /// yet published can still be *discovered* as a search candidate (that
    /// path is untouched and must stay untouched, per the investigation
    /// that led to this field -- excluding unpublished nodes from
    /// candidate selection made the underlying hazard measurably WORSE),
    /// it just can't be used as the ENTRY a concurrent insert descends
    /// through to the next-lower layer, since its own edges at that lower
    /// layer may not exist yet.
    pub(crate) published: AtomicU8,
}

pub(crate) struct NodeLayoutOffsets {
    pub(crate) vector_offset: usize,
    /// `layer_offsets[lc]` is the byte offset of layer `lc`'s first slot.
    pub(crate) layer_offsets: Vec<usize>,
}

/// The single source of truth for a layer's physical slot count: layer 0
/// gets `mmax0 + 1` slots, every other layer gets `mmax + 1` (the `+ 1`
/// is one slot of transient headroom — see `Node::layer_capacity`'s doc
/// comment in node.rs for why it exists at all). `compute_node_layout`,
/// `layer_byte_offset`, and `Node::layer_capacity` must all size layers
/// through this one function: if any of them diverged, `Node::layer`'s
/// `slice::from_raw_parts` could read past the region actually reserved
/// for the last layer — undefined behavior, not just a wrong answer.
/// Drift-guarded by `byte_offset_helpers_match_compute_node_layout`.
pub(crate) fn layer_slot_count(mmax0: usize, mmax: usize, lc: usize) -> usize {
    if lc == 0 { mmax0 + 1 } else { mmax + 1 }
}

// The `expect`s here guard only `Layout` arithmetic overflowing `isize`,
// which is unreachable for any real node (dim and slot counts are bounded
// far below `isize::MAX / 8` by upstream validation) — a `Result` return
// would force every caller to handle an impossible error.
#[allow(clippy::expect_used)]
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
        let capacity = layer_slot_count(mmax0, mmax, lc);
        let (extended, layer_offset) = layout
            .extend(
                Layout::array::<AtomicU64>(capacity)
                    .expect("slot array bytes must not overflow isize"),
            )
            .expect("layer layout must not overflow isize");
        layout = extended;
        layer_offsets.push(layer_offset);
    }

    (
        layout.pad_to_align(),
        NodeLayoutOffsets {
            vector_offset,
            layer_offsets,
        },
    )
}

/// Non-allocating equivalent of `compute_node_layout(..).1.vector_offset`,
/// for `Node::vector`'s hot path (called once per candidate distance
/// evaluation in `search_layer`). `dim` is unused today — a field's offset
/// depends only on what precedes it — but is kept for symmetry with
/// `layer_byte_offset` and in case `NodeHeader` ever grows a
/// dim-dependent prefix. Guarded against drift from `compute_node_layout`
/// by `byte_offset_helpers_match_compute_node_layout` below.
// `expect_used`: same impossible-overflow guard as `compute_node_layout`.
#[allow(clippy::expect_used)]
pub(crate) fn vector_byte_offset(dim: usize) -> usize {
    let (_, vector_offset) = Layout::new::<NodeHeader>()
        .extend(Layout::array::<f32>(dim).expect("dim*4 bytes must not overflow isize"))
        .expect("header+vector layout must not overflow isize");
    vector_offset
}

/// Non-allocating equivalent of
/// `compute_node_layout(..).1.layer_offsets[lc]`, for `Node::layer`'s hot
/// path. Walks the same `Layout::extend` sequence as `compute_node_layout`
/// but stops at layer `lc` without building a `Vec`. Panics if
/// `lc > level`, matching `Node::layer`'s bounds contract. Guarded against
/// drift by `byte_offset_helpers_match_compute_node_layout` below.
// `expect_used`: same impossible-overflow guard as `compute_node_layout`.
#[allow(clippy::expect_used)]
pub(crate) fn layer_byte_offset(
    dim: usize,
    level: usize,
    mmax0: usize,
    mmax: usize,
    lc: usize,
) -> usize {
    assert!(
        lc <= level,
        "layer {lc} requested but node's level is {level}"
    );
    let (mut layout, _) = Layout::new::<NodeHeader>()
        .extend(Layout::array::<f32>(dim).expect("dim*4 bytes must not overflow isize"))
        .expect("header+vector layout must not overflow isize");
    for current in 0..=lc {
        let capacity = layer_slot_count(mmax0, mmax, current);
        let (extended, layer_offset) = layout
            .extend(
                Layout::array::<AtomicU64>(capacity)
                    .expect("slot array bytes must not overflow isize"),
            )
            .expect("layer layout must not overflow isize");
        if current == lc {
            return layer_offset;
        }
        layout = extended;
    }
    unreachable!("loop always returns at current == lc")
}

/// Allocates and fully initializes a single raw block for a node: header,
/// vector, and every layer's slots preset to `EMPTY`. The returned
/// pointer is never freed or moved while any `NodeTable`/`Graph`
/// referencing it is alive; it is freed exactly once, via
/// [`dealloc_node`], when the `NodeTable` slot holding it is dropped (see
/// `node_table::Reclaim` and `NodeTable`'s `Drop` impl).
///
/// # Safety
/// The caller must ensure the returned pointer is eventually published
/// (made reachable to other threads) via a `Release`-or-stronger store,
/// since this function performs no synchronization itself. Today's
/// production chain is `Node::new` -> `NodeTable::insert`, whose
/// `AtomicPtr::store` provides that publication; `NodeTable::insert_ptr`
/// is the intended future consumer (Task 10's arena work) but is not yet
/// called from any production path.
// `expect_used`: the `try_from`s are caller-contract guards on parameters
// upstream code already bounds (`level` is clamped to `LEVEL_MASK` in
// graph.rs; `mmax0`/`mmax` are small tuning constants) — violating them is
// a caller bug worth a loud panic, not a recoverable error.
// `cast_ptr_alignment`: every `u8`-pointer cast below is provably aligned —
// `alloc` returns a pointer aligned to `layout.align()` (>= 8, since the
// header contains a `u64`), and every offset comes from `Layout::extend`,
// which aligns each region to its own type's alignment; the SAFETY comment
// at each cast site restates the specific guarantee.
// SAFETY[IDX-NODE-LAYOUT-ALLOC-NODE]: Callers publish the returned initialized allocation with a synchronizing store.
#[allow(clippy::expect_used, clippy::cast_ptr_alignment)]
pub(crate) unsafe fn alloc_node(
    row_id: u64,
    vector: &[f32],
    level: usize,
    mmax0: usize,
    mmax: usize,
) -> *mut u8 {
    let header = NodeHeader {
        row_id,
        dim: u32::try_from(vector.len()).expect("dim must fit in u32"),
        level: u8::try_from(level).expect("level must fit in u8 (see graph.rs LEVEL_MASK)"),
        mmax0: u16::try_from(mmax0).expect("mmax0 must fit in u16"),
        mmax: u16::try_from(mmax).expect("mmax must fit in u16"),
        deleted: AtomicU8::new(0),
        published: AtomicU8::new(0),
    };
    let (layout, offsets) = compute_node_layout(vector.len(), level, mmax0, mmax);
    let ptr = alloc_node_block(layout);
    if ptr.is_null() {
        // Not `assert!`: formatting a panic message would itself allocate,
        // during the very OOM condition being reported --
        // `handle_alloc_error` is built to run while allocation is failing.
        std::alloc::handle_alloc_error(layout);
    }

    // SAFETY[IDX-NODE-LAYOUT-WRITE-HEADER]: The fresh allocation reserves one aligned NodeHeader at offset zero.
    // reserves space for one `NodeHeader` at offset 0 with correct
    // alignment (`Layout::new::<NodeHeader>()` is `compute_node_layout`'s
    // first component) -- writing one `NodeHeader` here does not exceed
    // the allocation and does not read the uninitialized memory it
    // overwrites. `header` was fully validated before this allocation and
    // is moved here exactly once.
    unsafe {
        std::ptr::write(ptr.cast::<NodeHeader>(), header);
    }

    // SAFETY[IDX-NODE-LAYOUT-COPY-VECTOR]: The layout reserves the non-overlapping initialized vector region.
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
    for (lc, &layer_offset) in offsets.layer_offsets.iter().enumerate() {
        let slot_count = layer_slot_count(mmax0, mmax, lc);
        for i in 0..slot_count {
            // SAFETY[IDX-NODE-LAYOUT-INITIALIZE-SLOT]: Each computed slot address is within its uniquely initialized layer region.
            // size_of::<AtomicU64>()`) is within the region
            // `compute_node_layout` reserved for this layer via
            // `Layout::array::<AtomicU64>(capacity)` -- the same
            // `layer_slot_count(mmax0, mmax, lc)` slots being written
            // here -- and no other write targets this address.
            unsafe {
                let slot_ptr = ptr.add(layer_offset).cast::<AtomicU64>().add(i);
                std::ptr::write(slot_ptr, AtomicU64::new(EMPTY));
            }
        }
    }

    ptr
}

/// Frees a raw node block previously returned by [`alloc_node`].
/// Recomputes the exact [`Layout`] `alloc_node` used from the header's own
/// stored `dim`/`level`/`mmax0`/`mmax` fields -- `std::alloc::dealloc`
/// requires the layout passed here to match the allocation exactly, so
/// this reads those fields (a read, not a write, so it doesn't race with
/// anything) before deallocating.
///
/// # Safety
/// `ptr` must have been returned by `alloc_node`, must not already have
/// been freed, and the caller must have exclusive access to it (no other
/// reference may read or write through it during or after this call).
// SAFETY[IDX-NODE-LAYOUT-DEALLOC-NODE]: Callers provide the unique live allocation originally returned by alloc_node.
#[allow(clippy::cast_ptr_alignment)]
pub(crate) unsafe fn dealloc_node(ptr: *mut u8) {
    // SAFETY[IDX-NODE-LAYOUT-READ-HEADER]: The live allocation contains the initialized NodeHeader at offset zero.
    // fully-initialized `NodeHeader` at offset 0 before returning, and the
    // caller guarantees `ptr` is still valid and not aliased -- reading the
    // header back here is sound.
    let header = unsafe { &*ptr.cast::<NodeHeader>() };
    let (layout, _) = compute_node_layout(
        header.dim as usize,
        header.level as usize,
        header.mmax0 as usize,
        header.mmax as usize,
    );
    // SAFETY[IDX-NODE-LAYOUT-DEALLOC-BLOCK]: The recomputed layout exactly matches this uniquely owned allocation.
    // `alloc_node` used to build this block (the same function, the same
    // arithmetic), and the caller guarantees `ptr` was returned by
    // `alloc_node` and is not freed or aliased elsewhere.
    unsafe {
        std::alloc::dealloc(ptr, layout);
    }
}

#[cfg(all(test, not(loom)))]
// `cast_ptr_alignment`: the test reads back through the same
// provably-aligned offsets `compute_node_layout` produced (see the allow
// on `alloc_node` above).
#[allow(clippy::cast_ptr_alignment)]
mod tests {
    use super::*;
    use crate::node_table::Reclaim;

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

    #[test]
    fn byte_offset_helpers_match_compute_node_layout() {
        for &(dim, level, mmax0, mmax) in &[
            (1usize, 0usize, 16usize, 16usize),
            (3, 1, 32, 16),
            (512, 7, 32, 16),
            (768, 0, 256, 256),
        ] {
            let (layout, offsets) = compute_node_layout(dim, level, mmax0, mmax);
            assert_eq!(
                vector_byte_offset(dim),
                offsets.vector_offset,
                "vector offset drifted for (dim={dim}, level={level}, mmax0={mmax0}, mmax={mmax})"
            );
            // A real node built with the same parameters: `Node::layer`'s
            // `slice::from_raw_parts` capacity must exactly match the
            // inter-offset spacing `compute_node_layout` reserved — if
            // `Node::layer_capacity` ever sized a layer larger than its
            // reservation, the last layer's slice would read past the
            // allocation itself (UB), so this drift is guarded here
            // alongside the offset checks.
            let node = crate::node::Node::new(0, vec![0.0; dim], level, mmax0, mmax);
            for lc in 0..=level {
                assert_eq!(
                    layer_byte_offset(dim, level, mmax0, mmax, lc),
                    offsets.layer_offsets[lc],
                    "layer {lc} offset drifted for (dim={dim}, level={level}, mmax0={mmax0}, mmax={mmax})"
                );
                let reserved_bytes = if lc < level {
                    offsets.layer_offsets[lc + 1] - offsets.layer_offsets[lc]
                } else {
                    layout.size() - offsets.layer_offsets[lc]
                };
                assert_eq!(
                    node.layer(lc).capacity() * std::mem::size_of::<AtomicU64>(),
                    reserved_bytes,
                    "Node::layer_capacity drifted from compute_node_layout's reservation \
                     for (dim={dim}, level={level}, mmax0={mmax0}, mmax={mmax}, lc={lc})"
                );
            }
            // SAFETY[IDX-NODE-LAYOUT-TEST-RECLAIM]: This test exclusively owns the freshly constructed node.
            // never stored in a `NodeTable`, and nothing else references it.
            unsafe {
                node.reclaim();
            }
        }
    }

    #[test]
    #[should_panic(expected = "layer 2 requested but node's level is 1")]
    fn layer_byte_offset_panics_when_lc_exceeds_level() {
        layer_byte_offset(3, 1, 32, 16, 2);
    }

    #[test]
    fn alloc_node_rejects_invalid_header_before_raw_allocation() {
        for (level, mmax0, mmax) in [
            (usize::from(u8::MAX) + 1, 0, 0),
            (0, usize::from(u16::MAX) + 1, 0),
            (0, 0, usize::from(u16::MAX) + 1),
        ] {
            reset_raw_node_block_allocation_count();
            let result = std::panic::catch_unwind(|| {
                // SAFETY[IDX-NODE-LAYOUT-TEST-INVALID-ALLOC]: Invalid arguments panic before returning an allocation to reclaim.
                // so no publication obligation arises.
                unsafe { alloc_node(0, &[], level, mmax0, mmax) }
            });

            assert!(result.is_err());
            assert_eq!(raw_node_block_allocation_count(), 0);
        }
    }

    #[test]
    fn alloc_node_initializes_header_vector_and_every_slot_to_empty() {
        let vector = vec![1.0f32, 2.0, 3.0];
        // SAFETY[IDX-NODE-LAYOUT-TEST-ALLOCATE]: This test immediately owns the newly allocated, unpublished block.
        // across threads before the read.
        let ptr = unsafe { alloc_node(7, &vector, 1, 32, 16) };
        // SAFETY[IDX-NODE-LAYOUT-TEST-READ]: The freshly returned block is fully initialized and still exclusively owned.
        // fully initialized per its own contract.
        unsafe {
            let header = &*ptr.cast::<NodeHeader>();
            assert_eq!(header.row_id, 7);
            assert_eq!(header.dim, 3);
            assert_eq!(header.level, 1);
            assert_eq!(header.mmax0, 32);
            assert_eq!(header.mmax, 16);
            assert_eq!(header.deleted.load(std::sync::atomic::Ordering::SeqCst), 0);

            let (_, offsets) = compute_node_layout(3, 1, 32, 16);
            let vector_ptr = ptr.add(offsets.vector_offset).cast::<f32>();
            assert_eq!(std::slice::from_raw_parts(vector_ptr, 3), &[1.0, 2.0, 3.0]);

            for (lc, &layer_offset) in offsets.layer_offsets.iter().enumerate() {
                let slot_count = layer_slot_count(32, 16, lc);
                for i in 0..slot_count {
                    let slot_ptr = ptr.add(layer_offset).cast::<AtomicU64>().add(i);
                    assert_eq!(
                        (*slot_ptr).load(std::sync::atomic::Ordering::SeqCst),
                        EMPTY,
                        "layer {lc} slot {i} must start EMPTY"
                    );
                }
            }
        }
        // SAFETY[IDX-NODE-LAYOUT-TEST-DEALLOCATE]: The test owns this live block and no reference survives reclamation.
        // freed, and nothing else holds a reference to it.
        unsafe {
            dealloc_node(ptr);
        }
    }

    #[test]
    fn dealloc_node_frees_a_block_with_multiple_layers_without_use_after_free() {
        // Exercises a node with more than one layer (unlike the single-layer
        // case above), and reads the header fields `dealloc_node` depends on
        // *before* deallocating, then frees -- run under
        // `cargo miri test` this proves both "no leak" (nothing under
        // `alloc_node` escapes `dealloc_node`'s recomputed `Layout`) and "no
        // use-after-free" (nothing reads `ptr` afterward).
        let vector = vec![1.0f32; 512];
        // SAFETY[IDX-NODE-LAYOUT-TEST-MULTILAYER-ALLOCATE]: The test immediately owns the newly allocated multi-layer block.
        // across threads before the read.
        let ptr = unsafe { alloc_node(0, &vector, 4, 32, 16) };
        // SAFETY[IDX-NODE-LAYOUT-TEST-MULTILAYER-READ]: The fresh multi-layer block is initialized before this exclusive read.
        // `alloc_node`.
        unsafe {
            let header = &*ptr.cast::<NodeHeader>();
            assert_eq!(header.dim, 512);
            assert_eq!(header.level, 4);
        }
        // SAFETY[IDX-NODE-LAYOUT-TEST-MULTILAYER-DEALLOCATE]: The test uniquely owns this live multi-layer block.
        // freed, and nothing else holds a reference to it.
        unsafe {
            dealloc_node(ptr);
        }
    }
}
