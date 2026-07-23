//! Raw single-block layout for a graph node: header, vector, and every
//! layer's edge slots packed into one allocation. See
//! `docs/superpowers/specs/2026-07-23-single-allocation-hnsw-node-layout-design.md`.

use std::alloc::Layout;
use std::sync::atomic::AtomicU64;

use crate::slot_array::EMPTY;

// The `dead_code` allows throughout this module: nothing outside this
// module's own tests consumes it yet — the next task wires `alloc_node`,
// `NodeHeader`, and `NodeLayoutOffsets` into `Node`/`NodeTable` (same
// pattern as the not-yet-consumed accessors in `node.rs`).
#[allow(dead_code)]
#[repr(C)]
pub(crate) struct NodeHeader {
    pub(crate) row_id: u64,
    pub(crate) level: u8,
    pub(crate) mmax0: u16,
    pub(crate) mmax: u16,
    pub(crate) deleted: std::sync::atomic::AtomicU8,
}

#[allow(dead_code)]
pub(crate) struct NodeLayoutOffsets {
    pub(crate) vector_offset: usize,
    /// `layer_offsets[lc]` is the byte offset of layer `lc`'s first slot.
    pub(crate) layer_offsets: Vec<usize>,
}

// The `expect`s here guard only `Layout` arithmetic overflowing `isize`,
// which is unreachable for any real node (dim and slot counts are bounded
// far below `isize::MAX / 8` by upstream validation) — a `Result` return
// would force every caller to handle an impossible error.
#[allow(dead_code, clippy::expect_used)]
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
// `expect_used`: the `try_from`s are caller-contract guards on parameters
// upstream code already bounds (`level` is clamped to `LEVEL_MASK` in
// graph.rs; `mmax0`/`mmax` are small tuning constants) — violating them is
// a caller bug worth a loud panic, not a recoverable error.
// `cast_ptr_alignment`: every `u8`-pointer cast below is provably aligned —
// `alloc` returns a pointer aligned to `layout.align()` (>= 8, since the
// header contains a `u64`), and every offset comes from `Layout::extend`,
// which aligns each region to its own type's alignment; the SAFETY comment
// at each cast site restates the specific guarantee.
#[allow(dead_code, clippy::expect_used, clippy::cast_ptr_alignment)]
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
    assert!(
        !ptr.is_null(),
        "node allocation failed (layout: {layout:?})"
    );

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
        let slot_count = if layer_offset == offsets.layer_offsets[0] {
            mmax0 + 1
        } else {
            mmax + 1
        };
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

#[cfg(test)]
// `cast_ptr_alignment`: the test reads back through the same
// provably-aligned offsets `compute_node_layout` produced (see the allow
// on `alloc_node` above).
#[allow(clippy::cast_ptr_alignment)]
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
}
