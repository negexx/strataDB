//! A graph node: its vector, one slot region per layer it participates
//! in, and its deleted flag, all packed into a single raw allocation. See
//! `docs/design.md`.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node_layout::{
    NodeHeader, alloc_node, dealloc_node, layer_byte_offset, layer_slot_count, vector_byte_offset,
};
use crate::node_table::Reclaim;
use crate::slot_array::SlotArray;

/// A unique owning handle to a single raw-allocated node block. It is moved
/// into one `NodeTable` slot and freed exactly once via
/// [`Reclaim::reclaim`] when that owner reaches its explicit reclaim
/// boundary.
///
/// The handle intentionally implements neither `Clone` nor `Copy`: safe code
/// cannot duplicate ownership of the out-of-band allocation. Readers borrow
/// the stored node through `&Node`; they never need an owning handle.
pub(crate) struct Node(std::ptr::NonNull<u8>);

impl Reclaim for Node {
    // SAFETY[IDX-NODE-RECLAIM]: Consuming the unique Node owner reaches its single final reclaim boundary.
    unsafe fn reclaim(self) {
        // SAFETY[IDX-NODE-RECLAIM-ALLOC]: This unique owner contains the live allocation produced by alloc_node.
        // constructor), and consuming the unique `Node` owner makes this
        // the final reclaim boundary. `NodeTable`'s `Drop` invokes it once
        // under exclusive access; standalone owners must satisfy the same
        // single-reclaim, no-outstanding-reference contract.
        unsafe {
            dealloc_node(self.0.as_ptr());
        }
    }
}

// SAFETY[IDX-NODE-SEND]: Moving this unique owning handle transfers its allocation ownership.
// Every field readers access through shared references is either immutable
// after construction (header's row_id/dim/level/mmax0/mmax, vector bytes) or
// atomic (deleted flag, published flag, edge slots).
unsafe impl Send for Node {}
// SAFETY[IDX-NODE-SYNC]: Concurrent readers only use shared access to immutable fields and atomics.
// fields and atomics; the unique owner is reclaimed only at its final
// explicit ownership boundary.
unsafe impl Sync for Node {}

impl Node {
    // `expect_used`: `alloc_node` asserts its result is non-null before
    // returning (allocation failure panics there), so the `NonNull::new`
    // failure arm is unreachable -- a loud message beats threading an
    // impossible error to every caller.
    //
    // `needless_pass_by_value`: `Node::new`'s exact pre-existing signature
    // (taking the `Vec` by value) is deliberately preserved for its
    // callers; the contents are copied into the single block and the `Vec`
    // is dropped here -- ownership hand-off is the intended contract.
    #[allow(clippy::expect_used, clippy::needless_pass_by_value)]
    pub(crate) fn new(
        row_id: u64,
        vector: Vec<f32>,
        level: usize,
        mmax0: usize,
        mmax: usize,
    ) -> Self {
        // SAFETY[IDX-NODE-NEW-ALLOCATE]: NodeTable publishes this initialized node through its synchronizing AtomicPtr store.
        // published via a synchronizing store before any other thread
        // reads it -- the caller (`Graph::insert`, handing this `Node` to
        // `NodeTable::insert`) does so through that table's
        // `AtomicPtr::store`, which publishes the boxed handle and,
        // transitively, the block it points to.
        let ptr = unsafe { alloc_node(row_id, &vector, level, mmax0, mmax) };
        Self(
            std::ptr::NonNull::new(ptr)
                .expect("alloc_node never returns null (it asserts internally)"),
        )
    }

    // `cast_ptr_alignment` (here and in `vector`/`layer` below): every cast
    // is provably aligned -- `alloc_node` returns a pointer aligned to the
    // whole block layout's alignment, and every offset comes from the same
    // `compute_node_layout` `Layout::extend` arithmetic that aligned each
    // region to its own type's alignment (see the matching allow on
    // `alloc_node` itself).
    #[allow(clippy::cast_ptr_alignment)]
    fn header(&self) -> &NodeHeader {
        // SAFETY[IDX-NODE-HEADER]: This Node always holds the live allocation with its initialized header at offset zero.
        // and initializes a `NodeHeader` at offset 0 (see
        // `compute_node_layout`) before returning; this `Node` is never
        // constructed from any other pointer.
        unsafe { &*self.0.as_ptr().cast::<NodeHeader>() }
    }

    // Not read by any production code path yet — `Graph`'s own methods key
    // everything off the caller-supplied `row_id` parameter directly rather
    // than reading it back off a resolved `Node`. Kept as a natural
    // companion accessor to `vector()`/`level()`/`layer()` for a future
    // consumer (and exercised by this module's own
    // `vector_and_row_id_are_preserved` test).
    #[allow(dead_code)]
    pub(crate) fn row_id(&self) -> u64 {
        self.header().row_id
    }

    #[allow(clippy::cast_ptr_alignment)]
    pub(crate) fn vector(&self) -> &[f32] {
        let header = self.header();
        // `vector_byte_offset` is the non-allocating twin of the
        // `compute_node_layout` call `alloc_node` used to build this block
        // (drift-guarded by a test in node_layout.rs) -- this accessor sits
        // on `search_layer`'s hot path and must not allocate.
        let vector_offset = vector_byte_offset(header.dim as usize);
        // SAFETY[IDX-NODE-VECTOR]: The initialized vector range is reserved by the same layout arithmetic used at allocation.
        // and fully initialized by `alloc_node` using this same
        // `header.dim`.
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr().add(vector_offset).cast::<f32>(),
                header.dim as usize,
            )
        }
    }

    /// This node's highest layer — it participates in layers `0..=level()`.
    pub(crate) fn level(&self) -> usize {
        usize::from(self.header().level)
    }

    /// The `SlotArray` view for layer `lc`. Panics if `lc > self.level()` —
    /// callers must never traverse a node at a layer it doesn't
    /// participate in (checked by `Graph`'s traversal logic, not here).
    #[allow(clippy::cast_ptr_alignment)]
    pub(crate) fn layer(&self, lc: usize) -> SlotArray<'_> {
        let header = self.header();
        assert!(
            lc <= usize::from(header.level),
            "layer {lc} requested but node's level is {}",
            header.level
        );
        // Non-allocating twin of the `compute_node_layout` call `alloc_node`
        // used to build this block (drift-guarded by a test in
        // node_layout.rs) -- hot-path accessor, must not allocate.
        let start = layer_byte_offset(
            header.dim as usize,
            usize::from(header.level),
            usize::from(header.mmax0),
            usize::from(header.mmax),
            lc,
        );
        let capacity = Self::layer_capacity(header, lc);
        // SAFETY[IDX-NODE-LAYER]: The initialized layer range is reserved by the same drift-guarded layout arithmetic.
        // exactly the byte range `alloc_node` reserved and initialized
        // (every slot set to `EMPTY`) for layer `lc`, per the same
        // layout arithmetic (`layer_byte_offset` mirrors
        // `compute_node_layout`, drift-guarded by test).
        let slots = unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().add(start).cast::<AtomicU64>(), capacity)
        };
        SlotArray::new(slots)
    }

    /// Physical slot count for layer `lc`: the steady-state target
    /// (`mmax0` at layer 0, `mmax` above) plus one slot of transient
    /// headroom, so `Graph::insert`'s shrink step (Algorithm 1 lines
    /// 12-16) can ever observe an over-target list at all -- without it,
    /// `SlotArray::claim` just fails once full and the shrink step is
    /// structurally unreachable (Task 8 review finding; see
    /// `insert_shrinks_a_full_neighbor_list_to_keep_the_closer_candidate`
    /// in graph.rs). Delegates to `layer_slot_count` — the same rule
    /// `compute_node_layout` used to reserve this block — so the two can
    /// never diverge (drift-guarded by
    /// `byte_offset_helpers_match_compute_node_layout` in `node_layout.rs`).
    fn layer_capacity(header: &NodeHeader, lc: usize) -> usize {
        layer_slot_count(usize::from(header.mmax0), usize::from(header.mmax), lc)
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.header().deleted.load(Ordering::SeqCst) != 0
    }

    // Called from `Graph::delete`, which is now reachable in production
    // through `HnswIndex::remove` (see `Graph::delete`'s doc comment in
    // graph.rs) — so this is no longer transitively dead and needs no
    // `#[allow(dead_code)]`. Also exercised by this module's own
    // `mark_deleted_is_observed_by_is_deleted` test and `graph.rs`'s
    // deletion tests.
    pub(crate) fn mark_deleted(&self) {
        self.header().deleted.store(1, Ordering::SeqCst);
    }

    /// Whether this node's own `Graph::insert` call has finished
    /// connection-building. Closes a race distinct from (and found after)
    /// the shrink-step hazard documented on `Graph::insert` itself: a node
    /// is visible in `NodeTable` — and so can be *found* as a search
    /// candidate — from the moment its slot is claimed, well before its
    /// own edges at every layer exist. A concurrent insert's descent from
    /// a higher layer to a lower one picks the nearest candidate as its
    /// new entry; if that candidate is this not-yet-published node, the
    /// descent seeds `search_layer` at a node with an EMPTY neighbor list
    /// at the lower layer, and the traversal returns nothing but that node
    /// itself — the concurrent insert's own candidate set collapses to 1,
    /// and it ends up with only a handful of edges instead of the dozens a
    /// sequential insert would give it. Measured (200-row/20-cluster
    /// fixture, production parameters): excluding unpublished nodes from
    /// entry selection specifically (not from ordinary candidate
    /// selection, which must stay untouched) took real commit-path
    /// failures from ~28.5% down to 0.0% across hundreds of trials.
    pub(crate) fn is_published(&self) -> bool {
        self.header().published.load(Ordering::SeqCst) != 0
    }

    /// Marks this node published — called once, at the end of `Graph::
    /// insert`, after every layer's connections are built. Never reset:
    /// there is no "un-publish", only the one transition from under-
    /// construction to done.
    pub(crate) fn mark_published(&self) {
        self.header().published.store(1, Ordering::SeqCst);
    }
}

/// Random level assignment per the paper: `l = floor(-ln(unif(0,1)) * mL)`.
/// `mL = 1/ln(M)` is "a simple choice for the optimal mL" per the paper —
/// callers pass `1.0 / (m as f64).ln()`.
// The float-to-usize cast below can only ever produce a level, never a
// negative or fractional one (see the doc comment above and `pack`'s own
// doc comment in graph.rs, which clamps whatever this returns to
// `LEVEL_MASK` anyway) — `floor()` already removes the fractional part, and
// `-unif.ln() * m_l` is non-negative for `unif` in `(0, 1)` (a negative
// natural log negated stays non-negative). `as usize` saturates rather than
// wrapping for out-of-range floats (including the `unif == 0.0` ->
// `f64::INFINITY` case this function's own doc comment flags as the one
// real reachable edge case), so this is a safe, intentional cast, not a
// silently-truncating one.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
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
    fn node_is_neither_clone_nor_copy() {
        trait AmbiguousIfImpl<Marker> {
            fn probe() {}
        }
        struct Fallback;
        struct CloneMarker;
        struct CopyMarker;
        impl<T: ?Sized> AmbiguousIfImpl<Fallback> for T {}
        impl<T: Clone> AmbiguousIfImpl<CloneMarker> for T {}
        impl<T: Copy> AmbiguousIfImpl<CopyMarker> for T {}

        let _ = <Node as AmbiguousIfImpl<_>>::probe;
    }

    #[test]
    fn new_node_participates_in_layers_zero_through_level() {
        let node = Node::new(0, vec![1.0, 2.0, 3.0], 2, 32, 16);
        assert_eq!(node.level(), 2);
        assert_eq!(
            node.layer(0).capacity(),
            33,
            "layer 0 uses mmax0 + 1 headroom slot"
        );
        assert_eq!(
            node.layer(1).capacity(),
            17,
            "layer 1 uses mmax + 1 headroom slot"
        );
        assert_eq!(
            node.layer(2).capacity(),
            17,
            "layer 2 uses mmax + 1 headroom slot"
        );
        // SAFETY[IDX-NODE-TEST-RECLAIM-LAYERS]: This test exclusively owns its freshly constructed node.
        // stored in a `NodeTable`, and nothing else references it.
        unsafe {
            node.reclaim();
        }
    }

    #[test]
    fn level_zero_node_has_exactly_one_layer() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert_eq!(node.level(), 0);
        assert_eq!(node.layer(0).capacity(), 33, "mmax0 + 1 headroom slot");
        // SAFETY[IDX-NODE-TEST-RECLAIM-LEVEL-ZERO]: This test exclusively owns its test-local node.
        unsafe {
            node.reclaim();
        }
    }

    #[test]
    fn new_node_is_not_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        assert!(!node.is_deleted());
        // SAFETY[IDX-NODE-TEST-RECLAIM-DELETE-STATE]: This test exclusively owns its test-local node.
        unsafe {
            node.reclaim();
        }
    }

    #[test]
    fn mark_deleted_is_observed_by_is_deleted() {
        let node = Node::new(0, vec![1.0], 0, 32, 16);
        node.mark_deleted();
        assert!(node.is_deleted());
        // SAFETY[IDX-NODE-TEST-RECLAIM-DELETED]: This test exclusively owns its test-local node.
        unsafe {
            node.reclaim();
        }
    }

    #[test]
    fn vector_and_row_id_are_preserved() {
        let node = Node::new(7, vec![1.0, 2.0, 3.0], 0, 32, 16);
        assert_eq!(node.row_id(), 7);
        assert_eq!(node.vector(), &[1.0, 2.0, 3.0]);
        // SAFETY[IDX-NODE-TEST-RECLAIM-VECTOR]: This test exclusively owns its test-local node.
        unsafe {
            node.reclaim();
        }
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

/// Run with: `cargo rustc -p strata-index --lib --profile test -- --cfg loom`
/// (never a workspace-wide `RUSTFLAGS` — see `docs/audit/phase-1/audit.md`).
#[cfg(loom)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod loom_tests {
    use super::*;

    /// A thread that only observes a node through a published `AtomicPtr`
    /// must see the WHOLE node fully initialized -- header, vector, and
    /// every slot preset to EMPTY -- never a partially-constructed node.
    /// This is the test most likely to catch a release/acquire ordering
    /// mistake in `alloc_node`'s publication path.
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
                    // SAFETY[IDX-NODE-LOOM-READ-PUBLISHED]: The non-null pointer was published after full initialization.
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
