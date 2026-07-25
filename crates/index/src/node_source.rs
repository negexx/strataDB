//! Abstracts element access during HNSW traversal (neighbors, vectors, the
//! deleted flag) away from the live, mutable `Graph<D>` — so `search_layer`/
//! `k_nn_search`'s algorithm bodies can run unchanged over either today's
//! live graph or (starting in W3.2) an immutable on-disk segment. See
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §2, as corrected by `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md`
//! §2 (the `is_deleted` method below is the amendment's addition — the base
//! doc's sketch omitted it).
//!
//! Addressed in `u64` "local ids" universally: for a live graph the local id
//! IS the row-id (`row_id` is the identity function); a future segment's
//! local id is a `u32` ordinal, zero-extended.
//!
//! **Reentrancy precondition:** implementations of this trait must never
//! borrow `crate::graph`'s thread-local `SEARCH_SCRATCH` from within any of
//! their methods. `search_layer_generic` (in `crate::graph`) calls every
//! `NodeSource` method from inside its own active
//! `SEARCH_SCRATCH.with_borrow_mut` closure, so a `NodeSource` method that
//! itself tried to borrow `SEARCH_SCRATCH` — directly, or transitively
//! through some other function that does — would hit a nested
//! `RefCell::borrow_mut` and panic. `Graph<D>`'s own implementation below
//! satisfies this today (it touches no scratch state); a future
//! `NodeSource` implementation (e.g. a segment reader, from W3.2) must
//! preserve it too.

/// Traversal-time element access, implemented once per graph representation
/// (today: `Graph<D>`; from W3.2: a segment reader).
pub trait NodeSource {
    /// `(local id, level)` of the current entry point, or `None` if empty.
    fn entry_point(&self) -> Option<(u64, usize)>;
    /// The level of the node at `local`, or `None` if it doesn't exist.
    fn level(&self, local: u64) -> Option<usize>;
    /// Appends `local`'s neighbor ids at `level` into `out`, clearing `out`
    /// first. A no-op (after clearing) if `local` doesn't exist or has no
    /// slot array at `level`.
    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>);
    /// The vector stored at `local`, or `None` if it doesn't exist.
    fn vector(&self, local: u64) -> Option<&[f32]>;
    /// The row-id `local` corresponds to.
    fn row_id(&self, local: u64) -> u64;
    /// The established vector dimension of this source, or `0` if none yet.
    fn dimension(&self) -> usize;
    /// Whether `local` is tombstoned. Defaults to `false` — a segment (from
    /// W3.2) has no per-node deleted flag; deletion there is a manifest-level
    /// tombstone applied by the caller's `filter`, not this method. The live
    /// graph overrides this to check `Node::is_deleted()`, since
    /// `GraphResidueGuard`'s soft-delete mechanism is still active through
    /// W3.2a (see the amendment's §2 for why this default can't be omitted).
    fn is_deleted(&self, _local: u64) -> bool {
        false
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::NodeSource;

    // A minimal, independent NodeSource impl (not Graph<D>) — proves the
    // trait's default `is_deleted` compiles and behaves as documented
    // without depending on Graph<D>'s own test suite covering it.
    struct Stub;
    impl NodeSource for Stub {
        fn entry_point(&self) -> Option<(u64, usize)> {
            None
        }
        fn level(&self, _local: u64) -> Option<usize> {
            None
        }
        fn neighbors_into(&self, _local: u64, _level: usize, out: &mut Vec<u64>) {
            out.clear();
        }
        fn vector(&self, _local: u64) -> Option<&[f32]> {
            None
        }
        fn row_id(&self, local: u64) -> u64 {
            local
        }
        fn dimension(&self) -> usize {
            0
        }
    }

    #[test]
    fn is_deleted_defaults_to_false_when_not_overridden() {
        assert!(!Stub.is_deleted(0));
    }

    #[test]
    fn row_id_is_the_identity_function_for_a_stub_source() {
        assert_eq!(Stub.row_id(42), 42);
    }
}
