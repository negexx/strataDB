//! HNSW vector index, append-only delta log. See
//! `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6.

pub mod brute_force;
pub mod delta_log;
// `#[doc(hidden)] pub`, not a `internal-benchmarks` Cargo feature: an
// earlier version of this gated `pub` behind a feature so `graph`/
// `distance` stayed private in every "normal" build. That doesn't
// actually work for a workspace member: Cargo unifies a package's
// features across every unit in the same build graph, so any workspace
// crate requesting the feature (even only as a dev-dependency, even only
// for its own bench/test targets) makes rustc compile -- and every other
// workspace crate link against -- the SAME feature-enabled rlib under
// `cargo build --workspace`/`cargo test --workspace`/`cargo clippy
// --workspace --all-targets` (this project's actual CI commands, see
// .github/workflows/ci.yml). Verified empirically: it silently defeated
// itself, leaking `pub` into every workspace crate's production build.
// `#[doc(hidden)] pub` has no such hazard -- it's unconditionally `pub`
// (so `bench/`'s benchmark can name these types with zero Cargo
// resolver subtlety), just excluded from generated rustdoc and clearly
// marked as not a supported API. `strata-index` is an internal,
// unpublished workspace crate (no external consumers), so "technically
// reachable via `strata_index::graph::Graph`" carries none of the
// compatibility risk it would for a published library -- the real hard
// constraint (design doc: `HnswIndex`'s own public method signatures
// must never need to change) is untouched either way.
#[doc(hidden)]
pub mod distance;
#[doc(hidden)]
pub mod graph;
pub mod hnsw;
mod node;
mod node_table;
mod slot_array;

pub use brute_force::{Neighbor, brute_force_search};
pub use delta_log::{DeltaEntry, read_delta_log, write_delta_log};
pub use hnsw::{
    EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers, VectorMatch,
};
