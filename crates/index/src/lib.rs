//! HNSW vector index, append-only delta log. See
//! `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6.

pub mod brute_force;
pub mod delta_log;
#[cfg(feature = "internal-benchmarks")]
pub mod distance;
#[cfg(not(feature = "internal-benchmarks"))]
mod distance;
#[cfg(feature = "internal-benchmarks")]
pub mod graph;
#[cfg(not(feature = "internal-benchmarks"))]
mod graph;
pub mod hnsw;
mod node;
mod node_table;
mod slot_array;

pub use brute_force::{Neighbor, brute_force_search};
pub use delta_log::{DeltaEntry, read_delta_log, write_delta_log};
pub use hnsw::{
    EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers, VectorMatch,
};
