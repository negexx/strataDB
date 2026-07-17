//! HNSW vector index, append-only delta log. See
//! `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6.

pub mod brute_force;
pub mod delta_log;
pub mod hnsw;

pub use brute_force::{Neighbor, brute_force_search};
pub use delta_log::{DeltaEntry, read_delta_log, write_delta_log};
pub use hnsw::{
    EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers, VectorMatch,
};
