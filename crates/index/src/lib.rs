//! HNSW vector index, append-only delta log. See
//! `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6.

pub mod brute_force;

pub use brute_force::{Neighbor, brute_force_search};
