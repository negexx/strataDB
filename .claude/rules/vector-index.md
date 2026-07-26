---
paths:
  - "crates/index/**/*.rs"
---

# Vector Index (HNSW)

Read `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4 and §6 for the exact conflict-domain and manifest-format definitions this crate must implement against — the bullets below are guardrails, not a substitute.

- **The index is a set of immutable, self-contained segments — one per committing transaction — never a shared graph mutated in place.** A commit builds its segment outside the commit lock, fsyncs it, and publishes it by the same atomic manifest swap that publishes its rows; a snapshot's segment set is exactly its manifest's `segments` list. That is what lets index changes commit atomically alongside row data instead of being patched in separately after the fact. Don't reintroduce a shared mutable graph, or any "apply now, persist later" path, for a "quick" performance fix without a design discussion first. (This replaced an append-only delta log in S1 W3.2 — see `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` §0.2 and §1. The log's only job was reconstructing a graph that was never persisted; a segment *is* the persisted graph.)
- **Segments are immutable and never rewritten.** Deletion is the manifest's versioned tombstone set applied through the traversal filter, not a per-node flag and not a segment rewrite: a row committed in segment 3 and deleted at version 12 stays physically in segment 3 forever, and a snapshot at v11 must still see it.
- The index lives inside the same transaction boundary as row data — it is not an eventually-consistent side structure. Any code path that updates the index outside `crates/txn/`'s commit path is a bug.
- HNSW parameters (`max_nb_connection`, `ef_construction`, `ef_search`) are tuned via benchmarks (`bench/`), not guessed — cite the benchmark run when changing a default.
- `crates/index` is a from-scratch, fully lock-free HNSW implementation — it no longer depends on `hnsw_rs` (or `usearch`'s Rust bindings, or any other external ANN library). Full replacement was justified narrowly by a lock-free-concurrent-mutation requirement that no wrap/fork of an existing library could satisfy — see `docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md` and `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`. The only remaining external dependency is `anndists` (`simdeez_f` feature enabled), used narrowly for SIMD distance kernels — not for graph structure, traversal, or concurrency, all of which are this project's own code. **No HNSW library audited, in C++ or Rust, exposes graph internals for the segment serialization Strata's own on-disk format needs** — that codec (`crates/index/src/segment_{format,writer,reader}.rs`) is entirely Strata's own code regardless of backing implementation.
- IVF-PQ and other index types are an explicit non-goal for v1 — don't add a second index type without checking the Non-Goals table in `.claude/docs/architecture.md` first.
