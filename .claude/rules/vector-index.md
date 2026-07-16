---
paths:
  - "crates/index/**/*.rs"
---

# Vector Index (HNSW)

Read `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4 and §6 for the exact conflict-domain and manifest-format definitions this crate must implement against — the bullets below are guardrails, not a substitute.

- **Index mutations are represented as an append-only delta log (which nodes/edges changed), never in-place graph mutation.** This is what lets index changes commit atomically alongside row data instead of being patched in separately after the fact — don't reintroduce in-place mutation for a "quick" performance fix without a design discussion first.
- The index lives inside the same transaction boundary as row data — it is not an eventually-consistent side structure. Any code path that updates the index outside `crates/txn/`'s commit path is a bug.
- HNSW parameters (`max_nb_connection`, `ef_construction`, `ef_search`) are tuned via benchmarks (`bench/`), not guessed — cite the benchmark run when changing a default.
- `hnsw_rs` was chosen over `usearch`'s Rust bindings specifically to stay in pure Rust and avoid re-introducing a C++ core via FFI — that would undercut the reason the project switched to Rust in the first place (see `.claude/docs/decisions/0005-rust-over-cpp-reversal.md`). **Neither library exposes graph internals for a native delta log** — no HNSW library audited, in C++ or Rust, does. The transaction shim maintaining the delta log is entirely Strata's own code; budget real implementation time for it, don't assume the library gives you this for free.
- IVF-PQ and other index types are an explicit non-goal for v1 — don't add a second index type without checking the Non-Goals table in `.claude/docs/architecture.md` first.
