# Architecture — Strata

> A map for AI agents and humans. Update when the shape of the system changes, not for every leaf-level edit.

## One-paragraph overview

Strata is an embedded, single-node database engine storing structured columns and vector embeddings in one unified columnar format, built to give multiple concurrent AI agents (or concurrent tool calls from one agent) real transactional guarantees — snapshot isolation, optimistic concurrency control, atomic row+index commits, zero silent write-buffering — that existing vector stores (LanceDB, Pinecone, Qdrant, pgvector) don't provide. The storage format, query engine, and HNSW vector index (Phases 0-4) are necessary foundations; they exist to serve the concurrency work (Phase 6), which is the actual differentiator.

## System diagram

```
Client Layer
  query API · dataloader API · CLI · Python (PyO3)
        |
        v
  +-----------------------+-----------------------+
  |                       |                       |
Query Executor       Vector Index          Random-Access Reader
(scan/filter/agg,    (HNSW over a          (row-id -> row,
 vectorized ops)      vector column)        dataloader path)
  |                       |                       |
  +-----------------------+-----------------------+
        |
        v
Transaction & Conflict Resolution Layer   <- the flagship layer. Every
  OCC · snapshot isolation ·                 read/write from above passes
  atomic row+index commits                   through here; nothing bypasses it.
        |
        v
Manifest / Version Layer
  which files belong to version N; commit = atomic CAS of this pointer
        |
        v
Columnar Storage Format
  local disk first; object storage backend is Phase 9
```

The transaction/conflict layer is an explicit, load-bearing architectural component, not an implicit property of "the manifest happens to version things." Commit is a single compare-and-swap of the current manifest version pointer; nothing is visible to any reader until that swap succeeds.

## Major modules

| Module | Path | Purpose |
|--------|------|---------|
| Columnar storage format | `crates/storage/` | Fixed-size pages/column chunks, dictionary + RLE encoding, validity bitmaps, append-only files |
| Manifest & versioning | `crates/storage/` (manifest) | Lists files per version; the commit atomicity boundary (CAS) |
| Query & execution engine | `crates/query/` | Small expression/filter API (no full SQL parser), vectorized batch operators, predicate pushdown |
| Vector index | `crates/index/` | HNSW, filtered similarity search, append-only delta log for mutations (not in-place graph edits) |
| Transaction & conflict layer | `crates/txn/` | OCC, snapshot isolation, row/key-range conflict detection, atomic row+index commits — the flagship subsystem |
| Client bindings | `crates/bindings/` | PyO3 Python bindings (builds `strata_ext`), including an explicit transaction API (`begin`/`commit`/retry-on-conflict) |
| CLI | `crates/cli/` | Dataset/manifest inspection (`strata` binary) |
| Correctness harness | `tests/sim/` | `madsim`-based deterministic simulation testing, randomized fault injection (Phase 7) |

## Data flow

**Write path:** every write — single row or batch — is a transaction; there is no fire-and-forget ingestion mode. A transaction records the manifest version it started against, buffers its changes, and on commit: (1) conflict detection runs at row/key-range granularity against anything committed since the transaction started, (2) if clean, the row data and any vector-index delta are written durably (fsynced) together, (3) the manifest pointer is CAS'd to the new version, (4) only then is the write acknowledged to the caller.

**Read path:** a reader takes a snapshot of the current manifest version at transaction start and sees a consistent point-in-time view across both the row store and the vector index for that version — later commits are invisible to it, even if they land mid-read.

**Conflict path:** if the manifest pointer moved since a transaction's snapshot was taken, conflict detection runs before the CAS is attempted; a genuine conflict returns a typed error identifying the contested rows/keys to the caller (retry/merge is the caller's decision — no silent last-writer-wins by default).

## External dependencies

| Library | Purpose | Failure mode |
|---------|---------|--------------|
| `arrow` (arrow-rs) | In-memory columnar representation, SIMD-friendly, zero-copy | Version drift across the workspace — pinned once in `[workspace.dependencies]`, every crate inherits it |
| `hnsw_rs` | HNSW vector index (pure Rust — chosen over `usearch`'s Rust bindings to avoid re-introducing a C++ core via FFI; see ADR 0005) | Less battle-tested at scale than hnswlib/Faiss/usearch, so index-heavy benchmarks (Phase 4) should watch for it specifically |
| `loom` / `madsim` / `turmoil` | Concurrency correctness testing — the reusable off-the-shelf DST tooling that made Rust the right call for this project | N/A — dev/test-only, not shipped |
| `pyo3` / `maturin` | Python binding generation | ABI mismatch across Python versions — pin the target Python version per release build |

## Cross-cutting concerns

- **Concurrency correctness:** the borrow checker rules out data races in *safe* Rust at compile time — a real guarantee, but it says nothing about whether the OCC/conflict-detection logic is actually correct under a given interleaving. `loom` exhaustively tests the interleavings of locks/atomics/CAS loops that matter, on every change to `crates/txn/` or `crates/index/`. Phase 7's deterministic-simulation harness (`tests/sim/`) builds on `madsim`/`turmoil` — real, maintained, reusable crates, not a from-scratch simulator. This is the concrete payoff of the Rust-over-C++ reversal: C++ had no equivalent to `loom`/`madsim`, and ADR 0004 (superseded) spent real effort designing weaker workarounds for that exact gap. See ADR 0005.
- **Durability:** a write is acknowledged only after fsync + conflict-check + commit — see `.claude/rules/concurrency-txn-layer.md`.
- **Observability:** `EXPLAIN`-style output, scan/row metrics, and a conflict log recording every detected conflict (which transactions, which keys) for debugging contention patterns.
- **Auth / feature flags:** N/A — Strata is an embedded engine, not a hosted multi-tenant service.

## Design Principles (non-negotiable, revisit only in an emergency)

1. **Correctness before features.** Every other feature is secondary to the concurrency guarantees holding under real concurrent load.
2. **No write is acknowledged until it's actually safe.** Durable, conflict-checked, visible — no async buffering, ever, even at a throughput cost.
3. **The vector index is not a second-class citizen.** Same transaction boundary as row data.
4. **Vertical slices over layers.** Every milestone runs end-to-end, however small in scope.
5. **Cut ruthlessly, document the cut.** See Non-Goals below — no scope creep back in without noticing.
6. **A benchmark is the source of truth.** Each phase ends with a number that goes up, or a chaos test that goes from failing to passing.
7. **Read the reference before rebuilding the wheel.** Study FoundationDB, CockroachDB, and Jepsen's methodology before designing Strata's version of correctness-under-concurrency.

## Roadmap (phases)

| Phase | Goal | Exit Criterion |
|---|---|---|
| 0. Foundations & Transaction Model Design | File format spec + explicit definition of "conflict" and transaction boundary — **done, see `docs/design/phase-0-transaction-and-format-spec.md`** | Spec reviewed against FoundationDB/CockroachDB consistency docs + Lance's format spec |
| 1. Vertical Slice (single-writer) | MVP: create dataset, insert, scan, filter, brute-force NN search, kill -9 + restart recovers last committed version | The 6-step checklist passes |
| 2. Columnar Core & Vectorized Execution | Real encodings, batch-based scan/filter/project/aggregate | `GROUP BY` over 10M+ rows, correct, benchmarked |
| 3. Query Layer Refinement | Predicate pushdown, file/chunk pruning | `EXPLAIN` proves a filtered query skips untouched files |
| 4. Vector Index (HNSW) | Build + search, then filtered ANN | Recall@10/QPS benchmarked on a public embedding dataset |
| 5. Single-Writer MVCC & Snapshot Isolation | Manifest-based snapshots, readers never blocked | Concurrent-reader suite passes against a single writer |
| 6. Multi-Agent Concurrent Write Engine (flagship) | OCC, row-level conflict detection, atomic row+index commits, zero-buffering durability | The "v0.3 concurrent multi-agent write slice" checklist (below) passes under real concurrent load |
| 7. Correctness Harness / Chaos Testing | Deterministic simulation testing, randomized fault injection | Thousands of randomized concurrent-agent runs, zero invariant violations |
| 8. Versioning & Dataloader Path | Time travel, compaction, `get_batch`/`iter_shuffled` | A toy training loop reads a full epoch faster than raw Parquet |
| 9. Object Storage Backend | Same format/manifest logic against object storage | Full Phase 1-7 suite passes unmodified against the object-storage backend |
| 10. Bindings, Hardening, Benchmarking | Python bindings, CLI polish, full benchmark suite, public writeup | Graduation criteria met and documented publicly |

**Flagship milestone — "v0.3: concurrent multi-agent write slice"** (end of Phase 6): N simulated agents issue concurrent transactions (some conflicting, some not) against one shared dataset; every acknowledged write is durable and visible to the next reader; conflicting transactions get a typed error identifying contested rows; a transaction writing a row + updating the index commits both atomically or neither; a reader's open snapshot never sees a partial write from a later commit; the scenario re-runs under randomized process kills for many iterations with zero invariant violations.

## Non-Goals (cut list — revisit only after Phase 7)

| Cut | Why |
|---|---|
| Full serializability (snapshot isolation only) | Research-grade problem on a mutable vector index; SI covers the real target use cases |
| Multi-node/distributed transactions | Single-node/embedded only; distributed consensus is a different project |
| Full SQL parser/optimizer | Years of work; expression API covers the same queries |
| IVF-PQ / additional vector index types | HNSW alone is a complete v1; splitting effort steals hours from Phase 6 |
| Automatic/implicit conflict resolution | Silent resolution hides bugs; explicit surfacing is safer to get right first |
| Temporal/knowledge-graph memory features | Different skill set (NLP/graph extraction) than storage-engine correctness; possible v2/v3 |
| Catalog integrations, geospatial, full-text search | Product-surface features, not the differentiator |
| Object storage as the primary backend | Local disk first; cloud backend is Phase 9 |

## What this doc is NOT

- Not an exhaustive file list — that's discoverable
- Not API documentation — that lives near the code
- Not a tutorial — see the top-level README for getting started
