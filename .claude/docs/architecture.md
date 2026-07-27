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
| Vector index | `crates/index/` | HNSW, filtered similarity search, immutable per-commit on-disk segments (not in-place graph edits) |
| Transaction & conflict layer | `crates/txn/` | OCC, snapshot isolation, row/key-range conflict detection, atomic row+index commits — the flagship subsystem |
| Client bindings | `crates/bindings/` | PyO3 Python bindings (builds `strata_ext`), including an explicit transaction API (`begin`/`commit`/retry-on-conflict) |
| CLI | `crates/cli/` | Dataset/manifest inspection (`strata` binary) |
| Correctness harness | `tests/sim/` | Jepsen-style real-process-kill fault injection, randomized fault injection (Phase 7) — not `madsim`-based, see the Cross-cutting concerns section below |

## Data flow

**Write path:** every write — single row or batch — is a transaction; there is no fire-and-forget ingestion mode. A transaction records the manifest version it started against, buffers its changes, and on commit: (1) conflict detection runs at row/key-range granularity against anything committed since the transaction started, (2) if clean, the row data and any vector-index delta are written durably (fsynced) together, (3) the manifest pointer is CAS'd to the new version, (4) only then is the write acknowledged to the caller.

**Read path:** a reader takes a snapshot of the current manifest version at transaction start and sees a consistent point-in-time view across both the row store and the vector index for that version — later commits are invisible to it, even if they land mid-read.

**Conflict path:** if the manifest pointer moved since a transaction's snapshot was taken, conflict detection runs before the CAS is attempted; a genuine conflict returns a typed error identifying the contested rows/keys to the caller (retry/merge is the caller's decision — no silent last-writer-wins by default).

## External dependencies

| Library | Purpose | Failure mode |
|---------|---------|--------------|
| `arrow` (arrow-rs) | In-memory columnar representation, SIMD-friendly, zero-copy | Version drift across the workspace — pinned once in `[workspace.dependencies]`, every crate inherits it |
| `anndists` (`simdeez_f` feature) | SIMD-accelerated distance kernels only — `crates/index`'s HNSW graph structure, traversal, and on-disk segment format are this project's own from-scratch code, not `hnsw_rs` (fully replaced; see `docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md`) | N/A — narrow-purpose SIMD kernel dependency only |
| `loom` | Exhaustive interleaving testing of locks/atomics/CAS loops in `crates/txn`/`crates/index` | N/A — dev/test-only, not shipped |
| `pyo3` / `maturin` | Python binding generation | ABI mismatch across Python versions — pin the target Python version per release build |
| `hnsw_rs` | Retained deliberately as `bench/`'s comparison baseline (`bench/benches/lockfree_vs_hnsw_rs_bench.rs`) — NOT used by `crates/index`'s production HNSW implementation, which fully replaced it (see the `anndists` row above) | N/A — bench-only dependency; do not "clean up" as apparently-unused |

## Cross-cutting concerns

- **Concurrency correctness:** the borrow checker rules out data races in *safe* Rust at compile time — a real guarantee, but it says nothing about whether the OCC/conflict-detection logic is actually correct under a given interleaving. `loom` exhaustively tests the interleavings of locks/atomics/CAS loops that matter, on every change to `crates/txn/` or `crates/index/`. Phase 7's harness (`tests/sim/`) does NOT use `madsim`/`turmoil` — both were found to be async/tokio-shaped and a poor fit for this codebase's entirely synchronous production code (see `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md` §2). It instead follows Jepsen's methodology directly: real process spawn, real `std::process::abort()` at instrumented checkpoints, seed-reproducible scenarios. This is still the concrete payoff of the Rust-over-C++ reversal: `loom`'s exhaustive interleaving search has no C++ equivalent, and ADR 0004 (superseded) spent real effort designing weaker workarounds for that exact gap. See ADR 0005.
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
| 4. Vector Index (HNSW) | Build + search, then filtered ANN — **the monolithic baseline; superseded as the target layout by Phase S1 below** ([ADR 0008](decisions/0008-adopt-segmented-index-layout.md)) | Recall@10/QPS benchmarked on a public embedding dataset |
| 5. Single-Writer MVCC & Snapshot Isolation | Manifest-based snapshots, readers never blocked | Concurrent-reader suite passes against a single writer |
| 6. Multi-Agent Concurrent Write Engine (flagship) | OCC, row-level conflict detection, atomic row+index commits, zero-buffering durability | The "v0.3 concurrent multi-agent write slice" checklist (below) passes under real concurrent load |
| 7. Correctness Harness / Chaos Testing | Deterministic simulation testing, randomized fault injection | Thousands of randomized concurrent-agent runs, zero invariant violations |
| 8. Versioning & Dataloader Path | Time travel (no read-as-of API exists yet — only `snapshot()`/`current_version()`), compaction, `get_batch`/`iter_shuffled`. **Index-segment compaction/GC is Phase S2 below.** | A toy training loop reads a full epoch faster than raw Parquet |
| 9. Object Storage Backend | Same format/manifest logic against object storage | Full Phase 1-7 suite passes unmodified against the object-storage backend |
| 10. Bindings, Hardening, Benchmarking | Python bindings, CLI polish, full benchmark suite, public writeup | Graduation criteria met and documented publicly |

**Flagship milestone — "v0.3: concurrent multi-agent write slice"** (end of Phase 6): N simulated agents issue concurrent transactions (some conflicting, some not) against one shared dataset; every acknowledged write is durable and visible to the next reader; conflicting transactions get a typed error identifying contested rows; a transaction writing a row + updating the index commits both atomically or neither; a reader's open snapshot never sees a partial write from a later commit; the scenario re-runs under randomized process kills for many iterations with zero invariant violations.

### Segmented-layout & branching track ([ADR 0008](decisions/0008-adopt-segmented-index-layout.md))

Branching is a mandatory capability ([ADR 0008](decisions/0008-adopt-segmented-index-layout.md), Accepted), and it is only possible on a segmented immutable index — so the vector index moves off today's monolithic mutable HNSW. These phases are *additional* to 0–10 above, not a renumbering. The gating de-risk (recall-vs-segment-count) **has already run** (`bench/benches/segment_recall_bench.rs`): recall is segment-count-safe (0.974→0.998 across K=1→64), so the cost is latency, not correctness — compaction bounds latency, it is not load-bearing for recall. See [`scope-addendum-v2.md`](scope-addendum-v2.md) for the argument and [`FUTURE.md`](FUTURE.md) for everything deferred/refused.

| Phase | Goal | Exit Criterion |
|---|---|---|
| S1. Segmented immutable index layout — **[full spec →](design/phase-s1-segmented-index-spec.md)** | Replace the monolithic mutable HNSW with immutable segment files + a segment manifest; writes land in a new delta segment; queries fan out across segments and merge; recovery loads the manifest instead of rebuilding the graph. Includes per-segment **zone maps** (addendum §1.2 — per-segment min/max in the manifest) and their two prerequisites the current code lacks: **compound predicates** (`Predicate` is a flat `Eq/Lt/…` enum today, no `AND`/`OR`) and a **first-class timestamp/commit-time column** (only integer manifest versions exist now). | Fan-out search holds recall parity with the monolithic baseline (already shown recall-safe by `segment_recall_bench`); `Dataset::open` recovery drops from full-graph-rebuild (measured ~36 s @ 25k rows in `lifecycle_bench`) to a manifest load; a `timestamp ≥ X AND category = Y` predicate prunes whole segments before any vector is touched. |
| S2. Segment compaction & GC (extends Phase 8) | Merge segments to bound fan-out latency; **physically purge** soft-deleted nodes (real vector deletion + memory reclaim — today the graph only ever grows and deletes are soft); GC segments referenced by no live snapshot or branch (addendum §7 Q1, *unsolved*). | Segment count stays bounded under sustained writes; a deleted vector is provably gone from every segment (precondition for verifiable deletion); the chaos harness never reclaims a segment a live reader or branch still references. |
| B. Branching — the v2 thesis | `fork → fast abort → branch-scoped snapshot reads → merge`, in the addendum's shipping order, on the S1 layout. Fork is a manifest copy; abort discards a branch's delta segments; reads are snapshot-consistent across a branch's segment set; merge replays the branch's logical insert/delete set and rebuilds affected segments (correct-but-slow is acceptable). | Fork is O(segments) with **no index rebuild**; abort is O(mutations) and genuinely fast (the agentic hot path); ANN on a branch never sees another branch's writes; BranchBench (arXiv 2604.17180) passes under the chaos harness with many concurrent short-lived branches. |

**Where this slots — resolved: S1 landed first.** ADR 0008 argued layout-first because the segmented design is *not retrofittable*, and that is what happened: S1 (all five workstreams, W1-W5) merged and its exit criteria are met — see `design/phase-s1-segmented-index-spec.md` §9's closure status note. S1 was a genuine **migration**, not the "near-zero implementation delta" the addendum assumed for a greenfield choice — it replaced the shared mutable graph, the watermark-plus-exclusion-set visibility check, and delta-log replay recovery with segments, a tombstone-only visibility check, and manifest-driven segment loading. The next open question is no longer sequencing but scope: a hardening pass re-establishing Phase 6/7's correctness claims against the now-current segmented code (new chaos-harness workload coverage, a concurrent-segment-publication loom model) is the recommended next phase before S2 or Phase B — see the S1 spec's closure note for the specific gaps found.

**v3 storage primitives** (staleness tracking, verifiable deletion — needs S2, budget-shaped ANN `recall ≥ 0.9 OR cost ≤ X`) stay in [`FUTURE.md`](FUTURE.md); each stops deliberately short of the system that would consume it. Learned indexes: read, don't build (v4+, addendum §3.2).

## Non-Goals (cut list — revisit only after Phase 7)

| Cut | Why |
|---|---|
| Full serializability (snapshot isolation only) | Research-grade problem on a mutable vector index; SI covers the real target use cases |
| Multi-node/distributed transactions | Single-node/embedded only; distributed consensus is a different project |
| Full SQL parser/optimizer | Years of work; expression API covers the same queries |
| IVF-PQ / additional vector index types | HNSW alone is a complete v1; splitting effort steals hours from Phase 6 |
| Automatic/implicit conflict resolution | Silent resolution hides bugs; explicit surfacing is safer to get right first |
| Temporal/knowledge-graph memory features | Different skill set (NLP/graph extraction) than storage-engine correctness; memory is a *reference app on* Strata, not built *into* it — see [addendum §5](scope-addendum-v1.md) |
| Catalog integrations, geospatial, full-text search | Product-surface features, not the differentiator |
| Object storage as the primary backend | Local disk first; cloud backend is Phase 9 |
| Derivation engine (IVM over model calls) | That's an orchestrator + separate product; ship the staleness *primitive* and stop — [addendum §4](scope-addendum-v1.md) |
| Probe optimiser / cost-quality query planning | Requires a query optimiser → a query language; Strata is a storage engine (category error) |
| Belief semantics (bi-temporal validity, confidence, retraction cascades) | A *data model*; the moment the engine holds opinions about a "claim" it stops being a storage engine — build it *on* Strata (zone maps make temporal *filtering* fast; that's the storage boundary) |
| Neural databases (NeuroDB / SNH / NGDB) | Approximate range-aggregate query processing over a fixed distribution — a technique, not a database class, orthogonal to Strata ([addendum §4](scope-addendum-v2.md)) |
| Extreme multi-tenancy (fork) | Architectural fork, not a feature; embeddable-first makes million-tenant ~free (run N instances) |

The rows above come from [Scope Addendum v2](scope-addendum-v2.md) §4 — refused deliberately and recorded so they aren't relitigated as the space moves. Deferred (not refused) items live in [`FUTURE.md`](FUTURE.md).

## What this doc is NOT

- Not an exhaustive file list — that's discoverable
- Not API documentation — that lives near the code
- Not a tutorial — see the top-level README for getting started
