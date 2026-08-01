# Active decisions

This is the compact, current decision record. It replaces the active ADR directory. Historical
decisions remain summarized in [history/decisions.md](history/decisions.md). These records describe
intent and constraints; current source and tests determine what is actually implemented.

## 0003 - Snapshot isolation is the ceiling

**Status:** Accepted as the design ceiling; the current API is narrower.

Strata targets snapshot isolation rather than serializability. The implementation currently exposes
immutable snapshot reads and write-write optimistic conflict detection, not a full read/write
transaction snapshot API. Stronger isolation requires a new decision and evidence that the extra
coordination is worth its complexity.

## 0005 - Rust over C++

**Status:** Accepted and implemented as the repository direction.

Rust/Cargo is the workspace foundation, with Arrow-rs, PyO3, loom, and process-level chaos tooling.
The vector index is a from-scratch HNSW implementation; `anndists` is retained for distance kernels.
The former C++ toolchain direction is historical.

## 0008 - Immutable segmented index layout

**Status:** Accepted and implemented for the current index path.

Each vector-bearing commit writes an immutable HNSW segment. The manifest lists segments; readers load
the manifest's segment set and fan out search before merging results. The retired mutable shared graph
and delta-log design must not be reintroduced. This decision does not imply compaction, branching,
universal recall, or a cross-process publication protocol.

## 0006 - Group commit

**Status:** Proposed; not implemented.

The proposal batches the manifest durability step while making every caller wait for the fsync that
covers its own commit. It must not become silent buffering or weaken the acknowledgement invariant.
Before acceptance it needs a version model, failure/recovery semantics, a loom model, bounded latency
policy, and workload measurements. Current commit serialization remains authoritative.

## Decision rules

- Preserve the embedded, single-node scope and the one-process/shared-`Dataset` concurrency boundary.
- Do not claim durability, atomicity, or index consistency beyond the blockers in [status](status.md)
  and [phase-1-audit](phase-1-audit.md).
- Supersede decisions with a new dated record when the design changes; do not rewrite history silently.
