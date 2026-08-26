# Active decisions

This is the compact, current decision record. It replaces the active ADR directory. Historical
decisions remain summarized in [history/decisions.md](history/decisions.md). These records describe
intent and constraints; current source and tests determine what is actually implemented.

## 0003 - Snapshot isolation is the ceiling

**Status:** Accepted as the design ceiling; the current API is narrower.

Strata targets snapshot isolation rather than serializability. The implementation exposes immutable
snapshot reads and bounded transaction-base reads: scans (including predicate reads) and group
operations expose staged inserts, replacements, and deletes; lookup reflects staged replacements and
deletes only for physical row IDs already in the base snapshot, because staged inserts receive no
physical row ID until commit and cannot be looked up pre-commit. It also provides write-write
optimistic conflict detection. `vector_search` after staged writes returns a typed
unsupported-transaction-read error; this is not a full/general read/write query interface. Stronger
isolation requires a new decision and evidence that the extra coordination is worth its complexity.

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

## 0009 - Supported engine facade and package boundary

**Status:** Accepted for Phase 1.

`strata-txn`'s `Dataset`, `Snapshot`, and `Transaction` are the supported engine surface. Storage,
index, and query crates are internal implementation layers and are marked non-publishable; direct
use of those layers does not carry the facade's schema, conflict, or recovery guarantees. This
decision does not create a stable Python or administration API.

## 0010 - Deferred cross-process coordination seam

**Status:** Accepted as a future-design reservation; Phase 4 implementation is not approved.

Strata preserves the embedded, one-process/shared-`Dataset` boundary after the bounded Phase 1
correctness/durability and Phase 3 lifecycle slices. Those completed slices do not provide durable
process-boundary coordination. The storage core must not add native cross-process locks, independent
multi-writer publication, distributed transactions, or a second commit protocol at this stage.

The future coordination boundary must be versioned and typed. Its reserved contract includes:

- dataset identity and capability negotiation;
- protocol-version negotiation;
- expected-manifest-version preconditions for conditional publication;
- request IDs and idempotent retry semantics;
- typed contested-row conflicts and other typed failure results; and
- explicit visibility and durability acknowledgement semantics.

Phase 4 should begin only after evidence shows that separate-process workloads are real, current
shared-handle concurrency is a bottleneck, an IPC/RPC design fits the commit-latency budget, crash
recovery and stale-participant behavior are specified, and an operational owner exists for the
coordinator. The preferred first implementation is an optional single-owner actor/IPC/RPC bridge
around one authoritative `Dataset`, not independent openers or a distributed transaction engine.

This decision does not authorize implementation, dependency additions, or a new isolation level.

## 0011 - Stable client and administration surfaces

**Status:** Accepted and implemented within the embedded, single-process
boundary.

The supported client path is the `strata-txn` facade exposed through the
Rust API, the documented PyO3 package, and the `strata` administration CLI.
The query planner, schema migration surface, lifecycle reports, and
administration commands are versioned by the package/repository contract and
must reject unsupported or corrupt state loudly. Internal storage, index, and
query crates remain implementation layers rather than independent supported
APIs.

This decision does not add full serializability, cross-process coordination,
distributed transactions, or universal durability claims. On-disk schema and
manifest changes require explicit compatibility tests and a migration/recovery
path; unsupported future formats must return typed errors. Public-surface
changes require documentation, a regression test, and an update to the
compatibility notes before release. Lifecycle maintenance remains exclusive
and its measured stop-the-world/resource bounds remain part of the operational
contract.

## Decision rules

- Preserve the embedded, single-node scope and the one-process/shared-`Dataset` concurrency boundary.
- Do not claim durability, atomicity, or index consistency beyond the blockers in [status](status.md)
  and [phase-1-audit](audit/phase-1/audit.md).
- Supersede decisions with a new dated record when the design changes; do not rewrite history silently.
