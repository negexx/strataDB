# Architecture — Strata

Strata is an embedded, single-node, local-disk research/prototype database for structured Arrow data and vector embeddings. Its supported concurrency scope is concurrent writers in **one process sharing one `Dataset` handle**. Within that boundary, the intended contract is that a successful commit atomically publishes its row data and immutable vector-index segments through a manifest and is acknowledged only after it is durable, conflict-checked, and visible. The [Phase 1 audit](audits/phase-1-sol-audit-report.md) found in-scope counterexamples and durability/integrity gaps, so this contract remains Partial and blocked rather than an achieved universal guarantee.

For implementation status and known gaps, start with the [status ledger](status.md) and [capability roadmap](roadmap.md). Historical design rationale lives in [documentation history](history/README.md) and the current decisions in [the decision index](decisions/README.md).

## Implemented, partial, and absent

| Boundary | State | Current meaning |
|---|---|---|
| Local storage and manifests | Implemented | Arrow data files, per-file statistics/encoding metadata, immutable version manifests, and the `LocalFs` backend. |
| Shared-handle commits | Partial | The shared-handle commit path exists: write-write OCC, immutable snapshots, typed row-ID conflicts, and one manifest boundary for rows and vector segments. The Phase 1 audit is complete and this boundary remains blocked on the findings in the [consolidated report](audits/phase-1-sol-audit-report.md). |
| Vector index | Implemented | From-scratch HNSW stored as immutable per-commit segment files; searches fan out across manifest-listed segments and filter live/tombstoned rows. Segment parsing validates its format and CRCs. |
| Query primitives | Partial | Predicates, file-stat pruning, and in-memory group-by primitives exist. They are not a planner-integrated SQL/query engine. |
| CLI and Python | Partial | The CLI is an MVP inspection/demo tool with fixed assumptions. The PyO3 module exports only `placeholder_version`; it is not a database API. |
| Lifecycle and broader coordination | Not implemented | No compaction, vacuum, orphan cleanup, point lookup, time travel, schema catalog, migration layer, branch/fork/merge, object-store backend, cross-process protocol, distributed transactions, SQL parser, or alternate ANN family. |

## Components

| Component | Path | Responsibility |
|---|---|---|
| Storage | `crates/storage` | Arrow IPC data files, manifests/version records, per-file statistics and encoding metadata, and local filesystem persistence. |
| Transactions | `crates/txn` | `Dataset`, snapshots, row-ID allocation, write-write conflict checking, commit ordering, tombstones, and row/index publication. |
| Index | `crates/index` | From-scratch HNSW construction/search plus immutable segment encoding, validation, loading, and fan-out search. |
| Query | `crates/query` | Predicate evaluation, pruning decisions, and group-by primitives. |
| CLI | `crates/cli` | Fixed-shape demonstration and inspection commands. |
| Bindings | `crates/bindings` | A placeholder PyO3 extension module only. |

## Commit and snapshot lifecycle

1. `Dataset::begin` captures the current immutable snapshot's version and returns a write-only `Transaction`. The transaction buffers inserted batches and tombstones; it has no transactional scan or search API and does not provide read-your-own-writes.
2. `commit` assigns new global row IDs to inserts, prepares Arrow data batches and any immutable HNSW segment, and fsyncs those new files. A failed preparation may leave unreachable files on disk; cleanup is not implemented.
3. Under the shared handle's in-process commit lock, the transaction reloads the latest snapshot and checks its write set against commits since its captured version. Only write-write conflicts are tracked. A conflict is a typed error identifying contested row IDs; insufficient retained history is rejected conservatively.
4. A clean transaction creates a new manifest from the latest state, including data files, tombstones, and segment metadata, then publishes that manifest. The manifest transition is the intended visibility boundary: unreferenced data and segment files are not scanned or searched. Successful namespace publication does not by itself discharge the Phase 1 audit's currently blocked directory-durability and end-to-end integrity requirements.
5. Only after manifest publication succeeds does the handle install a new immutable snapshot. Existing snapshots retain their earlier manifest, segment set, and tombstone set; a newly obtained snapshot sees the committed version.

Publication is lock-serialized in one `Dataset` handle. It is not a storage-level conditional compare-and-swap protocol for independently opened handles or processes: those openers do not share the lock, allocator, recent-write history, or current in-memory snapshot.

## Data, query, and index behavior

Row data is append-only Arrow IPC files. New commits add files; they do not rewrite files already named by an earlier manifest. A manifest identifies the data files, tombstones, and index segments belonging to one immutable snapshot, which is why an existing reader can keep using its captured view while a later commit prepares another version. The manifest records each file and its available per-column statistics, but Strata has no complete schema catalog, compaction, vacuum, or orphan cleanup. Projection must not be presented as a general storage-level selective-read guarantee merely because unused arrays are not constructed.

An update tombstones the old physical row and writes replacement data with a newly allocated global row ID. Deletion is likewise tombstone-based. Stable logical-identity semantics are an open Phase 1 question; physical reclamation is absent.

The index is not a shared mutable graph. Each vector-carrying commit produces an immutable HNSW segment, and the manifest lists the segment set for each snapshot. Search asks applicable segments for local candidates, merges those candidates into a global top-k, and applies live/tombstone filtering during segment traversal. This provides the mechanics for a consistent segment set, not a proof of approximate-neighbour recall; recall and performance require measurement and test evidence. Segment count can grow with commits; no compaction or lifecycle management currently bounds that growth.

Predicates and per-file/per-segment statistics can rule out files or segments that cannot match. The available predicates, pruning, and in-memory group-by are useful primitives rather than a complete planner. Filtered ANN uses the predicate-derived live set and pruning gate while it searches the remaining segments. Its selectivity, recall, and cost boundaries are not a promise of a fully optimized filtered-query engine and remain part of Phase 1/2 evidence work.

## Deliberate boundaries

The intended isolation ceiling is snapshot isolation rather than serializability ([ADR 0003](decisions/0003-snapshot-isolation-not-serializability.md)). The implemented API is narrower: immutable snapshot reads plus write-write OCC, not a full read/write snapshot-transaction interface. Rust and loom remain part of the concurrency-correctness approach ([ADR 0005](decisions/0005-rust-over-cpp-reversal.md)): normal tests exercise behavior, targeted loom models explore selected shared-memory interleavings, and the chaos harness exercises crash/recovery scenarios. That evidence is valuable but bounded by the cases it covers, which is why the shared-handle path remains Partial in the Phase 1 audit. Immutable segments are the accepted index layout ([ADR 0008](decisions/0008-adopt-segmented-index-layout.md)).

Do not infer unsupported guarantees from those decisions. In particular, the current engine does not provide cross-process coordination, point lookup, complete schema management, full SQL, object storage, or branching. Those are tracked by the [roadmap](roadmap.md), not implied by the present implementation.
