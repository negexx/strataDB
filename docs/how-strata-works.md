# How Strata Works

Strata is an embedded, single-node database prototype for structured Arrow data and vector embeddings on local disk. It supports concurrent writers that share one in-process `Dataset` handle. The intended shared-handle commit boundary publishes row data and vector-index changes together through a manifest, but the [Phase 1 audit](audits/phase-1-sol-audit-report.md) found correctness, durability, and integrity blockers. Treat the [status ledger](status.md) as the implementation record; the [roadmap](roadmap.md) separates current behavior from future work.

## A write from start to finish

Callers create a `Transaction` with `Dataset::begin`. It records the version of the current immutable snapshot and buffers inserted Arrow batches plus requested tombstones. The transaction object is a write surface: it has no scan/search methods and does not expose buffered writes for read-your-own-writes.

At commit time, Strata assigns global row IDs to new rows, writes and fsyncs their Arrow data files, and builds/fsyncs an immutable HNSW segment when the batches carry vectors. Those files are still invisible because no manifest names them. The intended allocation contract is non-reuse, but restart recovery currently has a Phase 1 counterexample; a failed preparation or conflict may also leave unreachable orphans because cleanup is not implemented.

Next, the committing transaction takes the shared handle's commit lock. It reloads the latest snapshot and compares its write set with write sets committed since it began. This is write-write optimistic conflict detection. A conflict is returned as a typed error naming contested row IDs; when the bounded recent-write history is insufficient to prove safety, the transaction is conservatively rejected. There is no supported read-set conflict tracking.

For a clean commit, Strata derives a new manifest from that latest snapshot, adds data-file and segment entries, applies tombstones, and publishes the manifest. It then installs a new immutable in-memory snapshot. That order is the intended visibility boundary: a reader searches and scans only files and segments listed by its captured manifest, so the in-process path is designed to expose either the old version or the complete new version, never a row/index split. Directory-durability failure handling and end-to-end integrity remain blocked by the Phase 1 audit, so this is not a blanket power-loss guarantee.

## Reading a snapshot

`Dataset::snapshot` returns an immutable point-in-time view. Its manifest names a fixed set of immutable Arrow data files, tombstones, and index segments. A scan reads those named files and excludes tombstoned rows; a later manifest publication cannot alter the captured snapshot. This is the practical value of immutable files and manifests: readers retain a coherent earlier version while writers prepare and publish a later one.

A vector search uses that same snapshot's segment set. Each HNSW segment searches its own vectors for local candidates; Strata merges the candidates into a global top-k and applies the snapshot's live/tombstone filtering during traversal. The mechanism makes the row and index view come from one manifest, but it does not make approximate-neighbour recall self-proving. Recall, filtered behavior, and fan-out cost need the relevant benchmarks and tests. More segments mean more search fan-out, and no compaction currently limits that growth.

Snapshot reads are separate from `Transaction`. The code does not offer a transactional scan/search API, read-your-own-writes, or a general full read/write snapshot-isolation interface. The current contract is immutable snapshot reads plus write-write OCC for writers.

## Rows, updates, and deletion

Row IDs are intended to be dataset-global allocation values that are never reused, with gaps after failed writes remaining safe; restart recovery does not yet uphold that intent in all cases. `delete` adds a tombstone, so the physical bytes remain. `update` is a tombstone of the old physical row plus an insert of replacement data, which receives a new global row ID. Treating those physical IDs as stable logical identities would overstate the present API; the logical-identity model remains a Phase 1 question.

## Storage, queries, and index

Storage uses Arrow IPC data files and versioned manifests on the `LocalFs` backend. Manifests carry file entries, per-file statistics, tombstones, and immutable segment metadata. A predicate can first consult file or segment statistics and skip inputs that cannot contain a match; remaining data is evaluated in memory. Those steps are pruning and predicate primitives, not a SQL planner or a complete integrated query engine. Projection should not be described as a general storage-level selective-column-read guarantee merely because unused arrays are not constructed.

The vector index is a from-scratch HNSW implementation stored as immutable per-commit segments. Segment files contain validated format metadata and CRC-protected content. This replaces the retired pre-S1 mutable-graph/replay design; recovery loads the manifest's segments rather than replaying changes into a shared graph. Predicate-filtered ANN first uses pruning where available and builds a candidate/live constraint from the snapshot; it is not a guarantee that every selective predicate is cheap or that approximate recall is fixed by the architecture. [ADR 0008](decisions/0008-adopt-segmented-index-layout.md) records why the layout was chosen.

## Verification boundary

Rust's ownership model and safe-Rust rules address memory safety, but they do not by themselves prove the commit protocol or ANN quality. Normal tests cover behavior; targeted loom models explore selected lock and atomic interleavings; and the chaos harness kills/reopens processes at instrumented durability points. These are evidence sources with bounded workloads, not a blanket proof of every concurrency, crash, filtered-search, or performance case. The shared-handle transaction path is therefore under the Phase 1 audit described by the [status ledger](status.md).

## Scope boundary

The in-process `Dataset` handle owns the commit lock, row-ID allocator, recent-write history, and current snapshot. Independently opening the same directory—whether in another process or separately in the same process—does not share any of that state. Manifest publication is therefore not a cross-process conditional-CAS protocol.

The CLI is an MVP inspection/demo interface with fixed fixture assumptions. The Python extension is only a PyO3 linkage placeholder exporting `placeholder_version`, not a usable Python database API. There is also no compaction, vacuum, orphan cleanup, time travel, point lookup, schema catalog, migration compatibility layer, fork/branch/merge, object-store backend, distributed transaction protocol, full SQL, or additional ANN index family.

For current design decisions and historical trade-offs, see [the decision index](decisions/README.md) and [documentation history](history/README.md). Historical documents explain why Rust, the intended snapshot-isolation ceiling, and immutable segments were chosen; active guarantees are limited to what the source and [status ledger](status.md) describe.
