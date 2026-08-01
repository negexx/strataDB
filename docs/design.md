# Current design

This document describes the implementation boundary of the storage, transaction, index, and query
layers. It is intentionally shorter than the historical specifications. When it conflicts with code,
tests and manifests, the checked-in implementation wins and the discrepancy becomes documentation or
engineering work.

## Storage and manifests

Data is stored in Arrow IPC files with JSON version manifests. A manifest identifies the data files,
tombstones, statistics, and immutable vector segments visible in a snapshot. Local filesystem
operations are behind the `Backend`/`LocalFs` boundary, although some paths remain local-disk-specific.
The manifest is intended to be the single publication boundary for row and index visibility.

The current design does not provide compaction, vacuum, orphan cleanup, bounded segment growth,
time-travel retention, or a schema catalog. Directory durability, manifest identity, and end-to-end
row/file integrity remain Phase 1 work; see the audit rather than treating the format as corruption
proof.

## Transactions and snapshots

`Dataset::begin` creates a write-only transaction against the current immutable snapshot. Preparation
allocates physical row IDs, writes data, and builds vector segments before the commit lock is taken.
Under the shared handle's lock, write-write conflicts are checked against recent committed history.
A clean commit publishes a new manifest and installs a replacement immutable snapshot.

The API does not provide transactional scan/search or read-your-own-writes. The supported concurrency
boundary is one process sharing one `Dataset` handle. Independent openers do not share the lock,
allocator, history, or in-memory snapshot, and cross-process conditional publication is Phase 4.

Rows are append-only physical records. Delete adds a tombstone. Update tombstones the old physical row
and inserts replacement data with a new physical row ID. Future tombstones, target validation,
cardinality, logical identity, and restart-safe non-reuse are unresolved contract items.

## Immutable vector segments

The index crate owns HNSW construction, distance kernels, segment encoding, defensive loading, topology
checks, bounds checks, and checksums. A segment maps local ordinals to global physical row IDs. A
snapshot loads the manifest-listed segment set; search fans out to applicable segments, merges top-k,
and filters tombstoned rows. This is a coherent segment-set design, not a universal recall or latency
claim. There is no compaction path yet.

## Query primitives

The query crate provides predicates, statistics/zone-map pruning, filtered ANN constraints, explain
information, and in-memory group-by primitives. It does not yet provide a complete planner, SQL
parser, stable client contract, or guaranteed cost bound for selective queries.

## Verification boundary

Normal unit/integration tests, loom models, property tests, fuzz targets, real-process chaos/reopen
tests, and Criterion benchmarks cover useful slices. Several important suites are opt-in or not yet
CI-gated, and historical measurements are not evidence for the current segmented implementation.
Phase 1 requires regression tests for known counterexamples, explicit corruption/durability scope,
CI-visible concurrency gates, and a current performance matrix.
