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

Within the local-filesystem and one-process/shared-`Dataset` boundary, publication has three
outcomes: acknowledged success, a definite failure before final-name publication, or indeterminate
publication. In the indeterminate case, the immutable final-name candidate is readable but directory
sync failed; no durability acknowledgement is reported. Reopening can establish visibility but not
durability, and independent handles have no coordination protocol.

The current design provides explicit snapshot-preserving compaction, age-based manifest retention,
and vacuum of recognized unprotected objects. It does not provide arbitrary orphan cleanup,
guaranteed bounded segment growth, or time-travel retention. Dataset-owned schema, manifest identity, row/file integrity, and durable
row-ID high-water checks are implemented within the named local bounds. The versioned schema
catalog supports one explicit, bounded evolution: version 1 to version 2 may add one nullable
logical column, rewriting row objects and copying listed immutable segments before one new manifest
is published. Target schemas must pass the same dataset preflight used by create and recovery; in
particular `_row_id` and `_timestamp` remain reserved physical names. General schema evolution,
arbitrary type changes, reverse migrations, and universal power-loss claims remain deferred. See
the audit rather than treating the format as corruption proof.

## Transactions and snapshots

`Dataset::begin` captures an immutable base snapshot for a transaction. The stable, bounded
transaction read API merges that base with the transaction's private staged writes: scans (including
predicate reads) and group reads expose staged inserts, replacements, and deletes. Lookup is only
for physical row IDs already present in the base snapshot; it reflects a staged replacement or delete
of such a row, while a staged insert has no physical row ID until commit and cannot be looked up
pre-commit. `vector_search` can use the base index only while the transaction has no staged writes;
after staged writes it returns a typed unsupported-transaction-read error rather than silently
returning stale base-snapshot results. Preparation allocates physical row IDs, writes data,
and builds vector segments before the commit lock is taken. Under the shared handle's lock,
write-write conflicts are checked against recent committed history. A clean commit publishes a new
manifest and installs a replacement immutable snapshot.

Before returning `TxnError::IndeterminateManifestPublication`, commit installs the readable candidate
and records the write set, so that transaction is terminal and must not be replayed. Create returns
no handle; callers must open before retrying and must not create again when open succeeds. Compact,
migrate, and maintain can propagate `TxnError::IndeterminateManifestPublication { .. }`
after reconciling a verified-visible candidate into the shared handle. The error gives no durability
acknowledgement and must not be blindly replayed; reopening can establish visibility, not durability.
Existing snapshots remain immutable in every outcome.

Inserting transactions each write one immutable row-ID reservation. Recovering the allocation floor
reads O(commits) reservation metadata per insert, yielding O(commits²) cumulative metadata reads.
This unbounded allocator scan is excluded from lifecycle physical accounting and reclamation; it does
not coordinate independent handles.

The supported concurrency boundary is one process sharing one `Dataset` handle. This bounded read
API and write-write OCC have snapshot isolation as their ceiling, not full serializability; it is not
a general read/write query interface. Independent openers do not share the lock, allocator, history,
or in-memory snapshot, and cross-process conditional publication is Phase 4.

Rows are append-only physical records. Delete adds a tombstone. Update tombstones the old physical row
and inserts replacement data with a new physical row ID. Future tombstones, target validation,
cardinality, and restart-safe physical-ID non-reuse are enforced within the supported facade.
Logical identity remains a later contract item.

## Immutable vector segments

The index crate owns HNSW construction, distance kernels, segment encoding, defensive loading, topology
checks, bounds checks, and checksums. A segment maps local ordinals to global physical row IDs. A
snapshot loads the manifest-listed segment set; search fans out to applicable segments, merges top-k,
and filters tombstoned rows. This is a coherent segment-set design, not a universal recall or latency
claim. Explicit compaction rewrites the current live snapshot into replacement row/index objects and
reclaims superseded listed objects only after publication and active-snapshot checks.

## Query primitives

The query crate provides predicates, statistics/zone-map pruning, filtered ANN constraints, explain
information, in-memory group-by primitives, and a bounded logical/physical planner. The planner
accepts only the supported source/projection/predicate, grouping, and vector-search shapes; it
records selected physical operators plus captured file/segment pruning and transaction-overlay
observations, then reuses the immutable-snapshot operators. It is not SQL, a general optimizer, a
cost model, or a guaranteed cost bound for selective queries.

## Verification boundary

Each lifecycle mutation takes explicit lifecycle exclusivity followed by `commit_lock`, stopping write
preparation, publication, migration, and other lifecycle execution for that sub-operation while it
materializes live data, publishes, validates protected history, and reclaims eligible objects.
Immutable snapshot reads continue. `maintain` invokes these sub-operations sequentially, releasing
their guards between phases, so writers may interleave and the composed run is non-atomic. Cost scales
with live data and retained/protected history. The retained compaction fixture evidence is a 79.49
second median and 1,289.2 MB peak live memory; separate maintenance evidence records a 74.16-second
median and 1,090.4 MB peak live memory. These are not SLOs or maximums. `lifecycle_report` and
`storage_bound_met` exclude
`_meta/row-id-high-water` reservation objects from physical accounting and reclamation.

Normal unit/integration tests, loom models, property tests, fuzz targets, real-process chaos/reopen
tests, and Criterion benchmarks cover useful slices. Several important suites are opt-in or not yet
CI-gated, and historical measurements are not evidence for the current segmented implementation.
Phase 1 requires regression tests for known counterexamples, explicit corruption/durability scope,
CI-visible concurrency gates, and a current performance matrix.
