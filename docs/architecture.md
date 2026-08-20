# Architecture

Strata is an embedded, single-node, local-disk research/prototype database for structured Arrow data
and vector embeddings. Its supported concurrency boundary is concurrent writers in one process sharing
one `Dataset` handle. The intended commit contract is atomic row/index publication through a manifest,
and the Phase 1 audit's in-scope correctness, durability, and integrity blockers are now remediated
or evidence-closed within named local filesystem and shared-handle limits. The Phase 1 contract is
implemented with named limits, not a universal guarantee.

Read [status](status.md) for evidence, [roadmap](roadmap.md) for phases, and [phase-1-audit](audit/phase-1/audit.md)
for the audit trail and named limits.

## Components

| Component | Location | Responsibility |
|---|---|---|
| Storage | `crates/storage` | Arrow IPC files, manifests/version records, statistics, and local filesystem persistence. |
| Transactions | `crates/txn` | `Dataset`, snapshots, row-ID allocation, write-write OCC, tombstones, commit ordering, and row/index publication. |
| Index | `crates/index` | From-scratch HNSW construction/search, immutable segment encoding, validation, loading, and fan-out search. |
| Query | `crates/query` | Predicates, pruning decisions, and group-by primitives. |
| CLI | `crates/cli` | Typed query/inspection commands within the one-process boundary; mutating commands require explicit single-writer acknowledgement. |
| Bindings | `crates/bindings` | Thin PyO3 `1.0` Dataset/Snapshot/Transaction facade; tabular results use Arrow IPC bytes, vector matches and migration/explain results use stable named Python dictionaries, and engine failures retain typed categories. |

## Supported on-disk formats

Phase 0 supports the following local-disk artifacts. These are explicit format contracts, not a
general migration or compatibility framework:

| Artifact | Current format | Validation and rejection behavior |
|---|---|---|
| Row data file | Arrow IPC file containing the committed record batch; there is no Strata-owned row-file version discriminator | Compatibility is delegated to the pinned Arrow IPC reader. Dataset recovery validates schema, row count, physical columns, byte length, and CRC metadata; malformed or incompatible data is rejected. |
| Version manifest | JSON `ManifestEnvelope`, `format_version = 1`, CRC32C over the canonical envelope, and a version-matching filename | Legacy direct manifests, unsupported format versions, checksum mismatches, unknown envelope/manifest/entry fields, malformed schema bytes, and filename/version mismatches are rejected. |
| Immutable vector segment | `STRTSEG\0` header, segment format version 1, little-endian fields, squared-L2 metric, and header/body CRCs | Wrong magic/version/endianness/metric, malformed geometry/topology, invalid row IDs, and CRC failures are rejected before the segment is exposed to a snapshot. |

The supported backend for these formats is the local filesystem through `LocalFs`. Object storage,
cross-process coordination, format migration, and universal power-loss guarantees are later-phase
work and must not be inferred from these checks.

## What exists today

- Local Arrow data files, immutable version manifests, per-file statistics, and the `LocalFs` backend.
- Immutable snapshots with scan, predicate, explain, and vector-search reads.
- Write-write optimistic conflict detection for transactions sharing one `Dataset` handle.
- Immutable HNSW segments listed by the manifest; vector search fans out across segments and merges top-k.
- Predicate pruning, filtered ANN primitives, and in-memory group-by.
- `Dataset::lifecycle_report()`, a read-only snapshot-anchored inventory of manifest/data objects,
  reachability, and unreferenced-object candidates.
- `Dataset::retention_plan()`, a read-only, advisory latest-version and active-snapshot planning API
  for one shared `Dataset` handle.
- `Dataset::prune_manifests()`, a manifest-only historical-retention executor for that same shared
  handle.
- Explicit shared-handle lifecycle operations: snapshot-preserving `Dataset::compact()`,
  age-based `Dataset::prune_manifests_by_age()`, `Dataset::vacuum()` of recognized unprotected
  objects, and `Dataset::maintain()` for one coordinated maintenance run.
- Real-process crash/reopen tests, targeted loom models, fuzz targets, and benchmarks.

These are usable slices, not a finished database API. The implemented planner is deliberately
bounded: it builds logical source/predicate/projection, grouping, or vector-search pipelines and
returns a stable physical-plan explain value with the logical operators, selected physical
operators, and captured file/segment pruning and overlay observations. It validates the supported
shape, then reuses the existing immutable-snapshot scan, zone-map pruning, tombstone filtering,
group-by, and manifest-listed immutable-segment search operators; it does not add SQL, a general
optimizer, or a cost model. Local Criterion evidence for the fixed 256-row fixture is recorded in
the [Phase 3 verification report](phase-3-verification-report.md#task-3-query-planning-evidence);
it measures that fixture only, not a universal performance guarantee. There is no
arbitrary orphan cleanup, time
travel, or cross-process protocol. The Phase 2 Python/CLI surfaces provide stable bounded contracts
within the documented embedded, single-process boundary. Their remaining packaging/runtime limits
are recorded in [status](status.md); they do not make the accepted interfaces "awaiting
integration" or broaden the API to cross-process coordination, serializability, or a general
read/write query interface.

## Python API 1.0

`strata_ext.Dataset` provides `create`, `open`, `api_version`, `version`,
`schema_version`, `snapshot`, `begin`, and the explicit
`migrate_add_nullable_column` operation. `Snapshot` retains the compatible Arrow IPC scan,
lookup, and grouped-result methods and typed vector-match dictionaries; `explain_scan` returns
named logical/physical operator lists plus captured observations. The engine `Transaction` supports
bounded transaction-base snapshot reads: scans (including predicate reads) and groups expose staged
inserts, replacements, and deletes; lookup reflects staged replacements and deletes only for physical
row IDs already in the base snapshot, because staged inserts receive no physical row ID until commit
and cannot be looked up before then. The current Python `Transaction` facade stages one Arrow IPC
batch at a time and exposes overlay-aware `scan` reads (including its optional predicate), but does not expose
transaction lookup or group methods. It is terminally `committed` or `aborted`, and abort/drop never
publishes staged writes. Its `vector_search` returns the typed
`UnsupportedTransactionReadError` after staged writes, rather than a merged overlay result. Open,
reads, commit, and migration release the GIL around engine work. The API is limited to one Python
process sharing a `Dataset` handle: it does not provide cross-process coordination, serializability,
or a full/general read/write query interface. `ConflictError` exposes `contested_row_ids`;
`SchemaMigrationError`, `InvalidQueryError`,
`UnsupportedTransactionReadError`, `StorageDurabilityError`, and `CorruptionError` are stable
categories (and retain the existing `ValidationError`/`ExecutionError` base classes). Insufficient
history remains a distinct category.

## Commit lifecycle

1. `Dataset::begin` captures one immutable base snapshot and returns a `Transaction` bound to that
   snapshot's schema/version. It buffers Arrow batches and tombstones. Transaction scans (including
   predicate reads) and group reads merge that base with its private overlay, exposing staged inserts,
   replacements, and deletes. Lookup reflects staged replacements/deletes for an existing
   base-snapshot physical row ID; staged inserts have no physical row ID until commit and cannot be
   looked up pre-commit. `vector_search` after staged writes uses no merged overlay and returns a typed
   unsupported-transaction-read error.
2. Commit preparation allocates row IDs, writes/fsyncs Arrow files, and builds/fsyncs an immutable HNSW
   segment when vectors are present. Failed preparation may leave unreachable files; cleanup is absent.
3. Under the shared handle's commit lock, the transaction reloads the latest snapshot and checks its
   write set against committed history. Only write-write conflicts are tracked; insufficient history
   is rejected conservatively.
4. A clean commit creates and publishes a manifest containing data files, tombstones, and segment
   metadata, then installs a replacement immutable snapshot. The manifest is the intended visibility
   boundary; directory durability and end-to-end integrity are limited to the named local filesystem
   evidence and typed fail-closed paths documented in `docs/status.md`.

Within the supported local-filesystem, one-process/shared-`Dataset` boundary, publication has three
outcomes: acknowledged success; a definite failure before final-name publication; or indeterminate
publication. An indeterminate result means the immutable final-name manifest candidate was readable
after publication while directory synchronization failed, so Strata reports no durability
acknowledgement. A committing transaction installs that candidate and records its write set before it
returns `TxnError::IndeterminateManifestPublication`; the transaction is terminal and must not be
replayed. `Dataset::create` returns no handle on that outcome, so callers must `open` before any
retry and must not create again if open succeeds. `compact`, `migrate_schema`, and `maintain` can
return `TxnError::Storage(StorageError::PublicationIndeterminate(...))` before updating the existing
handle; callers stop using that handle, drop and reopen it, then inspect the recovered version and schema. A successful
reopen proves visibility, not durability. Existing snapshots remain immutable throughout, and
independent handles are not coordinated.

Publication is lock-serialized inside one `Dataset` handle. Independent handles/processes do not share
the lock, allocator, history, or in-memory snapshot, so the implementation is not a cross-process
conditional-CAS protocol.

## Lifecycle diagnostics and manifest retention

`Dataset::lifecycle_report()` captures one immutable snapshot and reports its manifest version together
with a best-effort backend listing of manifest/data object counts and bytes, reachable row files and
segments, tombstones, physical rows, and unreferenced-object candidates. The report is diagnostic
evidence only: it does not acquire the commit lock, mutate storage, or provide a globally atomic
filesystem inventory.

An orphan candidate is only an object not referenced by the captured manifest. It can include data
still required by an active snapshot, as well as temporary or unknown files. It is not safe to delete
through the inventory API. `Dataset::retention_plan()` remains an advisory latest-version and
active-snapshot policy for the shared handle.

`Dataset::prune_manifests()` is the separate, manifest-only executor slice. It acquires lifecycle
exclusivity before `commit_lock`, then rebuilds authority from the current manifest listing while
both guards are held. Durable manifest authority means the recovery-recognized numeric
`_versions/<version>.manifest` keys in that listing; malformed or duplicate numeric versions fail
closed. The authority retains the current manifest, the latest-version window, and active snapshots;
it carries exact listed keys and byte counts and deletes eligible historical manifests oldest first.
It never deletes row files, segments, temporary objects, or arbitrary orphan objects. A post-unlink
local directory-sync error is returned and a retry relists state under the same guards. The [inventory design](designs/phase-3/lifecycle-inventory.md), [manifest executor
design](designs/phase-3/manifest-retention-executor.md), and focused [inventory](../crates/txn/tests/lifecycle_inventory.rs)
and [executor](../crates/txn/tests/manifest_retention_executor.rs) tests define these distinct
boundaries.

`Dataset::compact()` publishes replacement row/index objects before reclaiming superseded listed
objects that are not protected by active snapshots. `Dataset::prune_manifests_by_age()` retains the
current/latest/active versions and protects legacy manifests whose publication timestamp is zero.
`Dataset::vacuum()` deletes only recognized temporary names or unprotected `.arrow`/`.seg` objects;
unknown objects, including unknown dotfiles, remain outside its authority. `Dataset::maintain()`
composes those operations and returns `storage_bound_met` from its final inventory. That field is an
observation from one completed run, not atomic or continuing storage-bound enforcement.

Mutating lifecycle operations are explicitly stop-the-world for write preparation, publication,
migration, and lifecycle execution on the shared handle: lifecycle exclusivity is taken before
`commit_lock`, which remains held while live data is materialized, a replacement is published,
protected history is validated, and eligible objects are reclaimed. Immutable snapshot reads can
continue. Cost therefore scales with live data and retained/protected history. The retained bounded
fixture/runner evidence is a 79.49-second median and 1,289.2 MB peak live memory; it is evidence
for that fixture and runner, not an SLO or a maximum. `lifecycle_report` and `storage_bound_met`
exclude `_meta/row-id-high-water` reservation objects from physical accounting.

Every inserting transaction creates one immutable row-ID reservation metadata object. Recovering the
allocation floor reads that catalog, making metadata reads O(commits) per inserting commit and
O(commits²) cumulatively; this is a documented shared-handle/local-filesystem cost, not an
independent-handle coordination mechanism.

## Reads, updates, and index behavior

`Dataset::snapshot` returns a fixed manifest/segment/tombstone view. Later commits cannot alter that
captured view. `Transaction` supplies bounded transaction-base snapshot reads: scans (including
predicate reads) and group operations expose staged inserts, replacements, and deletes; lookup
reflects staged replacements/deletes for an existing base-snapshot physical row ID, while staged
inserts have no physical row ID until commit and cannot be looked up pre-commit. It is not a general
read/write query interface, and `vector_search` after staged writes returns a typed
unsupported-transaction-read error rather than reading a merged overlay.

Rows are append-only physical records. `delete` and `update` must target one live physical row in the
transaction's base snapshot and are revalidated under the commit lock. `update` tombstones that old
physical row and inserts exactly one replacement with a new physical row ID; invalid, absent, or
already-dead targets return typed errors. Logical identity remains deferred.

Each vector-carrying commit creates an immutable HNSW segment. Search asks applicable segments for local
candidates, merges them into global top-k, and applies live/tombstone filtering. This provides a coherent
segment set, not a universal ANN recall or performance guarantee. Segment count grows with commits
until explicit compaction is requested; no universal supported segment-count or storage-growth bound
is claimed.

Predicates and statistics can prune files or segments that cannot match. Filtered ANN uses predicate
pruning and a live-set constraint. The bounded planner exposes those selections through its logical/
physical explain output, including captured row-file and index-segment totals, scanned/pruned counts,
and whether a transaction overlay was supplied. Those observations are snapshot facts, not cardinality
or latency estimates; the planner is not a SQL engine and does not guarantee that every selective
query is cheap. Planned reads retain the existing immutable-snapshot behavior and do not broaden the
write-write OCC isolation boundary.

## Deliberate boundaries

The design ceiling is snapshot isolation rather than serializability. The current API is narrower:
immutable snapshot reads; bounded transaction-base scans/predicate and group operations that expose
staged inserts, replacements, and deletes; lookup that reflects staged replacements/deletes only for
an existing base-snapshot physical row ID (staged inserts receive no physical row ID until commit);
and write-write OCC. It is not a full/general read/write query interface. Strata
remains embedded and single-node; distributed transactions, full SQL, automatic conflict resolution,
stronger isolation, extra ANN families, and an agent-memory/belief product are out of scope.

Normal tests, loom models, chaos tests, fuzz targets, and benchmarks provide bounded evidence rather
than a blanket proof. See [decisions](decisions.md), [current design](design.md), and the [roadmap](roadmap.md)
for the governing rationale and next steps.
