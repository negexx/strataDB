# Architecture

Strata is an embedded, single-node, local-disk research/prototype database for structured Arrow data
and vector embeddings. Its supported concurrency boundary is concurrent writers in one process sharing
one `Dataset` handle. The intended commit contract is atomic row/index publication through a manifest,
but the Phase 1 audit found in-scope correctness, durability, and integrity blockers; this contract is
Partial and blocked, not a universal guarantee.

Read [status](status.md) for evidence, [roadmap](roadmap.md) for phases, and [phase-1-audit](phase-1-audit.md)
for the current blockers.

## Components

| Component | Location | Responsibility |
|---|---|---|
| Storage | `crates/storage` | Arrow IPC files, manifests/version records, statistics, and local filesystem persistence. |
| Transactions | `crates/txn` | `Dataset`, snapshots, row-ID allocation, write-write OCC, tombstones, commit ordering, and row/index publication. |
| Index | `crates/index` | From-scratch HNSW construction/search, immutable segment encoding, validation, loading, and fan-out search. |
| Query | `crates/query` | Predicates, pruning decisions, and group-by primitives. |
| CLI | `crates/cli` | Typed query/inspection commands within the one-process boundary; mutating commands require explicit single-writer acknowledgement. |
| Bindings | `crates/bindings` | Thin PyO3 Dataset/Snapshot facade; tabular results use Arrow IPC bytes and vector results expose typed squared-L2 matches. |

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
- Real-process crash/reopen tests, targeted loom models, fuzz targets, and benchmarks.

These are usable slices, not a finished database API. There is no schema-evolution/migration workflow, planner,
stable Python API, compaction, vacuum, orphan cleanup, time
travel, or cross-process protocol. The Phase 2 Python/CLI surfaces are partial and remain subject to
their documented typed contracts and integration verification. “Partial” here describes client/API
maturity, not a claim that completed Phase 2 slices are outside their documented embedded,
single-process boundary.

## Commit lifecycle

1. `Dataset::begin` captures the current immutable snapshot version and returns a write-only
   `Transaction`. It buffers Arrow batches and tombstones; it has no transactional scan/search or
   read-your-own-writes.
2. Commit preparation allocates row IDs, writes/fsyncs Arrow files, and builds/fsyncs an immutable HNSW
   segment when vectors are present. Failed preparation may leave unreachable files; cleanup is absent.
3. Under the shared handle's commit lock, the transaction reloads the latest snapshot and checks its
   write set against committed history. Only write-write conflicts are tracked; insufficient history
   is rejected conservatively.
4. A clean commit creates and publishes a manifest containing data files, tombstones, and segment
   metadata, then installs a replacement immutable snapshot. The manifest is the intended visibility
   boundary; directory durability and end-to-end integrity are limited to the named local filesystem
   evidence and typed fail-closed paths documented in `docs/status.md`.

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
both guards are held. That authority retains the current manifest, the latest-version window, and
active snapshots; it carries exact listed keys and byte counts and deletes eligible historical
manifests oldest first. It never deletes row files, segments, temporary objects, or arbitrary orphan
objects. A post-unlink local directory-sync error is returned and a retry relists state under the same
guards. The [inventory design](phase-3-lifecycle-inventory-design.md), [manifest executor
design](phase-3-manifest-retention-executor-design.md), and focused [inventory](../crates/txn/tests/lifecycle_inventory.rs)
and [executor](../crates/txn/tests/manifest_retention_executor.rs) tests define these distinct
boundaries.

## Reads, updates, and index behavior

`Dataset::snapshot` returns a fixed manifest/segment/tombstone view. Later commits cannot alter that
captured view. `Transaction` writes and `Snapshot` reads are separate APIs; the current engine does not
provide a full read/write snapshot-transaction interface.

Rows are append-only physical records. `delete` and `update` must target one live physical row in the
transaction's base snapshot and are revalidated under the commit lock. `update` tombstones that old
physical row and inserts exactly one replacement with a new physical row ID; invalid, absent, or
already-dead targets return typed errors. Logical identity remains deferred.

Each vector-carrying commit creates an immutable HNSW segment. Search asks applicable segments for local
candidates, merges them into global top-k, and applies live/tombstone filtering. This provides a coherent
segment set, not a universal ANN recall or performance guarantee. Segment count grows with commits and
is not currently bounded by compaction.

Predicates and statistics can prune files or segments that cannot match. Filtered ANN uses predicate
pruning and a live-set constraint. These are query primitives, not a planner-integrated SQL engine or a
guarantee that every selective query is cheap.

## Deliberate boundaries

The design ceiling is snapshot isolation rather than serializability. The current API is narrower:
immutable snapshot reads plus write-write OCC. Strata remains embedded and single-node; distributed
transactions, full SQL, automatic conflict resolution, stronger isolation, extra ANN families, and an
agent-memory/belief product are out of scope.

Normal tests, loom models, chaos tests, fuzz targets, and benchmarks provide bounded evidence rather
than a blanket proof. See [decisions](decisions.md), [current design](design.md), and the [roadmap](roadmap.md)
for the governing rationale and next steps.
