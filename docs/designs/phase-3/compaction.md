# Phase 3 Compaction and Reclamation Design

**Status:** Implemented and accepted for the bounded Phase 3 lifecycle scope; Phase 3 is implemented within named bounds.
**Branch:** `codex/phase-3-lifecycle`

## Goal

Reduce long-lived row-file and immutable-vector-segment fan-out while preserving immutable
snapshots, crash recovery, typed errors, and the one-process/shared-`Dataset` boundary.

## Scope

This implemented slice provides an explicit `Dataset::compact(CompactionPolicy)` operation for one shared handle.
It compacts the current logical snapshot into new row data and vector-index objects, publishes one
new manifest, and reclaims only superseded objects proven unreachable from the current manifest and
active snapshot leases held by that same handle.

It does not add cross-process coordination, serializability, automatic background compaction,
retention-by-age, object storage, schema migration, or arbitrary orphan deletion. Existing
manifest-only pruning remains independent and continues to delete only historical manifests.

## Alternatives

1. **Manifest-only cleanup:** low risk, but does not reduce row/segment fan-out or query latency.
2. **Compaction without reclamation:** improves future reads but leaves disk and recovery growth;
   it also creates an unbounded duplicate-object window.
3. **Durable compaction followed by authority-checked reclamation (recommended):** publishes a
   complete replacement snapshot first, preserves active snapshots, then deletes only objects that
   are absent from every protected manifest. It has the strongest safety story within the existing
   shared-handle model, at the cost of temporary peak storage.

## API and policy

```rust
pub struct CompactionPolicy {
    pub retain_snapshots: bool,
}

pub struct CompactionReport {
    pub source_version: u64,
    pub published_version: u64,
    pub row_files_written: u64,
    pub segments_written: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
}

impl Dataset {
    pub fn compact(&self, policy: CompactionPolicy) -> Result<CompactionReport>;
}
```

The initial policy is deliberately narrow: `retain_snapshots = true` preserves every active
historical snapshot lease. A future policy may release leases explicitly, but compaction must never
invalidate a live snapshot implicitly.

## Amendment — 2026-08-13: supported output shape

The supported compaction contract is run-aware: an empty live set produces zero row files. A
nonempty live set produces one row file for each maximal contiguous run of live physical row IDs,
and compaction produces at most one vector segment. Tombstone gaps therefore may require more than
one replacement row file. This amendment supersedes this document's earlier “one replacement set”
shorthand; it does not expand the shared-handle concurrency boundary or introduce serializability.

## Protocol

1. Acquire lifecycle exclusivity, then the existing commit lock in the same order as manifest
   pruning. This prevents preparation/publication from racing compaction.
2. Capture the current immutable snapshot and active lease versions while both guards are held.
3. Read the current logical rows through the snapshot's visibility rules, preserving schema,
   physical row IDs, tombstones, and vector values.
4. Write new uniquely named row data and vector-segment objects to temporary paths. Every write is
   closed, checksummed, and directory-synchronized through existing storage helpers.
5. Build a replacement manifest with a fresh version, the same schema and row identity state, and
   only the newly written objects. Commit the manifest through the existing durable publication
   path. Until this succeeds, the old snapshot remains authoritative.
6. Rebuild deletion authority under both locks. Protect the published manifest, every manifest
   version held by an active lease, and every object listed by those manifests. Delete only old row
   files and segments absent from that protected set. A deletion or directory-sync error returns a
   typed error and leaves the new manifest valid for retry.
7. Return counts and bytes only for successful deletions. Temporary files not safely attributable
   to this operation remain orphan candidates for diagnostic inventory; this method does not sweep
   arbitrary unknown files.

## Crash and failure behavior

- Crash before manifest publication: the old manifest remains current; newly written objects are
  unreferenced candidates and are not deleted by recovery.
- Crash after manifest publication but before reclamation: the new manifest is authoritative and
  old objects remain safe, though temporarily unused.
- Reclamation failure after unlink: return the error; retry relists authority and never counts a
  missing object as a successful new deletion.
- Missing or malformed reachable objects fail closed before compaction publication.
- Any schema, row-ID, vector-dimension, checksum, or manifest validation failure aborts before
  publication.

## Verification

Implemented regression coverage includes:

1. Compacting an empty dataset publishes a valid replacement manifest.
2. Compaction preserves row values, physical row IDs, tombstone visibility, and vector-search
   results.
3. Compaction produces the supported run-aware replacement row-file shape and at most one vector
   segment.
4. An active historical snapshot remains readable after compaction and its referenced objects are
   not deleted.
5. A crash/failure before publication leaves the old snapshot and manifest readable.
6. A deletion failure leaves a valid new manifest and produces a retryable typed error.
7. Reopen after compaction loads only the published replacement objects and recovers the correct
   next row-ID/timestamp/attempt high-water state.
8. The report's deleted counts and bytes equal successful deletions only.

## Non-claims

This design does not establish universal power-loss durability, cross-process safety, serializable
isolation, a mandatory storage bound, or a universal latency improvement. It provides a safe,
explicit compaction operation whose benchmark impact must be measured on the pinned fixture.
