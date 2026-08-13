# Phase 3 Lifecycle Inventory and Diagnostics Design

Status: implemented within named bounds; approved design record for the read-only inventory slice.

This document governs the read-only inventory slice only. Its deletion exclusions apply to
`Dataset::lifecycle_report()` and do not prohibit the separately approved
[manifest retention executor](manifest-retention-executor.md).

## Goal

Add a read-only, snapshot-anchored lifecycle report to `Dataset` so callers can observe manifest
history, physical storage usage, reachable files, and unreferenced file candidates without changing
visibility, durability, retention, or reclamation behavior.

## Scope

This slice includes:

- a typed `Dataset::lifecycle_report()` Rust API;
- a report anchored to the immutable snapshot captured when the call begins;
- object enumeration through the existing `Backend::list()` abstraction;
- counts and byte totals for manifests, data objects, reachable row files, reachable vector segments,
  tombstones, and unreferenced data objects; and
- unit/integration coverage for empty, committed, multi-version, vector, and failed-preparation
  states.

This inventory slice explicitly excludes deletion, compaction, vacuum, retention policy, manifest
rewriting, segment rewriting, snapshot invalidation, and any claim that
an unreferenced object is immediately safe to remove. The separate manifest executor does not change
that boundary for row files, segments, temporary objects, or orphan candidates.

## Recommended architecture

`Dataset::lifecycle_report()` captures one immutable `Snapshot` and uses its manifest as the logical
reachability anchor. It then lists `_versions/` and `data/` through the dataset's local backend and
joins object keys against the captured manifest entries. A report records the captured manifest
version so a caller can detect that a concurrent commit may have changed the physical listing while
the report was being collected.

The report is observational and must not acquire the commit lock, mutate the manifest, or read a
second snapshot. Current-manifest reachability is the only classification in this slice. An object
not referenced by the captured manifest is an `orphan_candidate`, not a reclaimable object, because
an older in-memory snapshot may still reference it and this report never grants cleanup authority.

## Report contract

The public report should expose stable typed fields with `u64` counts and byte totals:

- `observed_version`: captured manifest version;
- `manifest_object_count` and `manifest_bytes`: all objects listed under `_versions/`;
- `current_manifest_bytes`: size of the captured version's manifest object, when listed;
- `data_object_count` and `data_bytes`: all objects listed under `data/`;
- `reachable_data_file_count` and `reachable_data_file_bytes`;
- `reachable_segment_count` and `reachable_segment_bytes`;
- `orphan_candidate_count` and `orphan_candidate_bytes`; and
- `tombstone_count` and `physical_row_count` from the captured manifest.

Byte totals use checked accumulation. An overflow or malformed object key must return a typed error
rather than wrap or silently omit the object. The report must retain an explicit documentation
statement that it is diagnostic evidence, not a universal storage or reclamation guarantee.

## Reachability rules

- Manifest objects are every object returned by `list("_versions/")`; the report does not parse old
  manifests in this slice.
- Current row files are `data/<DataFileEntry::name>` for every entry in the captured manifest.
- Current vector segments are `data/<SegmentEntry::name>` for every segment in the captured manifest.
- Every listed `data/` object not in either current reachable set is an orphan candidate.
- A missing reachable object is reported as an error, not converted into an orphan candidate.
- Temporary files and unknown files under `data/` remain candidates for later policy; this slice does
  not delete or classify them more aggressively.

## Error and concurrency behavior

Listing failures, checked-total overflow, invalid manifest-relative names, and missing reachable
objects return the existing typed storage/transaction error path. The method does not promise a
globally atomic filesystem inventory: `observed_version` identifies the logical snapshot, while
object counts and sizes are a best-effort point-in-time observation of the backend listing.

## Verification

Regression coverage proves:

1. a fresh dataset reports version zero and zero data/segment/tombstone usage;
2. committed row files and vector segments are counted and matched to manifest byte lengths;
3. multiple commits report accumulated manifest/data reachability without changing snapshots;
4. an injected failed preparation leaves an invisible object classified only as an orphan candidate;
5. missing reachable objects and invalid names fail closed; and
6. concurrent commits do not mutate an already captured report or its observed version.

Focused lifecycle storage/transaction tests and recorded Phase 3 verification evidence cover this
implemented slice. No benchmark or lifecycle reclamation claim is made by this slice.

## Boundary preservation

The design remains embedded, single-node, and one-process/shared-`Dataset` handle. It preserves
immutable snapshot reads plus write-write OCC, and adds no universal
durability, latency, memory, recovery, recall, or segment-count guarantee.
Cross-process coordination remains Phase 4 work. Compaction, vacuum, row/segment retention, and
orphan cleanup are implemented only by their separate bounded designs; this read-only report grants
no cleanup authority. See the separate manifest-only executor design for its narrower
historical-manifest slice.
