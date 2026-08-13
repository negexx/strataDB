# Phase 3 Vacuum Design

**Status:** implementation plan for the next bounded lifecycle slice

## Goal

Add an explicit, shared-handle vacuum operation that removes only safely classified unreferenced
data objects and temporary files, while preserving every manifest-listed object reachable from any
durable manifest and validating the current manifest and active snapshot leases.

## Boundary

Vacuum remains embedded and single-process. It does not coordinate independent `Dataset` handles,
delete manifests, rewrite rows, compact segments, apply age-based retention, or claim a universal
storage-growth bound. Unknown files and malformed authority remain fail-closed.

## Proposed API

```rust
pub struct VacuumReport {
    pub observed_version: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
}

impl Dataset {
    pub fn vacuum(&self) -> Result<VacuumReport>;
}
```

The operation acquires lifecycle exclusivity before `commit_lock`, rebuilds authority from a fresh
manifest and `data/` listing, validates the current and active-snapshot manifests, and deletes only
objects that are both safely named and absent from every durable manifest. Temporary files created by
interrupted writes and orphan files with `.arrow`/`.seg` suffixes are eligible; arbitrary names are
reported as unknown and left untouched.

## Failure and retry behavior

- A missing object referenced by the current or active snapshot manifest fails closed.
- A missing object referenced only by an obsolete manifest is ignored because that historical
  manifest is not protected by vacuum authority.
- A deletion error, including post-unlink directory-sync failure, returns an error and counts only
  deletions whose backend call completed successfully.
- A retry relists the directory and is safe after partial deletion.
- Vacuum never deletes `_versions/`, row-ID high-water files, or arbitrary unknown objects.

## Verification

Tests must cover empty vacuum, orphan row/segment cleanup, temporary-file cleanup, active snapshot
protection, malformed/missing protected objects, unknown-file preservation, deletion counts/bytes,
and retry after a post-unlink synchronization error. The existing lifecycle coordination loom model
covers the unchanged exclusive/preparation lock contract; a filesystem-level vacuum loom model is
not applicable because the behavior depends on backend listings and durable directory operations.
