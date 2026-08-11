# Phase 3 Manifest Retention Executor Design

Status: approved, bounded executor slice.

## Goal

Provide one shared-handle `Dataset::prune_manifests(RetentionPolicy)` operation that removes only
policy-eligible historical manifest objects. It is deliberately not a general garbage collector or
storage-reclamation protocol.

## Scope

The executor:

- retains the latest-version window and every manifest held by an active snapshot from the same
  shared `Dataset` handle;
- protects the current manifest unconditionally;
- deletes eligible historical manifest objects oldest version first;
- returns `ManifestPruneReport` with the observed version, successfully deleted versions, and their
  listed byte total; and
- uses the local `Backend::delete` contract, including its directory-sync durability boundary.

It does not delete row files, vector segments, temporary objects, arbitrary orphans, row-ID
catalogs, or any other data object. It does not compact, rewrite manifests, reclaim index storage,
or coordinate independent handles or processes.

## Authority and locking

`Dataset::retention_plan()` remains read-only and advisory. It never supplies deletion authority.

`Dataset::prune_manifests` first acquires lifecycle exclusivity, then `commit_lock`. The lifecycle
coordinator spans transaction preparation through publication, typed failure, and panic unwind;
therefore an executor cannot race a prepared row file or segment that has not yet reached manifest
publication. Writer preference prevents a queued executor from being starved by later preparations.

Only after both guards are held does execution build fresh authority. The authority revalidates the
latest-version policy and live snapshot leases, lists `_versions/`, and carries each candidate's
exact listed key and byte count. It never reconstructs a padded filename from a version, so legacy
unpadded manifest keys remain safe and compatible. Malformed, missing, unsafe, duplicate, or
otherwise unverifiable retention state returns the existing typed error path before deletion.

## Deletion and retry behavior

Candidates are historical versions below the observed current version that are outside both the
latest-version window and the active-snapshot set. The executor invokes `Backend::delete` in oldest
version order and adds a version and bytes to the report only after that call returns success.

`LocalFs::delete` may unlink a file and then fail while synchronizing its containing directories.
That error is returned rather than converted to success. A retry is safe: it reacquires both guards,
relists manifests, and does not treat a now-missing previously unlinked object as a successful new
deletion. This is a bounded local-filesystem durability statement, not a universal power-loss claim.

## Snapshot and process boundary

The supported boundary remains one process with one shared `Dataset` handle. Active lease tracking
does not cover independent `Dataset::open` handles or other processes. The operation preserves
immutable snapshots, write-write OCC, and the existing snapshot-isolation ceiling; it adds neither
serializability nor cross-process conditional publication.

## Relationship to lifecycle inventory

The [lifecycle inventory design](phase-3-lifecycle-inventory-design.md) governs a separate,
read-only report. Its orphan candidates remain diagnostic and are not executor authority. This
design authorizes only listed historical manifest deletion; row and segment reclamation requires a
later crash-safe design such as a journal or equivalent durable protocol.

## Verification

Focused integration tests cover latest-window pruning, active historical snapshots, final clone
release, exact unpadded keys, report totals, malformed authority, and retry after a post-unlink
sync failure. Transaction tests cover lifecycle admission, and the crate-scoped loom model verifies
preparation and exclusive lifecycle execution never overlap.
