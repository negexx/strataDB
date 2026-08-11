# Phase 3 Retention Planning and Snapshot Pinning Design

Status: implemented as a read-only, advisory Phase 3 planning slice; lifecycle-executor
remediation remains pending.

## Goal

Add a read-only retention planner that identifies which manifest and data objects must remain
reachable for the latest-version policy and all live `Snapshot` handles, while reporting older
objects that a future cleanup executor may reconsider.

This is the first safe Phase 3 step after lifecycle diagnostics. It does not delete, rewrite,
compact, or republish any object.

## Current boundaries

The supported engine boundary remains one process sharing one `Dataset` handle. `Dataset::snapshot()`
returns a cloned `Arc<Snapshot>`, and historical snapshots remain queryable after later commits.
Before this implementation, `Dataset` stored only the latest snapshot in `SnapshotCell`; no registry
recorded which historical `Arc<Snapshot>` values were still alive. The delivered slice adds the
`SnapshotLeaseRegistry` described below to provide that active-snapshot evidence. The storage
`Backend` already exposes `list`, `get`, and `delete`, but `delete` is not used by this slice.

The existing `Dataset::lifecycle_report()` is observational and current-snapshot anchored. Its
orphan candidates are not safe-to-delete claims because an older in-memory snapshot may still
reference the same objects. This design adds the missing active-snapshot evidence without changing
that report's contract.

Independent `Dataset` handles and processes are outside the supported coordination boundary. The
planner must document that its active-snapshot registry covers snapshots created from the calling
`Dataset` handle only; a later cleanup executor must not claim cross-handle or cross-process safety.

## Public API

Add a new `crates/txn/src/retention.rs` module and re-export these types from `strata_txn`:

```rust
pub struct RetentionPolicy {
    pub keep_latest_versions: u64,
}

pub struct RetentionPlan {
    pub observed_version: u64,
    pub active_snapshot_versions: Vec<u64>,
    pub retained_manifest_versions: Vec<u64>,
    pub retained_data_object_count: u64,
    pub retained_data_bytes: u64,
    pub eligible_manifest_versions: Vec<u64>,
    pub eligible_data_objects: Vec<RetentionCandidate>,
}

pub struct RetentionCandidate {
    pub key: String,
    pub bytes: u64,
}

impl Dataset {
    pub fn retention_plan(&self, policy: RetentionPolicy) -> Result<RetentionPlan>;
}
```

`keep_latest_versions` must be at least one. Zero is rejected with a typed transaction error; the
current manifest is never eligible for removal. Returned vectors are sorted and deduplicated so
callers can compare plans deterministically.

The words `eligible` and `candidate` are deliberately advisory. They mean an object was outside
the retained set at the captured observation point; they do not authorize deletion.

## Snapshot lease registry

Introduce an internal `SnapshotLease` containing the snapshot version and an internal
`SnapshotLeaseRegistry` owned by `Dataset` through `Arc`.

- Every `Snapshot` constructed by `Dataset::create`, `Dataset::open`, or commit publication receives
  an `Arc<SnapshotLease>`.
- The registry stores `Weak<SnapshotLease>` entries, not strong references, so it does not keep
  historical snapshots alive.
- `Dataset::snapshot()` continues to return `Arc<Snapshot>` with the existing signature; cloning
  that `Arc` naturally keeps the lease alive.
- A plan upgrades registry entries, removes dead weak entries, collects the versions, adds the
  current snapshot version, sorts, and deduplicates.
- Unit-test-only `Snapshot` constructors receive an unregistered lease so existing isolated
  snapshot tests do not need a `Dataset` registry.

The registry is evidence for a plan, not a deletion lock. A future executor must capture a fresh
plan and revalidate the active leases while holding the shared `Dataset` commit lock before any
mutation. Reacquiring that lock alone cannot turn an advisory plan into deletion authority:
`commit_lock` serializes manifest publication, but it does not protect row or segment files that a
concurrent transaction prepared before acquiring the lock. Any executor therefore needs preparation
leases, lifecycle epochs, or equivalent coordination that spans preparation through publication or
abort before it can make a deletion decision.

## Retention algorithm

`Dataset::retention_plan` performs these steps without acquiring `commit_lock`:

1. Load the current `Arc<Snapshot>` exactly once and record its version.
2. Collect live snapshot lease versions from the registry, including the captured current version.
3. List `_versions/` and `data/` through `LocalFs`.
4. Parse and validate version-manifest keys. Read each manifest needed for the latest-version
   window or an active snapshot through the existing `Backend::get` path and validate its envelope,
   checksum, version, and manifest-relative object names.
5. Retain every manifest in the latest `keep_latest_versions` versions and every active snapshot's
   manifest. Retain the union of every row-file and segment key referenced by those manifests.
6. Classify well-formed older manifests outside the retained version set as eligible manifest
   versions, and listed data objects outside the retained data-key set as eligible data candidates.
7. Return checked counts/byte totals and the captured version. Any storage error, malformed retained
   manifest, unsafe key, duplicate reachable key, or arithmetic overflow fails closed with the
   existing typed error path.

The planner is intentionally conservative: unknown or temporary objects are not silently converted
into deletion candidates, and a concurrent commit may make the returned plan stale. Because no
mutation occurs, the result remains diagnostic evidence rather than a reclamation guarantee.

## Storage support

The retention module may add a narrow storage helper for reading a specific version manifest with
its object byte count. It must reuse the existing `ManifestEnvelope` validation and canonical
checksum rules rather than duplicating JSON parsing. No storage format, backend trait, or delete
semantics change is allowed in this slice.

## Error behavior

- `keep_latest_versions == 0` returns a new typed `TxnError` variant.
- Missing or malformed manifests needed to compute the retained set return the existing storage,
  manifest, unsafe-path, corruption, or overflow errors; they are never treated as eligible files.
- Listing and read failures propagate unchanged through `Result`.
- The planner never returns a partially successful plan.
- The plan does not claim global filesystem atomicity or protection from independent openers.

## Testing strategy

Add focused tests under `crates/txn/tests/retention_plan.rs` and unit tests for the registry and pure
retention-set helpers:

1. A fresh dataset with policy `keep_latest_versions = 1` retains version zero and has no eligible
   objects.
2. Multiple commits retain exactly the latest policy window and report older manifests/data only
   when no live snapshot references them.
3. Holding an historical `Arc<Snapshot>` retains its version and all row/segment objects referenced
   by that snapshot; releasing it removes the version from a subsequent plan.
4. Multiple handles to the same `Arc<Snapshot>` keep one deduplicated active version until the last
   handle is dropped.
5. A concurrent commit after plan capture does not mutate the returned plan, and the plan exposes
   its observed version so callers can detect staleness.
6. Zero policy, malformed retained manifests, unsafe keys, missing retained objects, and byte/count
   overflow return typed errors without classifying the affected object as eligible.
7. The planner leaves current snapshots, manifests, data files, and segments unchanged; no test may
   observe a `Backend::delete` call because deletion is not part of this design.

Run the focused tests, `cargo fmt --check`, `cargo check -p strata-txn`, and the relevant workspace
tests before updating the status and roadmap documents.

## Explicit non-goals

This design does not implement deletion, vacuum, compaction, segment merging, manifest rewriting,
time travel, retention by wall-clock age, cross-process coordination, a background worker, a CLI
command, object-store conditional deletion, or a claim that Phase 3 is complete. Those require
separate designs and failure/recovery evidence.
