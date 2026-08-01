# Status

This ledger describes the checked-in implementation, not an aspirational API. Source and test paths
are evidence pointers; [roadmap](roadmap.md) owns phase ordering.

## Overall state

**Phase 1: Partial — blocked.** The [seven-lane Sol audit](phase-1-audit.md) found correctness,
durability, schema, API-boundary, verification, and performance-evidence blockers inside the supported
single-process/shared-`Dataset` boundary.

## Capability ledger

| Capability | State | Current boundary |
|---|---|---|
| Local storage/manifests | Implemented | Arrow files, manifests, statistics, and local filesystem persistence work. Lifecycle management does not. |
| Transactions/conflicts | Partial | Shared-handle write-write OCC and typed row-ID conflicts exist; no serializability claim. |
| Row/index publication | Partial | One manifest/snapshot transition exists; target validation, identity, integrity, and durability blockers remain. |
| Snapshot/query reads | Partial | Immutable scan, predicate, explain, and vector-search reads exist; no read/write transaction API. |
| Query operators/pruning | Partial | Predicates, file/segment pruning, filtered ANN, and group-by primitives exist; no complete planner. |
| Immutable vector segments | Implemented | Manifest-listed HNSW segments load and fan out across snapshots. Growth is unbounded without lifecycle work. |
| Update/delete identity | Partial | Tombstone-plus-replacement behavior exists; logical identity, target semantics, and cardinality remain open. |
| CLI | Partial | Fixed-assumption MVP inspection/demo commands; not a stable administration surface. |
| Python | Proposed | PyO3 scaffolding exports only `placeholder_version`; no database API exists. |
| Durability/recovery | Partial | File fsync and crash/reopen evidence exist. Dataset creation fails closed for ordered local directory-sync errors, but manifest integrity, broader recovery proof, and filesystem-specific evidence remain incomplete. |
| Schema/migrations | Partial | Caller batch shape and reserved columns are checked; no dataset-owned schema catalog or migration workflow. |
| Loom/chaos/fuzz/bench evidence | Partial | Useful tooling exists, but important models and opt-in suites are not all CI-visible or retained as current evidence. |
| Compaction/GC | Proposed | No compaction, vacuum, orphan cleanup, or bounded history implementation. |
| Cross-process coordination | Proposed | Independent openers do not share transaction state or durable conditional publication. |
| Branching/object storage | Proposed | No branch/merge or object-store backend is implemented. |

## Concurrency scope

The supported concurrency scope is **one process using one shared `Dataset` handle**. The commit lock,
row-ID allocator, recent-write history, and current snapshot live in that handle. Opening the same path
independently does not establish a transaction protocol.

## Directory-durability boundary

Dataset creation now fails rather than acknowledging a directory sync that the filesystem rejects.
It synchronizes every newly-created ancestor bottom-up, then the manifest directory through manifest
publication, and finally the dataset directory for `_versions/`. The platform boundary is deliberately
narrow: Windows uses a native directory handle with `FILE_FLAG_BACKUP_SEMANTICS`; POSIX uses a
directory handle; both are in scope only when the open and flush succeed. Unsupported, invalid-input,
and POSIX `EINVAL`-like outcomes are typed `DurabilityUnsupported`, not best-effort success. Remote
backends, cross-process publication, and universal power-loss proof remain out of scope.

A final dataset-directory sync failure can occur after the initial manifest becomes visible. The
`Dataset::create` call still fails and must not be treated as acknowledged; callers first probe with
`Dataset::open` rather than blindly retrying creation. A visible dataset remains subject to the failed
durability boundary until the underlying filesystem condition is repaired or replaced. See the
[Phase 1 audit](phase-1-audit.md#task-1-durability-recovery-boundary) for the recovery procedure.

## Status vocabulary

- **Implemented:** present with direct source/test evidence.
- **Partial:** a usable slice exists, but important scope, verification, API, or operational work remains.
- **Proposed:** planned direction; no supported capability claim.
- **Historical/Superseded:** preserved context that does not govern current behavior.
