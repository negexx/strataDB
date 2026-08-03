# Status

This ledger describes the checked-in implementation, not an aspirational API. Source and test paths
are evidence pointers; [roadmap](roadmap.md) owns phase ordering.

## Overall state

The current baseline includes merged PR #52 commit `d0b0a8e613cd58abdbc34b60ddde29ec2c2f979a`. The
[Phase 0 foundation audit](phase-0-audit.md) records the local foundation contract and its retained
evidence requirement. The [Phase 1 closeout ledger](phase-1-closeout-ledger.md) tracks the remaining
finding-level acceptance and evidence obligations; it does not change this Partial/blocked state.

**Phase 1: Partial — blocked.** The [seven-lane Sol audit](phase-1-audit.md) found correctness,
durability, schema, API-boundary, verification, and performance-evidence blockers inside the supported
single-process/shared-`Dataset` boundary.

## Capability ledger

| Capability | State | Current boundary |
|---|---|---|
| Local storage/manifests | Implemented | Arrow files, manifests, statistics, and local filesystem persistence work. Lifecycle management does not. |
| Transactions/conflicts | Partial | Shared-handle write-write OCC and typed row-ID conflicts exist; no serializability claim. |
| Row/index publication | Partial | Manifest/snapshot publication now validates target, row, segment, and vector identity; final branch verification and current evidence remain. |
| Snapshot/query reads | Partial | Immutable scan, predicate, explain, and vector-search reads exist; no read/write transaction API. |
| Query operators/pruning | Partial | Predicates, file/segment pruning, filtered ANN, and group-by primitives exist; no complete planner. |
| Immutable vector segments | Implemented | Manifest-listed HNSW segments load and fan out across snapshots. Growth is unbounded without lifecycle work. |
| Update/delete identity | Implemented within the supported facade | Physical live-target validation and one-row replacement cardinality are typed; logical identity remains deferred. |
| CLI | Partial | Fixed-assumption MVP inspection/demo commands; not a stable administration surface. |
| Python | Proposed | PyO3 scaffolding exports only `placeholder_version`; no database API exists. |
| Durability/recovery | Partial | File/directory durability, immutable row-ID high-water, manifest integrity, and crash/reopen evidence exist within named local bounds; full branch verification remains. |
| Schema/migrations | Partial | Dataset-owned schema and strict validation are implemented; schema evolution and migration remain deferred. |
| Loom/chaos/fuzz/bench evidence | Partial | Passing thorough-chaos evidence is local Ubuntu WSL only, not portable/native-platform or CI execution/log-retention evidence; local fuzz build/smoke now passes, while CI fuzz provenance and portable real-fixture performance bounds remain open. |
| Compaction/GC | Proposed | No compaction, vacuum, orphan cleanup, or bounded history implementation. |
| Cross-process coordination | Proposed | Independent openers do not share transaction state or durable conditional publication. |
| Branching/object storage | Proposed | No branch/merge or object-store backend is implemented. |

## Concurrency scope

The supported concurrency scope is **one process using one shared `Dataset` handle**. The commit lock,
row-ID allocator, recent-write history, and current snapshot live in that handle. Opening the same path
independently does not establish a transaction protocol.

## Directory-durability boundary

Dataset creation now fails rather than acknowledging a directory sync that the filesystem rejects.
Its immediate parent must already exist as the caller's durable anchor. Creation synchronizes the
dataset directory and that immediate parent; it does not create or synchronize an arbitrary
caller-owned ancestor chain. A retry after a pre-publication sync failure re-synchronizes this same
bounded pair before publishing the initial manifest. Manifest publication also synchronizes its
`_versions/` directory. The platform boundary is deliberately narrow: Windows uses a native directory
handle with `FILE_FLAG_BACKUP_SEMANTICS`; POSIX uses a directory handle; both are in scope only when
the open and flush succeed. Unsupported, invalid-input, and POSIX `EINVAL`-like outcomes are typed
`DurabilityUnsupported`, not best-effort success. Remote backends, cross-process publication, and
universal power-loss proof remain out of scope.

A final dataset-directory sync failure can occur after the initial manifest becomes visible. The
`Dataset::create` call still fails and must not be treated as acknowledged. Callers must first use
`Dataset::open` before retrying creation: if it opens, preserve/report the failed creation and repair
the filesystem boundary before relying on the dataset; only `NotFound` permits a later retry, which
again synchronizes the bounded dataset/parent chain. See the [Phase 1 audit](phase-1-audit.md#task-1-durability-recovery-boundary)
for the recovery procedure.

## Status vocabulary

- **Implemented:** present with direct source/test evidence.
- **Partial:** a usable slice exists, but important scope, verification, API, or operational work remains.
- **Proposed:** planned direction; no supported capability claim.
- **Historical/Superseded:** preserved context that does not govern current behavior.
