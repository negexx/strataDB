# Status

This ledger describes the checked-in implementation, not an aspirational API. Source and test paths
are evidence pointers; [roadmap](roadmap.md) owns phase ordering.

## Overall state

The current baseline is merged PR #58 commit `21811031d0fbe3ed3f55532941c056c0c9e091b0`. The
[Phase 0 foundation audit](phase-0-audit.md) records the foundation as implemented within its named
local bounds, with retained CI evidence. The [Phase 1 closeout ledger](phase-1-closeout-ledger.md)
tracks the remaining finding-level acceptance and evidence obligations; it does not change this
Partial/blocked state.

**Phase 0: Implemented within named local bounds.** The local Arrow/manifest/segmented-index
foundation and restart-safe row-ID regression are covered by local tests, retained Ubuntu foundation
provenance. Current CI configures 90-day retention of a native Windows restart transcript on future
workflow runs regardless of outcome, but no completed Windows run or retained artifact is linked in
this branch; Windows execution evidence remains pending. The exact loom gates build
and invoke crate-scoped binaries directly; normal Cargo summaries, timeouts, and interrupted jobs are
not loom passes. This does not claim portable filesystem behavior, cross-process coordination, serializability,
compaction, migration, object storage, or universal power-loss durability.

**Phase 1: Partial — blocked.** The [seven-lane Sol audit](phase-1-audit.md) found correctness,
durability, schema, API-boundary, verification, and performance-evidence blockers inside the supported
single-process/shared-`Dataset` boundary.

**Phase 2: Implemented within named bounds.** The approved T1–T5 Rust query contract, T6 Python IPC
facade, T7 typed CLI surface, and T8 integration evidence are complete within the documented
embedded/single-process boundary. This does not alter the independent Phase 1 blocked state or claim
serializability, universal durability/performance/recall, cross-process coordination, or lifecycle
reclamation.

**Phase 3: Partial.** `Dataset::lifecycle_report()` and
`Dataset::retention_plan(RetentionPolicy)` remain read-only diagnostic/planning evidence for one
shared `Dataset` handle. `Dataset::prune_manifests(RetentionPolicy)` now executes the narrower
manifest-only slice: it takes lifecycle exclusivity before `commit_lock`, rebuilds exact listed-key
authority under both locks, retains current/latest/active-snapshot manifests, and deletes only
eligible historical manifests. It does not reclaim rows, segments, temporary objects, or arbitrary
orphans; compaction, retention-by-age, and cross-process lifecycle work remain later designs. See
the [inventory design](phase-3-lifecycle-inventory-design.md), [manifest executor
design](phase-3-manifest-retention-executor-design.md), and focused [inventory](../crates/txn/tests/lifecycle_inventory.rs),
[planner](../crates/txn/tests/retention_plan.rs), and [executor](../crates/txn/tests/manifest_retention_executor.rs)
tests.

## Capability ledger

| Capability | State | Current boundary |
|---|---|---|
| Local storage/manifests | Implemented | Arrow files, manifests, statistics, and local filesystem persistence work. Manifest-only historical-manifest pruning exists; row/segment reclamation, compaction, vacuum, and orphan cleanup do not. |
| Transactions/conflicts | Partial | Shared-handle write-write OCC and typed row-ID conflicts exist; no serializability claim. |
| Row/index publication | Partial | Manifest/snapshot publication validates row ownership/uniqueness, manifest-listed metadata and checksums, and row/index consistency. It does not establish byte-for-byte identity of decoded Arrow vector values: tampering that changes vector values while recomputing the corresponding metadata/checksums is outside the supported integrity boundary. Final branch verification and current evidence remain. |
| Snapshot/query reads | Partial | Immutable scan, predicate, explain, and vector-search reads exist; no read/write transaction API. |
| Query operators/pruning | Partial | Predicates, file/segment pruning, filtered ANN, and group-by primitives exist; no complete planner. |
| Immutable vector segments | Implemented | Manifest-listed HNSW segments load and fan out across snapshots. Growth is unbounded without lifecycle work. |
| Update/delete identity | Implemented within the supported facade | Physical live-target validation and one-row replacement cardinality are typed; logical identity remains deferred. |
| CLI | Partial | Typed lookup, group-by, and query-scan commands are implemented; query-scan reports deterministic result indexes because physical `_row_id` is reserved, while legacy scan/filter/inspect/explain remain compatibility commands. Mutations require explicit single-writer acknowledgement. |
| Python | Partial | Thin PyO3 Dataset/Snapshot query facade returns Arrow IPC bytes for tabular results and typed vector matches; integration review remains. |
| Durability/recovery | Partial | File/directory durability, immutable row-ID high-water, manifest integrity, and crash/reopen evidence exist within named local bounds; full branch verification remains. |
| Schema/migrations | Partial | Dataset-owned schema and strict validation are implemented; schema evolution and migration remain deferred. |
| Lifecycle diagnostics, planning, and manifest pruning | Partial | `Dataset::lifecycle_report()` inventories one captured snapshot, and `Dataset::retention_plan()` remains advisory. `Dataset::prune_manifests()` uses preparation-spanning lifecycle exclusivity followed by `commit_lock` to rebuild exact listed-manifest authority and delete only eligible historical manifests. It preserves current/latest/active-snapshot manifests and does not delete data, segments, temporary objects, or arbitrary orphans. See the [inventory design](phase-3-lifecycle-inventory-design.md), [executor design](phase-3-manifest-retention-executor-design.md), and [executor tests](../crates/txn/tests/manifest_retention_executor.rs). |
| Loom/chaos/fuzz/bench evidence | Partial | Exact-head CI run [30904907577](https://github.com/negexx/strataDB/actions/runs/30904907577) at revision `6bcd020` retains current command/outcome provenance; the manual Ubuntu run [30897605936](https://github.com/negexx/strataDB/actions/runs/30897605936) passed the named loom gates and thorough-chaos `2000/2000` seed gate; native Ubuntu/Windows checks and the validated full 100K-row pinned-fixture segmented/lifecycle matrix passed in [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345) and [30907464857](https://github.com/negexx/strataDB/actions/runs/30907464857). Universal bounds and final limitations remain open. |
| Compaction/GC | Proposed | No compaction, vacuum, orphan cleanup, or bounded history implementation. |
| Cross-process coordination | Proposed | Independent openers do not share transaction state or durable conditional publication. The reserved future seam includes versioned capability negotiation, expected-manifest-version preconditions, request IDs with idempotent retries, typed conflicts, and explicit visibility/durability acknowledgements; Phase 4 entry gates are recorded in the [roadmap](roadmap.md#phase-4-reservation-and-entry-gates) and [decision 0010](decisions.md#0010---deferred-cross-process-coordination-seam). |
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
