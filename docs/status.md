# Status

This ledger describes the checked-in implementation, not an aspirational API. Source and test paths
are evidence pointers; [roadmap](roadmap.md) owns phase ordering.

## Overall state

The current Phase 1 audit branch is `codex/phase-1-audit` at commit `4d8fa3d`. The
[Phase 0 foundation audit](phase-0-audit.md)
records implementation closure only within its named local bounds; platform and loom evidence
remains incomplete. The [Phase 1 closeout ledger](phase-1-closeout-ledger.md) tracks the remaining
finding-level acceptance and evidence obligations; current exact-head evidence closes the phase
within its named bounds.

**Phase 0: Implemented within named local bounds.** This implementation closure does not establish
complete platform or loom coverage. The local Arrow/manifest/segmented-index foundation and
restart-safe row-ID regression are covered by local tests, retained Ubuntu foundation provenance.
Current CI configures 90-day retention of a native Windows restart transcript on future workflow
runs regardless of outcome, but no completed Windows run or retained artifact is linked in this
branch; Windows execution evidence is retained by the exact-head CI run. The exact loom gates build
and invoke crate-scoped binaries directly; normal Cargo summaries, timeouts, and interrupted jobs are
not loom passes. This does not claim portable filesystem behavior, cross-process coordination, serializability,
compaction, migration, object storage, or universal power-loss durability.

**Phase 1: Implemented within named bounds.** The [seven-lane Sol audit](phase-1-audit.md) findings
are implemented or evidence-closed inside the supported single-process/shared-`Dataset` boundary.

The [remaining performance evidence gates](phase-1-performance.md#remaining-evidence-gates) distinguish
bounded measurements from future product decisions; they do not reopen this phase.

**Phase 2: Implemented within named bounds.** The approved T1–T5 Rust query contract, T6 Python IPC
facade, T7 typed CLI surface, and T8 integration evidence are complete within the documented
embedded/single-process boundary. This does not alter the Phase 1 bounded status or claim
serializability, universal durability/performance/recall, cross-process coordination, or lifecycle
reclamation.

**Phase 3: Partial.** `Dataset::lifecycle_report()` and
`Dataset::retention_plan(RetentionPolicy)` remain read-only diagnostic/planning evidence for one
shared `Dataset` handle. `Dataset::prune_manifests(RetentionPolicy)` now executes the narrower
manifest-only slice: it takes lifecycle exclusivity before `commit_lock`, rebuilds exact listed-key
authority under both locks, retains current/latest/active-snapshot manifests, and deletes only
eligible historical manifests. It does not reclaim rows, segments, temporary objects, or arbitrary
orphans. `Dataset::compact(CompactionPolicy)` now publishes one replacement row file and vector
segment for the captured live snapshot, preserves active historical snapshots, and reclaims only
superseded listed objects after publication. Retention-by-age, arbitrary orphan cleanup, and
cross-process lifecycle work remain later designs. See
the [inventory design](phase-3-lifecycle-inventory-design.md), [manifest executor
design](phase-3-manifest-retention-executor-design.md), and focused [inventory](../crates/txn/tests/lifecycle_inventory.rs),
[planner](../crates/txn/tests/retention_plan.rs), and [executor](../crates/txn/tests/manifest_retention_executor.rs)
tests.

## Capability ledger

| Capability | State | Current boundary |
|---|---|---|
| Local storage/manifests | Implemented | Arrow files, manifests, statistics, and local filesystem persistence work. Manifest pruning and explicit snapshot-preserving compaction/reclamation exist; vacuum and arbitrary orphan cleanup do not. |
| Transactions/conflicts | Partial | Shared-handle write-write OCC and typed row-ID conflicts exist; no serializability claim. |
| Row/index publication | Partial | Manifest/snapshot publication validates row ownership/uniqueness, manifest-listed metadata and checksums, and row/index consistency. It does not establish byte-for-byte identity of decoded Arrow vector values: tampering that changes vector values while recomputing the corresponding metadata/checksums is outside the supported integrity boundary. Final branch verification and current evidence remain. |
| Snapshot/query reads | Partial | Immutable scan, predicate, explain, and vector-search reads exist; no read/write transaction API. |
| Query operators/pruning | Partial | Predicates, file/segment pruning, filtered ANN, and group-by primitives exist; no complete planner. |
| Immutable vector segments | Implemented | Manifest-listed HNSW segments load and fan out across snapshots; explicit compaction can replace the current segment set while preserving active historical snapshots. |
| Update/delete identity | Implemented within the supported facade | Physical live-target validation and one-row replacement cardinality are typed; logical identity remains deferred. |
| CLI | Partial | Typed lookup, group-by, and query-scan commands are implemented; query-scan reports deterministic result indexes because physical `_row_id` is reserved, while legacy scan/filter/inspect/explain remain compatibility commands. Mutations require explicit single-writer acknowledgement. |
| Python | Partial | Thin PyO3 Dataset/Snapshot query facade returns Arrow IPC bytes for tabular results and typed vector matches; integration review remains. |
| Durability/recovery | Partial | File/directory durability, immutable row-ID high-water, manifest integrity, and crash/reopen evidence exist within named local bounds; full branch verification remains. |
| Schema/migrations | Partial | Dataset-owned schema and strict validation are implemented; schema evolution and migration remain deferred. |
| Lifecycle diagnostics, planning, pruning, and compaction | Partial | `Dataset::lifecycle_report()` inventories one captured snapshot, `Dataset::retention_plan()` remains advisory, `Dataset::prune_manifests()` deletes only eligible historical manifests, and `Dataset::compact()` publishes replacement row/index objects while protecting active snapshots and reclaiming only superseded listed objects. Vacuum, arbitrary orphan cleanup, age-based retention, and universal storage-growth bounds remain open. See the lifecycle design documents and focused tests. |
| Loom/chaos/fuzz/bench evidence | Implemented within named bounds | Exact-head CI run [31644869407](https://github.com/negexx/strataDB/actions/runs/31644869407) passed the named functional gates; benchmark run [31647664161](https://github.com/negexx/strataDB/actions/runs/31647664161) passed the synthetic and full pinned-fixture matrices with retained provenance. Universal bounds and final limitations remain explicit non-claims. |
| Compaction/GC | Partial | Explicit snapshot-preserving row/segment compaction and post-publication reclamation are implemented. Vacuum, arbitrary orphan cleanup, age-based retention, and a supported storage-growth bound remain open. |
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
