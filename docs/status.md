# Status

This ledger describes the checked-in implementation, not an aspirational API. Source and test paths
are evidence pointers; [roadmap](roadmap.md) owns phase ordering.

## Overall state

The historical Phase 1 audit branch was `codex/phase-1-audit` at commit `4d8fa3d`; its
[Phase 0 foundation audit](audit/phase-0/audit.md)
records implementation closure only within its named local bounds; platform and loom evidence
remains incomplete. The [Phase 1 closeout ledger](phase-1-closeout-ledger.md) tracks the remaining
finding-level acceptance and evidence obligations; current exact-head evidence closes the phase
within its named bounds.

The closeout work is merged into `main` at `65449a9` (PR #68).

**Phase 0: Implemented within named local bounds.** This implementation closure does not establish
complete platform or loom coverage. The local Arrow/manifest/segmented-index foundation and
restart-safe row-ID regression are covered by local tests, retained Ubuntu foundation provenance.
Current CI configures 90-day retention of a native Windows restart transcript on future workflow
runs regardless of outcome, but no completed Windows run or retained artifact is linked in this
branch; Windows execution evidence is retained by the exact-head CI run. The exact loom gates build
and invoke crate-scoped binaries directly; normal Cargo summaries, timeouts, and interrupted jobs are
not loom passes. This does not claim portable filesystem behavior, cross-process coordination, serializability,
compaction, migration, object storage, or universal power-loss durability.

**Phase 1: Implemented within named bounds.** The [seven-lane Sol audit](audit/phase-1/audit.md) findings
are implemented or evidence-closed inside the supported single-process/shared-`Dataset` boundary.

The [remaining performance evidence gates](phase-1-performance.md#remaining-evidence-gates) distinguish
bounded measurements from future product decisions; they do not reopen this phase.

**Phase 2: Implemented within named bounds.** The approved T1–T5 Rust query contract, T6 Python IPC
facade, T7 typed CLI surface, and T8 integration evidence are complete within the documented
embedded/single-process boundary. This does not alter the Phase 1 bounded status or claim
serializability, universal durability/performance/recall, cross-process coordination, or lifecycle
reclamation.

**Phase 3: Implemented within named bounds.** `Dataset::lifecycle_report()` and
`Dataset::retention_plan(RetentionPolicy)` remain read-only diagnostic/planning evidence for one
shared `Dataset` handle. `Dataset::prune_manifests(RetentionPolicy)` now executes the narrower
manifest-only slice: it takes lifecycle exclusivity before `commit_lock`, rebuilds exact listed-key
authority under both locks, retains current/latest/active-snapshot manifests, and deletes only
eligible historical manifests. It does not reclaim rows or segments. `Dataset::vacuum()` removes
recognized temporary objects and unprotected `.arrow`/`.seg` objects only after every
recovery-recognized numeric `*.manifest` key has been validated; unknown object types remain
untouched. `Dataset::compact(CompactionPolicy)`
compacts an empty live set to zero row files; for a nonempty live set, it publishes one row file per
maximal contiguous live physical-row-ID run and at most one vector segment. It preserves active
historical snapshots and reclaims only superseded listed objects after publication.
`Dataset::prune_manifests_by_age(AgeRetentionPolicy)` and `Dataset::maintain(LifecycleMaintenancePolicy)`
provide explicit history and one final inventory observation of requested storage bounds;
`storage_bound_met` is the final observation of one explicit maintenance run, not atomic or
continuous enforcement. Active snapshots, protected history, unknown objects, and noncontiguous
physical row IDs can prevent the requested bound; the API is not a cross-process quota or SLO.
Snapshots remain protected rather than being deleted. Cross-process lifecycle work remains later
design. Fresh exact-head lifecycle evidence is recorded in the
[Phase 3 closeout audit](audit/phase-3/audit.md) and
[Phase 3 verification report](phase-3-verification-report.md). See
the [inventory design](designs/phase-3/lifecycle-inventory.md), [manifest executor
design](designs/phase-3/manifest-retention-executor.md), and focused [inventory](../crates/txn/tests/lifecycle_inventory.rs),
[planner](../crates/txn/tests/retention_plan.rs), and [executor](../crates/txn/tests/manifest_retention_executor.rs)
tests.

## Capability ledger

| Capability | State | Current boundary |
|---|---|---|
| Local storage/manifests | Implemented | Arrow files, manifests, statistics, and local filesystem persistence work. Manifest pruning, age retention, explicit snapshot-preserving compaction/reclamation, and recognized orphan vacuum exist; unknown object types remain outside cleanup authority. |
| Transactions/conflicts | Partial | Shared-handle write-write OCC and typed row-ID conflicts exist; no serializability claim. |
| Row/index publication | Partial | Manifest/snapshot publication validates row ownership/uniqueness, manifest-listed metadata and checksums, and row/index consistency. It does not establish byte-for-byte identity of decoded Arrow vector values: tampering that changes vector values while recomputing the corresponding metadata/checksums is outside the supported integrity boundary. Final branch verification and current evidence remain. |
| Snapshot/query reads | Partial | Immutable scan, predicate, explain, and vector-search reads exist. The stable transaction API also provides bounded transaction-base reads: scans (including predicate reads) and group reads expose staged inserts, replacements, and deletes; lookup reflects staged replacements/deletes only for physical row IDs already in the base snapshot, because staged inserts receive no physical row ID until commit and cannot be looked up pre-commit. `vector_search` after staged writes returns a typed unsupported-transaction-read error. This is not a general read/write query interface and does not provide full serializability. |
| Query operators/pruning | Implemented within named bounds | The bounded logical/physical planner covers supported immutable-snapshot scan, grouped aggregation, and vector-search requests. Its stable explain value lists logical operators, selected reused physical operators, and captured row-file/index-segment pruning plus overlay observations; these are not cost or cardinality guarantees. It delegates to the existing scan, zone-map pruning, tombstone, group-by, and immutable HNSW-segment paths, preserving their result and immutable-snapshot contracts. Local Criterion evidence used four committed 64-row batches (256 rows): direct/planned 95% intervals were 577.39–581.76/567.16–575.79 µs (projection), 136.70–137.28/138.81–140.99 µs (selective scan), 150.72–152.72/151.74–152.36 µs (group-by), and 1.3549–1.3710/1.3599–1.3729 ms (vector); see the command and limits in the [Phase 3 verification report](phase-3-verification-report.md#task-3-query-planning-evidence). This is not SQL, a general optimizer, a stronger isolation claim, or a universal performance result. |
| Immutable vector segments | Implemented | Manifest-listed HNSW segments load and fan out across snapshots; explicit compaction can replace the current segment set while preserving active historical snapshots. |
| Update/delete identity | Implemented within the supported facade | Physical live-target validation and one-row replacement cardinality are typed; logical identity remains deferred. |
| CLI | Implemented within named bounds | Stable administration commands expose `inspect --json`, `schema`, planned `explain --json`, `migration validate/run/status`, `manifest-status`, `recovery-status`, and `evidence`, alongside the existing typed lookup/group-by/query-scan and compatibility commands. Each new administration result has human and JSON rendering; process exit categories are stable as operational `1`, usage `2`, conflict `3`, unsupported `4`, and corruption `5`. `evidence` reports the retained Criterion command rather than treating one CLI invocation as a benchmark. All operations remain local and scoped to the opened dataset; mutation commands retain explicit single-writer acknowledgement and none add cross-process coordination. |
| Python | Partial | Thin PyO3 Dataset/Snapshot query facade returns Arrow IPC bytes for tabular results and typed vector matches; integration review remains. |
| Durability/recovery | Partial | File/directory durability, immutable row-ID high-water, manifest integrity, and crash/reopen evidence exist within named local bounds; full branch verification remains. |
| Schema/migrations | Implemented within named bounds | Every manifest captures a supported catalog version. The one explicit `add_nullable_column` transition rewrites row objects/copies immutable segments before atomically publishing the new manifest; unsupported, stale, reverse, incompatible, and lossy requests remain typed errors. The CLI exposes validation and execution only for that named transition; it does not provide arbitrary schema evolution. |
| Lifecycle diagnostics, planning, pruning, and compaction | Implemented within named bounds | `Dataset::lifecycle_report()` inventories one captured snapshot, retention APIs delete eligible historical manifests, and `Dataset::compact()` writes zero row files for an empty live set or one per maximal contiguous live physical-row-ID run for a nonempty set, with at most one vector segment, while protecting active snapshots. `Dataset::vacuum()` removes recognized unprotected objects. `Dataset::maintain()` returns `storage_bound_met` only as one explicit run's final inventory observation, not atomic or continuous enforcement: active snapshots, protected history, unknown objects, and noncontiguous physical row IDs can prevent a requested bound. It is not a cross-process quota or SLO; universal cross-process bounds remain outside scope. See the lifecycle design documents, [Phase 3 closeout audit](audit/phase-3/audit.md), [Phase 3 verification report](phase-3-verification-report.md), and focused tests. |
| Loom/chaos/fuzz/bench evidence | Implemented within named bounds | Exact-head CI run [31644869407](https://github.com/negexx/strataDB/actions/runs/31644869407) passed the named functional gates; benchmark run [31647664161](https://github.com/negexx/strataDB/actions/runs/31647664161) passed the synthetic and full pinned-fixture matrices with retained provenance. Universal bounds and final limitations remain explicit non-claims. |
| Compaction/GC | Implemented within named bounds | Explicit snapshot-preserving compaction writes zero row files for an empty live set or one per maximal contiguous live physical-row-ID run for a nonempty set, plus at most one vector segment. Post-publication reclamation, age-based manifest retention, recognized-object vacuum, and a maintenance operation whose final inventory may report `storage_bound_met` are implemented. That field is one explicit run's final observation, not an atomic or continuing bound: active snapshots, protected history, unknown objects, and noncontiguous physical row IDs can keep physical growth above a requested limit. It is not a cross-process quota or SLO. |
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
again synchronizes the bounded dataset/parent chain. See the [Phase 1 audit](audit/phase-1/audit.md#task-1-durability-recovery-boundary)
for the recovery procedure.

## Status vocabulary

- **Implemented:** present with direct source/test evidence.
- **Partial:** a usable slice exists, but important scope, verification, API, or operational work remains.
- **Proposed:** planned direction; no supported capability claim.
- **Historical/Superseded:** preserved context that does not govern current behavior.
