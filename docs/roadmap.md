# Roadmap

This is the active Phase 0–6 capability model. Historical phase numbers are preserved only as context
in [documentation history](history/README.md). Current implementation claims live in [status](status.md).

| Phase | Status | Scope | Exit signal |
|---|---|---|---|
| 0 — Foundation | Implemented within named local bounds | Local format, manifests, row allocation, and bounded transaction primitives. See the [Phase 0 foundation audit](audit/phase-0/audit.md). | Restart-safe row-ID non-reuse and retained foundation evidence pass within the named local-filesystem boundary. |
| 1 — Correctness and durability baseline | Implemented within named bounds | Shared-handle commits, immutable snapshots, typed conflicts, recovery/integrity, schema/error semantics, supported facade, and boundedness evidence. | All asserted guarantees have scope, implementation evidence, regression coverage, and current bounded performance evidence. |
| 2 — Query and usability | Implemented within named bounds | Stable schema/query APIs, scan/projection/filter/group-by integration, point lookup, CLI, and Python surface. See the [Phase 2 audit](audit/phase-2/audit.md). | Supported query/client behavior is documented and integration-tested within the embedded single-process boundary. |
| 3 — Operational lifecycle | Partial | Lifecycle diagnostics, explicit snapshot-preserving compaction, manifest retention including age policy, recognized orphan vacuum, and `Dataset::maintain(LifecycleMaintenancePolicy)` are implemented for one shared handle. Maintenance reports whether requested data-object and segment bounds were met; active snapshots and unknown object types remain explicit limitations. See the [inventory design](designs/phase-3/lifecycle-inventory.md), [executor design](designs/phase-3/manifest-retention-executor.md), [vacuum design](designs/phase-3/vacuum.md), and focused lifecycle tests. | Sustained operation safely bounds manifest/segment growth and manages retained data within an explicit supported policy. |
| 4 — Cross-process coordination | Proposed | Durable conditional publication, independent opener semantics, shared allocation, and process-boundary guarantees. | Separate processes coordinate without violating visibility, conflict, or durability invariants. |
| 5 — Branching and merge | Proposed | Fork, abort, merge, conflict reporting, and branch-aware manifests. | Branch behavior is correct under concurrency and recovery tests. |
| 6 — Object storage and deployment | Proposed | Object-store conditional writes, S3-compatible backends, remote recovery, and durability testing. | The supported correctness suite passes against supported remote backends. |

Cross-process coordination is owned exclusively by Phase 4. It is not an implementation task,
exit criterion, or blocker for Phases 0–3.

Phase 3 lifecycle now also includes explicit snapshot-preserving compaction and reclamation through
`Dataset::compact(CompactionPolicy)` plus bounded recognized-object cleanup through `Dataset::vacuum()`.
`Dataset::maintain(LifecycleMaintenancePolicy)` composes compaction, age retention, vacuum, and
inventory evidence. Universal growth enforcement across independent processes or unknown object
types remains outside the embedded single-process product boundary.

The Phase 3 exit signal above denotes evidence from one completed maintenance run. In particular,
`storage_bound_met` is the final inventory observation, not atomic or continuing storage-bound
enforcement; active snapshots, protected history, and unknown object types can keep physical growth
above a requested limit.

## Phase 4 reservation and entry gates

Phase 4 remains Proposed. The project reserves a future, versioned coordination seam without
implementing native cross-process writers now. The seam must carry dataset identity, capability and
protocol-version negotiation, expected-manifest-version preconditions, request IDs for idempotent
retries, typed row conflicts, and explicit visibility/durability acknowledgement semantics.

Phase 4 should begin only when all of these product and engineering signals are present:

- validated separate-process workloads rather than market momentum alone;
- measured evidence that the current shared-handle concurrency model is a bottleneck;
- an IPC/RPC design that fits the product's p95/p99 commit-latency budget;
- specified crash recovery, stale-participant, retry, and restart behavior; and
- a named operational owner for availability, security, observability, upgrades, and incident recovery.

The preferred first slice is an optional single-owner actor/IPC/RPC bridge around one authoritative
`Dataset`. Native independent multi-writer publication, distributed transactions, and a second commit
protocol remain out of scope until a superseding decision and evidence approve them. See [active
decision 0010](decisions.md#0010---deferred-cross-process-coordination-seam).

## Phase 1 closeout

The [Phase 1 audit](audit/phase-1/audit.md) is complete within its named bounds. Exact-head functional CI
[31644869407](https://github.com/negexx/strataDB/actions/runs/31644869407) and the full pinned-fixture
benchmark [31647664161](https://github.com/negexx/strataDB/actions/runs/31647664161) passed their
documented gates. Universal durability/performance bounds and later lifecycle work remain explicit
non-claims, not Phase 1 blockers.

Historical evidence references retained below:

- the manual Ubuntu CI run [30897605936](https://github.com/negexx/strataDB/actions/runs/30897605936) passed the named loom and thorough-chaos gates, including `2000/2000` chaos seeds;
- the completed exact-head branch CI run [30904907577](https://github.com/negexx/strataDB/actions/runs/30904907577) at revision `6bcd020` supplies retained command/outcome provenance, while the fresh native matrix [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345) covers Ubuntu and Windows; and
- universal operating-bound evidence and final limitations remain open; the validated full 100K-row segmented/lifecycle before/after matrix passed in [30907464857](https://github.com/negexx/strataDB/actions/runs/30907464857), showed mixed bounded results rather than a generalized product performance win, and does not establish universal bounds.

The [Phase 1 closeout ledger](phase-1-closeout-ledger.md) assigns each remaining finding an
acceptance assertion and future evidence location. It is tracking material, not a phase-exit claim.

Targeted implementation and regression evidence now covers future tombstones, live update/delete
targets and one-row replacement cardinality, restart-safe physical row-ID high-water allocation,
dataset-owned schema and recovery integrity checks, and the supported `Dataset`/`Snapshot`/
`Transaction` facade. The acknowledged local durability boundary is limited to successful POSIX
directory handles and Windows directory handles opened with `FILE_FLAG_BACKUP_SEMANTICS`; unsupported
or failed directory flushing returns `DurabilityUnsupported`. Legacy datasets without the required schema and
integrity metadata are rejected with `LegacyFormatNeedsMigration`, rather than opened unverified.

These are not requests for compaction in Phase 1.

## Deferred and refused scope

| Capability | Placement | Current boundary |
|---|---|---|
| Compaction, vacuum, orphan cleanup, bounded history | Phase 3 | Explicit compaction rewrites the current live snapshot, age retention removes eligible historical manifests, vacuum removes recognized unprotected `.arrow`/`.seg` and temporary objects, and `Dataset::maintain()` reports a final inventory observation of requested storage bounds; it is neither atomic nor continuing enforcement. Unknown object types and cross-process universal bounds remain out of scope. |
| Schema catalog, migrations, point lookup, time travel, stable query API | Phase 2–3 | Current Arrow batches/manifests are not a complete catalog or migration layer. |
| Independent-open and cross-process coordination | Phase 4 | Shared-handle locking is not durable conditional publication. |
| Fork, abort, branch reads, and merge | Phase 5 | Immutable segments are a prerequisite, not a delivered feature. |
| Object-store backend and deployment | Phase 6 | `LocalFs` is the implemented backend. |
| Usable Python API and stable administration CLI | Phase 2 | Typed Python/CLI query surfaces are implemented and integrated within the documented boundary. |

Strata deliberately refuses distributed transactions, full SQL planning, additional ANN families,
automatic conflict resolution, stronger isolation without a new decision, and agent memory/belief
semantics inside the database engine.
