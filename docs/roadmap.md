# Roadmap

This is the active Phase 0–6 capability model. Historical phase numbers are preserved only as context
in [documentation history](history/README.md). Current implementation claims live in [status](status.md).

| Phase | Status | Scope | Exit signal |
|---|---|---|---|
| 0 — Foundation | Implemented within named local bounds | Local format, manifests, row allocation, and bounded transaction primitives. See the [Phase 0 foundation audit](phase-0-audit.md). | Restart-safe row-ID non-reuse and retained foundation evidence pass within the named local-filesystem boundary. |
| 1 — Correctness and durability baseline | Partial — blocked | Shared-handle commits, immutable snapshots, typed conflicts, recovery/integrity, schema/error semantics, supported facade, and boundedness evidence. | All asserted guarantees have scope, implementation evidence, regression coverage, and current performance bounds. |
| 2 — Query and usability | Partial | Stable schema/query APIs, scan/projection/filter/group-by integration, point lookup, CLI, and Python surface. See the [Phase 2 audit](phase-2-audit.md). | Supported query/client behavior is documented and integration-tested. |
| 3 — Operational lifecycle | Proposed | Compaction, vacuum/orphan cleanup, history retention, index lifecycle, migrations, and diagnostics. | Sustained operation safely bounds manifest/segment growth and manages retained data. |
| 4 — Cross-process coordination | Proposed | Durable conditional publication, independent opener semantics, shared allocation, and process-boundary guarantees. | Separate processes coordinate without violating visibility, conflict, or durability invariants. |
| 5 — Branching and merge | Proposed | Fork, abort, merge, conflict reporting, and branch-aware manifests. | Branch behavior is correct under concurrency and recovery tests. |
| 6 — Object storage and deployment | Proposed | Object-store conditional writes, S3-compatible backends, remote recovery, and durability testing. | The supported correctness suite passes against supported remote backends. |

## Phase 1 blockers

The [Phase 1 audit](phase-1-audit.md) is complete. Phase 1 remains Partial and blocked by:

- incomplete VER-01 direct-regression inventory and final branch verification; the manual Ubuntu CI run [30861009780](https://github.com/negexx/strataDB/actions/runs/30861009780) passed the named loom and thorough-chaos gates;
- the completed exact-head branch CI run [30865323724](https://github.com/negexx/strataDB/actions/runs/30865323724) supplies retained command/outcome provenance, while the fresh native matrix [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345) covers Ubuntu and Windows; and
- incomplete full real-fixture before/after segmented performance and universal operating-bound evidence (the pinned Ubuntu fixture smoke passed in [30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345)).

The [Phase 1 closeout ledger](phase-1-closeout-ledger.md) assigns each remaining finding an
acceptance assertion and future evidence location. It is tracking material, not a phase-exit claim.

Targeted implementation and regression evidence now covers future tombstones, live update/delete
targets and one-row replacement cardinality, restart-safe physical row-ID high-water allocation,
dataset-owned schema and recovery integrity checks, and the supported `Dataset`/`Snapshot`/
`Transaction` facade. The acknowledged local durability boundary is limited to successful POSIX
directory handles and Windows directory handles opened with `FILE_FLAG_BACKUP_SEMANTICS`; unsupported
or failed directory flushing returns `DurabilityUnsupported`. Legacy datasets without the required schema and
integrity metadata are rejected with `LegacyFormatNeedsMigration`, rather than opened unverified.

These are not requests for cross-process transactions, serializability, or compaction in Phase 1.

## Deferred and refused scope

| Capability | Placement | Current boundary |
|---|---|---|
| Compaction, vacuum, orphan cleanup, bounded history | Phase 3 | No reclamation or segment-count bound is implemented. |
| Schema catalog, migrations, point lookup, time travel, stable query API | Phase 2–3 | Current Arrow batches/manifests are not a complete catalog or migration layer. |
| Independent-open and cross-process coordination | Phase 4 | Shared-handle locking is not durable conditional publication. |
| Fork, abort, branch reads, and merge | Phase 5 | Immutable segments are a prerequisite, not a delivered feature. |
| Object-store backend and deployment | Phase 6 | `LocalFs` is the implemented backend. |
| Usable Python API and stable administration CLI | Phase 2 | Bindings are a placeholder module; CLI commands are fixed-shape demo/inspection tooling. |

Strata deliberately refuses distributed transactions, full SQL planning, additional ANN families,
automatic conflict resolution, stronger isolation without a new decision, and agent memory/belief
semantics inside the database engine.
