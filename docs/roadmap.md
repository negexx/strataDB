# Capability roadmap

This is the active Phase 0–6 capability model. Older phase documents are mapped in [the status ledger](status.md); their numbers are not authoritative.

| Phase | Status | Scope | Depends on | Exit criteria |
|---|---|---|---|---|
| 0 — Foundation | Partial | Local format, manifests, the row-identity contract, and the bounded transaction model. | None | Format and transaction primitives are represented in source/tests; durable non-reuse of abandoned row-ID claims across reopen remains blocked under Phase 1. |
| 1 — Correctness and durability baseline | Partial — blocked | Shared-handle commit path, immutable snapshots, typed conflicts, row/index publication, recovery, schema/error behavior, manifest/row-file integrity boundaries, supported facade boundaries, manifest/segment growth and cleanup obligations, and verification evidence. | Phase 0 primitives | Exit criteria are not met; guarantees and lifecycle obligations must be explicitly bounded, tested, documented, and consistent with the supported public API. |
| 2 — Query and usability | Partial | Stable schema/query APIs, scan/projection/filter/group-by integration, point lookup, and coherent CLI/Python surfaces. | Phase 1 evidence | Supported query behavior and user surfaces are documented and covered by integration tests. |
| 3 — Operational lifecycle | Proposed | Compaction, vacuum/orphan cleanup, index lifecycle, bounded history, migration compatibility, and diagnostics. | Phases 1–2 | Sustained operation bounds manifest/segment growth and safely manages retained data. |
| 4 — Cross-process coordination | Proposed | Durable conditional publication, independent opener semantics, shared allocation/commit coordination, and process-boundary guarantees. | Phase 1 plus lifecycle rules | Separate processes can coordinate without violating acknowledged-write, conflict, or visibility invariants. |
| 5 — Branching and merge | Proposed | Fork, abort, merge, conflict reporting, and branch-aware manifests. | Immutable segments and Phase 4 coordination | Branch isolation, abort, and merge behavior are correct under concurrency and recovery tests. |
| 6 — Object storage and deployment | Proposed | Object-store conditional writes, S3-compatible backends, backend recovery, and remote durability testing. | Phases 3–4 | The relevant correctness suite passes against supported remote backends. |

## Phase boundaries

### Phase 0 — Foundation

Status: Partial. Local format and transaction primitives exist, but durable non-reuse of abandoned row-ID claims across immediate reopen remains a Phase 1 audit blocker.

Scope: local columnar storage, manifest/version primitives, the global non-reused row-ID contract, and the transaction-format contract.

Non-goals: cross-process coordination, object storage, branching, compaction, and a Python API.

Exit: the foundation remains documented in [the Phase 0 specification](design/phase-0-transaction-and-format-spec.md) and partially represented by current storage/transaction tests. It is not fully implemented while the restart-reuse counterexample remains unresolved.

### Phase 1 — Correctness and durability baseline

Status: Partial — blocked by the 2026-08-01 seven-lane Sol audit. See the [consolidated report](audits/phase-1-sol-audit-report.md) and its [lane reports](audits/phase-1/README.md).

Scope: atomic row-data plus immutable vector-segment publication through a manifest; snapshots; typed write-write conflicts; crash/recovery and corruption-integrity boundaries; update/delete identity semantics; schema enforcement; the supported facade for transaction invariants; manifest/segment growth and cleanup obligations; and loom/chaos/test evidence.

Dependencies: Phase 0 primitives; Phase 0's durable row-ID contract must be closed through this phase's recovery work.

Non-goals: cross-process transactions, full read/write snapshot isolation, silent conflict resolution, and unbounded-growth acceptance.

Exit: every asserted guarantee has a defined scope, implementation evidence, and direct verification; row and index visibility/recovery remain atomic within that scope; the supported corruption threat model is explicit and covered manifest/pruning metadata and row bytes have integrity plus semantic validation; the public facade carrying transaction invariants is enforced or explicitly bounded; manifest/segment growth is measured and the cleanup obligation is documented without assuming compaction exists; invalid tombstones, allocator reuse, manifest inconsistency, directory durability, schema ambiguity, and missing CI gates have direct dispositions and regression coverage.

Current audit blockers: validate delete/update targets and cardinality; resolve durable row-ID non-reuse; fail closed on directory-sync errors; validate manifest identity and semantic relationships; define the corruption threat model and protect covered manifest/pruning metadata and row bytes with checksums/integrity; establish dataset-owned schema semantics; close or disclaim invariant-bypassing public storage/index surfaces; gate transaction loom and non-skipping chaos/checkpoint evidence; and retain a current segmented performance/boundedness matrix. Cross-process coordination remains Phase 4 and compaction/GC remains Phase 3.

### Phase 2 — Query and usability

Scope: stable schema/query APIs, projection and scan behavior, filters and group-by, point lookup, and coherent CLI/Python interfaces.

Dependencies: Phase 1 evidence and stable error/schema behavior.

Non-goals: full SQL, a distributed query planner, and claiming the placeholder binding as a Python API.

Exit: documented supported queries and client interfaces have integration coverage and no ambiguity about unsupported operations.

### Phase 3 — Operational lifecycle

Scope: compaction, vacuum/orphan cleanup, index lifecycle management, bounded history, migration/version compatibility, and operational diagnostics.

Dependencies: Phases 1–2.

Non-goals: silently deleting data still reachable by a snapshot or future branch, and using compaction to mask correctness gaps.

Exit: retention, cleanup, and migration behavior are bounded, observable, and crash-safe.

### Phase 4 — Cross-process coordination

Scope: durable conditional publication/CAS, independent `Dataset::open` semantics, shared allocation, and commit coordination across processes.

Dependencies: Phase 1 commit/recovery evidence and Phase 3 lifecycle rules.

Non-goals: distributed consensus, multi-node transactions, or treating a local in-process lock as a process-wide protocol.

Exit: process-boundary tests prove conflict, durability, and visibility behavior for independently opened datasets.

### Phase 5 — Branching and merge

Scope: fork/branch, abort, merge, conflict reporting, and branch-aware manifests over immutable segments.

Dependencies: the segment layout, Phase 4 coordination, and lifecycle/GC rules.

Non-goals: branching designs that require rewriting shared index state, implicit merge resolution, and treating layout adoption as a shipped branching feature.

Exit: fork, abort, merge, recovery, and concurrent branch isolation have explicit semantics and pass correctness tests.

### Phase 6 — Object storage and deployment

Scope: object-store conditional writes, S3-compatible backends, backend-specific recovery, and remote durability testing.

Dependencies: Phase 4 conditional-publication semantics and Phase 3 lifecycle policy.

Non-goals: making object storage the unverified default, weakening acknowledged-write durability, or adding distributed transactions.

Exit: supported object-store backends pass the applicable storage, transaction, recovery, and chaos suites without changed guarantees.
