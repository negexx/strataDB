# Strata Documentation and Phase Roadmap Cleanup — Design

**Date:** 2026-08-01
**Status:** Approved direction; implementation plan pending
**Scope:** Documentation, ADR organization, project-status truthfulness, roadmap, and agent guidance. No runtime behavior changes.

## Problem

Strata has a substantial design and implementation history, but the documentation currently mixes active architecture with superseded designs. In particular, older vector-index, transaction, and phase documents describe a mutable graph, delta-log publication, or transaction semantics that no longer match the current immutable-segment implementation. There are also stale OpenCode references and broken links to removed rule files.

The result is avoidable ambiguity for both humans and AI agents:

- it is difficult to tell what is implemented, partial, proposed, or superseded;
- historical decisions are easy to mistake for current requirements;
- the roadmap is phase-numbered but lacks one authoritative status ledger;
- the public product claim is broader than the current API and concurrency boundary;
- documentation links and CI comments still reference retired project structure.

## Goals

1. Make the current implementation and its actual guarantees discoverable from a small set of authoritative documents.
2. Preserve ADR and design history in a separate, clearly marked documentation area.
3. Remove genuinely redundant or machine-specific project artifacts and repair stale references.
4. Establish a phase roadmap with explicit status, evidence, exit criteria, and known limitations.
5. Define Phase 1 as the current correctness/durability baseline and audit it with independent Sol review lanes.
6. Keep the cleanup documentation-only unless a stale reference requires a minimal configuration or CI edit.

## Non-goals

- No implementation of missing database features in this change.
- No rewrite of historical ADR decisions.
- No deletion of historical reasoning merely because a decision was later reversed.
- No expansion from single-process/shared-handle concurrency to cross-process or distributed concurrency.
- No new dependency or build-system migration.

## Chosen structure

Current and historical material will be separated as follows:

```text
docs/
  README.md                         # documentation map and reading order
  status.md                         # implementation truth ledger
  roadmap.md                        # phases, milestones, exit criteria
  architecture.md                   # current architecture only
  decisions/                        # current decisions and active proposals
  history/
    README.md                       # archive policy and historical index
    decisions/                      # superseded/reversed ADRs
    design/                         # superseded phase/mechanism specifications
    analysis/                       # historical investigations and audits
  design/                           # current or still-governing specifications
  superpowers/specs/                # design records for agent-assisted work
  superpowers/plans/                # executable implementation plans
```

Historical files will retain their original filenames and content wherever practical. Their headers and the history index will identify why they are historical and which current document supersedes them. References from active documents will point to the current replacement, with historical links only where the decision trail is useful.

## Status vocabulary

Every roadmap item and major design document will use one of these states:

- **Implemented:** present in the current source and covered by a passing verification path.
- **Partial:** some pieces exist, but the stated capability or guarantee is incomplete.
- **Proposed:** accepted as future direction but not implemented.
- **Historical:** retained for context; not an active requirement.
- **Superseded:** replaced by a later decision or design; retained in history.
- **Deferred:** intentionally out of the current scope.

“Implemented” will include an evidence pointer to source/tests/CI where the evidence is material. Product language will distinguish “shared `Dataset` handle in one process” from “multiple independent processes.”

## Phase model

The roadmap will use capability phases rather than treating every old numbered design document as a current milestone:

### Phase 0 — Repository and format foundation

Status: implemented in source, with some old specification text requiring reconciliation. Covers local storage primitives, manifest/version model, Arrow persistence, recovery, and basic dataset lifecycle.

### Phase 1 — Correctness and durability baseline

Status: current audit target; partially implemented. Scope is the existing single-process, shared-handle engine:

- atomic row-data plus vector-segment publication through a manifest;
- immutable snapshots and typed write-write conflicts;
- crash recovery and durability boundaries;
- update/delete identity semantics, schema enforcement, and error behavior;
- boundedness of manifest/segment growth and cleanup obligations;
- test, loom, chaos, and CI evidence.

Phase 1 is complete only when its guarantees are explicitly bounded, tested, documented, and consistent with the public API. It does not silently promise cross-process coordination or full read/write snapshot transactions.

### Phase 2 — Query and usability surface

Status: partially implemented. Covers stable schema/query APIs, projection and scan behavior, filter/group-by integration, point lookup, and a coherent CLI/Python surface.

### Phase 3 — Operational lifecycle

Status: proposed. Covers compaction, vacuum/orphan cleanup, index lifecycle management, bounded history, migration/version compatibility, and operational diagnostics.

### Phase 4 — Multi-handle and cross-process coordination

Status: proposed. Covers durable conditional publication/CAS, shared allocation/commit coordination, independent opener semantics, and the guarantees required for process boundaries.

### Phase 5 — Branching and merge workflows

Status: proposed. Covers fork/branch, abort, merge, conflict reporting, and branch-aware manifests.

### Phase 6 — Object storage and deployment backends

Status: proposed. Covers object-store conditional writes, S3-compatible backends, backend-specific recovery, and remote durability testing.

Phase numbers in old documents will be mapped to this capability model in `docs/status.md`; old phase documents will not be treated as authoritative merely because their number matches.

## Phase 1 Sol audit pack

The audit will use independent read-only Sol lanes, followed by a controller reconciliation against source, tests, and CI:

1. **Correctness:** commit ordering, conflict detection, update/delete semantics, recovery, and invariant preservation.
2. **Concurrency:** lock scope, interleavings, shared-handle assumptions, loom coverage, and independent-open behavior.
3. **Durability/crash consistency:** fsync/rename/manifest recovery, torn or corrupt manifests, orphan files, and platform assumptions.
4. **Index and data atomicity:** vector segment eligibility, row/index visibility, search correctness, stale/tombstoned rows, and segment fan-out.
5. **Performance:** manifest cloning/serialization, segment growth, scan/pruning behavior, memory residency, and benchmark evidence.
6. **Architecture/API:** layering, public escape hatches around the transaction boundary, schema ownership, API/documentation honesty, and future extensibility.
7. **Verification/documentation:** test quality, CI gates, ignored/opt-in suites, stale claims, and traceability from requirements to evidence.

Each lane will report: findings with severity, exact evidence locations, confidence, affected phase, and a recommended disposition. Findings will be classified as a documentation correction, a Phase 1 blocker, or a later-phase item.

## Cleanup rules

- Preserve ADR history; move superseded or reversed ADRs into `docs/history/decisions/`.
- Move superseded mechanism specifications and analyses into the corresponding history subfolder rather than deleting them.
- Delete only redundant generated files, retired tool configuration, or artifacts with no historical or operational value, after checking references.
- Update active documents to use current terminology and links.
- Keep a history index and status ledger so an agent can understand why a file moved.
- Do not modify source code unless a CI/configuration reference is demonstrably stale and the change is limited to that reference.

## Verification

The documentation pass will be verified by:

- checking all changed links and searching for retired `.opencode`, delta-log, mutable-graph, and over-broad transactional claims;
- confirming every archived file is indexed and every active ADR has an explicit status;
- running the repository’s normal formatting/tests only if the cleanup touches build/CI/configuration files;
- running the Phase 1 Sol audits after the status ledger and current architecture baseline are in place;
- reconciling audit findings before claiming the roadmap is complete.

## Risks and mitigations

- **Loss of discoverability:** preserve filenames/content in `docs/history/` and add indexes.
- **Overstating completion:** require evidence pointers and separate partial/proposed states.
- **Mixing implementation and documentation work:** keep this change documentation-only and list source findings as roadmap work.
- **Confusing historical phase numbers with current phases:** add an explicit mapping in the status ledger.
- **Parallel audit drift:** run independent read-only lanes against the same revision and reconcile centrally.
