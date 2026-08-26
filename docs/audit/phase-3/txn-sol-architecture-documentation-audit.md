# Strata-Txn Sol Architectural Documentation and Decision Record Audit

Date: 2026-08-15  
Scope: active architecture/design/status/roadmap/ADR documents, module
comments, public API documentation, links, and doc-test evidence  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 9 head `dcaa3da`; this remediation is the follow-up
implementation slice for the findings below.

## Verdict

**IMPLEMENTED within named bounds.** The active contract now records the
single-process/shared-`Dataset` boundary, indeterminate publication behavior,
lifecycle resource limits, supported public surfaces, and compatibility rules.
Historical evidence remains historical and is not rewritten.

## Findings

### [P1 — resolved] Indeterminate manifest publication is absent from active guidance

Locations:

- [`crates/txn/src/dataset.rs:2229`](../../../crates/txn/src/dataset.rs#L2229)
- [`crates/storage/src/manifest.rs:370`](../../../crates/storage/src/manifest.rs#L370)
- [`docs/architecture.md:5`](../../architecture.md:5)
- [`docs/architecture.md:105`](../../architecture.md:105)
- [`docs/status.md:72`](../../status.md:72)

The active documentation says Phase 1 blockers are remediated and describes
publication followed by snapshot installation, but does not document that a
manifest can become recoverable before directory synchronization returns an
error. It lacks retry, reopen, reconciliation, and stale shared-handle
guidance for this indeterminate outcome.

### [P1 — resolved] Lifecycle resource costs are missing from the active contract

Locations:

- [`crates/txn/src/dataset.rs:442`](../../../crates/txn/src/dataset.rs#L442)
- [`docs/status.md:40`](../../status.md:40)
- [`docs/roadmap.md:19`](../../roadmap.md:19)
- [`crates/txn/src/maintenance.rs:59`](../../../crates/txn/src/maintenance.rs#L59)

Compaction holds lifecycle exclusivity and `commit_lock` while rebuilding the
live dataset. Retained evidence reports approximately 79.49 seconds and 1.29
GB peak live memory for its bounded fixture. Row-ID reservation metadata also
has quadratic read growth and is excluded from lifecycle accounting. The active
lifecycle contract does not explain these stop-the-world and accounting
boundaries.

### [P2 — resolved] Active module documentation contains stale claims and links

Examples:

- [`crates/txn/src/lib.rs:1`](../../../crates/txn/src/lib.rs#L1) and
  [`crates/txn/src/dataset.rs:1`](../../../crates/txn/src/dataset.rs#L1) link to
  a nonexistent `docs/phase-1-audit.md`.
- [`crates/txn/src/lifecycle.rs:114`](../../../crates/txn/src/lifecycle.rs#L114)
  describes an implemented lifecycle report as future work.
- [`crates/index/src/segment_set.rs:25`](../../../crates/index/src/segment_set.rs#L25)
  says compaction does not exist.
- [`crates/storage/src/manifest.rs:355`](../../../crates/storage/src/manifest.rs#L355)
  still says Phase 1 has no conflict detection.
- Obsolete “Phase 6/spec §” labels remain in active comments.

Historical files under `docs/history/` are intentionally excluded from this
finding because they are clearly archival.

### [P2 — resolved] Active status/design records contradict current completion evidence

Locations:

- [`docs/status.md:72`](../../status.md:72)
- [`docs/status.md:79`](../../status.md:79)
- [`docs/design.md:68`](../../design.md:68)
- [`docs/designs/agent-production-readiness.md:3`](../../designs/agent-production-readiness.md:3)

These files still describe final branch verification as outstanding, important
suites as not CI-gated, or review fixes as in progress despite the current
implementation and recorded verification evidence.

### [P2 — resolved] The new focused audit records contain broken relative source links

The first four focused audit records use four parent traversals from
`docs/audit/phase-3`, but the repository root is three parents away. Some links
also encode `:line` in a way ordinary Markdown resolvers treat as part of the
filename. The affected records are:

- `txn-sol-correctness-static-hygiene.md`
- `txn-sol-performance-resource-audit.md`
- `txn-sol-coverage-mutation-audit.md`
- `txn-sol-concurrency-thread-safety-audit.md`

The focused records now use three parent traversals from
`docs/audit/phase-3`; line references remain informational source locations.

### [P3 — resolved] Public client documentation is thin

PyO3 has class summaries, but little method-level guidance for the stable
client surface. CLI module documentation describes legacy/crash-loop behavior
but not the stable administration surface listed in `docs/status.md`.

### [P3 — resolved] Decision ledger lacks follow-on rationale for accepted later surfaces

ADR 0009 explicitly did not create stable Python/admin contracts, but those
contracts, schema migration, and lifecycle authority are now implemented. No
accepted follow-on decision summarizes their compatibility and rollback
constraints. ADRs 0003, 0005, 0008, and proposed 0006 remain internally
coherent and do not need deletion as historical decisions.

## Positive alignment

- Single-process/shared-`Dataset` scope and rejection of serializability and
  cross-process guarantees are consistently documented.
- Immutable manifest-listed vector segments and row/index publication align with
  the inspected commit path.
- Vacuum, retention, and maintenance correctly limit deletion authority and
  describe `storage_bound_met` as a one-run observation.
- Schema evolution and ANN recall/performance claims are generally bounded.
- Historical documentation is clearly separated under `docs/history/`.

## Fresh validation

- `cargo doc --workspace --no-deps`: passed with no rustdoc warnings under the
  configured MSVC/Windows SDK environment.
- `cargo test --workspace --doc --no-default-features`: passed; 12 doctests
  passed. PyO3 `cdylib` doctests remain unsupported by Cargo and were skipped
  with Cargo's warning.
- `git diff --check`: passed.
- `lychee`, `markdownlint`, and `markdown-link-check` are unavailable.
- A repository-wide local Markdown validator was interrupted; no global
  link-clean claim is made.

Implementation files: `docs/designs/phase-3/audit5-architecture-documentation-remediation.md`
and `docs/plans/audit5-architecture-documentation-plan.md`. The remaining
limitations are explicit: documentation tooling not installed is not claimed
green, and the contract does not make universal durability, serializability,
or cross-process coordination claims.

