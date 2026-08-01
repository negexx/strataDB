# Documentation history

History is preserved for rationale and traceability, but is non-authoritative. When a historical document conflicts with active documentation, current source, tests, or accepted current ADRs, use the active material.

Reserved archive locations:

- [decisions/](decisions/) — superseded, reversed, or otherwise historical ADRs.
- [design/](design/) — retired mechanism specifications and implementation plans.
- [analysis/](analysis/) — investigations whose conclusions describe an older baseline.

## Archive index

### Decisions

| Document | Status | Why it moved | Current replacement |
|---|---|---|---|
| [ADR 0001 — Template](decisions/0001-template.md) | Historical | Replaced by the active decision index and no longer a decision record. | [Decision index](../decisions/README.md) |
| [ADR 0002 — C++ as the implementation language](decisions/0002-cpp-over-rust.md) | Superseded | The Rust reversal replaced the C++ language choice. | [ADR 0005](../decisions/0005-rust-over-cpp-reversal.md) |
| [ADR 0004 — Toolchain audit](decisions/0004-toolchain-audit-2026-07.md) | Superseded | It documents the retired C++ toolchain. | [ADR 0005](../decisions/0005-rust-over-cpp-reversal.md) |
| [ADR 0007 — Segmented vs. monolithic index](decisions/0007-segmented-vs-monolithic-index-layout.md) | Superseded | It framed the decision later made by ADR 0008. | [ADR 0008](../decisions/0008-adopt-segmented-index-layout.md) |

### Design

| Document | Status | Why it moved | Current replacement |
|---|---|---|---|
| [Phase 2 implementation plan](design/phase-2-implementation-plan.md) | Historical | Completed legacy implementation plan. | [Phase 2 encodings/group-by spec](../design/phase-2-encodings-and-groupby-spec.md) |
| [Phase 3 implementation plan](design/phase-3-implementation-plan.md) | Historical | Completed legacy implementation plan. | [Phase 3 query-refinement spec](../design/phase-3-query-refinement-spec.md) |
| [Phase 4 implementation plan](design/phase-4-implementation-plan.md) | Superseded | It targets the retired mutable-index/delta-log mechanism. | [Phase S1 segmented-index spec](../design/phase-s1-segmented-index-spec.md) |
| [Phase 4 vector-index spec](design/phase-4-vector-index-spec.md) | Superseded | It defines the retired mutable-index/delta-log mechanism. | [Phase S1 segmented-index spec](../design/phase-s1-segmented-index-spec.md) |
| [Phase 5 MVCC spec](design/phase-5-mvcc-snapshot-isolation-spec.md) | Historical | Its legacy phase numbering is mapped into the current capability model. | [Status ledger](../status.md) |
| [Scope Addendum v1](design/scope-addendum-v1.md) | Superseded | Its proposed segmented-layout decision was resolved by ADR 0008 and expanded by v2. | [Scope Addendum v2](../scope-addendum-v2.md) and [ADR 0008](../decisions/0008-adopt-segmented-index-layout.md) |

### Analysis

| Document | Status | Why it moved | Current replacement |
|---|---|---|---|
| [2026-07-23 complexity audit](analysis/2026-07-23-complexity-audit.md) | Historical | Static analysis of an older code baseline. | [Status ledger](../status.md) |
| [2026-07-23 OCC proposal review](analysis/2026-07-23-occ-proposal-review.md) | Historical | Review of an older transaction proposal and baseline. | [Status ledger](../status.md) |
| [2026-07-24 ingest/recovery performance audit](analysis/2026-07-24-ingest-recovery-performance-audit.md) | Historical | Performance snapshot for the retired index architecture. | [Status ledger](../status.md) |
| [2026-07-25 filtered-vector-search memory audit](analysis/2026-07-25-filtered-vector-search-memory-audit.md) | Historical | Performance snapshot for an older code baseline. | [Status ledger](../status.md) |
| [2026-07-26 full-pipeline performance audit](analysis/2026-07-26-full-pipeline-performance-audit.md) | Historical | Investigation of a pre-merge S1 branch baseline. | [Status ledger](../status.md) |