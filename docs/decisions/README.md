# Decision index

This index identifies decisions that govern current work. Decision status is distinct from implementation status; consult [the status ledger](../status.md) before claiming a capability is shipped.

## Active decisions

| Decision | Status | Current effect |
|---|---|---|
| [ADR 0003 — Snapshot isolation, not full serializability](0003-snapshot-isolation-not-serializability.md) | Accepted policy; implementation Partial | Sets the intended isolation ceiling. It does not establish a full read/write snapshot-transaction API. |
| [ADR 0005 — Rust over C++ reversal](0005-rust-over-cpp-reversal.md) | Accepted | Rust is the project implementation language and loom is part of the concurrency-correctness approach. |
| [ADR 0008 — Adopt segmented index layout](0008-adopt-segmented-index-layout.md) | Accepted | The active index layout is immutable per-commit segments listed by the manifest; this does not ship branching or compaction. |

## Active proposals

| Decision | Status | Scope |
|---|---|---|
| [ADR 0006 — Group commit](0006-group-commit.md) | Proposed | A possible batching optimization; it does not change the current acknowledged-write contract. |

## Historical and superseded material

| Decision | Status | Current replacement |
|---|---|---|
| [ADR 0001 — Template](../history/decisions/0001-template.md) | Historical | [Decision index](README.md) |
| [ADR 0002 — C++ as the implementation language](../history/decisions/0002-cpp-over-rust.md) | Superseded | [ADR 0005](0005-rust-over-cpp-reversal.md) |
| [ADR 0004 — Toolchain audit](../history/decisions/0004-toolchain-audit-2026-07.md) | Superseded | [ADR 0005](0005-rust-over-cpp-reversal.md) |
| [ADR 0007 — Segmented vs. monolithic index](../history/decisions/0007-segmented-vs-monolithic-index-layout.md) | Superseded | [ADR 0008](0008-adopt-segmented-index-layout.md) |