# ADR 0005 — Reversal: Rust, not C++

**Status:** Accepted — supersedes ADR 0002 (C++ as the implementation language) and ADR 0004
(the 2026-07 toolchain audit).
**Date:** 2026-07-16

## Context

ADR 0002 selected C++ despite identifying deterministic concurrency testing as a major project
cost. The C++ toolchain was then scaffolded with CMake/Ninja, vcpkg, GoogleTest, static-analysis
tools, Arrow C++, usearch, and nanobind. Before engine behavior was implemented, the project owner
reconsidered the choice. Rust already had the required ownership model and mature interleaving-test
tooling, so switching back was still inexpensive.

## Decision

Use Rust edition 2024 and a Cargo workspace. The current stack is:

- Arrow-rs for columnar storage;
- a from-scratch HNSW implementation with immutable segment encoding;
- PyO3/maturin for the eventual Python surface;
- `loom` for selected lock/atomic interleavings; and
- process-based simulation/chaos tests for crash and recovery evidence.

This ADR chooses the implementation language and toolchain. It does not imply that every planned
client API, recovery guarantee, or verification suite is already complete; consult the active status
ledger and roadmap.

## Alternatives considered

- **Stay on C++:** rejected because the toolchain and deterministic-concurrency-testing gap imposed
  more project cost without a compensating product advantage.
- **Use a Rust binding over a C++ index core:** rejected because it would reintroduce the C++ build
  and FFI complexity the reversal was intended to remove.
- **Make `cargo-nextest` mandatory:** deferred; ordinary `cargo test` is the current supported runner.

## Consequences

- Positive: safe Rust, Cargo, and loom provide a simpler foundation for concurrency-sensitive code.
- Positive: the project avoids a second native toolchain and can use Rust's ownership checks in the
  transaction, storage, and index layers.
- Negative: the earlier C++ workspace and its in-progress dependency build were discarded.
- Neutral: ADR 0002 and ADR 0004 remain preserved under [`docs/history/decisions`](../history/decisions/)
  as historical context; they do not govern the current stack.

## How to revisit

Revisit only with a new ADR after an explicit engineering review. A future language reversal should
not be started by editing this record or by reviving the historical OpenCode/C++ configuration.
