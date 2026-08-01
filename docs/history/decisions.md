# Historical decisions

These summaries preserve decisions that shaped Strata but no longer govern the active design.

## ADR 0001 - initial template

The first project template established the repository's intent and documentation convention. It was
superseded as the storage, transaction, and index design became concrete.

## ADR 0002 - C++ over Rust

The early C++ direction was later reversed by ADR 0005. Rust/Cargo is now the workspace foundation;
the old toolchain rationale is retained only to explain the transition.

## ADR 0004 - toolchain audit

The July 2026 audit compared the original toolchain assumptions and documented the reasons for moving
to Rust, Arrow-rs, PyO3, loom, and process-level chaos tooling. ADR 0005 is the current decision.

## ADR 0007 - segmented versus monolithic index

The earlier comparison evaluated a monolithic mutable HNSW against immutable per-commit segments. ADR
0008 accepted the segmented layout. The old document's branching, compaction, and CAS discussion is
context only; none of those capabilities is implied by the accepted layout.

## Reading rule

When an old decision conflicts with `docs/decisions.md`, `docs/design.md`, or current code, the current
documents and implementation win. A design change should add a new dated decision rather than rewrite
these summaries.
