# ADR 0002 — C++ as the implementation language

**Status:** Accepted
**Date:** 2026-07-15

## Context

Strata's spec deliberately left the implementation language unstated. The engine is a correctness-critical, concurrent, embedded storage engine (columnar + HNSW vector index + a custom OCC/snapshot-isolation transaction layer), built solo, targeting Python bindings as the practical client surface, with a ~2.2 year core roadmap.

## Decision

Use modern C++ (C++20/23), with CMake+Ninja, vcpkg (manifest mode), GoogleTest, clang-tidy/clang-format, Apache Arrow C++ for the columnar representation, hnswlib for HNSW, and nanobind for Python bindings.

## Alternatives considered

- **Rust (research recommendation):** borrow-checker gives compile-time data-race freedom, and purpose-built deterministic-simulation-testing tooling already exists (`loom`, `madsim`/`turmoil`) — directly relevant to Phase 7. `arrow-rs`+`PyO3`+`maturin` is a mature path. Rejected in favor of C++ by explicit project-owner choice, not a technical deficiency — see Consequences below for what that trade actually costs.
- **Zig:** TigerBeetle proves DST is achievable in Zig for exactly this kind of engine, but only via a fully bespoke simulator (VOPR) built over years by a funded team — not realistic solo. No borrow checker; Arrow/HNSW ecosystem essentially nonexistent natively. Rejected.

## Consequences

- Positive: most mature ecosystem for this exact domain — Arrow C++ is the reference Arrow implementation, hnswlib is the reference HNSW library; `nanobind` is a fast, modern Python-binding path.
- Negative: no compile-time memory-safety guarantee for concurrent code — ASan/UBSan/TSan are runtime nets, not compile-time proofs, so a data race can still ship if a code path isn't exercised under TSan. **No reusable deterministic-simulation-testing library exists for C++** (Rust's `madsim`/`diviner` have no C++ equivalent; FoundationDB's own Flow/Simulation is tightly coupled to FDB internals) — Phase 7's chaos harness must be built from scratch, FoundationDB/TigerBeetle-VOPR style. This is the single largest scope cost of choosing C++ over Rust for this specific project.
- Neutral: `vcpkg` has ready ports for every library this project needs (Arrow, hnswlib, GoogleTest, nanobind), so dependency management is not itself a cost.

## How to revisit

If Phase 6/7's from-scratch DST harness turns out to be a bigger time sink than the rest of the concurrency work combined, that's the signal to reopen this decision — not sunk-cost through it. Write a new ADR; don't edit this one.
