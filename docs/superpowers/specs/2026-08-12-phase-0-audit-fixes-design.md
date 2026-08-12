# Phase 0 Audit Fixes Design

## Goal

Close the confirmed Phase 0 audit defects and evidence gaps without changing the on-disk format, the supported one-process/shared-`Dataset` concurrency boundary, or the public transaction model.

## Recommended approach

The PR will make compatibility-preserving corrections and bounded runtime improvements:

- Align CI, the declared MSRV, and the pinned local toolchain at Rust 1.97.1.
- Narrow recovery documentation to the guarantees the implementation actually verifies.
- Make lifecycle and chaos tests deterministic and exact-target based.
- Make required loom commands explicit and retain timeout evidence instead of treating normal Cargo discovery as proof.
- Add production-boundary fuzz targets for current manifest recovery and segment decoding.
- Add Windows restart evidence and longer-lived summarized provenance.
- Fix stale analysis links.
- Prevent `LiveSetCache` admission from exceeding its byte budget while preserving query results.
- Replace the global candidate sort in `SegmentSet::fan_out` with a bounded top-k selection that preserves nearest-wins duplicate semantics.

The PR will not redesign manifest history, introduce chunked row files, make recovery lazy, or add a fairness protocol. Those are format/operational decisions requiring workload budgets and new crash/migration evidence. The roadmap and performance documentation will record those gates explicitly.

## Invariants

- Existing manifests, row files, and index segments remain readable and writable.
- A cache optimization may skip retention, but must return the same computed `LiveSet`.
- Vector search returns the same global row IDs, nearest-first order, and nearest occurrence for duplicate row IDs.
- Lifecycle tests must establish that the pruning executor is queued before asserting admission ordering.
- Chaos ambiguity is calculated from exact in-flight target row IDs, not operation counts alone.
- No supported concurrency or durability claim is broadened beyond one process and local filesystem evidence.

## Verification strategy

Every runtime change follows TDD: add a regression test, observe the expected failure, implement the smallest fix, then run the targeted test and relevant loom model. CI and documentation changes receive config parsing, stale-reference, and focused smoke checks. The completed branch receives workspace build/test/fmt/clippy, targeted parallel-insert and loom checks, and a complete Sol review before publishing.

## Explicitly gated follow-up work

The audits identified, but this PR does not silently claim to solve:

- Manifest/history growth and serialized publication cost.
- Unbounded segment count and per-segment ANN work.
- Cold filtered vector full-file I/O.
- Full-payload recovery latency and memory.
- Lifecycle fairness under repeated exclusive executors.

Each remains documented with a measurable workload, latency/memory budget, and entry condition for a later design.
