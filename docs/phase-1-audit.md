# Phase 1 Sol audit

**Date:** 2026-08-01
**Scope:** embedded local-disk engine, one process, one shared `Dataset` handle.
**Verdict:** Phase 1 is Partial and blocked.

This is the active consolidated audit. It preserves the lane finding IDs so code comments, tests, and
future fixes can still refer to the original evidence without maintaining seven overlapping reports.
Line numbers are deliberately omitted because they are unstable; use the finding ID plus current
source/tests as the anchor.

## Finding register

| ID | Severity | Area | Disposition / required action |
|---|---|---|---|
| COR-01 / CONC-01 / IDX-01 | P0 | Future tombstones | Validate delete targets or define absent-target semantics so a tombstone cannot hide a later acknowledged insert. Add a regression test. |
| COR-02 / CONC-03 | P0 | Row-ID reuse | Persist or otherwise recover the allocator high-water mark so abandoned pre-publication claims cannot be reused after restart. |
| COR-03 / DUR-01 / DUR-02 | P0 | Directory durability | Make directory sync and dataset creation durability fail closed; test acknowledgement and recovery boundaries. |
| COR-04 / DUR-03a | P0 | Manifest identity | Require manifest filename/version and payload/version agreement during recovery. |
| DUR-03b | P0 | Corruption integrity | Define the corruption threat model and protect covered manifest/pruning metadata and row bytes with integrity checks or explicitly document excluded classes. |
| COR-05 / ARCH-02 | P0 | Update/delete contract | Define target existence, replacement cardinality, logical identity, and typed errors; reject unsupported shapes. |
| ARCH-01 | P0 | Schema ownership | Establish dataset-owned schema validation so positional casts cannot relabel or misinterpret acknowledged data. |
| ARCH-03 | P1 | Error semantics | Preserve `InsufficientHistory` instead of converting it into a misleading row-conflict payload. |
| CONC-02 / VER-02 | P1 | Loom gates | Make transaction and live-set-cache models visible CI gates with reproducible commands. |
| ARCH-05 | P1 | Facade boundary | Close invariant-bypassing public storage/index surfaces or explicitly disclaim them outside `Dataset` guarantees. |
| DUR-04 / VER-03 | P1 | Chaos/checkpoints | Prevent thorough durability and checkpoint suites from self-skipping while reporting success. |
| VER-01 | P1 | Regression coverage | Add direct tests for each known counterexample before declaring Phase 1 complete. |
| PERF-01..05 | P1 | Bounds/evidence | Capture current segmented measurements and define supported history, segment, recovery, and memory bounds. Compaction remains Phase 3. |
| IDX-04 / ARCH-04 / VER-07 | P1 | Claim accuracy | Correct decision and status language that treats limited recall experiments or broad snapshot/durability evidence as universal proof. |
| CONC-04 / DUR-08 | Later | Cross-process | Move independent opener and durable conditional publication work to Phase 4; do not expand Phase 1 scope. |
| PERF-02..05 / DUR-06 | Later | Lifecycle | Compaction, vacuum, orphan cleanup, bounded history, and index lifecycle belong to Phase 3; document current growth meanwhile. |
| ARCH-06..08 | Later | Client/backend surfaces | Decide subordinate-crate leakage, backend plumbing, and CLI version semantics during later API stabilization. |

## Evidence that must be preserved

- Manifest publication is the intended visibility boundary for row data and immutable vector segments
  inside the supported shared-handle scope.
- Immutable snapshots, defensive segment validation, tests, loom models, chaos tooling, fuzz targets,
  and benchmarks provide a useful correctness nucleus, but not a blanket proof.
- Historical recall, chaos, and performance results must state their workload, seed count, and old
  implementation baseline. In particular, the 2,000-seed chaos run and the DBpedia embedding recipe
  are bounded evidence, not general guarantees.

## Exit order

1. Reproduce and fix future tombstones, stale targets, update cardinality, manifest mismatch, and
   allocator restart reuse.
2. Define absent-row, schema, integrity, and supported-facade contracts.
3. Make durability fail closed and add recovery/corruption tests.
4. Gate loom, chaos, fuzz provenance, and known regressions in CI.
5. Capture current segmented performance and operating bounds.
6. Only then advance Phase 2 usability or Phase 3 lifecycle work.

Cross-process transactions, serializability, compaction, full SQL, branching, object storage, and
additional ANN families are not Phase 1 exit requirements.
