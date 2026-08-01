# ADR 0008 — Adopt the segmented immutable index layout

**Status:** Accepted
**Date:** 2026-07-24
**Supersedes:** [ADR 0007](../history/decisions/0007-segmented-vs-monolithic-index-layout.md), which recorded the open choice.
**Source:** [Scope Addendum v2](../scope-addendum-v2.md), sections 1–2.

## Decision

The vector index uses immutable segment files plus a manifest. A vector-carrying commit writes a new
segment; searches fan out across manifest-listed segments and merge candidates. This layout keeps
the index structurally aligned with append-only row files and preserves a future option for
branching, abort, merge, and compaction.

The current implementation is narrower than the original product decision: v1 has no branching or
compaction, and manifest publication is lock-serialized for one shared in-process `Dataset` handle.
It is not a storage-level conditional-CAS protocol for independent handles or processes.

## What this decides, and what it does not

Decided:

- `crates/index` and `crates/storage` use an immutable segmented layout rather than a shared mutable
  graph as the publication model.
- Segment metadata and row data become visible through the same manifest/snapshot transition within
  the supported shared-handle boundary.

Not decided here:

- v1 branching, fork, abort, merge, or branch-aware manifests;
- the concrete segment format, compaction policy, or fan-out search tuning; or
- cross-process publication and allocation semantics.

Those concerns are tracked in the active roadmap and the Phase 1 audit, not implied by this ADR.

## Gating experiment and current evidence

The original adoption experiment in `bench/benches/segment_recall_bench.rs` tested a 20k × 512-dim
workload with M=16, ef_construction=200, ef_search=32, and k=10. It observed no recall cliff as
segments increased and measured roughly segment-linear latency growth. The result supports the
layout decision for that workload; it is not a universal recall or operating-bound guarantee.

| Segments | Recall@10 | Query time | Relative latency |
|---:|---:|---:|---:|
| 1 | 0.974 | 274 µs | 1× |
| 2 | 0.975 | 682 µs | 2.5× |
| 8 | 0.990 | 1,901 µs | 6.9× |
| 32 | 0.996 | 4,967 µs | 18× |
| 64 | 0.998 | 9,390 µs | 34× |

The result is historical evidence, not proof that more segments improve search or that compaction
currently bounds latency. The Phase 1 performance audit requires retained, attributable measurements
of the current segmented implementation and an explicit operating bound. Compaction, lifecycle
management, and zone-map effectiveness remain future work.

## Empirical context

The historical lifecycle benchmark measured monolithic recovery as a full HNSW rebuild on open. The
segmented layout removes that rebuild by loading immutable segment images. Current recovery still
scales with retained manifests and segment bytes, so the improvement does not eliminate the need for
Phase 1 growth and recovery measurements.

## Consequences

- Positive: immutable segments make snapshot publication and future branching structurally simpler.
- Positive: recovery validates/loads segment images instead of rebuilding one mutable graph.
- Negative: each vector-carrying commit adds a segment, and search/recovery costs can grow without
  compaction or retention management.
- Boundary: the layout decision does not override the supported shared-handle concurrency boundary,
  the current write-only transaction API, or the Phase 1 audit's correctness/durability findings.

## How to revisit

Revisit through a new ADR if the project changes the segment layout, adopts compaction semantics, or
adds cross-process/branch-aware publication. Do not infer those features from this decision alone.
