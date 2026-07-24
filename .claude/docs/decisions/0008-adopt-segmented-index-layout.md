# ADR 0008 — Adopt the segmented immutable index layout

**Status:** Accepted.
**Date:** 2026-07-24
**Supersedes:** [ADR 0007](0007-segmented-vs-monolithic-index-layout.md) (which framed this as an open decision).
**Source:** [Scope Addendum v1](../scope-addendum-v1.md) §1–2.

## Decision

**Branching is a mandatory capability for Strata** (product decision, 2026-07-24). Branching a vector
index is only possible if the index is stored as **immutable segment files plus a manifest**, because
forking a monolithic mutable graph means diffing a structure where one insert rewires up to `M` links
across levels. The monolithic layout forecloses branching permanently and is not retrofittable.

Therefore: **the vector index will use a segmented immutable layout** — writes land in a new delta
segment, queries fan out across segments and merge, background compaction merges segments. This is the
Lucene / Milvus / Iceberg mechanism, and it makes the index symmetric with how Strata already stores
row data (append-only files + manifest + atomic CAS).

## What this decides, and what it does NOT

**Decided:**
- The index storage layout is segmented-immutable, not monolithic-mutable. Any `crates/index` /
  `crates/storage` change that would harden the monolithic design against this migration is now a
  regression against a committed direction — check this ADR first.

**NOT decided here (deliberately, per addendum §1 "ship zero branching features in v1"):**
- **v1 still ships no branching.** Buying the layout option ≠ building fork/abort/merge. Those are v2
  (see [`FUTURE.md`](../FUTURE.md)), after the Phase 6 commit path and Phase 7 correctness harness are
  done. This ADR commits the *layout*, not the *features*.
- The concrete segment format, the compaction policy, and the fan-out/merge search path are **not**
  designed yet. They need their own design pass (brainstorming → plan → spec), not an inline sketch.

## The gating de-risk — must run before the segment format is finalized

Making branching mandatory does not remove the segmented layout's one real risk; it makes measuring it
urgent. From addendum §6:

- **Q2 — recall-vs-segment-count curve (the pivotal unknown).** Query fan-out across N segments plus a
  merge is a real recall-per-millisecond penalty versus one large graph. Whether that penalty is
  *bounded and compaction-recoverable* is unmeasured. **It is cheap to prototype against the existing
  `crates/index` graph** (partition the same vectors into K sub-graphs, search all K, merge, measure
  recall@10 and latency vs K) long before any engine change — and it should run first, because a bad
  curve dictates how aggressive compaction has to be, or in the worst case reopens the whole approach.
- **Q1 — compaction under branch churn** (when to compact; never compact a segment a live branch
  references) is the hard, still-unsolved part of the design that follows.
- **Q3 — whether fast abort scales** at the vector-index layer, not with segments-touched.

## Empirical support already in hand

`bench/benches/lifecycle_bench.rs` (25k rows × 512-dim) measured monolithic recovery — a full HNSW
rebuild from the delta log on every `Dataset::open` — at 36.3 s, ~as much as the 38.6 s original
ingest. The segmented layout replaces that rebuild with a manifest load, so it fixes the largest cost
the lifecycle benchmark found, independent of branching. That is a second, already-proven reason to
adopt it beyond the branching thesis.

## Consequences

- Positive: fork becomes O(segments); recovery stops being a full rebuild; the index reuses the
  manifest/CAS/snapshot machinery Strata already has for rows (removing the monolithic index's odd-one-out
  status — the delta-log-replay + soft-delete residue dance that produced this session's atomicity bugs);
  verifiable deletion (GDPR/EU AI Act, addendum §3) becomes tractable.
- Negative: query fan-out + merge recall/latency penalty (magnitude = Q2, unmeasured); a compaction
  policy Strata does not yet have (Q1, unsolved); more moving parts (segment format, merge-on-read,
  compaction scheduler); small-dataset overhead until first compaction.
- Neutral: v1's visible feature set is unchanged — this is a layout decision, and branching ships in v2.

## How to revisit

Accepted decisions are revisited only by a superseding ADR. The one result that could force that is
Q2's recall curve coming back unbounded/uncompactable — which is exactly why it runs before the format
is committed. If Q2 invalidates fan-out search, a superseding ADR records the measurement and the
alternative (e.g. a single active graph + immutable frozen segments hybrid).
