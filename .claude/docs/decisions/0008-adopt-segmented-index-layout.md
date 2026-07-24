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

## Measured result — the gating de-risk (Q2) has been run: GO

`bench/benches/segment_recall_bench.rs` ran the §7.2 experiment on the real dataset (20k × 512-dim,
Strata's production HNSW params M=16 / ef_construction=200 / ef_search=32, k=10, recall@10 vs exact
brute-force ground truth). K contiguous segments, fan-out search, merge top-k:

| K | recall@10 | µs/query | latency vs K=1 |
|---|---|---|---|
| 1 (monolithic, = today) | 0.974 | 274 | 1× |
| 2 | 0.975 | 682 | 2.5× |
| 8 | 0.990 | 1,901 | 6.9× |
| 32 | 0.996 | 4,967 | 18× |
| 64 | 0.998 | 9,390 | 34× |

**Verdict: recall is segment-count-safe; the cost is latency, ~linear in K.** Recall does not fall as
segments accumulate — it rises (0.974 → 0.998), because fan-out over-fetches k candidates per segment
and each small segment is searched near-exhaustively at fixed ef. That over-fetch *is* the latency
cost, which is why latency grows ~linearly.

**Why this is the good outcome for this decision.** The feared failure was recall collapsing with
segment count — which would make compaction *load-bearing for correctness*: a lagging compactor would
silently return worse answers. That did not happen. Instead compaction only needs to bound *latency*,
which it does by construction (fewer segments → less fan-out). A lagging compactor makes queries
slower, never wrong. Q2 does not invalidate the segmented layout.

**Honest caveats (don't over-read the recall rise):**
- The recall *increase* is an over-fetch artifact, not evidence that "more segments = better search." A
  latency-matched comparison (shrink ef per segment as K grows) would show recall roughly flat, not
  rising. The load-bearing claim is only "no recall cliff," which holds.
- 34× at K=64 is a real cost, but bounded in practice: compaction keeps K small (at K=2–8, latency is
  2.5–7× and absolute latency stays ~1–2 ms), and zone maps (addendum §1.2) prune most segments for
  filtered/temporal queries before any search.

**Design implications for the segment format / compaction policy:**
1. Compaction targets a **latency** SLA (bounded K), not a recall floor — a much easier constraint.
2. The search path should over-fetch per segment (it already helps recall here); a latency-budgeted
   variant (ef shrinking with K) is a follow-up if a query is latency-bound, not a v1 requirement.
3. Zone-map pruning (§1.2) is complementary and worth building alongside, since it attacks the same
   fan-out cost from the other side.

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
