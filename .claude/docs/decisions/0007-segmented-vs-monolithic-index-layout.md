# ADR 0007 — Segmented immutable index layout vs. monolithic mutable HNSW

**Status:** Proposed — needs a human decision. Not implemented.
**Date:** 2026-07-24
**Source:** [Scope Addendum v1](../scope-addendum-v1.md) §1. Read it for the full argument; this ADR
is the tracked decision point.

## Context

Strata's v1 vector index is a **monolithic mutable HNSW**: one shared `HnswIndex` whose inserts
rewire neighbour links in place, rebuilt in full on `Dataset::open` by replaying the delta log
(`crates/txn/src/dataset.rs::replay_index`, `crates/index/src/graph.rs`). This is the currently
shipped design and everything in Phases 1–7 is built on it.

The addendum argues this is the **one architectural choice on its list that is not retrofittable.**
The alternative is a set of **immutable index segment files plus a manifest**: writes land in a new
delta segment, queries fan out across segments and merge, and background compaction merges segments.
This is the Lucene / Milvus / Iceberg layout.

**Why it can't be deferred:** every other addendum item is additive and can be built later; the index
storage layout forecloses. Monolithic makes a forkable vector index (the v2 thesis, addendum §2)
permanently impossible, because forking a mutable graph means diffing a structure where one insert
touches up to `M` links across levels. Segmented makes fork O(number of segments) — copy a manifest,
point writes at a new delta segment.

**Empirical footnote (measured, `bench/benches/lifecycle_bench.rs`, 25k rows × 512-dim):** recovery
via full graph rebuild costs 36.3 s — almost as much as the original 38.6 s ingest. That is direct
support for the addendum's claim that crash recovery and compaction are dramatically simpler under a
segmented layout, and it is the cost the monolithic design pays on every process start today.

## Decision

**Not yet made.** The addendum's recommendation is: **adopt the segmented immutable layout in v1 and
ship zero branching features in v1** — buy the option (cheap: ~1 week of design, near-zero
implementation delta at this stage) without building on it yet.

This ADR exists to force the decision to be made explicitly and recorded, per Design Principle #5
("cut ruthlessly, document the cut") and #7 ("read the reference before rebuilding the wheel").

## Alternatives considered

- **Keep monolithic mutable HNSW (status quo).** Simplest, already shipped, and adequate for every
  Phase 1–7 exit criterion. Rejected *as the long-term layout* only if branching (addendum §2) is a
  real goal — it makes that capability impossible, not merely harder. If branching is declined, this
  is the correct choice and this ADR should be closed as "monolithic retained."
- **Adopt segmented in v1.** Buys the branching option and simplifies recovery/compaction/snapshot
  isolation, at the cost of query fan-out across N segments (a real recall-per-millisecond penalty
  that must be held in check by an aggressive compaction policy). The addendum's recommendation.
- **Retrofit segmented later.** The addendum's central claim is that this is not viable — the layout
  is load-bearing for too much downstream (recovery, snapshot sets, fork). Listed only to name it as
  rejected.

## Consequences

- Positive (segmented): fork/abort (addendum §2) becomes possible; recovery stops being a full
  rebuild; verifiable deletion (§3) becomes tractable; compaction scheduling is explicit.
- Negative (segmented): every query fans out across segments and merges — a measurable recall/latency
  cost, and a compaction policy Strata does not yet have (addendum §6 Q1, unsolved).
- Neutral: v1 ships no branching either way. The decision is purely about *layout*, so the visible v1
  feature set is unchanged.

## Open questions blocking the decision (addendum §6)

1. **Compaction policy under branch churn** — when to compact, how to avoid compacting segments a
   live branch references. Unsolved.
2. **Recall degradation curve across segment count** — needs measurement, not reasoning. *This is the
   one that could invalidate the whole segmented direction*, and it is cheap to prototype against the
   existing `crates/index` graph long before any engine change. **Prototype this before deciding.**
3. **Whether fast abort is achievable** at the vector-index layer, or whether abort cost scales with
   segments-touched in a way that breaks the agentic use case.

## How to revisit

ADRs are immutable once committed. When the decision is made, supersede this with a new ADR recording
the choice and the measurement (esp. Q2's recall-vs-segments curve) that justified it. Until then,
`crates/index`/`crates/storage` index-storage work proceeds on the monolithic design, and any change
that would make a later segmented migration *harder* should reference this ADR first.
