# Strata — Scope Addendum v1

> **SUPERSEDED by [`scope-addendum-v2.md`](scope-addendum-v2.md)** (2026-07-24). v2 adds zone maps
> (§1.2), a learned-index note (§3.2), neural databases to the refused list (§4), and a research
> triage filter (§6); the branching thesis, refusals, and memory positioning are unchanged. Read v2.
> This file is kept for history.

**Status:** Proposal. Nothing here is decided until you decide it.
**Context:** Written after a landscape review of open database gaps (mid-2026). This does not replace the existing MVP spec — it amends it in exactly one place and defers everything else.

> Filed into the project docs 2026-07-24. This page is the canonical statement of the proposal.
> **UPDATE 2026-07-24: §1's decision is made.** Branching was declared mandatory, so the segmented
> immutable index layout is adopted — [ADR 0008](decisions/0008-adopt-segmented-index-layout.md)
> (Accepted), superseding [ADR 0007](decisions/0007-segmented-vs-monolithic-index-layout.md). The
> §2 branching thesis is now a committed post-Phase-6 direction, not a proposal. §3–§5 stances are
> unchanged. Deferred/refused items are indexed in [`FUTURE.md`](FUTURE.md); the roadmap and Non-Goals
> in [`architecture.md`](architecture.md) point here rather than duplicating rationale.

---

## TL;DR

One decision is time-sensitive and cheap. Everything else on this page can wait, and most of it should be refused.

| | Item | Phase | Cost |
|---|---|---|---|
| **DO NOW** | Segmented immutable index layout | v1 | ~1 week of design; near-zero implementation delta |
| Later | Fork / abort (branching) | v2 | Large, but unlocked by the above |
| Later | Merge | v2.1 | Correct-but-slow is fine |
| Later | Staleness tracking on derived columns | v3 | Small |
| Later | Verifiable deletion | v3 | Small |
| Later | Budget-shaped ANN API | v3 | Days |
| **Refused** | Derivation engine, probe optimizer, belief semantics, multi-tenancy fork | — | — |

---

## 1. The only decision that can't be deferred

### Segmented immutable index files, not a monolithic mutable HNSW

**The choice.** Either the vector index is one big mutable graph that inserts rewire in place, or it's a set of immutable segment files plus a manifest, with writes landing in a new delta segment and compaction merging segments in the background.

**Why it's time-sensitive.** This is a storage layout decision. It is not retrofittable. Every other item on this page is additive; this one forecloses.

**Why segmented.**

- A single HNSW insert rewires up to `M` neighbour links across multiple levels. The delta of a fork is therefore *not small*, and you'd be diffing a mutable graph — which is why nobody has a forkable vector index today.
- With segments, fork is O(number of segments): copy a manifest, point writes at a new delta segment. This is the mechanism behind Lucene, Milvus, and Iceberg branching.
- It also makes crash recovery, snapshot isolation, and compaction scheduling dramatically simpler — all things v1 needs *anyway*, independent of branching.

**What it costs.** Query fans out across N segments and merges results. That is a real recall-per-millisecond penalty versus one large graph, and it must be managed with an aggressive compaction policy. Lucene and Milvus both pay this cost deliberately.

**Recommendation: adopt the layout in v1 and ship zero branching features in v1.**

The point is not to build branching now. The point is that the alternative decision makes branching permanently impossible, and branching is where the differentiated thesis lives (§2). Buying the option is cheap; the option is not.

---

## 2. Branching — the v2 thesis

### The gap

Every system shipping database branching today — Neon, Xata, Databricks Lakebase, Bauplan — branches **rows only**. Indexes are rebuilt on the branch. That is tolerable for a developer sandbox and useless for an agent, because rebuilding an HNSW index is the expensive part.

Nobody has a forkable vector index. The reason is structural: nobody put the vector index inside the transactional engine. **Strata's core architectural decision is the precondition for the capability.** That's the whole argument — it isn't scope creep, it's the payoff for the correctness work already planned.

### Why the demand is real, not speculative

- Berkeley's CIDR '26 paper ("Supporting Our AI Overlords") reports Neon production data: agents created **20× more branches and 50× more rollbacks** than humans.
- The same paper names the requirement *multi-world isolation* — thousands of near-identical forks, logically isolated but physically overlapping, with **fast aborts** rather than fast commits.
- A benchmark already exists (BranchBench, arXiv 2604.17180) with no engine that passes it. That is the signature of a genuinely open systems problem: the operation is named, measured, and unserved.

### Shipping order

The agentic workload is asymmetric — **fork-and-discard is the common case, merge is rare.** Build in that order:

1. **Fork** — O(segments) manifest copy.
2. **Abort** — O(mutations). Must be genuinely fast; this is the hot path, unlike in any traditional DB.
3. **Read isolation** — snapshot-consistent ANN across a branch's segment set.
4. **Merge** — correct but slow is acceptable for a year. Replay the branch's logical insert/delete set and rebuild affected segments. Do not optimise this before someone complains.

### What this does to positioning

From *"a vector store with better consistency guarantees"* — a benchmark argument you spend years defending against pgvector — to:

> **The storage engine an agent can fork.**

Sharper, structurally un-retrofittable by Postgres, and it has a benchmark with no incumbent.

---

## 3. Small primitives worth taking (v3, not before)

Each is a storage-layer fact, deliberately stopping short of the system that would consume it.

**Staleness tracking.** Store `(source_version, derivation_fn_version) → derived_value` and expose "which rows have stale derivations" as a query. Do not recompute anything. Somebody else's orchestrator owns the model calls; Strata owns knowing what's dirty.

**Verifiable deletion.** Every vector store tombstones and compacts lazily — the bytes survive. "Provably scrubbed from the index and all segments" is narrow, unglamorous, and about to be legally forced (GDPR / EU AI Act). Segmented layout makes this tractable; monolithic HNSW makes it nearly impossible.

**Budget-shaped ANN.** You already have the recall knob (`ef_search`). Expose `recall ≥ 0.9 OR cost ≤ X` instead of a raw integer. Days of work, and it's the honest sliver of cost-aware planning without pretending to be a query optimiser.

---

## 4. Explicitly refused

Recorded so the decision doesn't get relitigated every time the space moves.

**Derivation engine (IVM over model calls).** Requires model invocation, GPU/API concurrency control, retries, and budget scheduling. That's an orchestrator and a separate product. Ship the staleness primitive; stop there.

**Probe optimiser / cost-quality planning.** Requires a query optimiser, which requires a query language. Strata is a storage engine. Category error at this stage.

**Belief semantics (bi-temporal validity, confidence propagation, retraction cascades).** This is a *data model*. Build it **on** Strata, not **in** it. The moment the storage engine holds opinions about what a "claim" is, it stops being a storage engine.

**Extreme multi-tenancy.** An architectural fork, not a feature. Note the escape hatch: if Strata ships embeddable (Rust library, DuckDB/LanceDB-shaped) rather than server-first, million-tenant is close to free — run N instances. That's an independent argument for embeddable-first.

**Agent memory as a product.** See §5.

---

## 5. On "agent memory is the next database"

The demand claim is right. The layer claim is wrong, and the distinction decides what you build.

Every serious player in the category — Zep/Graphiti, Mem0, Supermemory, Letta, MemoriesDB — is a data model plus an extraction pipeline plus a retrieval policy sitting on someone else's engine. MemoriesDB calls itself a database and runs on Postgres + pgvector. **Not one of them wrote a storage engine.**

Two consequences:

1. The differentiating logic is LLM-driven (extraction, contradiction detection, consolidation policy). When your moat is a prompt, your competitor is the company that makes the model — and every model provider ships native memory.
2. The published benchmarks (LoCoMo, LongMemEval, BEAM) measure *retrieval accuracy*, which is a pipeline-quality metric, not a storage metric. Nobody has demonstrated a wall that a new engine is required to break. Contrast branching, where the wall is measured and named.

**The correct relationship: memory is the reference application; Strata is the substrate.**

Ask what a belief store actually needs from storage: write a new fact, invalidate the contradicted one, update the vector index, update the graph edges — *atomically, or the memory is silently corrupt*. Retrieve at a snapshot where the temporal graph and the vector index agree. Retract a source and cascade deletion consistently through every derived index.

That is a description of Strata. Every memory company today does this at the application layer with eventual consistency and hopes the races are rare — which is precisely why "the agent remembered something it shouldn't have" and "the agent forgot what I just told it" are the two dominant failure modes in the category.

So: **build the engine, then a thin memory layer on top as the demo.** You get the crowded market's attention and own the layer nobody else has. Enter memory directly and you're startup sixteen with worse distribution, competing on the axis where your advantage is smallest.

---

## 6. Open questions I don't have answers to

Flagged honestly rather than papered over.

1. **Compaction policy under branch churn.** If thousands of short-lived branches each write a delta segment, when do you compact, and how do you avoid compacting segments a live branch still references? This is the hard part of §1 and it's unsolved here.
2. **Recall degradation curve across segment count.** Needs measurement, not reasoning. The segmented layout is only defensible if the penalty is bounded and compaction-recoverable.
3. **Whether fast abort is actually achievable** at the vector-index layer, or whether abort cost scales with segments-touched in a way that breaks the agentic use case.
4. **Embeddable vs. server-first.** Affects §4's multi-tenancy escape hatch and probably the whole distribution strategy. Not yet decided.

Question 2 is the one that could invalidate §1. It's also cheap to test with a prototype long before the engine exists.

---

## 7. Next action

Exactly one thing:

> Make the segmented-vs-monolithic layout decision, and write it into the MVP spec.

Everything else in this document goes into `FUTURE.md` and is not reopened until there is a working commit path.

---

*The recurring failure mode on this project is that scope questions arrive before code. Expanding the thesis is the most comfortable available form of not starting. This document exists to close the question, not to open it.*

---

## Grounding note (added when filed, not part of the original proposal)

The current v1 vector index **is** the "monolithic mutable graph" this addendum argues against: a
single shared `HnswIndex` mutated in place, rebuilt in full on `Dataset::open` by replaying the
delta log (`crates/txn/src/dataset.rs::replay_index`, `crates/index/src/graph.rs`). The
`lifecycle_bench` run at 25k rows measured that rebuild directly — recovery (36.3 s) costs almost as
much as the original ingest (38.6 s), because both build the whole graph. That is empirical support
for §1's "crash recovery is dramatically simpler with segments" claim and for §6 Q2 being the
measurable pivot: the segmented layout's recall-vs-segment-count penalty is the number that decides
whether the trade is worth it, and it is cheap to prototype against the existing graph before any
engine work. It does **not** decide the question — see ADR 0007.
