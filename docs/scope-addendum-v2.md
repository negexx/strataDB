# Strata — Scope Addendum v2

**Status:** Proposal. Supersedes v1.
**Changes from v1:** §1 gains zone maps (new, and it makes the layout decision more load-bearing). §3 gains a deferred learned-index note. §4 gains neural databases. §6 is new — a triage filter for evaluating future research, because more of it will arrive.
**Unchanged:** the branching thesis, the refusals, the memory positioning, and the next action.

> Filed 2026-07-24. Canonical scope statement (supersedes [`scope-addendum-v1.md`](scope-addendum-v1.md)).
> §1.1's decision is **made**: branching is mandatory → segmented layout adopted,
> [ADR 0008](decisions/0008-adopt-segmented-index-layout.md) (Accepted). §1.2 (zone maps) is a v1
> companion primitive unlocked by that layout. §8.2's experiment (recall-vs-segment-count) is
> implemented in `bench/benches/segment_recall_bench.rs`; its measured verdict is recorded in ADR 0008.

---

## TL;DR

One decision is time-sensitive and cheap. Everything else here can wait, and most of it should be refused.

| | Item | Phase | Cost |
|---|---|---|---|
| **DO NOW** | Segmented immutable index layout | v1 | ~1 week design; near-zero implementation delta |
| **DO NOW** | Per-segment zone maps | v1 | ~100 lines; unlocks temporal/filtered pruning |
| Later | Fork / abort | v2 | Large, but unlocked by the above |
| Later | Merge | v2.1 | Correct-but-slow is fine |
| Later | Staleness tracking | v3 | Small |
| Later | Verifiable deletion | v3 | Small |
| Later | Budget-shaped ANN API | v3 | Days |
| Read later | Learned indexes | v4+ | Research, not roadmap |
| **Refused** | Derivation engine, probe optimiser, belief semantics, neural DB, multi-tenancy fork | — | — |

---

## 1. The only decisions that can't be deferred

### 1.1 Segmented immutable index files, not a monolithic mutable HNSW

**The choice.** Either the vector index is one large mutable graph that inserts rewire in place, or it's a set of immutable segment files plus a manifest, with writes landing in a new delta segment and background compaction merging them.

**Why it's time-sensitive.** This is a storage layout decision. It is not retrofittable. Everything else in this document is additive; this one forecloses.

**Why segmented.**

- A single HNSW insert rewires up to `M` neighbour links across levels. The delta of a fork is therefore *not small*, and you'd be diffing a mutable graph — which is exactly why nobody has a forkable vector index today.
- With segments, fork is O(number of segments): copy a manifest, point writes at a new delta segment. This is the mechanism behind Lucene, Milvus, and Iceberg branching.
- It simplifies crash recovery, snapshot isolation, and compaction scheduling — all of which v1 needs anyway, independent of branching.
- It is the only layout that makes §1.2 and §3.2 possible.

**What it costs.** Queries fan out across N segments and merge. That is a real recall-per-millisecond penalty versus one large graph, and it must be managed with an aggressive compaction policy. Lucene and Milvus both pay this deliberately.

**Recommendation: adopt the layout in v1 and ship zero branching features in v1.**

The point is not to build branching now. The point is that the alternative decision makes branching permanently impossible, and branching is where the differentiated thesis lives (§2). The option is cheap; not having it is not.

### 1.2 Per-segment zone maps *(new in v2)*

**The problem.** Filtered ANN is one of the genuinely hard problems in vector search, and its failure mode is nasty: teams benchmark pure ANN, deploy, then discover filtered queries return 3 results instead of 10. Post-filtering on metadata after retrieval degrades badly under concurrency, and temporal predicates ("what did we know as of December") are the common case for any agent-memory workload.

**The cheap answer.** Segments are written in time order. Store per-segment min/max for a timestamp column (and any other low-cardinality filter column) in the manifest. A temporal predicate then becomes **segment pruning** — skip whole segments before touching a single vector.

This is Iceberg partition pruning and Lucene's segment-level metadata, applied to vector segments. Roughly a hundred lines of work.

**Why it belongs in v1, not v3.** It is nearly free *given* §1.1, and impossible without it. Monolithic HNSW gives you nowhere to hang the metadata. Deciding §1.1 and skipping §1.2 wastes the decision.

**Scope boundary.** This is a pruning primitive, not a temporal data model. Zone maps are storage. Validity intervals, confidence propagation, and retraction cascades are a data model — see §4.

---

## 2. Branching — the v2 thesis

### The gap

Every system shipping database branching today — Neon, Xata, Databricks Lakebase, Bauplan — branches **rows only**. Indexes rebuild on the branch. Tolerable for a developer sandbox, useless for an agent, because rebuilding the HNSW index is the expensive part.

Nobody has a forkable vector index. The reason is structural: nobody put the vector index inside the transactional engine. **Strata's core architectural decision is the precondition for the capability.** That's the whole argument — not scope creep, but the payoff for correctness work already planned.

### Why the demand is real, not speculative

- Berkeley's CIDR '26 paper ("Supporting Our AI Overlords") reports Neon production data: agents created **20× more branches and 50× more rollbacks** than humans.
- The same paper names the requirement *multi-world isolation* — thousands of near-identical forks, logically isolated but physically overlapping, with **fast aborts** rather than fast commits.
- A benchmark already exists (BranchBench, arXiv 2604.17180) with no engine that passes it. That is the signature of a real open systems problem: the operation is named, measured, and unserved.

### Shipping order

The workload is asymmetric — **fork-and-discard is the common case, merge is rare.** Build in that order:

1. **Fork** — O(segments) manifest copy.
2. **Abort** — O(mutations). Must be genuinely fast; this is the hot path, unlike in any traditional DB.
3. **Read isolation** — snapshot-consistent ANN across a branch's segment set.
4. **Merge** — correct but slow is acceptable for a year. Replay the branch's logical insert/delete set, rebuild affected segments. Do not optimise before someone complains.

### Positioning

From *"a vector store with better consistency guarantees"* — a benchmark argument you spend years defending against pgvector — to:

> **The storage engine an agent can fork.**

Sharper, structurally un-retrofittable by Postgres, and it has a benchmark with no incumbent.

---

## 3. Later

### 3.1 Small primitives (v3)

Each is a storage-layer fact, deliberately stopping short of the system that would consume it.

**Staleness tracking.** Store `(source_version, derivation_fn_version) → derived_value` and expose "which rows have stale derivations" as a query. Recompute nothing. Someone else's orchestrator owns the model calls; Strata owns knowing what's dirty.

**Verifiable deletion.** Every vector store tombstones and compacts lazily — the bytes survive. "Provably scrubbed from the index and all segments" is narrow, unglamorous, and about to be legally forced (GDPR / EU AI Act). Segmented layout makes it tractable; monolithic HNSW makes it nearly impossible.

**Budget-shaped ANN.** You already have the recall knob (`ef_search`). Expose `recall ≥ 0.9 OR cost ≤ X` instead of a raw integer. Days of work, and the honest sliver of cost-aware planning without pretending to be a query optimiser.

### 3.2 Learned indexes — read, don't build *(new in v2)*

Worth reading for the structural parallel, not for the roadmap.

Learned indexes treat a lookup as CDF regression rather than pointer-chasing. Their known weakness is exactly your workload: performance degrades under updates, because maintaining the CDF invariant forces global retraining that blocks queries.

Sig2Model (arXiv 2509.20781) is the current best attempt — sigmoid boosting for localised model adjustment, GMM-predicted placeholder pre-allocation in high-update regions, deferred full retraining. Reported: ~20× lower retraining cost, ~3× higher QPS, ~1000× less memory.

**Why it's relevant to you and still not on the roadmap:**

- The *shape* of Sig2Model's fix — pre-allocate for expected writes, adjust locally, defer the global rebuild — is the same idea as delta segments plus background compaction. Different index type, same insight. Useful confirmation that the §1.1 instinct generalises.
- But Strata is write-heavy by design, learned indexes are weakest under writes, and this is a 2025 preprint, not production-proven. **v4 at earliest.** Do not let an interesting paper become a work item.

---

## 4. Explicitly refused

Recorded so these don't get relitigated every time the space moves.

**Derivation engine (IVM over model calls).** Requires model invocation, GPU/API concurrency control, retries, budget scheduling. That's an orchestrator and a separate product. Ship the staleness primitive; stop.

**Probe optimiser / cost-quality planning.** Requires a query optimiser, which requires a query language. Strata is a storage engine. Category error at this stage.

**Belief semantics (validity intervals, confidence propagation, retraction cascades).** A *data model*. Build it **on** Strata, not **in** it. The moment the storage engine holds opinions about what a "claim" is, it stops being a storage engine. Note the boundary against §1.2: zone maps make temporal *filtering* fast, which is storage; deciding what a fact *means* over time is not.

**Neural databases (NeuroDB / SNH / NGDB).** *(new in v2)* Real research, badly oversold in most write-ups. It approximates aggregate range queries over a fixed data distribution — no exact answers, no arbitrary queries, and updates are the open problem. It's an approximate-query-processing technique, not a database class, and it is orthogonal to everything Strata does.

**Extreme multi-tenancy.** An architectural fork, not a feature. Escape hatch worth remembering: if Strata ships embeddable (Rust library, DuckDB/LanceDB-shaped) rather than server-first, million-tenant is close to free — run N instances. An independent argument for embeddable-first.

**Agent memory as a product.** See §5.

---

## 5. On "agent memory is the next database"

The demand claim is right. The layer claim is wrong, and the distinction decides what you build.

Every serious player — Zep/Graphiti, Mem0, Supermemory, Letta, Cognee — is a data model plus an extraction pipeline plus a retrieval policy on someone else's engine. MemoriesDB calls itself a database and runs on Postgres + pgvector. **Not one wrote a storage engine.**

Two consequences:

1. The differentiating logic is LLM-driven — extraction, contradiction detection, consolidation policy. When your moat is a prompt, your competitor is the company that makes the model, and every model provider ships native memory.
2. The published benchmarks (LoCoMo, LongMemEval, BEAM) measure *retrieval accuracy* — a pipeline-quality metric, not a storage metric. Nobody has demonstrated a wall that a new engine is required to break. Contrast branching, where the wall is measured and named.

**Correct relationship: memory is the reference application; Strata is the substrate.**

What a belief store actually needs from storage: write a new fact, invalidate the contradicted one, update the vector index, update the graph edges — *atomically, or the memory is silently corrupt.* Retrieve at a snapshot where the temporal graph and the vector index agree. Retract a source and cascade deletion consistently through every derived index.

That is a description of Strata. Every memory company today does this at the application layer with eventual consistency and hopes the races are rare — which is precisely why "the agent remembered something it shouldn't have" and "the agent forgot what I just told it" are the two dominant failure modes in the category.

So: **build the engine, then a thin memory layer on top as the demo.** You get the crowded market's attention and own the layer nobody else has. Enter memory directly and you're startup sixteen with worse distribution, competing where your advantage is smallest.

---

## 6. Triage filter for the next research dump *(new in v2)*

More surveys will arrive. Most will be literature reviews wearing strategy clothes. Three questions, in order — if any fails, the item is reading material, not a work item.

**1. Is there a measured wall?**
Can someone name an operation that is too slow or impossible today, with a number attached from a real workload? Branching passes: 20×/50× from Neon production, plus a benchmark nobody passes. Agent memory fails: the benchmarks measure pipeline quality, not storage limits. *A category being popular is not a wall.*

**2. Does it survive Postgres?**
Assume pgvector or an extension ships it in 18 months. If that kills the idea, it was a feature, not a thesis. Atomic multi-index commits survive (structural). Better filtered search does not.

**3. Are the numbers checkable?**
Cited, from a named system, internally coherent. Watch for precise-looking figures with no source — and for internal contradictions, which are the reliable tell. One recent survey claimed a system "outperforms Redis by a factor of 0.78," which describes a regression. When one number in a document is incoherent, none of them were verified.

Applied to the last survey received: the learned-index section passed all three (real paper, real numbers, useful parallel). The neural-database section passed 3 but failed 1 and 2. Everything cited without a source failed 3 outright. Net yield: one paragraph in §3.2 and one primitive in §1.2.

**That ratio is normal. Expect it, and stop reading when you hit it.**

---

## 7. Open questions

Flagged rather than papered over.

1. **Compaction policy under branch churn.** If thousands of short-lived branches each write a delta segment, when do you compact, and how do you avoid compacting segments a live branch still references? The hard part of §1.1, unsolved here.
2. **Recall degradation curve across segment count.** Needs measurement, not reasoning. The layout is only defensible if the penalty is bounded and compaction-recoverable.
3. **Whether fast abort is achievable** at the vector-index layer, or whether abort cost scales with segments-touched in a way that breaks the agentic case.
4. **Embeddable vs. server-first.** Affects §4's multi-tenancy escape hatch and probably the whole distribution strategy. Undecided.

Question 2 could invalidate §1.1. It's also cheap to test with a throwaway prototype long before the engine exists — build N segments over a known dataset, measure recall and latency versus one monolithic index at varying N. **That experiment is worth more than the next ten surveys.**

---

## 8. Next action

Exactly two things, both small:

> 1. Make the segmented-vs-monolithic call and write it into the MVP spec.
> 2. Run the segment-count recall experiment (question 2 above).

Everything else goes into `FUTURE.md` and is not reopened until there is a working commit path.

---

*Scope questions are cheaper than code and feel like progress. This document exists to close them, not to collect them. If a v3 of this file becomes necessary before the experiment in §7.2 is run, that is itself the signal.*

---

## Filing status (added when filed, not part of the proposal)

- **§8.1 (segmented decision):** done — [ADR 0008](decisions/0008-adopt-segmented-index-layout.md) Accepted (branching mandatory → segmented).
- **§8.2 (recall experiment):** done — `bench/benches/segment_recall_bench.rs`. Measured verdict recorded in ADR 0008. This is the experiment §7.2 calls "worth more than the next ten surveys"; it ran *before* a v3 was needed, which §closing says is the healthy signal.
- **§1.2 (zone maps):** recorded as a v1 companion "do now" in [`architecture.md`](architecture.md) and [`FUTURE.md`](FUTURE.md); it's a per-segment-manifest primitive the ADR 0008 layout must reserve room for. Not yet built.
- **§4 (neural databases):** added to the Non-Goals cut list in `architecture.md`.
