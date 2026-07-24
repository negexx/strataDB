# FUTURE — deferred and refused scope

> Created 2026-07-24 per [Scope Addendum v1](scope-addendum-v1.md) §7. This is the parking lot: items
> that are **not** reopened until there is a working commit path (Phase 6) and, for the branching
> line, until [ADR 0007](decisions/0007-segmented-vs-monolithic-index-layout.md) is decided.
>
> Rationale is **not** duplicated here — each item links to where it's argued. The point of this file
> is a single place that answers "is X in scope?" with "no, and here's why, don't relitigate it."

## The one thing that is NOT deferred — DECIDED

- **Segmented immutable index layout — adopted** ([ADR 0008](decisions/0008-adopt-segmented-index-layout.md),
  Accepted). Branching is mandatory, and it's only possible on a segmented index. This decides the
  *layout* now (so index-storage work must not harden the monolithic design); the branching *features*
  below still wait for Phase 6/7. **Before the segment format is committed, run the recall-vs-segment
  prototype (§6 Q2 below)** — it's on the critical path and could dictate the compaction strategy.

## Deferred (post-Phase-6, in order)

| Item | Target | Precondition | Notes |
|---|---|---|---|
| Fork (branch) | v2 | segmented layout (ADR 0008 ✓) + Phase 6/7 done | O(segments) manifest copy. The differentiated thesis — "the storage engine an agent can fork." [Addendum §2](scope-addendum-v1.md) |
| Abort (fast discard) | v2 | Fork | O(mutations); the agentic *hot path*, unlike a traditional DB. |
| Branch read isolation | v2 | Fork | Snapshot-consistent ANN across a branch's segment set. |
| Merge | v2.1 | Fork + abort | Correct-but-slow is acceptable for ~a year. Replay the branch's logical insert/delete set, rebuild affected segments. **Do not optimise before someone complains.** |
| Staleness tracking on derived columns | v3 | — | Store `(source_version, derivation_fn_version) → derived_value`; expose "which rows are stale" as a query. **Do not recompute anything.** [Addendum §3](scope-addendum-v1.md) |
| Verifiable deletion | v3 | segmented layout | "Provably scrubbed from index + all segments." Narrow, and about to be legally forced (GDPR / EU AI Act). Tractable under segments, near-impossible under monolithic HNSW. |
| Budget-shaped ANN API | v3 | — | Expose `recall ≥ 0.9 OR cost ≤ X` over the existing `ef_search` knob. Days of work; the honest sliver of cost-aware planning, not a query optimiser. |

## Refused (recorded so the space moving doesn't reopen them)

Full rationale in [Addendum §4](scope-addendum-v1.md) and §5. These are also reflected in
[`architecture.md`](architecture.md)'s Non-Goals.

- **Derivation engine (IVM over model calls)** — that's an orchestrator + a separate product. Ship the
  staleness primitive; stop there.
- **Probe optimiser / cost-quality planning** — requires a query optimiser, which requires a query
  language. Strata is a storage engine. Category error at this stage.
- **Belief semantics (bi-temporal validity, confidence propagation, retraction cascades)** — a *data
  model*. Build it **on** Strata, not **in** it. The moment the engine holds opinions about what a
  "claim" is, it stops being a storage engine.
- **Extreme multi-tenancy (fork)** — an architectural fork, not a feature. Escape hatch: ship
  embeddable and million-tenant is ~free (run N instances) — an independent argument for
  embeddable-first (open question, [Addendum §6 Q4](scope-addendum-v1.md)).
- **Agent memory as a product** — memory is the *reference application*, Strata is the *substrate*.
  Build the engine, then a thin memory layer on top as the demo; don't become "startup sixteen" in a
  crowded market competing on the axis where the advantage is smallest. [Addendum §5](scope-addendum-v1.md)

## Open questions that gate the above

From [Addendum §6](scope-addendum-v1.md). Q2 is the one that could invalidate the whole segmented
direction and is cheap to prototype against the existing `crates/index` graph *before* any engine
work:

1. Compaction policy under branch churn (unsolved).
2. Recall degradation curve vs. segment count — **measure, don't reason.** The pivotal one.
3. Whether fast abort is achievable at the vector-index layer.
4. Embeddable vs. server-first (affects multi-tenancy + distribution).
