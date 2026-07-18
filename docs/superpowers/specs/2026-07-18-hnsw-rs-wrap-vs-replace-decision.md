# Wrap/extend `hnsw_rs` vs. full replacement — decision memo

**Date:** 2026-07-18
**Trigger:** `llm-council`'s recommended "one thing to do first" before starting a from-scratch lock-free HNSW rewrite (`.superpowers/council/council-transcript-20260718-140939.md`) — write down, convincingly, why extending/wrapping `hnsw_rs` is insufficient, directly confronting `.claude/docs/decisions/0005-rust-over-cpp-reversal.md`'s stated rationale for choosing `hnsw_rs` in the first place.

**Method:** this memo evaluates each of the four requirements independently against wrap/extend, rather than treating "replace `hnsw_rs`" as one bundled decision. That's deliberate — the council's own finding was that this proposal is 3-4 stacked problems presented as one ticket, and the same error (treating a bundle as one yes/no) would corrupt this analysis too if repeated here.

## The four requirements, evaluated independently

### 1. Generic distance metrics (L2, cosine, inner product)

**Verdict: does not require replacement. Already available today.**

Strata's own code imports `hnsw_rs::prelude::{DistL2, FilterT, Hnsw}` and instantiates `Hnsw<'static, f32, DistL2>` (`crates/index/src/hnsw.rs`) — the distance type is already a generic parameter of `hnsw_rs`'s own `Hnsw` struct, not a hardcoded choice. `hnsw_rs` is built on the `anndists` crate, which defines multiple `Distance` implementations beyond `DistL2` (cosine and dot-product variants are standard `anndists` offerings). Supporting cosine/inner-product in Strata today is very likely a matter of instantiating `Hnsw` with a different `anndists::Distance` type and threading that choice through `HnswIndex::new`'s signature — no fork, no rewrite, arguably not even a large change to the existing wrapper.

This requirement should be dropped from the case for replacement entirely. If it's still wanted, it's a small, independent task against the *existing* dependency.

### 2. Match or beat `hnsw_rs`'s own recall@k and QPS

**Verdict: does not require replacement.**

`hnsw_rs`'s own tunable parameters (`max_nb_connection`/M, `ef_construction`, `ef_search`) are exposed today and are, per Strata's own code comments, deliberately left at "small, correctness-only values for now" — `HNSW_MAX_NB_CONNECTION=16`, `HNSW_EF_CONSTRUCTION=200` — pending the benchmark pass `.claude/rules/vector-index.md` already calls for ("tuned via benchmarks, not guessed"). "Beat `hnsw_rs`" as a goal is close to self-contradictory when the comparison baseline is the untuned version of the same library Strata already depends on — the honest, available move is to run that benchmark pass first and see where the existing dependency actually lands before concluding it needs replacing to hit a performance bar.

### 3. Fully lock-free concurrent mutation (atomics + epoch-based reclamation)

**Verdict: requires replacement, IF this stays a hard requirement.**

This is the one requirement where wrap/extend is genuinely not viable, and not narrowly — `hnsw_rs`'s concurrency model (an internal lock, per its own `&self`-safe `insert`) is not a surface detail bolted onto an otherwise-agnostic core; it's load-bearing for how the crate's internal mutable state is structured. Retrofitting lock-freedom onto an existing lock-based data structure generally means redesigning its internal memory layout from the ground up — at that point "patch the existing crate" and "write a new one" converge on nearly the same amount of work, except patching also inherits an unfamiliar codebase's existing assumptions and loses the ability to design the layout around lock-freedom from the start. If this requirement survives, full replacement is the honest, not merely convenient, answer.

**But:** per the council's own finding, this requirement is a self-imposed stretch goal, not something the project's actual stated success bar (match/beat `hnsw_rs`) requires — `hnsw_rs` itself is lock-based and clears its own bar trivially. This requirement's necessity is a choice, not a constraint.

### 4. Native soft-delete — excluding tombstoned nodes from traversal *routing*, not just results

**Verdict: requires more than a thin wrapper, but not necessarily a full replacement.**

This is the requirement that most directly motivated "replace, don't wrap." It's worth being precise about why a thin external wrapper can't do this: Strata's existing `is_visible` closure is composed into `hnsw_rs`'s own `FilterT` predicate and does affect which nodes make it into search *results* — but it cannot change which nodes the algorithm's internal greedy traversal steps *through* on the way to those results, because that traversal loop is entirely private to `hnsw_rs`. A tombstoned node keeps serving as a live stepping-stone for other queries regardless of the external filter. Changing that requires touching the actual graph-walk implementation.

Two ways to touch it, not one:
- **Fork `hnsw_rs` and patch its internal search loop.** This is real, and smaller in scope than reimplementing INSERT/SELECT-NEIGHBORS-HEURISTIC/layer-assignment/everything else from scratch. But `.claude/rules/vector-index.md` already states, from prior research this project did before choosing `hnsw_rs`: **"Neither library exposes graph internals for a native delta log — no HNSW library audited, in C++ or Rust, does."** That finding was about delta logs specifically, but the underlying fact — the traversal internals aren't exposed as an extension point in any audited library, `hnsw_rs` included — applies here too. A fork wouldn't be "add a hook"; it would mean owning and understanding `hnsw_rs`'s full internal implementation well enough to safely modify its core algorithm, then maintaining that fork against upstream indefinitely. That is not meaningfully cheaper or lower-risk than a from-scratch implementation of just the traversal/mutation logic — it may be worse, since it adds the burden of an unfamiliar codebase on top of the same algorithmic work.
- **Full replacement**, which this memo's requirement-3 analysis already found necessary anyway *if lock-freedom stays a requirement* — in which case requirement 4 is free to satisfy in the same rewrite, since routing-aware traversal is just another property of a graph you're already building from scratch.

## The actual, honest conclusion

Full replacement is justified by exactly one requirement — lock-free concurrency — and that requirement is a self-imposed stretch goal that the project's own stated success bar does not require. Native soft-delete-with-routing-exclusion independently pushes past "thin wrapper," but its cheapest honest path is a `hnsw_rs` fork, not necessarily a ground-up rewrite, *unless* lock-freedom is being pursued anyway, in which case bundling both into one rewrite is reasonable since you're already committed to owning the traversal internals.

This means the real decision isn't "wrap vs. replace" in the abstract — it's **whether lock-freedom stays in scope**, and everything else follows from that:

- **If lock-freedom stays in scope:** full replacement is genuinely the right call, not just the more ambitious one — a lock-based fork of `hnsw_rs` wouldn't get you there either. Native soft-delete comes along for free in the same effort. Phase 6 pauses for this, and that pause needs to be priced explicitly (see the council transcript's recommendation to re-sequence through `writing-plans` with the cost stated).
- **If lock-freedom is descoped** (as the council recommended, and as this project's own success bar already permits): the honest, smaller path is a `hnsw_rs` fork scoped *only* to the traversal-routing change for soft-delete, built behind the same coarse lock `hnsw_rs` already uses today — not a full rewrite, not a pause on Phase 6. Generic metrics and the recall/QPS bar are separately achievable against the dependency as-is, no fork needed.

This memo does not decide which of those two paths to take — that's the actual open question this page was supposed to force into the open, and it is genuinely open. What it does establish: "full replacement" is not automatically justified by the full requirement bundle, only by the lock-freedom requirement specifically. Any decision to proceed with full replacement should be made on that basis explicitly, not on the combined weight of all four requirements, three of which don't need it.
