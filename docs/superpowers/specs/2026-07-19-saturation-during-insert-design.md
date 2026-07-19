# Saturation-During-Insert Recall Fix — Design

**Date:** 2026-07-19
**Status:** Approved for implementation planning

## 1. Goal and scope

Follow-up to the HNSW search performance improvements plan (PR #10, merged).
After that plan landed, a re-run of `lockfree_vs_hnsw_rs_bench` found
`Graph`'s recall@10 had dropped from 0.9970 (pre-plan) to 0.9890
(post-plan) — still comfortably above the 0.8 gate and still beating
`hnsw_rs` (~0.97), but a real, deterministic, code-caused regression, not
noise (confirmed via a bit-for-bit-identical re-run and a Fable 5
architecture audit).

**Root cause, isolated via a real A/B (temporarily toggled, reverted, not
committed):** saturation-based early termination (`search_layer`'s
"Patience in Proximity" mechanism, added in the search-performance plan)
is unconditional — it fires during `Graph::insert`'s own internal
`search_layer` calls (ef=1 descent and the real `ef_construction=200`
connection-building), not just at query time. Firing during construction
truncates the candidate set `SELECT-NEIGHBORS-HEURISTIC` gets to choose
from, permanently baking slightly-worse edges into the graph — a one-time
build-speed win traded for a recurring per-query recall cost, which is a
worse trade than the *intended* one (query-time saturation trading
per-query eval count for per-query recall, where the tradeoff at least
recurs on the same side of the ledger).

The audit's A/B:

| Configuration | Recall@10 |
|---|---|
| Saturation on everywhere (current, merged state) | 0.9890 |
| Saturation off during `insert` only, still on for queries | 0.9940 |
| Saturation off everywhere (pre-plan baseline) | 0.9970 |

This confirms disabling saturation specifically during `Graph::insert`'s
`search_layer` calls recovers the entire construction-side loss
(+0.0050), while query-time saturation (the remaining 0.9940→0.9970 gap)
is the accepted, intended trade the original plan already signed off on.

**This fix is scoped narrowly**: make saturation conditional per call,
disable it for `Graph::insert`'s two calls, keep it for
`Graph::k_nn_search`'s two calls. It does not reopen query-time
saturation, does not touch `HnswIndex`'s public API, and does not add any
tunable/adaptive middle ground between "on" and "off" (see §4).

## 2. Mechanism

`search_layer` gains a `saturate: bool` parameter, added immediately
after `filter` in its signature — matching this codebase's existing
convention of threading per-call behavior explicitly (as `filter` already
is) rather than through hidden state or a second near-duplicate function.
When `saturate` is `false`, the saturation-check block is skipped
entirely (the loop's original Algorithm 2 stopping condition — result set
full and the next candidate strictly farther than the worst member — is
untouched and still applies either way; only the *early*-exit path is
gated).

**Call site assignments** (4 total, all in `crates/index/src/graph.rs`):

- `Graph::insert`'s ef=1 descent call → `saturate: false`
- `Graph::insert`'s `ef_construction` connection-building call → `saturate: false`
- `Graph::k_nn_search`'s ef=1 descent call → `saturate: true`
- `Graph::k_nn_search`'s real-`ef` layer-0 call → `saturate: true`

`HnswIndex`'s public API is untouched — this is entirely internal to
`Graph`, the same shape as the existing `alpha` parameter (added in the
search-performance plan, also internal-only).

## 3. Testing and verification strategy

Three distinct pieces of evidence, matching what "doing this properly"
requires — not just a compiling change:

1. **A discriminating unit test** proving `saturate: false` genuinely
   changes traversal behavior, not just that the parameter exists and
   compiles. Reuses the `CountingL2` distance-eval-counting pattern
   already established twice in this codebase (the bench file's
   `print_distance_calls_per_search`, Task 2's own saturation test):
   build two graphs from identical inputs and parameters, one calling
   `search_layer` with `saturate: true`, one with `saturate: false` (both
   directly, not through `insert`/`k_nn_search`, to isolate the parameter
   itself), on a fixture already known to trigger saturation (reusing or
   adapting Task 2's existing 10-point-cluster/10-satellite fixture,
   which is already proven to discriminate). Assert `saturate: false`
   visits strictly more candidates than `saturate: true` on the same
   fixture.
2. **Real insert-throughput measurement**, not just search-side recall.
   Add wall-clock timing (`std::time::Instant`, printed via `println!` —
   not a `criterion::bench_function`, since criterion's repeated-sampling
   model doesn't fit a single bulk 10k-vector insert) around the existing
   insert loop in `bench/benches/lockfree_vs_hnsw_rs_bench.rs`. Requires
   a genuine before/after: capture the *current* (saturation-everywhere)
   insert wall-clock time as a baseline (first implementation step, before
   any code changes), then re-measure after the fix lands.
3. **Recall recovery confirmed via the real end-to-end benchmark**, not a
   synthetic fixture alone. Re-run `lockfree_vs_hnsw_rs_bench` after the
   fix and confirm `Graph`'s recall@10 moves back toward 0.9940 (the
   audit's measured number for "saturation off during insert, on for
   queries") — not merely "some improvement in the right direction."

If the measured insert-time cost turns out to be large enough to be a
real concern (no threshold defined in advance — this is a judgment call
for whoever reviews the actual numbers), that's a signal to revisit §4's
rejected adaptive-saturation option, not a reason to silently accept a
worse insert-time regression than the recall gain justifies.

**Actual outcome (recorded after implementation, not a prediction):**
recall@10 landed at exactly 0.9940 as targeted, reproduced identically
across two independent runs. Insert wall-clock got *faster*, not slower
as predicted above — the saturation bookkeeping (clearing/extending a
`HashSet`, computing an intersection) costs real time every loop
iteration regardless of whether it ever triggers an early exit, and
`Graph::insert`'s `ef_construction = 200` phase requires a 60-iteration
stable-membership streak to fire at all, rarely reached while the
result set is still churning during construction. Removing the check
strips that per-iteration cost while forfeiting an early exit that
seldom paid for itself during insert — net speedup, not the assumed
tradeoff. See the implementation plan's "Task 3 Results" section for
the exact numbers.

## 4. Explicitly not pursued: adaptive/tunable saturation during insert

Considered and rejected for this fix: instead of a binary on/off, give
`Graph::insert`'s calls a much higher patience threshold (weaker
saturation) rather than disabling it outright, trying to keep some of
the construction-time speed benefit while recovering some recall. Rejected
because the audit already found "off entirely during insert" fully
recovers the construction-side loss with no evidence a partial version is
necessary — adding a tunable parameter with no validated benefit is
exactly the kind of premature complexity this project's conventions
("YAGNI ruthlessly") argue against. Revisit only if §3's insert-time
measurement shows the full-disable cost is unacceptably high.

## 5. References

- `docs/superpowers/plans/2026-07-19-hnsw-search-performance-improvements-plan.md`
  — the plan this is a follow-up to; Task 2 introduced the saturation
  mechanism this fix scopes down.
- `docs/superpowers/specs/2026-07-19-hnsw-search-performance-improvements-design.md`
  §3 — the original saturation design (Teofili & Lin, ECIR 2025,
  "Patience in Proximity").
- Fable 5 architecture audit (this session, no separate file — see the
  conversation transcript) — the empirical A/B that isolated the root
  cause and validated the fix's expected recall recovery before any code
  was written.
