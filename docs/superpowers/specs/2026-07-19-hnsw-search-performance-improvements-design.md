# HNSW Search Performance Improvements — Design

**Date:** 2026-07-19
**Status:** Approved for implementation planning

## 1. Goal and scope

The lock-free HNSW rewrite (`docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`)
shipped and already beats `hnsw_rs` on both recall (0.9970 vs 0.9750) and
median latency (~417µs vs ~468µs, real 10k-vector 512-dim dataset). This
design covers a follow-up performance pass, motivated by a concrete
benchmark finding, not speculative optimization: a distance-call profiling
addition to `bench/benches/lockfree_vs_hnsw_rs_bench.rs` measured that a
real `k_nn_search` call performs **~889 distance evaluations**, and
**~71% of full search latency is plausibly explained by raw distance
computation** — with reason to believe the true share is higher, since the
isolated per-call measurement (330.64ns) compares the same two vectors
repeatedly (cache-friendly), while a real search's calls are against
scattered, unpredictable neighbor vectors (cache-unfriendly).

That finding was investigated further (via a Fable 5 architecture audit,
`.superpowers/` session transcript, prompted specifically to check whether
graph reordering — the classic answer to "scattered neighbor lookups" —
is safe to build on top of the lock-free `NodeTable`) and produced a more
important discovery: **`anndists` (Strata's distance backend) has been
running its scalar fallback the entire time.** `anndists`'s `Cargo.toml`
declares `default = []` for its `[features]` table; Strata's own
`anndists = "0.1"` dependency line requests no features, so it silently
gets the scalar path — directly contradicting the original lock-free
rewrite's design doc, which assumed `anndists` was "already
SIMD-accelerated" (§4). This is not a new problem to solve — it's an
already-designed requirement that was never actually turned on.

**Five items, sequenced by expected value and risk:**

1. Enable `anndists`'s SIMD feature (new, highest expected value, lowest risk)
2. Saturation-based early termination in `search_layer`
3. Pre-allocated per-query search buffers
4. α-tunable pruning in `select_neighbors_heuristic` (internal only, not exposed through `HnswIndex`)
5. Software prefetch in `search_layer` — conditional on post-#1/#3 benchmark evidence

**Explicitly deferred, not part of this design:** true graph reordering
(RCM/Gorder-style physical relocation of already-inserted nodes). The
Fable 5 audit found this is a genuine architectural conflict, not a
tuning question — see §7.

## 2. Enable `anndists` SIMD

**Change:** `crates/index/Cargo.toml`'s `anndists = "0.1"` becomes
`anndists = { version = "0.1", features = ["simdeez_f"] }`.

**Why `simdeez_f`, not `stdsimd`:** `anndists` offers two SIMD feature
flags. `stdsimd` maps to Rust's still-nightly-gated `std::simd`
(portable_simd) — `rust-toolchain.toml` pins `channel = "1.90"` (stable),
so `stdsimd` is not usable without a much bigger, separate decision
(pinning the whole workspace to nightly). `simdeez_f` pulls in the
`simdeez` crate, which does SIMD via runtime CPU-feature detection on
stable Rust — no toolchain change needed.

**No code changes.** `distance.rs`'s `L2`/`Cosine`/`Dot` already delegate
to `anndists::dist::{DistL2, DistCosine}` — those types internally
dispatch to the SIMD path once the feature is compiled in. This is a
one-line dependency change plus a rebuild.

**Verification, not assumption:** rerun
`cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench` after the
change and confirm the isolated `l2_distance_eval_only` benchmark's
reported time actually drops before treating this as a win. The Fable 5
audit's 3-6x estimate is a class-of-improvement estimate for this kernel
shape, not a number to trust without re-measuring on this exact dataset
and hardware.

## 3. Saturation-based early termination ("Patience in Proximity")

Adapts the strategy from Teofili & Lin, ECIR 2025 ("Patience in Proximity:
A Simple Early Termination Strategy for HNSW Graph Traversal") to
`search_layer`'s existing loop shape.

**The real design question, not just a parameter choice:** the paper
computes saturation φ over the *top-k* result set's membership stability
across consecutive candidate visits. `search_layer` doesn't have a "k" —
its `result` heap is capped at the caller's `ef` (`k_nn_search` only
truncates to `k` after `search_layer` returns). Substituting `ef` for the
paper's `k` throughout is the only granularity that actually exists at
this layer, so:

- `γ = 95%` (saturation threshold), as in the paper.
- `P = max(7, ⌈0.3 · ef⌉)` (patience — consecutive saturated checks
  required before stopping), substituting `ef` for the paper's `k`.
- Saturation is checked against `result`'s current membership (row-ids
  currently held in the `ef`-capped max-heap) each time a candidate is
  popped from `candidates` and processed, comparing against the
  membership at the previous check.
- No separate "is ef too small to bother" flag: at `ef = 1` (the multi-layer
  descent phase's greedy search), `P = max(7, 1) = 7`, and a 1-element
  result set's membership is either unchanged (both checks are patience
  hits) or changed (never saturated) — the mechanism is self-limiting
  without extra branching, though this should be confirmed empirically
  during implementation, not assumed.

**Termination logic:** add a second break condition to `search_layer`'s
existing `while let Some(...) = candidates.pop()` loop, alongside the
existing Algorithm 2 line 7-8 check — if the patience streak reaches `P`,
break, same as the existing early-exit path.

**Non-negotiable test requirement, given this project's repeated
"vacuous test" lesson:** a passing compile is not evidence this preserves
recall. The implementation task must include a test that runs the same
query with and without patience-based termination enabled (e.g., via a
test-only toggle or by comparing against a fixture built before this
change) and confirms recall doesn't measurably regress on a real or
realistic fixture — not just that the code runs without panicking.

## 4. Pre-allocated per-query search buffers

`search_layer` currently allocates a fresh `HashSet<u64>` (visited),
`BinaryHeap<Reverse<Candidate>>` (candidates), and `BinaryHeap<Candidate>`
(result) on every call.

**Approach: `thread_local!` scratch buffers**, cleared (not reallocated)
and reused across calls on the same thread. Chosen over an explicit
object pool because:
- `search_layer`/`k_nn_search`/`HnswIndex::search` is a synchronous,
  blocking call path with no async runtime or thread-pool handoff — a
  `thread_local!` buffer genuinely stays with "the same logical caller"
  across a call, unlike in an async context where a future can resume on
  a different worker thread.
- Zero changes to any public or `pub(crate)` signature — the buffers are
  an implementation detail of `search_layer`'s body, not a new parameter.
- An object pool would add real complexity (pool sizing, `Drop`-guard
  return-on-panic logic) to solve a problem (bursty multi-thread reuse)
  with no current evidence it exists — YAGNI per this project's own
  conventions.

**Scope:** applies to `search_layer` only (the hot path measured in the
benchmark). `select_neighbors_heuristic`'s own working `Vec` allocations
are out of scope for this pass — no evidence they're hot (Algorithm 4 runs
once per insert's connection-building step, not once per distance
evaluation).

## 5. α-tunable pruning (RobustPrune / Vamana)

Generalizes `select_neighbors_heuristic`'s Algorithm 4 line 11 diversity
check from `pairwise_dist(candidate_id, picked) < query_dist` to
`pairwise_dist(candidate_id, picked) < query_dist / alpha`, matching
Vamana's RobustPrune reachability parameter (Subramanya et al.,
"DiskANN"; α ≥ 1, α = 1 reproduces the original HNSW heuristic exactly, α
> 1 relaxes the diversity check and retains more longer-range edges).

**Where `alpha` lives — internal only, not through `HnswIndex`:**
`alpha: f64` threads through `select_neighbors_heuristic` and
`Graph::insert`/`insert_batch` (both `pub(crate)`, no API-compatibility
constraint). `HnswIndex::new`'s public constructor is explicitly frozen
(hard constraint carried over from the whole lock-free rewrite, so
`crates/txn` never needs to change) — this design does **not** add a new
parameter to it. `HnswIndex`'s own internal call sites hardcode
`alpha = 1.0`, exactly reproducing today's behavior, byte-for-byte.

**Why internal-only is the right scope for this pass:** there is no
current evidence the fixed (α=1) heuristic is actually a bottleneck for
recall or connectivity — this item exists to make α *measurable*, not to
commit to shipping a non-default value. `bench/benches/lockfree_vs_hnsw_rs_bench.rs`
already constructs `Graph<L2>` directly with full parameter control, so a
future benchmark task can sweep α values and produce real evidence before
anyone decides whether `HnswIndex` should ever expose it — a separate,
later decision, not part of this design.

## 6. Software prefetch — conditional, sequenced last

`search_layer`'s neighbor-expansion loop (`node.layer(lc).occupied()`)
already materializes the full neighbor-id list before computing distances
one at a time. Standard practice (e.g. `hnswlib`) is to resolve all
neighbor node pointers first and issue a prefetch instruction for each
neighbor's vector before the distance-computation loop, hiding scattered
cache-miss latency behind the resolution work.

**This item is explicitly conditional**, not committed: implement it only
if a benchmark re-run *after* items 1 and 3 land still shows heavy
memory-stall time relative to compute time. If SIMD (item 1, §2 above)
alone closes most of the gap the original profiling identified, this item
may not be worth its complexity.

If implemented: `#[cfg(target_arch = "x86_64")]`-gated
`core::arch::x86_64::_mm_prefetch` calls, `unsafe`, with a `// SAFETY:`
comment per this project's convention (`unsafe_op_in_unsafe_fn = "deny"`
workspace-wide) — non-x86_64 targets simply skip prefetching (correctness
is unaffected either way; it's purely a latency hint).

## 7. Explicitly deferred: true graph reordering

The Fable 5 audit (full transcript available via this session) found
graph reordering (RCM/Gorder-style physical node relocation) is not a
tuning knob — it is a genuine architectural conflict with the lock-free
rewrite's central invariant: **once a node is published, it is never
moved or freed**, which is the entire reason Stage 1 needs no epoch-based
reclamation. Relocating a node's backing allocation while a concurrent
`search_layer`/`insert` call holds a live reference into it (both do, for
extended windows — `insert` holds a reference to the just-inserted node's
vector across its *entire* connection-building phase) is a genuine
use-after-free, not a corner case.

The audit also found reordering wouldn't deliver its classic win on the
current layout even if it were safe: each node is currently 4-5 separate
heap allocations (chunk directory → chunk → `Node` → `layers` buffer →
vector buffer), so row-id adjacency doesn't imply memory adjacency today
regardless of any reordering applied at the row-id level.

The only sound path to true reordering is a periodic full-graph rebuild
with atomic whole-table swap (mirroring `crates/txn`'s
`ArcSwap<Snapshot>` pattern) — which requires solving the in-flight-writer
problem (a rebuild that snapshots at time T and swaps later loses any
insert committed in between, a lost update — a flagship-claim violation,
not a perf detail) via either write quiescence or delta-capture-and-replay.
This is architecturally the same future project as Stage 2's already-deferred
active-edge-cleanup-on-delete (both need the same rebuild-with-replay
infrastructure) and should be designed once, together, not bolted onto
this performance pass. **Not scheduled. No committed timeline. Explicit
permission to never land**, matching this project's existing stance on
Stage 2.

If pursued in the future, the audit's suggested first step is a
single-allocation node layout (co-locate a node's header + edges + vector
into one allocation, still publish-once, zero new concurrency surface) —
a much smaller, self-contained item that would also make reordering
actually deliver its advertised win if ever built. Not part of this
design; recorded here so it isn't lost.

## 8. Testing strategy

- **Item 1 (SIMD):** no new test — existing `distance.rs` unit tests
  already exercise `L2::eval` for correctness; behavior is unchanged, only
  the backend implementation. Verification is the benchmark re-run
  described in §2, not a unit test.
- **Item 2 (patience):** a dedicated recall-preservation test per §3 —
  non-negotiable, given this project's history of tests that pass
  identically whether the underlying logic is correct or subtly wrong.
- **Item 3 (buffers):** existing `search_layer` tests (already extensive —
  nearest-neighbor correctness, filter/traversal, deleted-node exclusion)
  continue to prove correctness; add one test confirming buffer reuse
  across repeated calls on the same thread doesn't leak state from a
  prior call (e.g., a stale `visited` entry causing a real, reachable
  neighbor to be skipped).
- **Item 4 (α):** unit tests on `select_neighbors_heuristic` directly, at
  α=1.0 (must exactly match today's existing test expectations — a
  regression test that the default is unchanged) and at α>1.0 (proving
  the relaxed diversity check actually retains a longer-range edge a
  strict α=1 heuristic would have pruned — a genuinely discriminating
  test, not just "it compiles").
- **Item 5 (prefetch, if implemented):** no correctness test needed
  (prefetch is a pure latency hint, cannot change results) — verified via
  benchmark only.
- **No loom tests required for items 2-4**: none of them touch atomics,
  CAS, or any concurrency-sensitive code path — they're pure-logic changes
  to already-lock-free-safe functions' internal control flow. Item 3
  (thread-local buffers) touches concurrency only in the sense that each
  thread gets its own buffer, which is exactly what `thread_local!`
  guarantees — no new shared-state race is introduced, so this doesn't
  meet the "concurrency-touching change" bar this project's conventions
  set for mandatory loom coverage.

## 9. References

- Fable 5 architecture audit (this session) — full graph-reordering
  feasibility analysis, the `anndists` scalar-fallback discovery.
- `bench/benches/lockfree_vs_hnsw_rs_bench.rs`'s distance-call profiling
  addition (`print_distance_calls_per_search`, `CountingL2`) — the
  empirical basis for this whole design.
- Teofili & Lin, "Patience in Proximity: A Simple Early Termination
  Strategy for HNSW Graph Traversal in Approximate k-Nearest Neighbor
  Search," ECIR 2025 — `https://cs.uwaterloo.ca/~jimmylin/publications/Teofili_Lin_ECIR2025.pdf`.
- Subramanya et al., DiskANN / Vamana's RobustPrune routine — the α
  reachability parameter.
- "Graph Reordering for Cache-Efficient Near Neighbor Search," NeurIPS
  2022, arXiv:2104.03221 — background for the deferred item in §7.
- `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md` §2
  (memory model/zero-reclamation invariant) and §4 (the SIMD requirement
  this design closes).
- `crates/txn/src/dataset.rs`'s `ArcSwap<Snapshot>` — the precedent cited
  for §7's deferred rebuild-and-swap approach.
