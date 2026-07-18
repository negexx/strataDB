# Lock-Free HNSW Rewrite — Design

**Date:** 2026-07-18
**Branch:** `explore/hnsw-lockfree-rewrite`
**Status:** Approved for implementation planning

## 1. Goal and scope

Replace `hnsw_rs` in `crates/index/` with a from-scratch, fully lock-free HNSW
implementation (Malkov & Yashunin, arXiv:1603.09320), reached via extensive
brainstorming and an `llm-council` deliberation
(`.superpowers/council/council-transcript-20260718-140939.md`) and a
follow-up decision memo evaluating wrap/extend against full replacement
(`docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md`).
That memo's finding: full replacement is justified specifically by the
lock-freedom requirement — the project's own success bar (match/beat
`hnsw_rs`'s recall@k and QPS) doesn't itself require it, but lock-freedom
was chosen deliberately, twice-confirmed after explicit risk disclosure, and
stays in scope.

**Two stages, with a hard boundary between them:**

- **Stage 1 (this design's actual deliverable):** a fully lock-free HNSW —
  INSERT, SEARCH-LAYER, K-NN-SEARCH, both SELECT-NEIGHBORS variants — over a
  fixed-capacity atomic slot-array node representation, generic over
  distance metrics, with soft-delete scoped to **tombstone-flagging only**:
  excluded from results, still used as a live traversal waypoint by other
  queries (matching `hnsw_rs`'s current behavior exactly, except the flag
  lives natively in the graph instead of being bolted on externally via
  Strata's `is_visible` closure).
- **Stage 2 (explicitly not scheduled):** active edge cleanup /
  connectivity-aware repair when a node is deleted. No reference algorithm
  exists anywhere — the paper's own Future Work section lists "removal" as
  unsolved. Tracked as an open research spike: no committed timeline,
  explicit permission to never land, and it must never gate or block Stage 1
  shipping.

Phase 6 (Strata's concurrent-write engine, approved plan on
`feature/phase-6-concurrent-writes`) stays paused for Stage 1's duration.
Phase 6's plan doesn't need to change once Stage 1 ships — it depends on
`HnswIndex`'s public API (`insert`/`search`/`search_filtered`/
`established_dimension`), not on `hnsw_rs` specifically. **This is a hard
constraint on Stage 1, not an incidental observation:** the new
implementation must preserve `HnswIndex`'s existing public method
signatures exactly (same parameter/return types Phase 6's already-approved
plan was written against), so swapping the internals never requires
touching `crates/txn/` at all.

## 2. Node representation and memory model

**Row-id-indexed storage, no hashing.** Strata's row-ids are globally
monotonic, dense `u64`s assigned sequentially by the transaction layer.
Node lookup is therefore pure arithmetic, not a hash map: a demand-allocated
chunked array where chunk index = `row_id / CHUNK_SIZE`, offset = `row_id %
CHUNK_SIZE`. Chunks are allocated lazily as the graph grows and, once
allocated, are **never freed or moved** — reading a chunk never risks a
use-after-free, so no epoch guard is needed to access node data. Only the
top-level directory of chunk *pointers* is pre-sized generously (cheap
regardless of scale, since it holds pointers, not node data).

**Chunk publish race:** if two threads race to allocate the same chunk
index, this resolves without any reclamation machinery — the loser's
chunk was never visible to any other thread (a plain `AtomicPtr`
compare-exchange on the directory slot), so it's synchronously dropped, no
epoch tracking required.

**One allocation per node, not one per layer.** A node's neighbor slots
(`Mmax0` at layer 0, `Mmax` at every layer above — `Mmax0 = 2*M` per the
paper's own recommendation) are computed once at insert time from the
node's randomly-assigned level and packed into a single contiguous buffer:
layer 0's slots, then layer 1's, then layer 2's, etc. `SEARCH-LAYER`'s hot
loop walks neighbor-to-neighbor constantly; keeping each node's own edges
contiguous is a direct win against the actual bottleneck (cache misses
during traversal), not incidental tidiness.

**Slot representation.** Each slot is an `AtomicU64`: `EMPTY` or an
occupying neighbor's row-id. Claiming an edge is
`compare_exchange(EMPTY, neighbor_id, ...)` against the next open-looking
slot, retrying against the next slot on failure. A node's deleted state is
a separate `AtomicBool`, not encoded in any slot — whether a *neighbor* is
currently live is a lookup against that neighbor's own flag, not a slot
property.

**Given Stage 1 never frees node/edge data at all** (no active removal,
tombstoning is a flag flip, not a deallocation), `crossbeam-epoch` does not
appear anywhere in Stage 1's design. This is a direct, deliberate
consequence of the tombstone-flag-only scope from Section 1, not an
oversight — it removes the single riskiest class of lock-free bug (a
traversal thread touching memory another thread's epoch advance has
reclaimed) from the design entirely, because nothing is ever reclaimed.

## 3. Adapting the paper's algorithms

**SEARCH-LAYER (Algorithm 2)** needs almost no change — visited-set,
candidate queue, and result list are all thread-local. `neighbourhood(c)`
at layer `lc` becomes a plain atomic load per occupied slot (no CAS, search
never mutates). The deleted-flag check gates entry into the result list
`W`, never traversal through a node's own edges — this is what "tombstone
as waypoint, excluded from results" means mechanically.

**INSERT (Algorithm 1)** keeps its two-phase shape (ef=1 descent to find an
entry point, then real connection-building from `min(L,l)` down to layer
0). What changes: step 11 (bidirectional connections) and steps 12-16
(shrinking an oversized neighbor) become CAS-slot-claim loops instead of
in-place list mutation, per Section 2. Step 19 (advancing the top-layer
entry point when this node's level exceeds the current maximum) becomes a
single `AtomicU64` compare-exchange on a graph-level entry-point cell.

**SELECT-NEIGHBORS-HEURISTIC (Algorithm 4)** is a pure function once its
candidate input is captured into local memory — no adaptation needed. Its
output (the chosen `M` neighbors) is what the CAS-slot-claim loop writes
back.

**Why no OCC-retry-loop is needed** (resolving the council's flagged
"linearizability, not retry-loop" concern): shrinking a neighbor's
connections is a per-slot `compare_exchange(occupied, EMPTY, ...)`. If that
CAS fails, another thread already changed that exact slot — the correct
response is simply to leave the edge alone, since whatever is there now is
newer information than what this thread planned to remove. A failed slot
CAS is a self-resolving no-op, not an error condition requiring a retry
loop the way `Transaction::commit()`'s whole-snapshot CAS does.

## 4. Performance-critical design choices

**SIMD distance computation via a proven crate, not hand-rolled
intrinsics.** Close to required, not optional, to hit the "match/beat
`hnsw_rs`" bar — `hnsw_rs`'s own distance backend (`anndists`) is already
SIMD-accelerated. Depend on a proven SIMD distance crate (`anndists`
itself, or `simsimd`) for the distance-function layer specifically, same
"don't reinvent a solved problem" logic that justified `hnsw_rs` in the
first place — narrowed to the one piece (distance kernels) that's
genuinely solved elsewhere, keeping the actual novel work (lock-free graph,
native deletion) as the focus.

**Generic distance metrics** (L2, cosine, inner product) are a trait bound
over the distance function, threaded through the graph type — orthogonal
to the concurrency design, no interaction with the slot-array/CAS
machinery.

**Batch insert.** `Transaction::commit()` already inserts many rows per
call, not one at a time — a dedicated `insert_batch` amortizes entry-point
lookups across the batch instead of repeating them per row.

**Explicitly not adopted, and why:**
- **Lossy compression / reduced precision** (f16/int8 quantization) would
  work against the recall@k success bar — `hnsw_rs` stores full f32;
  quantizing makes "beat hnsw_rs" harder, not easier.
- **GPU vectorization** doesn't fit — Strata is an embedded single-node
  engine with no GPU dependency anywhere in its stack.
- **Branchless micro-optimization** of the deleted-flag check is premature
  — revisit only if benchmarking shows it's actually hot.

## 5. Testing strategy

Given Section 2's finding that Stage 1 has no reclamation machinery at all,
the loom-testable surface is smaller than the council's original risk
assessment feared. Four isolated loom tests, each modeling one small,
bounded primitive — mirroring this project's own established pattern
(Phase 5/6 never loom-tested `Transaction::commit()` end-to-end; they
modeled the essential race abstractly on loom's own primitives):

1. **Slot-array CAS claim/shrink.** A small fixed-capacity array (capacity
   4, loom-tractable), 2-3 threads: some claiming slots, one shrinking.
   Proves no slot ever reaches a torn state, a claim never silently
   overwrites another thread's already-claimed slot, concurrent
   claim+shrink never produces a phantom edge.
2. **Chunk-directory publish-or-discard race.** Two threads racing to
   allocate the same chunk index — exactly one wins, the loser's
   allocation is safely discarded, every subsequent reader observes the
   same winning pointer.
3. **Entry-point CAS race.** Directly reuses the shape of Phase 5's
   existing `one_writer_store_races_safely_with_many_readers_load` test for
   the top-layer entry-point cell.
4. **Not loom-tested** (too large a state space, same call already made for
   full-`commit()` coverage): end-to-end INSERT/SEARCH-LAYER interleaving —
   covered instead by real-thread stress tests below.

Beyond loom:

5. **Real-thread stress test.** Many concurrent inserting/searching threads
   against a real graph — every inserted row-id must be findable
   afterward, no panics under maximum slot-claim contention.
6. **Recall/QPS benchmark against `hnsw_rs`** — the literal success-bar
   test, on a public dataset, matching this project's Phase 4
   exit-criterion language ("benchmarked on a public embedding dataset").
7. **Deletion correctness test**, applying the exact lesson this project
   already learned in Phase 5 (the "vacuous test" catch): query at a
   tombstoned node's own location, where it would be the true nearest
   match if the deleted-flag check were broken, not a geometrically
   implausible spot — proving the flag actually gates results while the
   node still serves as a live traversal waypoint.

## 6. References

- `.superpowers/council/council-transcript-20260718-140939.md` — the
  llm-council deliberation this design's node representation, reclamation
  strategy, and staging are built on.
- `docs/superpowers/specs/2026-07-18-hnsw-rs-wrap-vs-replace-decision.md` —
  why full replacement, not wrap/extend, and specifically why only the
  lock-freedom requirement justifies it.
- `.claude/docs/decisions/0005-rust-over-cpp-reversal.md` — the ADR this
  proposal reopens; the decision memo above addresses its rationale
  directly rather than ignoring it.
- `.claude/rules/vector-index.md` — "neither library exposes graph
  internals for a native delta log... no HNSW library audited, in C++ or
  Rust, does" — the prior research finding that ruled out patching
  `hnsw_rs` as a cheaper alternative to full replacement.
- arXiv:1603.09320 (Malkov & Yashunin) — Algorithms 1-5, recommended
  parameters (M 5-48, `Mmax0 = 2*M`, `efConstruction` 100-500,
  `mL = 1/ln(M)`).
