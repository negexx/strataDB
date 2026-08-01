# Ingest + Recovery Performance Audit

**Status:** Historical
**Current replacement:** [Status ledger](../../status.md)
**Archived from:** `docs/analysis/2026-07-24-ingest-recovery-performance-audit.md`

> Date: 2026-07-24 · Two parallel Opus-tier audits (one on the ingest+commit write path, one on
> `Dataset::open` recovery), code-verified against source, then synthesized.
>
> **Coordination note — S1 is being built in parallel.** A separate session is implementing the
> segmented immutable index layout ([ADR 0008](../../decisions/0008-adopt-segmented-index-layout.md),
> spec [`phase-s1-segmented-index-spec.md`](../../design/phase-s1-segmented-index-spec.md)). This doc is
> written to **compose with that work, not collide with it.** Every finding is tagged:
> **[DO NOW — survives/serves S1]**, **[DON'T — S1 deletes it]**, or **[FEEDS S1]**. Read the S1
> reframe (§3) before acting on anything.

---

## 1. Measured baseline

`bench/benches/lifecycle_bench.rs`, 25k rows × 512-dim real OpenAI embeddings:

| Phase | Wall | Alloc churn | Peak live | Nature |
|---|---|---|---|---|
| ingest + commit | 38.6 s (~650 rows/s) | 3,780 MB | 116 MB | ~95% HNSW graph construction |
| recovery / reopen | 36.3 s (~690 rows/s) | 3,017 MB | 86 MB | full HNSW rebuild (replay every insert) |

Both phases are **CPU-bound on HNSW graph construction**. Recovery is nearly as expensive as ingest
because it re-runs the same inserts; it just swaps encode+fsync for JSON-read+parse. Everything else
(Arrow encode, stats, fsync, delta-log JSON) is single-digit-percent of each.

## 2. The one finding that dominates both phases

**The lock-free, loom-tested concurrent-insert machinery in `crates/index` is used for nothing on the
write path.** Every production graph insert runs single-threaded inside `commit_lock`
(`dataset.rs:842-910`), and recovery replays inserts single-threaded (`dataset.rs:1268-1277`). That
single fact is why both phases sit at ~650–690 rows/s.

Both audits reached this independently. Parallelizing insertion is **~4–6× on ingest** (38.6 s →
~7–10 s) and **~4–8× on recovery**, from the *same* capability. The graph internals are already
loom-covered (a 16-thread concurrent-insert stress test exists at `graph.rs:1338-1408`; slot-array,
node-table, and entry-point races are loom-tested). Scaling is **sub-linear** — the shared
`EntryPoint::advance_if_higher` CAS (`graph.rs:96-117`) and hot high-level slot arrays cap it around
4–6×, not 8×. Measure at 2/4/8 threads before fixing a thread count.

## 3. The S1 reframe — READ THIS BEFORE ACTING

S1 changes *where* the graph lives (immutable segments) and makes recovery a **segment load, not a
rebuild**. That splits this audit's findings cleanly:

- **Ingest still builds HNSW graphs** — under S1 it builds them *into segments*, but the inserts, the
  distance computations, and the allocations are the same. **So every ingest/construction optimization
  here survives S1 and compounds with it** — it makes S1's per-segment build faster too.
- **Recovery's rebuild is deleted by S1** (W5 loads segments). **So every recovery-*rebuild*
  optimization here is throwaway** — S1 removes the thing being optimized.

Therefore the rule for this audit, given S1 is in flight:

> **Optimize graph *construction* (helps ingest and S1's segment build). Do NOT optimize the monolithic
> *rebuild* (S1 replaces it entirely).**

This inverts the recovery audit's default advice: its "parallelize the replay / stream the delta-log
read" stopgaps were premised on "if S1 is months out." **S1 is not months out — it is being built now
— so those recovery-specific stopgaps should be skipped.** The only recovery-relevant work worth doing
is what *feeds* S1 (the graph-serialization format).

## 4. DO NOW — survives S1, compounds with it

| # | Change | Impact | Where | Risk / status |
|---|---|---|---|---|
| A | **`ef_construction` recall/build sweep** | 200→100 ≈ **~2×** on the dominant per-insert cost; near-linear | `dataset.rs:1212` (one constant) | recall-tradeoff — **measure, don't guess**. Zero code conflict with S1. Start here. |
| B | **Allocation hoisting in the insert hot loops** | Collapses most of the 3.78 GB churn + shaves build CPU | `slot_array.rs:69-75`, `graph.rs:273,483,493,698,701` | safe-now (thread-local, mirrors existing `SearchScratch`). In `crates/index` — coordinate file overlap with S1. |
| C | **`insert_owned` — remove the wasted vector copy** | ~50 MB churn @ 25k; serves ingest **and** S1 segment build | `hnsw.rs:182`, `dataset.rs:900-910` | safe-now. Vectors are copied **3×** today; the middle `to_vec` is pure waste. |
| D | **Parallel graph insertion (build the reusable primitive in `crates/index`)** | **~4–6× ingest** (the big one); also speeds S1's per-segment build | primitive in `crates/index`; call site `dataset.rs:900-910` | needs-a-loom-test + design note. Build the *primitive* in `crates/index` (S1-friendly); coordinate the *call site* with S1's write-path migration. |
| E | Hoist per-row Arrow downcast in `build_delta_entries`; fix loop-invariant node re-lookups in the heuristic | 2 of 3 allocs/row removed; minor CPU | `dataset.rs:1320-1340` (ref fix exists at `group_by.rs:194-210`); `graph.rs:573-578,715-717,485-492` | safe-now. `build_delta_entries` is in the write path S1 is migrating — check with the S1 session. |

**Why A is first:** it's pure measurement (sweep `{50,100,150,200}` against `vector_search_bench` /
`segment_recall_bench`, time the build, read recall@10). It has **zero** code-conflict surface with
S1, and it decides a constant that both the monolithic path and S1's segments inherit. `ef_construction`
is the build-cost dial, and the insert-time saturation early-exit is **disabled** (`saturate=false`,
`graph.rs:453,464`), so there's no adaptive escape — every build traversal runs the full ef=200 beam.
Recall is ~0.985 with lots of headroom, so 200 is likely over-provisioned. Cite the run per the
vector-index rules.

**Coordination on B/C/D/E:** they touch `crates/index` (insert/search hot loops) and the commit write
path — both areas S1 is actively migrating. The hot-loop functions (occupied, search_layer, the
heuristic, insert) are largely orthogonal to S1's format/manifest/recovery focus, so overlap is
moderate — but the **parallel-insert primitive and `insert_owned` should be added to `crates/index` as
reusable methods** (which S1's segment builder can call), rather than inline in the commit loop, to
minimize collision and to directly benefit S1.

## 5. DON'T BUILD — S1 deletes it

| Change | Why not |
|---|---|
| **Monolithic graph checkpoint** (serialize the built graph so `open` loads it) | This *is* S1/W5. ADR 0008 explicitly discourages `crates/index`/`crates/storage` changes that harden the monolithic design. A monolithic checkpoint wired into `open` is throwaway integration S1 rips out. |
| **Parallelize the replay** | S1 removes the replay entirely (loads segments). The parallel-*insert* primitive (D) is worth building; wiring it into the monolithic replay loop is not. |
| **Stream the delta-log read (`BufReader::lines`)** | Memory cleanup on a path S1 may delete — S1 §7 Q1 may remove the delta log or demote it to a WAL. |
| **Binary delta-log encoding** | S1 §7 Q4 mandates a binary *segment* format and may delete the delta log. Building a bespoke binary delta log for recovery is throwaway. |

## 6. FEEDS S1 — hand to the S1 session

**The graph-serialization format.** The recovery prize ("load, don't rebuild") is S1/W5, and it needs
exactly the thing this audit's #1 recovery finding describes: a length-prefixed binary format that
serializes the built graph (node table + per-layer edge lists + vectors) so it loads without re-running
inserts — turning `O(n·log n·dist)` into `O(nodes)` deserialization. That is **S1 §7 Q4** (segment
format) and **§7 Q2** (vectors-in-segment vs reference — see the separate recommendation to have the
*segment own the vectors*). `NodeTable::insert_ptr` (`node_table.rs:206`, currently unused) is a
ready-made entry point for reconstructing nodes from pre-built raw blocks with no re-insert. **This is
groundwork the S1 session should own, not a standalone recovery feature.**

## 7. Full findings — Ingest + commit audit

1. **Parallelize HNSW insertion within a commit** — ~4–6× (→ ~7–10 s), medium effort, needs loom
   test. Inserts stay inside `commit_lock`, so OCC/conflict/single-CAS are untouched; only critical-
   section wall-time shrinks. `GraphResidueGuard`'s row-id collection must become thread-safe
   (per-worker Vecs merged). Inter-commit parallelism (build outside the lock) is a bigger win but
   **design-gated** (entangles visibility + residue guard + conflict-vs-mutation ordering) — do
   intra-commit first.
2. **Kill the per-candidate `occupied()` heap alloc** (`slot_array.rs:69-75`, called at `graph.rs:273`
   once per popped candidate in the ef=200 traversal, and `:483,:493` in shrink) — the largest single
   slice of the 3.78 GB churn (~150 KB/insert). Iterator or scratch-buffer form; snapshot is already
   non-atomic-across-slots so an iterator preserves semantics. *Corrects the prior audit: the
   "70–100 allocs/insert" figure undercounts the build path — it misses this per-pop loop.*
3. **`ef_construction=200` is the build-cost dial** (`dataset.rs:1212`) — near-linear; saturation
   early-exit is disabled during insert so there's no adaptive escape. Sweep against measured recall.
4. **Remove redundant `vector.to_vec()`** (`hnsw.rs:182`) — the vector is copied 3× (delta build →
   `to_vec` → node block); the middle is waste. Add `insert_owned`, consume deltas by value.
5. **Hoist per-row Arrow downcast in `build_delta_entries`** (`dataset.rs:1320-1340`, ref fix at
   `group_by.rs:194-210`) — 3 allocs/row → 1. **Fix loop-invariant node re-lookups** in
   `pairwise_distance` (`graph.rs:573-578`) called from the heuristic's O(m²) diversity check and the
   O(cap²) shrink.
6. **`select_neighbors_heuristic` per-call Vecs** (`graph.rs:698,701`) — fold into the thread-local
   scratch from #2.
7. **Delta-log write** (`delta_log.rs:39-42`) — `serde_json::to_writer` instead of `to_string` (byte-
   identical, no per-entry `String`); ~1–2% of the 38.6 s. Binary encoding is design-gated + low-value.

## 8. Full findings — Recovery / reopen audit

1. **Graph checkpoint / load-don't-rebuild** — the whole prize (~36 s → sub-second, ~30–50×; churn →
   tens of MB). **SUBSUMED BY S1/W5.** Do not build a standalone monolithic checkpoint. The
   serialization *format* is S1/W3 groundwork (§6 above).
2. **Parallelize the replay** — ~4–8×. Same primitive as ingest #1. **Stopgap only if S1 were far
   out — it isn't, so skip the replay wiring;** build the primitive for ingest instead.
3. **Stream the delta-log read + `insert_owned`** (`delta_log.rs:60`, `hnsw.rs:182`) — caps peak
   delta-log memory, removes one copy/row. `insert_owned` is worth it (serves both, survives S1); the
   streaming read is throwaway if S1 deletes the delta log.
4. **Binary delta-log encoding (read side)** — subsumed by S1 §7 Q1/Q4; don't build for recovery alone.

Recovery does one thing right: it reads only delta logs, never re-reads/re-parses the Arrow data files
— no redundant data-file I/O (the whole-file re-read pathology is query-path only, in
`row_ids_matching`). Peak-live (86 MB) is the resident graph and is unavoidable whether built or loaded
— don't chase it.

## 9. What does NOT help either phase

- **LTO / `codegen-units=1`** — already measured **−70% on unfiltered vector search** and reverted
  (`6892d25`). Do not reapply.
- **Saturation early-exit** (prior audit #8's O(ef²) concern) — gated **off** during insert; query-path
  only. Irrelevant to both build phases.
- **Manifest O(v²) growth** (prior audit #1) — the bench does 5 commits, ~600× below the
  ~2,000–3,000-file crossover. Zero effect here.
- **`conflicts_with` quadratic scan** (prior audit #3) — insert-only commits have an empty write-set
  and short-circuit before any scan. No effect on ingest.
- **fsync batching / group commit** — 5 commits here; a small constant of the 38.6 s. Relevant to
  many-small-commit throughput, not bulk ingest.
- **Buffered Arrow IPC data-file writer** (prior audit #9) — correct on its own merits, but only 5 file
  writes here; the build is CPU-bound, so it barely moves the number.

## 10. Sequencing (S1-aware)

1. **`ef_construction` sweep** (§4A) — pure measurement, zero S1 conflict. Answers whether a ~2× ingest
   win is free. Do this first; it's the cheapest information.
2. **Allocation-hoisting PR in `crates/index`** (§4B, C, E) — collapses the 3.78 GB churn, shaves CPU,
   survives S1. Coordinate file overlap with the S1 session.
3. **Parallel-insert primitive in `crates/index`** (§4D) — the ~4–6× lever; build it as a reusable
   method so S1's segment builder benefits too. Design note + loom test.
4. **Hand the graph-serialization format to the S1 session** (§6) as W3/§7 Q4 input — do not build a
   monolithic checkpoint.
5. **Skip every recovery-rebuild optimization** (§5) — S1/W5 owns the recovery killshot.

## 11. Net

Stack §4A–D and ingest plausibly drops from 38.6 s to **~4–6 s** (parallel × lower-ef × less
alloc-thrash), and — because S1 still builds graphs into segments — those wins carry into S1's
per-segment build. Recovery's 36.3 s is **not** attacked here on purpose: S1's segment load takes it to
sub-second, and any monolithic-rebuild work would be throwaway. The clean division of labor with the
in-flight S1 work: **this audit makes graph *construction* faster (ingest + S1 segment build); S1 makes
*recovery* a load instead of a rebuild.**
