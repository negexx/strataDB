# Full-pipeline performance audit (A-Z)

> 11 independent Opus 5 investigations, one per architectural section, run in parallel against
> `feat/phase-s1-segmented-index` @ `6f15aea` (the segmented-index rewrite, W3+W4a+W4b merged —
> the most advanced branch, not yet on `main`). Cross-references to `main` (`652b06a`, this
> worktree) are called out explicitly where relevant. Every finding below is grounded in
> file:line citations from the actual code; nothing here is generic advice.
>
> This document also corrects one error caught during synthesis: §B's agent claimed
> `PredicateKey`/a resolved-row-id cache already exists on the S1 branch. Direct verification
> (`git ls-tree`, `grep`) and §F's independent investigation both confirm this is false — S1 has
> neither. That claim is removed from §B below.

## How to read this

Each section (A-K) maps to a real subsystem, not an arbitrary slice. Every recommendation is
tagged **impact** (high/medium/low) and **cost** (small/medium/large). Anything that would
violate one of Strata's non-negotiable invariants — no silent write-buffering, snapshot isolation
only, single-node only, HNSW-only, lock-freedom in `crates/index` — is called out explicitly as
**FLAGGED**, not proposed.

## Top findings, ranked

Cutting across all 11 sections, in priority order (severity/certainty first, then impact × cost):

1. **[§J] Unbounded memory leak — ~512 KiB permanently leaked per commit.** `crates/index`'s
   `NodeTable`/`Node` allocations are never freed (`node_layout.rs:143-144` states this is
   deliberate — the old monolithic design never dropped a graph for the process's lifetime). The
   segmented rewrite now builds and drops one `HnswIndex` *per commit*
   (`crates/txn/src/dataset.rs:1356`), so every commit leaks a full 512 KiB `Chunk` plus every
   node block. This is a genuine bug, not a tuning question. **Fix is cheap and needs no `loom`
   proof** — a plain `Drop` has exclusive access by construction. (R1, §J)
2. **[§I/§A] The arrow-ipc malformed-schema panic guard is missing on this branch.** Confirmed:
   `catch_unwind`/`CorruptDataFile` appear nowhere in `crates/storage/src/datafile.rs` on S1. This
   is the exact bug this session found, fixed, and reported upstream
   ([apache/arrow-rs#10437](https://github.com/apache/arrow-rs/issues/10437)) — it's already
   fixed on `main` (`eeae3b9`/`1e1cfb1`/`652b06a`) but S1 forked before that landed. **Cherry-pick
   before merge; this is a live crash-on-corrupt-file risk today**, independent of any
   performance question.
3. **[§E] `SegmentReader::from_bytes` — the new binary segment parser — has zero fuzz coverage.**
   `fuzz/fuzz_targets/` on S1 contains only `manifest_parse.rs`. This is exactly the class of
   code (new, untrusted-binary-input parsing) that this session's `datafile_parse` fuzz target
   found a real crash in for the *old* format. Its own doc comment's "fails closed, no panic"
   claim is asserted, not proven. Concrete fuzz target designs are in §E below, ready to use.
4. **[§H] Manifest commit cost is O(v²), ~80% attributable to I/O, and it gates everything else.**
   Every commit deep-clones the *entire* accumulated manifest, re-serializes it to JSON, and
   rewrites+fsyncs the whole file (`manifest.rs:193-217`, `dataset.rs:998`). Measured 12.2ms → 39.5ms
   as file count grows 300 → 6000. **§G's own conflict-check optimizations cannot move end-to-end
   commit latency until this is fixed** — at 6000 files, manifest work (~39ms) is ~500x the
   conflict check (~57µs). This is the single highest-leverage fix in the transaction layer.
   Bonus finding: no manifest-file GC exists either — ~2.6 GB of historical manifests accumulate
   for a trivial dataset by commit 6000.
5. **[§F/§I] ~130x read amplification on every predicate-filtered query.** A row is 2080 bytes;
   the embedding vector is 2048 of them (98.46%). Resolving which rows match a predicate
   (`row_ids_matching`) reads the *entire* row body regardless — Arrow IPC's `FileReader` reads
   the whole contiguous message before any column projection is applied
   (confirmed against arrow-ipc source, §I). Two independent, complementary fixes proposed: split
   scalar/vector columns into separate files (§F, larger change, full fix) or write multi-block
   IPC files to get block-level byte-range reads within the existing format (§I, smaller change,
   most of the win). This, combined with finding 6, explains nearly all of the "filtered vector
   search regressed" numbers from this session's earlier benchmarking — see reconciliation note
   below.
6. **[§C] HNSW fan-out across segments is sequential and unparallelized; a new bottleneck (SipHash
   saturation checks) was found alongside it.** `SegmentSet::fan_out` is a plain `for` loop, every
   segment searched at the full unmodified `ef_search`, zero threading dependency in
   `crates/index` at all. Independently, `k_nn_search_generic`'s saturation bookkeeping runs two
   SipHash `HashSet`s per popped candidate — comparable cost to the actual distance computation,
   previously unidentified. Parallelizing fan-out (`std::thread::scope`, no new dependency, thread-
   local scratch already supports it) plus fixing saturation are both high-impact, small-cost.
7. **[§E] Segment compaction is unsolved, and the naive approach is provably wrong, not just
   slow.** No compaction exists; segment count grows unboundedly with commit count, which
   directly determines §6's fan-out cost. Concatenating two segments' HNSW graphs would silently
   produce a *disconnected graph* (no edges between the two original node sets) — worse than
   today's fan-out, not better. §E worked out a sound alternative (seeded-merge: keep the larger
   graph, re-insert the smaller one's vectors through the normal insert path — the same approach
   Lucene uses for HNSW merge) with a concrete cost model.
8. **[§D] A proven 2.51x insert speedup was never ported to the new segment-build path.** S1's
   segment builder is still fully sequential (confirmed: zero `rayon`/`thread::scope`/production
   `spawn` in the build path). The old monolithic branch's parallel-insert commit (`3697ba8`)
   ports cleanly and is *simpler* on the new architecture — the panic-safety/residue-guard
   machinery that made up most of that commit's size isn't needed, since a failed segment build
   just gets discarded rather than corrupting a long-lived shared graph.
9. **[§G] Group commit is still just a proposed ADR.** The ~12ms fsync floor per commit (ADR
   0006) has no implementation. §G worked out a concrete design that respects the no-silent-
   buffering invariant precisely (batch the fsync syscall only, never the acknowledgement) and
   flagged the exact reordering hazard an implementer could introduce by accident.
10. **GPU and NUMA: independently evaluated and rejected by both §C and §K, for consistent
    reasons.** Worth stating plainly so it doesn't get re-litigated: HNSW's construction is
    sequentially-dependent pointer-chasing (not a GPU-shaped workload without becoming a
    different index entirely, which would violate HNSW-only); a single query's distance-eval
    workload is far too small to amortize PCIe transfer; predicate/group-by are bandwidth-bound,
    where PCIe is slower than RAM. NUMA has no realized benefit because the write path is single-
    threaded inside one global lock and datasets at Strata's target scale fit in one socket's
    memory. Both §C and §K reached this independently — treat it as settled, not open.

### Reconciliation note: the "24x filtered-search regression" from this session's earlier benchmarking

Earlier benchmarking this session found filtered vector search ~24x slower on S1 than on `main`.
That was diagnosed as mostly an artifact of S1 forking before `main`'s `live_set_cache` (a
per-snapshot resolved-row-id cache) landed — S1 re-resolves the predicate on every repeated query
instead of caching it once. This audit's §F and §I confirm and *sharpen* that finding: **the
cache was hiding a 130x read amplification, not eliminating it.** A cold cache, or 50 distinct
predicates instead of one repeated one, puts `main` back on roughly the same curve S1 is on today.
The read-amplification fix (finding 5) is the one that holds regardless of query mix or caching;
porting the cache back (as previously identified) is still worth doing, but is not sufficient on
its own.

---

## §A — Client/API surface & FFI boundary

**Files:** `crates/bindings` (PyO3), `crates/cli`.

### Current state
`crates/bindings` is a 17-line placeholder (`placeholder_version()` only) — no real Python API
exists yet, so there is no FFI boundary to measure for zero-copy/GIL behavior. The CLI
(`crates/cli/src/main.rs`) pays full `Dataset::open` cost on every single invocation, including
commands that never touch the vector index: `load_segments` does `fs::read` (copy 1) into
`SegmentReader::from_bytes` (copy 2 into aligned memory), CRC32Cs the whole body, and runs full
structural validation — over 100% of on-disk index bytes, for e.g. `strata inspect`.

### Bottleneck root causes
- **B1**: eager, unconditional, triple-pass segment load in `Dataset::open`, paid by every CLI
  invocation regardless of whether it needs the index.
- **B2**: `handle_search` resolves k matched row-ids via a *full table scan* (`snapshot.scan`)
  instead of a targeted lookup — negates the index advantage for the CLI path specifically.
- **B3**: no FFI code exists to have a bottleneck in. The design constraints are already correctly
  written down in `.opencode/rules/python-bindings.md` (mandates `allow_threads` around blocking
  calls, typed exceptions not stringified) — nothing violates them because nothing implements
  them yet.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| R1 | Make segment loading lazy in `Dataset::open` — keep manifest metadata, defer `fs::read`+`from_bytes` to first `vector_search`. | High | Medium |
| R2 | `mmap` segment files instead of `fs::read`+copy. | High | Medium-Large |
| R3 | CLI `search`: use zone-map/predicate pruning on the resolved row-ids instead of a full scan. | High (CLI only) | Small-Medium |
| R4 | When bindings land: numpy/buffer-protocol for vectors, native Python types for results (never JSON), `py.allow_threads` around `vector_search`/`scan`/`commit`. | High (future) | Medium |
| R6 | Add a `Dataset::open`-at-N-segments benchmark — none exists today, so R1/R2 have no baseline. | Medium | Small |

### Invariant flags
None. `allow_threads` releases the GIL around a synchronous call — it is not async and does not
touch the sync-production-code decision.

---

## §B — Query execution (scan, predicate mask, group-by)

**Files:** `crates/query`. Existing benchmark: `group_by_bench.rs`.

### Current state
`mask` already fully delegates to `arrow::compute`'s vectorized comparison kernels — there is no
row-by-row reimplementation to fix here. `group_by` is **half-converted**: its state is columnar,
but the driver loop still does per-row dynamic dispatch through `dyn Array`/`AggFunc` matching.
Hash map is plain `std::collections::HashMap` (SipHash); `foldhash`/`ahash` are already in the
dependency tree but unused for this. Recorded numbers (1M rows) show a **regression** at low
cardinality (82.6ms → 95.9ms) alongside wins elsewhere (1.24s → 531ms at 1M rows) — consistent
with hashing cost dominating at low group counts.

### Bottleneck root causes
- SipHash on every row even where hashing dominates the cost.
- `RowConverter::convert_columns` materializes a full extra encode+copy of the whole batch before
  aggregation starts.
- Per-row enum dispatch blocks autovectorization of Sum/Min/Max entirely.
- Redundant casts: aggregating the same column with 3 functions casts it 3 times.
- `scan_with_predicate` casts the *entire* batch to the target schema before filtering, so cast
  cost scales with rows scanned, not rows matched.
- **Cross-reference with §I**: `datafile.rs`'s doc comment claims column projection saves ~204MB
  vs ~1.6MB; this is directly contradicted by the measurement in `snapshot.rs` (~2ms of ~109ms).
  One of these is simply wrong — see §I's fix (the comment is stale; §I recommends correcting it).

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| R1 | Swap the default hasher for `foldhash`/`ahash` in `group_by`. | High | Small |
| R2 | Two-phase aggregation: build a `group_idx` vector once, then one monomorphized loop per aggregate over native-typed slices. | High | Medium |
| R3 | Dedup casts across aggregates sharing a column. | Medium | Small |
| R4 | `scan_with_predicate`: mask the raw batch, filter, *then* cast survivors only. | Medium | Small |
| R5 | Short-circuit `And` when the left mask's match count is 0; order predicate leaves cheapest-first. | Medium | Small |
| R6 | Add a predicate/scan benchmark — none exists (`mask`/`should_scan_file`/`scan_with_predicate` are entirely unmeasured today). | Medium | Medium |

### Invariant flags
None. No recommendation adds a SQL parser, serializability, or cross-node work.

**Correction applied during synthesis**: the original §B report claimed a `PredicateKey`
identity-cache mechanism already exists on this branch's `snapshot.rs`. Direct verification
(`git ls-tree`, `grep -n PredicateKey`) at the exact commit (`6f15aea`) found neither the type nor
any reference to it. §F's independent investigation reached the same conclusion. This claim is
removed; see §F for the real state of caching on this branch.

---

## §C — HNSW search (read path)

**Files:** `crates/index/src/{graph,node,node_table,slot_array,hnsw,segment_reader}.rs`,
`segment_set.rs`'s `fan_out`.

### Current state
Confirms the session's earlier finding structurally: fan-out is sequential
(`segment_set.rs:174-206`), every segment searched at the same unmodified `ef_search=32`, zero
threading dependency in `crates/index`. New finding: segments are fully RAM-resident, not mmap'd
— segment count drives RSS directly, not just latency.

### Bottleneck root causes (new, beyond what was already known)
- **Saturation bookkeeping dominates the inner loop.** Every popped candidate clears and refills
  a `HashSet<u64>` (SipHash) with all `ef` result ids and intersects it — at ef=32, ~64-96 SipHash
  ops per pop, comparable cost to the actual 512-dim distance evaluation. `saturate=true`
  unconditionally on both descent and layer-0. Not previously identified.
- **Correction to the session's earlier SlotArray finding**: on x86-64, `SeqCst` *loads* compile
  to a plain `mov` — identical to `Relaxed`. The 264-byte-per-node scan cost is pure memory
  traffic, not fence overhead. Weakening the ordering is a correctness cleanup, not a performance
  fix on this architecture (it only helps aarch64). **The actual fix for the scan cost is
  bounding the scan itself** — see §J's R3 (an occupancy high-water mark), which is the
  correct fix this section's own investigation converged on independently.
- Wasted `sqrt`: `anndists` returns a square-rooted norm; Strata immediately squares it back.
- Segment count growth is real and compounds: fan-out pays an `ef` floor *per segment*
  regardless of segment size, so total traversal work grows roughly linearly with segment count
  even at fixed total row count.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| H1 | Parallel fan-out via `std::thread::scope` + a shared atomic cursor (no new dependency — `SEARCH_SCRATCH` is already thread-local). Don't spawn one thread per segment; spawn `min(segments, cores)` workers pulling from a cursor. Estimated ~4-4.5x at 5 segments/8 cores. | High | Small |
| H2 | Drop or gate saturation checking on the segment path, or replace the two `HashSet`s with a per-search `Vec<u32>` visited-epoch buffer sized to the segment's known node count. | High | Small |
| M1 | Per-segment `ef` scaled to segment size relative to total, with a `max(k, ...)` floor to protect recall — gate behind `segment_recall_bench`. | Medium | Medium |
| M2 | Segment compaction (see §E) — the only real fix for unbounded fan-out growth. | Medium | Large |
| L1 | Relax slot-array ordering to `Relaxed`/`Acquire` — correctness cleanup, aarch64-only benefit. Needs the loom proof designed in §J first. | Low | Small |
| L2 | Add a squared-L2 metric variant to skip the wasted `sqrt`. | Low | Small |

### GPU/SIMD verdict
`anndists` genuinely hits AVX2 at runtime (confirmed via `is_x86_feature_detected!`) — not
silently falling back to scalar. **GPU: reject**, consistent with §K. HNSW's per-query work is
~1.5M FLOPs / under 100µs; PCIe round-trip alone is 5-20µs with no intra-query parallelism to
amortize it against, and HNSW's traversal is inherently serial-dependent. GPU only wins for
batched brute-force at ≥1M vectors with concurrent queries — a different product, worth an
explicit ADR rejection rather than leaving it ambiguous.

### Invariant flags
**FLAGGED**: batching commits to reduce segment count (the "obvious" fix for growing segment
count) would be silent write-buffering — `segment_set.rs` itself already forbids this. Compaction
(M2) is the sanctioned fix.

---

## §D — HNSW construction (write path)

**Files:** `crates/index/src/graph.rs` (insert, neighbor pruning), `node_table.rs`, `node.rs`,
`crates/txn/src/dataset.rs`'s `build_and_write_segment`.

### Current state
Segment build is a plain sequential loop. Params: M=16, max_layer=16, ef_construction=100 (halved
from 200 in an earlier perf pass, trading recall 0.9855→0.9820 at 100k for 2.2x build speed).
Alpha is hardcoded to 1.0 (plain Algorithm 4, no RobustPrune relaxation). SIMD is live for
construction too (same `anndists` path as search) — no gap there.

**Correction to the section brief's premise**: segment builds happen *before* the commit lock is
acquired, so cross-segment parallelism (concurrent transactions' builds overlapping) is already
possible — the actual gap is *within*-segment parallel insert, which the 2.51x-proven `3697ba8`
commit provides on the old branch but was never ported here.

### Bottleneck root causes
- Sequential within-segment insert — `insert_batch` (a documented "thin sequential loop") exists
  but isn't even used by the segment builder.
- **The shrink/pruning step, not initial neighbor selection, dominates cost** — roughly 70-80% of
  insert time, ~8000 distance evaluations per row at layer 0, with fresh `Vec` allocations and an
  O(n²) `contains`-based filter on every call.
- Double allocation per node: one packed block plus one separate `Box::into_raw` per node in
  `NodeTable::insert` — a faster path (`insert_ptr`) already exists but is marked dead code,
  reserved for a future arena.
- `NodeTable::new` ignores the caller's known-upfront `expected_capacity` and always allocates a
  fixed-size directory regardless of segment size.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| D1 | Port `insert_batch_parallel` to the segment-build loop — **simpler than the original**, since the segment's graph is thread-private until publish, so the panic-safety/residue-guard machinery that made up most of the original commit isn't needed (a failed build just discards the graph). | High (2.5x precedent) | Medium |
| D2 | Hoist the shrink step's per-call allocations into reusable scratch buffers; replace the O(n²) `contains` filter with a bitset/sorted probe. | High | Small |
| D3 | Memoize distance computation within one shrink call instead of re-fetching both nodes per pair. | Medium-High | Small |
| D4 | Wire the existing (currently dead-code) `insert_ptr` fast path plus a segment-sized arena, using the segment's known row count upfront. | Medium | Medium |
| D5 | Expose alpha as a tunable, bench {1.0, 1.2} — note this trades recall for *more* build cost (keeps more edges), it's orthogonal to `ef_construction`. | Low-Medium | Small |

### Invariant flags
No violation. D1 keeps the build fully inside `write_phase`, before durability/conflict-check/
publish — nothing here changes when a write is acknowledged. **FLAGGED**: any framing of D1 as
"background/deferred segment build" would be silent write-buffering — reject that framing
specifically if it comes up during implementation.

---

## §E — Segmented index infrastructure (lifecycle, compaction, format)

**Files:** `crates/index/src/{segment_reader,segment_set}.rs`, `crates/storage/src/stats.rs`,
`crates/txn/src/dataset.rs`'s `load_segments`/`build_and_write_segment`.

### Current state
Recovery's 414x win (26.55s→64ms) is real but measured in the friendliest regime — 5 large
segments. Per-segment fixed costs (one open+read, one full copy, two CRC passes) are amortized
over large files there; a per-transaction workload (many small commits) would see those fixed
costs dominate instead. No compaction/GC exists at all; segment count grows with commit count
forever.

### Bottleneck root causes
- Double copy on load: `fs::read` into a `Vec`, then a second full copy into aligned memory for
  `SegmentReader`. Segments are permanently RAM-resident (never evicted) once loaded.
- `load_segments` is strictly serial across an arbitrary number of independent, individually-
  validated files.
- The manifest's per-commit `SegmentEntry.zone_map` duplicates stats already present in
  `DataFileEntry.stats` — roughly doubling the redundant bytes rewritten every commit (compounds
  §H's finding).
- Zone-map string bounds are stored with **no truncation** — a wide text column would bloat every
  `SegmentEntry` and get rewritten in full every commit.
- Orphaned `.seg` files from failed commits are deliberately never cleaned up (for crash safety)
  but there is no GC path for them either — permanent leak of whole segment files, distinct from
  §J's in-memory leak.
- `from_bytes`'s "fails closed, no panic" claim is asserted, not fuzzed (see top findings #3).

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| — | Two fuzz targets for `SegmentReader::from_bytes` (raw-bytes and header-patched-then-adversarial-body variants) — concrete sketches available, seed corpus from real `.seg` files. | High | Medium |
| — | Parallelize `load_segments` (`into_par_iter`, validation is pure, manifest order preserved via indexed collect). | High | Medium |
| — | **Segment compaction, done honestly**: naive concatenation of two segments' graphs is *silently wrong* — it produces a disconnected graph (no edges between the two original node sets), strictly worse than today's fan-out, not a fix. The sound alternative is seeded-merge — keep the larger source graph, re-insert the smaller source's vectors through the normal insert path (this is how Lucene merges HNSW segments). Cost scales with the *smaller* side only. Policy should be size-tiered (not leveled — vectors have no ordering key, so leveled compaction's read-amplification argument doesn't apply here; the only payoff is bounding segment count). | High | Large |
| — | Truncate string zone-map bounds (e.g. 32 chars, truncating conservatively). | Medium | Small |
| — | Read directly into the aligned allocation (`read_exact`) instead of `fs::read` + copy. | Medium | Small |
| — | mmap segments — real, but smaller win than it sounds: the body CRC check faults in the whole file at open regardless, so the load-time win is the memcpy only; the real benefit is evictable steady-state RSS, not open latency. | Low | Medium |
| — | Deduplicate `DataFileEntry.stats` vs `SegmentEntry.zone_map`. | Low | Small |

### On bloom filters / HyperLogLog as a zone-map alternative
Mostly negative, and important to state honestly rather than ship something that doesn't help:
a bloom filter helps `col = v` on high-cardinality, value-clustered columns, but does **nothing**
for a predicate like this session's benchmark case (`category = id % 10`) — every segment
genuinely contains every value, so no per-segment summary can prune it. HyperLogLog has no
membership test at all and can't prune anything — it's a planner input, not a zone map. **The
only real fix for a genuinely-unprunable predicate is compaction-time clustering** (repartition
by that column so the predicate becomes segment-correlated) or reducing per-segment scan cost
directly (§C). Also: kilobyte-scale summaries must not go in the manifest's `SegmentEntry` — the
manifest is fully rewritten every commit (§H), so any such summary belongs in the segment file
itself or a sidecar, never in the JSON manifest.

### Invariant flags
**FLAGGED**: naive graph concatenation during compaction is a correctness trap, not proposed.
**FLAGGED**: compaction must publish through the normal commit path (manifest CAS, re-validating
source segments are still current) — a background thread building merged bytes is fine (pure
CPU), but swapping the live index outside the commit path would violate the "index mutations stay
inside the transaction layer" rule.

---

## §F — Snapshot / random-access read layer

**Files:** `crates/txn/src/snapshot.rs` (both `main` and S1, compared directly).

### Current state
A row is 2080 bytes; the embedding vector is 2048 of them — **98.46% of every row is vector
data**, and resolving a predicate match needs only 16 bytes of it (0.77%) — a 130x read
amplification, precisely matching the measured ~105ms/~205MB cost at 100k rows.

### Bottleneck root causes
- **Porting `main`'s resolved-row-id cache to S1 is not a mechanical port — it's a real design
  problem.** S1 added compound `And`/`Or` predicates that `main`'s cache-key type doesn't support
  at all; a naive port would make structurally-different predicates collide in the cache and
  **silently return the wrong cached result** — e.g. `category=1 AND amount>5` colliding with
  `category=1 AND amount>9`. This needs a recursive cache-key design, not a copy-paste.
- `main`'s `LiveSet` type (what the cache actually stores) doesn't exist in S1's `crates/index`
  at all — the port spans three crates, not one.
- Good news: zone-map segment pruning does **not** require the cache key to account for which
  segments exist — `row_ids_matching` only ever looks at row-data files, never segments, so
  `(Snapshot, Predicate) → live_ids` stays a pure function keyed on the predicate alone.
- The cache's memory budget is sized by byte count, but bitset size grows with max row-id forever
  (row-ids are never reused) — the budget doesn't actually track query selectivity.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| F1 | **Split scalar/vector columns into separate files** — the scalar file becomes ~32 bytes/row instead of 2080, so `row_ids_matching` reads ~1.6MB instead of ~208MB at 100k rows (130x). This is strictly the larger fix vs. §I's multi-block alternative, and would make porting the cache back nearly optional. | High | Medium |
| F2 | Port the resolved-row-id cache — but design the recursive predicate key *and* its And/Or non-collision test **first**, before writing any caching logic. | High | Medium |
| F3 | Switch the cached live-id representation to a Roaring bitmap for selectivity-proportional sizing — do this after F1/F2, not before; the merge-across-segments path doesn't currently exercise set operations that would benefit further. | Medium | Medium |
| F4 | Bloom filters ahead of the file read, for high-cardinality equality predicates specifically — honest caveat: zero win on this session's benchmark predicate (see §E), useful only for point lookups. | Low (this workload) / High (point lookups) | Medium |

### Invariant flags
**FLAGGED**: splitting into two files means commit must fsync both, still as a single durability
point — acking after only the scalar file is written would be silent write-buffering. Not
proposed, but flagged since the refactor makes it a tempting shortcut.
**FLAGGED**: the cache is sound only because a `Snapshot` is immutable — never add incremental
invalidation on commit, and never share a cache instance across snapshots.

**Headline point worth restating**: the "24x regression" is largely a stale-fork artifact, but the
*underlying* 130x read amplification is real on both branches — `main`'s cache just hides it for
repeated identical predicates. F1 is the fix that helps regardless of query pattern.

---

## §G — Transaction & OCC conflict resolution

**Files:** `crates/txn/src/dataset.rs` (`Transaction::commit`), `commit_log.rs`.

### Current state
**The originally-seeded O(n·E) conflict-check finding is stale — it was already fixed** three
commits after the audit that found it (binary-search range start, hashed membership above a
measured threshold, `HashSet` dedup). Current cost is O(W + log E + R), not O(n·E). Measured: 57µs
clean at write-set size 1, 611µs at write-set size 10,000 — and `lifecycle_bench`'s
concurrent-commit numbers (65-116 commits/sec) reflect an insert-only workload where the conflict
check short-circuits in O(1); they say nothing about conflict-check cost under real contention.

### Bottleneck root causes
- **One global mutex covers the entire commit**, not just fsync — confirmed by reading the actual
  lock scope: conflict-check, manifest clone, dimension check, allocator read, manifest commit
  (2 fsyncs + full JSON serialize), claim release, and the log push are *all* serialized under one
  lock, regardless of whether two transactions touch disjoint rows. Only the data/segment file
  write happens outside it, correctly.
- Residual conflict-check cost (57µs) is currently invisible next to the ~12ms fsync floor — but
  becomes the bottleneck the moment §H's manifest cost is fixed.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| 1 | Implement group commit per ADR 0006 — the only real fix for the fsync floor. | High | Large |
| 2 | Invert the commit log to a `row_id → last_version` hash map instead of a range-scanned ring buffer — write-sets are individual tombstoned row-ids from arbitrary predicates, not contiguous ranges, so this fits better than an interval tree. Reduces the check to O(W) probes independent of log size. | Medium | Small |
| 3 | Move the manifest clone + JSON serialize outside the commit lock where possible. | Medium | Medium |
| — | Lock striping by row-id range: **not worth it** — the manifest CAS and version counter are inherently single-writer regardless, so striping would only relieve the already-negligible conflict-check cost. | Low | — |

A concrete group-commit design was worked out respecting the durability invariant precisely: only
the fsync *syscall* may be batched across concurrently-arriving commits (one thread becomes leader,
merges queued manifests, does one fsync, wakes all waiters) — a commit's `Ok(())` must still not
return until the fsync covering its own version has completed. A specific reordering hazard was
flagged: the version-visible-swap and claim-release must happen *after* the batched fsync
completes, not before, or an unfsynced write becomes visible — exactly the kind of subtle bug an
implementer could introduce by accident while wiring this up.

### Invariant flags
**FLAGGED** (from ADR 0006 itself): "fsync every N commits" or any variant that acknowledges before
the covering fsync completes would violate the durability invariant. Group commit is only legal
as a shared syscall, never a deferred acknowledgement.

---

## §H — Manifest / version layer

**Files:** `crates/storage/src/manifest.rs`, the CAS commit protocol in `dataset.rs`.

### Current state
The prior complexity audit's O(v²) curve (12.2ms → 39.5ms, 300 → 6000 files) was measured with an
**id-only schema — no vector column, so no `SegmentEntry`s were ever written.** On a real vector
workload, `SegmentEntry` (carrying its own full zone-map) is written every commit too, and is
larger per-entry than the measured `DataFileEntry` — real bytes-per-commit are at least 2x the
measured curve, and the point where growth becomes clearly super-linear likely moves down to
roughly 1000-1500 commits, not 2000-3000.

### Bottleneck root cause (the mechanism, not just the symptom)
Confirmed precisely: every commit does (a) a deep clone of the *entire* accumulated manifest
(every historical `DataFileEntry` and `SegmentEntry`, each carrying a stats/zone-map hash map) to
append one or two new entries, (b) a full `serde_json` serialization of that entire cloned
structure, and (c) a full rewrite + fsync + atomic rename of the whole file — all three happen
*inside* the commit lock. A single commit at file count v costs O(v); summed over v commits, that
is the O(v²) total. Rough attribution: ~15-20% of the per-commit cost is the clone+serialize
(CPU), ~80% is the actual write+fsync (I/O) — meaning a smaller serialization format helps mainly
by shrinking bytes written, not by saving CPU.

**Second, previously unmeasured finding**: nothing anywhere garbage-collects old `_versions/*`
manifest files. By commit 6000, the directory holds ~2.6 GB of historical manifests for what
should be a trivial dataset.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| 1 | Append-only delta log + periodic off-commit-path checkpoints (Delta Lake/Iceberg-style): write only this commit's new entries as a small delta file; the CAS itself is unchanged in shape (still one atomic write+fsync+rename of the *highest-numbered* file — just a small delta, not the whole state); checkpoint the full flattened manifest periodically, off the commit path, never as the publish point itself. | High | Large |
| 2 | Persistent/structural-sharing collections (e.g. `imbl::Vector`) for the manifest's lists, turning the O(v) clone into O(log v) — needed even after #1, since the in-memory `Snapshot` still holds a flat list. | Medium | Medium |
| 3 | The in-memory `SegmentSet` append path has the identical shape (self-documented O(parts²)/session) — fix alongside #2 with the same persistent-collection approach; #1 does not touch this. | Medium | Small |
| 4 | GC old manifest version files past a retention window. | Medium | Small-Medium |
| 5 | Binary serialization format instead of JSON — do this *after* #1, which reduces bytes written far more than format choice alone would. | Medium | Medium |
| 6 | Hoist the per-commit `create_dir_all` call (currently runs every commit unconditionally). | Low | Small |

### Cross-section note
**This is the highest-leverage single fix identified in this whole audit for the transaction
layer specifically** — §G's own conflict-check optimizations are already fast enough (57µs) to be
invisible next to today's ~12-39ms manifest cost; fixing §H is the prerequisite for §G's
recommendations to matter at all.

### Invariant flags
**FLAGGED**: writing the manifest only every Nth commit (the "obvious" cheap version of batching)
violates both the no-silent-write-buffering and single-atomic-CAS invariants — reject. **FLAGGED**:
a checkpoint must never become the CAS target itself, or a second publish point gets introduced;
checkpoints must stay purely a discardable read-optimization.

---

## §I — Columnar storage format (Arrow IPC)

**Files:** `crates/storage/src/datafile.rs`, `stats.rs`.

### Live risk (independent of performance)
Confirmed: the arrow-ipc panic guard added earlier this session (`catch_unwind`/`CorruptDataFile`)
does not exist anywhere in this file on the S1 branch. Both `FileReader::try_new` call sites can
still abort the whole process on a malformed schema — the exact bug already fixed on `main` and
reported upstream. **Cherry-pick before merge.**

### Current state
Nothing is compressed on disk (default `IpcWriteOptions`, `batch_compression_type: None`). Exactly
one `RecordBatch` per file, always — no multi-block usage despite the format supporting it.

### Root causes
- **Confirmed against arrow-ipc's actual source**: the whole-body-read-regardless-of-projection
  behavior is an *implementation* limitation of the `arrow-ipc` crate, not a fundamental property
  of the IPC format — the format's own flatbuffer schema carries explicit (offset, length) pairs
  per buffer that a custom reader *could* use for targeted byte-range reads. `FileReader` simply
  doesn't do this; it reads the full message body up front, then applies projection only during
  in-memory array construction.
- **A doc-comment in this file is actively wrong** and should be corrected regardless of any other
  change — it claims projection saves ~204MB vs ~1.6MB, directly contradicted by the actual
  measurement elsewhere in the codebase (~2ms of ~109ms saved).
- Zone maps are per-file only (one `RecordBatch` per file) — no sub-file pruning granularity
  exists.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| 1 | Cherry-pick the panic guard from `main`. | High (correctness) | Small |
| 2 | **Write multiple blocks per IPC file** instead of one (e.g. one block per few-thousand rows). Arrow IPC's own reader already supports seeking to and reading individual blocks — this yields genuine byte-range reads *inside the existing format*, plus per-block zone maps, and is fully append-compatible. This is a smaller, more incremental alternative to §F's file-split proposal that captures most of the same win. | High | Medium |
| 3 | Fix the false projection-savings claim in the doc comment. | Medium (prevents future wrong decisions) | Small |
| 4 | A custom byte-range reader using the flatbuffer buffer-offset metadata directly — real, but means owning IPC decoding indefinitely; only pursue if #2 turns out insufficient. | High | Large |
| 5 | Per-block stats (count, null-count) alongside #2. | Medium | Medium |
| 6 | mmap: marginal, deprioritize — saves one memcpy (~30ms of the ~105ms cost) but arrow-ipc's reader requires `Read + Seek`, and the CRC/decode step would still fault in the whole file regardless. | Low | Medium |
| 7 | Compression: skip for the embedding column (near-incompressible, and decompression is slower than the page-cache read it would replace — a net loss); reconsider for scalar columns only after #2 makes selective per-chunk compression possible. | Low | Medium |

### On Parquet as an alternative
Genuinely delivers column-chunked reads, but is a bigger redesign than it sounds — Parquet writes
its footer last with row-group metadata accumulated in memory across a stateful multi-batch
writer object, which changes the durability/fsync story the current per-batch `write_batch` model
depends on. Recommendation #2 gets most of the practical win at a fraction of the migration cost;
revisit Parquet only if #2 measurably falls short.

### On the panic-guard's own overhead
Confirmed negligible: Rust's unwinding is table-driven (SEH on Windows/MSVC, DWARF elsewhere) —
nothing is pushed or registered at runtime on the non-panic path; the only cost is one non-inlined
call boundary, paid once per file open, immaterial next to a 205MB read.

### Invariant flags
**FLAGGED**: multi-block files (#2) must not become a vehicle for write-buffering — each commit's
own file must still be fully fsynced before that commit returns; multi-block means chunking *one
commit's own rows* within its own file, never accumulating rows across separate commits before a
flush. **FLAGGED**: Parquet or any production-row-format swap is a Phase-1 design-level decision,
not a tuning knob — treat as architectural if ever revisited.

---

## §J — Lock-free concurrency primitives

**Files:** `crates/index/src/{node_table,slot_array,node,graph}.rs`.

### The leak (see Top Findings #1)
`NodeTable`/`Node` have no `Drop` implementation anywhere — deliberate under the old design, where
a graph lived for the whole process lifetime. The segmented rewrite invalidated that assumption:
one `HnswIndex` is now built and dropped *per commit*, so every commit leaks a full 512 KiB
`Chunk` plus every node block, unboundedly, for the life of the process. **Fix needs no `loom`
proof** — a plain `Drop` has exclusive access (`&mut self`) by construction, so there's no
concurrency reasoning required, only a leak test and Miri.

### Ordering: conservative-by-default, not provably necessary
Every atomic in `slot_array.rs`/`node_table.rs` is `SeqCst`, copied verbatim from the original
design sketch with no ordering-justifying comment anywhere in the codebase — contrasted with a
genuinely reasoned ordering decision elsewhere in `graph.rs` that *is* documented at length. Three
independent reasons this is over-strong:
- Slot values are self-contained row-ids carrying no dependent data that needs to be published
  through them.
- The structure's own documentation already states results are "not a true atomic snapshot across
  slots" — so `SeqCst`'s cross-slot total order is paid for but never consumed by any caller.
- The production query path never touches these atomics concurrently at all today — segment
  builds are sequential, and the read/search path operates on plain immutable slices with zero
  atomics.

**Important honest caveat**: on x86-64, this ordering change would cost nothing today (SeqCst and
Relaxed loads emit the same instruction) — the real benefit is aarch64, and preventing future
misuse. **It does not fix §C's scan-cost finding** — that requires bounding the scan itself (see
below), not weakening its ordering.

Two specific loom experiments were designed (not just "add a loom test" vaguely) to prove a weaker
ordering safe before any change ships — both are ready to implement directly, described in the
full report.

### Cache-line layout
Real false sharing identified: a per-node "deleted" flag (written on tombstone) sits in the same
64-byte cache line as fields read on every hot-path vector/level access — one tombstone write
invalidates that line for every concurrent reader. No `CachePadded` or alignment is used anywhere
in the workspace despite `crossbeam-utils` already being in the dependency tree transitively.

### Reclamation
`crossbeam-epoch` is present only as an unrelated transitive dependency (pulled in by
`rayon-core`/`crossbeam-deque`, nothing to do with `crates/index`). It is **not the right tool
here** — nothing is ever removed while readers run, so there's no concurrent-reclaim hazard to
solve; adopting it would add real dependency weight, per-access pinning cost, and substantial loom
proof work for a problem that doesn't exist. The actual gap is simpler and more severe: there's no
reclamation at all, hence the leak above.

### Recommendations
| # | Recommendation | Impact | Cost |
|---|---|---|---|
| R1 | Implement `Drop` for `NodeTable`, freeing chunks and node blocks. | **High** | **Small** — no loom needed, verify with Miri + a leak test |
| R3 | Bound the occupancy scan with a per-layer high-water mark instead of scanning every slot — **this, not ordering, is the actual fix for §C's per-node scan cost.** | High | Medium — needs its own loom model, and must avoid reintroducing the two-atomic torn-pair bug an earlier fix in this same codebase already had to correct |
| R4 | Move the "deleted" flag out of the hot read-mostly cache line. | Medium | Small — layout-only, no loom needed |
| R2 | Relax slot-array/node-table ordering to the minimum provably-sufficient level, per the two designed loom experiments. | Medium (aarch64), ~zero (x86-64) | Medium — do not ship without the loom proofs landing first |
| R5 | `CachePadded` on the node-table chunk directory. | Low today (build is sequential) | Medium — defer until concurrent insert is real |

### Invariant flags
None of the above introduces a lock — R1's `Drop` takes `&mut self`, i.e. exclusive access by
construction, adding no synchronization to any concurrent path. **FLAGGED**: the tempting
"simpler" leak fixes — wrapping the table in a `Mutex`, or switching to `Arc`-based refcounted
node storage — would abandon the project's stated lock-freedom requirement, and `Arc`'s refcount
traffic would reintroduce the exact cache-line contention this section is trying to remove, while
still not fixing the chunk-level leak. Do neither.

---

## §K — Cross-cutting (allocator, NUMA, GPU, async)

### Current state
No custom global allocator anywhere in production code — only the benchmark harnesses' own
counting wrappers, which delegate to the platform default (Windows/MSVC's heap, not a strong
choice for HNSW's many-small-allocation pattern). No release-profile tuning exists either (no
LTO, default codegen-units, no target-cpu). The single-allocation node layout already eliminated
the *worst* version of the small-alloc problem; what remains is concentrated in a handful of
short-lived `Vec`s in the neighbor-shrink loop (cross-references §D's finding directly) and one
double-hold of segment bytes at both commit and open time (cross-references §E's double-copy
finding).

### Findings, per question
1. **Allocator**: worth benchmarking, but expectations should be moderated — the strongest version
   of the argument was already addressed by the existing node layout. Realistic expectation is
   5-15% on ingest wall-clock, not a multiple. Not a clean one-liner either: only one global
   allocator may exist per binary, so the benchmark harnesses' own counting wrappers need
   rewiring to delegate to the new allocator instead of `System`, or memory benchmarks silently
   stop working.
2. **NUMA: no.** The project's stated target (single-node, embedded, laptop/container/edge scale)
   never reaches the point where cross-socket memory placement matters, and independently, the
   write path is single-threaded inside one global lock today anyway — the prerequisite for NUMA
   to matter doesn't exist.
3. **GPU: no, across all four workloads asked about, for four different specific reasons** —
   construction is sequentially-dependent pointer-chasing that would require becoming a different
   index entirely (violating HNSW-only); single-query search is far too small to amortize PCIe
   transfer against, and SIMD already captures the realistic win at this scale; predicate/scan and
   group-by are both memory-bandwidth-bound, where PCIe is *slower* than host RAM, making GPU
   strictly negative, not just unhelpful. Consistent with §C's independent conclusion. A CUDA/ROCm
   dependency would also directly contradict the project's positioning as a simple, embeddable
   engine.
4. **FFI/GIL**: the right guardrail (`allow_threads` around blocking calls) is already correctly
   documented in project rules — there's no code yet to have gotten it wrong, so this is an
   enforcement item for whenever the real API lands, not an optimization to make today. Confirmed
   this does not touch the sync-production-code invariant.
5. **The one genuine I/O-wait finding in the whole audit**: `load_segments` reads N segment files
   sequentially and blocking at `Dataset::open` — real I/O wait, but **the fix is threads
   (`std::thread::scope`), not async.** This does not reopen the async-vs-sync question elsewhere
   in the system.
6. New mechanical finding: segment bytes are held in memory twice at both commit time and open
   time (a `Vec` is built, then immediately copied again into aligned storage) — a direct,
   measurable contributor to peak memory, independent of the allocator question.

### Recommendations
| # | Action | Impact | Cost |
|---|---|---|---|
| 1 | `[profile.release] lto = "thin", codegen-units = 1` | Medium | Small |
| 2 | Extend existing scratch reuse to the neighbor-shrink loop's remaining allocations (cross-references §D) | Medium-High | Small |
| 3 | Emit aligned bytes directly at segment-build time instead of copying twice (cross-references §E) | Medium | Medium |
| 4 | Benchmark mimalloc — after fixing the benchmark harnesses' allocator wiring | Medium | Small |
| 5 | Parallelize `load_segments` with `std::thread::scope` | Medium (open latency) | Small |
| 6 | Enforce `allow_threads` when the real PyO3 API is written | High (later) | Small |

### Recommending explicitly against
GPU for all four workloads asked about; NUMA tuning; treating an allocator swap as a free
one-liner (it isn't, quite); async anywhere, including at the FFI boundary (GIL release is not
async, and the one real I/O-wait case has a threads fix, not an async one).

---

## Appendix: how sections interact

A few dependencies worth keeping in mind when prioritizing actual implementation work, since
several sections' recommendations only pay off once another section's fix lands first:

- **§G's conflict-check optimizations are currently invisible and will stay that way until §H's
  manifest-rewrite cost is fixed** — at scale, manifest work outweighs conflict-checking by two to
  three orders of magnitude.
- **§J's leak exists *because of* §E's per-commit segment lifecycle** — the old "never free"
  design was sound when a graph lived for the process's whole life; it became a bug the moment the
  architecture started building-and-dropping one per commit. Any future architectural change with
  a similar "object built and discarded per operation" shape should re-check this class of
  assumption specifically.
- **§C's per-segment scan-cost finding and §J's ordering investigation converged on the same
  answer independently**: the fix is bounding what gets scanned (an occupancy high-water mark),
  not weakening atomic ordering — worth noting that two independent investigations agreeing on
  this is a meaningfully stronger signal than either alone.
- **§F and §I proposed two different fixes for the same root cause** (whole-row-body reads) —
  §F's file-split is the fuller fix, §I's multi-block-IPC is the smaller incremental step that
  captures most of the win within the existing format. Worth prototyping §I's version first given
  its much lower cost, and only reaching for §F's larger change if it proves insufficient.
- **§A, §E, and §K each independently found the same double-copy-of-segment-bytes pattern** from
  three different angles (CLI startup cost, segment lifecycle, and cross-cutting memory
  respectively) — three independent confirmations of the same fix (read directly into aligned
  memory instead of copying twice).
