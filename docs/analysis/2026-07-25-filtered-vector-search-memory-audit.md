# Filtered Vector Search Memory Audit

> Date: 2026-07-25 · Opus-tier audit (design decision run through an LLM council, code-verified
> against source) of Strata's #1 remaining performance bottleneck: filtered vector search's memory
> cost. Follows the format of
> [`2026-07-24-ingest-recovery-performance-audit.md`](2026-07-24-ingest-recovery-performance-audit.md)
> (that doc lives on the S1 branch line; this one is on `main`/`perf/*`).

## 1. Measured baseline

`bench/benches/lifecycle_bench.rs`, 25k rows × 512-dim real OpenAI embeddings, `category = id % 10`
(10 distinct values), one live `Snapshot` for the whole run:

| Phase | Wall | Alloc'd | Peak live | Nature |
|---|---|---|---|---|
| vector search, unfiltered | 22.84 ms | 0.0 MB | 0.0 MB | CPU (graph traversal + SIMD dist) |
| vector search, filtered, **same predicate** ×50 | 3.90 s (78.00 ms/query) | 2559.5 MB | 10.2 MB | I/O-bound (row-id resolve) + CPU |
| vector search, filtered, **varying predicate** ×50 (cycled 10 categories at this measurement) | 4.49 s (89.72 ms/query) | 2559.4 MB | 10.2 MB | I/O-bound (row-id resolve) + CPU |

Filtered search is ~1,700–2,000× the wall time of unfiltered and is the single largest allocation
source in the whole lifecycle — ahead of ingest+commit (765.0 MB / 5 commits) and recovery
(384.4 MB). The **varying-predicate row is new in this audit** (added to `lifecycle_bench` here,
`bench/benches/lifecycle_bench.rs` Phase 7b) — it exists to answer a question the council raised:
does a fix that only helps *repeated identical* predicates just optimize the benchmark's own
degenerate shape? Pre-fix, the two numbers are statistically indistinguishable (as expected — nothing
caches anything yet), so this row is the honest cold-path baseline a post-fix run must be compared
against, not a finding on its own.

> **Note on this row's category count:** at this initial measurement, Phase 7b cycled all 10
> categories. §8's review round found this double-counted one of the 10 "cold" misses as a leftover
> cache hit from Phase 7 (which queries `category = 3` against the same snapshot first), and corrected
> the phase to cycle the 9 categories disjoint from Phase 7's. §7's post-fix numbers are measured
> against the corrected 9-category version — the current `bench/benches/lifecycle_bench.rs` cycles 9,
> not 10, which is why this row's count no longer matches the file if you go look.

## 2. Root cause — confirmed against source, not re-derived

`Snapshot::vector_search` (`crates/txn/src/snapshot.rs:249`), when given a predicate, calls
`row_ids_matching` to resolve live row-ids, then `HnswIndex::search_filtered`. Two costs compound,
confirmed by reading both call sites directly (not just the existing code comments, which already
named the first):

1. **`row_ids_matching` re-reads every surviving file's whole Arrow IPC body, every call.**
   `read_surviving_files` → `read_batch_columns` (`crates/storage/src/datafile.rs:99`) opens an
   `arrow::ipc::reader::FileReader` with a column projection, but arrow-rs's `FileReader` always reads
   a record batch's whole contiguous message body off disk before decoding — projection skips array
   *construction*, not the *read*. At 25k rows × 512-dim, that's the full ~51 MB/file, every query,
   regardless of which columns are actually needed. This is already documented at the call site
   (`snapshot.rs:279-296`) from a prior measurement session — this audit confirmed it still holds and
   quantified it at the current row count.
2. **`HnswIndex::search_filtered` rebuilds a dense row-id bitset from scratch on every call**
   (at the time of this finding: `crates/index/src/hnsw.rs:665-669`, `let mut live = vec![0_u64; max_id/64+1]`
   then a loop over `live_ids`; that bitset-building logic has since moved to `crates/index/src/live_set.rs`'s
   `LiveSet::from_row_ids` as part of §5/§7's fix). This was not called out in the code's own comments
   (which focus on why a bitset beats a `HashSet`) and was found during this audit's design review:
   caching only the resolved row-ids would still pay this rebuild on every cache hit.

Neither cost depends on the query vector — both are pure functions of `(file set, predicate)`, i.e. of
`(Snapshot, Predicate)` given storage is append-only within a snapshot's file set.

**Ruled out as in scope:** Phase S1 (segmented immutable HNSW layout, [ADR
0008](../decisions/0008-adopt-segmented-index-layout.md)) is confirmed orthogonal — its own spec
(§4.1) states a segment "may store only graph structure + row-id mapping to avoid duplicating
embeddings" while still reading vectors from the row file. S1 changes how the *vector index* is
stored, not how row files are read to resolve a predicate's live row-ids. This fix does not wait on
S1 and is not designed around it.

## 3. Candidate fixes considered

| Fix | Description | Verdict |
|---|---|---|
| (a) Per-snapshot cache of resolved row-ids/bitset | `Snapshot` gains a bounded, correctly-keyed cache from predicate → resolved live-set, populated lazily, discarded whole when the `Snapshot` is dropped | **Adopted** — see §5 |
| (b) Genuinely column-chunked on-disk format | Replace Arrow IPC files with a custom column-chunk layout so a column read never touches its neighbors' bytes at the disk-read level | **Rejected** — cross-cutting change to `crates/storage`, the layer every read/write goes through. Already explicitly deferred twice in this project's history (Phase 0 spec §6; Phase 2 spec's "Alternatives considered": *"a much larger undertaking... revisit only if a concrete requirement Arrow's own types can't satisfy actually shows up"*). Re-evaluated now that this is the #1 bottleneck: still rejected, because (a) closes the measured gap at a fraction of the risk. Nothing here is a requirement Arrow's own types can't satisfy — it's a caching gap, not a format gap. |
| (c) Custom Arrow IPC projection reader (parse the footer/message FlatBuffers metadata directly, seek to only the needed byte ranges) | Doesn't change the on-disk format — a dedicated reader for this one hot path, bypassing arrow-rs's `FileReader` | **Rejected for now** — this is, in substance, most of (b)'s hard part (hand-parsing binary format internals) done in the highest-risk way (manual offset math, an off-by-one silently corrupts reads) for one call site. No workload currently requires it: repeated-predicate reuse within a live snapshot is the dominant pattern this bench and plausible agent traffic both exhibit, and (a) captures that reuse entirely. Revisit only if post-(a) data shows the *cold*-path (first read of a never-before-seen predicate) still dominates. |

A prior, narrower framing of (c) was reportedly considered and rejected during Phase S1 W1's design
(2026-07-25) for similar reasons; that specific writeup could not be located in this repository's
history (checked `docs/superpowers/`, `docs/`, and all S1 branches) — the reasoning above was
derived independently and reaches the same conclusion.

## 4. Design review — LLM council

Given this is a memory/latency tradeoff over the transaction layer's read path (`crates/txn`), and
per `AGENTS.md`'s mandate to run hard architectural/tradeoff decisions through `llm-council`,
the choice above was put to a 5-advisor council (Contrarian, First Principles, Expansionist, Outsider,
Executor) with independent peer review and Opus-tier chairman synthesis. Full transcript not saved;
findings that changed the design:

- **4/5 advisors said ship (a) now; council-wide consensus rejected (b) and (c) for now.** Matches
  §3's conclusion independently.
- **The Contrarian's objection was the one the peer-review round rated strongest (4/5 reviewers):**
  "bounded by construction" is false as stated. The cache doesn't die with the `Snapshot`'s *logical*
  life — it dies with the last `Arc<Snapshot>`. `Dataset::snapshot()` is `current.load_full()`; a
  long-running reader holding a snapshot across many distinct ad-hoc predicates grows an unbounded
  `HashMap` for the hold's duration. **This changed the design from "unbounded HashMap" to "byte-budgeted
  cache" (§5).**
  - The Expansionist's counter-position ("cache aggressively, populate always, don't gate") was
    independently flagged as the council's clearest error by all 5 reviewers, precisely because it
    turns the Contrarian's diagnosed leak into policy. Rejected.
- **Debug-string cache keys were rejected by 3/5 advisors independently** (`Predicate::Debug` is not
  a documented stable format; risks silent perf-regressing misses or, worse, silent structural
  collisions). `Predicate` derives `Debug`/`Clone`/`PartialEq` but not `Eq`/`Hash` (`Value::Float64`
  doesn't support `Hash`) — a real `PredicateKey` type is needed instead of `format!("{:?}")`.
- **Chairman's synthesis, verified against source, added the finding in §2.2 above** (the bitset
  rebuild in `search_filtered`) — caching row-ids alone leaves that cost on the table; the cache
  should hold the resolved bitset (`LiveSet`), not a `Vec<usize>`.
- **All 5 peer reviewers independently flagged the same gap the advisors missed:** a
  `Mutex`-guarded structure added to `Snapshot` is a concurrency-touching change to `crates/txn`,
  which `.opencode/rules/concurrency-txn-layer.md` and `AGENTS.md` make a mandatory `loom`
  interleaving test, not an optional one.
- **Lock discipline was identified as the actual design question**, not "Mutex vs. lock-free" in the
  abstract: holding the lock across the ~51 MB file read would serialize concurrent readers on
  unrelated predicates behind one slow read. The lock must never be held across I/O.
- The council independently converged on the same critique this doc's §1 anticipated: the benchmark's
  identical-predicate shape is a degenerate case, and a fix validated only against it risks optimizing
  the demo rather than the product — hence the new varying-predicate bench phase.
- One reviewer proposed keying the cache on the immutable *file set* rather than `Snapshot` identity,
  so entries survive across commits for files unaffected by a given commit. Deferred — see §7 (PR3).

## 5. Decision

Ship **(a)**, in the corrected form the council converged on:

- **Cache type:** a `LiveSet` (dense row-id bitset, hoisted out of `HnswIndex::search_filtered` into
  a public `crates/index` type) rather than `Vec<usize>` — eliminates both the file re-read *and* the
  per-query bitset rebuild.
- **Key:** a hand-rolled `PredicateKey` (`crates/query`) that derives `Eq`/`Hash` properly (floats via
  raw `to_bits` identity — see §7's review-round correction for why this must NOT canonicalize
  `-0.0`/`+0.0` or NaN payloads), not a `Debug`-string.
- **Bound:** a byte budget (not an entry-count LRU — entries are now uniformly sized bitsets, so a
  byte budget is trivial and actually bounds memory, unlike an entry cap).
- **Lock discipline:** never hold the lock across the file read; a placeholder-then-fill pattern so
  concurrent misses on different keys never block each other, and concurrent misses on the *same* key
  don't duplicate the read.
- **Correctness:** sound because `Snapshot` is immutable and the cache is discarded whole, never
  invalidated incrementally — recorded as a doc comment on the field, not left as tribal knowledge.
- **Verification:** unit tests (hit/miss/over-budget-skip), plus a `loom` interleaving test for
  concurrent cache population (scoped per `.opencode/rules/concurrency-txn-layer.md`, never a
  workspace-wide `RUSTFLAGS --cfg loom`).

**Rejected:** (b), (c), and a Bloom-filter/finer-grained zone-map extension to `should_scan_file`
(considered during the council round, dismissed on inspection — `should_scan_file` already runs
per-file min/max pruning today via `ColumnStats`, and for a low-cardinality `category` column spread
across every file, every file's min/max already spans every category value; a Bloom filter would
answer "maybe" for every file too, delivering ~0% on this workload).

## 6. Implementation sequencing

1. **PR 1 — `PredicateKey`** (`crates/query`). New type, `Eq`/`Hash`, keys floats by raw `to_bits`
   identity (no canonicalization — see §7's review-round correction). Unit tests only; no cache yet.
   Independent, mergeable alone.
2. **PR 2 — the cache** (`crates/txn/src/snapshot.rs`, `crates/index`). `LiveSet` hoisted into
   `crates/index` as a public type; `Snapshot` gains a byte-budgeted, `Mutex`-guarded
   `PredicateKey -> Arc<LiveSet>` cache; `row_ids_matching`/`vector_search` read through it. Loom test
   for the concurrent-population race. Benchmark before/after on both the identical- and
   varying-predicate `lifecycle_bench` phases (§1).
3. **PR 3 — deferred, gated on PR 2's measurement.** Per-file `(file_name, PredicateKey)` memoization
   on `Dataset` surviving across commits (the reviewer-proposed idea from §4). Build only if PR 2's
   varying-predicate numbers show cross-commit predicate repetition still dominates cost in practice.

Each PR reviewed by the Opus reviewer subagent before being marked done, per `AGENTS.md`'s
"what done means" checklist.

## 7. Measured result — PR 1 + PR 2 implemented

`PredicateKey` (`crates/query/src/predicate_key.rs`), `LiveSet` (`crates/index/src/live_set.rs`,
hoisted out of `HnswIndex::search_filtered`, with `search_filtered_live` added as the
already-built-bitset entry point), and `LiveSetCache` (`crates/txn/src/live_set_cache.rs`, byte-budgeted
at 64 MiB, never holds its slot-map lock across a compute, and gives concurrent misses on the same key
exactly one real compute — loom-tested) are implemented and wired into `Snapshot::vector_search`. Full
methodology as §1: same machine, same 25k-row/512-dim dataset, `cargo bench --bench lifecycle_bench`.

| Phase | Before | After | Change |
|---|---|---|---|
| filtered, same predicate ×50 | 3.90 s / 2559.5 MB | **79.82 ms / 51.3 MB** | **−98.0% wall, −98.0% alloc** |
| filtered, varying predicate ×50 (9 categories, disjoint from phase 7's) | 4.49 s / 2559.4 MB | **457.42 ms / 460.7 MB** | **−89.8% wall, −82.0% alloc** |
| unfiltered (unaffected, control) | 22.84 ms / 0.0 MB | 18.64 ms / 0.0 MB | noise |

Both numbers land exactly where the design predicted, which is itself evidence the mechanism is doing
what it claims rather than something else moving the number:

- **Same-predicate:** 51.3 MB total ≈ one real file read for all 50 queries (matches §1's
  ~51 MB/query baseline for a single read) — 1 cache miss + 49 hits, as expected for one predicate
  against one live snapshot.
- **Varying-predicate:** 460.7 MB ≈ 9 × 51.2 MB — one real read per distinct category, each then reused
  for its remaining repeats within the 50-query loop (9 misses + 41 hits) — confirms §4's council
  concern was correctly addressed: this fix helps realistic *varying*-but-repeating predicate
  workloads, not only the benchmark's original identical-predicate degenerate case. (The bench phase
  cycles 9 categories, not 10 — see the review-round correction below for why.)

`ingest+commit` and `recovery` wall times also moved between runs (e.g. 19.32 s → 9.92 s ingest across
different runs of this session) despite this fix touching neither path — background machine variance,
not attributable to this change. Flagged rather than silently reported, per this project's own
"measure the thing that gates the expensive work" lesson (don't let a coincidental number get credited
to an unrelated fix).

**Verification:** `cargo build --workspace`, `cargo test --workspace` (all crates, all suites, incl.
the existing snapshot-isolation and filtered-vector-search correctness tests — no regressions),
`cargo clippy --workspace --all-targets -- -D warnings` (clean), `cargo fmt --check` (clean), and the
`LiveSetCache` loom interleaving test (`cargo rustc -p strata-txn --lib --profile test -- --cfg loom`,
scoped per `.opencode/rules/concurrency-txn-layer.md`) — two concurrent misses on the same key compute
exactly once, across loom's full interleaving search.

PR 3 (per-file cache surviving across commits) remains deferred: the varying-predicate number above
already shows large wins from *within-snapshot* reuse; whether *cross-commit* reuse matters enough to
justify PR 3's extra complexity needs real workload data this audit doesn't have, not another
benchmark guess.

## 8. Opus review round — a critical bug caught and fixed before merge

The mandatory Opus reviewer subagent (`AGENTS.md`'s "what done means" checklist) requested
changes on the first pass. One finding was critical and is the reason this section exists: **the
original `PredicateKey` canonicalized `-0.0`/`+0.0` together and collapsed all NaN payloads into one
bit pattern, on the assumption that this matched `f64`'s logical equality.** That assumption was wrong
for the thing that actually matters: `strata_query::mask`'s comparison kernel (arrow-rs) compares
`f64` by **bitwise/total-order identity**, not IEEE equality — `Eq("amount", 0.0)` and
`Eq("amount", -0.0)` select different, disjoint rows from a column holding both. A cache keyed on the
canonicalized form would silently return one predicate's cached live set for the other — a wrong
filtered-search result with no error, in the one subsystem whose entire premise is "no silently stale
vector search results." Verified empirically by the reviewer against this workspace's own `mask`.

**Fix:** `PredicateKey`'s float arm is now the raw `v.to_bits()`, with no canonicalization at all.
Distinct bit patterns simply never share a key — the safe direction, costing at most a redundant
recompute, never a wrong answer. The two unit tests that asserted the old (wrong) behavior were
inverted to assert the correct one (`crates/query/src/predicate_key.rs`), and a new
`Snapshot`-level regression test (`vector_search_with_two_different_predicates_against_one_snapshot_stays_correct_for_both`,
`crates/txn/src/dataset.rs`) queries one live snapshot with two different predicates and confirms
neither's result is contaminated by the other's cached entry.

Two further (non-critical) findings were also fixed: the byte budget only charged a `LiveSet`'s own
payload bytes, undercounting real per-entry overhead and — combined with the fact that a **failed**
`compute` never reached the payload-charging step — leaving unbounded growth open to a caller issuing
many distinct *failing* predicates against one long-lived snapshot. Both are closed by charging a
fixed `ENTRY_OVERHEAD_BYTES` at slot-creation time, before `compute` runs and regardless of whether it
succeeds (`crates/txn/src/live_set_cache.rs`), with a test proving budget-exhaustion-by-failed-entries
now correctly blocks further growth. The benchmark's varying-predicate phase was also corrected (it
originally cycled all 10 categories including phase 7's, silently turning one of its 10 "cold" misses
into a leftover cache hit); §7's numbers above are the corrected, honest run.

All fixes re-verified against the full "what done means" gate (build, test, clippy, fmt, loom) before
being folded into §7's reported numbers — this section documents that the first review round caught a
real correctness bug rather than treating "reviewed" as a formality.

**Second review round.** Re-review (mandatory before marking done, given the first round requested
changes) confirmed all of the above and approved, but found one more instance of the same class of
issue: `ENTRY_OVERHEAD_BYTES` was a flat 256-byte charge that ignored `PredicateKey`'s own
variable-length fields — a predicate with a long column name or a long `Utf8` value (e.g.
`Eq("body", Value::Utf8(<10 KB string>))`, a legitimate filter) could still consume far more real
memory than its charged 256 bytes implied, the same unbounded-growth shape as the failed-compute hole
already closed, just lower-probability. Fixed by adding `PredicateKey::variable_byte_size()`
(`crates/query/src/predicate_key.rs`, sums the column name's length plus a string value's own length —
zero for `Int`/`Float` values, which are stored inline) and charging it alongside
`ENTRY_OVERHEAD_BYTES` at slot-creation time (`crates/txn/src/live_set_cache.rs`), with a test
(`a_long_string_predicate_values_bytes_count_against_the_budget`) proving a long string value alone
exhausts a budget that a fixed-only charge would have left with headroom. Also corrected on this pass:
stale line-number references in §2 (the bitset-rebuild code has since moved to
`crates/index/src/live_set.rs`) and this doc's §1 table, which described the initial pre-fix
measurement's 10-category bench phase without noting it was later corrected to 9 (see the note under
§1's table). Re-verified against the full gate again after these changes; second review APPROVED.
