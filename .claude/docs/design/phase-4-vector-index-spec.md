# Phase 4 Spec — Vector Index (HNSW)

**Status:** Draft — approved design, not yet implemented. Design deliverable for the "Vector Index (HNSW)" phase in `.claude/docs/architecture.md`'s roadmap (exit criterion: "Recall@10/QPS benchmarked on a public embedding dataset").

**Scope:** real HNSW build + search over the vector column (`crates/index`), wired into the commit path as an append-only delta log sharing the transaction boundary with row data (`crates/txn`), plus filtered ANN (predicate + similarity search combined) and a benchmark proving recall/throughput on a real embedding dataset. Does not implement UPDATE, DELETE, tombstone GC, or a second index type — see §11.

**Prerequisite:** this spec builds directly on `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 (row-id definition and lifecycle, added via `llm-council` review — see `.superpowers/council/council-transcript-20260716-174711.md`). §8 defines the `u64` row-id, its assignment-at-commit-CAS semantics, the `Insert`/`Tombstone` delta-log entry shape, and the recovery-by-replay model. This spec does not redefine any of that — it specifies how `crates/index` and `crates/txn` implement against it.

## 1. `HnswIndex` (`crates/index`)

A thin wrapper around `hnsw_rs::prelude::Hnsw<f32, DistL2>` (squared L2 — matching `brute_force_search`'s existing distance semantics from Phase 1, so recall@10 is an apples-to-apples comparison against the same metric):

```rust
pub struct VectorMatch {
    pub row_id: u64,
    pub squared_distance: f32,
}

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistL2>,
    tombstones: HashSet<u64>,  // row-ids logically removed; graph nodes remain until Phase 8 compaction
}

impl HnswIndex {
    pub fn new(max_nb_connection: usize, max_elements: usize, max_layer: usize, ef_construction: usize) -> Result<Self, IndexError>;
    pub fn insert(&self, row_id: u64, vector: &[f32]);
    pub fn tombstone(&mut self, row_id: u64);
    pub fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<VectorMatch>;
    pub fn search_filtered(&self, query: &[f32], k: usize, ef_search: usize, live_ids: &[usize]) -> Vec<VectorMatch>;
}
```

`VectorMatch` is a distinct type from Phase 1's `brute_force::Neighbour` — deliberately, not an oversight. `Neighbour.row_index` means "position within the specific array passed to that call"; `VectorMatch.row_id` means the persistent, global row-id (§8 of the Phase 0 spec). Reusing `Neighbour` for both would let a caller silently misinterpret a global identity as a local array position (or vice versa) depending on which search path produced it — reusing the name is the actual footgun here, not the extra type.

`insert`/`tombstone` are the two operations the delta-log replay (§2) drives — `HnswIndex` itself has no file I/O and no manifest awareness; it is a pure in-memory index, matching `hnsw_rs`'s own scope. `search`/`search_filtered` both apply the tombstone set as an implicit filter (a tombstoned row-id is never returned, whether or not a caller-supplied predicate is present) — `search_filtered`'s `live_ids` parameter is the *predicate*-matching set; tombstones are subtracted from it before the call reaches `hnsw_rs`, so `hnsw_rs`'s `FilterT` only ever sees genuinely live, predicate-matching ids.

`new`'s parameter order matches `hnsw_rs::Hnsw::new(max_nb_connection, max_elements, max_layer, ef_construction, dist)` exactly (verified against the installed `hnsw_rs-0.3.4` source — `max_elements` is the second parameter, not the third, and the crate does not re-export a builder that would make this order self-documenting at the call site). `new` returns `Result`, not `Self`, because `hnsw_rs::Hnsw::new` calls `std::process::exit(1)` — not a panic, an unconditional, uncatchable process termination — when `max_nb_connection > 256`. `HnswIndex::new` must validate this bound itself and return a typed `IndexError` before ever reaching the library call, or a bad config kills the whole process instead of surfacing as a normal error.

`max_nb_connection`, `ef_construction` defaults are tuned via the benchmark in §6, not guessed, per `.claude/rules/vector-index.md`. `max_elements` is sized from the dataset's current row count at `Dataset::open` time (§2), with headroom for the session's inserts — confirmed against the installed source that this is purely a `Vec::with_capacity` hint per graph layer (`PointIndexation::new`), not a hard cap: exceeding it costs a reallocation, never panics or silently drops data.

## 2. Delta log integration (`crates/txn`)

**Write path.** `Transaction::commit` (already computing per-file column stats per Phase 3) additionally, for each pending batch with a non-null vector column: builds one `Insert{row_id, vector}` delta-log entry per row (using the row-ids assigned per §8's commit-CAS protocol), serializes this commit's entries to a new append-only file under `data/` (own file, mirroring `crates/storage`'s per-commit row-data files — not embedded in the row data file itself, since the index delta log and row data are conceptually independent append-only logs that happen to share a commit), and includes this delta file's name in the manifest entry for the new version, alongside the existing `DataFileEntry` list. `sync_dir` covers this file the same as row data files — fsynced before the manifest CAS, per §3's durability ordering.

**Read path / recovery.** `Dataset::open` builds a fresh `HnswIndex` by replaying every committed delta-log file, in commit order, up to the current manifest version: each `Insert` entry calls `HnswIndex::insert`, each `Tombstone` calls `HnswIndex::tombstone` (no `Tombstone` entries exist yet in Phase 4 — see §11 — but the replay loop handles the variant now so Phase 5/6 don't need to touch this loop). The built index is cached on `Dataset` (a field alongside the existing `manifest`), built once per `open`, not per search call.

## 3. Search API (`crates/txn::Dataset`)

```rust
pub fn vector_search(
    &self,
    query: &[f32],
    k: usize,
    predicate: Option<&Predicate>,
) -> Result<Vec<VectorMatch>>
```

Without a predicate: `self.index.search(query, k, EF_SEARCH_DEFAULT)`.

With a predicate: resolve the predicate-matching row-id set (an internal helper reading each surviving file's raw on-disk batch — which already carries the hidden `_row_id` column written at commit time per §2's write path — and applying `strata_query::filter` to it directly, rather than going through the public `scan_with_predicate`, whose caller-supplied logical schema never includes `_row_id` and would drop it), sort the resulting ids (needed for `hnsw_rs`'s built-in `impl FilterT for Vec<usize>`, which binary-searches), widen `ef_search` per §4, then `self.index.search_filtered(query, k, ef_widened, &live_ids)`.

`VectorMatch` (§1) is returned as-is; `Dataset` does not translate row-ids back to user-facing column values — that's the caller's job (`scan` + a row-id lookup, or a future `Dataset::get_by_row_id` if profiling shows this path matters — not built now, YAGNI).

## 4. Filtered-ANN `ef` widening

`hnsw_rs::search_filter` runs the graph traversal at a fixed candidate width `ef`, then discards non-matching candidates from that fixed set — so a selective predicate can return fewer than `k` results even when enough true matches exist deeper in the graph. Rather than guess a fixed `ef`, widen it using a cheap, already-available signal from Phase 3:

```rust
fn widen_ef(base_ef: usize, dataset: &Dataset, predicate: &Predicate) -> usize {
    let explain = dataset.explain(predicate);
    let selectivity_upper_bound =
        explain.scanned.len() as f64 / explain.total_files.max(1) as f64;
    let scale = (1.0 / selectivity_upper_bound.max(MIN_SELECTIVITY_FLOOR)).min(MAX_EF_SCALE);
    ((base_ef as f64) * scale).round() as usize
}
```

`explain`'s scanned/total file ratio is a coarse, file-granularity *upper bound* on selectivity (some rows within a scanned file may still not match — the ratio can only overestimate how many rows survive, never underestimate, which is the safe direction: erring toward a wider `ef` costs search time, never correctness). `MIN_SELECTIVITY_FLOOR` (e.g. `0.01`) and `MAX_EF_SCALE` (e.g. `20`) bound worst-case latency when a predicate is extremely selective or every file happens to survive pruning — both are benchmark-tuned constants per §6, not guesses. This reuses `Dataset::explain` exactly as built in Phase 3 — no new stats machinery.

## 5. CLI (`crates/cli`)

`strata search` switches to the HNSW-backed `vector_search` by default. An `--exact` flag keeps the Phase 1 brute-force path reachable (`brute_force_search` is not removed — see §1) for correctness spot-checks against the approximate index. A `--filter <column> <op> <value>` option (reusing the same `Predicate` parsing already written for `strata explain` in Phase 3) drives the `predicate` argument to `vector_search`.

## 6. Benchmark (`bench/`)

Follows Phase 2's established `check_correctness`-before-timing pattern (`bench/benches/group_by_bench.rs`):

- **Dataset:** a public dataset of real LLM/text embeddings (not classic CV descriptors like SIFT — chosen to match Strata's actual audience and typical dimensionality). Concrete dataset choice (exact HuggingFace/source URL, vector count, dimensionality) is an implementation-plan decision, verified against the real, current hosting location before being hardcoded into a benchmark — this spec commits to the *category* (public, real embeddings, LLM-relevant), not a URL that could rot.
- **Correctness gate:** recall@10 computed by comparing `vector_search`'s results against `brute_force_search`'s exact results on the same query set, before any QPS number is trusted — mirrors Phase 2's "verify against an independently-implemented naive reference before trusting timing" discipline.
- **Reported numbers:** Recall@10 and QPS, both for unfiltered search and for at least one filtered-search scenario (to exercise §4's `ef`-widening path, not just the unfiltered baseline).
- **Parameter tuning:** `max_nb_connection`, `ef_construction`, `ef_search` defaults are chosen by running this benchmark across a small grid, not guessed — the winning values and the benchmark run that produced them are cited in the commit that sets the defaults, per `.claude/rules/vector-index.md`.

## 7. Data flow

**Write path (extends Phase 2/3's, unchanged in spirit):** batch arrives at `Transaction::commit` → column stats computed (Phase 3) → `encode_batch` runs (Phase 2) → delta-log `Insert` entries built from the batch's vector column + assigned row-ids (§8 of the Phase 0 spec) → row data file and delta-log file both written, both covered by the same `sync_dir` → manifest CAS references both.

**Read path (new):** `Dataset::open` → replay all delta-log files into a fresh `HnswIndex` (§2) → cached on `Dataset` → `vector_search` (§3) consults the cached index, optionally narrowing via `scan_with_predicate` + `ef` widening (§4).

## 8. Error handling

`vector_search` returns `Result<Vec<VectorMatch>, TxnError>` — errors propagate from the row-id resolution helper (predicate column doesn't exist, type mismatch — same errors `filter`/`scan_with_predicate` already surface) when a predicate is supplied; the index-only path (`HnswIndex::search`) does not itself fail except on a dimension mismatch between `query` and the indexed vectors, checked upfront the same way `brute_force_search` already does (Phase 1's dimension-mismatch fix) rather than silently truncating.

## 9. Testing

- **`HnswIndex`** unit tests: insert + search finds the true nearest neighbor on a small hand-built set (mirrors `brute_force_search`'s existing exact-match test); a tombstoned row-id is never returned by `search`/`search_filtered` even when it is the true nearest neighbor.
- **`widen_ef`**: unit tests against synthetic `ExplainResult`-shaped inputs — full selectivity (all files scanned) leaves `ef` at `base_ef`; a highly selective case scales `ef` up to (not past) `MAX_EF_SCALE`.
- **Delta-log round trip**: commit a batch with vectors, reopen the dataset (fresh `Dataset::open`, forcing a real replay from disk, not an in-memory shortcut), confirm `vector_search` finds the same results as before the reopen — this is the crash-recovery-equivalent test for the index, mirroring Phase 1's MVP checklist's kill-9 test in spirit (a full process restart, not just a fresh `Dataset` struct in the same process).
- **Integration (the actual exit criterion):** the benchmark itself (§6) is the exit-criterion evidence — a real recall@10/QPS number on a public dataset, not just passing unit tests.

## 10. Alternatives considered

- **Whole-graph dump/reload per commit** (using `hnsw_rs`'s own `file_dump`/`load_hnsw`) instead of an append-only insert-level delta log: rejected — `hnsw_rs` does support this (verified against the installed source), but dumping the *entire* graph on every commit is O(graph size), not O(commit size), which defeats the point of Strata's append-only, per-commit-file architecture used everywhere else (row data, Phase 3's stats). The delta-log approach costs O(commit size) to write and O(total historical inserts) to replay once at open — acceptable until Phase 8 compaction exists to bound replay cost, matching how `scan()` today also reads every historical file with no compaction yet.
- **Pre-filter (scan-then-brute-force within the matching rows) instead of `hnsw_rs`'s native post-filter** for filtered ANN: rejected for the general case — defeats the purpose of the index for large datasets with a non-selective predicate (falls back to O(n) brute force exactly when the index would help most). `ef` widening (§4) keeps the HNSW graph traversal in the loop while still respecting the filter.
- **A separate `crates/index`-owned conflict/versioning scheme independent of the row-id spec**: rejected — `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4 already ties the vector-index conflict domain to row-id 1:1; inventing a second identity scheme here would contradict that spec, not extend it.

## 11. Non-goals for this phase

- UPDATE and DELETE (the row-id spec in Phase 0 §8 defines their *future* semantics so Phase 4's code isn't written against an undefined lifecycle, but no UPDATE/DELETE API ships in this phase)
- Tombstone GC / graph compaction (Phase 8's responsibility, per Phase 0 §8)
- IVF-PQ or any second index type (project-wide non-goal, `.claude/docs/architecture.md`)
- Distributed/multi-node index sharding (project-wide non-goal)
- A `Dataset::get_by_row_id` convenience lookup (not needed until a caller profile shows it matters)
