# Algorithmic Complexity Audit — Strata

> Date: 2026-07-23 · Scope: all production `crates/*/src` (~12.4k lines) · Method: five parallel
> per-crate auditors, cross-checked against each other and against pinned dependency source.

## How to read this

**Nothing here is measured.** Every figure below is static analysis — Big-O plus an
allocation/syscall count read off the source. The repo already has `criterion` benches
(`bench/benches/`) covering group-by, vector search, and concurrent commit. **Measure before and
after any change taken from this document.** Several items below are explicitly predicted to be
*smaller* wins than they look (and one is predicted to currently be a net *loss* — see Q2).

Confidence is annotated per finding:

- **Corroborated** — two independent auditors reached it from different crates. Highest confidence.
- **Source-verified** — the auditor read the pinned dependency source to confirm, and cited it.
- **Analytical** — derived from Strata's own source only.

Variables used throughout: `n` rows · `d` vector dimension · `M` neighbours/layer · `ef` beam width
· `F` data files in the manifest · `T` tombstones · `v` committed versions · `g` distinct groups ·
`R` live ids in a filtered search · `E` commit-log entries.

---

## Verdict

The **algorithms are almost all in the right complexity class already.** Hash aggregation, HNSW
search/insert, predicate pruning, and the OCC commit structure are textbook-correct, and several
subsystems are already carefully optimised with the reasoning written down in comments.

The real problems are not Big-O choices inside a function. They are:

1. **Two structures that grow with history and are never compacted**, turning per-commit and
   per-open work into O(history) — and cumulative work into **O(v²)**.
2. **Constant factors**: per-row allocation and per-row dynamic dispatch in Arrow loops, SipHash on
   internal integer keys, and unbuffered writers.
3. **Cross-crate boundaries that only offer "materialise everything" primitives**, so callers
   allocate megabytes to read one column.

Nine items are worth doing. Four need a design decision, not a patch.

---

## Top findings, ranked

### 1. Manifest is O(history) per commit → O(v²) cumulative — **Corroborated**

`crates/txn/src/dataset.rs:602-604,644` · `crates/storage/src/manifest.rs:39-71,103-129`

`Manifest.data_files` and `.tombstones` accumulate across every version and are **never pruned —
there is no compaction or GC path anywhere in the codebase.** Every single commit then:

- deep-clones the whole list (`dataset.rs:602`) — at F=10k files × 3 stats columns that is roughly
  **80k allocations**, because each entry clones two `String`s plus a whole `HashMap<String,
  ColumnStats>`;
- `extend`s it (reallocating, since the clone has exact capacity);
- re-serialises **the entire accumulated history** to JSON (`manifest.rs:110`);
- writes and fsyncs it — **all inside the global commit lock.**

So commit N costs O(F+T), and total work across N commits is **O(N²)**. This is the dominant
scaling limit of the engine, and it is paid serially by every writer.

The txn and storage auditors found this independently from opposite ends. That makes it the
highest-confidence finding in the audit.

- **Cheap partial fix (in-crate, safe):** make `data_files` a persistent/shared structure
  (`imbl::Vector`, or `Arc<[DataFileEntry]>` with copy-on-append) so the clone at `dataset.rs:602`
  is O(1). Removes one of three O(F) passes and nearly all 80k allocations. Serialization stays
  O(F).
- **Real fix (design-gated):** incremental manifests — each version stores a delta plus a parent
  pointer, with periodic compaction. This is exactly what Lance (already this project's cited
  storage reference) does. It preserves every invariant: commit is still one atomic rename, writes
  are still durable before acknowledgment, nothing is buffered.
- **Do not** "fix" this by moving `commit_manifest` out of the lock or acknowledging before fsync.
  That breaks the single-CAS rule and the durability invariant.

### 2. `read_current()` scans the whole version directory on every open — **Analytical**

`crates/storage/src/manifest.rs:141-172`

Every `Dataset::open()` lists `_versions/` and parses *every* manifest filename ever committed —
O(v) — then reads and parses the winner, O(F·c+T). Nothing ever deletes old version files.

The auditor verified call frequency rather than assuming it: this is once per `Dataset::open`
(`dataset.rs:164,221`), **not** once per transaction. Still a hot path for an embedded store that
many agent processes open concurrently.

**Fix:** a small atomically-updated `CURRENT` pointer file (the RocksDB/Delta pattern), written with
the same tmp-write → fsync → rename sequence already in use, making the common path O(1). Fall back
to the directory scan if the pointer is missing or stale, which preserves crash-safety. Costs one
extra fsync per commit — strictly *more* durable work, so it does not touch the no-silent-buffering
invariant. Small, low-risk, and the highest value-per-line fix in the storage crate.

### 3. `conflicts_with` is quadratic in the write set — **Analytical**

`crates/txn/src/commit_log.rs:57-98`

The OCC conflict check — the flagship path — has three problems, all inside the commit lock:

- it iterates **all** E=2048 entries even to skip them by version (`:89-92`), despite `entries`
  being strictly version-ascending and therefore binary-searchable;
- `write_set.contains(row_id)` (`:94`) is a **linear scan of the committing write set** — O(n) per
  candidate row;
- `!contested.contains(row_id)` is a second linear scan.

Worst case, a bulk delete of n=100k against a full log averaging 10 rows/entry is **~2×10⁹
comparisons while holding the global commit lock**, stalling every other writer.

**Fix** — hash the write set once, binary-search the range start, dedup via a set. O(n + log E + W_r).
Preserves exact current semantics including the first-encountered ordering of `contested`:

```rust
let mine: HashSet<u64> = write_set.iter().copied().collect();          // O(n) once
let start = self.entries.partition_point(|(v, _)| *v <= since_version); // O(log E)
let mut seen = HashSet::new();
for (version, ws) in self.entries.iter().skip(start) {
    if *version > up_to_version { break; }
    for row_id in ws {
        if mine.contains(row_id) && seen.insert(*row_id) { contested.push(*row_id); }
    }
}
```

Keep the existing `write_set.is_empty()` short-circuit (`:70`) *before* building `mine`, so
insert-only commits stay allocation-free. No invariant or loom impact — lock discipline is unchanged.

### 4. Filtered vector search materialises O(R) state per query — **Conflict, resolved below**

`crates/txn/src/snapshot.rs:230` · `crates/index/src/hnsw.rs:260`

See "Conflicts resolved" — this is the one place the two heavyweight auditors disagreed, and
**neither proposal was right.**

### 5. `row_ids_matching` copies every column to read one — **Analytical**

`crates/txn/src/snapshot.rs:240-259`

`filter(&batch, predicate)` at `:242` materialises a fully filtered `RecordBatch` — copying **every
column, including the embedding column** — when only `_row_id` is ever read (`:243-256`). At
D=1536, a 100k-row match copies roughly **590 MB** and discards all of it.

**Fix:** compute the boolean mask once and apply it *only* to the `_row_id` column. This needs the
mask half of `strata_query::filter` exposed — `crates/query/src/predicate.rs:56` already computes
exactly that internally, so it is a small, clean API addition (`pub fn mask(batch, predicate) ->
Result<BooleanArray>`), not new logic.

### 6. Recovery rebuilds the entire HNSW graph from JSON — **Corroborated**

`crates/txn/src/dataset.rs:777-802` · `crates/index/src/delta_log.rs:59`

`Dataset::open` replays every delta-log entry through `HnswIndex::insert`, i.e. a **full graph
build** — O(n · l · [ef_c·M·d + m·M²·d]). At n=1M that is tens of seconds to minutes, on the
critical path of every process start. Compounding it:

- every vector is **allocated twice** — once by serde into `DeltaEntry::Insert.vector`, then again
  by `HnswIndex::insert`'s `vector.to_vec()` (`hnsw.rs:181`);
- `read_delta_log` does `read_to_string` of the *whole* file (`delta_log.rs:60`), so a 100k×512 log
  holds ~300 MB of JSON text **plus** ~200 MB of parsed vectors simultaneously.

**Three tiers:**
- *Safe, immediate:* an owned-vector `insert_owned` entry point removes one full copy per row.
- *Safe, immediate:* stream via `BufReader::lines()` instead of `read_to_string`, capping peak
  memory at one line. Better still, return an iterator — `dataset.rs:789` is the only production
  caller and already consumes it in a `for` loop.
- *Design-gated:* a serialized graph checkpoint turns recovery into an O(n·M) load. **This does not
  violate the append-only-delta-log invariant** — the log stays the source of truth and the
  checkpoint is a derived cache — but it is a storage-format addition and needs its own decision doc.

### 7. Per-row Arrow downcast + `Arc` allocation — **and the codebase already has the fix**

This anti-pattern appears independently in two crates:

- `crates/txn/src/dataset.rs:844-858` — `FixedSizeListArray::value(i)` mints a **fresh `Arc<dyn
  Array>` per row**, then re-downcasts per row. **Three allocations per row where one would do**;
  at 100k rows that is 300k allocations instead of 100k.
- `crates/index/src/brute_force.rs:43-47` — same shape: `n` `Arc` refcount atomics plus `n` dynamic
  type checks inside the scan loop.

**`crates/query/src/group_by.rs:194-210` already does this correctly** — it hoists the downcast out
of the row loop, with a comment explaining why. That is the reference implementation; the other two
sites should copy it. Downcast `values()` once, read `value_length()`, then slice the flat buffer
directly (honouring `offset()`).

Caveat on `brute_force.rs`: it backs `search --exact` and the recall ground truth in
`bench/benches/lockfree_vs_hnsw_rs_bench.rs`. Hoisting the downcast is safe; **reassociating its
summation is not** — it changes last-ulp results and could perturb recall tie-breaking.

### 8. HNSW query-path constant factors — **Analytical**

Four separable wins in `crates/index/src/graph.rs`, roughly in order:

- **Saturation early-exit costs more than it saves at realistic `d`** (`:305-334`). It rebuilds a
  result-id set and intersects it **per popped candidate** — O(ef) per pop, so **O(ef²) per
  `search_layer`**. At ef=200/M=32 that is ~40k hash ops against ~614k cycles of actual distance
  work: **~15-20% overhead at d=768, and roughly 100% at d=128.** Replace with an O(1) incremental
  delta counter — `result` only changes via one push and one pop, so exact overlap is trackable
  without rebuilding anything.
- **`visited: HashSet<u64>` → generation-stamped array** (`:166`). Removes SipHash from the
  traversal *and* makes the per-call `clear()` (`:217`, currently **O(buckets)**, not O(len)) an
  O(1) counter bump. `node_table.rs` already guarantees dense monotonic row-ids, so the precondition
  holds. This is what hnswlib's `VisitedList` does.
- **`SlotArray::occupied()` heap-allocates per popped candidate** (`slot_array.rs:73`, called at
  `graph.rs:273`) — **~200 allocations per query.** Add a lazy iterator or scratch-buffer fill; the
  snapshot is already documented as non-atomic across slots, so an iterator preserves semantics exactly.
- **Double hash** at `:274-277` — `contains` then `insert` hashes the same key twice. One-line fix:
  `if !scratch.visited.insert(id) { continue; }`.

Also: ~70-100 heap allocations per `Graph::insert` (`:437,442,456,458,463,466,674`) — an
`InsertScratch` thread-local mirroring the existing `SearchScratch` would remove essentially all of it.

### 9. Unbuffered Arrow IPC writer — the fix is a one-line constructor swap — **Source-verified**

`crates/storage/src/datafile.rs:27-36`

`File::create` is handed **unbuffered** to `FileWriter::try_new`. The auditor traced arrow-ipc
58.3.0 and confirmed it issues many separate `write_all` calls per batch — magic, padding, per-buffer
body, per-buffer padding, continuation markers, length prefixes — each an individual syscall.

arrow-ipc already ships `FileWriter::try_new_buffered` for exactly this. `into_inner()` is documented
to flush first, so the durability sequence is unchanged: flush → `sync_all` → return.

**This codebase already applied this exact fix to the delta log** (commit `perf(index): buffer delta
log writes to coalesce per-entry syscalls`) — it just was never applied to the storage crate's main
data-file writer. Same technique, same reasoning, no correctness risk.

### 10. Hashing strategy — right idea, correctly different per threat surface — **Corroborated**

Three auditors independently flagged that **every hot hash structure uses std's default SipHash**.
They calibrated the fix differently, and that distinction is correct — do not flatten it:

| Site | Keys | Recommendation |
|---|---|---|
| `query/group_by.rs:219` group index | derived from **user data**, multi-agent store | `ahash` — keeps per-process random seeding, so no HashDoS regression |
| `txn/snapshot.rs:70` tombstones | internal row-ids | fast integer hasher; plus short-circuit on `tombstones.is_empty()` |
| `index/graph.rs:166` visited set | internal row-ids | replace the hash structure entirely (see #8) |

`ahash` and `hashbrown` are **already in `Cargo.lock`** transitively via `arrow-array`, so making
`ahash` a direct dependency of `strata-query` compiles no new code. `group_index_of` is the hottest
loop in the query engine — hit once per row.

Correctness is unaffected by a hasher swap: `Row<'_>`'s `Eq` is exact byte equality, and
`group_by.rs:674-804` already tests 5,000 rows / 2,500 groups *including real collisions*.

### 11. `search_filtered` / query-crate smaller wins — **Analytical**

- `crates/query/src/group_by.rs:168-184` — the Float64 cast is cached per `(column, func)` pair, not
  per **distinct column**. `SUM/AVG/MIN/MAX` over one column triggers **four** independent O(r)
  casts instead of one. Dedupe by column name. **Med**, workload-dependent.
- `crates/query/src/group_by.rs:95-123` + `293-326` — the `Vec<AggValue>` intermediate is
  **16 bytes/group** (empirically verified by compiling `size_of`) versus 8 for the native value,
  and is immediately unwrapped back to a typed `Vec` one call later. Collapse the two steps; saves
  an O(g) alloc+copy and a transient 2× per-group blowup.
- `crates/storage/src/encoding.rs:86` — `row.owned()` heap-copies every row into the `HashSet`, but
  the auditor verified against arrow-row 58.3.0 source that `Row<'_>` **already implements
  `Hash`/`Eq` on borrowed bytes**. The owned copy is simply unnecessary.
- `crates/storage/src/encoding.rs:60-61` — the full column is row-converted O(n) *before* the
  cardinality bail-out loop, defeating the bail-out precisely on high-cardinality columns (IDs),
  which is the common case. Convert in bounded chunks.
- `crates/storage/src/stats.rs:40,51,62` — min and max are two separate kernel calls per column;
  verified no combined kernel exists in arrow-arith 58.3.0, so a hand-rolled single pass halves
  memory traffic on the commit path.
- `crates/txn/src/snapshot.rs:48-50` — `widen_ef` calls `explain()`, which clones **F filename
  `String`s**, then uses only `.len()`. Add a count-only helper; leave the public `explain` alone.
- `crates/txn/src/snapshot.rs:159-160` — `scan_with_predicate` **casts before filtering**, so every
  column of every surviving row is cast then discarded. Filter first, cast the smaller result.
  `row_ids_matching:242` already proves raw-batch filtering works.

### 12. Build profile — **MEASURED AND REJECTED. Do not apply.**

> **This finding was wrong.** It was ranked as a free win on static reasoning; measuring it refuted
> that. `lto = "thin"` + `codegen-units = 1` makes **unfiltered vector search ~70% slower**:
> 96.95 µs → 163.91 µs and 164.47 µs across two runs, both with tight CIs, against a 100k×512
> real-embedding index. Even the pessimistic end of the baseline's interval (105.7 µs) leaves a 55%
> gap, so this does not depend on baseline noise. Filtered search was neutral (−6.2%, then +0.2%).
>
> Group-by *did* improve ~11–16% on its stable cases, which is what made this look good before the
> vector path was measurable — but trading ~70% on the core vector search path for that is a bad
> deal in a vector database. Applied in `7ebedb4`, reverted in `6892d25`.
>
> The specific mechanism claimed below — that LTO is needed to inline `DistL2::eval` across the
> crate boundary into the search loop — is exactly what the measurement contradicts. Inlining the
> AVX2 kernel into the hot loop appears to cost more than the call it removes. Mechanism not
> investigated further; the direction is reproducible and that was enough to reject it.
>
> The original static reasoning is left below unedited, as a record of how confident and wrong it read.



The workspace has **no `[profile.release]` and no `.cargo/config.toml`**, so release builds use
`codegen-units = 16` and `lto = false`. With heavy cross-crate traffic (`strata-txn` → `strata-index`
→ `strata-storage`, plus `imbl`), this blocks inlining of exactly the hot paths — including
`DistL2::eval`, which is neither generic nor `#[inline]` and so cannot inline across the crate
boundary today.

```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

**What I initially got wrong, and corrected:** I first suspected the baseline x86-64 target meant no
AVX2 in the distance kernels. That is **false**, and worth recording so nobody "fixes" it. `anndists`
is pulled in with the `simdeez_f` feature (`crates/index/Cargo.toml:27`) and dispatches on
`is_x86_feature_detected!("avx2")` at **runtime** (`distances.rs:100,173,290,367,438,523,547`).
**The SIMD win is already taken — do not "add SIMD" to the distance path.** The baseline-ISA cost
applies only to *non-distance* code. `target-cpu=native` would help there but breaks binary
portability, so it belongs in an opt-in documented profile, not the default.

---

## Conflicts resolved

**The two heavyweight auditors reached opposite conclusions about the same two lines.**

- **txn:** delete `live_ids.sort_unstable()` (`snapshot.rs:230`) — dead work, the only consumer
  builds an order-insensitive `HashSet`.
- **index:** *keep* the sort, and replace the `HashSet` (`hnsw.rs:260`) with `binary_search` — "the
  single largest win in this crate."

Both are locally reasonable and **both are wrong**, because each optimises one side of the boundary
while assuming the other stays fixed. At R=100k, sorting ~100k `u64` and building a 100k-entry
SipHash set are *both* in the low-milliseconds range — for a search that should take ~100-200 µs.
Either proposal leaves an O(R) per-query cost that dominates the O(ef·M) ≈ 6,400 probes it exists to serve.

**Resolution: eliminate the per-query O(R) materialisation instead.** Since `node_table.rs` already
guarantees dense monotonic row-ids, a **dense bitset** dominates both proposals:

|  | build | probe | memory @ R=100k |
|---|---|---|---|
| `HashSet` (today) | O(R) SipHash | O(1) hashed | ~1 MB |
| sort + `binary_search` | O(R log R) | O(log R) ≈ 17 cmp | ~800 KB |
| **bitset** | O(R) bit-sets | **O(1) branchless** | **~12.5 KB** |

Then delete the sort *and* the `HashSet`. Best long-term: build the filter **once per snapshot**
rather than once per query, since the same predicate is typically reused across queries.

---

## Already optimal — do not "fix" these

Several of these look like optimisation targets and are load-bearing for correctness. Verified
optimal by the auditors:

- **`EntryPoint`'s single-word `(row_id, level)` packing** (`graph.rs:20-118`) — this is not merely
  fast, it is **the fix for a loom-found torn-state race.** Do not split it.
- **`validate_delta_dimensions`** (`dataset.rs:881`) — do not fuse this pass into the apply loop to
  "save a pass." The separate pass **is** the zero-partial-mutation mechanism.
- **The conflict-check-before-graph-mutation ordering** (`dataset.rs:558-561`) — load-bearing.
- **`imbl::HashSet` for tombstones** — source-verified O(1) clone in imbl 7.0.1; exactly right for
  O(1) snapshot forking with structural sharing.
- **`RowConverter` batch encode/decode** (`group_by.rs:186-192,253-287`) and the hoisted Float64
  downcasts (`:194-210`) — textbook DuckDB-style vectorisation.
- **All of `predicate.rs`** — `filter`, `compare`, `should_scan_file` correctly hoist dispatch out of
  every row loop and delegate to Arrow kernels.
- `CommitLog::push` (O(1), allocation-free, correctly takes ownership); `get_or_create_chunk`'s
  drop-the-loser reclamation; `clear_matching`'s load-then-CAS; `brute_force`'s
  `select_nth_unstable_by`; the `BufWriter`-before-`sync_all` ordering in `delta_log.rs`; the atomic
  `fs::rename` CAS; compact JSON (already landed); `resolve_display_rows`'s O(n+k) hashmap join.

One asymmetry worth noting: `SlotArray::clear_matching` (`slot_array.rs:62`) already uses the correct
load-then-CAS pattern, but **`claim` (`:44`) does not** — it issues a `lock cmpxchg` per occupied
slot, taking each cache line exclusive (RFO) *even on failure*. Load-first, then CAS. **This alters
loom-verified concurrency and must be re-validated** against the existing test at `slot_array.rs:158`.

---

## Design-gated — decision doc, not a patch

Per `.opencode/rules/concurrency-txn-layer.md`, throughput changes in the txn layer are design
conversations. These four qualify:

1. **Incremental manifests + data-file compaction** (finding #1). The only real fix for O(v²).
2. **Serialized HNSW graph checkpoint** (finding #6). Turns recovery from a full rebuild into an
   O(n·M) load. Compatible with the append-only invariant — the log stays authoritative.
3. **Binary delta-log encoding.** JSON costs ~3× write amplification and **0.1-0.25 s of pure float
   formatting per 10k-row commit**, on the commit critical path. This reverses a documented decision
   at `delta_log.rs:23-27`. *Free and immediate regardless:* swap `to_string` + `writeln!` for
   `to_writer` — identical output bytes, eliminates E `String` allocations.
4. **Group commit.** Genuinely High impact, and **not** a violation of the durability invariant —
   all participants block until the shared fsync completes, so nothing is acknowledged early. But it
   changes one-version-per-transaction into one-version-per-group, which `CommitLog`'s version-keyed
   history and the loom tests both depend on.

**Explicitly rejected** (would break invariants): moving `commit_manifest` or the HNSW insert outside
the commit lock; acknowledging before fsync; any async write buffering; reordering the conflict check
relative to graph mutation.

---

## Documentation defects found

Two comments actively mislead, both about the same subject:

1. **`AGENTS.md`** lists `hnsw_rs` as the HNSW library. `crates/index/Cargo.toml` has **no
   such dependency** — it depends only on `anndists`. `hnsw_rs` is a **bench-only** dependency, used
   as the comparison baseline in `bench/benches/lockfree_vs_hnsw_rs_bench.rs`. The production index
   is Strata's own hand-rolled lock-free HNSW (`graph.rs`, 1759 lines with loom shims).
2. **`crates/index/src/distance.rs:64-66`** states `simdeez_f` is "not enabled anywhere in this
   workspace." `crates/index/Cargo.toml:27` enables it explicitly. The surrounding *conclusion*
   (avoid `anndists::DistDot` due to its zero-tolerance `assert!`) still holds on other grounds, but
   the stated reasoning is stale.

---

## MEASURED (2026-07-23) — finding #1 confirmed, but its remedy is not the one ranked below

`bench/benches/manifest_growth_bench.rs` now closes the gap described in the next section.
One dataset, sequential commits, one data file each, `id` column only (no vector column, so no
HNSW insert is involved). Results on the dev machine:

| Commits | Mean commit | vs first |
|---|---|---|
| 0–299 | 12.2 ms | 1.00x |
| 1200–1499 | 17.8 ms | 1.46x |
| 3000–3299 | 30.5 ms | 2.50x |
| 5700–5999 | 39.5 ms | **3.24x** |

Manifest size grew linearly as predicted: 287,869 B at F=2000 and 867,869 B at F=6000 (~145 B/file).

**Two results that change the plan:**

1. **The crossover is ~2000–3000 data files.** A first run at 2000 commits showed only **1.10x**
   drift, well inside noise — below the crossover, fsync (~12 ms) completely dominates the O(F)
   work. `concurrent_commit_bench` runs 400 commits, ~40x below the crossover, which is precisely
   why the lock-based design has measured healthy.
2. **The ranked "cheap partial fix" — persistent `Manifest.data_files` — targets the wrong term.**
   The O(F) cost is (a) the deep clone, (b) `serde_json` serialization of the whole manifest, and
   (c) writing + fsyncing an ever-larger file. A persistent/shared structure fixes only (a). At
   F=6000 the manifest is 868 KB written and fsynced *per commit*, and the +27 ms of growth tracks
   file size far more closely than it could track ~18k clone allocations. Treat that attribution as
   strongly indicated, not settled — splitting (a)/(b)/(c) with instrumentation is the next
   measurement, and should happen before any work is done here.

**Consequences:** incremental manifests (a delta plus a parent pointer per version) move from
"eventual real fix" to *the* fix, because they are the only option that addresses (b) and (c).
Group commit rises to the top of the list on the same evidence: with an empty manifest a commit
still costs ~12 ms of essentially pure fsync, so fsync batching wins at *every* scale, independent
of history.

## MEASURED (2026-07-23) — filtered vector search is ~1,500x slower than unfiltered, and the bottleneck is not what the audit guessed

> The numbers in the table just below are the *first-pass* figures and are contaminated by machine
> drift; the corrected same-session A/B and the located bottleneck are in the "UPDATE" at the end of
> this section. The ~1,500x order-of-magnitude gap between filtered and unfiltered is real and holds;
> what changed is the *cause* (a full-file re-read, not the embedding copy) and the *size of the fix
> applied here* (−13%, not order-of-magnitude).

With the benchmark dataset downloaded (100k real 512-dim OpenAI embeddings, recall@10 = 0.985):

| Operation | Cost |
|---|---|
| `vector_search` unfiltered, top-10 | **96.95 µs** |
| `vector_search` filtered, top-10, 1-of-10 categories | **152.88 ms** |
| A full durable commit, for scale | ~12 ms |

**A single filtered top-10 costs more than twelve sequential durable commits.** This is the largest
effect measured anywhere in this audit, and nothing in the original ranking put it near the top.

It is the predicted cost of findings #5 and #8 compounding, now with a number attached. Per query,
against R ≈ 10,000 matching rows:

1. `row_ids_matching` (`snapshot.rs:242`) calls `filter` on the whole `RecordBatch`, materializing
   **every column including the 512-dim embeddings** — ~20 MB copied and discarded — to read one
   `u64` column.
2. `live_ids.sort_unstable()` (`snapshot.rs:230`) sorts 10k ids.
3. `search_filtered` (`hnsw.rs:260`) builds a fresh 10k-entry `HashSet` per query.

Only step 3's `ef·M` ≈ 6,400 probes are actual search work; steps 1–3 are all O(R) setup paid before
the traversal starts.

**This reorders the whole document.** The manifest O(F) growth was ranked #1 and does not bite until
~2000–3000 files; this is a ~1,500x factor on a core operation at 100k rows, today.

### UPDATE — the fix was applied and measured, and the audit's prediction here was wrong

Findings #4 and #5 were implemented (`7a675cd`): `mask()` added to `crates/query`, `row_ids_matching`
applies it to the `_row_id` column alone, `search_filtered` uses a dense bitset, and the dead
`live_ids.sort_unstable()` is gone. Result, measured back-to-back on one machine state (stash / bench
/ restore / bench):

| | before | after | change |
|---|---|---|---|
| filtered top-10 | 147.02 ms | 127.92 ms | **−13.0%** (CI −16.0…−10.3, p=0.00) |

**Two corrections to what is written above:**

1. **The numbers in the table above (96.95 µs / 152.88 ms) and the "−28.6%/−29.8%" figures from the
   first runs of this fix are contaminated.** They were computed against a baseline captured hours
   earlier, and this machine drifted enough over the session to move *unchanged* unfiltered search
   from 89 µs to 168 µs. The honest, same-session A/B number is **−13%**, and it supersedes them.

2. **The audit predicted this fix would be an "order-of-magnitude" win (finding #5). It was not, and
   phase instrumentation shows why.** Within one run: `row_ids_matching` 133–157 ms, `widen_ef` 9 µs,
   `search_filtered` 1.3–1.8 ms. Resolving the row-ids is ~99% of the path, and that cost is **not**
   the 20 MB embedding *copy* the fix removed — it is re-reading the whole ~205 MB data file per query.
   Arrow IPC stores a record batch as one contiguous body and `FileReader` reads all of it before
   decoding, so column projection (also added, `read_batch_columns`) skips array *construction* but
   not the *read* — worth only ~2 ms of ~109 ms. The 20 MB copy and the `HashSet`/sort were real
   waste and worth removing, but they were never the bottleneck.

**The real bottleneck, now located:** ~205 MB re-read from page cache at ~1.5 GB/s on every filtered
query. Removing it needs either a per-snapshot cache of resolved row-ids (snapshots are immutable, so
sound — a memory/latency tradeoff to decide deliberately) or a genuinely column-chunked file format
(the format change `datafile.rs`'s module doc already defers). Both are design decisions, documented
at the call site in `snapshot.rs`. This, not the `mask`/bitset work already landed, is the item that
would actually collapse filtered-search latency.

The earlier `live_ids` sort-vs-`HashSet` debate (see "Conflicts resolved") is settled and moot: the
bitset is order-insensitive, so both the sort and the hash are gone — but it was always a rounding
error next to the file read.

---

## Benchmark coverage gap — the headline finding is currently unmeasurable

**No existing bench can detect finding #1.** `bench/benches/concurrent_commit_bench.rs` runs
`NUM_THREADS = 8` × `COMMITS_PER_THREAD = 50` — **400 commits maximum** — and drives them through
`criterion`'s `iter_batched` with a `setup_dataset` closure, so **the manifest starts empty on every
iteration.** The vector variant is smaller still (8 × 10 = 80 commits).

That measures aggregate throughput against a small, always-reset manifest. It cannot distinguish
"every commit is uniformly slow" from "commit #400 costs 400× commit #1" — which is precisely the
distinction finding #1 turns on.

So the finding that gates the **most expensive** remediation (incremental manifests: a storage-format
change, a decision doc, and a migration path) is the one finding with **zero empirical support**. Fix
that before designing for it.

What's needed is small: one dataset, ~2000 sequential commits, report per-commit latency sampled at
100 / 500 / 1000 / 2000. Roughly 40 lines, no production code touched.

Both outcomes are valuable:

- **Latency curves upward** → finding #1 is real, the decision doc is justified by data, and the
  sample points give you the crossover where it starts to hurt.
- **Latency stays flat** → the O(v²) is theoretical at your scale. Defer that entire design track and
  spend the effort on constant factors instead.

---

## Recommended plan

Ordered by leverage, not by size. The principle: **measure the thing that gates the expensive work
before doing the expensive work.**

**1. Close the benchmark gap above.** Highest leverage in this document, because either result
removes work. Purely additive — a new bench file, no production code.

**2. ~~Take the free win~~ — DONE AND REJECTED. Do not repeat this step.**

Measured and reverted: it costs ~70% on unfiltered vector search. See finding #12. What follows was
the original reasoning, kept because the *calibration* advice in it turned out to be the valuable
part — running a change against the benchmarks before trusting it is exactly what caught this.



```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

Zero code change. Run the existing benches before and after. If the delta is undetectable, that is
itself worth knowing — it tells you the noise floor of your benchmark setup *before* you start
trusting it to adjudicate subtler changes.

**3. One PR of one-liners.** Each independently safe and independently verifiable:

| Change | Site |
|---|---|
| Collapse double hash to a single `insert` | `crates/index/src/graph.rs:274-277` |
| `serde_json::to_writer` instead of `to_string` + `writeln!` | `crates/index/src/delta_log.rs:40` |
| `FileWriter::try_new_buffered` | `crates/storage/src/datafile.rs:29` |
| Drop the unnecessary `.owned()` | `crates/storage/src/encoding.rs:86` |
| `ahash::RandomState` for the group index | `crates/query/src/group_by.rs:219` |

**4. Then `conflicts_with`** (#3) — best value-per-line in the audit and fully self-contained. Budget
for it honestly: it lives in `crates/txn`, so per this project's own conventions it needs a **loom
interleaving test**, not just a unit test. That is most of the work, not the rewrite itself.

**5. Allocation hoisting**, using `group_by.rs:194-210` as the reference implementation:
`build_delta_entries`, `brute_force_search`, `occupied()`, and an `InsertScratch` thread-local.

**6. The `mask()` API addition**, which then unblocks `row_ids_matching` (#5) and the bitset filter (#4).

**7. The four design-gated items**, each with its own decision doc — sequenced *after* step 1, since
step 1 determines whether the first of them is worth writing at all.

### Hold until after step 1

**The saturation counter (#8).** It is the finding most likely to be a net *loss* if changed blindly:
the analysis says the current early-exit already costs roughly what it saves at d=128 and only pays
for itself at d=768. Which side of that line you are on is an empirical question about your actual
embedding dimension and `ef`, not an analytical one.

### Deliberately skipped

The Low-impact micro-opts — `Arc<Path>` in `Dataset`/`Transaction`, `with_capacity` hints,
`f32::total_cmp`, `Field` deep-clones in `append_row_id_column`, `OnceLock` for `mvp_schema`. All
real, none worth PR churn standalone. Fold them in opportunistically when you are already editing
those files for another reason.

One exception worth promoting if you touch `graph.rs` anyway: `f32::total_cmp` is not purely a
micro-opt — the current `partial_cmp(...).unwrap_or(Equal)` comparators (`graph.rs:144,339,675`) are
not a total order under NaN, which `sort_by` does not guarantee sane behaviour for. That one is
correctness-adjacent.
