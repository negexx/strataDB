# Review of the OCC / Conflict-Resolution Proposal Document

> Date: 2026-07-23 · Reviewer: Fable 5 (deep-analysis tier), independently re-verified against
> source by the main session · Subject: an externally-supplied audit proposal for the `crates/txn`
> transaction subsystem.
>
> Cross-references the code-verified complexity audit in
> [`2026-07-23-complexity-audit.md`](2026-07-23-complexity-audit.md).

## One-line verdict

The document reads authoritatively but **roughly half of it dissolves on contact with the code.**
It is strongest where it overlaps the prior complexity audit (manifest O(F) growth, the benchmark's
blind spot, group commit as a design-gated item, keeping arc-swap / custom OCC) and weakest exactly
where it asserts specifics — several of its proposals target things the code **already does.** Its
use of type names that do not exist (`TransactionError`, `VersionOverflow`, `InsufficientHistory` as
an error *variant*) suggests it was written without reading `commit_log.rs` or `error.rs`.

The most valuable thing to come out of the review was not about the document at all: verifying its
(misdiagnosed) poison section surfaced a **real, narrow correctness gap** touching the flagship "no
silently stale vector search results" guarantee. See the last section.

## Ground truth (facts every verdict hangs on — all re-verified against source)

- **Tombstone set is already `imbl::HashSet<u64>`** — `snapshot.rs:25`, built at `dataset.rs:179,786`.
  `imbl = "7"` is a direct dependency **of `strata-txn`** (`crates/txn/Cargo.toml:33`) and appears
  **nowhere in `crates/index`**. The clone at `dataset.rs:588` is **O(1)** (imbl-7.0.1
  `hash/set.rs:615-632`, documented "Time: O(1)", a shared-pointer clone).
- **Manifest clone IS O(F+T)** — `dataset.rs:602` deep-clones `Vec<DataFileEntry>` (each = two
  `String`s + a `HashMap<String,ColumnStats>`) plus `Vec<u64>` tombstones. This is the real finding.
- **Commit log stores bare row-ids** — `VecDeque<(u64, Vec<u64>)>` (`commit_log.rs:31`). Already the
  "flat array of 64-bit row IDs" the doc proposes migrating to. `push` is O(1) (`:46-51`).
- **Version arithmetic is already checked** — `latest_version.checked_add(1).ok_or_else(||
  TxnError::ManifestOverflow(...))` at `dataset.rs:575-577`; row-id arithmetic checked at `:711-713,
  :850-851`. No ring-buffer index arithmetic exists (`VecDeque` owns it).
- **`arc-swap` 1.9.2 is real and used** for the snapshot pointer (`Cargo.toml:32`,
  `dataset.rs:74,259,555,660`).
- **Poison recovery exists at exactly one site** — `dataset.rs:552-553`,
  `.unwrap_or_else(PoisonError::into_inner)`, with a written justification at `:545-549`.
- **Loom tests already exist** — `dataset.rs:2818-2952`: conflicting deleters, disjoint deleters,
  writer-vs-reader swap race; plus contested-row assertions (`:2251`) and graph-untouched-on-loss
  (`:2616`).

## Scorecard

| Section | Verdict | Reason |
|---|---|---|
| Intro / 18% exit evidence at small F | **Correct** | Benchmark real, conditions as described; but "reverses vs sequential" overstated — Amdahl says throughput *converges* to sequential, never crosses below |
| Manifest persistent structures | **Half correct / half redundant** | Manifest clone O(F) real (`:602`) and worth fixing; tombstone half already imbl O(1) (`snapshot.rs:25`) — redundant. Neither fixes the O(F+T) JSON-serialize + fsync *also* in-lock |
| Optimistic Lock Coupling | **Wrong** | Ignores the no-undo shared graph and the in-lock fsync; moving durability out breaks single-CAS + recovery; "sub-µs lock hold" is off by 10³–10⁴ |
| Commit-log compression + capacity | **Unfounded premise** | Log already stores flat `Vec<u64>` (`commit_log.rs:31`); "robust row data" is false; 8× capacity ≠ same memory. Blind bump also ignores the recorded telemetry-first policy (`:59-68`) |
| Copy-on-write abort path | **Partially correct, negligible** | One real clone on a rare error arm (`:570`); "zero allocations in critical section" false; types misnamed |
| Poison recovery | **Fact right, threat model wrong** | `into_inner` exists (`:552`), but named targets (CommitLog, manifest pointer) can't be half-mutated; the real hazard (graph residue) is missed, and abort wouldn't fix its Err-path variant |
| Integer safety | **Redundant** | `checked_add` → `ManifestOverflow` already at `:575-577`; no ring-buffer arithmetic; u64 overflow is ~10¹⁶-years noise |
| Pipelined group commits | **Directionally right, underspecified** | Ack-after-fsync preserved (invariant-safe), but version allocation, per-group versioning, and fsync-failure cascades unaddressed; design-gated by the rules file |
| Loom test spec | **Redundant** | All three scenarios already exist (`:2818-2952`); also, arc-swap 1.9.2 isn't loom-instrumentable — the spec doesn't know this |
| Build-vs-buy (arc-swap / ring buffer / OCC) | **Correct** | All three "keep what exists" calls are grounded; zero net change proposed |
| Architectural synthesis | **Wrong headline** | "O(1) critical-section dwell" is impossible while JSON-serialize + fsync + graph-apply stay in-lock |

## What's genuinely actionable (ranked)

1. **Close the benchmark gap first** — the sequential-growth latency curve. Both this doc and the
   prior audit agree the 18% figure can't see O(F) growth. Gates everything else.
2. **Persistent/shared `Manifest.data_files`** (`imbl::Vector` or `Arc<[DataFileEntry]>`
   copy-on-append), folding in the overlooked `Manifest.tombstones: Vec<u64>` (`manifest.rs:55`) —
   the doc's one substantive correct proposal, already better-specified as prior-audit finding #1.
   Full fix (incremental manifests) is a design doc.
3. **`conflicts_with` rewrite** — but per prior-audit #3 (hash the write-set once + `partition_point`
   on the version-sorted entries), *not* the doc's per-entry binary search; budget for the loom test.
4. **Group commit** — as a decision doc addressing version allocation, per-group versioning, and
   fsync-failure cascades; not before item 1 provides data.
5. **Move `write_set` into the error at `dataset.rs:570`** — one line; fold in opportunistically.

**Redundant (already done):** imbl tombstones; checked version/row-id arithmetic; the loom spec;
"compressing" the commit log.
**Wrong on premises:** commit-log "robust row data"; O(T) tombstone clone; poisoning corrupting the
CommitLog/manifest pointer; sub-microsecond lock hold; zero-allocation critical section; O(1) dwell.

---

## Real bug surfaced (independent of the document's quality)

**Failed-commit graph residue becomes search-visible via watermark advancement.** The document
gestured at this in its poison section but misdiagnosed it (blamed the CommitLog and manifest
pointer, which cannot be half-mutated). Re-verified against source by the main session:

**Reachability chain (all confirmed):**

1. Row-ids are assigned from a **Dataset-level `AtomicU64`, advanced eagerly pre-lock** —
   `dataset.rs:705` (`next_row_id_counter.fetch_add(num_rows, SeqCst)` inside `write_pending_batches`).
2. Inside the commit lock, this txn's vectors are inserted into the **shared, in-place, no-undo**
   HNSW graph at `dataset.rs:591` (`self.graph.insert(...)?`), **before** durability.
3. `commit_manifest(&self.dir, &manifest)?` at `dataset.rs:644` can return `Err` (any I/O error —
   ENOSPC, EIO, permissions). The `?` returns from `commit()` **without undoing the graph inserts of
   step 2 and without rolling back the counter of step 1.** The snapshot swap (`:660`) never runs, so
   *this* txn doesn't publish — but the graph mutation and the counter advance both persist in-memory.
4. A **later successful commit** loads the advanced counter (`dataset.rs:618`) and sets
   `watermark = manifest.next_row_id - 1` (`:651`) — now covering the orphan row-ids.
5. `is_visible` (`snapshot.rs:69-71`) is `row_id <= watermark && !tombstoned` — **no manifest
   membership check.** The orphans are not tombstoned and are now under the watermark ⇒ visible.
6. Unfiltered `vector_search` (`snapshot.rs:223-227`) traverses the shared graph filtered only by
   `is_visible` ⇒ **returns orphan row-ids whose data file is in no committed manifest**, so a `scan`
   / random-access read cannot resolve them.

**Refinement:** the *filtered* `vector_search` path (`snapshot.rs:229-234`) is accidentally immune —
it intersects results with `live_ids` resolved from manifest data files, which never contain the
orphans. Only the plain top-k path is exposed.

**Severity:** violates the rules-file invariant *"a transaction that writes a row and updates the
vector index commits both atomically; a crash or conflict mid-transaction must leave neither behind"*
— here a failed transaction leaves the index mutation behind. Directly touches the flagship "no
silently stale vector search results" claim. **Bounded:** in-process only; a restart replays cleanly
from manifest-referenced delta logs (`dataset.rs:777-802`), so it never persists to disk. Not a
concurrency race — a single-threaded `commit` whose `commit_manifest` fails, followed by any later
successful commit, is sufficient to trigger it.

### Resolution (fixed 2026-07-23)

Two coupled changes in `Transaction::commit`:

1. **Graph deltas are applied after `commit_manifest` succeeds**, not before. Nothing between the
   old and new sites reads graph state, and the conflict-detection-before-graph-mutation property
   becomes strictly stronger (the whole manifest build and the durability step now separate them).
2. **`validate_delta_dimensions` is re-run *inside* the commit lock**, before any durable write.

Change 2 is not optional decoration — the first attempt at this fix shipped only change 1, and
**Opus review caught that it was a strict regression.** The reasoning that justified it ("the moved
loop cannot fail, because `validate_delta_dimensions` already ran") was a TOCTOU error: that call
sits at `dataset.rs:540`, *before* the lock at `:550`, and on a graph with no established dimension
it derives `expected` from the transaction's own deltas and accepts anything. Two concurrent
insert-only transactions with different dimensions therefore both pass it, and never conflict with
each other either, because an empty write-set short-circuits to `Clean`. The loser would then fail
*after* `commit_manifest` had made its version durable, leaving a manifest listing two data files
whose delta logs cannot both replay into one graph — **a dataset no future `Dataset::open` can ever
load**, plus a subsequent commit silently renaming over an already-durable version. Reproduced by the
reviewer 299/300 runs. Moving delta application past the durability point is only safe once the
check that guards it is taken under the same lock.

**Tests** (`crates/txn/tests/failed_commit_index_residue.rs`): one single-threaded test for applying
deltas too early, one 25-iteration concurrent test for validating too early. Both were confirmed by
negative control — with the in-lock validation removed, the concurrent test fails on iteration 0 with
`Dataset::open failed with "query has 5 dimensions, but the index expects 3"`.

**Known residual, deliberately not fixed here:** `NodeTable` indexes its chunk directory with a plain
slice index sized for `MAX_ROW_ID_CAPACITY`, and that ceiling is enforced only on the `Dataset::open`
replay path (`dataset.rs:799`) — never on the write path. A session committing past it panics, and
post-change that panic unwinds *after* durability rather than before. Pre-existing, in a different
crate, and requires ~1e9 row-ids in one session; needs either a commit-time cap or a checked lookup
in `crates/index`.
