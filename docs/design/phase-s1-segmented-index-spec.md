# Phase S1 — Segmented Immutable Index Layout — Design Spec

**Status:** Implemented and closed — see the §9 status note (2026-07-26) for the exit-criteria record.
**Decided by:** [ADR 0008](../decisions/0008-adopt-segmented-index-layout.md) (Accepted) — branching is
mandatory, only possible on a segmented index. The recall-vs-segment de-risk already ran
(`bench/benches/segment_recall_bench.rs`): recall is segment-count-safe, cost is latency.
**Context:** [`scope-addendum-v2.md`](../scope-addendum-v2.md) §1.1–§1.2, [`architecture.md`](../architecture.md)
roadmap S1 row, and [`how-strata-works.md`](../how-strata-works.md) §12 for the conceptual picture.
**Read before implementing:** `.opencode/rules/concurrency-txn-layer.md`, `.opencode/rules/vector-index.md`,
`phase-4-vector-index-spec.md`, `phase-5-mvcc-snapshot-isolation-spec.md`.

---

## 1. Goal

Replace the single monolithic, mutable, in-memory HNSW graph with a set of **immutable index
segments** plus a **segment manifest**, so that:

- a commit produces a new segment rather than mutating a shared graph;
- a search fans out across segments and merges;
- opening the store **loads** segments instead of rebuilding one graph from the change log;
- range/temporal predicates prune whole segments via per-segment **zone maps**.

S1 delivers the *layout* only. It ships **zero** branching features. Branching (Phase B) and segment
compaction/GC (Phase S2) build on this; they are out of scope here.

## 2. Non-goals (deferred, do not build in S1)

- **Branching** (fork / abort / branch-scoped reads / merge) — Phase B, after S1 and after the Phase
  6/7 correctness baseline is confirmed.
- **Compaction / GC of segments** — Phase S2. S1 may accumulate one segment per commit; *bounding*
  the count is S2's job. S1 must not depend on compaction for correctness (the recall de-risk proved
  it doesn't — fan-out is recall-safe regardless of segment count; only latency grows).
- Dataloader, object storage, language bindings — the productization tail.
- Verifiable deletion, staleness tracking, budget-shaped ANN — v3.

## 3. What we are migrating *from* (current monolithic design)

Understand this precisely before touching it; S1 is a **migration**, not greenfield.

- The vector index is one shared `HnswIndex` (a from-scratch lock-free HNSW) held in memory, pointed
  at by every `Snapshot`'s `Arc<HnswIndex>`.
- Each commit appends the new vectors' inserts/tombstones to a **per-data-file delta log** (append-only
  newline-JSON) and applies them to the shared graph inside the commit lock.
- `Dataset::open` **replays every delta log** across every data file to rebuild the whole graph —
  measured at ~36 s for 25k rows in `lifecycle_bench`, the system's single most expensive operation.
- A failed commit's graph inserts are undone by a soft-delete RAII guard (`GraphResidueGuard`), and
  visibility is enforced by a watermark + tombstone set + an in-flight claim registry. **This is the
  machinery that produced this session's two atomicity bugs** — S1 largely dissolves it (see §6).

## 4. Target design

### 4.1 A segment

An **immutable index segment** is a self-contained, serialized HNSW over the vectors of one commit
(or, later, one compaction output). It is written once, fsynced, and never modified. On disk it is a
sibling of the row data files, listed in the manifest.

A segment file contains: the serialized graph structure (nodes, per-level neighbour lists), the
vectors themselves (or a reference to the row data file's vector column — decide during design; the
row file already holds the vectors, so the segment may store only graph structure + row-id mapping to
avoid duplicating embeddings), and a small header with its vector count and dimension.

**Key reframe:** today the delta log is a *change record* that must be *replayed* (re-inserted, an
`O(n·log n)` graph build) to reconstruct the graph. A segment is the *built result* — loading it is
`O(nodes)` deserialization, no distance computation, no graph construction. This is the whole recovery
win, and it is the crux of the format design.

### 4.2 The segment manifest

The manifest already lists row data files with per-column stats. Extend it to also list the index
segments for the current version, each entry carrying: the segment file name, its vector count, and
its **zone map** (per-segment min/max for the timestamp column and any low-cardinality filter columns).
The manifest remains the single source of truth published by the same atomic swap as today.

### 4.3 Fan-out search + merge

A vector search asks **each segment in the current manifest** for its local top-k, then merges the
per-segment results into a global top-k. Visibility (tombstones) is still applied *during*
each segment's traversal, exactly as today — not as a post-filter. **This shape is already prototyped
and measured recall-safe** in `bench/benches/segment_recall_bench.rs`; reuse it directly. For a
filtered search, zone-map pruning (§4.5) first drops segments that cannot match, then the surviving
segments are fanned out over.

### 4.4 Delta-segment write path

A commit, instead of applying inserts to a shared graph inside the lock, **builds a new segment** for
its vectors and adds it to the manifest. Building the segment (the HNSW inserts) is expensive and must
happen **outside** the commit lock, alongside the existing pre-lock data-file write — preserving the
"expensive work is not serialized" property. The in-lock step becomes: conflict-check, add the segment
entry to the new manifest, atomic-swap. This is structurally identical to how row data files are
already committed.

### 4.5 Zone-map pruning

With a timestamp column and compound predicates (the two prerequisites, §5.1–§5.2), a predicate like
`timestamp >= X AND category = Y` is evaluated against each segment's zone map first: any segment whose
min/max range cannot contain a matching row is skipped before its graph is touched. This is the same
mechanism as the existing row-file pruning (`should_scan_file`), extended to index segments. Expose it
through the existing `explain`-style path so it is testable ("this query touched 2 of 40 segments").

### 4.6 Manifest-load recovery

`Dataset::open` loads the segment list from the manifest and deserializes each segment, instead of
replaying delta logs to rebuild one graph. The delta log's role shrinks: it is either removed in favour
of the segment file being the durable record, or kept only as the write-ahead record for a
not-yet-sealed segment (design decision — see §7 Q1). Recovery time should collapse from the measured
~36 s rebuild to a load proportional to segment bytes.

## 5. Workstreams and PR sequencing

Build as vertical slices, one PR each, each ending green. Order is chosen so the low-risk, additive,
independently-useful work lands first and the risky core migration lands on a stable base.

### 5.1 W1 — Compound predicates (additive, no migration risk)

Extend the filter representation from a single flat condition to a small boolean tree (`And`, `Or`,
and the existing leaf comparisons). Evaluate it as a combined boolean column mask over the existing
vectorized filter path (compose per-leaf masks with boolean-and/or kernels — do not chain
`filter_record_batch` calls). Extend file-level pruning (`should_scan_file`) so a compound predicate
can still skip files: a leaf prunes as today, an `And` prunes if *either* side prunes, an `Or` prunes
only if *both* sides prune.
- **Invariants:** none of the concurrency invariants are touched; this is query-layer only.
- **Tests:** correctness vs a naive reference over random compound predicates; a pruning test proving
  an `And` skips files a single leaf could not.
- **Exit:** `timestamp >= X AND category = Y` filters and prunes correctly.

### 5.2 W2 — First-class timestamp column (additive)

Add a system-populated commit-time value per row (analogous to the hidden row-id column). Populated by
the commit path from a single monotonic clock read per commit, so all rows in a commit share a time and
time is non-decreasing across versions. Exposed for predicates and for zone maps.
- **Design decisions to make:** logical vs physical clock (recommend a monotonic commit counter or a
  captured wall-clock at commit — must be non-decreasing and crash-consistent); whether it is a real
  column in the row file or manifest metadata (recommend a real hidden column so predicates work
  uniformly).
- **Invariants:** must not break the existing hidden-row-id-column handling; timestamp is assigned in
  the same place row-ids are, under the same durability guarantee.
- **Tests:** monotonicity across commits; recovery preserves timestamps; a temporal predicate filters.
- **Known gap from W1, resolved here (see
  `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md` §8):** `should_scan_file` fails
  open on a missing-stats column, but `read_batch_columns` hard-errors if a *predicate-referenced*
  column is genuinely absent from a file's schema — flagged as a risk once this workstream's timestamp
  column meant some files could predate it. Resolved as moot: every dataset-creating code path in this
  repo builds fresh, no persisted fixtures exist, and the timestamp column is assigned at commit time
  from a dataset's very first commit — there is no reachable "old file predates the column" scenario
  within one dataset's lifetime under one consistently-versioned build, given this project's existing
  no-backward-compatibility policy. No special missing-column handling is built in W2.
- **Exit:** rows carry a queryable, monotonic commit time.

### 5.3 W3 — Segment format + delta-segment writes + fan-out search (the core migration)

This is the risky one. Do it as a behaviour-preserving refactor first, then flip the mechanism:
1. **Introduce the segment abstraction with a single segment.** Make search go through a "segment set"
   that initially holds exactly one segment = today's whole graph. This is a pure refactor: same
   behaviour, same tests green, but the search path is now fan-out-shaped (over a set of one).
2. **Change the write path to build a per-commit segment** outside the lock and add it to the manifest,
   instead of mutating the shared graph in-lock. Now the segment set grows by one per commit.
3. **Change search to fan out** over the manifest's segment set and merge (the prototyped shape).
- **Invariants (critical):** a row write and its segment commit atomically or neither; no write
  acknowledged until durable; snapshot isolation preserved (a snapshot references a fixed segment set);
  conflicts still typed. **Every step gets a loom test** for the commit/publish interleaving, scoped
  per the rules file.
- **Tests:** recall parity with the pre-migration monolithic baseline (integration test, not just the
  bench); the existing concurrent-commit and snapshot-isolation suites stay green; a failed commit
  leaves no segment in the manifest and nothing searchable.
- **Exit:** commits produce segments, search fans out, recall parity holds.

### 5.4 W4 — Zone-map pruning

Compute each segment's zone map (timestamp + low-cardinality columns) at segment-build time; store it
in the manifest segment entry; prune segments a compound predicate cannot match before fan-out.
- **Tests:** an `explain`-style assertion that a temporal+category predicate touches only the segments
  whose ranges overlap; recall unaffected on the surviving segments.
- **Exit:** a selective temporal query skips whole segments.

> **Status (2026-07-26):** implemented. Shipped in two stages exactly as staged by
> [`2026-07-26-s1-w4-zone-map-design-amendment.md`](../../../docs/superpowers/specs/2026-07-26-s1-w4-zone-map-design-amendment.md):
> W4a (compute+store, PR #36) added `SegmentEntry.zone_map`; W4b (prune+explain,
> `feat/s1-w4b-zone-map-pruning`) added `crates/index`'s opaque per-part zone-map payload and
> `SegmentSet::search_filtered_pruned`, and wired `Snapshot::vector_search` to gate fan-out through
> it via `zone_map_permits_scan` (`strata_query::should_scan_file` underneath). The amendment's
> corrected framing held: `crates/index` never depends on `strata-query`/`strata-storage` — pruning
> decisions are computed in `crates/txn` and passed down as an opaque `Arc<dyn Any + Send + Sync>`
> gate closure.

### 5.5 W5 — Manifest-load recovery

Change `Dataset::open` to deserialize the manifest's segments instead of replaying delta logs to
rebuild. Decide the delta log's residual role (§7 Q1).
- **Tests:** the crash-recovery test still recovers the last committed version; recovery reads the
  same graph state that a rebuild would have (recall parity after reopen); **the chaos harness thorough
  tier still passes** — this is the gate that the cutover did not regress crash recovery.
- **Exit:** `lifecycle_bench` recovery time collapses from ~36 s to a segment load.

> **Status (2026-07-26):** implemented. This entire workstream shipped inside S1 W3.2a (PR #33,
> `Dataset::open` → `load_segments`, reading `manifest.segments` directly — no delta log remains to
> decide a residual role for; it was deleted in the same PR) rather than as its own later step, per
> the W3 design doc's own approved deviation
> (`docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` §4's "Approved
> deviation from the written spec's workstream split"). Recorded here now, late, exactly because that
> doc said to record it "when W3 is opened, don't let it be a silent divergence" and it wasn't — the
> resulting confusion between this section's still-open-looking text and the actual shipped state is
> what produced the "W3.3 vs W4" circularity the 2026-07-26 amendment above had to resolve.

## 6. The snapshot-isolation simplification (benefit + hazard)

The current shared-mutable-graph design forced a delicate mechanism: soft-delete of a failed commit's
inserts (`GraphResidueGuard`), a watermark, and an in-flight claim registry — the exact machinery that
produced this session's two atomicity bugs. **Immutable segments largely dissolve this.** A failed
commit simply does not add its segment to the manifest, exactly as a failed row-data write is already
handled — there is no shared graph to leave residue in, because a segment is only ever *published* via
the manifest swap or not at all.

This is a real win, and also a real hazard during migration: **do not remove the existing safety
machinery until the segment path provably replaces its guarantee.** Migrate the guarantee, then remove
the now-redundant mechanism, each step gated by the loom tests and the chaos harness. The invariant
"a failed transaction leaves neither the row nor the index behind" must hold at every commit of the
migration, not just at the end.

## 7. Open questions to resolve during design (invoke brainstorming / Opus 5-tier review)

1. **Delta log's residual role.** Once segments are the durable built result, is the per-commit delta
   log removed entirely (segment file *is* the record), or kept as a write-ahead log for a segment not
   yet fully written? Affects crash-consistency of a commit interrupted mid-segment-write.
2. **Vectors: duplicate in the segment, or reference the row file?** The row data file already stores
   the vector column. A segment could store only graph structure + row-id mapping and read vectors from
   the row file, avoiding duplication — at the cost of a coupling between segment and row file. Decide.
3. **Segment granularity.** One segment per commit is simplest but produces many tiny segments under
   frequent small commits (the fan-out latency the recall bench measured). S1 accepts this; S2 compacts.
   But confirm the per-commit-segment build cost is acceptable for the common small-commit case.
4. **Serialization format for a segment.** Must be loadable without re-running inserts. Length-prefixed
   binary is the natural choice; note the delta log's JSON was flagged as slow/write-amplifying, so do
   not repeat that here.

## 8. Invariants that must hold at every step (from the rules files)

- No write acknowledged until durable + conflict-checked + visible. No async buffering, ever.
- A row write and its index-segment commit are atomic — both published or neither.
- Snapshot isolation preserved; readers never block writers; a snapshot sees a fixed, consistent
  segment set + row-file set.
- Conflicts surfaced as typed errors naming contested rows.
- Every concurrency-touching change gets a loom interleaving test, scoped per the rules file (never a
  workspace-wide `RUSTFLAGS --cfg loom`).
- Safe Rust by default; any `unsafe` carries a `// SAFETY:` comment.
- Index mutations stay inside the transaction boundary — a segment is only ever published through the
  commit path's manifest swap.

## 9. Phase-level exit criteria

- Fan-out search holds recall parity with the monolithic baseline (integration test + the existing
  recall bench).
- `Dataset::open` recovery drops from full-graph-rebuild (~36 s @ 25k rows, `lifecycle_bench`) to a
  manifest-driven segment load — benchmarked, the number collapses.
- A `timestamp >= X AND category = Y` predicate prunes whole segments before any vector is scanned —
  proven by an `explain`-style test.
- Full workspace suite green; `clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt
  --check` clean; loom green (scoped); **and the chaos harness thorough tier (`STRATA_CHAOS_THOROUGH=1`,
  2000 seeds) still passes** — the migration must not regress crash-recovery correctness.
- Every task reviewed by the Opus reviewer before being marked done (mandatory, per CLAUDE.md).

> **Status (2026-07-26): Phase S1 exit criteria met — closing.** All five workstreams (W1-W5, see §5)
> are merged, plus the two follow-up correctness fixes the migration exposed:
> `Snapshot::is_visible` collapsed to the tombstone check (the row-id in-flight registry the old
> shared-graph design needed is now provably redundant — see `crates/txn/src/row_id.rs`'s module doc
> and loom "Model 3"), and `Snapshot::scan`/`scan_with_predicate` now honor tombstones (a real,
> independently-discovered gap against `docs/design/phase-0-transaction-and-format-spec.md`
> §8, not part of the original S1 scope but found and fixed during it). The chaos harness thorough
> tier ran clean: 2000/2000 seeds, zero invariant violations, ~6.4 minutes
> (`STRATA_CHAOS_THOROUGH=1 cargo test -p strata-sim --test chaos
> thorough_tier_satisfies_the_phase_7_exit_criterion --release`). The recovery-latency bullet above
> (full-graph-rebuild → manifest-driven segment load) is structurally certain — `Dataset::open`'s
> delta-log replay path no longer exists at all, replaced by an `O(bytes)` segment deserialize — but
> no fresh benchmark run recorded a specific number against `lifecycle_bench` as part of closing this
> phase; that re-measurement is optional polish, not a blocker, per the W4 design amendment's own
> note that it's "not a blocker, since the new integration test doesn't depend on the bench or the ADR
> table at all."
>
> **Read this caveat before treating S1 as fully proven under load, not just "the literal exit
> criterion's text is satisfied":** the chaos harness's workload
> (`crates/chaos-worker/src/main.rs`) is insert-only, single-row-per-commit, and constructed so every
> commit succeeds cleanly — it never exercises a delete, an update, a genuine write-write conflict, a
> predicate-filtered `scan_with_predicate`/`vector_search` (zone-map pruning), or a multi-batch
> commit's zone-map merge, all under a crash. So this run proves S1's segment-commit and
> manifest-recovery mechanism is crash-safe for the scenarios it actually drives — it does not yet
> prove the *interaction* between S1's segments and Phase 6's conflict/delete machinery is crash-safe
> under load. That gap, and several other post-S1 correctness-coverage and stale-documentation
> findings the migration left behind, are this session's own audit findings — not yet written down in
> a committed doc — and are the intended starting scope for the Phase 6/7 hardening pass
> `architecture.md`'s "Where this slots" sequencing note calls for after S1; a follow-up plan doc
> should capture them properly before that work starts.

## 10. Process

- Brainstorm the segment format + migration approach *before* writing code; the segment format and the
  W3 cutover are architectural — design them at the Opus 5 tier, review before implementing.
- `writing-plans` for the multi-PR migration; `test-driven-development` for the logic;
  `verification-before-completion` before any "done" claim.
- PRs only, never push to `main`. One workstream per PR, in the §5 order.

## 11. Risk summary

The additive workstreams (W1, W2) are low-risk. The core migration (W3) and recovery cutover (W5) are
where the danger is: they touch the commit path and the snapshot-isolation mechanism that this session
just finished stabilizing. The mitigations are (a) the behaviour-preserving-refactor-first approach in
W3.1, (b) migrating the failed-commit guarantee before removing the old machinery (§6), and (c) the
chaos harness as the non-negotiable gate on every step that touches durability or recovery. Do not
treat the addendum's "near-zero implementation delta" as a schedule — it assumed a greenfield decision;
this is a migration of built subsystems.
