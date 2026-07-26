# Phase S1 — W4 (Zone-Map Pruning) Design Amendment

**Date:** 2026-07-26
**Amends:** `.claude/docs/design/phase-s1-segmented-index-spec.md` §5.4 (W4) and §5.5 (W5), and
[`2026-07-25-s1-w3-2a-segment-write-path.md`](../plans/2026-07-25-s1-w3-2a-segment-write-path.md)'s
"Scope decision" section (the part re-scoping what it calls "W3.3").
**Trigger:** Per this project's staged process, the remaining S1 work was re-verified against the
actual code now that W3.1, W3.2a, and W3.2b are all merged. An Opus-tier read surfaced a genuine
planning defect — not just a labeling one — plus several concrete implementation corrections. This
amendment resolves them before any W4 plan is written.

---

## 1. "W3.3" is retired. The remaining work is W4, staged W4a/W4b. The recall-parity test is separate, unstaged validation debt on already-shipped code.

The base spec (§5) defines five workstreams, W1–W5. W3's own three internal steps (segment
abstraction, per-commit write path, fan-out search) are now **all shipped** — the third step, real
multi-part fan-out with dedup, was pulled forward into W3.2a itself (see that plan's own "Scope
decision"). That plan then labeled the *leftover* work "W3.3" and described it as: zone-map pruning
(originally W4's job), the `explain`-shaped segment-consultation assertion (also originally W4's own
test, spec §5.4), and the monolithic recall-parity integration test (a W3 exit criterion, spec §5.3).

This produces a real circularity, not just an odd name: the same plan document says W3.3 "consum[es]
W4's populated `SegmentEntry.zone_map`" while separately listing "**W4:** computing or pruning on
`SegmentEntry.zone_map`" as explicitly out of scope. No W4 plan exists. Followed literally, an
implementer would write pruning logic against a field (`SegmentEntry.zone_map`) that is still
unconditionally empty in every code path, and every pruning test would pass vacuously — `should_scan_file`
fails open on a missing key, so "prunes nothing because pruning isn't wired" and "prunes nothing because
there's nothing to prune against" are indistinguishable failure modes.

**Corrected scoping, binding from here forward:**

- **"W3.3" as a label is retired.** W3 is done.
- **The remaining work is W4**, and it is planned as one unit (computation and pruning cannot be
  split across independently-plannable work, because pruning has nothing to consume without
  computation existing first) — but it ships as **two sequenced PRs within that one plan**:
  - **W4a — compute and store.** Aggregate each commit's per-batch `ColumnStats` into its
    `SegmentEntry.zone_map` at segment-build time. Nothing reads it yet. Touches the commit path
    (`write_phase`/`build_and_write_segment`) — flagship-subsystem code — so it gets the same
    behavior-preserving discipline as W3.1: every existing test stays green *unmodified*, plus new
    tests asserting the computed zone map's contents and that it round-trips through `Dataset::open`.
  - **W4b — prune and explain.** Thread a predicate through to skip segments the zone map proves
    can't match, and extend `explain` to report which segments were consulted. Touches only the read
    path (`Snapshot::vector_search`/`explain`), and is correctness-neutral if it fails open (identical
    behavior to "no pruning" — today's actual behavior).

  The reason these are two PRs and not one: unlike W3.2a's fan-out (where deferring shipped a silent
  *recall regression* — a genuine correctness cliff forcing everything into one commit), deferring
  pruning after computation ships just means "no pruning yet," which is indistinguishable from
  today's behavior. There is no correctness cliff here, and the two halves have sharply different
  blast radii (one touches the commit lock's neighborhood, the other doesn't touch it at all).

- **The monolithic recall-parity integration test is pulled out of W4 entirely and lands first, on its
  own, before W4a.** It validates the fan-out that already merged in W3.2a and depends on nothing
  zone-map-related. It has been unlabeled validation debt on shipped code since W3.2a merged, and
  bundling it into a zone-map plan would delay it for a dependency that doesn't exist.

## 2. `crates/index` cannot host the pruning evaluator — it needs a new public API, not "wiring"

The base spec's §3 claims the zone-map evaluator is unchanged, reused wiring: `` `should_scan_file(&entry.zone_map,
predicate)` unchanged ``. That's true of the *function* — `should_scan_file`'s signature
(`&HashMap<String, ColumnStats>, &Predicate) -> bool`) and its `And`/`Or`/leaf fail-open semantics
are exactly compatible with `SegmentEntry.zone_map` — but it is misleading about *where the call can
happen*. `crates/index`'s dependencies are `anndists`, `arrow`, `bytemuck`, `crc32c`, `thiserror` only
— deliberately, per the pre-W3.1 amendment's crate-ownership decision, to keep `crates/index`
dependency-light and avoid pulling `chaos-injection` feature-unification into it. `should_scan_file`
lives in `strata-query`, and `Predicate`/`ColumnStats` cross `strata-storage`/`strata-query` — none of
which `crates/index` can depend on without inverting the project's layering.

**Corrected instruction:** the pruning *decision* (which segments survive a predicate) is made in
`crates/txn`, which already depends on both `strata-query` and `strata-storage`. `crates/index` gains
new, narrow public API on `SegmentSet` to act on a decision made elsewhere — a subset-aware search
entry point (e.g. `SegmentSet::search_pruned`/`search_filtered_pruned` taking a part-selection gate
alongside the existing arguments), not a `Predicate`-aware one. `crates/index` never sees a
`Predicate` or a `ColumnStats`; it only ever sees "search these parts, skip those." This is real new
API surface for W4b to design, not existing wiring to flip on.

## 3. The manifest↔segment-set pairing is positional-only today, and pruning makes that a silent-wrong-results hazard

`Snapshot` holds `manifest: Arc<Manifest>` and `index: SegmentSet` as two independent fields. Their
positional correspondence (`manifest.segments[i]` describes the same segment as `index`'s i-th part)
is maintained by construction (`SegmentSet::from_segments` at open, in manifest order;
`with_appended` at commit, appending to match) and checked only by a `debug_assert_eq!` on `.len()` —
never on identity. Every consumer today (`fan_out`, `established_dimension`) treats all parts
uniformly, so this has never mattered. **A pruning implementation that zips `manifest.segments[i]`'s
zone map to part `i` to decide whether to query it makes this alignment load-bearing for
correctness** — a reordering, a skipped/duplicated append, or any future change that doesn't
preserve strict positional correspondence becomes a silent wrong-results bug (querying the wrong
segment's data under the right segment's zone-map decision, or vice versa), not a panic, not a typed
error.

**Corrected instruction, binding for W4b:** do not implement pruning as two parallel arrays zipped by
index. Instead, make the pairing structural — carry each segment's zone map (or the whole
`SegmentEntry`, or at minimum a stable identifier) alongside the `Arc<SegmentReader>` inside
`IndexPart::Sealed` itself, populated once at the point a `SegmentReader` is constructed (either at
publish time in `write_phase`, or at load time in `load_segments` — pick whichever keeps
`SegmentSet`'s existing construction sites simplest, and record the choice in the W4 plan). This
converts "is the zone map the search sees really this part's own zone map" from an unstated invariant
into a type-level guarantee, and removes the positional-correspondence hazard entirely rather than
just documenting it. If a debug-only cross-check remains worth keeping alongside this (e.g. an
occasional re-validation against `manifest.segments`), that's an addition, not a substitute.

## 4. `ExplainResult` gets new, separate fields — never merge segment counts into the existing row-file counters

`Snapshot::vector_search`'s filtered path calls `widen_ef`, which computes
`explain.scanned.len() / explain.total_files` as a selectivity proxy driving `ef_search` width for
*every* filtered vector search. `ExplainResult` has exactly one construction site and every consumer
only reads its fields, so extending it is additive and safe — but the extension must add **new**
fields (e.g. `segments_scanned`/`segments_skipped`/`segments_total`), never merge segment counts into
`scanned`/`total_files`/`skipped`. Merging would silently change `ef_search` width across every
filtered vector search in the system, an unrelated and unintended behavior change riding along with a
reporting feature.

## 5. Zone-map computation: reuse the existing per-batch stats machinery, but the merge-across-a-commit's-batches step is new code

`write_pending_batches` already computes `ColumnStats` per batch (reusing the same `compute_stats`
that powers row-file zone maps) and a single-point `_timestamp` `ColumnStats` per commit, both fed
into `DataFileEntry`. **Reuse this exact computation** for the segment's zone map — do not
reimplement stats computation. But `build_and_write_segment` currently receives only the row inserts,
not these stats, and no merge-across-batches helper (min-of-mins, max-of-maxes, for a commit that
carries more than one batch) exists anywhere in the codebase yet. W4a must: (a) plumb the per-batch
stats through `write_pending_batches` → `write_phase` → `build_and_write_segment`, and (b) write the
merge helper. Binding merge rule, consistent with the "absent/empty always means must-scan" fail-safe
invariant: **a column appears in the segment's merged zone map only if *every* batch in the commit
contributed stats for that column**; a column any batch is missing stats for is dropped from the
merged map entirely (never partially represented). Note this makes the zone map a conservative
*superset* of the segment's actual row-id range where relevant — the segment holds only rows *with*
vectors, while the source stats come from full batches — which is the correct, safe direction (never
over-prunes).

## 6. Set the right expectation for what "the feature works" looks like — it is not a wall-clock win yet

Measured today: `Snapshot::vector_search`'s filtered path spends ~133-157ms in `row_ids_matching`
(resolving the predicate's matching row-ids by reading every surviving row data file) versus ~1.3-1.8ms
in `search_filtered` itself, on a 100k-row/512-dim dataset. Zone-map pruning only shrinks the
fan-out/search side of that split — it does nothing for `row_ids_matching`, and the *unfiltered*
`vector_search` path (`predicate: None`) has no predicate to prune with at all. **W4b's proof of
"working" is the `explain`-shaped assertion (fewer segments consulted for a selective predicate),
not a wall-clock benchmark.** State this explicitly in the W4 plan so a reviewer doesn't reasonably
conclude the feature accomplishes nothing when the end-to-end latency barely moves.

Also worth noting for the same reason: because one commit produces exactly one segment, a
`_timestamp` zone map is always a single point (min == max) per segment — a timestamp-only predicate
will therefore prune almost perfectly and prove less than it looks like it does. The exit criterion's
own example predicate, `timestamp >= X AND category = Y`, is right precisely because the `category`
conjunct is what actually exercises the compound-predicate evaluator against a real per-segment range
rather than a degenerate point.

## 7. The recall-parity test must be built fresh — `segment_recall_bench.rs` cannot be adapted

The existing `bench/benches/segment_recall_bench.rs` (the ADR 0008 gating de-risk) cannot become the
integration test: it lives in `bench/` and needs the gitignored 100k-row parquet dataset (absent by
default), it calls `HnswIndex` directly rather than `SegmentSet`/`SegmentReader`/`Dataset`, and it
keys its synthetic "segments" by global row-ids — the opposite of production's segment-local `0..N`
keying. The integration-level parity test must be written fresh at the `Dataset`/`Snapshot` level,
using small synthetic multi-segment datasets (the same style the W3.2a plan's own test suite already
established), comparing `Snapshot::vector_search`'s fan-out result against a brute-force reference
over the same points — mirroring `crates/index/src/segment_set.rs`'s existing
`merged_top_k_matches_brute_force_ground_truth_over_the_full_point_set` test, lifted to the
transaction layer.

Separately, ADR 0008's own cited recall table was measured at `ef_construction=200`; the bench's
constant was already updated to `100` (the current production default) in an earlier commit, so a
fresh `cargo bench --bench segment_recall_bench` run (once the parquet dataset is available) is the
only outstanding step to re-validate ADR 0008's table at the current default — this is optional
polish for the recall-parity work, not a blocker, since the new integration test doesn't depend on
the bench or the ADR table at all.

## 8. Minor corrections, recorded for completeness

- `SegmentEntry` should get `#[serde(deny_unknown_fields)]` for consistency with `DataFileEntry` and
  `Manifest` (both already have it, added when the manifest's silent-legacy-open gap was fixed during
  W3.2a's final review). Harmless omission today; worth picking up in W4a while `manifest.rs` is
  already being touched for the `zone_map` population work.
- The base spec's §5.5 (W5 — Manifest-load recovery) was never updated to record that all of its
  content shipped inside W3.2a, even though the W3 design doc's own §5.5 explicitly said to record
  this "when W3 is opened, don't let it be a silent divergence." It wasn't recorded, and this is
  exactly the kind of gap that produced the W3.3/W4 circularity in the first place. A follow-up
  doc-only fix (not part of W4's own plan) should append a status note to §5.5 the same way earlier
  amendments did for other sections.
