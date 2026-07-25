# Phase S1 W3 — Design Amendment (post graph-construction-cost merge)

**Date:** 2026-07-25
**Amends:** [`2026-07-24-s1-segment-format-w3-migration-design.md`](2026-07-24-s1-segment-format-w3-migration-design.md)
**Trigger:** Per this project's own re-verification requirement, the design doc's concrete code-level
claims were checked against `crates/index`/`crates/txn` as they stand after merging `main`'s
graph-construction-cost perf work (`HNSW_EF_CONSTRUCTION` 200→100, `SearchScratch` buffer reuse,
`insert_owned`, downcast hoisting, `serde_json::to_writer`) into `feat/phase-s1-segmented-index`. That
work landed 2026-07-25, the day after the design doc was approved. An Opus-tier read of the real code
(not the doc's prose) found four discrepancies that would mislead an implementer following the doc
verbatim, plus several benign drifts worth recording. This amendment is the durable correction; the
base doc's text is left as-is (historical record of what was approved 2026-07-24) and this file takes
precedence wherever the two disagree.

---

## 1. §2 correction — `neighbor_buf` doesn't exist; the buffer-reuse win is already banked

§2 proposes routing `neighbors_into` through a *new* `SearchScratch.neighbor_buf` field, framing this as
eliminating a per-node-visit `Vec` allocation and predicting a **latency improvement** as the
correctness signal for W3.1.

**Reality:** the graph-construction-cost merge already did this. `SearchScratch` (`graph.rs:169`) has
`occupied_buf: Vec<u64>` (not `neighbor_buf`), and `search_layer`'s hot loop already calls
`node.layer(lc).occupied_into(&mut scratch.occupied_buf)` (`graph.rs:295`) against it.
`SlotArray::occupied()` — the allocating method §2 cites as the thing being fixed — is now
`#[allow(dead_code)]`, test-only.

**Corrected instruction:** W3.1's `NodeSource::neighbors_into` for the `Live` variant routes through
the *existing* `scratch.occupied_buf`, not a new field. **The acceptance signal for W3.1 is strict
parity with today's benchmark numbers, not improvement** — the improvement already happened. Do not
treat a flat `search`/`k_nn_search` bench delta as a failure; treat a *regression* as one.

## 2. §2 correction — `NodeSource` needs a deleted-flag accessor

§2's trait sketch has no way to ask "is this node deleted," reasoning that "a segment has no `deleted`
flag" (true — deletion is a manifest-level tombstone for segments). But W3.1 is a **behavior-preserving
refactor that keeps `GraphResidueGuard` alive**, and `search_layer` currently gates on
`!node.is_deleted()` at two sites (`graph.rs:255-256`, `graph.rs:319-320`) *in addition to* the
visibility `filter`. `GraphResidueGuard::drop` (`dataset.rs:673-675`) calls `HnswIndex::remove` →
`Node::mark_deleted` — this is live, tested machinery until W3.2b removes it.

**Corrected trait:**

```rust
pub trait NodeSource {
    fn entry_point(&self) -> Option<(u64, usize)>;
    fn level(&self, local: u64) -> Option<usize>;
    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>);
    fn vector(&self, local: u64) -> Option<&[f32]>;
    fn row_id(&self, local: u64) -> u64;
    fn dimension(&self) -> usize;
    fn is_deleted(&self, local: u64) -> bool { false }   // NEW — default false, SegmentReader takes it unchanged
}
```

The `Live` impl (over `Graph<D>`) overrides this to call the real `Node::is_deleted()`. `SegmentReader`
uses the default. This must land in W3.1, not be discovered as a test failure partway through.

**Also flagged (reentrancy):** `Graph::insert` itself borrows `SEARCH_SCRATCH` at two sites outside
`search_layer` (`graph.rs:498`, `graph.rs:524`; see the reentrancy invariant documented at
`graph.rs:150-167`). No `NodeSource` impl may itself borrow `SEARCH_SCRATCH`, or a `neighbors_into` call
made from inside an active `with_borrow_mut` will panic on a nested borrow. `SegmentReader`'s
bounds-checked slice accessors (§2 of the base doc) don't touch `SEARCH_SCRATCH`, so this is satisfied
by construction as designed — recorded here so it isn't accidentally violated by a future optimization.

## 3. §7 correction — deleting `delta_log.rs` takes load-bearing logic with it

§7 says `crates/index/src/delta_log.rs` is "deleted in W3.2." Literally true of the *file*, but
`DeltaEntry` (`delta_log.rs:18`) is not only the on-disk log format — it is today's in-memory carrier
between `write_phase` and the commit-apply loop, and three pieces of logic currently live alongside it
that W3.2's segment build still needs:

1. **`build_delta_entries`** (`dataset.rs:1401`) — the Arrow `FixedSizeList<Float32>` → `Vec<f32>`
   extraction. W3.2's "build a fresh `Graph<L2>` over just this commit's vectors" needs exactly this
   data, extracted the same way.
2. **Non-finite (NaN/Inf) vector rejection**, enforced inside that same function. Its comment ties the
   justification to delta-log JSON encoding, but the real reason to keep it is that a NaN component
   poisons every distance comparison in `search_layer` (`Candidate::cmp`'s `partial_cmp` fallback,
   `graph.rs:142-147`) — that hazard exists independent of the delta log and doesn't go away with it.
3. **`validate_delta_dimensions`** (called at `dataset.rs:878`, pre-lock) — dimension pre-validation so
   a mid-loop mismatch can't half-mutate the graph. The segment build has the identical hazard (a
   half-built segment must never be fsynced/published), so this check's *purpose* transfers directly
   even though its current call site goes away.

**Corrected instruction:** W3.2 deletes the JSON on-disk log path (`write_delta_log`, `read_delta_log`)
and `replay_index` (`dataset.rs:1353`, sole caller `Dataset::open`). It **relocates** (renaming as
appropriate — "delta" terminology is legacy once there's no log) the vector-extraction function, its
non-finite guard, and the dimension pre-validation into the segment-build call path. None of those three
get deleted; only the log-shaped I/O around them does.

## 4. §7 / §4 correction — segment writer/reader crate ownership must be decided, and the chaos checkpoint site follows from it

§1/§2 place the segment format and `SegmentReader` in `crates/index` (`node_layout.rs` is cited as the
precedent for its aligned-buffer code). §4/W3.2 says to add a chaos checkpoint "immediately after the
segment fsync (existing `chaos-injection` feature mechanism)." These two claims are in tension:
`chaos_checkpoint` is `strata_storage::chaos::chaos_checkpoint`, and **`crates/index` does not depend on
`strata-storage`** (its `Cargo.toml` deps are `anndists`, `arrow`, `serde`, `serde_json`, `thiserror`
only). The base doc never states which crate owns the writer, and the two placements it does specify are
incompatible without either adding a new dependency edge or splitting the work.

**Decision (recorded here, binding for W3.2):** `crates/index` owns **pure, in-memory (de)serialization
only** — `SegmentWriter`/`SegmentReader`-shaped functions that take a built `Graph<D>` (or the
CSR-flat pieces of one) and produce/consume `Box<[u8]>`, with zero file I/O and zero `strata-storage`
dependency. `crates/txn/src/dataset.rs`'s `write_phase` — which already owns file creation, fsync,
`sync_dir`, and depends on `strata-storage` for exactly this purpose with today's row data files — is
where the `.seg` file is actually written, fsynced, and where the new chaos checkpoint is added,
mirroring the existing data-file write pattern instead of introducing a new one. This keeps
`crates/index` dependency-light (consistent with `.claude/rules/vector-index.md`'s framing of it as a
from-scratch, narrowly-scoped implementation) and requires no `crates/index` → `strata-storage` edge, so
none of the `chaos-injection` feature-unification hazard CLAUDE.md warns about (workspace builds pulling
the feature into `strata-storage`) gets pulled into a crate that shouldn't need it.

**New dependency note (not a discrepancy, but undocumented in the base doc):** §1's format needs
`bytemuck` (typed-slice casts) and a CRC32C implementation; neither is a workspace dependency today.
Add both to `crates/index/Cargo.toml` only, and justify them in the W3.2 commit message per CLAUDE.md's
"don't add dependencies without justifying them" rule.

---

## Benign drifts (recorded, no action required beyond awareness)

- **`Graph::insert` signature grew an `alpha: f64` parameter** (`graph.rs:413`, hardcoded to `1.0` by
  `HnswIndex::insert_owned`). Doesn't affect the migration; `insert` still calls the generic search
  functions with `self` as source exactly as §2 describes.
- **The in-lock commit loop calls `insert_owned`, not `insert`** (`dataset.rs:956-966`) — same
  deletion target for W3.2, one call renamed.
- **ADR 0008's recall-vs-segment-count table was measured at `ef_construction=200`**, not today's `100`.
  The bench's own constant was updated to 100 (commit `b986e77`, which flagged this exact gap in its own
  message) but the ADR was not re-run. The qualitative conclusion ("no recall cliff") is very unlikely to
  flip at ef=100, but **W3.3's recall-parity test tolerance must be calibrated against a fresh
  measurement, not against the ADR's stale table.**
- **`search_filtered`'s live-id bitset is rebuilt on every call** (`hnsw.rs:342-346`), not built once and
  shared as §4/W3.3 assumes. Fanning out to K segments naively would rebuild it K times per query.
  W3.3 needs either a new entry point taking a pre-built bitset/filter, or the construction hoisted into
  `SegmentSet::search` — call this out explicitly in the W3.3 task, it's not free.
- **`Manifest` gained `commit_time_high_water: i64`** (W2, after the base doc's §3 sketch was written).
  No conflict with `SegmentEntry`/`Manifest.segments`; the sketch's "existing fields unchanged" is just
  stale, not wrong.
- `HNSW_EF_CONSTRUCTION` 200→100 has no other impact on the design — nothing in §1's per-segment header
  field or §5's loom fixture sizing assumed the old default, and the existing residue loom model already
  uses 1 row / dim 3, inside the doc's stated budget.

---

## Net effect on the W3.1→W3.2a→W3.2b→W3.3 staging

No change to the staging itself (§4 of the base doc). The corrections above are entirely within-stage:
W3.1 gains the `is_deleted` trait method and the corrected buffer-reuse claim; W3.2 gains the
relocate-don't-delete instruction for vector extraction/validation and the crate-ownership decision for
the writer. The chaos-thorough-tier gate, the loom model plan, and the W3.2a/W3.2b `GraphResidueGuard`
sequencing all still apply unchanged.
