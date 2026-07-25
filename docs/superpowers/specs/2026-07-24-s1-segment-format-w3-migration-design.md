# Phase S1 — Segment Format & W3 Migration — Design

> **Amended 2026-07-25 (pre-W3.1):** see
> [`2026-07-25-s1-w3-design-amendment.md`](2026-07-25-s1-w3-design-amendment.md) before implementing.
> §2's `neighbor_buf`/latency-improvement claim, the `NodeSource` deleted-flag gap, §7's
> "delete `delta_log.rs`" instruction, and the segment writer's crate ownership are all corrected there
> against the code as it stands after merging `main`'s graph-construction-cost perf work.
>
> **Amended again 2026-07-25 (post-W3.1, pre-W3.2):** see
> [`2026-07-25-s1-w3-2-design-amendment.md`](2026-07-25-s1-w3-2-design-amendment.md) before implementing
> W3.2. Resolves a direct contradiction between this doc's §4 (delete-`Live` enum shape) and the W3.1
> plan (additive-`Sealed` shape) in favor of delete-`Live`; supplies the missing answer for where
> `Transaction.graph` comes from once there is no `Live` part (the design doc was silent — `live_arc()`
> postdates it); and corrects the segment-builder instructions (build via `HnswIndex`, not a
> crate-inaccessible `Graph<L2>`; key by segment-local ordinals; skip vector-less commits).
>
> The text below is left as originally approved; both amendments take precedence where they disagree
> with it, and the second amendment takes precedence over the first for anything concerning W3.2.

**Date:** 2026-07-24
**Trigger:** [`phase-s1-segmented-index-spec.md`](../../../.claude/docs/design/phase-s1-segmented-index-spec.md) §7
and §10 mandate a brainstorm-then-Opus-review pass on the concrete segment format and the W3 cutover
mechanics before any code is written, since the format is hard to reverse and W3 touches the commit
path / snapshot-isolation machinery this project just finished stabilizing. This doc is that pass. It
resolves spec §7's four open questions plus three additional design questions the spec left implicit,
and records one deliberate deviation from the spec's own W3/W5 workstream split (approved below).

**Process:** Live context-gathering (delta log, HNSW node layout, manifest, commit path all mapped
against the real code) → three product-scope decisions made via `superpowers:brainstorming` dialogue →
the concrete format/abstraction/migration design authored by an Opus-5-tier planning pass over that
context → presented back and approved section-by-section. Full transcript context lives in this
session; this doc is the durable record.

---

## 0. Decisions made before the format could be designed

Three of spec §7's questions are product/scope calls, not engineering ones, and were settled first
because the format design depends on them:

1. **Vectors are duplicated in the segment**, not referenced from the row data file. Rejected
   alternative: reference row files by row_id. Arrow IPC's stock `FileReader` has no cheap per-row
   projection — it reads the whole message body regardless of projection — so "reference" would mean
   either paying that cost on every segment build/search, or building a custom projection-aware IPC
   reader plus a vector cache to make search fast. Both are real, orthogonal scope stacked onto the
   riskiest workstream in the phase. Duplication makes a segment fully self-contained at the cost of
   some disk bytes; the custom-IPC-reader idea is filed as a future storage-layer optimization
   (useful beyond segments — e.g. query pruning could reuse it) but is explicitly not part of S1.
2. **The delta log is removed entirely for the index path.** Its only purpose today is being replayed
   to reconstruct a graph that isn't itself persisted. Once a segment *is* the durable built graph —
   built fully outside the commit lock, fsynced, and only referenced by the manifest on success, so an
   interrupted/unfsynced segment write is just an orphaned file nothing points to, exactly like today's
   row data files — there is nothing left for a delta log to do. No intermediate WAL is needed because a
   segment is atomic-by-construction: referenced or not, never partially applied.
3. **No backward compatibility.** This is pre-release; old on-disk datasets in the monolithic format
   are not expected to open under the new code. `Dataset::open` needs no dual-format logic.

These three together are what make the W3/W5 reshuffle in §4 necessary (§0.2/§0.3 mean there is no
delta-log fallback to lean on while segment-loading is deferred to a later workstream).

---

## 1. On-disk segment format

**Format:** fixed 128-byte header, then four aligned sections — `row_ids`, `levels`, `adjacency`,
`vectors` — little-endian, CRC-checked, loadable with `O(bytes)` work: offset/length validation plus
one CRC pass, **zero distance evaluations, zero graph construction**. This is the entire recovery win.

```
header (fixed, 128 B):
  magic "STRTSEG\0" | format_version: u32 | flags: u32        // bit0 = little-endian
  node_count: u32 | dim: u32 | max_level: u32 | entry_point: u32  // u32::MAX = empty
  metric: u8 | m: u16 | mmax0: u16 | mmax: u16 | ef_construction: u16 | m_l: f64
  row_id_min: u64 | row_id_max: u64
  section_off[4]: u64 | section_len[4]: u64      // row_ids, levels, adjacency, vectors
  body_crc32c: u32 | header_crc32c: u32

row_ids  : [u64; node_count]                      // local idx -> global row_id, ASCENDING
levels   : [u8;  node_count]
adjacency: for l in 0..=max_level:
             offsets[l]  : [u32; node_count + 1]
             neighbors[l]: [u32; offsets[l][node_count]]
vectors  : [f32; node_count * dim]                // 64-byte aligned section start
```

**Why CSR-flat over a direct mirror of today's node layout.** The alternative considered was
serializing `node_layout.rs`'s existing per-node block format verbatim (header + inline vector +
per-layer `SlotArray`s), which would reuse tested code almost unchanged. Rejected because it would
permanently bake mutation-era artifacts into an on-disk format S2 compaction and Phase B branching will
read for the life of the product: the `+1` headroom slot per `SlotArray` (needed only for live CAS-based
shrink during concurrent insert) and `EMPTY = u64::MAX` sentinel padding waste roughly 40% of adjacency
bytes on a typical level-0 array and force a sentinel scan instead of a length-known read, and it would
encode neighbours as full `u64` row-ids instead of compact segment-local `u32` ordinals. CSR-flat costs
more new (simple) writer/reader code up front in exchange for a format the rest of the system's future
lives with.

**Accessors are O(1) arithmetic, no sentinel scan:**
`neighbors(i, l) = &neighbors[l][offsets[l][i] .. offsets[l][i+1]]`,
`vector(i) = &vectors[i*dim .. (i+1)*dim]`.

**row_id → local index:** no side table. A segment is built from exactly one transaction's contiguous
row-id claim, so `row_ids` is ascending by construction — reverse lookup is a binary search over the
already-resident array. Assert ascending at load.

**Loading:** read the whole file into a 64-byte-aligned owned `Box<[u8]>` (one small `AlignedBytes`
helper, one `// SAFETY:` comment, following the precedent already established in `node_layout.rs`),
validate once, then hand out typed slices via `bytemuck::cast_slice` (checks alignment/size, no new
unsafe surface beyond the one helper). The layout is deliberately mmap-ready (absolute offsets, aligned
sections) so a future phase can swap `read` for `mmap` without a format change — S1 does a plain read;
mmap is out of scope here and would add a dependency plus a torn-read hazard for no S1-visible benefit.

**Zone map lives in the manifest, not the segment file.** The entire point of a zone map is to skip a
segment *without opening it*; putting the map inside the file being avoided defeats the purpose. The
manifest is already fully resident and already carries `HashMap<String, ColumnStats>` per data file, so
the same type and the same pruning evaluator apply unchanged (§3). Cost: a lone `.seg` file is not
self-describing for an offline repair tool. Accepted — the CLI reads the manifest anyway.

---

## 2. In-memory representation and traversal reuse

A `NodeSource` trait, addressed in `u64` "local ids" universally — for the live graph (present only
transiently during the W3.1 refactor, see §4) the local id *is* the row-id; for a segment it is the
`u32` ordinal, zero-extended. This keeps `SearchScratch`'s existing `HashSet<u64>`/`BinaryHeap<Candidate>`
untouched — no generic index-type parameter threading through the search internals.

```rust
pub trait NodeSource {
    fn entry_point(&self) -> Option<(u64, usize)>;          // (local, level)
    fn level(&self, local: u64) -> Option<usize>;
    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>);
    fn vector(&self, local: u64) -> Option<&[f32]>;
    fn row_id(&self, local: u64) -> u64;
    fn dimension(&self) -> usize;
}
```

`neighbors_into` takes an out-buffer rather than returning a borrowed slice because the live graph
cannot lend one — its slots are individually-CAS'd `AtomicU64`s, so it must snapshot regardless; a
segment's implementation is one `extend_from_slice` memcpy. Routing through a reused
`SearchScratch.neighbor_buf` also removes a per-node-visit `Vec` allocation that exists in today's hot
loop (`SlotArray::occupied()` allocates fresh on every visit) — this step should show a small search
latency *improvement*, not just parity, which is a stronger correctness signal than "no regression."

The visibility filter stays row-id keyed (`Fn(u64) -> bool`), evaluated as `filter(src.row_id(local))` —
visibility and predicates live in the row-id identity domain per the Phase 0 spec; traversal lives in
the local-index domain. `search_layer`/`k_nn_search` move from inherent methods on `Graph<D>` to generic
functions over `&impl NodeSource` (or a `Searcher<'a, S, D>`); **algorithm bodies are unchanged**, only
element access (`self.nodes.get(id)`, `node.layer(lc).occupied()`, `self.entry_point.get()`) becomes a
trait call. `Graph::insert` keeps calling the generic search functions with `self` as the source and is
otherwise untouched — this is the "purely additive" boundary: `Graph<D>` gains a trait impl and loses two
method bodies to free functions; `SlotArray`, `NodeTable`, `Node` are unchanged.

```rust
pub struct SegmentReader {
    bytes: Box<[u8]>,                 // 64-byte aligned, never moves
    node_count: u32, dim: u32, max_level: u32,
    entry: Option<(u32, u32)>,
    params: HnswParams,
    off: SectionOffsets,              // validated once at load
}
impl NodeSource for SegmentReader { /* bounds-checked slice arithmetic only */ }
```

Start with safe bounds-checked accessors; only escalate to cached raw pointers (with a `// SAFETY:`
note, same pattern `Node` already uses) if a benchmark shows the extra bounds check matters on the
layer-0 hot loop. Rejected: a self-referential-struct crate (`ouroboros`/`yoke`) to avoid the bounds
check — a new dependency and real complexity for a cost not yet shown to matter.

A segment has no `deleted` flag — that flag exists solely to undo failed commits on a mutable shared
graph. In the segmented design, deletion is the manifest's versioned tombstone set applied through the
traversal filter (§5).

---

## 3. Manifest extension

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub name: String,               // relative path, "{attempt_id:020}.seg"
    pub format_version: u32,
    pub vector_count: u64,
    pub dimension: u32,
    pub row_id_min: u64,            // inclusive
    pub row_id_max: u64,            // inclusive
    pub byte_len: u64,
    #[serde(default)]
    pub zone_map: HashMap<String, ColumnStats>,   // W3 ships this empty; W4 populates + prunes
}

pub struct Manifest {
    // ...existing fields unchanged...
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
}
```

`DataFileEntry.delta_log` is removed (§0.2, §0.3). `zone_map` deliberately reuses `ColumnStats` and the
existing pruning evaluator so W4 is wiring, not a new mechanism — `should_scan_file(&entry.zone_map,
predicate)` unchanged. Per the spec's own W3/W4 split, *computing* the zone map is W4's job; W3 defines
the field and writes it empty. **Guard invariant, binding for both workstreams:** an absent or empty
`zone_map` must always mean "must scan," never "may prune" — `#[serde(default)]` makes absent and empty
indistinguishable at the type level, and both must fail safe in the pruning evaluator W4 writes.

`format_version` is per-segment (not per-dataset) because segments are immutable and never rewritten — a
future writer must still be able to read today's segments. `byte_len` + `row_id_{min,max}` let
`Dataset::open` reject a truncated file and pre-size before any I/O. There is deliberately no
back-reference from a segment to a data file: coupling a segment to a row file's continued presence is
exactly what §0.1 rejected. The row-id range is informational only, never a read path.

---

## 4. W3.1 → W3.2 → W3.3 migration mechanics

### Approved deviation from the written spec's workstream split

The spec (§5.3, §5.5) assigns `Dataset::open`'s segment-loading logic to **W5**, after W3 lands the
write/search path. Given §0.2/§0.3 above (no delta log, no back-compat), that split doesn't hold: once
`replay_index` and `delta_log.rs` are deleted, there is no fallback for `Dataset::open` to lean on while
segment-loading is deferred — the two changes are not actually separable. **Approved:** fold
`Dataset::open`'s segment-loading into **W3.2**, and redefine **W5 as verification-only** — the
recovery-parity test, the `lifecycle_bench` number, and the chaos-thorough-tier gate, with no new
implementation work of its own. This changes the spec; record it there when W3 is opened, don't let it
be a silent divergence discovered mid-implementation.

### W3.1 — segment set of one (pure refactor, zero behavior change)

`Snapshot.graph: Arc<HnswIndex>` becomes `Snapshot.index: SegmentSet`:

```rust
pub enum IndexPart {
    Live(Arc<HnswIndex>),          // exists only during W3.1; deleted in W3.2
    Sealed(Arc<SegmentReader>),
}
pub struct SegmentSet { parts: Arc<[IndexPart]> }

impl SegmentSet {
    pub fn search(&self, q: &[f32], k: usize, ef: usize,
                  filter: &impl Fn(u64) -> bool) -> Result<Vec<VectorMatch>, IndexError>;
}
```

W3.1's `search` delegates to the single `Live` part. An enum rather than `Arc<dyn NodeSource>` gives
static dispatch on the hot path and, more importantly, makes deleting the `Live` variant in W3.2 a
**compile error at every remaining call site** — the forcing function the migration wants. The write
path is untouched at this step: the in-lock `graph.insert` loop and `GraphResidueGuard` still exist and
still run. The manifest gains `segments: Vec<SegmentEntry>` here too, empty, so W3.2 is purely a write-
and-open-path change, not a format change.

*Proof it's still correct:* full existing suite green with zero test edits, plus a new equivalence test
asserting `SegmentSet::search` over one `Live` part returns results identical to today's
`HnswIndex::search` for a fixed dataset/query set. No new loom model needed (no new concurrency
introduced yet).

### W3.2 — per-commit segment, built outside the lock; `Dataset::open` loads segments

In `write_phase` (already outside `commit_lock`, already the fsync point): after writing data files,
build a fresh `Graph<L2>` over just this commit's vectors keyed by the transaction's claimed row-ids,
serialize to `{attempt_id:020}.seg` (§1's format), fsync, `sync_dir`, emit a `SegmentEntry`. Add a chaos
checkpoint immediately after the segment fsync (existing `chaos-injection` feature mechanism). Inside the
lock, after the conflict check: `manifest.segments.push(entry)` and **no index mutation of any kind.**
The new `Snapshot`'s `SegmentSet` is the previous snapshot's parts plus an `Arc<SegmentReader>` over the
same bytes just fsynced (no read-back needed — same buffer), with a debug-only assertion that re-reading
the file reproduces an identical structure.

Because there is no supported path that produces a durable `Live` part (§0.3: a fresh dataset has none,
pre-S1 datasets are unsupported), this step **deletes** the `Live` variant, `replay_index`,
`delta_log.rs`, and (per the sub-sequencing below) `GraphResidueGuard`, and `SegmentSet` becomes
`Arc<[Arc<SegmentReader>]>` built by `Dataset::open` reading `manifest.segments`. This is consistent
with the spec's own §2 acceptance that "S1 may accumulate one segment per commit," and requires no
historical-data migration because none exists.

**Sub-sequencing within W3.2, per spec §6 ("migrate the guarantee, then remove the mechanism"):**
- **W3.2a** flips the write path to build-and-publish-a-segment while `GraphResidueGuard` stays in the
  code but records nothing (its `Drop` becomes a no-op — there's no shared graph left to leave residue
  in). Land the failed-commit tests below against this state first.
- **W3.2b** deletes `GraphResidueGuard` once those tests are green against the segment-publish path,
  proving the guarantee moved before the old mechanism is removed rather than trusting it moved.

*Proof it's still correct:* `manifest.segments.len() == N` after N insert-commits; the failed-commit
test (§5) in its I/O-failure and typed-conflict flavors; concurrent-commit and snapshot-isolation suites
green; `Dataset::open` after a fresh multi-commit sequence reproduces the same search results as before
reopening.

### W3.3 — real fan-out search

`SegmentSet::search` queries every part for its local top-k at full per-segment `ef` (the shape already
prototyped in `bench/benches/segment_recall_bench.rs` — the over-fetch is *why* recall rises with segment
count per ADR 0008, not an accident to tune away), merges by distance, truncates to k. The live-id
bitset for `search_filtered` is built once per query and shared across segments, not rebuilt per segment.
Dedup by row-id in the merge: unnecessary in S1 (each row-id lives in exactly one segment, since there's
no compaction yet) but implemented now so S2's compaction — where a row transiently exists in both a
source segment and its compacted output — doesn't require reopening the merge logic.

*Proof it's still correct:* an integration-level recall-parity test (not only the bench) — build a
monolithic reference index over the same vectors, assert fan-out recall@10 is within tolerance on a fixed
query set; an `explain`-shaped assertion that every segment was consulted (W4 later asserts fewer are, once
pruning exists).

---

## 5. `GraphResidueGuard`, watermark, in-flight — what replaces them, and the tests that prove it

**Working through whether a published segment can ever contain an individually-invisible row:**

1. *Another transaction's uncommitted rows* — impossible. A segment holds exactly one transaction's own
   claimed row-ids and is referenced by the manifest only after that transaction's `commit_manifest`
   succeeds.
2. *In-flight claims* — `in_flight`/`watermark` exist today because the shared graph received inserts
   before durability, so a concurrent reader needed a way to exclude ids merely *claimed* by another
   in-progress transaction. With segments, a snapshot's segment set is exactly its manifest's list, and a
   concurrent transaction's claimed-but-uncommitted row-ids appear in **no segment on that list** — the
   channel `in_flight` existed to guard against is gone.
3. *Tombstones* — **still needed, still per-row, still evaluated at traversal time.** A row committed in
   segment #3 and deleted at version 12 remains physically present in segment #3 forever (segments are
   immutable; deletion never rewrites one). A snapshot at v11 must still see it; v13 must not. This is
   versioned and cannot collapse to a segment-level check.

`Snapshot::is_visible` has exactly one production consumer (`vector_search`'s two graph-filter closures),
so nothing else keeps `watermark`/`in_flight` alive once this collapses. **Recommendation:** `is_visible`
becomes `!self.tombstones.contains(&row_id)`, and `RowIdAllocator`'s `active` registry (the in-flight
tracking) is deleted, leaving `claim` a plain bounds-checked counter advance — **but as its own PR after
W3.3 is green**, not folded into W3.2. This is the single largest simplification S1 unlocks and the one
most likely to hide a mistake if rushed; it deserves its own diff and its own review pass.

### Tests proving "a failed transaction leaves neither the row nor the index behind"

**Unit/deterministic**, three flavors of the W3.2 failed-commit test: injected I/O failure at
`commit_manifest`, a typed `Conflict`, and a panic between segment fsync and manifest swap (assert via
caught unwind that the dataset is unchanged and a subsequent commit still succeeds). Each asserts: (a)
`Err` returned, (b) `dataset.snapshot().version` unchanged, (c) `vector_search` never returns the
attempted row-id, (d) no manifest entry names the orphaned segment file, (e) reopening the dataset
reproduces (a)-(d), (f) the orphan `.seg` file **does** exist on disk (asserting only "not referenced,"
not "never written" — otherwise the test would accidentally validate the wrong thing).

**Chaos:** the new post-segment-fsync checkpoint means the thorough tier (`STRATA_CHAOS_THOROUGH=1`, 2000
seeds) exercises a real `std::process::abort()` at exactly the dangerous instant — the crash-side twin of
the loom models below, and the spec's non-negotiable gate for this migration.

**loom models**, run per `.claude/rules/concurrency-txn-layer.md`'s scoped-cfg pattern, each commit on a
`spawn_committer`-sized thread (1 MiB stack, within the 5-thread cap):

- **Model 1 — failed commit is invisible.** Thread A runs `commit` with
  `inject_manifest_commit_failure = true`: claims row-ids, builds and fsyncs its segment, returns `Err`
  before the manifest swap. Thread B calls `snapshot()` then `vector_search`. Assert: B's matches contain
  no row-id from A's claimed range; B's `manifest.segments` names no file from A. Root thread after join
  asserts `snapshot().version` is unchanged from pre-commit. Interleavings loom must explore: B's
  `current.load()` landing (i) before A takes `commit_lock`, (ii) between A's segment fsync and A's
  `Err`, (iii) after A's `Err` and any guard-equivalent cleanup.
- **Model 2 — row + segment publish atomically.** A commits successfully; B snapshots and scans plus
  vector-searches concurrently. Assert B never observes a partial state — either the complete pre-commit
  state or the complete post-commit state, never A's row present under the old manifest version or the
  version bumped with A's segment absent. This is close to trivially true once both live in one
  `Manifest` published by a single atomic swap, but it is the entire justification for deleting the old
  guard/registry machinery, so it must be proven, not assumed.
- **Model 3 — gate for deleting `in_flight`.** A and B commit disjoint rows concurrently; the root reads
  afterward and a third reader reads during. Assert no reader ever observes a partial segment set. Must
  stay green both before and after `RowIdAllocator.active` is removed — it's the regression gate for that
  follow-up PR.

**Flagged risk, not papered over:** W3.2's `commit` performs a real (if tiny) HNSW build plus segment
serialization inside a loom model thread. loom's exploration cost is exponential in operation count, and
the existing `COMMIT_STACK_SIZE` was sized empirically against a much smaller commit shape. Keep loom
fixtures to 1-2 rows, dim 2-3. If a model still blows up in time or stack budget, the fallback is
extracting a `publish_segment(&mut Manifest, SegmentEntry)` helper and loom-modeling just that plus the
atomic swap, with the segment build itself stubbed out — a weaker model, but one that still covers the
interleaving that actually matters (publish ordering, not HNSW correctness, which the non-loom test suite
already covers). Decide whether the fallback is needed only after measuring actual loom run time, not
preemptively.

---

## 6. Explicitly not decided here

- **S2 compaction policy** — out of scope per the spec's non-goals; this design's CSR-flat format and
  mmap-readiness are chosen so compaction has room to work later, not to pre-solve it.
- **Verifiable deletion / staleness tracking** — v3 primitives; not designed here, only kept unblocked by
  the manifest/segment shape.
- **Whether `SegmentReader` should mmap instead of read** — flagged as a free future upgrade the format
  supports; not built in S1.

---

## 7. Critical files for implementation

- `crates/index/src/graph.rs` — `search_layer`/`k_nn_search` become generic over `NodeSource`;
  `insert` untouched.
- `crates/index/src/node_layout.rs` — precedent for the segment reader's aligned-buffer/offset-arithmetic
  code to follow.
- `crates/txn/src/dataset.rs` — `write_phase` builds the segment; the in-lock graph-mutation loop,
  `GraphResidueGuard`, and `replay_index` are deleted (W3.2a/b sequencing above).
- `crates/storage/src/manifest.rs` — `SegmentEntry` + `Manifest.segments`; `DataFileEntry.delta_log`
  removed.
- `crates/txn/src/snapshot.rs` — `graph: Arc<HnswIndex>` → `index: SegmentSet`; `is_visible` collapses to
  the tombstone check (as its own follow-up PR, per §5).
- `crates/index/src/delta_log.rs` — deleted in W3.2.
