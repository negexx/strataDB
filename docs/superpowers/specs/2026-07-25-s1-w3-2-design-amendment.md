# Phase S1 W3.2 — Design Amendment (post W3.1 merge)

**Date:** 2026-07-25
**Amends:** [`2026-07-24-s1-segment-format-w3-migration-design.md`](2026-07-24-s1-segment-format-w3-migration-design.md) §4's
W3.2 description, and supersedes one point of
[`2026-07-25-s1-w3-design-amendment.md`](2026-07-25-s1-w3-design-amendment.md) (the W3.1 amendment).
**Trigger:** Per this project's staged process, W3.2's design was re-verified against the actual code
now that W3.1 (`NodeSource`, `SegmentSet`/`IndexPart`, the empty `Manifest.segments` field) is merged
(PR #31). W3.1 was not a no-op: it added real API surface (`live_arc`, `sole_live`, `build_live_filter`,
a new node-existence admission gate) that neither the base design doc nor the W3.1 amendment could
anticipate, and its own PR review changed what W3.2 will actually encounter. An Opus-tier read of the
current `crates/index`/`crates/txn`/`crates/storage` code found three discrepancies serious enough that
an implementer following the prior docs verbatim would build the wrong thing, plus several smaller
corrections. This amendment is the durable record; it takes precedence over the base doc and the W3.1
amendment wherever they disagree about W3.2.

---

## 1. `SegmentSet`'s enum shape: the two prior docs specify contradictory things — resolve in favor of delete-`Live`

The base design doc §4 justifies making `IndexPart` an enum specifically because "deleting the `Live`
variant in W3.2 [is] a compile error at every remaining call site — the forcing function the migration
wants." The W3.1 plan's own "Scope decision" section instead describes W3.2 as **adding**
`Sealed(Arc<SegmentReader>)` as a new variant alongside `Live` — an additive change. Only the
delete-`Live` shape actually delivers the guarantee the base doc cites; the additive shape does not,
and the current code already demonstrates why.

**What's actually true today:** W3.1's final review found the original `unreachable!()`-based
destructuring didn't achieve the "compile error" property at all, and a fix landed
(`crates/index/src/segment_set.rs`'s `sole_live()`):

```rust
fn sole_live(&self) -> &Arc<HnswIndex> {
    let [part] = self.parts.as_ref() else {
        unreachable!("SegmentSet has exactly one part until W3.2")
    };
    match part {
        IndexPart::Live(index) => index,
    }
}
```

This closes half the gap: the inner `match` is exhaustive over `IndexPart`'s variants, so adding
`Sealed` *does* force a compile error at this one site. But the outer `let [part] = self.parts.as_ref()
else { ... }` is a refutable **arity** check, not a variant check — the moment `SegmentSet` ever holds
two parts (which is the entire point of W3.2: one segment per commit, accumulating), every one of
`sole_live`'s four callers (`search`, `search_filtered`, `established_dimension`, `live_arc`) panics at
runtime on the production search path, with zero compile-time signal anywhere.

**Corrected instruction, binding for W3.2:** W3.2a deletes `IndexPart::Live`, `SegmentSet::from_live`,
`SegmentSet::sole_live`, and `SegmentSet::live_arc` together, in the same commit that introduces
`IndexPart::Sealed` and the first real multi-part `SegmentSet`. Do not add `Sealed` "alongside" `Live` as
a staging step — that additive shape is exactly what silently disables the forcing function (§4's
sub-sequencing for `GraphResidueGuard`, which genuinely does need a two-step land-then-remove approach
per §5, is a different concern and is unaffected by this correction). If W3.2a's own internal staging
needs a transient period where both variants exist, replace the arity-refutable slice pattern with
length-independent iteration (`for part in self.parts.iter() { match part { ... } }`) in that same
transient commit, not later.

## 2. Where `Transaction.graph: Arc<HnswIndex>` comes from once there is no `Live` part — the design doc has no answer, and the obvious answer is wrong

Neither the base doc nor the W3.1 amendment could address this: `SegmentSet::live_arc()` didn't exist
when they were written. It was added during W3.1 specifically because `Dataset::begin()`
(`crates/txn/src/dataset.rs:422`) needs a bare `Arc<HnswIndex>` to seed a new `Transaction`'s own
`graph` field, cloned from the current snapshot.

**The tempting shortcut — keep one `Live` part around specifically for in-flight transactions to write
into, alongside the accumulating `Sealed` parts — is wrong, and would silently defeat §5's central
claim.** §5 argues that once "a snapshot's segment set is exactly its manifest's list," the
`in_flight`/`watermark` machinery that exists solely to hide another transaction's uncommitted-but-
already-graph-resident inserts becomes unnecessary, because there is no shared mutable graph to hide
anything in. A surviving `Live` part *is* exactly that shared mutable graph. Keeping it around to solve
the `Transaction.graph` sourcing problem would let every existing test stay green (nothing exercises the
would-be-reintroduced hazard directly) while quietly falsifying the premise the rest of §5's
simplification stands on.

**Corrected design, binding for W3.2a:** `Transaction` does not hold a live, shared `Arc<HnswIndex>` at
all once W3.2a lands. Concretely:

- **`GraphResidueGuard`, the in-lock apply loop (`self.graph.insert_owned(...)`), and `self.graph.remove(row_id)`
  in the guard's `Drop`** are deleted in W3.2a as the base doc's §4 already describes — nothing new here,
  just confirming these three call sites (`crates/txn/src/dataset.rs` around the `GraphResidueGuard`
  struct, its `Drop` impl, and the commit loop) go away together, and with them the entire reason
  `Transaction` needed a live graph handle.
- **The one surviving consumer, `validate_delta_dimensions(&deltas, &self.graph)`** (pre-lock,
  currently reading `graph.established_dimension()`), must be re-sourced to not need a live graph at
  all. The established dimension is already available without opening any segment file:
  `SegmentEntry.dimension` (`crates/storage/src/manifest.rs`) is recorded per-segment. Change
  `validate_delta_dimensions`'s signature to take a plain `usize` (the established dimension, `0` if
  none yet), sourced from the current snapshot's `SegmentSet`. `SegmentSet::established_dimension()`
  already exists (added in W3.1) but currently has zero production callers — only three `dataset.rs`
  tests use it. Reimplement it over sealed parts once `Sealed` exists (e.g. the first non-empty part's
  recorded `dimension`, `0` if there are no segments yet) so this becomes its first production caller.
- **Net deletions this amendment adds to W3.2a's scope** (not previously enumerated by either prior
  doc): `Transaction.graph: Arc<HnswIndex>` the field itself, `SegmentSet::from_live`,
  `SegmentSet::live_arc`, `Dataset::begin()`'s `live_arc()` call site, and the two `Snapshot`-construction
  sites that currently build a fresh live graph via `replay_index`/`new_hnsw_index`
  (`Dataset::create_with_commit_log_capacity` and `Dataset::open`).

## 3. The segment builder: what it actually builds, what it keys by, and when it runs at all

§4's instruction — "build a fresh `Graph<L2>` over just this commit's vectors keyed by the transaction's
claimed row-ids" — needs three corrections before an implementer starts from it.

### 3a. Build an `HnswIndex`, not a raw `Graph<L2>`

`Graph::insert` takes nine parameters including `unif`, the per-node HNSW level draw, which
`HnswIndex::insert_owned` derives internally from its own monotonic counter
(`crates/index/src/hnsw.rs`'s `row_counter`). Calling `Graph::insert` directly from `crates/txn` means
re-implementing that plumbing outside the type that owns it. Worse: **`HnswIndex.graph` is `pub(crate)`
to `strata-index`**, so `crates/txn` cannot obtain a `&Graph<L2>` to hand to a `crates/index`-owned
serializer even if it built one by hand.

**Corrected instruction:** build a fresh `HnswIndex` via the existing `new_hnsw_index(capacity)` helper
(`crates/txn/src/dataset.rs`) and call `insert_owned` once per delta — structurally identical to today's
`replay_index` loop, minus the file read. Give `crates/index` a new `HnswIndex`-taking serialization
entry point (e.g. `HnswIndex::to_segment_bytes(&self) -> Result<Box<[u8]>, IndexError>`) rather than
anything that takes a `Graph<L2>` directly. `insert_owned` (not `insert`) is already the right call and
needs no change — it's the same method the in-lock loop calls today, and it moves the `Vec<f32>`
straight out of a `DeltaEntry::Insert`, which is exactly what the relocated vector-extraction logic
(§3 of the W3.1 amendment) already produces.

### 3b. Key by segment-local ordinals `0..N`, not global row-ids

§1's on-disk format stores adjacency as segment-local `u32` ordinals plus a separate ascending
`row_ids: [u64; node_count]` mapping array — building the segment's working `HnswIndex` keyed by global
row-ids forces an extra remap pass to produce that array, and costs real memory in the process:
`NodeTable` demand-allocates a fixed-size chunk per 65536-row-id span regardless of how few ids actually
land in it, and ignores its `expected_capacity` hint for that decision. A 10-row commit at, say,
row-id 5,000,000 would allocate a full fresh chunk for a span it uses ten slots of. Keying the segment's
working index `0..N` (N = this commit's row count) instead makes §1's `row_ids` section a direct
positional dump (already ascending by construction, satisfying §1's own load-time assertion), and
confines the working index's `NodeTable` to its first chunk regardless of the commit's actual row-id
range.

### 3c. Not every commit produces a segment

`build_delta_entries` already skips null-vector rows and returns empty when a commit's batch has no
`"vector"` column at all; `write_phase` already returns early for a delete-only transaction. §4's stated
proof criterion — "`manifest.segments.len() == N` after N insert-commits" — only holds for commits that
actually carry vectors. **Corrected criterion:** a commit whose relocated vector-extraction step (see
W3.1 amendment §3) produces zero rows writes no `.seg` file and pushes no `SegmentEntry` at all; this
must be an explicit test (assert `manifest.segments.len()` is unchanged after a vector-less commit), not
an accidental consequence of an empty-segment code path that then needs its own load-time support in
`SegmentReader` for §1's `node_count=0`/`entry_point=u32::MAX` case. Deciding not to write an empty
segment is simpler and is the corrected instruction.

**Unaffected by any of the above:** §5's loom Model 1 (`inject_manifest_commit_failure`, checked inside
`commit_lock` after the apply loop) still correctly models "segment fsynced, manifest not committed"
once the segment build/fsync moves into `write_phase` (pre-lock) as designed. No change needed there.

## 4. `NodeSource`'s new admission-gate precondition applies to `SegmentReader`

W3.1 added a correctness fix (result admission now requires `source.vector(local).is_some()` in
addition to `!is_deleted(local)` and the caller's `filter`) that postdates both prior docs. Binding for
`SegmentReader`: **`vector(local)` must return `None` for any out-of-range or otherwise invalid local
id — never panic, never return garbage** — since a corrupt or truncated segment's adjacency section
naming an out-of-range ordinal must fail closed at this gate rather than crash the search path. §1's
format is already compatible with this (its own §2 sketch already says "bounds-checked slice
arithmetic only"); this amendment upgrades that from a style preference to a stated correctness
requirement. Also note `vector()` is now called twice per visited node (once for distance computation,
once at the admission gate) — §2's "escalate to cached raw pointers only if a benchmark shows the bounds
check matters" guidance now applies against roughly double the call volume it did when written.

## 5. Chaos-checkpoint placement: name the concrete mechanism, not an appeal to an existing pattern that doesn't exist

The W3.1 amendment's §4 said the segment write should go through `crates/txn`'s `write_phase`,
"mirroring the existing data-file write pattern." Checked against the actual code: `crates/txn` does no
raw file I/O today at all, and calls `chaos_checkpoint()` nowhere — every existing checkpoint
(`datafile.rs`'s two, `manifest.rs`'s two) lives *inside* `strata-storage`. There is no existing
"`crates/txn` writes bytes and checkpoints" pattern to mirror.

**Corrected instruction:** add `strata_storage::write_bytes(path: &Path, bytes: &[u8]) -> Result<()>`
alongside the existing `write_batch`, carrying its own `chaos_checkpoint()` call after `sync_all()` —
this keeps every chaos checkpoint site inside `strata-storage`, consistent with today's actual
architecture, rather than introducing a new pattern of `crates/txn` doing raw file I/O plus an explicit
cross-crate checkpoint call. `crates/txn`'s `write_phase` calls this new function the same way it
already calls `write_batch`.

**Also note, not a blocker but must be fixed in the same commit:** `strata-storage`'s chaos-injection
uses one process-global checkpoint counter, and `STRATA_CHAOS_ABORT_AT` counts checkpoints since process
start. Adding a per-commit segment-write checkpoint raises the per-commit checkpoint count from 5 to 6.
`tests/sim/tests/chaos.rs`'s existing comment describing the per-commit checkpoint count and the
`MAX_ABORT_THRESHOLD` constant's justification becomes stale text (the constant's numeric value stays
safely above the new total, so no test behavior breaks) — update the comment in the same PR that adds
the new checkpoint.

## 6. Delta-log deletion's actual blast radius (5 locations, not the 1-2 either prior doc implies)

Removing the delta-log on-disk path touches: `crates/txn/src/dataset.rs` (as already covered),
`crates/storage/src/manifest.rs` (`DataFileEntry.delta_log` — see the note below on why this needs care),
`crates/index/src/lib.rs` (`pub mod delta_log;` and its `pub use` line, plus the crate doc's own first
line, which currently reads "HNSW vector index, append-only delta log"), `crates/index/src/delta_log.rs`
itself, and **`bench/benches/concurrent_commit_bench.rs`**, which neither prior doc named and which will
fail to compile once the delta-log types it references are removed.

**`DataFileEntry.delta_log` removal needs care beyond "delete the field":** it is not
`#[serde(default)]` today, so every manifest on disk stops deserializing the moment it's removed —
acceptable per §0.3 (no backward compatibility is required), but `replay_index`'s
`safe_join(&data_dir, &entry.delta_log)` call and `safe_join`'s own doc comment (which names
`DataFileEntry.delta_log` specifically as the contract it enforces) must be updated in the same commit,
not left referencing a removed field.

## 7. Update this project's own persistent rules in the W3.2 PR

`.claude/CLAUDE.md`'s Conventions section and `.claude/rules/vector-index.md` both currently state, as a
binding correctness rule, "index mutations are an append-only delta log, never in-place graph
mutation." W3.2 replaces the *mechanism* (delta log → durable segment) while preserving the *guarantee*
it exists to serve (atomic row+index commit, no write acknowledged before durability) — but unless both
files are updated in the same PR, the repository's own persistent memory will describe an invariant the
code no longer implements. Per `CLAUDE.md`'s own stated principle ("stale instructions are worse than
missing ones"), update both bullets to describe the segment-based mechanism when W3.2 lands.

## 8. One item from the W3.1 amendment is now resolved — strike it

The W3.1 amendment's "benign drifts" section flagged that `search_filtered`'s live-id bitset was
rebuilt on every call and that W3.3's fan-out would need either a new entry point or a hoist into
`SegmentSet::search` to avoid rebuilding it per segment. **W3.1 already did this hoist**:
`build_live_filter` is a `pub(crate)` free function in `crates/index/src/hnsw.rs`, called once inside
`SegmentSet::search_filtered` before the underlying search. W3.3's fan-out loop can pass the same
resulting filter closure to every part with zero rebuilds. This item is closed; W3.3's plan should not
carry it forward as outstanding work.

---

## Net effect on W3.2's staging

No change to the W3.2a/W3.2b split itself (base doc §4, unchanged by this amendment). Within that
staging: W3.2a now explicitly includes deleting `Live`/`from_live`/`sole_live`/`live_arc` together (not
adding `Sealed` alongside them), re-sourcing `validate_delta_dimensions` off a plain `usize`, building
segments via a fresh `HnswIndex` (not a raw `Graph<L2>`) keyed `0..N`, skipping segment/entry creation
for vector-less commits, adding `strata_storage::write_bytes` with its own chaos checkpoint, and touching
the 5 delta-log-adjacent files listed in §6 together. `SegmentReader` (built in W3.2, per §1/§2 of the
base doc, otherwise unchanged by this amendment) must fail closed on out-of-range `vector()` lookups per
§4 above.
