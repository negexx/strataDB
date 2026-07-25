# S1 W3.2a — Segment Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the delta-log-replayed, shared, mutable `HnswIndex` with a durable per-commit `.seg` segment: `write_phase` builds a fresh `HnswIndex` over just this commit's vectors, serializes it to the on-disk CSR-flat segment format, fsyncs it outside the commit lock, and the in-lock step publishes it as a `SegmentEntry` in the manifest plus an `Arc<SegmentReader>` in the new snapshot's `SegmentSet` — with **no index mutation of any kind inside the lock**, and `Dataset::open` loading segments from `manifest.segments` instead of replaying delta logs.

**Architecture:** `crates/index` gains a pure, in-memory segment codec (`segment_format.rs` constants/`AlignedBytes`, `segment_writer.rs`'s `encode_segment`, `segment_reader.rs`'s `SegmentReader: NodeSource`) with zero file I/O and zero `strata-storage` dependency. `crates/storage` gains `write_bytes` (mirroring `write_batch`, carrying its own chaos checkpoint) so every chaos-checkpoint site stays inside `strata-storage`. `crates/txn`'s `write_phase` builds the segment via a fresh `HnswIndex` keyed by segment-local ordinals `0..N`, writes it through `write_bytes`, and hands `commit` a `(SegmentEntry, Arc<SegmentReader>)` pair to publish atomically with the row data. `SegmentSet` becomes a real multi-part set with basic fan-out search.

**Tech Stack:** Rust (edition 2024), existing `strata-index`/`strata-txn`/`strata-storage` crates, two new `crates/index`-only dependencies (`bytemuck`, `crc32c`), `loom` for the two interleaving models, `cargo test`/`clippy`/`fmt`/`doc` as the verification gate.

## Global Constraints

- Every task must leave `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check` green before its commit. CI additionally runs `cargo doc --workspace --no-deps` and `cargo deny check bans sources advisories` (`.github/workflows/ci.yml`) — both must stay clean.
- Rust edition 2024, `rust-version = "1.90"` (root `Cargo.toml`).
- `unsafe` is permitted **only** in `AlignedBytes` (Task 2), and only with a `// SAFETY:` comment. `unsafe_op_in_unsafe_fn = "deny"` is workspace-wide.
- `unwrap_used`/`expect_used` are `warn` workspace-wide (and `-D warnings` in CI): no `unwrap()`/`expect()` in any non-test code added by this plan.
- The on-disk format is **little-endian by definition**. Both codec files carry a `#[cfg(target_endian = "big")] compile_error!` guard rather than silently producing a mislabeled file.
- **No backward compatibility** (base design doc §0.3): datasets written before this plan are not expected to open. Do not add dual-format logic.
- Binding source documents, in precedence order (later wins):
  1. `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` (base design)
  2. `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md` (pre-W3.1 amendment)
  3. `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` (**post-W3.1 amendment — binding, supersedes both above for W3.2**)
  4. This plan (records the one decision none of the three resolved — see "Scope decision" below)
- Loom runs are scoped per `.claude/rules/concurrency-txn-layer.md`: **never** a workspace-wide `RUSTFLAGS="--cfg loom"`. Use `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`, then run the produced binary directly. Every loom thread that runs a `Transaction::commit` must go through `spawn_committer` (1 MiB stack); loom caps threads at **5 created per execution**, root included.
- `.claude/CLAUDE.md`'s Model dispatch table applies: tasks are written for a **Sonnet-tier implementer** and an **Opus-tier reviewer**. Every architectural decision is recorded here; an implementer should need no further judgment calls.

---

## Scope decision: W3.2a ships basic multi-part fan-out search, not just the write path

The base design doc §4 splits "per-commit segment, built outside the lock" (W3.2) from "real fan-out search" (W3.3). Taken literally that is **not shippable**: the moment W3.2a produces a second segment (i.e. the second vector-carrying commit after it lands), a `SegmentSet::search` that only consults one part silently stops finding rows in every other segment. That is a real recall regression shipped on the integration branch, and it violates this project's own "Vertical slices over layers — every milestone should run end-to-end, however small" principle (`.claude/CLAUDE.md`, Conventions).

**Decision, binding for this plan:** `SegmentSet::search`/`search_filtered` do **basic multi-part fan-out in W3.2a** — query every part for its own top-`k` at the caller's full per-part `ef_search`, map each part's local ordinals back to row-ids, merge by ascending distance, dedup by row-id (keeping the nearest occurrence), truncate to `k`. No zone-map pruning, no segment-count-aware `ef` tuning, no `explain`-style reporting.

**W3.3 is therefore re-scoped to:** zone-map-based segment pruning (consuming W4's populated `SegmentEntry.zone_map`), the integration-level recall-parity test against a monolithic reference index (with tolerance calibrated against a *fresh* measurement at `ef_construction = 100`, not ADR 0008's stale `ef=200` table — see the pre-W3.1 amendment's "benign drifts"), and the `explain`-shaped "which segments were consulted" assertion. The **merge mechanics themselves are this plan's job** and must be tested here.

**Two costs of this decision, accepted and recorded rather than discovered later:**
1. `SegmentSet::with_appended` clones the parts slice on every commit — O(segments) `Arc` clones per commit, O(n²) across a session. Accepted for S1: the spec explicitly accepts "one segment per commit," and S2 compaction is what bounds `n`.
2. Search cost is linear in segment count with no pruning until W4. A single-row-per-commit workload (e.g. `bench/benches/concurrent_commit_bench.rs`) accumulates one segment per row. This is inherent to the accepted S1 layout, not introduced here; do not "fix" it by batching commits or by deferring segment writes — that would violate the no-silent-buffering invariant.

---

## File structure

| File | Task | Responsibility |
|---|---|---|
| `crates/storage/src/datafile.rs` | 1 | `write_bytes` — raw byte file + fsync + chaos checkpoint |
| `crates/storage/src/lib.rs` | 1 | re-export `write_bytes` |
| `tests/sim/tests/chaos.rs` | 1 | per-commit checkpoint-count comment (5 → 6) |
| `crates/index/Cargo.toml` | 2 | `bytemuck`, `crc32c` deps |
| `crates/index/src/segment_format.rs` | 2 | format constants, header field offsets, `SegmentParams`, `align_up`, `AlignedBytes` |
| `crates/index/src/segment_writer.rs` | 2 | `encode_segment` — pure in-memory serializer |
| `crates/index/src/hnsw.rs` | 2, 7 | `HnswIndex::to_segment_bytes`; new `IndexError` variants; `Serde` variant removed |
| `crates/index/src/segment_reader.rs` | 3 | `SegmentReader` + its `NodeSource` impl, fail-closed accessors |
| `crates/index/src/segment_set.rs` | 4, 8 | `IndexPart::Sealed`, fan-out search, `empty`/`from_segments`/`with_appended`; `Live` deleted in 8 |
| `crates/txn/src/dataset.rs` | 5, 6, 9, 10 | `VectorInsert`, segment build/publish, `Dataset::open` segment loading, tests, loom models |
| `crates/txn/src/error.rs` | 6 | `TxnError::CorruptSegment` |
| `crates/storage/src/manifest.rs` | 6 | `DataFileEntry.delta_log` removed |
| `crates/index/src/delta_log.rs` | 7 | deleted |
| `crates/index/src/lib.rs` | 2, 3, 7 | module wiring; delta-log exports + crate doc line removed |
| `bench/benches/concurrent_commit_bench.rs` | 7 | stale `DeltaEntry` doc comment |
| `.claude/CLAUDE.md`, `.claude/rules/vector-index.md` | 11 | "append-only delta log" rule → segment-based mechanism |

---

## Explicitly out of scope (separate follow-up plans)

- **W3.2b:** deleting `GraphResidueGuard` itself. It **stays in the code** in this plan, made inert (base design §4's W3.2a/W3.2b sub-sequencing: "migrate the guarantee, then remove the mechanism").
- **Loom Model 3** and the `RowIdAllocator.active` / `in_flight` / `Snapshot::is_visible` simplification — explicitly its own PR after W3.3 per base design §5.
- **W4:** computing or pruning on `SegmentEntry.zone_map`. This plan writes it empty.
- **W3.3:** zone-map pruning, the monolithic-baseline recall-parity test, the `explain`-style segment-consultation assertion.
- **The chaos thorough tier** (`STRATA_CHAOS_THOROUGH=1`, 2000 seeds). It is W3's **phase-exit gate**, not a step of this plan — the controller runs it after this plan's tasks land and before W3 as a whole is called done (base design §5/§9).

---

### Task 1: `strata_storage::write_bytes` + chaos checkpoint accounting

**Files:**
- Modify: `crates/storage/src/datafile.rs` (add `write_bytes` after `write_batch`, which ends at line 36)
- Modify: `crates/storage/src/lib.rs:12` (re-export)
- Modify: `tests/sim/tests/chaos.rs:15-21` (the `MAX_ABORT_THRESHOLD` doc comment)

**Interfaces:**
- Consumes: `crate::error::Result`, `crate::chaos::chaos_checkpoint` (both already in this module's scope).
- Produces: `pub fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()>` — Task 6's `build_and_write_segment` calls exactly this.

**Why here and not in `crates/txn`:** post-W3.1 amendment §5. `crates/txn` does no raw file I/O today and calls `chaos_checkpoint()` nowhere; every existing checkpoint lives inside `strata-storage`. There is no "`crates/txn` writes bytes and checkpoints" pattern to mirror, so this keeps all six checkpoint sites in one crate.

- [ ] **Step 1: Write the failing test in `crates/storage/src/datafile.rs`**

Add to the existing `#[cfg(test)] mod tests` block (which starts at line 112), after `write_then_read_round_trips`:

```rust
    #[test]
    fn write_bytes_then_read_round_trips_exactly() {
        let dir = tempfile::Builder::new()
            .prefix("strata-write-bytes-test-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("blob.seg");

        // Deliberately includes a zero byte and a high byte: this is a raw
        // binary writer, not a text one, and must not transform anything.
        let payload: Vec<u8> = vec![0x00, 0x53, 0x54, 0xFF, 0x01, 0x00, 0x00, 0x00];
        write_bytes(&path, &payload).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), payload);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_bytes_truncates_an_existing_file_rather_than_appending() {
        // `File::create` semantics, matching `write_batch`. Asserted
        // explicitly because a segment filename is derived from a unique
        // attempt id and must never be reused -- if it ever were, silent
        // appending would produce a file that still passes its own header
        // CRC while carrying trailing garbage.
        let dir = tempfile::Builder::new()
            .prefix("strata-write-bytes-truncate-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("blob.seg");

        write_bytes(&path, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        write_bytes(&path, &[9, 9]).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), vec![9, 9]);
        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p strata-storage write_bytes`
Expected: FAIL — `cannot find function 'write_bytes' in this scope`.

- [ ] **Step 3: Implement `write_bytes` in `crates/storage/src/datafile.rs`**

Add `use std::io::Write as _;` to the imports at the top of the file (after `use std::fs::File;` at line 11, before `use std::path::Path;` at line 12 — `rustfmt` will keep the `std` group ordered). Then add this function immediately after `write_batch`'s closing `}` (line 36) and before `sync_dir`'s doc comment (line 38):

```rust
/// Writes `bytes` to `path` verbatim, fsyncing before returning so the
/// caller can rely on durability once this returns — the raw-byte twin of
/// [`write_batch`], for payloads that are already a finished on-disk format
/// rather than an Arrow batch (today: `crates/index`'s `.seg` segments).
///
/// Lives here, not in the caller, so **every** chaos checkpoint in the
/// commit protocol stays inside `strata-storage` — see
/// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §5.
/// Truncating (`File::create`) exactly like [`write_batch`]: callers derive
/// their filenames from a collision-free attempt id, so an existing file at
/// `path` is a bug to overwrite, not content to append to.
///
/// # Errors
///
/// Returns an error if `path` can't be created, written, or fsynced.
pub fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    crate::chaos::chaos_checkpoint(); // segment content is now durable
    Ok(())
}
```

- [ ] **Step 4: Re-export it from `crates/storage/src/lib.rs`**

Change line 12 from:

```rust
pub use datafile::{read_batch, read_batch_columns, sync_dir, write_batch};
```

to:

```rust
pub use datafile::{read_batch, read_batch_columns, sync_dir, write_batch, write_bytes};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p strata-storage write_bytes`
Expected: PASS — both `write_bytes_then_read_round_trips_exactly` and `write_bytes_truncates_an_existing_file_rather_than_appending`.

- [ ] **Step 6: Update the stale per-commit checkpoint count in `tests/sim/tests/chaos.rs`**

Replace lines 15-21 (the doc comment on `MAX_ABORT_THRESHOLD` and the constant itself) with:

```rust
/// Comfortably above the total number of checkpoints one full run
/// produces. A vector-carrying commit passes through six: `write_batch`'s
/// data-file fsync, `write_bytes`'s segment fsync (added by S1 W3.2a),
/// `sync_dir`'s data-dir fsync, `commit_manifest`'s tmp-sync, its rename,
/// and `sync_dir`'s versions-dir fsync. At 6 per commit and 15 ops max
/// here that is 90 — so a threshold in this range can still land anywhere
/// from "crash on the very first commit" to "never crashes, all ops
/// complete". A delete-only commit produces fewer (no data file, no
/// segment); this workload has none.
///
/// `STRATA_CHAOS_ABORT_AT` counts checkpoints since **process start** off
/// one process-global counter, so this constant must be re-checked
/// whenever a checkpoint site is added or removed anywhere in
/// `strata-storage`.
const MAX_ABORT_THRESHOLD: u64 = 200;
```

(The numeric value is unchanged — 90 is still well under 200, so no test behavior changes. Only the justification was stale.)

- [ ] **Step 7: Verify the whole workspace is still green**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all four clean. `tests/sim`'s `fast_tier_random_seeds_survive_random_crash_points` still passes (nothing about checkpoint *counts* changed yet — `write_bytes` has no caller until Task 6).

- [ ] **Step 8: Commit**

```bash
git add crates/storage/src/datafile.rs crates/storage/src/lib.rs tests/sim/tests/chaos.rs
git commit -m "feat(storage): add write_bytes with its own chaos checkpoint

The raw-byte twin of write_batch, for payloads that are already a
finished on-disk format. Keeps every chaos-checkpoint site inside
strata-storage rather than introducing raw file I/O plus a cross-crate
checkpoint call in crates/txn -- see
docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md section 5.

Also refreshes tests/sim/tests/chaos.rs's per-commit checkpoint-count
comment (5 -> 6); MAX_ABORT_THRESHOLD's value is unchanged and still
safely above the new total."
```

---

### Task 2: Segment format constants, `AlignedBytes`, and the writer

**Files:**
- Modify: `crates/index/Cargo.toml` (add `bytemuck`, `crc32c` under `[dependencies]`, lines 26-31)
- Create: `crates/index/src/segment_format.rs`
- Create: `crates/index/src/segment_writer.rs`
- Modify: `crates/index/src/hnsw.rs` (new `IndexError` variants after line 46; `to_segment_bytes` inside `impl HnswIndex`, before its closing `}` at line 327)
- Modify: `crates/index/src/lib.rs` (module declarations at lines 33-38, `pub use` block at lines 40-46)

**Interfaces:**
- Consumes: `crate::node_source::NodeSource` (existing), `crate::hnsw::IndexError` (existing, extended here), `crate::distance::L2` indirectly via `HnswIndex.graph: Graph<L2>`.
- Produces:
  - `pub const SEGMENT_FORMAT_VERSION: u32 = 1` (re-exported at the crate root) — Task 6 writes it into `SegmentEntry.format_version`.
  - `pub(crate) const HEADER_LEN: usize = 128`, the `OFF_*` header field offsets, `MAGIC`, `FLAG_LITTLE_ENDIAN`, `METRIC_L2`, `VECTORS_ALIGN`, `NO_ENTRY_POINT` — Task 3's reader consumes all of them.
  - `pub(crate) struct SegmentParams { m: usize, mmax0: usize, mmax: usize, ef_construction: usize, m_l: f64 }` — Task 3 reads it back off a loaded header.
  - `pub(crate) fn align_up(value: usize, align: usize) -> usize`.
  - `pub(crate) struct AlignedBytes` with `from_slice(&[u8]) -> Self`, `as_slice(&self) -> &[u8]`, `len(&self) -> usize` — Task 3's `SegmentReader` owns one.
  - `pub(crate) fn encode_segment<S: NodeSource>(source: &S, row_ids: &[u64], params: SegmentParams) -> Result<Box<[u8]>, IndexError>`.
  - `pub fn HnswIndex::to_segment_bytes(&self, row_ids: &[u64]) -> Result<Box<[u8]>, IndexError>` — Task 6 calls exactly this.
  - New `IndexError` variants: `SegmentEmpty`, `SegmentTooLarge(String)`, `SegmentCorrupt(String)`.

**On-disk format (the authority — Task 3's reader must match this byte for byte):**

```
header (fixed, 128 bytes, little-endian):
   0  magic            [u8; 8]   b"STRTSEG\0"
   8  format_version   u32       = SEGMENT_FORMAT_VERSION
  12  flags            u32       bit0 = 1 (little-endian)
  16  node_count       u32       >= 1
  20  dim              u32       >= 1
  24  max_level        u32
  28  entry_point      u32       local ordinal; u32::MAX = none
  32  metric           u8        0 = L2
  33  reserved0        [u8; 1]   zero
  34  m                u16
  36  mmax0            u16
  38  mmax             u16
  40  ef_construction  u16
  42  reserved1        [u8; 6]   zero
  48  m_l              f64
  56  row_id_min       u64
  64  row_id_max       u64
  72  section_off      [u64; 4]  absolute file offsets: row_ids, levels, adjacency, vectors
 104  section_len      [u32; 4]  byte lengths of the same four sections
 120  body_crc32c      u32       CRC32C over bytes[128 ..]
 124  header_crc32c    u32       CRC32C over bytes[0 .. 124]

body:
  row_ids   : [u64; node_count]              STRICTLY ASCENDING, 8-byte aligned start
  levels    : [u8;  node_count]
  adjacency : for l in 0 ..= max_level:            (section start 4-byte aligned)
                offsets_l   : [u32; node_count + 1]   ascending, offsets_l[0] == 0
                neighbors_l : [u32; offsets_l[node_count]]   every value < node_count
  vectors   : [f32; node_count * dim]        section start 64-byte aligned
  inter-section padding is zero-filled and IS covered by body_crc32c.
```

**Two format decisions recorded here, both deviations from the base design §1's sketch:**
1. **`section_len` is `[u32; 4]`, not `[u64; 4]`.** §1's sketch of eight `u64` descriptors plus its named preamble fields totals 136 bytes, which does not fit the "fixed 128-byte header" the same paragraph specifies. Offsets stay `u64` (so the layout stays mmap-ready and future-proof); lengths become `u32`, capping any *single section* at 4 GiB and failing loudly (`IndexError::SegmentTooLarge`) rather than silently. A single commit whose vector section reached 4 GiB would be ~2M rows of 512-dim `f32` in one transaction — far past anything S1 targets.
2. **`entry_point()` reports the entry node's level by reading `levels[entry_point]`, not `max_level`.** These coincide by construction (`EntryPoint::advance_if_higher`), but reading the stored per-node level is exact and does not depend on that invariant surviving a future change.

- [ ] **Step 1: Add the two dependencies to `crates/index/Cargo.toml`**

Change the `[dependencies]` block (lines 26-31) from:

```toml
[dependencies]
anndists = { version = "0.1", features = ["simdeez_f"] }
arrow.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

to:

```toml
[dependencies]
anndists = { version = "0.1", features = ["simdeez_f"] }
arrow.workspace = true
# Checked typed-slice casts for the `.seg` segment codec (u8 <-> u64/u32/f32).
# Already in this workspace's dependency graph via `arrow`, so this adds no
# new crate -- only a direct edge. Chosen over hand-rolled `from_le_bytes`
# loops because it makes the alignment precondition a checked API call
# rather than a comment.
bytemuck = "1"
# CRC32C (Castagnoli) for the segment header/body checksums the format
# requires (segment-format design doc section 1). Hardware-accelerated where
# available with a portable software fallback; no transitive runtime
# dependencies. `crc32fast` is NOT a substitute -- it implements CRC-32
# (IEEE), a different polynomial than the format specifies.
crc32c = "0.6"
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

(`serde`/`serde_json` are removed in Task 7, once `delta_log.rs` — their only consumer — is gone. Leave them here for now or the crate will not compile.)

- [ ] **Step 2: Confirm the dependency footprint is acceptable**

Run: `cargo tree -p strata-index --depth 2 | grep -E "bytemuck|crc32c"`
Expected: `bytemuck v1.x` resolves to the version already in `Cargo.lock` (1.25.2 at the time of writing — no second version), and `crc32c v0.6.x` appears with no runtime dependencies of its own.

Run: `cargo deny check bans sources advisories`
Expected: clean (CI runs exactly this). If `bans` reports a *new* `multiple-versions` warning naming `bytemuck`, that is a genuine finding — report it rather than suppressing it; `multiple-versions` is `warn`, not `deny`, in `deny.toml:143`, so it will not fail the check either way.

- [ ] **Step 3: Create `crates/index/src/segment_format.rs`**

```rust
//! Constants, field offsets and shared helpers for the on-disk immutable
//! segment format. See
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §1 for the format's rationale (CSR-flat rather than a mirror of
//! `node_layout.rs`'s mutation-era per-node blocks), and the S1 W3.2a plan
//! (`docs/superpowers/plans/2026-07-25-s1-w3-2a-segment-write-path.md`) for
//! the two recorded deviations: `section_len` is `[u32; 4]` so the header
//! fits the specified 128 bytes, and the entry point's level is read from
//! the `levels` section rather than assumed equal to `max_level`.
//!
//! This module is the single source of truth shared by
//! [`crate::segment_writer`] and [`crate::segment_reader`]: if a constant
//! here changes, both sides change together by construction.

// The format is little-endian by definition, and both the writer and the
// reader reinterpret host-endian bytes via `bytemuck::cast_slice`.
// Supporting a big-endian host would need an explicit byte-swap pass on
// both sides. No such target is in this project's scope, so fail at compile
// time rather than silently emitting a file whose `flags` bit lies about
// its own byte order.
#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

/// File magic. Eight bytes so the header's first `u32` field lands on a
/// natural boundary.
pub(crate) const MAGIC: [u8; 8] = *b"STRTSEG\0";

/// Per-segment (not per-dataset) format version — segments are immutable
/// and never rewritten, so a future writer must still be able to read a
/// segment written today. Recorded in the manifest's
/// `SegmentEntry.format_version` as well as in the file's own header.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

/// Fixed header size. Also the offset of the first body section, and
/// already 64-byte aligned, which is what lets `row_ids` start immediately
/// after it with no padding.
pub(crate) const HEADER_LEN: usize = 128;

/// `flags` bit 0: the file's multi-byte fields are little-endian.
pub(crate) const FLAG_LITTLE_ENDIAN: u32 = 1;

/// `metric` discriminant for squared-L2 (`crate::distance::L2`). The only
/// metric this crate has; recorded so a future second metric cannot be
/// silently misread.
pub(crate) const METRIC_L2: u8 = 0;

/// `entry_point` sentinel meaning "this segment has no entry point". Never
/// written today (the writer rejects an empty segment outright), but the
/// reader must recognise it rather than treating it as ordinal 4294967295.
pub(crate) const NO_ENTRY_POINT: u32 = u32::MAX;

/// Required alignment of the `vectors` section's start, so a future mmap
/// upgrade can hand out `&[f32]` views with no copy and SIMD-friendly
/// alignment.
pub(crate) const VECTORS_ALIGN: usize = 64;

/// Number of body sections: `row_ids`, `levels`, `adjacency`, `vectors`.
pub(crate) const SECTION_COUNT: usize = 4;

pub(crate) const SECTION_ROW_IDS: usize = 0;
pub(crate) const SECTION_LEVELS: usize = 1;
pub(crate) const SECTION_ADJACENCY: usize = 2;
pub(crate) const SECTION_VECTORS: usize = 3;

// Header field byte offsets. See this module's doc comment and the plan's
// format table; every one of these is < HEADER_LEN by construction.
pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_FORMAT_VERSION: usize = 8;
pub(crate) const OFF_FLAGS: usize = 12;
pub(crate) const OFF_NODE_COUNT: usize = 16;
pub(crate) const OFF_DIM: usize = 20;
pub(crate) const OFF_MAX_LEVEL: usize = 24;
pub(crate) const OFF_ENTRY_POINT: usize = 28;
pub(crate) const OFF_METRIC: usize = 32;
pub(crate) const OFF_M: usize = 34;
pub(crate) const OFF_MMAX0: usize = 36;
pub(crate) const OFF_MMAX: usize = 38;
pub(crate) const OFF_EF_CONSTRUCTION: usize = 40;
pub(crate) const OFF_M_L: usize = 48;
pub(crate) const OFF_ROW_ID_MIN: usize = 56;
pub(crate) const OFF_ROW_ID_MAX: usize = 64;
pub(crate) const OFF_SECTION_OFF: usize = 72;
pub(crate) const OFF_SECTION_LEN: usize = 104;
pub(crate) const OFF_BODY_CRC: usize = 120;
pub(crate) const OFF_HEADER_CRC: usize = 124;

/// The HNSW build parameters a segment records so a reader can describe
/// (and a future compactor can reproduce) how it was built. Carried in the
/// header; not consulted during search, which needs only the graph itself.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SegmentParams {
    pub(crate) m: usize,
    pub(crate) mmax0: usize,
    pub(crate) mmax: usize,
    pub(crate) ef_construction: usize,
    pub(crate) m_l: f64,
}

/// Rounds `value` up to the next multiple of `align`, which must be a
/// non-zero power of two. Saturating rather than wrapping: an overflow here
/// would be a section offset past `usize::MAX`, which the caller's own
/// `SegmentTooLarge` bounds already rule out, and saturating keeps this
/// helper total.
pub(crate) fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two(), "align must be a power of two");
    value.div_ceil(align).saturating_mul(align)
}

/// 64 bytes of storage with 64-byte alignment — the unit `AlignedBytes`
/// allocates in.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Align64([u8; 64]);

/// An owned byte buffer guaranteed to start on a 64-byte boundary, so
/// `bytemuck::try_cast_slice` can hand out `&[u64]`/`&[u32]`/`&[f32]` views
/// of its aligned sub-ranges without ever failing on alignment. Follows the
/// precedent `node_layout.rs` already sets for this crate's aligned-buffer
/// code (base design doc §1: "one small `AlignedBytes` helper, one
/// `// SAFETY:` comment").
///
/// Deliberately not `Vec<u8>`: `Vec<u8>`'s allocation is only 1-byte
/// aligned, so a `&[f32]` cast of its contents can fail at runtime
/// depending on the allocator's whim — an intermittent, environment-
/// dependent failure, which is the worst possible shape for a durability
/// path.
pub(crate) struct AlignedBytes {
    blocks: Vec<Align64>,
    len: usize,
}

impl AlignedBytes {
    /// Copies `src` into a fresh 64-byte-aligned allocation, zero-padding
    /// the tail of the final block.
    pub(crate) fn from_slice(src: &[u8]) -> Self {
        let block_count = src.len().div_ceil(64).max(1);
        let mut blocks = vec![Align64([0_u8; 64]); block_count];
        // SAFETY: `Align64` is `#[repr(C, align(64))]` around exactly one
        // `[u8; 64]` and therefore has size 64 with no padding, so
        // `blocks`'s allocation is a contiguous, fully-initialized run of
        // `block_count * 64` bytes. `blocks.as_mut_ptr()` is valid for
        // writes over that whole run, is uniquely borrowed here (`&mut
        // self`-equivalent: `blocks` is a fresh local), and `u8` has
        // alignment 1, so the cast can never be misaligned. The slice is
        // dropped before `blocks` is moved into `Self`.
        let dst = unsafe {
            std::slice::from_raw_parts_mut(blocks.as_mut_ptr().cast::<u8>(), block_count * 64)
        };
        // Cannot panic: `dst.len()` is `src.len().div_ceil(64) * 64`, which
        // is always `>= src.len()`.
        if let Some(head) = dst.get_mut(..src.len()) {
            head.copy_from_slice(src);
        }
        Self {
            blocks,
            len: src.len(),
        }
    }

    /// The logical contents — exactly `src.len()` bytes, without the
    /// zero padding `from_slice` added to reach a whole block.
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: same layout guarantee as `from_slice` — `blocks` is a
        // contiguous run of `blocks.len() * 64` initialized bytes, `u8` has
        // alignment 1, and `self.len <= blocks.len() * 64` by construction
        // (`from_slice` is the only constructor). The returned slice
        // borrows `self`, so it cannot outlive `blocks`.
        unsafe { std::slice::from_raw_parts(self.blocks.as_ptr().cast::<u8>(), self.len) }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn header_field_offsets_all_fit_inside_the_fixed_header() {
        // The one property every `read_*` in the reader relies on to be
        // panic-free after a single `len >= HEADER_LEN` check.
        assert_eq!(OFF_SECTION_OFF + SECTION_COUNT * 8, OFF_SECTION_LEN);
        assert_eq!(OFF_SECTION_LEN + SECTION_COUNT * 4, OFF_BODY_CRC);
        assert_eq!(OFF_BODY_CRC + 4, OFF_HEADER_CRC);
        assert_eq!(OFF_HEADER_CRC + 4, HEADER_LEN);
    }

    #[test]
    fn the_header_is_already_vector_section_aligned() {
        // Why `row_ids` can start at HEADER_LEN with no padding, and why a
        // future mmap upgrade needs no header change.
        assert_eq!(HEADER_LEN % VECTORS_ALIGN, 0);
        assert_eq!(HEADER_LEN % 8, 0);
    }

    #[test]
    fn align_up_rounds_to_the_next_multiple_and_leaves_exact_multiples_alone() {
        assert_eq!(align_up(0, 64), 0);
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
        assert_eq!(align_up(5, 4), 8);
        assert_eq!(align_up(8, 4), 8);
    }

    #[test]
    fn aligned_bytes_round_trips_its_contents_and_starts_64_byte_aligned() {
        let src: Vec<u8> = (0..100_u8).collect();
        let aligned = AlignedBytes::from_slice(&src);
        assert_eq!(aligned.len(), 100);
        assert_eq!(aligned.as_slice(), src.as_slice());
        assert_eq!(
            aligned.as_slice().as_ptr() as usize % VECTORS_ALIGN,
            0,
            "the whole point of this type"
        );
    }

    #[test]
    fn aligned_bytes_handles_an_empty_input_without_a_zero_sized_allocation() {
        let aligned = AlignedBytes::from_slice(&[]);
        assert_eq!(aligned.len(), 0);
        assert!(aligned.as_slice().is_empty());
    }

    #[test]
    fn aligned_bytes_lets_bytemuck_cast_every_typed_view_the_format_needs() {
        // The property the reader depends on: an aligned base plus
        // aligned-by-construction section offsets means no `try_cast_slice`
        // can fail on alignment.
        let src = vec![0_u8; 256];
        let aligned = AlignedBytes::from_slice(&src);
        let bytes = aligned.as_slice();
        assert!(bytemuck::try_cast_slice::<u8, u64>(&bytes[0..64]).is_ok());
        assert!(bytemuck::try_cast_slice::<u8, u32>(&bytes[64..128]).is_ok());
        assert!(bytemuck::try_cast_slice::<u8, f32>(&bytes[128..256]).is_ok());
    }
}
```

- [ ] **Step 4: Wire `segment_format` into `crates/index/src/lib.rs`**

In the module-declaration block (currently lines 33-38, alphabetically ordered), add `mod segment_format;` immediately after `mod node_table;` and before `mod segment_set;`. In the `pub use` block (lines 40-46), add after the `pub use node_source::NodeSource;` line:

```rust
pub use segment_format::SEGMENT_FORMAT_VERSION;
```

- [ ] **Step 5: Run the new format tests**

Run: `cargo test -p strata-index segment_format`
Expected: all six `segment_format::tests` pass.

Run: `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: clean. If `clippy::len_without_is_empty` fires on `AlignedBytes::len`, add `#[allow(clippy::len_without_is_empty)]` above the `impl AlignedBytes` block with the comment `// An AlignedBytes is only ever built from a whole segment file, which is never empty; an `is_empty` accessor would have no caller.`

- [ ] **Step 6: Add the three new `IndexError` variants in `crates/index/src/hnsw.rs`**

Insert these variants into the `IndexError` enum immediately after the `Serde` variant (lines 45-46), before the closing `}` at line 47:

```rust
    #[error("cannot build a segment with no vectors")]
    SegmentEmpty,
    #[error("segment exceeds the format's size limits: {0}")]
    SegmentTooLarge(String),
    #[error("segment is corrupt or was written by an incompatible writer: {0}")]
    SegmentCorrupt(String),
```

- [ ] **Step 7: Create `crates/index/src/segment_writer.rs`**

```rust
//! Pure, in-memory serialization of a built HNSW graph into the on-disk
//! segment format ([`crate::segment_format`]). **Zero file I/O and zero
//! `strata-storage` dependency by design** — the crate-ownership decision
//! recorded in `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md`
//! §4: `crates/index` produces the bytes, `crates/txn` writes and fsyncs
//! them through `strata_storage::write_bytes` (which carries the chaos
//! checkpoint).
//!
//! The source graph is addressed by **segment-local ordinals `0..N`**, not
//! global row-ids — see
//! `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §3b for
//! why (`row_ids` becomes a direct positional dump that is ascending by
//! construction, and the working index's `NodeTable` stays inside its first
//! 65536-id chunk regardless of the commit's actual row-id range).

use crate::hnsw::IndexError;
use crate::node_source::NodeSource;
use crate::segment_format::{
    FLAG_LITTLE_ENDIAN, HEADER_LEN, MAGIC, METRIC_L2, OFF_BODY_CRC, OFF_DIM, OFF_EF_CONSTRUCTION,
    OFF_ENTRY_POINT, OFF_FLAGS, OFF_FORMAT_VERSION, OFF_HEADER_CRC, OFF_M, OFF_MAGIC, OFF_MAX_LEVEL,
    OFF_METRIC, OFF_MMAX, OFF_MMAX0, OFF_M_L, OFF_NODE_COUNT, OFF_ROW_ID_MAX, OFF_ROW_ID_MIN,
    OFF_SECTION_LEN, OFF_SECTION_OFF, SECTION_COUNT, SEGMENT_FORMAT_VERSION, SegmentParams,
    VECTORS_ALIGN, align_up,
};

#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

fn too_large(what: &str) -> IndexError {
    IndexError::SegmentTooLarge(what.to_string())
}

/// Serializes `source`'s graph — addressed by local ordinals
/// `0..row_ids.len()` — into a complete, CRC-checked segment image.
///
/// `row_ids[local]` is the global row-id that local ordinal `local`
/// represents. It must be strictly ascending, which it is by construction
/// for a segment built from one transaction's contiguous row-id claim with
/// null-vector rows skipped.
///
/// # Errors
///
/// - [`IndexError::SegmentEmpty`] if `row_ids` is empty, if `source` has no
///   established dimension, or if `source` has no entry point. A commit
///   with no vectors must not call this at all — it writes no segment (see
///   the W3.2 amendment §3c).
/// - [`IndexError::SegmentCorrupt`] if `source` is not a well-formed
///   `0..N`-keyed graph: a missing node or vector at some ordinal, a level
///   above 255, an entry point outside `0..N`, or a neighbor ordinal
///   outside `0..N`.
/// - [`IndexError::DimensionMismatch`] if any node's vector length differs
///   from `source.dimension()`.
/// - [`IndexError::SegmentTooLarge`] if any count or section length
///   overflows the format's `u32` fields.
#[allow(clippy::too_many_lines)] // One linear, top-to-bottom encoder; splitting it would
// scatter the format's field order across functions, which is exactly the
// thing that must stay readable in one screenful next to the format table.
pub(crate) fn encode_segment<S: NodeSource>(
    source: &S,
    row_ids: &[u64],
    params: SegmentParams,
) -> Result<Box<[u8]>, IndexError> {
    if row_ids.is_empty() {
        return Err(IndexError::SegmentEmpty);
    }
    let n = row_ids.len();
    let node_count = u32::try_from(n).map_err(|_| too_large("node_count exceeds u32"))?;
    if row_ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(IndexError::SegmentCorrupt(
            "row_ids must be strictly ascending".to_string(),
        ));
    }

    let dim = source.dimension();
    if dim == 0 {
        return Err(IndexError::SegmentEmpty);
    }
    let dim_u32 = u32::try_from(dim).map_err(|_| too_large("dim exceeds u32"))?;

    // Levels, plus the per-node well-formedness checks. Done up front so a
    // malformed source fails before any buffer is sized.
    let mut levels: Vec<u8> = Vec::with_capacity(n);
    for local in 0..u64::from(node_count) {
        let level = source.level(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!(
                "no node at local ordinal {local}: every ordinal in 0..{node_count} must be populated"
            ))
        })?;
        let level = u8::try_from(level).map_err(|_| {
            IndexError::SegmentCorrupt(format!("node {local}'s level {level} exceeds u8"))
        })?;
        let vector = source.vector(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!("no vector at local ordinal {local}"))
        })?;
        if vector.len() != dim {
            return Err(IndexError::DimensionMismatch {
                query_len: vector.len(),
                expected: dim,
            });
        }
        levels.push(level);
    }
    let max_level = usize::from(levels.iter().copied().max().unwrap_or(0));
    let max_level_u32 = u32::try_from(max_level).map_err(|_| too_large("max_level exceeds u32"))?;

    let Some((entry_local, _)) = source.entry_point() else {
        return Err(IndexError::SegmentEmpty);
    };
    let entry_point = u32::try_from(entry_local)
        .ok()
        .filter(|&e| e < node_count)
        .ok_or_else(|| {
            IndexError::SegmentCorrupt(format!(
                "entry point {entry_local} is outside 0..{node_count}"
            ))
        })?;

    // CSR adjacency, one (offsets, neighbors) pair per layer.
    let mut layer_offsets: Vec<Vec<u32>> = Vec::with_capacity(max_level + 1);
    let mut layer_neighbors: Vec<Vec<u32>> = Vec::with_capacity(max_level + 1);
    let mut buf: Vec<u64> = Vec::new();
    for layer in 0..=max_level {
        let mut offsets: Vec<u32> = Vec::with_capacity(n + 1);
        let mut neighbors: Vec<u32> = Vec::new();
        offsets.push(0);
        for local in 0..u64::from(node_count) {
            source.neighbors_into(local, layer, &mut buf);
            for &neighbor in &buf {
                let ordinal = u32::try_from(neighbor)
                    .ok()
                    .filter(|&o| o < node_count)
                    .ok_or_else(|| {
                        IndexError::SegmentCorrupt(format!(
                            "neighbor {neighbor} of node {local} at layer {layer} is outside 0..{node_count}"
                        ))
                    })?;
                neighbors.push(ordinal);
            }
            let end = u32::try_from(neighbors.len())
                .map_err(|_| too_large("a layer's neighbor count exceeds u32"))?;
            offsets.push(end);
        }
        layer_offsets.push(offsets);
        layer_neighbors.push(neighbors);
    }

    // Section sizing and placement. `row_ids` starts at HEADER_LEN, which is
    // already 64-byte (hence 8-byte) aligned, so it needs no padding.
    let row_ids_len = n.checked_mul(8).ok_or_else(|| too_large("row_ids section"))?;
    let levels_len = n;
    let adjacency_len = layer_offsets
        .iter()
        .zip(&layer_neighbors)
        .try_fold(0_usize, |acc, (offsets, neighbors)| {
            let per_layer = offsets
                .len()
                .checked_add(neighbors.len())
                .and_then(|elems| elems.checked_mul(4))?;
            acc.checked_add(per_layer)
        })
        .ok_or_else(|| too_large("adjacency section"))?;
    let vectors_len = n
        .checked_mul(dim)
        .and_then(|elems| elems.checked_mul(4))
        .ok_or_else(|| too_large("vectors section"))?;

    let off_row_ids = HEADER_LEN;
    let off_levels = off_row_ids
        .checked_add(row_ids_len)
        .ok_or_else(|| too_large("levels offset"))?;
    let off_adjacency = align_up(
        off_levels
            .checked_add(levels_len)
            .ok_or_else(|| too_large("adjacency offset"))?,
        4,
    );
    let off_vectors = align_up(
        off_adjacency
            .checked_add(adjacency_len)
            .ok_or_else(|| too_large("vectors offset"))?,
        VECTORS_ALIGN,
    );
    let total = off_vectors
        .checked_add(vectors_len)
        .ok_or_else(|| too_large("total file length"))?;

    let mut out = vec![0_u8; total];

    // --- body ---
    write_at(&mut out, off_row_ids, bytemuck::cast_slice::<u64, u8>(row_ids))?;
    write_at(&mut out, off_levels, &levels)?;

    let mut cursor = off_adjacency;
    for (offsets, neighbors) in layer_offsets.iter().zip(&layer_neighbors) {
        let offsets_bytes = bytemuck::cast_slice::<u32, u8>(offsets);
        write_at(&mut out, cursor, offsets_bytes)?;
        cursor += offsets_bytes.len();
        let neighbors_bytes = bytemuck::cast_slice::<u32, u8>(neighbors);
        write_at(&mut out, cursor, neighbors_bytes)?;
        cursor += neighbors_bytes.len();
    }

    let mut cursor = off_vectors;
    for local in 0..u64::from(node_count) {
        let vector = source.vector(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!("no vector at local ordinal {local}"))
        })?;
        let vector_bytes = bytemuck::cast_slice::<f32, u8>(vector);
        write_at(&mut out, cursor, vector_bytes)?;
        cursor += vector_bytes.len();
    }

    // --- header ---
    write_at(&mut out, OFF_MAGIC, &MAGIC)?;
    write_at(&mut out, OFF_FORMAT_VERSION, &SEGMENT_FORMAT_VERSION.to_le_bytes())?;
    write_at(&mut out, OFF_FLAGS, &FLAG_LITTLE_ENDIAN.to_le_bytes())?;
    write_at(&mut out, OFF_NODE_COUNT, &node_count.to_le_bytes())?;
    write_at(&mut out, OFF_DIM, &dim_u32.to_le_bytes())?;
    write_at(&mut out, OFF_MAX_LEVEL, &max_level_u32.to_le_bytes())?;
    write_at(&mut out, OFF_ENTRY_POINT, &entry_point.to_le_bytes())?;
    write_at(&mut out, OFF_METRIC, &[METRIC_L2])?;
    write_at(
        &mut out,
        OFF_M,
        &u16::try_from(params.m)
            .map_err(|_| too_large("m exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_MMAX0,
        &u16::try_from(params.mmax0)
            .map_err(|_| too_large("mmax0 exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_MMAX,
        &u16::try_from(params.mmax)
            .map_err(|_| too_large("mmax exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_EF_CONSTRUCTION,
        &u16::try_from(params.ef_construction)
            .map_err(|_| too_large("ef_construction exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(&mut out, OFF_M_L, &params.m_l.to_le_bytes())?;
    // Non-empty and strictly ascending, both checked above.
    let (Some(&row_id_min), Some(&row_id_max)) = (row_ids.first(), row_ids.last()) else {
        return Err(IndexError::SegmentEmpty);
    };
    write_at(&mut out, OFF_ROW_ID_MIN, &row_id_min.to_le_bytes())?;
    write_at(&mut out, OFF_ROW_ID_MAX, &row_id_max.to_le_bytes())?;

    let section_offs = [off_row_ids, off_levels, off_adjacency, off_vectors];
    let section_lens = [row_ids_len, levels_len, adjacency_len, vectors_len];
    for i in 0..SECTION_COUNT {
        let off = u64::try_from(section_offs[i]).map_err(|_| too_large("section offset"))?;
        write_at(&mut out, OFF_SECTION_OFF + i * 8, &off.to_le_bytes())?;
        let len = u32::try_from(section_lens[i]).map_err(|_| {
            IndexError::SegmentTooLarge(format!(
                "section {i} is {} bytes, which exceeds this format's u32 section length",
                section_lens[i]
            ))
        })?;
        write_at(&mut out, OFF_SECTION_LEN + i * 4, &len.to_le_bytes())?;
    }

    // CRCs last: the body CRC covers every byte after the header (padding
    // included, which is why the buffer is zero-initialized), and the header
    // CRC covers the header up to but excluding itself.
    let body_crc = crc32c::crc32c(out.get(HEADER_LEN..).unwrap_or(&[]));
    write_at(&mut out, OFF_BODY_CRC, &body_crc.to_le_bytes())?;
    let header_crc = crc32c::crc32c(out.get(..OFF_HEADER_CRC).unwrap_or(&[]));
    write_at(&mut out, OFF_HEADER_CRC, &header_crc.to_le_bytes())?;

    Ok(out.into_boxed_slice())
}

/// Copies `src` into `out` at `at`, or reports a corrupt/oversized layout
/// rather than panicking on an out-of-range slice index. Every call site
/// computed `at` from this function's own sizing arithmetic, so a failure
/// here is an encoder bug, not bad input — but it must surface as a typed
/// error, not a panic on the commit path.
fn write_at(out: &mut [u8], at: usize, src: &[u8]) -> Result<(), IndexError> {
    let end = at.checked_add(src.len()).ok_or_else(|| too_large("write offset"))?;
    let dst = out.get_mut(at..end).ok_or_else(|| {
        IndexError::SegmentCorrupt(format!(
            "encoder tried to write {} bytes at offset {at} of a {}-byte buffer",
            src.len(),
            out.len()
        ))
    })?;
    dst.copy_from_slice(src);
    Ok(())
}
```

- [ ] **Step 8: Add `HnswIndex::to_segment_bytes` in `crates/index/src/hnsw.rs`**

Add this method inside `impl HnswIndex`, immediately after `search_filtered` (which ends at line 326) and before the block's closing `}` at line 327:

```rust
    /// Serializes this index into a complete on-disk segment image, in
    /// memory — no file I/O (see [`crate::segment_writer`]'s module doc for
    /// why the write/fsync lives in `crates/txn` instead).
    ///
    /// This index must be a **fresh, per-commit index keyed by
    /// segment-local ordinals `0..row_ids.len()`**, built by calling
    /// [`Self::insert_owned`] once per vector with `local` as the key —
    /// *not* the dataset's global row-ids. `row_ids[local]` supplies the
    /// global row-id each ordinal stands for, and must be strictly
    /// ascending. See
    /// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §3b.
    ///
    /// # Errors
    ///
    /// See [`crate::segment_writer::encode_segment`]: [`IndexError::SegmentEmpty`]
    /// for an empty `row_ids` or an index with no vectors,
    /// [`IndexError::SegmentCorrupt`] for a graph that is not a well-formed
    /// `0..N` keying, [`IndexError::DimensionMismatch`] for a ragged vector,
    /// and [`IndexError::SegmentTooLarge`] if the image would overflow the
    /// format's `u32` fields.
    pub fn to_segment_bytes(&self, row_ids: &[u64]) -> Result<Box<[u8]>, IndexError> {
        crate::segment_writer::encode_segment(
            &self.graph,
            row_ids,
            crate::segment_format::SegmentParams {
                m: self.m,
                mmax0: self.mmax0,
                mmax: self.mmax,
                ef_construction: self.ef_construction,
                m_l: self.m_l,
            },
        )
    }
```

- [ ] **Step 9: Wire `segment_writer` into `crates/index/src/lib.rs`**

Add `mod segment_writer;` to the module-declaration block, immediately after `mod segment_set;`. No `pub use` — `encode_segment` is `pub(crate)`; the public entry point is `HnswIndex::to_segment_bytes`.

- [ ] **Step 10: Add the writer's own tests to `crates/index/src/segment_writer.rs`**

Append to the file:

```rust
#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
mod tests {
    use super::*;
    use crate::hnsw::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    use crate::segment_format::{OFF_HEADER_CRC, SECTION_ADJACENCY, SECTION_VECTORS};

    /// A fresh index keyed by segment-local ordinals `0..n`, exactly as
    /// `crates/txn`'s segment builder will key it. Quasi-random,
    /// non-collinear coordinates for the same reason `hnsw.rs`'s own
    /// `insert_cluster` uses them: collinear points let the diversity
    /// heuristic prune the graph into a degenerate shape.
    #[allow(clippy::cast_possible_truncation)]
    fn local_keyed_index(n: usize) -> HnswIndex {
        const PHI: f64 = 0.618_033_988_749_895;
        const SQRT2: f64 = 0.414_213_562_373_095;
        const SQRT3: f64 = 0.732_050_807_568_877;
        let index = HnswIndex::new(
            MaxConnections(4),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(20),
        )
        .unwrap();
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        ((f * PHI).fract() * 1000.0) as f32,
                        ((f * SQRT2).fract() * 1000.0) as f32,
                        ((f * SQRT3).fract() * 1000.0) as f32,
                    ],
                )
                .unwrap();
        }
        index
    }

    fn read_u32(bytes: &[u8], at: usize) -> u32 {
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(&bytes[at..at + 4]);
        u32::from_le_bytes(buf)
    }

    fn read_u64(bytes: &[u8], at: usize) -> u64 {
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(&bytes[at..at + 8]);
        u64::from_le_bytes(buf)
    }

    #[test]
    fn header_records_the_magic_version_and_geometry() {
        let index = local_keyed_index(20);
        let row_ids: Vec<u64> = (100..120).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert_eq!(&bytes[0..8], &MAGIC);
        assert_eq!(read_u32(&bytes, OFF_FORMAT_VERSION), SEGMENT_FORMAT_VERSION);
        assert_eq!(read_u32(&bytes, OFF_FLAGS), FLAG_LITTLE_ENDIAN);
        assert_eq!(read_u32(&bytes, OFF_NODE_COUNT), 20);
        assert_eq!(read_u32(&bytes, OFF_DIM), 3);
        assert_eq!(bytes[OFF_METRIC], METRIC_L2);
        assert_eq!(read_u64(&bytes, OFF_ROW_ID_MIN), 100);
        assert_eq!(read_u64(&bytes, OFF_ROW_ID_MAX), 119);
        assert!(
            read_u32(&bytes, OFF_ENTRY_POINT) < 20,
            "the entry point must be a local ordinal, not a row-id"
        );
    }

    #[test]
    fn sections_are_aligned_contiguous_and_cover_the_whole_file() {
        let index = local_keyed_index(20);
        let row_ids: Vec<u64> = (0..20).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        let offs: Vec<usize> = (0..SECTION_COUNT)
            .map(|i| read_u64(&bytes, OFF_SECTION_OFF + i * 8) as usize)
            .collect();
        let lens: Vec<usize> = (0..SECTION_COUNT)
            .map(|i| read_u32(&bytes, OFF_SECTION_LEN + i * 4) as usize)
            .collect();

        assert_eq!(offs[0], HEADER_LEN, "row_ids starts right after the header");
        assert_eq!(lens[0], 20 * 8);
        assert_eq!(lens[1], 20);
        assert_eq!(lens[SECTION_VECTORS], 20 * 3 * 4);
        assert_eq!(
            offs[SECTION_VECTORS] % VECTORS_ALIGN,
            0,
            "the vectors section start must be 64-byte aligned"
        );
        assert_eq!(offs[SECTION_ADJACENCY] % 4, 0);
        for i in 1..SECTION_COUNT {
            assert!(
                offs[i] >= offs[i - 1] + lens[i - 1],
                "section {i} must not overlap section {}",
                i - 1
            );
        }
        assert_eq!(
            bytes.len(),
            offs[SECTION_VECTORS] + lens[SECTION_VECTORS],
            "the file must end exactly where the last section does"
        );
    }

    #[test]
    fn both_crcs_match_the_bytes_they_cover() {
        let index = local_keyed_index(10);
        let row_ids: Vec<u64> = (0..10).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert_eq!(read_u32(&bytes, OFF_BODY_CRC), crc32c::crc32c(&bytes[HEADER_LEN..]));
        assert_eq!(
            read_u32(&bytes, OFF_HEADER_CRC),
            crc32c::crc32c(&bytes[..OFF_HEADER_CRC])
        );
    }

    #[test]
    fn row_ids_are_a_direct_positional_dump_of_the_caller_supplied_array() {
        // The W3.2 amendment section 3b property: keying the working index
        // 0..N makes the row_ids section a straight copy, with no remap
        // pass and no reordering.
        let index = local_keyed_index(5);
        let row_ids: Vec<u64> = vec![7, 11, 13, 5_000_000, 5_000_001];
        let bytes = index.to_segment_bytes(&row_ids).unwrap();
        let off = read_u64(&bytes, OFF_SECTION_OFF) as usize;
        let decoded: &[u64] = bytemuck::cast_slice(&bytes[off..off + 5 * 8]);
        assert_eq!(decoded, row_ids.as_slice());
    }

    #[test]
    fn an_empty_row_id_list_is_rejected_rather_than_producing_an_empty_segment() {
        // A vector-less commit must write no segment at all (W3.2
        // amendment section 3c), so this path must never be reachable with
        // an empty list -- and must fail loudly if it ever is.
        let index = local_keyed_index(3);
        assert!(matches!(
            index.to_segment_bytes(&[]),
            Err(IndexError::SegmentEmpty)
        ));
    }

    #[test]
    fn non_ascending_row_ids_are_rejected() {
        let index = local_keyed_index(3);
        assert!(matches!(
            index.to_segment_bytes(&[5, 5, 9]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            index.to_segment_bytes(&[9, 5, 1]),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_row_id_list_longer_than_the_graph_is_rejected_rather_than_encoding_garbage() {
        let index = local_keyed_index(3);
        let row_ids: Vec<u64> = (0..5).collect();
        assert!(matches!(
            index.to_segment_bytes(&row_ids),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }
}
```

- [ ] **Step 11: Run the writer tests**

Run: `cargo test -p strata-index segment_writer`
Expected: all seven `segment_writer::tests` pass.

- [ ] **Step 12: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps`
Expected: all clean. `to_segment_bytes` has no production caller yet (Task 6 adds it); it is `pub`, so no `dead_code` warning.

- [ ] **Step 13: Commit**

```bash
git add crates/index/Cargo.toml crates/index/src/segment_format.rs \
        crates/index/src/segment_writer.rs crates/index/src/hnsw.rs crates/index/src/lib.rs
git commit -m "feat(index): add the on-disk segment format and its writer

Adds crates/index-only dependencies bytemuck (checked typed-slice casts;
already in the workspace graph via arrow, so no new crate) and crc32c
(the Castagnoli polynomial the format specifies -- crc32fast implements
CRC-32 IEEE and is not a substitute).

Fixed 128-byte header plus four aligned sections (row_ids, levels,
CSR-flat adjacency, vectors), both CRC-checked. Two recorded deviations
from the design doc's section 1 sketch: section_len is [u32; 4] so the
header actually fits the specified 128 bytes, and the entry point's level
is read from the levels section rather than assumed equal to max_level.

HnswIndex::to_segment_bytes is pure and in-memory -- no file I/O, no
strata-storage dependency, per the W3 amendment section 4."
```

---

### Task 3: `SegmentReader` and its `NodeSource` impl

**Files:**
- Create: `crates/index/src/segment_reader.rs`
- Modify: `crates/index/src/lib.rs` (module declarations; `pub use`)

**Interfaces:**
- Consumes: everything `segment_format.rs` produced in Task 2 (`HEADER_LEN`, all `OFF_*`, `MAGIC`, `FLAG_LITTLE_ENDIAN`, `METRIC_L2`, `NO_ENTRY_POINT`, `VECTORS_ALIGN`, `SECTION_*`, `SegmentParams`, `AlignedBytes`), `crate::node_source::NodeSource`, `crate::hnsw::IndexError`.
- Produces:
  - `pub struct SegmentReader` with `pub fn from_bytes(raw: &[u8]) -> Result<Self, IndexError>`, `pub fn node_count(&self) -> usize`, `pub fn dimension(&self) -> usize`, `pub fn row_id_at(&self, local: u64) -> Option<u64>`, `pub fn row_id_range(&self) -> (u64, u64)`, `pub fn byte_len(&self) -> usize`, `pub fn format_version(&self) -> u32`.
  - `impl NodeSource for SegmentReader`.
  - Re-exported as `strata_index::SegmentReader` — Task 4's `IndexPart::Sealed` and Task 6's `Dataset::open` both name it.

**Binding requirement (post-W3.1 amendment §4):** `vector(local)` must return `None` — never panic, never return garbage — for any out-of-range or otherwise invalid local id. W3.1's admission gate calls `source.vector(local).is_some()` on every visited node in addition to the caller's `filter`, so a corrupt or truncated segment naming an out-of-range ordinal must **fail closed at that gate rather than crash the search path**. Every accessor in this file is written that way: `get(..)` + `try_cast_slice(..).ok()`, never indexing, never `unwrap`. Note also that `vector()` is now called **twice per visited node** (once for the distance evaluation, once at the admission gate) — base design §2's "escalate to cached raw pointers only if a benchmark shows the bounds check matters" therefore applies against roughly double the call volume it was written for. Do not pre-optimise it here.

**Reentrancy precondition (pre-W3.1 amendment §2):** no `NodeSource` method may borrow `crate::graph`'s thread-local `SEARCH_SCRATCH`, directly or transitively — `search_layer_generic` calls every `NodeSource` method from inside its own active `with_borrow_mut` closure. This file touches no scratch state, satisfying it by construction; do not introduce a scratch borrow in a future optimisation.

- [ ] **Step 1: Write the failing round-trip test first**

Create `crates/index/src/segment_reader.rs` containing only the module doc comment and this test module, so the test compiles against a type that does not exist yet:

```rust
//! Placeholder — replaced in Step 3.

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
mod tests {
    use crate::hnsw::{EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers};
    use crate::node_source::NodeSource;
    use crate::segment_reader::SegmentReader;

    #[allow(clippy::cast_possible_truncation)]
    fn local_keyed_index(n: usize) -> HnswIndex {
        const PHI: f64 = 0.618_033_988_749_895;
        const SQRT2: f64 = 0.414_213_562_373_095;
        const SQRT3: f64 = 0.732_050_807_568_877;
        let index = HnswIndex::new(
            MaxConnections(4),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(20),
        )
        .unwrap();
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        ((f * PHI).fract() * 1000.0) as f32,
                        ((f * SQRT2).fract() * 1000.0) as f32,
                        ((f * SQRT3).fract() * 1000.0) as f32,
                    ],
                )
                .unwrap();
        }
        index
    }

    fn round_trip(n: usize, row_ids: &[u64]) -> (HnswIndex, SegmentReader) {
        let index = local_keyed_index(n);
        let bytes = index.to_segment_bytes(row_ids).unwrap();
        let reader = SegmentReader::from_bytes(&bytes).unwrap();
        (index, reader)
    }

    #[test]
    fn every_nodes_level_vector_and_neighbor_list_survives_the_round_trip() {
        // The single most important property of the whole format: the
        // reader must present byte-identical graph structure to what the
        // in-memory graph exposed, so `search_layer_generic` traverses the
        // same graph either way.
        let n = 40;
        let row_ids: Vec<u64> = (1000..1000 + n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        let source = &index.graph;

        assert_eq!(reader.node_count(), n);
        assert_eq!(reader.dimension(), source.dimension());

        let mut from_graph: Vec<u64> = Vec::new();
        let mut from_segment: Vec<u64> = Vec::new();
        for local in 0..n as u64 {
            assert_eq!(reader.level(local), source.level(local), "level of {local}");
            assert_eq!(reader.vector(local), source.vector(local), "vector of {local}");
            assert_eq!(reader.row_id(local), row_ids[local as usize]);
            let level = source.level(local).unwrap();
            for layer in 0..=level {
                source.neighbors_into(local, layer, &mut from_graph);
                reader.neighbors_into(local, layer, &mut from_segment);
                // The CSR encoding preserves the live graph's slot order,
                // so this is an exact list comparison, not a set one.
                assert_eq!(
                    from_segment, from_graph,
                    "neighbors of node {local} at layer {layer}"
                );
            }
        }
    }

    #[test]
    fn entry_point_round_trips_as_a_local_ordinal_with_its_own_level() {
        let n = 30;
        let row_ids: Vec<u64> = (0..n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        assert_eq!(reader.entry_point(), index.graph.entry_point());
    }

    #[test]
    fn searching_the_segment_returns_the_same_results_as_searching_the_source_graph() {
        // Proves the format is not merely structurally equal but
        // behaviourally equal through the real traversal code -- which is
        // what `SegmentSet::search` will run.
        let n = 200;
        let row_ids: Vec<u64> = (0..n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        let query = [500.0_f32, 500.0, 500.0];

        let from_graph =
            crate::graph::k_nn_search_generic(&index.graph, &crate::distance::L2, &query, 10, 32, |_| true)
                .unwrap();
        let from_segment =
            crate::graph::k_nn_search_generic(&reader, &crate::distance::L2, &query, 10, 32, |_| true)
                .unwrap();

        assert_eq!(
            from_segment.len(),
            from_graph.len(),
            "same result count: {from_segment:?} vs {from_graph:?}"
        );
        for (a, b) in from_segment.iter().zip(&from_graph) {
            assert_eq!(a.0, b.0, "same local ordinal, in the same rank order");
            assert!(
                (a.1 - b.1).abs() < f32::EPSILON,
                "same distance: {} vs {}",
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn vector_returns_none_for_an_out_of_range_local_id_rather_than_panicking() {
        // Binding requirement, W3.2 amendment section 4: this is the
        // admission gate `search_layer_generic` relies on to fail closed on
        // a corrupt adjacency entry.
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        assert!(reader.vector(10).is_none());
        assert!(reader.vector(u64::MAX).is_none());
        assert!(reader.level(10).is_none());
        assert!(reader.level(u64::MAX).is_none());
    }

    #[test]
    fn neighbors_into_clears_its_buffer_and_yields_nothing_for_an_out_of_range_node_or_layer() {
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        let mut out = vec![999_u64; 5];
        reader.neighbors_into(10, 0, &mut out);
        assert!(out.is_empty(), "stale contents must never leak through");
        reader.neighbors_into(0, 1_000, &mut out);
        assert!(out.is_empty());
        reader.neighbors_into(u64::MAX, 0, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn row_id_of_an_out_of_range_local_id_is_a_sentinel_no_real_row_can_hold() {
        // `NodeSource::row_id` cannot return an Option (the trait is
        // infallible there), so an out-of-range lookup returns u64::MAX --
        // above `crates/txn`'s enforced 1e9 row-id ceiling, so the
        // visibility filter rejects it and the result is never admitted.
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        assert_eq!(reader.row_id(10), u64::MAX);
        assert!(reader.row_id_at(10).is_none());
    }

    #[test]
    fn a_truncated_file_is_rejected_rather_than_read_past_its_end() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert!(matches!(
            SegmentReader::from_bytes(&bytes[..bytes.len() / 2]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            SegmentReader::from_bytes(&bytes[..10]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            SegmentReader::from_bytes(&[]),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_flipped_body_byte_is_caught_by_the_body_crc() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let mut bytes = index.to_segment_bytes(&row_ids).unwrap().into_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            SegmentReader::from_bytes(&bytes),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_flipped_header_byte_is_caught_by_the_header_crc() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let mut bytes = index.to_segment_bytes(&row_ids).unwrap().into_vec();
        // Corrupt `node_count` -- a field whose corruption would otherwise
        // be read as a plausible geometry.
        bytes[crate::segment_format::OFF_NODE_COUNT] ^= 0x0F;
        assert!(matches!(
            SegmentReader::from_bytes(&bytes),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_wrong_magic_or_format_version_is_rejected_before_anything_else_is_read() {
        let row_ids: Vec<u64> = (0..5).collect();
        let index = local_keyed_index(5);
        let good = index.to_segment_bytes(&row_ids).unwrap();

        let mut wrong_magic = good.clone().into_vec();
        wrong_magic[0] = b'X';
        assert!(matches!(
            SegmentReader::from_bytes(&wrong_magic),
            Err(IndexError::SegmentCorrupt(_))
        ));

        let mut wrong_version = good.into_vec();
        let bumped = (crate::segment_format::SEGMENT_FORMAT_VERSION + 1).to_le_bytes();
        wrong_version[crate::segment_format::OFF_FORMAT_VERSION
            ..crate::segment_format::OFF_FORMAT_VERSION + 4]
            .copy_from_slice(&bumped);
        // Recompute the header CRC so the version check, not the CRC check,
        // is what rejects it.
        let crc = crc32c::crc32c(&wrong_version[..crate::segment_format::OFF_HEADER_CRC]);
        wrong_version[crate::segment_format::OFF_HEADER_CRC
            ..crate::segment_format::OFF_HEADER_CRC + 4]
            .copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            SegmentReader::from_bytes(&wrong_version),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_single_node_segment_round_trips() {
        // The commonest real shape in S1: one commit, one row.
        let row_ids: Vec<u64> = vec![42];
        let (index, reader) = round_trip(1, &row_ids);
        assert_eq!(reader.node_count(), 1);
        assert_eq!(reader.row_id_range(), (42, 42));
        assert_eq!(reader.entry_point(), index.graph.entry_point());
        assert_eq!(reader.vector(0), index.graph.vector(0));
        let mut out = Vec::new();
        reader.neighbors_into(0, 0, &mut out);
        assert!(out.is_empty(), "a lone node has no neighbors");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p strata-index segment_reader`
Expected: FAIL to compile — `unresolved import 'crate::segment_reader::SegmentReader'` (and `file not found for module 'segment_reader'` until Step 4 wires it up; wire the module in first if the error is the latter).

- [ ] **Step 3: Replace the placeholder doc comment in `crates/index/src/segment_reader.rs` with the implementation**

Put this **above** the `#[cfg(all(test, not(loom)))] mod tests` block written in Step 1:

```rust
//! Read-only view over one immutable on-disk segment ([`crate::segment_format`]),
//! implementing [`NodeSource`] so `search_layer_generic`/`k_nn_search_generic`
//! traverse it with the identical algorithm they use for the live graph.
//!
//! **Loading is `O(bytes)`**: offset/length validation, one CRC pass over
//! the header and one over the body, one ascending check over `row_ids` and
//! one range check over the adjacency entries — **zero distance
//! evaluations, zero graph construction**. That is the entire recovery win
//! this format exists for (base design doc §1).
//!
//! **Every accessor fails closed.** No indexing, no `unwrap`, no panic: an
//! out-of-range local id yields `None` (or `u64::MAX` for
//! [`NodeSource::row_id`], which the trait forces to be infallible). Binding
//! per `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §4 —
//! `search_layer_generic`'s admission gate calls `vector(local).is_some()`
//! on every visited node, so a corrupt adjacency entry naming an
//! out-of-range ordinal must be excluded there, not crash the search path.
//!
//! **Reentrancy:** nothing here borrows `crate::graph`'s `SEARCH_SCRATCH`,
//! directly or transitively — see [`crate::node_source`]'s module doc for
//! why a future optimisation must not change that.

use crate::hnsw::IndexError;
use crate::node_source::NodeSource;
use crate::segment_format::{
    AlignedBytes, FLAG_LITTLE_ENDIAN, HEADER_LEN, MAGIC, METRIC_L2, NO_ENTRY_POINT, OFF_BODY_CRC,
    OFF_DIM, OFF_EF_CONSTRUCTION, OFF_ENTRY_POINT, OFF_FLAGS, OFF_FORMAT_VERSION, OFF_HEADER_CRC,
    OFF_M, OFF_MAGIC, OFF_MAX_LEVEL, OFF_METRIC, OFF_MMAX, OFF_MMAX0, OFF_M_L, OFF_NODE_COUNT,
    OFF_ROW_ID_MAX, OFF_ROW_ID_MIN, OFF_SECTION_LEN, OFF_SECTION_OFF, SECTION_ADJACENCY,
    SECTION_COUNT, SECTION_LEVELS, SECTION_ROW_IDS, SECTION_VECTORS, SEGMENT_FORMAT_VERSION,
    SegmentParams, VECTORS_ALIGN,
};

#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

fn corrupt(detail: impl Into<String>) -> IndexError {
    IndexError::SegmentCorrupt(detail.into())
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut buf = [0_u8; 4];
    if let Some(src) = bytes.get(at..at + 4) {
        buf.copy_from_slice(src);
    }
    u32::from_le_bytes(buf)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut buf = [0_u8; 8];
    if let Some(src) = bytes.get(at..at + 8) {
        buf.copy_from_slice(src);
    }
    u64::from_le_bytes(buf)
}

fn read_f64(bytes: &[u8], at: usize) -> f64 {
    f64::from_bits(read_u64(bytes, at))
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    let mut buf = [0_u8; 2];
    if let Some(src) = bytes.get(at..at + 2) {
        buf.copy_from_slice(src);
    }
    u16::from_le_bytes(buf)
}

pub struct SegmentReader {
    /// The whole file, in a 64-byte-aligned owned allocation that never
    /// moves — every typed view below is a checked cast of a sub-range of
    /// it, so it must outlive them all.
    bytes: AlignedBytes,
    node_count: usize,
    dim: usize,
    max_level: usize,
    /// `(local ordinal, that node's own level)`, or `None` for a segment
    /// with no entry point. The level is read from the `levels` section
    /// rather than assumed equal to `max_level` — see this plan's format
    /// decision #2.
    entry: Option<(u32, usize)>,
    row_ids_off: usize,
    levels_off: usize,
    vectors_off: usize,
    /// `layer_off[l]` = `(byte offset of layer l's offsets array, byte
    /// offset of layer l's neighbors array)`. Computed once at load by
    /// walking the adjacency section, so no accessor ever has to.
    layer_off: Vec<(usize, usize)>,
    params: SegmentParams,
    format_version: u32,
    row_id_min: u64,
    row_id_max: u64,
}

impl SegmentReader {
    /// Validates and loads a complete segment image.
    ///
    /// `raw` is copied into a 64-byte-aligned owned buffer, so the caller
    /// keeps ownership of its own bytes and the reader is `'static`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SegmentCorrupt`] — with a message naming the
    /// specific check that failed — if `raw` is shorter than the header,
    /// has the wrong magic/format version/endianness flag/metric, fails
    /// either CRC, declares an out-of-range or misaligned section, declares
    /// a `node_count`/`dim` of zero, has non-ascending `row_ids`, has a
    /// node level above `max_level`, has a malformed CSR offsets array, or
    /// names a neighbor ordinal outside `0..node_count`.
    #[allow(clippy::too_many_lines)] // One linear validation pass whose order is
    // load-bearing (magic -> CRC -> geometry -> sections -> contents);
    // splitting it would hide that order across call sites.
    pub fn from_bytes(raw: &[u8]) -> Result<Self, IndexError> {
        if raw.len() < HEADER_LEN {
            return Err(corrupt(format!(
                "file is {} bytes, shorter than the {HEADER_LEN}-byte header",
                raw.len()
            )));
        }
        let bytes = AlignedBytes::from_slice(raw);
        let b = bytes.as_slice();

        if b.get(OFF_MAGIC..OFF_MAGIC + 8) != Some(&MAGIC[..]) {
            return Err(corrupt("bad magic: not a Strata segment file"));
        }
        let stored_header_crc = read_u32(b, OFF_HEADER_CRC);
        let actual_header_crc = crc32c::crc32c(b.get(..OFF_HEADER_CRC).unwrap_or(&[]));
        if stored_header_crc != actual_header_crc {
            return Err(corrupt(format!(
                "header CRC32C mismatch: stored {stored_header_crc:#010x}, computed {actual_header_crc:#010x}"
            )));
        }

        let format_version = read_u32(b, OFF_FORMAT_VERSION);
        if format_version != SEGMENT_FORMAT_VERSION {
            return Err(corrupt(format!(
                "segment format version {format_version}, but this build reads only {SEGMENT_FORMAT_VERSION}"
            )));
        }
        let flags = read_u32(b, OFF_FLAGS);
        if flags & FLAG_LITTLE_ENDIAN == 0 {
            return Err(corrupt("segment is not flagged little-endian"));
        }
        let metric = b.get(OFF_METRIC).copied().unwrap_or(u8::MAX);
        if metric != METRIC_L2 {
            return Err(corrupt(format!(
                "segment uses metric discriminant {metric}, but this build has only L2 ({METRIC_L2})"
            )));
        }

        let node_count = usize::try_from(read_u32(b, OFF_NODE_COUNT))
            .map_err(|_| corrupt("node_count does not fit in usize"))?;
        let dim = usize::try_from(read_u32(b, OFF_DIM))
            .map_err(|_| corrupt("dim does not fit in usize"))?;
        let max_level = usize::try_from(read_u32(b, OFF_MAX_LEVEL))
            .map_err(|_| corrupt("max_level does not fit in usize"))?;
        if node_count == 0 {
            // A vector-less commit writes no segment at all (W3.2 amendment
            // §3c), so a zero-node segment can only be corruption.
            return Err(corrupt("node_count is zero"));
        }
        if dim == 0 {
            return Err(corrupt("dim is zero"));
        }
        if max_level > usize::from(u8::MAX) {
            return Err(corrupt(format!("max_level {max_level} exceeds u8")));
        }

        // Section table: offsets/lengths in range, non-overlapping,
        // correctly aligned, and exactly the sizes the geometry implies.
        let mut offs = [0_usize; SECTION_COUNT];
        let mut lens = [0_usize; SECTION_COUNT];
        for i in 0..SECTION_COUNT {
            offs[i] = usize::try_from(read_u64(b, OFF_SECTION_OFF + i * 8))
                .map_err(|_| corrupt(format!("section {i}'s offset does not fit in usize")))?;
            lens[i] = usize::try_from(read_u32(b, OFF_SECTION_LEN + i * 4))
                .map_err(|_| corrupt(format!("section {i}'s length does not fit in usize")))?;
            let end = offs[i]
                .checked_add(lens[i])
                .ok_or_else(|| corrupt(format!("section {i}'s extent overflows")))?;
            if offs[i] < HEADER_LEN || end > b.len() {
                return Err(corrupt(format!(
                    "section {i} spans {}..{end}, outside the {}-byte file's body",
                    offs[i],
                    b.len()
                )));
            }
        }
        for i in 1..SECTION_COUNT {
            if offs[i] < offs[i - 1] + lens[i - 1] {
                return Err(corrupt(format!("section {i} overlaps section {}", i - 1)));
            }
        }

        let expected_row_ids = node_count
            .checked_mul(8)
            .ok_or_else(|| corrupt("row_ids section size overflows"))?;
        if lens[SECTION_ROW_IDS] != expected_row_ids {
            return Err(corrupt(format!(
                "row_ids section is {} bytes, expected {expected_row_ids} for {node_count} nodes",
                lens[SECTION_ROW_IDS]
            )));
        }
        if lens[SECTION_LEVELS] != node_count {
            return Err(corrupt(format!(
                "levels section is {} bytes, expected {node_count}",
                lens[SECTION_LEVELS]
            )));
        }
        let expected_vectors = node_count
            .checked_mul(dim)
            .and_then(|elems| elems.checked_mul(4))
            .ok_or_else(|| corrupt("vectors section size overflows"))?;
        if lens[SECTION_VECTORS] != expected_vectors {
            return Err(corrupt(format!(
                "vectors section is {} bytes, expected {expected_vectors}",
                lens[SECTION_VECTORS]
            )));
        }
        if offs[SECTION_ROW_IDS] % 8 != 0 {
            return Err(corrupt("row_ids section is not 8-byte aligned"));
        }
        if offs[SECTION_ADJACENCY] % 4 != 0 {
            return Err(corrupt("adjacency section is not 4-byte aligned"));
        }
        if offs[SECTION_VECTORS] % VECTORS_ALIGN != 0 {
            return Err(corrupt("vectors section is not 64-byte aligned"));
        }

        let stored_body_crc = read_u32(b, OFF_BODY_CRC);
        let actual_body_crc = crc32c::crc32c(b.get(HEADER_LEN..).unwrap_or(&[]));
        if stored_body_crc != actual_body_crc {
            return Err(corrupt(format!(
                "body CRC32C mismatch: stored {stored_body_crc:#010x}, computed {actual_body_crc:#010x}"
            )));
        }

        // Contents. `row_ids` strictly ascending is the precondition the
        // format's "no side table, binary-search the resident array"
        // reverse lookup rests on (base design doc §1).
        let row_ids: &[u64] = b
            .get(offs[SECTION_ROW_IDS]..offs[SECTION_ROW_IDS] + lens[SECTION_ROW_IDS])
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .ok_or_else(|| corrupt("row_ids section could not be viewed as [u64]"))?;
        if row_ids.windows(2).any(|w| w[0] >= w[1]) {
            return Err(corrupt("row_ids are not strictly ascending"));
        }

        let levels: &[u8] = b
            .get(offs[SECTION_LEVELS]..offs[SECTION_LEVELS] + lens[SECTION_LEVELS])
            .ok_or_else(|| corrupt("levels section is out of range"))?;
        if levels.iter().any(|&l| usize::from(l) > max_level) {
            return Err(corrupt("a node's level exceeds the segment's max_level"));
        }

        // Walk the adjacency section once: record each layer's two array
        // offsets, and check every CSR offsets array and neighbor ordinal.
        let mut layer_off: Vec<(usize, usize)> = Vec::with_capacity(max_level + 1);
        let mut cursor = offs[SECTION_ADJACENCY];
        let adjacency_end = offs[SECTION_ADJACENCY] + lens[SECTION_ADJACENCY];
        for layer in 0..=max_level {
            let offsets_bytes = (node_count + 1)
                .checked_mul(4)
                .ok_or_else(|| corrupt("a layer's offsets array size overflows"))?;
            let offsets_end = cursor
                .checked_add(offsets_bytes)
                .filter(|&e| e <= adjacency_end)
                .ok_or_else(|| corrupt(format!("layer {layer}'s offsets array runs past the adjacency section")))?;
            let offsets: &[u32] = b
                .get(cursor..offsets_end)
                .and_then(|s| bytemuck::try_cast_slice(s).ok())
                .ok_or_else(|| corrupt(format!("layer {layer}'s offsets array could not be viewed as [u32]")))?;
            if offsets.first() != Some(&0) {
                return Err(corrupt(format!("layer {layer}'s offsets array does not start at 0")));
            }
            if offsets.windows(2).any(|w| w[0] > w[1]) {
                return Err(corrupt(format!("layer {layer}'s offsets array is not non-decreasing")));
            }
            let neighbor_count = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| corrupt("a layer's neighbor count does not fit in usize"))?;
            let neighbors_bytes = neighbor_count
                .checked_mul(4)
                .ok_or_else(|| corrupt("a layer's neighbors array size overflows"))?;
            let neighbors_end = offsets_end
                .checked_add(neighbors_bytes)
                .filter(|&e| e <= adjacency_end)
                .ok_or_else(|| corrupt(format!("layer {layer}'s neighbors array runs past the adjacency section")))?;
            let neighbors: &[u32] = b
                .get(offsets_end..neighbors_end)
                .and_then(|s| bytemuck::try_cast_slice(s).ok())
                .ok_or_else(|| corrupt(format!("layer {layer}'s neighbors array could not be viewed as [u32]")))?;
            // Checked once here so the hot loop never has to. `vector()`
            // still fails closed independently, per amendment §4.
            if neighbors
                .iter()
                .any(|&n| usize::try_from(n).is_ok_and(|n| n >= node_count))
            {
                return Err(corrupt(format!(
                    "layer {layer} names a neighbor ordinal outside 0..{node_count}"
                )));
            }
            layer_off.push((cursor, offsets_end));
            cursor = neighbors_end;
        }
        if cursor != adjacency_end {
            return Err(corrupt(format!(
                "adjacency section has {} trailing bytes after {} layers",
                adjacency_end - cursor,
                max_level + 1
            )));
        }

        let entry_raw = read_u32(b, OFF_ENTRY_POINT);
        let entry = if entry_raw == NO_ENTRY_POINT {
            None
        } else {
            let idx = usize::try_from(entry_raw)
                .ok()
                .filter(|&i| i < node_count)
                .ok_or_else(|| corrupt(format!("entry point {entry_raw} is outside 0..{node_count}")))?;
            let level = levels
                .get(idx)
                .copied()
                .ok_or_else(|| corrupt("entry point has no level"))?;
            Some((entry_raw, usize::from(level)))
        };

        Ok(Self {
            node_count,
            dim,
            max_level,
            entry,
            row_ids_off: offs[SECTION_ROW_IDS],
            levels_off: offs[SECTION_LEVELS],
            vectors_off: offs[SECTION_VECTORS],
            layer_off,
            params: SegmentParams {
                m: usize::from(read_u16(b, OFF_M)),
                mmax0: usize::from(read_u16(b, OFF_MMAX0)),
                mmax: usize::from(read_u16(b, OFF_MMAX)),
                ef_construction: usize::from(read_u16(b, OFF_EF_CONSTRUCTION)),
                m_l: read_f64(b, OFF_M_L),
            },
            format_version,
            row_id_min: read_u64(b, OFF_ROW_ID_MIN),
            row_id_max: read_u64(b, OFF_ROW_ID_MAX),
            bytes,
        })
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// The vector dimension every node in this segment carries. Inherent
    /// twin of [`NodeSource::dimension`], so callers that hold a
    /// `SegmentReader` directly need not import the trait.
    #[must_use]
    pub fn dimension(&self) -> usize {
        self.dim
    }

    /// The global row-id local ordinal `local` stands for, or `None` if
    /// `local` is out of range. The fallible form; [`NodeSource::row_id`]
    /// is the infallible one the traversal uses.
    #[must_use]
    pub fn row_id_at(&self, local: u64) -> Option<u64> {
        let idx = usize::try_from(local).ok()?;
        self.row_ids().get(idx).copied()
    }

    /// `(row_id_min, row_id_max)`, both inclusive, as recorded in the
    /// header. Informational — never a read path (base design doc §3).
    #[must_use]
    pub fn row_id_range(&self) -> (u64, u64) {
        (self.row_id_min, self.row_id_max)
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// The HNSW parameters this segment was built with. Not consulted
    /// during search (the graph is already built); kept so the CLI and a
    /// future compactor can report/reproduce them.
    #[must_use]
    pub(crate) fn params(&self) -> SegmentParams {
        self.params
    }

    fn row_ids(&self) -> &[u64] {
        self.bytes
            .as_slice()
            .get(self.row_ids_off..self.row_ids_off + self.node_count * 8)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }

    fn levels(&self) -> &[u8] {
        self.bytes
            .as_slice()
            .get(self.levels_off..self.levels_off + self.node_count)
            .unwrap_or(&[])
    }

    fn layer_offsets(&self, layer: usize) -> &[u32] {
        let Some(&(offsets_off, neighbors_off)) = self.layer_off.get(layer) else {
            return &[];
        };
        self.bytes
            .as_slice()
            .get(offsets_off..neighbors_off)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }

    fn layer_neighbors(&self, layer: usize) -> &[u32] {
        let Some(&(_, neighbors_off)) = self.layer_off.get(layer) else {
            return &[];
        };
        // A layer's neighbors run from its own array start to the next
        // layer's offsets array (or, for the last layer, to the end of the
        // adjacency section — which is where the vectors section's padding
        // begins, so the next layer's `offsets_off` is unavailable). Both
        // bounds were validated at load; recompute the end from the CSR
        // offsets array so this stays a pure function of validated state.
        let count = self
            .layer_offsets(layer)
            .last()
            .copied()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        self.bytes
            .as_slice()
            .get(neighbors_off..neighbors_off + count * 4)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }
}

impl NodeSource for SegmentReader {
    fn entry_point(&self) -> Option<(u64, usize)> {
        self.entry.map(|(local, level)| (u64::from(local), level))
    }

    fn level(&self, local: u64) -> Option<usize> {
        let idx = usize::try_from(local).ok()?;
        self.levels().get(idx).copied().map(usize::from)
    }

    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>) {
        out.clear();
        let Ok(idx) = usize::try_from(local) else {
            return;
        };
        if idx >= self.node_count || level > self.max_level {
            return;
        }
        let offsets = self.layer_offsets(level);
        let (Some(&start), Some(&end)) = (offsets.get(idx), offsets.get(idx + 1)) else {
            return;
        };
        let (Ok(start), Ok(end)) = (usize::try_from(start), usize::try_from(end)) else {
            return;
        };
        let neighbors = self.layer_neighbors(level);
        let Some(slice) = neighbors.get(start..end) else {
            return;
        };
        out.extend(slice.iter().map(|&n| u64::from(n)));
    }

    fn vector(&self, local: u64) -> Option<&[f32]> {
        let idx = usize::try_from(local).ok()?;
        if idx >= self.node_count {
            return None;
        }
        let start = self.vectors_off.checked_add(idx.checked_mul(self.dim * 4)?)?;
        let end = start.checked_add(self.dim * 4)?;
        let bytes = self.bytes.as_slice().get(start..end)?;
        bytemuck::try_cast_slice(bytes).ok()
    }

    fn row_id(&self, local: u64) -> u64 {
        // The trait cannot return an Option here, so an out-of-range lookup
        // yields a sentinel above `crates/txn`'s enforced 1e9 row-id
        // ceiling: the visibility filter rejects it, so a corrupt segment
        // can never smuggle a result through this path. See
        // [`Self::row_id_at`] for the fallible form.
        self.row_id_at(local).unwrap_or(u64::MAX)
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    // `is_deleted` deliberately uses the trait's `false` default: a segment
    // has no per-node deleted flag. Deletion in the segmented design is the
    // manifest's versioned tombstone set, applied through the caller's
    // `filter` (base design doc §2/§5).
}
```

- [ ] **Step 4: Wire `segment_reader` into `crates/index/src/lib.rs`**

Add `mod segment_reader;` to the module-declaration block (after `mod node_table;`, keeping alphabetical order relative to `segment_format`/`segment_set`/`segment_writer`), and add to the `pub use` block:

```rust
pub use segment_reader::SegmentReader;
```

- [ ] **Step 5: Run the reader tests**

Run: `cargo test -p strata-index segment_reader`
Expected: all eleven `segment_reader::tests` pass, in particular `every_nodes_level_vector_and_neighbor_list_survives_the_round_trip` and `searching_the_segment_returns_the_same_results_as_searching_the_source_graph`.

If `searching_the_segment_...` fails on *rank order* while structural equality passes, the bug is in `neighbors_into`'s CSR slicing (order), not in the format. If it fails on *result count*, check `entry_point`.

- [ ] **Step 6: Silence the one expected dead-code warning, if it fires**

`SegmentReader::params` has no caller in this plan (a future CLI `inspect` subcommand is its intended consumer). If `cargo clippy -p strata-index --all-targets -- -D warnings` reports `method 'params' is never used`, add directly above it:

```rust
    // Not consumed by any production path in W3.2a — kept because the
    // header already carries these fields and a reader that could not
    // report them would make the format non-self-describing. Same pattern
    // as `node.rs`'s `row_id()` accessor.
    #[allow(dead_code)]
```

- [ ] **Step 7: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps`
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add crates/index/src/segment_reader.rs crates/index/src/lib.rs
git commit -m "feat(index): add SegmentReader implementing NodeSource

Loads a segment in O(bytes) -- offset/length validation, two CRC passes,
one ascending check, one adjacency range check -- with zero distance
evaluations and zero graph construction.

Every accessor fails closed: an out-of-range local id yields None from
vector()/level() and u64::MAX from row_id(), never a panic. That is the
binding requirement from
docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md section 4:
W3.1's admission gate calls vector(local).is_some() on every visited
node, so a corrupt adjacency entry must be excluded there rather than
crash the search path.

Proven equivalent to the source graph both structurally (level, vector
and neighbor list per node) and behaviourally (identical
k_nn_search_generic results)."
```

---

### Task 4: `SegmentSet` gains `Sealed` parts and multi-part fan-out search

**Files:**
- Modify: `crates/index/src/segment_set.rs` (module doc lines 1-10; `IndexPart` lines 17-21; `sole_live` lines 39-58 — **deleted**; `search` lines 60-92; `search_filtered` lines 94-130; `established_dimension` lines 132-138; `live_arc` lines 140-148; test module lines 151-277)

**Interfaces:**
- Consumes: `SegmentReader` (Task 3), `k_nn_search_generic` (existing `pub(crate)` in `graph.rs:754`), `build_live_filter` (existing `pub(crate)` in `hnsw.rs:360`), `HnswIndex`/`IndexError`/`VectorMatch`.
- Produces:
  - `pub enum IndexPart { Live(Arc<HnswIndex>), Sealed(Arc<SegmentReader>) }` — `Live` is deleted in **Task 8**, not here.
  - `pub fn SegmentSet::empty() -> Self`
  - `pub fn SegmentSet::from_segments(parts: Vec<Arc<SegmentReader>>) -> Self`
  - `pub fn SegmentSet::with_appended(&self, reader: Arc<SegmentReader>) -> Self`
  - `pub fn SegmentSet::len(&self) -> usize`, `pub fn SegmentSet::is_empty(&self) -> bool`
  - `search`/`search_filtered`/`established_dimension` keep their existing signatures and become length-independent over any mix of parts.
  Tasks 6 and 9-10 call all of these.

**Why `Live` survives this task (post-W3.1 amendment §1, verbatim):** the amendment forbids adding `Sealed` *alongside* `Live` as a lasting staging shape, because that silently disables the compile-error forcing function. It explicitly permits a transient period where both variants exist **provided** "the arity-refutable slice pattern [is replaced] with length-independent iteration (`for part in self.parts.iter() { match part { ... } }`) **in that same transient commit, not later**." This task does exactly that: `sole_live` — whose `let [part] = self.parts.as_ref() else { unreachable!() }` is the arity-refutable pattern that would panic at runtime the moment a second part exists — is **deleted in this commit**, and every method becomes a `for … { match … }` loop that is exhaustive over `IndexPart`. Task 8 then removes `Live` itself once `crates/txn` has no caller left. Do **not** reorder these two tasks and do **not** leave `sole_live` in place "until Task 8."

**The trap this task exists to avoid, stated explicitly:** `k_nn_search_generic` returns **local ids**, not row-ids. For a `Live` part the two coincide (`Graph<D>::row_id` is the identity). For a `Sealed` part the local id is the segment-local ordinal and **must** be mapped through `SegmentReader::row_id_at` before it becomes a `VectorMatch.row_id`. Skipping that map returns ordinals as if they were row-ids — a silent, plausible-looking wrong answer that every existing single-segment test would still pass.

- [ ] **Step 1: Write the failing fan-out tests**

In `crates/index/src/segment_set.rs`'s existing `mod tests` (line 153), add these alongside the three existing tests. They need a segment-building helper, so add that first:

```rust
    /// Builds one sealed segment over `n` quasi-random 3-d points whose
    /// global row-ids start at `row_id_base` — the exact shape
    /// `crates/txn`'s per-commit builder produces: a fresh index keyed by
    /// segment-local ordinals `0..n`, serialized, and loaded back.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    fn build_sealed(n: usize, row_id_base: u64, offset: f32) -> Arc<crate::SegmentReader> {
        let index = HnswIndex::new(
            MaxConnections(4),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(20),
        )
        .unwrap();
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        offset + ((f * PHI).fract() * 100.0) as f32,
                        offset + ((f * SQRT2).fract() * 100.0) as f32,
                        offset + ((f * SQRT3).fract() * 100.0) as f32,
                    ],
                )
                .unwrap();
        }
        let row_ids: Vec<u64> = (row_id_base..row_id_base + n as u64).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();
        Arc::new(crate::SegmentReader::from_bytes(&bytes).unwrap())
    }

    #[test]
    fn an_empty_segment_set_searches_to_no_results_instead_of_erroring() {
        // A freshly created dataset has no segments at all. This must be a
        // clean empty result, not an error and not a panic.
        let set = SegmentSet::empty();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(set.search(&[0.0, 0.0, 0.0], 5, 32, |_| true).unwrap().is_empty());
        assert!(
            set.search_filtered(&[0.0, 0.0, 0.0], 5, 32, &[0, 1, 2], |_| true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(set.established_dimension(), 0);
    }

    #[test]
    fn search_returns_global_row_ids_not_segment_local_ordinals() {
        // The single most dangerous bug this layer can have: every sealed
        // part is keyed 0..n, so a missing row_id_at() map returns
        // ordinals that look exactly like plausible row-ids. Row-id base
        // 1_000_000 makes the two impossible to confuse.
        let set = SegmentSet::from_segments(vec![build_sealed(30, 1_000_000, 0.0)]);
        let hits = set.search(&[50.0, 50.0, 50.0], 5, 32, |_| true).unwrap();
        assert!(!hits.is_empty());
        assert!(
            hits.iter().all(|m| m.row_id >= 1_000_000),
            "results must be global row-ids, not local ordinals: {hits:?}"
        );
    }

    #[test]
    fn search_fans_out_across_every_part_and_finds_rows_in_all_of_them() {
        // The recall property this plan's Scope decision exists to
        // protect: two segments, two well-separated clusters, and a query
        // near each one must find that cluster's own rows. A
        // consult-one-part implementation passes for one query and fails
        // for the other.
        let near = build_sealed(30, 0, 0.0); // row-ids 0..30 around the origin
        let far = build_sealed(30, 500, 10_000.0); // row-ids 500..530, far away
        let set = SegmentSet::from_segments(vec![near, far]);
        assert_eq!(set.len(), 2);

        let near_hits = set.search(&[50.0, 50.0, 50.0], 3, 32, |_| true).unwrap();
        assert_eq!(near_hits.len(), 3, "{near_hits:?}");
        assert!(
            near_hits.iter().all(|m| m.row_id < 30),
            "a query near segment 0 must return segment 0's rows: {near_hits:?}"
        );

        let far_hits = set
            .search(&[10_050.0, 10_050.0, 10_050.0], 3, 32, |_| true)
            .unwrap();
        assert_eq!(far_hits.len(), 3, "{far_hits:?}");
        assert!(
            far_hits.iter().all(|m| (500..530).contains(&m.row_id)),
            "a query near segment 1 must return segment 1's rows -- this is the \
             assertion a consult-one-part implementation fails: {far_hits:?}"
        );
    }

    #[test]
    fn merged_results_are_ordered_by_ascending_distance_and_capped_at_k() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
            build_sealed(30, 900, 50_000.0),
        ]);
        let hits = set.search(&[50.0, 50.0, 50.0], 4, 32, |_| true).unwrap();
        assert_eq!(hits.len(), 4, "k must cap the merged set, not each part");
        for pair in hits.windows(2) {
            assert!(
                pair[0].squared_distance <= pair[1].squared_distance,
                "merged results must be nearest-first across parts: {hits:?}"
            );
        }
    }

    #[test]
    fn a_visibility_predicate_is_applied_uniformly_across_every_part() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
        ]);
        // Hide every row-id below 500 -- i.e. the whole first segment.
        let hits = set.search(&[50.0, 50.0, 50.0], 5, 32, |id| id >= 500).unwrap();
        assert!(!hits.is_empty(), "the second segment's rows are still visible");
        assert!(
            hits.iter().all(|m| m.row_id >= 500),
            "the predicate must gate every part, not only the first: {hits:?}"
        );
    }

    #[test]
    fn search_filtered_applies_its_live_id_set_across_every_part() {
        let set = SegmentSet::from_segments(vec![
            build_sealed(30, 0, 0.0),
            build_sealed(30, 500, 10_000.0),
        ]);
        // Only even row-ids from the far segment are live.
        let live_ids: Vec<usize> = (500..530).step_by(2).collect();
        let hits = set
            .search_filtered(&[50.0, 50.0, 50.0], 5, 32, &live_ids, |_| true)
            .unwrap();
        assert!(!hits.is_empty());
        assert!(
            hits.iter().all(|m| m.row_id >= 500 && m.row_id % 2 == 0),
            "only live ids may come back, from any part: {hits:?}"
        );
    }

    #[test]
    fn with_appended_leaves_the_original_set_untouched() {
        // Snapshots are immutable and share their parts; publishing a new
        // segment must never mutate an already-published snapshot's view.
        let base = SegmentSet::from_segments(vec![build_sealed(10, 0, 0.0)]);
        let grown = base.with_appended(build_sealed(10, 500, 10_000.0));

        assert_eq!(base.len(), 1, "the original set must not have grown");
        assert_eq!(grown.len(), 2);

        let from_base = base
            .search(&[10_050.0, 10_050.0, 10_050.0], 1, 32, |_| true)
            .unwrap();
        assert!(
            from_base.iter().all(|m| m.row_id < 10),
            "the pre-append set must not see the appended segment: {from_base:?}"
        );
        let from_grown = grown
            .search(&[10_050.0, 10_050.0, 10_050.0], 1, 32, |_| true)
            .unwrap();
        assert_eq!(from_grown.first().map(|m| m.row_id >= 500), Some(true));
    }

    #[test]
    fn established_dimension_reads_the_first_non_empty_part() {
        let set = SegmentSet::from_segments(vec![build_sealed(5, 0, 0.0)]);
        assert_eq!(set.established_dimension(), 3);
        assert_eq!(SegmentSet::empty().established_dimension(), 0);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p strata-index segment_set`
Expected: FAIL to compile — `no function or associated item named 'empty' found`, `... 'from_segments' ...`, `... 'with_appended' ...`, `... 'len' ...`, `... 'is_empty' ...`.

- [ ] **Step 3: Replace `crates/index/src/segment_set.rs`'s module doc (lines 1-10)**

```rust
//! The set of index parts a snapshot searches over: an immutable,
//! cheaply-clonable list of sealed on-disk segments, one per vector-carrying
//! commit. A snapshot's segment set is exactly its manifest's
//! `segments` list, which is what makes a published snapshot's index view
//! and its row view the same atomic fact (base design doc §4/§5).
//!
//! [`SegmentSet::search`]/[`SegmentSet::search_filtered`] query **every**
//! part for its own top-`k` at the caller's full per-part `ef_search`, map
//! each part's local ordinals back to global row-ids, merge by ascending
//! distance, dedup by row-id, and truncate to `k`. The over-fetch is
//! deliberate: it is *why* recall rises with segment count (ADR 0008), not
//! an accident to tune away. Zone-map-based pruning of parts that provably
//! cannot match is W4's job; nothing here prunes.
//!
//! Dedup by row-id is a no-op in S1 (each row-id lives in exactly one
//! segment, since there is no compaction yet) and is implemented now so
//! S2's compaction — where a row transiently exists in both a source
//! segment and its compacted output — does not require reopening this
//! merge logic.
//!
//! [`IndexPart::Live`] is a **transient** variant, present only while
//! `crates/txn` is being cut over to the segment write path; it is deleted
//! in the same workstream. See
//! `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §1 for
//! why every method below iterates length-independently and matches
//! exhaustively rather than destructuring a fixed-arity slice.
```

- [ ] **Step 4: Replace `IndexPart` and add the constructors (lines 17-58, i.e. through the end of `sole_live`)**

Replace everything from `/// One part of a segment set.` (line 17) through `sole_live`'s closing `}` (line 58) with:

```rust
/// One part of a segment set.
pub enum IndexPart {
    /// The legacy shared, mutable in-memory graph. **Transient** — exists
    /// only until `crates/txn`'s write path is fully cut over to segments,
    /// and is deleted in this same workstream. No code may rely on it.
    Live(Arc<HnswIndex>),
    /// One immutable on-disk segment, loaded once and shared by every
    /// snapshot whose manifest lists it.
    Sealed(Arc<SegmentReader>),
}

/// The set of index parts a snapshot searches. Cheap to clone (`Arc<[_]>`).
#[derive(Clone)]
pub struct SegmentSet {
    parts: Arc<[IndexPart]>,
}

impl SegmentSet {
    /// A set with no parts — a freshly created dataset, or one whose
    /// commits have all been vector-less. Searches to an empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            parts: Arc::from(Vec::new()),
        }
    }

    /// Builds a segment set of exactly one live part, wrapping the legacy
    /// shared mutable index. **Transient** — see [`IndexPart::Live`].
    #[must_use]
    pub fn from_live(index: Arc<HnswIndex>) -> Self {
        Self {
            parts: Arc::from(vec![IndexPart::Live(index)]),
        }
    }

    /// Builds a segment set over already-loaded sealed segments, in
    /// manifest order. `Dataset::open`'s constructor.
    #[must_use]
    pub fn from_segments(parts: Vec<Arc<SegmentReader>>) -> Self {
        Self {
            parts: parts.into_iter().map(IndexPart::Sealed).collect(),
        }
    }

    /// A new set holding this set's parts plus `reader`, in that order.
    /// `self` is untouched — an already-published snapshot must never see
    /// a segment committed after it was taken.
    ///
    /// This clones the parts slice, so it is O(parts) per commit and
    /// O(parts²) across a session. Accepted for S1, which explicitly
    /// tolerates one segment per commit; S2's compaction is what bounds the
    /// part count. Do not "fix" it by deferring or batching segment
    /// publication — that would break the no-silent-buffering invariant.
    #[must_use]
    pub fn with_appended(&self, reader: Arc<SegmentReader>) -> Self {
        let mut parts: Vec<IndexPart> = Vec::with_capacity(self.parts.len() + 1);
        for part in self.parts.iter() {
            match part {
                IndexPart::Live(index) => parts.push(IndexPart::Live(Arc::clone(index))),
                IndexPart::Sealed(sealed) => parts.push(IndexPart::Sealed(Arc::clone(sealed))),
            }
        }
        parts.push(IndexPart::Sealed(reader));
        Self {
            parts: Arc::from(parts),
        }
    }

    /// How many parts this set searches. Exposed so `crates/txn`'s tests
    /// can assert the manifest's segment list and the snapshot's in-memory
    /// view never disagree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Queries every part and merges the results — see this module's doc
    /// comment for the merge contract.
    fn fan_out(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let mut merged: Vec<(u64, f32)> = Vec::new();
        for part in self.parts.iter() {
            match part {
                IndexPart::Live(index) => {
                    // A live graph's local id IS its row-id, so no mapping.
                    let raw = k_nn_search_generic(
                        &index.graph,
                        &crate::distance::L2,
                        query,
                        k,
                        ef_search,
                        filter,
                    )?;
                    merged.extend(raw);
                }
                IndexPart::Sealed(reader) => {
                    let raw = k_nn_search_generic(
                        reader.as_ref(),
                        &crate::distance::L2,
                        query,
                        k,
                        ef_search,
                        filter,
                    )?;
                    // A segment's local id is its ordinal within THIS
                    // segment. Returning it unmapped would hand the caller
                    // a plausible-looking wrong row-id -- see this task's
                    // `search_returns_global_row_ids_not_segment_local_ordinals`.
                    merged.extend(
                        raw.into_iter()
                            .filter_map(|(local, dist)| Some((reader.row_id_at(local)?, dist))),
                    );
                }
            }
        }
        merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        // Nearest-first order means the retained occurrence of a duplicated
        // row-id is always its nearest one.
        let mut seen = std::collections::HashSet::with_capacity(merged.len());
        merged.retain(|&(row_id, _)| seen.insert(row_id));
        merged.truncate(k);
        Ok(merged
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }
```

- [ ] **Step 5: Replace `search`, `search_filtered`, `established_dimension` and `live_arc` (old lines 60-148)**

Replace those four methods (everything from `/// Mirrors [`HnswIndex::search`]` down to and including `live_arc`'s closing `}`, but **not** the `impl` block's own closing `}` at line 149) with:

```rust
    /// Approximate nearest-neighbor search across every part, gated by
    /// `is_visible` during traversal (never as a post-filter over an
    /// already-capped top-k).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] if `query`'s length
    /// doesn't match a part's established dimension.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        self.fan_out(query, k, ef_search, &is_visible)
    }

    /// As [`Self::search`], additionally restricted to `live_ids`.
    /// `live_ids` membership and `is_visible` are composed into ONE
    /// predicate by [`build_live_filter`] — built **once here** and shared
    /// by every part, never rebuilt per part.
    ///
    /// # Errors
    ///
    /// Same as [`Self::search`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let filter = build_live_filter(live_ids, is_visible);
        self.fan_out(query, k, ef_search, &filter)
    }

    /// The vector dimension this set's parts were built at, or `0` if the
    /// set is empty (no vector has ever been committed). `crates/txn` uses
    /// this to pre-validate a commit's vector dimensions before building
    /// anything — see
    /// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §2.
    ///
    /// Every part necessarily agrees (that pre-validation is what enforces
    /// it), so the first non-empty part's dimension is the set's.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        self.parts
            .iter()
            .map(|part| match part {
                IndexPart::Live(index) => index.established_dimension(),
                IndexPart::Sealed(reader) => reader.dimension(),
            })
            .find(|&dim| dim != 0)
            .unwrap_or(0)
    }

    /// Recovers the underlying live index. **Transient** — see
    /// [`IndexPart::Live`]; deleted with that variant.
    ///
    /// # Panics
    ///
    /// Panics if this set does not hold exactly one `Live` part, which no
    /// caller can produce once `crates/txn` is cut over.
    #[must_use]
    pub fn live_arc(&self) -> Arc<HnswIndex> {
        for part in self.parts.iter() {
            if let IndexPart::Live(index) = part {
                return Arc::clone(index);
            }
        }
        unreachable!("live_arc called on a set with no Live part")
    }
```

- [ ] **Step 6: Update the imports at the top of the file (lines 12-15)**

```rust
use std::sync::Arc;

use crate::graph::k_nn_search_generic;
use crate::hnsw::{HnswIndex, IndexError, VectorMatch, build_live_filter};
use crate::segment_reader::SegmentReader;
```

- [ ] **Step 7: Make the test module's constants reachable from the new helper**

The existing test module already defines `PHI`, `SQRT2`, `SQRT3` (lines 163-165) and imports `EfConstruction, MaxConnections, MaxElements, MaxLayers` (line 155). `build_sealed` from Step 1 uses all of them plus `HnswIndex` (already in scope via `use super::*`). No import changes needed.

- [ ] **Step 8: Run the tests**

Run: `cargo test -p strata-index segment_set`
Expected: PASS — the three pre-existing `Live` equivalence tests (unchanged and still asserting exact row-id order and distance) plus the eight new ones.

If `search_over_one_live_part_matches_hnsw_index_search_directly` now fails, the merge changed single-part behavior — it must not: sorting is a no-op on an already-sorted list, dedup is a no-op on unique row-ids, and `truncate(k)` was already applied inside `k_nn_search_generic`. Diff the merge against the code above rather than adjusting that test.

- [ ] **Step 9: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean. `crates/txn` still compiles: `from_live`, `live_arc`, `search`, `search_filtered` and `established_dimension` all kept their signatures.

If clippy flags `clippy::len_without_is_empty` — it will not, since `is_empty` is defined — or `clippy::missing_panics_doc` on `live_arc`, the `# Panics` section above already satisfies it.

- [ ] **Step 10: Commit**

```bash
git add crates/index/src/segment_set.rs
git commit -m "feat(index): SegmentSet gains Sealed parts and real fan-out search

Adds IndexPart::Sealed, empty/from_segments/with_appended, and a
multi-part fan-out merge: every part is queried for its own top-k at the
caller's full per-part ef, local ordinals are mapped back to global
row-ids, results merge by ascending distance, dedup by row-id, truncate
to k. No zone-map pruning -- that is W4.

Deletes sole_live in this same commit, per
docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md section 1:
its arity-refutable slice pattern would have panicked at runtime, with
no compile-time signal, the moment a second part existed. Every method is
now length-independent iteration plus an exhaustive match.

IndexPart::Live survives only until crates/txn is cut over; it is deleted
later in this same workstream.

Fan-out lands here rather than in W3.3 because a write path that produces
a second segment while search consults one is a shipped recall
regression -- see the Scope decision in
docs/superpowers/plans/2026-07-25-s1-w3-2a-segment-write-path.md."
```

---

### Task 5: Relocate the vector-extraction and dimension-validation logic in `crates/txn`

**Files:**
- Modify: `crates/txn/src/dataset.rs` — `write_pending_batches` (lines 1222-1287), `build_delta_entries` (lines 1380-1454), `validate_delta_dimensions` (lines 1456-1492), the `commit` call site (line 878), `write_phase`'s return type (line 1121), the five `build_delta_entries_*` tests (lines 3611-3804)

**Interfaces:**
- Consumes: `SegmentSet::established_dimension` (Task 4, unchanged signature).
- Produces:
  - `pub(crate) struct VectorInsert { pub(crate) row_id: u64, pub(crate) vector: Vec<f32> }`
  - `fn build_vector_inserts(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<VectorInsert>>`
  - `fn validate_vector_dimensions(inserts: &[VectorInsert], established: usize) -> Result<()>`
  Task 6 consumes all three.

**Why this is its own task (pre-W3.1 amendment §3):** deleting `delta_log.rs` takes three pieces of load-bearing logic with it if done carelessly — the Arrow `FixedSizeList<Float32>` → `Vec<f32>` extraction, the non-finite (NaN/Inf) rejection, and the dimension pre-validation. **None of the three is deleted; only the log-shaped I/O around them is.** Doing the relocation first, with the delta log still being written, makes this a pure rename that every existing test proves, and leaves Task 6 as a clean behavioural flip. A reviewer can accept this task and reject Task 6, or vice versa.

**Why `validate_*` takes a plain `usize` (post-W3.1 amendment §2):** `Transaction.graph` ceases to exist in Task 6, so the check cannot read `graph.established_dimension()`. The established dimension is available from the current snapshot's `SegmentSet` without opening any file. Re-sourcing it now, while the graph still exists, isolates the signature change from the behavioural change.

- [ ] **Step 1: Add `VectorInsert` and rename `build_delta_entries` in `crates/txn/src/dataset.rs`**

Replace the whole of `build_delta_entries` (lines 1380-1454, doc comment included) with:

```rust
/// One row's vector, ready to be inserted into a segment's working index.
/// The in-memory carrier between `write_pending_batches` and the segment
/// build — the role `strata_index::DeltaEntry` used to play before the
/// delta log was removed. There is no `Tombstone` counterpart: deletion is
/// the manifest's versioned `tombstones` list, never an index-level entry
/// (base design doc §5).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorInsert {
    pub(crate) row_id: u64,
    pub(crate) vector: Vec<f32>,
}

/// Extracts one [`VectorInsert`] per row in `batch` with a non-null vector,
/// keyed by the row-ids assigned starting at `row_id_base`. A `batch` with
/// no `"vector"` column at all (a table with no vector column defined)
/// simply produces no entries — that's not an error, unlike a `"vector"`
/// column present with the wrong type, which is. A commit that produces
/// zero entries writes **no segment at all** (see `build_and_write_segment`
/// and `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §3c).
///
/// Also rejects any row whose vector contains a non-finite (`NaN`/`Infinity`)
/// component. This guard predates the segment format — it was originally
/// justified by the delta log's JSON encoding, which silently wrote
/// non-finite `f32`s as `null` — but the reason to keep it is independent
/// of any on-disk encoding: a `NaN` component poisons every distance
/// comparison in `search_layer_generic` (`Candidate::cmp`'s `partial_cmp`
/// fallback silently treats an incomparable pair as equal), so one bad
/// vector would corrupt search results for the whole segment. Must run
/// before any file for this batch is written to disk.
///
/// # Errors
///
/// Returns an error if `batch` has a `"vector"` column that isn't a
/// `FixedSizeList<Float32>`, or if any row's vector contains a non-finite
/// component.
fn build_vector_inserts(batch: &RecordBatch, row_id_base: u64) -> Result<Vec<VectorInsert>> {
    let Ok(vec_idx) = batch.schema_ref().index_of("vector") else {
        return Ok(Vec::new());
    };
    let vectors = batch
        .column(vec_idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column must be FixedSizeList".to_string(),
            ))
        })?;

    // Downcast the flattened child array once, before the per-row loop,
    // instead of calling `vectors.value(i)` (a fresh sliced ArrayRef + Arc
    // allocation) and re-downcasting the result on every row -- the
    // concrete child type is invariant per column, only the row index
    // changes (mirrors the fix already applied in group_by.rs). Every row's
    // slice is then a plain `i * value_length` index into the flat buffer:
    // `FixedSizeListArray::offset()` and `Float32Array::offset()` are both
    // hardcoded to 0 in arrow-array 58.3.0 (`slice()` bakes any logical
    // offset directly into a new, already-adjusted `values` buffer rather
    // than tracking a separate offset field -- confirmed against the
    // installed source), so no extra offset arithmetic is needed here.
    let value_length = usize::try_from(vectors.value_length()).unwrap_or(0);
    let flat: &arrow::array::Float32Array =
        vectors.values().as_any().downcast_ref().ok_or_else(|| {
            TxnError::Arrow(arrow::error::ArrowError::CastError(
                "vector column's inner type must be Float32".to_string(),
            ))
        })?;
    let flat_values = flat.values();

    let mut entries = Vec::with_capacity(vectors.len());
    for i in 0..vectors.len() {
        if vectors.is_null(i) {
            continue;
        }
        let start = i * value_length;
        let row = &flat_values[start..start + value_length];
        let row_id = row_id_base.checked_add(u64::try_from(i)?).ok_or_else(|| {
            TxnError::ManifestOverflow(format!("row_id_base {row_id_base} + {i}"))
        })?;
        if row.iter().any(|component| !component.is_finite()) {
            return Err(TxnError::NonFiniteVectorComponent { row_id });
        }
        entries.push(VectorInsert {
            row_id,
            vector: row.to_vec(),
        });
    }
    Ok(entries)
}
```

- [ ] **Step 2: Replace `validate_delta_dimensions` (lines 1456-1492)**

```rust
/// Validates that every vector in `inserts` shares one consistent
/// dimension — both against each other, and against `established` (the
/// dimension already fixed by whatever has been committed so far, or `0` if
/// nothing has) — before a segment is built from any of them.
///
/// This is a **pre-build, pre-lock** check, and it is what keeps a
/// half-built segment from ever being fsynced or published: `insert_owned`'s
/// only fallible path is dimension validation, so without this a ragged
/// batch would fail partway through the working index's construction after
/// earlier vectors had already been inserted. The half-built index is
/// discarded either way, but failing before any I/O keeps the error cheap
/// and keeps the failure mode identical whichever pending batch is ragged.
///
/// `established` is a plain `usize`, not a graph handle: after S1 W3.2a
/// there is no shared live graph to ask. The caller sources it from the
/// current snapshot's `SegmentSet::established_dimension()` — available
/// without opening any segment file. See
/// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §2.
///
/// # Errors
///
/// Returns [`TxnError::Index`] wrapping an
/// [`strata_index::IndexError::DimensionMismatch`] if any vector's length
/// disagrees with `established`, or with an earlier vector's length in this
/// same commit when `established` is `0`.
fn validate_vector_dimensions(inserts: &[VectorInsert], established: usize) -> Result<()> {
    let mut expected = established;
    for insert in inserts {
        if expected == 0 {
            expected = insert.vector.len();
        } else if insert.vector.len() != expected {
            return Err(TxnError::Index(
                strata_index::IndexError::DimensionMismatch {
                    query_len: insert.vector.len(),
                    expected,
                },
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Rewire `write_pending_batches` to carry `VectorInsert`s (lines 1222-1287)**

Change its return type from `Result<Vec<DeltaEntry>>` to `Result<Vec<VectorInsert>>`, rename the local `all_deltas` → `all_inserts` and `deltas` → `inserts`, and convert to `DeltaEntry` only at the (still present, deleted in Task 6) delta-log write. Concretely, inside the `for (i, batch)` loop replace:

```rust
            let deltas = build_delta_entries(batch, row_id_base)?;
```

with:

```rust
            let inserts = build_vector_inserts(batch, row_id_base)?;
```

and replace:

```rust
            let delta_file_name = format!("{attempt_id:020}-{i}.deltalog");
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
            all_deltas.extend(deltas);
```

with:

```rust
            // Still written for now: Task 6 of the S1 W3.2a plan is what
            // removes the delta-log path, so this task stays a pure
            // relocation that every existing test proves unchanged.
            let delta_file_name = format!("{attempt_id:020}-{i}.deltalog");
            let deltas: Vec<DeltaEntry> = inserts
                .iter()
                .map(|insert| DeltaEntry::Insert {
                    row_id: insert.row_id,
                    vector: insert.vector.clone(),
                })
                .collect();
            write_delta_log(&data_dir.join(&delta_file_name), &deltas)?;

            data_files.push(DataFileEntry {
                name: file_name,
                stats,
                delta_log: delta_file_name,
            });
            all_inserts.extend(inserts);
```

and change `let mut all_deltas = Vec::new();` to `let mut all_inserts = Vec::new();`, and the final `Ok(all_deltas)` to `Ok(all_inserts)`.

Also update its doc comment's second paragraph (lines 1192-1197) from "Returns every `DeltaEntry` produced across all pending batches" to "Returns every [`VectorInsert`] produced across all pending batches, in order — the segment build consumes these directly."

- [ ] **Step 4: Update `write_phase`'s return type and `commit`'s call site**

Change `write_phase`'s signature (line 1117-1121) return type from:

```rust
    ) -> Result<(Vec<DataFileEntry>, Vec<DeltaEntry>, Option<RowIdClaim>)> {
```

to:

```rust
    ) -> Result<(Vec<DataFileEntry>, Vec<VectorInsert>, Option<RowIdClaim>)> {
```

In `commit` (lines 877-878), replace:

```rust
        let (new_data_files, deltas, mut claim) = self.write_phase(&data_dir, ts)?;
        validate_delta_dimensions(&deltas, &self.graph)?;
```

with:

```rust
        let (new_data_files, inserts, mut claim) = self.write_phase(&data_dir, ts)?;
        // Sourced from the current snapshot's segment set rather than from
        // a live graph handle -- see `validate_vector_dimensions`' doc.
        let established_dimension = self.current.load().index.established_dimension();
        validate_vector_dimensions(&inserts, established_dimension)?;
```

- [ ] **Step 5: Update the in-lock apply loop (lines 956-966) to consume `VectorInsert`s**

Replace:

```rust
        for delta in deltas {
            match delta {
                DeltaEntry::Insert { row_id, vector } => {
                    self.graph.insert_owned(row_id, vector)?;
                    residue_guard.record(row_id);
                }
                DeltaEntry::Tombstone { row_id } => {
                    tombstones.insert(row_id);
                }
            }
        }
```

with:

```rust
        for insert in inserts {
            self.graph.insert_owned(insert.row_id, insert.vector)?;
            residue_guard.record(insert.row_id);
        }
```

(The `Tombstone` arm was unreachable: `build_delta_entries` never produced one — only the hand-edited-delta-log test at line 3835 did, and only through `replay_index`, not through this loop. Deleting it here is not a behavior change.)

- [ ] **Step 6: Rename the five `build_delta_entries_*` tests (lines 3611-3804)**

Rename each test and its call, and replace the `DeltaEntry` destructuring with direct field access:

| Old name | New name |
|---|---|
| `build_delta_entries_skips_null_vector_rows_without_erroring` | `build_vector_inserts_skips_null_vector_rows_without_erroring` |
| `build_delta_entries_produces_the_correct_vector_per_row` | `build_vector_inserts_produces_the_correct_vector_per_row` |
| `build_delta_entries_reads_the_correct_vector_from_a_sliced_batch` | `build_vector_inserts_reads_the_correct_vector_from_a_sliced_batch` |
| `build_delta_entries_errors_on_wrong_inner_type_even_with_zero_rows` | `build_vector_inserts_errors_on_wrong_inner_type_even_with_zero_rows` |
| `build_delta_entries_errors_when_vector_column_is_not_a_fixed_size_list` | `build_vector_inserts_errors_when_vector_column_is_not_a_fixed_size_list` |
| `build_delta_entries_errors_when_vector_inner_type_is_not_float32` | `build_vector_inserts_errors_when_vector_inner_type_is_not_float32` |

In the first test, replace:

```rust
        let deltas = build_delta_entries(&batch, 0).unwrap();
        assert_eq!(
            deltas.len(),
            1,
            "the null-vector row must be skipped, not errored on"
        );
        match &deltas[0] {
            DeltaEntry::Insert { row_id, .. } => assert_eq!(*row_id, 0),
            DeltaEntry::Tombstone { .. } => panic!("expected an Insert entry"),
        }
```

with:

```rust
        let inserts = build_vector_inserts(&batch, 0).unwrap();
        assert_eq!(
            inserts.len(),
            1,
            "the null-vector row must be skipped, not errored on"
        );
        assert_eq!(inserts[0].row_id, 0);
```

In the second and third tests, replace the `as_insert` closure and its uses. For `build_vector_inserts_produces_the_correct_vector_per_row`:

```rust
        let inserts = build_vector_inserts(&batch, 100).unwrap();
        assert_eq!(inserts.len(), 3);
        let as_pair = |i: &VectorInsert| (i.row_id, i.vector.clone());
        assert_eq!(as_pair(&inserts[0]), (100, vec![1.0, 2.0, 3.0]));
        assert_eq!(as_pair(&inserts[1]), (101, vec![4.0, 5.0, 6.0]));
        assert_eq!(as_pair(&inserts[2]), (102, vec![7.0, 8.0, 9.0]));
```

and for `build_vector_inserts_reads_the_correct_vector_from_a_sliced_batch`:

```rust
        let inserts = build_vector_inserts(&sliced, 0).unwrap();
        assert_eq!(inserts.len(), 2);
        let as_pair = |i: &VectorInsert| (i.row_id, i.vector.clone());
        assert_eq!(as_pair(&inserts[0]), (0, vec![7.0, 8.0, 9.0]));
        assert_eq!(as_pair(&inserts[1]), (1, vec![10.0, 11.0, 12.0]));
```

In the remaining three, replace `build_delta_entries(` with `build_vector_inserts(` and `result` stays an `is_err()` check — no other change.

- [ ] **Step 7: Add a direct unit test for the re-sourced validator**

Add next to the renamed tests:

```rust
    #[test]
    fn validate_vector_dimensions_rejects_a_ragged_commit_against_a_plain_established_dimension() {
        // The signature change the W3.2 amendment section 2 requires: the
        // check reads a `usize`, not a live graph handle, because after
        // W3.2a there is no shared graph to ask.
        let ragged = vec![
            VectorInsert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            VectorInsert {
                row_id: 1,
                vector: vec![1.0, 2.0],
            },
        ];
        let result = validate_vector_dimensions(&ragged, 0);
        assert!(
            matches!(
                result,
                Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: 2,
                        expected: 3
                    }
                ))
            ),
            "two pending vectors of different lengths must be rejected even with \
             nothing established yet: {result:?}"
        );
    }

    #[test]
    fn validate_vector_dimensions_rejects_a_commit_disagreeing_with_the_established_dimension() {
        let inserts = vec![VectorInsert {
            row_id: 0,
            vector: vec![1.0, 2.0],
        }];
        let result = validate_vector_dimensions(&inserts, 3);
        assert!(
            matches!(
                result,
                Err(TxnError::Index(
                    strata_index::IndexError::DimensionMismatch {
                        query_len: 2,
                        expected: 3
                    }
                ))
            ),
            "{result:?}"
        );
    }

    #[test]
    fn validate_vector_dimensions_accepts_an_empty_commit_and_a_consistent_one() {
        assert!(validate_vector_dimensions(&[], 3).is_ok());
        assert!(validate_vector_dimensions(&[], 0).is_ok());
        let consistent = vec![
            VectorInsert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            VectorInsert {
                row_id: 1,
                vector: vec![4.0, 5.0, 6.0],
            },
        ];
        assert!(validate_vector_dimensions(&consistent, 3).is_ok());
        assert!(validate_vector_dimensions(&consistent, 0).is_ok());
    }
```

- [ ] **Step 8: Run the whole `crates/txn` suite — this is the "pure relocation" proof**

Run: `cargo test -p strata-txn`
Expected: every pre-existing test passes **unmodified except for the six renames above**. In particular `commit_rejects_inconsistent_batch_dimensions_before_touching_the_shared_graph`, `committing_a_batch_with_a_non_finite_vector_component_is_rejected_cleanly`, and `reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log` must all still pass — nothing about the on-disk format or the commit protocol changed in this task.

- [ ] **Step 9: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 10: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "refactor(txn): relocate vector extraction and dimension validation off the delta log

Pure relocation, no behavior change: build_delta_entries ->
build_vector_inserts returning a new VectorInsert carrier, and
validate_delta_dimensions -> validate_vector_dimensions taking a plain
usize sourced from the snapshot's SegmentSet::established_dimension()
rather than a live graph handle.

The delta log is still written; the segment write path lands next. Doing
the relocation first keeps this a rename every existing test proves,
per docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md section 3
(these three pieces of logic are relocated, never deleted, with the log)
and section 2 of the 2026-07-25 W3.2 amendment (the usize signature)."
```

---

### Task 6: The cutover — build, write and publish a segment per commit

**Files:**
- Modify: `crates/txn/src/error.rs` (new `CorruptSegment` variant after `UnsafeManifestPath`, line 43)
- Modify: `crates/storage/src/manifest.rs` (`DataFileEntry.delta_log` at lines 33-36 — removed; six test literals at lines 256, 271, 276, 303, 382, 482)
- Modify: `crates/txn/src/dataset.rs` — imports (25-32), `create_with_commit_log_capacity` (270-304), `open` (334-387), `begin` (417-439), `Transaction` (456-501), `GraphResidueGuard` (589-677), `commit` (873-1089), `write_phase` (1117-1190), `write_pending_batches` (1222-1287), `replay_index` (1338-1378 — **deleted**), `safe_join`'s doc (1494-1501), and the affected tests

**Interfaces:**
- Consumes: `strata_storage::write_bytes` (Task 1), `HnswIndex::to_segment_bytes` + `SEGMENT_FORMAT_VERSION` (Task 2), `SegmentReader::from_bytes`/`node_count`/`row_id_range`/`byte_len` (Task 3), `SegmentSet::empty`/`from_segments`/`with_appended`/`len`/`established_dimension` (Task 4), `VectorInsert`/`build_vector_inserts`/`validate_vector_dimensions` (Task 5).
- Produces:
  - `struct PublishedSegment { entry: strata_storage::SegmentEntry, reader: Arc<strata_index::SegmentReader> }`
  - `fn Transaction::build_and_write_segment(data_dir: &Path, attempt_id: u64, inserts: Vec<VectorInsert>) -> Result<Option<PublishedSegment>>`
  - `fn load_segments(dir: &Path, manifest: &Manifest) -> Result<strata_index::SegmentSet>`
  - `pub(crate) fn Transaction::pause_before_manifest_commit(&mut self, checkpoint: Checkpoint)` (renamed from `pause_after_graph_apply`)
  - `TxnError::CorruptSegment(String)`
  Tasks 9 and 10 consume the test-only injectors; nothing else consumes these.

**The five behavioural facts this task establishes, all of them load-bearing:**
1. The segment is built, serialized and fsynced **entirely outside `commit_lock`**, in `write_phase` — which is already the fsync point for row data.
2. **Zero index mutation happens inside the lock.** The in-lock step appends a `SegmentEntry` to the manifest, nothing more.
3. A commit whose vector extraction yields zero rows writes **no `.seg` file and pushes no `SegmentEntry`** (post-W3.1 amendment §3c). `manifest.segments.len() == N` holds only for `N` *vector-carrying* commits.
4. The working index is keyed by **segment-local ordinals `0..N`**, not global row-ids (amendment §3b): `row_ids` becomes a positional dump that is ascending by construction, and the working `NodeTable` stays inside its first 65536-id chunk no matter how high the commit's actual row-ids are.
5. `GraphResidueGuard` **stays in the code and becomes inert** (base design §4's W3.2a/W3.2b split: migrate the guarantee, then remove the mechanism). Task 9's failed-commit tests land against this state; a follow-up plan deletes the type.

- [ ] **Step 1: Add `TxnError::CorruptSegment` in `crates/txn/src/error.rs`**

Insert after the `UnsafeManifestPath` variant (lines 42-43):

```rust
    #[error("segment listed in the manifest is unusable: {0}")]
    CorruptSegment(String),
```

- [ ] **Step 2: Remove `DataFileEntry.delta_log` in `crates/storage/src/manifest.rs`**

Delete lines 33-36 (the `delta_log` field and its doc comment) from `DataFileEntry`, leaving:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
}
```

Then delete the `delta_log: "d.deltalog".to_string(),` line from all six test literals in this file (lines 256, 271, 276, 303, 382, 482). **No `#[serde(default)]` shim and no migration:** removing a non-`default` field means every manifest already on disk stops deserializing, which is explicitly acceptable per base design §0.3 (no backward compatibility, this is pre-release).

- [ ] **Step 3: Update `safe_join`'s doc contract in `crates/txn/src/dataset.rs` (lines 1494-1501)**

Replace its doc comment with:

```rust
/// Joins `name` onto `data_dir`, rejecting any `name` whose path
/// components aren't all bare filename segments (`Component::Normal`) — a
/// `name` containing `..` or an absolute path (which `Path::join` would
/// otherwise resolve/replace unchecked) must never let a corrupted/hostile
/// manifest read a file outside the dataset's own `data/` directory.
/// `DataFileEntry.name` and `SegmentEntry.name`
/// (`crates/storage/src/manifest.rs`) are both documented as "relative to
/// the dataset's data/ directory" — this is what actually enforces that
/// contract instead of merely documenting it, for both.
```

(The old text named `DataFileEntry.delta_log`, which no longer exists; `SegmentEntry.name` is the new second consumer, via `load_segments` below.)

- [ ] **Step 4: Update the imports at the top of `crates/txn/src/dataset.rs` (lines 25-32)**

```rust
use strata_index::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
use strata_storage::{
    ColumnStats, DataFileEntry, Manifest, SegmentEntry, Value, commit_manifest, compute_stats,
    read_current, write_batch, write_bytes,
};
```

- [ ] **Step 5: Delete `replay_index` (lines 1338-1378) and add `load_segments` in its place**

```rust
/// Loads every segment `manifest` lists into a [`strata_index::SegmentSet`],
/// in manifest order. This is what replaced delta-log replay: a segment is
/// the durable built graph, so recovery is `O(bytes)` validation with zero
/// distance evaluations and zero graph construction, rather than replaying
/// every historical insert through `HnswIndex::insert_owned`.
///
/// Used only by [`Dataset::open`]. A freshly created dataset has no
/// segments and starts from [`strata_index::SegmentSet::empty`].
///
/// # Errors
///
/// Returns [`TxnError::UnsafeManifestPath`] if a segment name tries to
/// escape `data/`, [`TxnError::Io`] if a listed segment can't be read,
/// [`TxnError::CorruptSegment`] if a segment's on-disk length disagrees
/// with the length the manifest records for it (a truncated or overwritten
/// file), or [`TxnError::Index`] if a segment fails its own header/body
/// validation.
fn load_segments(dir: &Path, manifest: &Manifest) -> Result<strata_index::SegmentSet> {
    let data_dir = data_subdir(dir);
    let mut parts = Vec::with_capacity(manifest.segments.len());
    for entry in &manifest.segments {
        let path = safe_join(&data_dir, &entry.name)?;
        let bytes = std::fs::read(&path)?;
        // Checked before parsing so a truncated file is reported as the
        // truncation it is, rather than as whichever internal check its
        // remaining bytes happen to trip first. `SegmentEntry.byte_len`
        // exists for exactly this (base design doc §3).
        if u64::try_from(bytes.len())? != entry.byte_len {
            return Err(TxnError::CorruptSegment(format!(
                "segment {} is {} bytes on disk but the manifest records {}",
                entry.name,
                bytes.len(),
                entry.byte_len
            )));
        }
        parts.push(Arc::new(strata_index::SegmentReader::from_bytes(&bytes)?));
    }
    Ok(strata_index::SegmentSet::from_segments(parts))
}
```

Keep `MAX_REASONABLE_ROW_ID_CAPACITY` (lines 1319-1336) exactly as it is, but update its doc comment's first line from "enforced at open before any row-id from that manifest can reach the index" — the wording still holds; the *enforcement site* moves into `Dataset::open` in Step 7. Add to the end of its doc comment:

```rust
/// Enforced in [`Dataset::open`] directly (it used to live in
/// `replay_index`, which no longer exists).
```

- [ ] **Step 6: `Dataset::create_with_commit_log_capacity` starts from an empty segment set**

In `create_with_commit_log_capacity` (lines 270-304), delete line 282 (`let graph = new_hnsw_index(0)?;`) and change the `Snapshot` literal's index line (292) from:

```rust
            index: strata_index::SegmentSet::from_live(Arc::new(graph)),
```

to:

```rust
            // A brand-new dataset has committed no vectors, so it has no
            // segments — not an empty graph. `vector_search` on it returns
            // an empty result, which is what it always did.
            index: strata_index::SegmentSet::empty(),
```

- [ ] **Step 7: `Dataset::open` loads segments instead of replaying**

In `open` (lines 334-387), replace line 337:

```rust
        let (graph, tombstones) = replay_index(&dir, &manifest)?;
```

with:

```rust
        // The capacity guard used to live inside `replay_index`, which
        // sized an `HnswIndex` from `next_row_id`. Nothing sizes an
        // allocation from it any more, but the ceiling is still a
        // panic-safety bound on what row-ids may reach `NodeTable` — see
        // `MAX_REASONABLE_ROW_ID_CAPACITY`.
        if manifest.next_row_id > MAX_REASONABLE_ROW_ID_CAPACITY {
            return Err(TxnError::UnreasonableCapacity(
                manifest.next_row_id,
                MAX_REASONABLE_ROW_ID_CAPACITY,
            ));
        }
        let index = load_segments(&dir, &manifest)?;
        // The manifest's tombstone list is now the only source: index-level
        // tombstone entries went away with the delta log, and never had a
        // producer on the commit path anyway.
        let tombstones: imbl::HashSet<u64> = manifest.tombstones.iter().copied().collect();
```

and change the `Snapshot` literal's index line (375) from `index: strata_index::SegmentSet::from_live(Arc::new(graph)),` to `index,`.

Also update the `in_flight` comment inside that literal (lines 366-372) — its last sentence says "their delta logs are not replayed". Replace that sentence with: "and their segments were never listed in a manifest, so nothing exists at those ids to be found."

- [ ] **Step 8: Remove `Transaction.graph` and `Dataset::begin`'s `live_arc()` call**

In `Dataset::begin` (lines 417-439), delete line 422 (`graph: snapshot.index.live_arc(),`) and change line 434-437's checkpoint field name:

```rust
            #[cfg(test)]
            pause_after_row_id_claim: None,
            #[cfg(test)]
            pause_before_manifest_commit: None,
            #[cfg(any(test, loom))]
            inject_panic_before_manifest_commit: false,
```

(The last field is added in Task 9; add it here now so `begin` is edited once. Task 9 adds the matching struct field and use — if you prefer to keep this task self-contained, omit that line and add it in Task 9 instead; either is acceptable as long as the crate compiles at the end of each task.)

**Decision:** add it in Task 9, not here. Leave `begin` with only the two rename-affected lines changed:

```rust
            #[cfg(test)]
            pause_after_row_id_claim: None,
            #[cfg(test)]
            pause_before_manifest_commit: None,
```

In the `Transaction` struct (lines 456-501), delete line 465 (`graph: Arc<HnswIndex>,`) and rename the `pause_after_graph_apply` field (lines 497-500) to:

```rust
    /// Test-only: stops this commit inside `commit_lock`, after its
    /// conflict check has passed and its segment is already fsynced, but
    /// before `commit_manifest` makes any of it durable. See [`Checkpoint`].
    #[cfg(test)]
    pause_before_manifest_commit: Option<Checkpoint>,
```

Rename the setter (lines 748-755) to:

```rust
    /// Test-only: stops [`Self::commit`] inside `commit_lock`, after the
    /// conflict check and after this commit's `.seg` file is durable, but
    /// before `commit_manifest`. The instant at which a concurrent reader
    /// could observe a partially-applied commit, if one were possible —
    /// after W3.2a it is not, because nothing shared has been touched yet.
    #[cfg(test)]
    pub(crate) fn pause_before_manifest_commit(&mut self, checkpoint: Checkpoint) {
        self.pause_before_manifest_commit = Some(checkpoint);
    }
```

- [ ] **Step 9: Make `GraphResidueGuard` inert (lines 589-677)**

Replace the entire block — doc comment, struct, impl, and `Drop` impl — with:

```rust
/// **Inert as of S1 W3.2a. Deleted in W3.2b.**
///
/// Until W3.2a, [`Transaction::commit`] applied each insert's vector to a
/// shared `Arc<HnswIndex>` *before* `commit_manifest` made the commit
/// durable, so any failure in between left that transaction's vectors in
/// the shared graph with no manifest entry backing them — and a later
/// commit's watermark would eventually make them visible to
/// [`crate::Snapshot::vector_search`] as dangling hits: rows `scan` could
/// never corroborate. This guard soft-deleted them on the way out (on an
/// early `?` return *or* a panic).
///
/// **That hazard no longer exists, by construction rather than by
/// compensation.** A commit's vectors now only ever exist in a fresh,
/// per-commit `HnswIndex` that is dropped with the failed `write_phase`,
/// plus an orphaned `.seg` file that no manifest references — exactly like
/// an orphaned row data file, and exactly as invisible. There is no shared
/// graph left to leave residue in.
///
/// The type survives this workstream's first step deliberately, per the
/// base design doc §4's "migrate the guarantee, then remove the mechanism"
/// sub-sequencing: the failed-commit tests land against a state where the
/// old mechanism is still present and provably doing nothing, which proves
/// the guarantee moved rather than trusting that it did. W3.2b (its own
/// plan) then deletes the type, its two call sites, and this comment.
struct GraphResidueGuard {
    /// Whether this commit is still short of its durability point. Read by
    /// `Drop` below; set false by [`Self::disarm`] once `commit_manifest`
    /// has succeeded.
    armed: bool,
}

impl GraphResidueGuard {
    fn new() -> Self {
        Self { armed: true }
    }

    /// Marks this commit as past its durability point.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for GraphResidueGuard {
    fn drop(&mut self) {
        if self.armed {
            // Deliberately empty, not an omission — see this type's doc
            // comment. A commit that never reached its durability point has
            // nothing to compensate: its vectors were never published
            // anywhere a reader can reach.
        }
    }
}
```

If clippy objects to the empty block (it should not — there is no lint for a commented-out-by-intent block, and `needless_return` does not fire because there is no `return`), add `#[allow(clippy::needless_late_init)]`-style scoped allow only with a justification comment; do **not** delete the `armed` field, which would make `disarm` dead code and change the shape W3.2b's diff removes.

- [ ] **Step 10: Rewrite `write_phase` (lines 1117-1190)**

Replace its return type, body tail, and doc comment:

```rust
    /// Spec §3 step 3's durable write, run *before* `commit_lock` is
    /// acquired. Claims this transaction's row-ids, writes its data files,
    /// builds and fsyncs this commit's index segment, and fsyncs the data
    /// directory — none of which needs conflict information to proceed, and
    /// none of which can collide with a concurrent transaction's own
    /// writes, because every path it touches is unique to this attempt.
    ///
    /// The filename prefix comes from `write_attempt_counter`, **not**
    /// `base_version + 1`: two truly concurrent transactions can share the
    /// same stale `base_version`, which would make them compute the same
    /// "next version" and collide on the same filename before either
    /// reaches `commit_lock`. `write_attempt_counter` is unique per attempt
    /// regardless of version, which is what makes doing any of this outside
    /// the lock safe at all.
    ///
    /// Building the segment out here is the whole point of the S1 W3.2
    /// migration: the real HNSW construction cost leaves the critical
    /// section entirely, and an interrupted or unfsynced segment write is
    /// just an orphaned file nothing points to — exactly like today's
    /// orphaned row data files.
    ///
    /// Returns the new `DataFileEntry`s, this commit's published segment
    /// (`None` for a commit that carries no vectors — see
    /// [`Self::build_and_write_segment`]), and the row-id claim to hold
    /// until the commit reaches its durability point (`None` for a
    /// delete-only transaction, which inserts no rows, claims no row-ids,
    /// and has nothing to hide from concurrent readers).
    ///
    /// # Errors
    ///
    /// Same conditions as [`Transaction::commit`]'s own doc comment:
    /// dictionary-encoding failure, a non-finite vector component, a
    /// vector-dimension disagreement, an I/O failure writing or fsyncing a
    /// file, or [`TxnError::ManifestOverflow`] if the row-id range would run
    /// past `u64::MAX`.
    fn write_phase(
        &self,
        data_dir: &Path,
        ts: i64,
    ) -> Result<(Vec<DataFileEntry>, Option<PublishedSegment>, Option<RowIdClaim>)> {
```

Keep the body unchanged from the `if self.pending.is_empty()` guard down to the `write_pending_batches` call, changing only the empty-pending early return to `return Ok((Vec::new(), None, None));`, then replace the tail (the `sync_dir` call and `Ok(...)`) with:

```rust
        // Pre-validate before building anything: `insert_owned`'s only
        // fallible path is dimension validation, so a ragged commit must be
        // rejected before a half-built segment can be produced. Sourced
        // from the current snapshot's segment set, not a live graph handle.
        let established_dimension = self.current.load().index.established_dimension();
        validate_vector_dimensions(&inserts, established_dimension)?;

        let segment = Self::build_and_write_segment(data_dir, attempt_id, inserts)?;

        // Fsyncing each file's *content* (already done inside `write_batch`
        // and `write_bytes`) is not sufficient — the new directory entries
        // themselves must also be fsynced, or a real power-loss crash can
        // leave a file's bytes durable while the file itself is absent.
        // Must happen before the manifest commit.
        strata_storage::sync_dir(data_dir)?;
        Ok((new_data_files, segment, Some(claim)))
```

(The `let inserts = Self::write_pending_batches(...)` line keeps its name from Task 5; rename the binding from `deltas` if Task 5 left it as such.)

- [ ] **Step 11: Add `PublishedSegment` and `build_and_write_segment`**

Add the struct immediately before `impl Transaction` (i.e. after `GraphResidueGuard`'s `Drop` impl):

```rust
/// A commit's index segment, in the two forms `commit` needs: the manifest
/// entry that makes it durable, and an already-validated reader over the
/// same bytes that were just fsynced, so the new snapshot's `SegmentSet`
/// needs no read-back.
struct PublishedSegment {
    entry: SegmentEntry,
    reader: Arc<strata_index::SegmentReader>,
}
```

and this method inside `impl Transaction`, immediately after `write_phase`:

```rust
    /// Builds this commit's index segment from `inserts`, writes and fsyncs
    /// it as `{attempt_id:020}.seg`, and returns everything `commit` needs
    /// to publish it — or `None` if this commit carries no vectors.
    ///
    /// **A vector-less commit writes no segment and pushes no
    /// `SegmentEntry`** (post-W3.1 amendment §3c). That is simpler than
    /// writing an empty segment, which would need its own `node_count == 0`
    /// support in `SegmentReader`; `manifest.segments.len() == N` therefore
    /// holds after N *vector-carrying* commits, not after N commits.
    ///
    /// The working index is keyed by **segment-local ordinals `0..N`**, not
    /// by global row-ids (amendment §3b). Two reasons, both concrete:
    /// `NodeTable` demand-allocates a fixed-size chunk per 65536-row-id
    /// span regardless of how few ids land in it, so a 10-row commit at
    /// row-id 5,000,000 would allocate a whole chunk for ten slots; and
    /// keying `0..N` makes the segment's `row_ids` section a direct
    /// positional dump that is ascending by construction, with no remap
    /// pass.
    ///
    /// # Errors
    ///
    /// [`TxnError::Index`] if the working index rejects an insert or the
    /// serializer rejects the built graph, [`TxnError::Io`] if the `.seg`
    /// file can't be written or fsynced, or [`TxnError::TryFromInt`] if a
    /// count doesn't fit its manifest field.
    fn build_and_write_segment(
        data_dir: &Path,
        attempt_id: u64,
        inserts: Vec<VectorInsert>,
    ) -> Result<Option<PublishedSegment>> {
        if inserts.is_empty() {
            return Ok(None);
        }
        let node_count = inserts.len();
        let index = new_hnsw_index(node_count)?;
        let mut row_ids = Vec::with_capacity(node_count);
        for (local, insert) in inserts.into_iter().enumerate() {
            row_ids.push(insert.row_id);
            index.insert_owned(u64::try_from(local)?, insert.vector)?;
        }
        let bytes = index.to_segment_bytes(&row_ids)?;

        let name = format!("{attempt_id:020}.seg");
        let path = data_dir.join(&name);
        write_bytes(&path, &bytes)?;

        // Built from the same buffer that was just fsynced — no read-back
        // on the commit path (base design doc §4).
        let reader = strata_index::SegmentReader::from_bytes(&bytes)?;

        // Debug-only structural cross-check that what landed on disk parses
        // back to the same segment. Excluded under `loom`: loom re-runs the
        // model closure once per interleaving, and an extra whole-file read
        // per commit would multiply an already-expensive model's I/O.
        #[cfg(all(debug_assertions, not(loom)))]
        {
            match std::fs::read(&path)
                .map_err(TxnError::from)
                .and_then(|on_disk| {
                    strata_index::SegmentReader::from_bytes(&on_disk).map_err(TxnError::from)
                }) {
                Ok(reread) => {
                    debug_assert_eq!(
                        reread.node_count(),
                        reader.node_count(),
                        "the fsynced segment must parse back to the same node count"
                    );
                    debug_assert_eq!(
                        reread.row_id_range(),
                        reader.row_id_range(),
                        "the fsynced segment must parse back to the same row-id range"
                    );
                    debug_assert_eq!(
                        reread.byte_len(),
                        reader.byte_len(),
                        "the fsynced segment must be exactly as long as the buffer written"
                    );
                }
                Err(e) => debug_assert!(false, "the just-fsynced segment failed to re-read: {e}"),
            }
        }

        // Non-empty (checked above) and strictly ascending (enforced by the
        // serializer, which would have errored otherwise).
        let (Some(&row_id_min), Some(&row_id_max)) = (row_ids.first(), row_ids.last()) else {
            unreachable!("row_ids is non-empty: `inserts` was checked non-empty above")
        };

        let entry = SegmentEntry {
            name,
            format_version: strata_index::SEGMENT_FORMAT_VERSION,
            vector_count: u64::try_from(node_count)?,
            dimension: u32::try_from(index.established_dimension())?,
            row_id_min,
            row_id_max,
            byte_len: u64::try_from(bytes.len())?,
            // W3 ships this empty; W4 populates it. An absent or empty zone
            // map must always mean "must scan", never "may prune".
            zone_map: std::collections::HashMap::new(),
        };
        Ok(Some(PublishedSegment {
            entry,
            reader: Arc::new(reader),
        }))
    }
```

- [ ] **Step 12: Stop writing delta logs in `write_pending_batches`**

In `write_pending_batches` (lines 1222-1287), delete the `delta_file_name`/`deltas`/`write_delta_log` block added in Task 5 Step 3, and change the `DataFileEntry` push to:

```rust
            data_files.push(DataFileEntry {
                name: file_name,
                stats,
            });
            all_inserts.extend(inserts);
```

Update its doc comment's opening line from "Writes every pending batch's data file and delta-log file" to "Writes every pending batch's data file".

- [ ] **Step 13: Rewrite `commit`'s body between the write phase and the snapshot swap**

In `commit` (lines 873-1089):

Replace lines 877-878 (as left by Task 5) with:

```rust
        let (new_data_files, new_segment, mut claim) = self.write_phase(&data_dir, ts)?;
```

(the dimension validation moved into `write_phase` in Step 10 — delete the call and the `established_dimension` binding from `commit`).

Replace the apply-loop block (lines 941-973, from `let mut tombstones = ...` through the `pause_after_graph_apply` checkpoint) with:

```rust
        // Tombstones layer on top of the *latest* snapshot's set (not this
        // transaction's stale begin()-time view), so a clean commit
        // composes with everything that landed in between.
        let mut tombstones = latest_snapshot.tombstones.as_ref().clone();
        // **No index mutation happens here, or anywhere else inside this
        // lock.** This commit's segment was built and fsynced in
        // `write_phase`, outside the lock; publishing it is the
        // `manifest.segments.push` below, which is part of the same atomic
        // manifest swap that publishes the row data. That is the entire
        // point of the S1 W3.2 migration.
        //
        // Declared here, in the position the old graph-compensation guard
        // occupied, so W3.2b's deletion diff is a straight removal — see
        // [`GraphResidueGuard`], which is now inert.
        let mut residue_guard = GraphResidueGuard::new();
```

Then, after the `manifest.data_files.extend(new_data_files);` line (982), add:

```rust
        // The index side of the same atomic publish. Appended, never
        // substituted: a concurrent, non-conflicting transaction's segment
        // that landed after this one began is already in
        // `latest_snapshot.manifest.segments` and must survive.
        if let Some(published) = &new_segment {
            manifest.segments.push(published.entry.clone());
        }
```

Move the test-only checkpoint to just before the fault injector (i.e. immediately before line 1043's `#[cfg(any(test, loom))]` block):

```rust
        // Test-only rendezvous: this commit's `.seg` file and data files are
        // durable and its conflict check has passed, but `commit_manifest`
        // below has not yet made any of it visible. Absent entirely from
        // production builds.
        #[cfg(test)]
        if let Some(checkpoint) = &self.pause_before_manifest_commit {
            checkpoint.arrive();
        }
```

Finally, replace the `Snapshot` construction (lines 1076-1086) with:

```rust
        let watermark = manifest.next_row_id.saturating_sub(1);
        // The new snapshot's segment set is the previous snapshot's parts
        // plus a reader over the very bytes just fsynced — no read-back.
        let index = match new_segment {
            Some(published) => latest_snapshot.index.with_appended(published.reader),
            None => latest_snapshot.index.clone(),
        };
        debug_assert_eq!(
            index.len(),
            manifest.segments.len(),
            "a snapshot's segment set must be exactly its manifest's segment list"
        );
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            index,
            watermark,
            in_flight: visibility.in_flight,
            tombstones: Arc::new(tombstones),
        };
        self.current.store(Arc::new(snapshot));
```

(`manifest` is moved into `Arc::new(manifest)` after the `debug_assert_eq!` reads its length — keep that order.)

- [ ] **Step 14: Update `commit`'s and `Dataset::create`/`open`'s doc comments**

`commit`'s doc comment (lines 769-872) describes the old in-lock graph application at length. Replace the paragraph beginning "and only if clean are this commit's own new delta entries applied…" through the end of the "Formerly a known limitation, now closed" section with:

```rust
    /// A conflicting transaction leaves the manifest — and therefore every
    /// reader's index view — completely untouched. The new manifest,
    /// segment list and tombstone set are layered on top of the latest
    /// snapshot's state, so a clean commit composes with whatever else
    /// committed after this transaction began. Only after `commit_manifest`
    /// succeeds is the new `Snapshot` swapped in.
    ///
    /// **This commit's index segment is built, serialized and fsynced in
    /// `write_phase`, outside `commit_lock`** — the real HNSW construction
    /// cost is not in the critical section, and the in-lock step performs
    /// no index mutation of any kind. An interrupted or unfsynced segment
    /// write leaves an orphaned `.seg` file that no manifest references,
    /// exactly like an orphaned row data file.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Conflict`] — naming every contested row-id — if
    /// another transaction that committed after this one began wrote any
    /// row in this transaction's write-set, or (conservatively, with this
    /// transaction's entire write-set as the contested rows) if the
    /// bounded in-memory commit log has already evicted history needed to
    /// prove cleanliness.
    ///
    /// Returns [`TxnError::NonFiniteVectorComponent`] if any pending batch's
    /// vector column contains a `NaN`/`Infinity` component — checked, and
    /// rejected, before any file for that batch is written to disk. Returns
    /// [`TxnError::Index`] wrapping a `DimensionMismatch` if this commit's
    /// vectors disagree with each other or with the dimension already
    /// established by committed segments — checked before the segment is
    /// built, so a half-built segment can never be fsynced. Also returns an
    /// error if any pending batch fails to dictionary-encode, if the segment
    /// can't be serialized or written, or if the manifest commit's atomic
    /// rename fails.
    ///
    /// **Every one of these leaves the dataset with nothing this transaction
    /// wrote reachable by any later reader**, and needs no compensating
    /// action to make that true. The manifest stays unadvanced, so this
    /// commit's data files and its `.seg` file are orphaned on disk and
    /// invisible to both [`crate::Snapshot::scan`] (which reads only
    /// manifest-listed data files) and [`crate::Snapshot::vector_search`]
    /// (which searches only manifest-listed segments). There is no shared
    /// mutable graph for a failed commit to leave residue in — see
    /// [`GraphResidueGuard`], which is inert as of W3.2a for exactly this
    /// reason.
    ///
    /// Two in-memory traces do outlive a failed commit, neither reachable
    /// as data: the row-ids it claimed (never recycled — a row-id gap is
    /// explicitly safe, a *searchable* gap is not, spec §8), and the
    /// orphaned `.seg` file itself, which stays on disk until a future
    /// garbage-collection pass. Unlike before W3.2a, a failed first-ever
    /// vector commit no longer poisons the session's established dimension:
    /// that is read from the manifest's segments, which the failed commit
    /// never joined.
```

Update `Dataset::open`'s doc comment (lines 306-311) by appending:

```rust
    /// Index recovery is loading `manifest.segments` — `O(bytes)` of
    /// validation per segment, with zero distance evaluations and zero
    /// graph construction — not replaying an insert log.
```

- [ ] **Step 15: Update the tests this task breaks**

1. **`opening_a_legacy_pre_attempt_id_manifest_does_not_destroy_its_data_files`** (line 2159): delete the `delta_log:` line from the `Manifest` literal (line 2204) and delete the `strata_index::write_delta_log(...)` call and its comment (lines 2212-2219).

2. **`scan_errors_instead_of_traversing_outside_data_dir_on_an_unsafe_manifest_entry`** (line 3384): delete the `delta_log:` line (3398) and the `std::fs::write(dir.join("data").join("d.deltalog"), "")` line and its comment (3407-3410).

3. **`reopening_a_dataset_rebuilds_the_vector_index_from_the_delta_log`** (line 3255): rename to `reopening_a_dataset_loads_the_vector_index_from_the_manifests_segments`, replace the comment at lines 3283-3286 with:

```rust
        // Force a real load from disk, not an in-memory shortcut -- this is
        // the crash-recovery-equivalent test for the index (a fresh Dataset
        // struct, same process, but the segment set is definitely rebuilt
        // from the .seg file the manifest lists, not carried over).
```

and add, after the existing assertions:

```rust
        assert_eq!(
            reopened.snapshot().manifest.segments.len(),
            1,
            "one vector-carrying commit must have produced exactly one segment"
        );
        assert_eq!(
            reopened.snapshot().index.len(),
            1,
            "the loaded segment set must match the manifest's segment list"
        );
```

4. **`replay_index_applies_tombstone_entries_from_the_delta_log`** (line 3807): **delete the whole test.** Its mechanism — hand-appending a `DeltaEntry::Tombstone` to a delta-log file and replaying it — no longer exists, and never had a production producer (`build_delta_entries` emitted only `Insert`). Deletion, not adaptation, is correct: the tombstone path it stood in for is `Snapshot::tombstones` from `manifest.tombstones`, already covered by `delete_tombstones_a_row_and_it_becomes_invisible` (line 3869) and the delete/update suite around it.

5. **`commit_rejects_inconsistent_batch_dimensions_before_touching_the_shared_graph`** (line 4239): rename to `commit_rejects_inconsistent_batch_dimensions_without_publishing_any_segment`, and replace the final "leaked row-id" block (lines 4330-4356) with:

```rust
        // The assertion that actually discriminates fixed-from-buggy. Before
        // W3.2a this checked that row-id 1 (the mismatched transaction's
        // first, individually-valid 3-d batch) had not been inserted into a
        // *shared* graph. There is no shared graph now, so the equivalent
        // property is that the rejected commit published no segment at all:
        // the manifest's segment list is unchanged, and so is the snapshot's
        // in-memory view of it. A half-built segment reaching the manifest
        // would show up here as a segment count of 2.
        assert_eq!(
            snapshot_after.manifest.segments.len(),
            segments_before,
            "a rejected commit must publish no segment: {:?}",
            snapshot_after.manifest.segments
        );
        assert_eq!(
            snapshot_after.index.len(),
            segments_before,
            "the snapshot's segment set must stay in lockstep with the manifest"
        );
        let leaked = snapshot_after
            .index
            .search(&[1.0, 0.0, 0.0], 2, 200, |_| true)
            .unwrap();
        assert!(
            leaked.iter().all(|m| m.row_id != 1),
            "row-id 1 must not be searchable -- a rejected commit must apply zero \
             of its vectors, not just the ones after the first failure: {leaked:?}"
        );
```

and add, next to `let established_before = ...` (line 4258):

```rust
        let segments_before = snapshot_before.manifest.segments.len();
```

6. **`a_failed_commits_vector_is_never_searchable_after_a_later_commit_advances_the_watermark`** (line 4418): the positive control at lines 4508-4512 asserts `snapshot.index.established_dimension() == 3` with the justification "the failed commit's vector must genuinely have reached the graph." That is no longer why it is 3 — the *seed* commit established it, and the failed commit contributes nothing. Replace those five lines with:

```rust
        assert_eq!(
            snapshot.index.established_dimension(),
            3,
            "the seed commit established dimension 3; the failed commit contributes \
             nothing to it, which is itself the W3.2a improvement -- a failed \
             first-ever vector commit no longer poisons the session's dimension"
        );
```

7. **`a_concurrent_reader_never_sees_an_in_flight_commits_vector`** (line 4534): change `slow.pause_after_graph_apply(apply_point);` (line 4583) to `slow.pause_before_manifest_commit(apply_point);`, rename the locals `apply_point`/`applied` to `publish_point`/`ready_to_publish` throughout, and replace the comment at lines 4603-4604 with:

```rust
        // Step 3: release the slow transaction into `commit_lock` and stop
        // it just before `commit_manifest`. Its `.seg` file is durable but
        // no manifest references it, so -- unlike before W3.2a, when its
        // vector was physically in the shared graph at this instant -- there
        // is nothing for a reader to observe even in principle. The
        // assertion below is now a structural guarantee rather than a
        // race the in-flight registry has to win; it is kept because it is
        // the end-to-end proof that the guarantee actually moved.
```

8. **`losing_transactions_graph_insert_never_lands_when_it_conflicts`** (line 4362): rename to `losing_transactions_vectors_never_become_searchable_when_it_conflicts` and update its two references in other tests' comments (lines 4409-4413, 4733-4735, 4780-4782) accordingly. Its assertions are unchanged and still pass.

- [ ] **Step 16: Build and fix any remaining compile errors mechanically**

Run: `cargo build --workspace`
Expected: the only remaining errors are `DataFileEntry` literals still carrying `delta_log`, or `Snapshot`/`Transaction` field accesses this task removed. Fix each by the pattern above. `crates/index`'s `delta_log` module still exists and still compiles — Task 7 removes it.

- [ ] **Step 17: Run the full test suite**

Run: `cargo test --workspace`
Expected: everything passes. In particular:
- `insert_then_commit_then_scan_round_trips`, the concurrency suite, and the snapshot-isolation suite are unchanged and must be green.
- `tests/sim`'s `fast_tier_random_seeds_survive_random_crash_points` must be green — this is the first run where a commit produces 6 checkpoints instead of 5, and where invariant 4 (every visible row findable in the index) is served by multi-segment fan-out rather than one monolithic graph.

- [ ] **Step 18: Run the loom models (they must still pass unchanged)**

Run: `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`
Then run the produced `target/debug/deps/strata_txn-*` binary, filtered to `dataset::loom_tests`.
Expected: all five existing models pass. `a_failed_commits_graph_residue_is_never_searchable_under_concurrent_commits` now proves the property structurally rather than by compensation — its assertions are unchanged and must not be relaxed.

If it exceeds its stack or time budget, that is the risk base design §5 flagged (a real HNSW build now runs inside a model thread). The documented fallback is in that section; **measure before applying it**, and report the measurement rather than silently switching.

- [ ] **Step 19: Run the full gate**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps`
Expected: all clean.

- [ ] **Step 20: Commit**

```bash
git add crates/txn/src/dataset.rs crates/txn/src/error.rs crates/storage/src/manifest.rs
git commit -m "feat(txn): build, fsync and publish one index segment per commit

write_phase now builds a fresh HnswIndex over just this commit's vectors,
keyed by segment-local ordinals 0..N (not global row-ids -- see the W3.2
amendment section 3b for the NodeTable chunk-allocation and remap-pass
reasons), serializes it, and fsyncs it through strata_storage::write_bytes
-- all outside commit_lock. Inside the lock the only index work is
manifest.segments.push: no index mutation of any kind.

A commit carrying no vectors writes no .seg file and pushes no
SegmentEntry (amendment section 3c), so manifest.segments.len() == N
holds after N vector-carrying commits, not after N commits.

Dataset::open loads manifest.segments -- O(bytes) validation per segment,
zero distance evaluations, zero graph construction -- replacing
replay_index. DataFileEntry.delta_log is removed; no compatibility shim,
per design doc section 0.3.

GraphResidueGuard stays and becomes inert: a failed commit's vectors only
ever existed in a per-commit index that is dropped, plus an orphaned .seg
file no manifest references. W3.2b deletes the type once the
failed-commit tests are green against this state, per the base design
doc's migrate-then-remove sub-sequencing."
```

---

### Task 7: Delete the delta log

**Files:**
- Delete: `crates/index/src/delta_log.rs`
- Modify: `crates/index/src/lib.rs` (crate doc line 1; `pub mod delta_log;` line 6; `pub use delta_log::{...}` line 41)
- Modify: `crates/index/src/hnsw.rs` (`IndexError::Serde` lines 45-46; `insert_owned`'s doc comment lines 160-169; `insert`'s doc comment line 154)
- Modify: `crates/index/Cargo.toml` (`serde`, `serde_json`, `tempfile`)
- Modify: `bench/benches/concurrent_commit_bench.rs:67-72` (stale doc comment)

**Interfaces:**
- Removes: `strata_index::DeltaEntry`, `strata_index::read_delta_log`, `strata_index::write_delta_log`, `strata_index::delta_log` (the module), `IndexError::Serde`.
- Produces: nothing new.

**Blast radius, verified against the current tree (post-W3.1 amendment §6 enumerated five locations; three of them were already handled in Task 6):**

| Location | Status after Task 6 |
|---|---|
| `crates/txn/src/dataset.rs` | done — no delta-log reference remains |
| `crates/storage/src/manifest.rs` (`DataFileEntry.delta_log`) | done |
| `crates/index/src/lib.rs` | **this task** |
| `crates/index/src/delta_log.rs` | **this task** |
| `bench/benches/concurrent_commit_bench.rs` | **this task** — and note the amendment overstated the risk: the only reference is the text `DeltaEntry::Insert` inside backticks in a `///` doc comment on a private helper (line 70). That is not an intra-doc link and benches are not built by `cargo doc`, so it would **not** have failed to compile. It is still stale text and is fixed here. |

- [ ] **Step 1: Delete the module file**

```bash
git rm crates/index/src/delta_log.rs
```

- [ ] **Step 2: Update `crates/index/src/lib.rs`**

Change the crate doc (lines 1-3) from:

```rust
//! HNSW vector index, append-only delta log. See
//! `.claude/rules/vector-index.md` and
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6.
```

to:

```rust
//! HNSW vector index: a lock-free in-memory graph plus the immutable,
//! self-contained on-disk segment format that graph is sealed into, one
//! segment per committing transaction. See `.claude/rules/vector-index.md`,
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §4/§6, and
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §1 for the format.
```

Delete `pub mod delta_log;` (line 6) and delete the `pub use delta_log::{DeltaEntry, read_delta_log, write_delta_log};` line (41).

- [ ] **Step 3: Remove `IndexError::Serde` in `crates/index/src/hnsw.rs`**

Delete lines 45-46:

```rust
    #[error("delta log entry serialization error: {0}")]
    Serde(#[from] serde_json::Error),
```

`IndexError::Io` (lines 43-44) has no remaining producer either — this crate does no file I/O — but leave it: it is a `pub` enum variant with no dead-code lint, and removing it is a public-API change with no benefit. Add a note above it:

```rust
    // No producer in this crate today (segment (de)serialization is
    // entirely in-memory; file I/O belongs to `crates/txn`). Retained so a
    // future consumer that does own I/O has a variant to use, and because
    // removing it would churn `TxnError`'s `#[from]` conversion for nothing.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
```

- [ ] **Step 4: Fix the two doc comments in `hnsw.rs` that reference the delta log**

Line 154-155, inside `insert`'s `# Errors` section, change:

```rust
    /// Checked upfront (inside `Graph::insert`'s own
    /// `check_or_establish_dimension` call) so a corrupted delta-log entry
    /// with a wrong-length vector can never reach the distance function.
```

to:

```rust
    /// Checked upfront (inside `Graph::insert`'s own
    /// `check_or_establish_dimension` call) so a wrong-length vector can
    /// never reach the distance function.
```

Lines 160-169, inside `insert_owned`'s doc comment, change "`crates/txn`'s commit-apply loop and recovery replay both already own a freshly-deserialized/freshly-built `Vec<f32>`… (or out of the delta log)" to:

```rust
    /// Same as [`Self::insert`], but takes ownership of `vector` and moves
    /// it straight into the graph instead of cloning a borrowed slice.
    /// `crates/txn`'s per-commit segment builder already owns a
    /// freshly-built `Vec<f32>` at its call site — routing through
    /// `insert`'s `&[f32]` there would force a wasted clone of the full
    /// 512-dim embedding on every insert, on top of the one copy already
    /// paid getting the vector out of Arrow in the first place.
    /// `Graph::insert` moves `vector` into `Node::new` from there, not a
    /// further copy — so this takes the vector from two copies down to one,
    /// not three to two.
```

- [ ] **Step 5: Remove the now-unused dependencies from `crates/index/Cargo.toml`**

Delete `serde.workspace = true` and `serde_json.workspace = true` from `[dependencies]`, and `tempfile = { workspace = true }` from `[dev-dependencies]` — `delta_log.rs` was the only consumer of all three (`serde` had no remaining `derive` use anywhere else in the crate; `tempfile` was used only by `delta_log.rs`'s own tests). The block becomes:

```toml
[dependencies]
anndists = { version = "0.1", features = ["simdeez_f"] }
arrow.workspace = true
# Checked typed-slice casts for the `.seg` segment codec (u8 <-> u64/u32/f32).
# Already in this workspace's dependency graph via `arrow`, so this adds no
# new crate -- only a direct edge. Chosen over hand-rolled `from_le_bytes`
# loops because it makes the alignment precondition a checked API call
# rather than a comment.
bytemuck = "1"
# CRC32C (Castagnoli) for the segment header/body checksums the format
# requires (segment-format design doc section 1). Hardware-accelerated where
# available with a portable software fallback; no transitive runtime
# dependencies. `crc32fast` is NOT a substitute -- it implements CRC-32
# (IEEE), a different polynomial than the format specifies.
crc32c = "0.6"
thiserror.workspace = true

[dev-dependencies]
loom = "0.7"
```

- [ ] **Step 6: Verify the dependency removal actually compiles before going further**

Run: `cargo build -p strata-index && cargo test -p strata-index`
Expected: clean. If `serde`/`serde_json`/`tempfile` turn out to have another consumer, `cargo build` names it — put the dependency back with a comment saying what still needs it, rather than working around the error.

- [ ] **Step 7: Fix the stale doc comment in `bench/benches/concurrent_commit_bench.rs` (lines 67-72)**

```rust
/// Schema for the vector-workload benchmarks: an `id` column plus a
/// `"vector"` `FixedSizeList<Float32, VECTOR_DIM>` column — the presence of
/// a `"vector"` column is what makes each commit actually extract vectors
/// (inside `Transaction::commit`'s write phase) and therefore build,
/// serialize and fsync a real `.seg` segment. The plain-`Int64`
/// benchmarks above never exercise this path: a commit with no vectors
/// writes no segment at all.
```

Also update line 71's neighbouring reference at line 88 and line 161 if they still say `HnswIndex::insert` in a way that implies an in-lock apply — line 161's "so `HnswIndex::insert` never runs inside `commit_lock` during them" is now trivially true of *every* benchmark; change it to "so no segment is built during them, which is what isolates the conflict-check cost from the index-build cost."

- [ ] **Step 8: Confirm no reference survives anywhere**

Run: `git grep -n -i "delta_log\|deltalog\|DeltaEntry\|replay_index" -- ':!docs/' ':!.claude/docs/'`
Expected: **no output**. Any hit outside `docs/` (historical design records, which stay as-is) is a leftover. `.claude/CLAUDE.md` and `.claude/rules/vector-index.md` are handled in Task 11 — if the grep hits those two files, that is expected at this point; nothing else may hit.

- [ ] **Step 9: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps && cargo deny check bans sources advisories`
Expected: all clean. `cargo deny` may now report **fewer** advisory findings than before if `serde_json`'s removal from this crate changed the graph — it will not, since `crates/storage` still uses it, but check the output rather than assuming.

- [ ] **Step 10: Commit**

```bash
git add -A crates/index bench/benches/concurrent_commit_bench.rs
git commit -m "refactor(index): delete the delta log

The delta log's only purpose was being replayed to reconstruct a graph
that was not itself persisted. A segment IS the durable built graph, so
there is nothing left for it to do (design doc section 0.2). No
intermediate WAL replaces it: a segment is atomic by construction --
referenced by a manifest or not, never partially applied.

Removes delta_log.rs, its crate-root exports, IndexError::Serde, and the
serde/serde_json/tempfile dependencies it was the sole consumer of.
crates/index now depends on anndists, arrow, bytemuck, crc32c and
thiserror only.

The three pieces of load-bearing logic that lived alongside DeltaEntry --
Arrow vector extraction, the non-finite guard, and dimension
pre-validation -- were relocated, not deleted, two commits ago."
```

---

### Task 8: Delete `IndexPart::Live`

**Files:**
- Modify: `crates/index/src/segment_set.rs` (module doc's last paragraph; `IndexPart::Live`; `from_live`; `live_arc`; the three `Live`-arm matches in `with_appended`/`fan_out`/`established_dimension`; the three `Live`-based tests at lines 199-276 and the `build_index` helper at 180-197)
- Modify: `crates/txn/src/snapshot.rs` (the `test_snapshot_with_in_flight` helper, lines 343-365)

**Interfaces:**
- Removes: `IndexPart::Live`, `SegmentSet::from_live`, `SegmentSet::live_arc`.
- Produces: nothing new. `IndexPart` becomes a single-variant enum.

**Why this is a separate task, and why it is not the end of the enum:** post-W3.1 amendment §1's forcing-function argument is about *deletion producing compile errors at every remaining call site*. Doing it as its own commit is what makes that observable: `cargo build --workspace` before this task's edits to `crates/txn` should name every surviving caller, and after Task 6 there should be exactly one (`snapshot.rs`'s test helper). `IndexPart` stays an enum afterwards — S2 compaction and Phase B branching are the variants it exists for — and a single-variant enum trips no lint.

- [ ] **Step 1: Confirm `crates/txn` has exactly one remaining caller**

Run: `git grep -n "from_live\|live_arc\|IndexPart::Live" -- crates/ bench/ tests/`
Expected: hits in `crates/index/src/segment_set.rs` only, plus `crates/txn/src/snapshot.rs`'s test helper at line 352. If any *production* `crates/txn` line appears, Task 6 is incomplete — fix that before continuing.

- [ ] **Step 2: Replace `crates/txn/src/snapshot.rs`'s test helper (lines 343-365)**

```rust
    fn test_snapshot_with_in_flight(
        watermark: u64,
        tombstoned: &[u64],
        in_flight: &[RowIdRange],
    ) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            version: 1,
            manifest: Arc::new(Manifest::empty()),
            // These tests exercise `is_visible` only — the watermark,
            // in-flight and tombstone arithmetic — and never search, so an
            // empty segment set is exactly right and avoids building an
            // index nothing queries.
            index: strata_index::SegmentSet::empty(),
            watermark,
            in_flight: in_flight.into(),
            tombstones: Arc::new(tombstoned.iter().copied().collect()),
        }
    }
```

and delete the now-unused import at line 335:

```rust
    use strata_index::{EfConstruction, MaxConnections, MaxElements, MaxLayers};
```

- [ ] **Step 3: Delete the `Live` variant and its two accessors in `crates/index/src/segment_set.rs`**

- Delete the `Live(Arc<HnswIndex>)` variant and its doc comment from `IndexPart`, leaving only `Sealed`.
- Delete `SegmentSet::from_live` entirely.
- Delete `SegmentSet::live_arc` entirely.
- In `with_appended`, replace the loop body's `match` with the single remaining arm:

```rust
        for part in self.parts.iter() {
            match part {
                IndexPart::Sealed(sealed) => parts.push(IndexPart::Sealed(Arc::clone(sealed))),
            }
        }
```

- In `fan_out`, delete the `IndexPart::Live(index) => { … }` arm entirely.
- In `established_dimension`, replace the `match` with:

```rust
            .map(|part| match part {
                IndexPart::Sealed(reader) => reader.dimension(),
            })
```

- Replace the module doc's last paragraph (the `IndexPart::Live` transience note) with:

```rust
//! [`IndexPart`] is an enum with one variant today rather than a bare
//! `Arc<SegmentReader>` alias: S2 compaction and Phase B branching are the
//! variants it exists for, and every method below already matches
//! exhaustively, so adding one is a compile error at each site rather than
//! a runtime surprise. That forcing property is exactly what
//! `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §1
//! required when `Live` was removed.
```

- Update the `use` line to drop `HnswIndex` if nothing else in the file names it — the test module's `build_sealed` helper still does, so keep `HnswIndex` imported inside the test module (`use super::*` already provides it via the file-level import; if the file-level import becomes unused, move it into the test module as `use crate::hnsw::HnswIndex;`).

- [ ] **Step 4: Delete the three `Live`-only tests and the `build_index` helper**

Delete `build_index` (lines 180-197 and its doc comment at 166-179), `search_over_one_live_part_matches_hnsw_index_search_directly`, `search_filtered_over_one_live_part_matches_hnsw_index_search_filtered_directly`, and `established_dimension_matches_the_underlying_index`.

**Do not simply delete the equivalence property they carried.** Replace them with the sealed equivalent, which asserts the same thing against the shape that now exists:

```rust
    #[test]
    fn search_over_one_sealed_part_matches_searching_its_source_graph_directly() {
        // The successor to W3.1's Live-part equivalence tests: one part,
        // and SegmentSet::search must agree exactly with running the same
        // generic traversal against the graph the segment was sealed from.
        // Verified at k=40/ef_search=5 against a deliberately sparse
        // fixture, where a k/ef_search argument swap would change the
        // result set (see `build_index`'s deleted doc comment in git
        // history for why an over-connected fixture cannot catch that).
        let n = 500;
        let index = HnswIndex::new(
            MaxConnections(2),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(5),
        )
        .unwrap();
        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        ((f * PHI).fract() * 1000.0) as f32,
                        ((f * SQRT2).fract() * 1000.0) as f32,
                        ((f * SQRT3).fract() * 1000.0) as f32,
                    ],
                )
                .unwrap();
        }
        let row_ids: Vec<u64> = (0..n as u64).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();
        let set = SegmentSet::from_segments(vec![Arc::new(
            crate::SegmentReader::from_bytes(&bytes).unwrap(),
        )]);
        let query = [500.0_f32, 500.0, 500.0];

        let direct = crate::graph::k_nn_search_generic(
            &index.graph,
            &crate::distance::L2,
            &query,
            40,
            5,
            |_| true,
        )
        .unwrap();
        let via_set = set.search(&query, 40, 5, |_| true).unwrap();

        assert_eq!(via_set.len(), direct.len(), "{via_set:?} vs {direct:?}");
        for (a, b) in via_set.iter().zip(&direct) {
            // Row-ids are 0..n here, so they equal the local ordinals --
            // which is the one case where the ordinal-vs-row-id mapping bug
            // is invisible. `search_returns_global_row_ids_not_segment_local_ordinals`
            // is the test that catches that; this one is about traversal
            // equivalence.
            assert_eq!(a.row_id, b.0, "row-id order must match exactly");
            assert!(
                (a.squared_distance - b.1 * b.1).abs() < f32::EPSILON,
                "distances must match exactly: {} vs {}",
                a.squared_distance,
                b.1 * b.1
            );
        }
    }
```

- [ ] **Step 5: Build and confirm the deletion produced no surprises**

Run: `cargo build --workspace`
Expected: clean. If any call site is named, it is one Task 6 missed — fix it there in spirit (remove the reliance on a live graph), never by reintroducing `from_live`.

- [ ] **Step 6: Run the full gate**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps`
Expected: all clean.

- [ ] **Step 7: Commit**

```bash
git add crates/index/src/segment_set.rs crates/txn/src/snapshot.rs
git commit -m "refactor(index): delete IndexPart::Live, from_live and live_arc

Completes the delete-Live shape the W3.2 amendment section 1 requires:
Sealed was never added 'alongside' Live as a lasting staging step, and
the arity-refutable sole_live pattern -- which would have panicked at
runtime with no compile-time signal the moment a second part existed --
went away in the same commit that introduced Sealed.

IndexPart stays an enum: S2 compaction and Phase B branching are the
variants it exists for, and every method matches exhaustively so adding
one is a compile error at each site.

W3.1's three Live-part equivalence tests are replaced by their sealed
equivalent, not dropped."
```

---

### Task 9: The failed-commit tests — three flavors, six assertions each

**Files:**
- Modify: `crates/txn/src/dataset.rs` — `Transaction` struct (add one injector field), `Dataset::begin` (initialize it), `commit` (the panic-injection site), and the test module (three new tests plus one shared helper)

**Interfaces:**
- Consumes: everything Tasks 6-8 produced.
- Produces: `pub(crate) fn Transaction::inject_panic_before_manifest_commit(&mut self)`, and a test helper `fn orphaned_segment_files(ds: &Dataset) -> Vec<String>`.

**What these prove (base design §5's six-point list, verbatim):** for each of three failure shapes — an injected I/O failure at `commit_manifest`, a typed `Conflict`, and a **panic between segment fsync and manifest swap** — assert:
- (a) `Err` is returned (or the panic is caught),
- (b) `dataset.snapshot().version` is unchanged,
- (c) `vector_search` never returns the attempted row-id,
- (d) no manifest entry names the orphaned segment file,
- (e) reopening the dataset reproduces (a)-(d),
- (f) the orphan `.seg` file **does** exist on disk.

(f) is not decoration: without it the suite would pass just as happily against an implementation that never wrote a segment at all, and would therefore be validating the wrong thing. It is the assertion that pins "orphaned, not never-written."

- [ ] **Step 1: Add the panic injector to `Transaction`**

In the `Transaction` struct, next to `inject_manifest_commit_failure`, add:

```rust
    /// Test-only fault injection: makes [`Transaction::commit`] panic at
    /// the instant between this commit's segment being fsynced and its
    /// manifest being swapped in — the one window where a crash could, in
    /// principle, leave a durable segment referenced by nothing. Distinct
    /// from [`Self::inject_manifest_commit_failure`], which returns a typed
    /// error at the same point: a panic unwinds through every guard and
    /// `Drop` on the way out, which is the failure shape that would expose
    /// a compensating action that only ran on the `?` path.
    ///
    /// Scoped to one `Transaction` rather than a thread-local for the same
    /// reason as the sibling injector: `loom` multiplexes its model
    /// threads.
    #[cfg(any(test, loom))]
    inject_panic_before_manifest_commit: bool,
```

and the setter next to `inject_manifest_commit_failure`'s (around line 735):

```rust
    /// Test-only: see [`Self::inject_panic_before_manifest_commit`].
    #[cfg(any(test, loom))]
    pub(crate) fn inject_panic_before_manifest_commit(&mut self) {
        self.inject_panic_before_manifest_commit = true;
    }
```

In `Dataset::begin`'s `Transaction { … }` literal, add next to the existing injector:

```rust
            #[cfg(any(test, loom))]
            inject_panic_before_manifest_commit: false,
```

- [ ] **Step 2: Add the panic-injection site in `commit`**

Immediately after the existing `inject_manifest_commit_failure` block and immediately before `commit_manifest(&self.dir, &manifest)?;`:

```rust
        // Test-only fault injection modelling a panic at the instant this
        // commit's segment is durable but its manifest is not. Absent
        // entirely from production builds.
        #[cfg(any(test, loom))]
        assert!(
            !self.inject_panic_before_manifest_commit,
            "injected panic between segment fsync and manifest swap (test fault injection)"
        );
```

(An `assert!` rather than a bare `panic!` so the message is greppable from `catch_unwind`'s payload and so clippy's `panic` lint — not enabled here, but cheap insurance — has nothing to say.)

- [ ] **Step 3: Add the shared test helper**

In the test module, next to `temp_dir`:

```rust
    /// Every `.seg` file physically present in `ds`'s data directory that
    /// the current manifest does **not** list. A failed commit must leave
    /// exactly one — orphaned, not absent.
    fn orphaned_segment_files(ds: &Dataset) -> Vec<String> {
        let referenced: std::collections::HashSet<String> = ds
            .snapshot()
            .manifest
            .segments
            .iter()
            .map(|s| s.name.clone())
            .collect();
        let mut orphans: Vec<String> = std::fs::read_dir(ds.data_dir())
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(".seg") && !referenced.contains(name))
            .collect();
        orphans.sort();
        orphans
    }

    /// Asserts base design §5's six-point list for a dataset whose most
    /// recent commit failed, then reopens and asserts (a)-(d) again.
    /// `attempted_query` is the failed commit's own vector, whose
    /// distinctive coordinates make a hit for it unambiguous.
    fn assert_failed_commit_left_no_trace(
        dir: &std::path::Path,
        ds: &Dataset,
        version_before: u64,
        segments_before: usize,
        attempted_query: &[f32],
    ) {
        // (b) the visible version never advanced.
        let snapshot = ds.snapshot();
        assert_eq!(
            snapshot.version, version_before,
            "a failed commit must not advance the visible version"
        );

        // (d) no manifest entry names the orphaned segment, and the
        // snapshot's in-memory segment set agrees with the manifest.
        assert_eq!(
            snapshot.manifest.segments.len(),
            segments_before,
            "a failed commit must publish no SegmentEntry: {:?}",
            snapshot.manifest.segments
        );
        assert_eq!(
            snapshot.index.len(),
            segments_before,
            "the snapshot's segment set must stay in lockstep with the manifest"
        );

        // (f) the orphan really was written -- without this the whole test
        // would pass against an implementation that never wrote a segment.
        let orphans = orphaned_segment_files(ds);
        assert_eq!(
            orphans.len(),
            1,
            "exactly one orphaned .seg file must exist on disk: {orphans:?}"
        );

        // (c) the attempted row is not searchable. Asserted by distance
        // rather than by row-id so it cannot pass vacuously on an empty
        // result set caused by a broken search.
        let hits = snapshot.vector_search(attempted_query, 1, None).unwrap();
        assert!(
            hits.is_empty() || hits[0].squared_distance > 1000.0,
            "the failed commit's vector must never be searchable: {hits:?}"
        );

        // (e) reopening reproduces all of the above. This is the assertion
        // that catches an in-memory-only cleanup that never made it to disk.
        let reopened = Dataset::open(dir).unwrap();
        let reopened_snapshot = reopened.snapshot();
        assert_eq!(reopened_snapshot.version, version_before);
        assert_eq!(reopened_snapshot.manifest.segments.len(), segments_before);
        assert_eq!(reopened_snapshot.index.len(), segments_before);
        assert_eq!(
            orphaned_segment_files(&reopened).len(),
            1,
            "the orphan must survive a reopen -- it is garbage, not corruption"
        );
        let reopened_hits = reopened_snapshot
            .vector_search(attempted_query, 1, None)
            .unwrap();
        assert!(
            reopened_hits.is_empty() || reopened_hits[0].squared_distance > 1000.0,
            "the failed commit's vector must still not be searchable after a \
             reopen: {reopened_hits:?}"
        );
    }
```

- [ ] **Step 4: Write the three tests**

```rust
    #[test]
    fn a_commit_failing_at_the_manifest_step_leaves_an_orphaned_segment_and_nothing_else() {
        // Flavor 1 of base design §5's failed-commit test: a recoverable
        // I/O failure (e.g. ENOSPC) at `commit_manifest`, injected at
        // exactly that step -- after this commit's .seg file is already
        // fsynced.
        let dir = temp_dir("failed-commit-io-orphan");
        let ds = Dataset::create(&dir).unwrap();

        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ));
        seed.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();
        assert_eq!(segments_before, 1, "the seed commit produced one segment");

        let mut failing = ds.begin();
        failing.insert(vector_batch(
            vec![2i64],
            cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
        ));
        failing.inject_manifest_commit_failure();
        // (a)
        let result = failing.commit();
        assert!(
            result.is_err(),
            "the injected manifest-commit failure must make this commit fail, \
             else this test proves nothing: {result:?}"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        // A subsequent commit must still succeed -- a failed commit leaves
        // no state that blocks the next one.
        let mut next = ds.begin();
        next.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ));
        next.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.segments.len(), segments_before + 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_conflicting_commit_leaves_an_orphaned_segment_and_nothing_else() {
        // Flavor 2: a typed Conflict. The losing transaction wrote and
        // fsynced its segment in `write_phase`, before the lock, so the
        // orphan exists -- and must never be referenced.
        let dir = temp_dir("failed-commit-conflict-orphan");
        let ds = Dataset::create(&dir).unwrap();

        let mut setup = ds.begin();
        setup.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ));
        setup.commit().unwrap();

        // Both begin from the same snapshot, then commit sequentially, so
        // which one loses is fixed by test order rather than by an explored
        // interleaving -- there is no concurrency to model here, only a
        // specific sequence to regression-test. Both use `update`, since a
        // delete-only transaction inserts nothing and would build no
        // segment at all.
        let mut winner = ds.begin();
        winner.update(0, vector_batch(vec![2i64], cluster_vectors(1, [500.0, 500.0, 500.0], 0.0)));
        let mut loser = ds.begin();
        loser.update(0, vector_batch(vec![3i64], cluster_vectors(1, [900.0, 900.0, 900.0], 0.0)));

        winner.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();

        // (a)
        let result = loser.commit();
        assert!(
            matches!(result, Err(TxnError::Conflict { .. })),
            "expected the second update to conflict on row 0, got {result:?}"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_panic_between_segment_fsync_and_manifest_swap_leaves_an_orphaned_segment_and_nothing_else()
    {
        // Flavor 3: a panic, not an early `?` return. This is the shape
        // that would expose a compensating action wired only into the error
        // path -- and, historically, the shape `GraphResidueGuard`'s `Drop`
        // existed to survive. After W3.2a nothing needs to survive it,
        // because nothing shared was ever touched; this test is what proves
        // that rather than assuming it.
        let dir = temp_dir("failed-commit-panic-orphan");
        let ds = Dataset::create(&dir).unwrap();

        let mut seed = ds.begin();
        seed.insert(vector_batch(
            vec![1i64],
            cluster_vectors(1, [0.0, 0.0, 0.0], 0.0),
        ));
        seed.commit().unwrap();
        let version_before = ds.snapshot().version;
        let segments_before = ds.snapshot().manifest.segments.len();

        // The default panic hook would print a backtrace for a panic this
        // test deliberately causes, which is noise in an otherwise clean
        // run. Suppressed only around the `catch_unwind`, then restored.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        // (a) -- `Transaction` is not `UnwindSafe` (it holds `Arc`s and an
        // `ArcSwap` handle), and it does not need to be: the panic happens
        // before any shared state is mutated, and the only thing that could
        // observe a torn value -- the manifest -- is never written on this
        // path. `AssertUnwindSafe` records that reasoning explicitly.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut panicking = ds.begin();
            panicking.insert(vector_batch(
                vec![2i64],
                cluster_vectors(1, [900.0, 900.0, 900.0], 0.0),
            ));
            panicking.inject_panic_before_manifest_commit();
            panicking.commit()
        }));
        std::panic::set_hook(previous_hook);
        assert!(
            outcome.is_err(),
            "the injected panic must actually unwind out of commit, else this \
             test proves nothing"
        );

        assert_failed_commit_left_no_trace(
            &dir,
            &ds,
            version_before,
            segments_before,
            &[900.0, 900.0, 900.0],
        );

        // A subsequent commit must still succeed -- the panic must not have
        // left `commit_lock` poisoned in a way that blocks progress. (It
        // does poison it; `commit` recovers a poisoned lock via
        // `PoisonError::into_inner`, and this is what proves that path is
        // still exercised and still correct after the guard went inert.)
        let mut next = ds.begin();
        next.insert(vector_batch(
            vec![3i64],
            cluster_vectors(1, [500.0, 500.0, 500.0], 0.0),
        ));
        next.commit().unwrap();
        assert_eq!(ds.snapshot().manifest.segments.len(), segments_before + 1);
        let after = ds
            .snapshot()
            .vector_search(&[500.0, 500.0, 500.0], 1, None)
            .unwrap();
        assert!(
            after.first().is_some_and(|m| m.squared_distance < 0.001),
            "the post-panic commit's vector must be searchable: {after:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 5: Add the vector-less-commit test the amendment §3c requires**

Base design §4's stated proof criterion (`manifest.segments.len() == N` after N insert-commits) only holds for vector-carrying commits, and amendment §3c requires this be an **explicit test**, not an accidental consequence:

```rust
    #[test]
    fn a_commit_with_no_vector_column_writes_no_segment_at_all() {
        // Post-W3.1 amendment §3c: deciding not to write an empty segment
        // is simpler than writing one and then needing node_count == 0
        // support in SegmentReader. Asserted explicitly so it can't
        // regress into "we write an empty segment and nobody noticed".
        let dir = temp_dir("vector-less-commit-writes-no-segment");
        let ds = Dataset::create(&dir).unwrap();

        // A plain Int64 batch: no "vector" column at all.
        let schema = test_schema();
        let mut plain = ds.begin();
        plain.insert(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
                .unwrap(),
        );
        plain.commit().unwrap();

        assert_eq!(ds.snapshot().version, 1, "the commit itself must succeed");
        assert!(
            ds.snapshot().manifest.segments.is_empty(),
            "a vector-less commit must push no SegmentEntry: {:?}",
            ds.snapshot().manifest.segments
        );
        assert!(ds.snapshot().index.is_empty());
        assert!(
            orphaned_segment_files(&ds).is_empty(),
            "no .seg file may be written at all -- not even an unreferenced one"
        );
        assert_eq!(ds.snapshot().manifest.data_files.len(), 1, "the rows are still committed");
        assert_eq!(ds.snapshot().scan(&schema).unwrap().num_rows(), 3);

        // And a delete-only commit likewise.
        let mut deleting = ds.begin();
        deleting.delete(0);
        deleting.commit().unwrap();
        assert!(ds.snapshot().manifest.segments.is_empty());
        assert!(orphaned_segment_files(&ds).is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn n_vector_carrying_commits_produce_exactly_n_segments_and_all_stay_searchable() {
        // The design doc §4 proof criterion, with amendment §3c's
        // correction applied -- and, because search fans out over every
        // part (this plan's Scope decision), also the end-to-end proof that
        // a row committed in segment 0 is still findable after segment 4
        // lands. Without fan-out this second half fails.
        let dir = temp_dir("n-commits-n-segments");
        let ds = Dataset::create(&dir).unwrap();

        let centers = [
            [0.0_f32, 0.0, 0.0],
            [1000.0, 0.0, 0.0],
            [0.0, 1000.0, 0.0],
            [0.0, 0.0, 1000.0],
            [1000.0, 1000.0, 1000.0],
        ];
        for (i, center) in centers.iter().enumerate() {
            let mut txn = ds.begin();
            txn.insert(vector_batch(
                vec![i64::try_from(i).unwrap()],
                cluster_vectors(1, *center, 0.0),
            ));
            txn.commit().unwrap();
            assert_eq!(
                ds.snapshot().manifest.segments.len(),
                i + 1,
                "one segment per vector-carrying commit"
            );
            assert_eq!(ds.snapshot().index.len(), i + 1);
        }

        for (i, center) in centers.iter().enumerate() {
            let hits = ds.snapshot().vector_search(center, 1, None).unwrap();
            assert_eq!(
                hits.first().map(|m| m.row_id),
                Some(u64::try_from(i).unwrap()),
                "the row committed in segment {i} must still be the nearest match \
                 for its own vector after every later segment landed: {hits:?}"
            );
        }

        // And after a reopen, which loads all five from the manifest.
        let reopened = Dataset::open(&dir).unwrap();
        assert_eq!(reopened.snapshot().index.len(), 5);
        for (i, center) in centers.iter().enumerate() {
            let hits = reopened.snapshot().vector_search(center, 1, None).unwrap();
            assert_eq!(
                hits.first().map(|m| m.row_id),
                Some(u64::try_from(i).unwrap()),
                "segment {i}'s row must survive a reopen: {hits:?}"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p strata-txn failed_commit -- --nocapture` then `cargo test -p strata-txn segment`
Expected: the three failed-commit tests, `a_commit_with_no_vector_column_writes_no_segment_at_all`, and `n_vector_carrying_commits_produce_exactly_n_segments_and_all_stay_searchable` all pass.

If `n_vector_carrying_commits_...`'s second loop fails for early `i`, fan-out is broken (Task 4). If it fails only after the reopen, `load_segments` is dropping or reordering parts (Task 6).

- [ ] **Step 7: Run the full gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "test(txn): prove a failed commit leaves neither the row nor the index behind

Three flavors from the base design doc section 5 -- injected I/O failure
at commit_manifest, a typed Conflict, and a panic between segment fsync
and manifest swap -- each asserting the full six-point list: Err (or a
caught panic), version unchanged, the attempted vector never searchable,
no manifest entry naming the orphan, all of it reproduced after a reopen,
and the orphan .seg file DOES exist on disk.

That last assertion is the one that stops the suite from passing against
an implementation that never wrote a segment at all.

Plus the amendment section 3c criterion made explicit: a vector-less (or
delete-only) commit writes no .seg file and pushes no SegmentEntry, and
N vector-carrying commits produce exactly N segments with every one of
them still searchable -- before and after a reopen."
```

---

### Task 10: Loom Models 1 and 2

**Files:**
- Modify: `crates/txn/src/dataset.rs`'s `#[cfg(loom)] mod loom_tests` (lines 4828-5306)

**Interfaces:**
- Consumes: `spawn_committer`/`COMMIT_STACK_SIZE`/`loom_vector_batch`/`loom_plain_batch` (all existing in that module), `Transaction::inject_manifest_commit_failure` (existing), `Snapshot.manifest`/`.index` (both `pub(crate)`, reachable from this same-crate module).
- Produces: two new `#[test]` functions. Model 3 is **explicitly not** in this plan (base design §5: it is the regression gate for the separate `RowIdAllocator.active` deletion PR).

**Hard constraints, from `.claude/rules/concurrency-txn-layer.md` and base design §5:**
- Every thread running a `commit` goes through `spawn_committer` (1 MiB stack). The model's **root thread** gets loom's 32 KiB default and cannot be resized, so it does setup and assertions only.
- loom caps threads at **5 created per execution**, and a terminated thread never frees its slot. Both models below sit at 3 (root + 2). Do not add a third spawned thread.
- Fixtures stay at **1-2 rows, dim 3** — a commit now performs a real (if tiny) HNSW build *and* a segment serialization inside a model thread, and loom's exploration cost is exponential in operation count.
- Run scoped: `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`, never a workspace-wide `RUSTFLAGS`.

- [ ] **Step 1: Add Model 1 to `mod loom_tests`**

```rust
    #[test]
    fn a_failed_commits_segment_is_never_visible_to_a_concurrent_reader() {
        // Base design §5, loom Model 1 -- "failed commit is invisible."
        //
        // Thread A commits with `inject_manifest_commit_failure`: it claims
        // row-ids, builds and fsyncs its segment, then returns Err before
        // the manifest swap. Thread B takes a snapshot and searches
        // concurrently. Under EVERY interleaving, B must observe neither
        // A's row-id nor A's segment file.
        //
        // The interleavings loom explores that matter here are B's
        // `current.load()` landing (i) before A takes `commit_lock`,
        // (ii) between A's segment fsync and A's Err, and (iii) after A's
        // Err. The property is the same in all three -- which is exactly
        // the point: after W3.2a it holds structurally (a snapshot's
        // segment set is its manifest's list, and A's segment never enters
        // a manifest), not because a compensating action won a race.
        //
        // Deliberately minimal per §5's flagged risk: one row, dim 3, and
        // only A carries a vector.
        loom::model(|| {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-failed-segment-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = crate::Dataset::create(&dir).unwrap();
            let version_before = ds.snapshot().version;

            let ds_failing = ds.clone();
            let failing = spawn_committer(move || {
                let mut txn = ds_failing.begin();
                txn.insert(loom_vector_batch(1, [900.0, 900.0, 900.0]));
                txn.inject_manifest_commit_failure();
                txn.commit()
            });

            let ds_reader = ds.clone();
            let reader = spawn_committer(move || {
                let snapshot = ds_reader.snapshot();
                let hits = snapshot
                    .vector_search(&[900.0, 900.0, 900.0], 1, None)
                    .unwrap();
                let segment_names: Vec<String> = snapshot
                    .manifest
                    .segments
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();
                (snapshot.version, snapshot.index.len(), hits, segment_names)
            });

            assert!(
                failing.join().unwrap().is_err(),
                "the injected manifest-commit failure must make this commit fail"
            );
            let (observed_version, observed_parts, hits, segment_names) = reader.join().unwrap();

            assert_eq!(
                observed_version, version_before,
                "no snapshot may exist at a version the failed commit never produced"
            );
            assert!(
                segment_names.is_empty(),
                "a reader must never see a manifest naming the failed commit's \
                 segment file: {segment_names:?}"
            );
            assert_eq!(
                observed_parts, 0,
                "the observed snapshot's segment set must match its manifest's \
                 (empty) segment list"
            );
            assert!(
                hits.is_empty(),
                "the failed commit's vector was the only one ever inserted, and \
                 must never be searchable under any interleaving: {hits:?}"
            );

            // The root thread's own post-join view, which is the quiescent
            // half of the property.
            assert_eq!(
                ds.snapshot().version,
                version_before,
                "a failed commit must not advance the visible version"
            );
            assert!(ds.snapshot().manifest.segments.is_empty());

            std::fs::remove_dir_all(&dir).ok();
        });
    }
```

- [ ] **Step 2: Add Model 2 to `mod loom_tests`**

```rust
    #[test]
    fn a_commits_row_and_its_segment_become_visible_as_one_atomic_step() {
        // Base design §5, loom Model 2 -- "row + segment publish
        // atomically."
        //
        // A commits successfully; B snapshots and then both scans and
        // vector-searches THAT SAME snapshot. B must observe either the
        // complete pre-commit state or the complete post-commit state --
        // never A's row present under the old manifest version, and never
        // the version bumped with A's segment absent.
        //
        // This is close to trivially true once both live in one `Manifest`
        // published by a single atomic swap, but it is the entire
        // justification for deleting the old guard/registry machinery, so
        // §5 requires it be proven rather than assumed.
        //
        // B reads one snapshot and derives every assertion from it: taking
        // two snapshots would let a commit land in between and make the
        // test assert nothing.
        loom::model(|| {
            let dir = tempfile::Builder::new()
                .prefix(&format!(
                    "strata-loom-atomic-publish-{}-{:?}-",
                    std::process::id(),
                    loom::thread::current().id()
                ))
                .tempdir()
                .unwrap()
                .keep();
            let ds = crate::Dataset::create(&dir).unwrap();

            let ds_writer = ds.clone();
            let writer = spawn_committer(move || {
                let mut txn = ds_writer.begin();
                txn.insert(loom_vector_batch(1, [900.0, 900.0, 900.0]));
                txn.commit()
            });

            let ds_reader = ds.clone();
            let reader = spawn_committer(move || {
                let snapshot = ds_reader.snapshot();
                let hits = snapshot
                    .vector_search(&[900.0, 900.0, 900.0], 1, None)
                    .unwrap();
                (
                    snapshot.version,
                    snapshot.manifest.data_files.len(),
                    snapshot.manifest.segments.len(),
                    snapshot.index.len(),
                    hits.len(),
                )
            });

            writer.join().unwrap().unwrap();
            let (version, data_files, segments, parts, hit_count) = reader.join().unwrap();

            // The in-memory segment set and the manifest's segment list are
            // the two halves that must never disagree, in any observed
            // state.
            assert_eq!(
                parts, segments,
                "a snapshot's segment set must always equal its manifest's segment \
                 list -- observed {parts} parts against {segments} entries at \
                 version {version}"
            );

            match version {
                0 => {
                    assert_eq!(data_files, 0, "the pre-commit state has no data file");
                    assert_eq!(segments, 0, "...and no segment");
                    assert_eq!(hit_count, 0, "...and nothing to find");
                }
                1 => {
                    assert_eq!(data_files, 1, "the post-commit state has A's data file");
                    assert_eq!(segments, 1, "...and A's segment");
                    assert_eq!(
                        hit_count, 1,
                        "...and A's row is findable in it -- a version bump with \
                         the segment absent, or present but unsearchable, is the \
                         partial state this model rules out"
                    );
                }
                other => panic!("no interleaving may produce version {other}"),
            }

            std::fs::remove_dir_all(&dir).ok();
        });
    }
```

- [ ] **Step 3: Add the module-level note recording why Model 3 is absent**

Append to `mod loom_tests`'s doc comment (which ends at line 4827):

```rust
/// **Model 3 is deliberately absent.** Base design §5 defines it as the
/// regression gate for deleting `RowIdAllocator.active` / `in_flight` /
/// collapsing `Snapshot::is_visible` to the tombstone check — explicitly
/// "its own PR after W3.3 is green", not folded into this workstream. It
/// belongs in that PR, where it must pass both before and after the
/// deletion.
```

- [ ] **Step 4: Build the loom binary**

Run: `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`
Expected: builds clean. If it fails with `cannot find module or crate 'loom'` pointing at `crates/index`, a workspace-wide `RUSTFLAGS` was used — re-read `.claude/rules/concurrency-txn-layer.md` and use the `cargo rustc` form.

- [ ] **Step 5: Run the loom models, timing them**

Run the produced `target/debug/deps/strata_txn-*` binary filtered to `dataset::loom_tests`, and **record the wall time of each of the three commit-running models**.

Expected: all seven models (five pre-existing plus the two new ones) pass.

**This is the step base design §5 flagged as the plan's real risk:** a commit now performs a real HNSW build *plus* a segment serialization inside a model thread, and loom's exploration cost is exponential in operation count. If either new model blows its time or stack budget:
1. **Measure first** — record the actual runtime and the failure mode (a coroutine stack overflow surfaces as a bare access violation / exit 139 with no backtrace, not a `stack overflow` message).
2. The documented fallback is to extract a `publish_segment(&mut Manifest, SegmentEntry)` helper and loom-model just that plus the atomic swap, with the segment build stubbed out — a weaker model that still covers the interleaving that actually matters (publish ordering, not HNSW correctness, which the non-loom suite already covers).
3. **Report the measurement and the decision**; do not apply the fallback silently, and do not apply it preemptively.

- [ ] **Step 6: Run the normal (non-loom) gate too**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean. The `#[cfg(loom)]` module is not compiled in a normal build, so this only confirms nothing else regressed.

- [ ] **Step 7: Commit**

```bash
git add crates/txn/src/dataset.rs
git commit -m "test(txn): loom models 1 and 2 for the segment publish path

Model 1 -- a failed commit's segment is never visible to a concurrent
reader, under every interleaving of the failing committer and the reader:
no snapshot at an unproduced version, no manifest naming the orphan, no
searchable vector.

Model 2 -- a commit's row and its segment become visible as one atomic
step: a reader observes either the complete pre-commit state or the
complete post-commit state, and a snapshot's in-memory segment set always
equals its manifest's segment list.

Both sit at 3 of loom's hard 5-thread-per-execution cap, with 1-row/dim-3
fixtures per the design doc's exploration-cost warning.

Model 3 stays out: the design doc scopes it as the regression gate for
the separate RowIdAllocator.active deletion PR, where it must pass both
before and after."
```

---

### Task 11: Update the project's own persistent rules

**Files:**
- Modify: `.claude/CLAUDE.md` (the Stack section's `crates/index` line; the Architecture crate list; the Conventions bullet; the Don't bullet)
- Modify: `.claude/rules/vector-index.md` (the first bullet)
- Modify: `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` (append a status note to §4's W3.2a paragraph)

**Interfaces:** none — documentation only.

**Why this is in scope (post-W3.1 amendment §7):** both files currently assert, as a *binding correctness rule*, "index mutations are an append-only delta log, never in-place graph mutation." W3.2a replaces the mechanism (delta log → durable segment) while preserving the guarantee it existed to serve (atomic row+index commit; no write acknowledged before durability). Left unedited, the repository's own persistent memory would describe an invariant the code no longer implements — and `.claude/CLAUDE.md`'s own closing line says "stale instructions are worse than missing ones."

**Do not weaken the guarantee while restating the mechanism.** The new wording must still forbid: acknowledging a write before it is durable, mutating a published index in place, and any index update that bypasses `crates/txn`'s commit path.

- [ ] **Step 1: Update `.claude/CLAUDE.md`'s Conventions bullet**

Replace:

```markdown
- **The vector index shares the transaction boundary with row data.** Index mutations are an append-only delta log, never in-place graph mutation, so they can commit atomically alongside row writes.
```

with:

```markdown
- **The vector index shares the transaction boundary with row data.** A committing transaction builds its own immutable index segment outside the commit lock, fsyncs it, and the manifest swap that publishes its rows is the same one that publishes the segment — so row data and index commit atomically. **No published index is ever mutated in place**: a snapshot's segment set is exactly its manifest's segment list. (This replaces the append-only delta log, which existed to reconstruct a graph that was never persisted; S1 W3.2 made the segment itself the durable built graph. The guarantee is unchanged, only the mechanism.)
```

- [ ] **Step 2: Update `.claude/CLAUDE.md`'s Don't bullet**

Replace:

```markdown
- Don't let index mutations happen outside the transaction layer's delta log, even for "just a quick fix"
```

with:

```markdown
- Don't let an index update happen outside the transaction layer's commit path — no writing a `.seg` file, and no publishing a `SegmentEntry`, from anywhere but `Transaction::commit`'s write phase, even for "just a quick fix"
```

- [ ] **Step 3: Update `.claude/CLAUDE.md`'s two `crates/index` descriptions**

In the Stack section's HNSW bullet, replace the sentence "No HNSW library audited (C++ or Rust) exposed graph internals for a native delta log — Strata's transaction shim maintains that log itself regardless." with:

```markdown
No HNSW library audited (C++ or Rust) exposed graph internals for the segment serialization Strata's own on-disk format needs — that codec (`crates/index/src/segment_{format,writer,reader}.rs`) is Strata's own code regardless of backing implementation.
```

In the Cargo workspace layout list, replace:

```markdown
- `crates/index/` — HNSW vector index, append-only delta log (see `rules/vector-index.md`) (`strata-index`)
```

with:

```markdown
- `crates/index/` — HNSW vector index and the immutable on-disk segment format it seals into (see `rules/vector-index.md`) (`strata-index`)
```

- [ ] **Step 4: Update `.claude/rules/vector-index.md`'s first bullet**

Replace:

```markdown
- **Index mutations are represented as an append-only delta log (which nodes/edges changed), never in-place graph mutation.** This is what lets index changes commit atomically alongside row data instead of being patched in separately after the fact — don't reintroduce in-place mutation for a "quick" performance fix without a design discussion first.
```

with:

```markdown
- **The index is a set of immutable, self-contained segments — one per committing transaction — never a shared graph mutated in place.** A commit builds its segment outside the commit lock, fsyncs it, and publishes it by the same atomic manifest swap that publishes its rows; a snapshot's segment set is exactly its manifest's `segments` list. That is what lets index changes commit atomically alongside row data instead of being patched in separately after the fact. Don't reintroduce a shared mutable graph, or any "apply now, persist later" path, for a "quick" performance fix without a design discussion first. (This replaced an append-only delta log in S1 W3.2 — see `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` §0.2 and §1. The log's only job was reconstructing a graph that was never persisted; a segment *is* the persisted graph.)
- **Segments are immutable and never rewritten.** Deletion is the manifest's versioned tombstone set applied through the traversal filter, not a per-node flag and not a segment rewrite: a row committed in segment 3 and deleted at version 12 stays physically in segment 3 forever, and a snapshot at v11 must still see it.
```

- [ ] **Step 5: Record the staging status in the design doc**

Append to `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`'s §4, at the end of the "Sub-sequencing within W3.2" bullet list:

```markdown
> **Status (2026-07-25):** W3.2a is implemented by
> [`docs/superpowers/plans/2026-07-25-s1-w3-2a-segment-write-path.md`](../plans/2026-07-25-s1-w3-2a-segment-write-path.md).
> That plan takes one decision this document deferred: **basic multi-part
> fan-out search ships in W3.2a, not W3.3** — otherwise the second commit
> after W3.2a lands is a silent recall regression. W3.3 is correspondingly
> re-scoped to zone-map pruning, the monolithic-baseline recall-parity test,
> and the `explain`-shaped segment-consultation assertion. See that plan's
> "Scope decision" section.
```

- [ ] **Step 6: Verify nothing else in the repo's own rules still asserts the old mechanism**

Run: `git grep -n -i "delta log\|delta-log" -- .claude/`
Expected: **no output**. Historical design records under `docs/` keep their original text and are not edited.

- [ ] **Step 7: Run the full gate one last time**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --workspace --no-deps && cargo deny check bans sources advisories`
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add .claude/CLAUDE.md .claude/rules/vector-index.md \
        docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md
git commit -m "docs: index rule is immutable segments, not an append-only delta log

Both .claude/CLAUDE.md and .claude/rules/vector-index.md asserted the
delta log as a binding correctness rule. S1 W3.2 replaced the mechanism
while preserving the guarantee it served (atomic row+index commit, no
write acknowledged before durability), so leaving them would have the
repository's persistent memory describing an invariant the code no longer
implements -- which CLAUDE.md's own closing line calls out as worse than
missing.

The new wording keeps every prohibition the old one carried: no
acknowledging before durability, no in-place mutation of a published
index, no index update outside the commit path.

Also records the W3.2a/W3.3 fan-out re-scoping in the design doc's
staging section, so it isn't a divergence discovered mid-W3.3."
```

---

## Plan-level exit criteria (run after Task 11, before calling W3.2a done)

- `cargo build --workspace` — clean, no warnings.
- `cargo test --workspace` — every test passes.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- `cargo doc --workspace --no-deps` — clean (CI runs it).
- `cargo deny check bans sources advisories` — clean (CI runs it; two new direct dependencies were added).
- Loom, scoped per `.claude/rules/concurrency-txn-layer.md` — all seven models green, **with the two new models' runtimes recorded** (base design §5's flagged risk).
- `tests/sim`'s fast tier green: `cargo test -p strata-sim fast_tier` (or the workspace run above, which includes it).
- **Opus-tier `reviewer` subagent review of the full diff across all 11 tasks** — mandatory per `.claude/CLAUDE.md`, regardless of which model implemented it.
- Confirm by inspection, not just by green tests, that:
  1. **No index mutation occurs inside `commit_lock`.** The only index-related statement in the critical section is `manifest.segments.push(...)`.
  2. **`SegmentSet::search` maps sealed parts' local ordinals through `row_id_at`.** Every row-id base in this plan's own tests is 0-based except one test specifically designed to catch this; re-read `fan_out` directly.
  3. **`SegmentReader::vector` returns `None` rather than panicking** for every out-of-range input (post-W3.1 amendment §4).
  4. **A vector-less commit writes nothing** — no `.seg` file, no `SegmentEntry` (amendment §3c).

## Phase-level gate that is NOT part of this plan

`STRATA_CHAOS_THOROUGH=1 cargo test -p strata-sim thorough_tier` (2000 seeds, real process spawn, real `std::process::abort()` at the new post-segment-fsync checkpoint) is **W3's overall exit gate**, per base design §5 and §9 — the crash-side twin of the loom models above. It must run and be clean before W3 as a whole is called done. It is deliberately **not** a step of this plan: it is expensive, it is a phase-level gate rather than a task-level one, and the controller runs it after this plan's tasks land.

---

## Self-review

**1. Spec coverage.** Walking the three binding documents section by section:

| Requirement | Task |
|---|---|
| Base §1 — on-disk format: 128-byte header, 4 aligned sections, CRC-checked, `O(bytes)` load with zero distance evals / zero graph construction | 2 (writer + format), 3 (reader) |
| Base §1 — `AlignedBytes`, one `// SAFETY:`, `node_layout.rs` precedent | 2 |
| Base §1 — `row_ids` ascending, asserted at load; reverse lookup needs no side table | 2 (writer enforces), 3 (reader validates) |
| Base §2 — `SegmentReader: NodeSource`, bounds-checked slice arithmetic only | 3 |
| Base §2 — segment has no deleted flag; deletion is the manifest tombstone set | 3 (uses the trait's `false` default), 11 (rule text) |
| Base §3 — `SegmentEntry` populated; `zone_map` written empty; `DataFileEntry.delta_log` removed | 6 |
| Base §4 — `write_phase` builds the segment outside the lock, fsyncs, emits a `SegmentEntry`; chaos checkpoint after the segment fsync | 1 (checkpoint site), 6 (build/write/publish) |
| Base §4 — inside the lock: `manifest.segments.push` and **no index mutation** | 6 |
| Base §4 — new snapshot's `SegmentSet` = previous parts + reader over the same buffer, no read-back; debug-only re-read assertion | 6 |
| Base §4 — `Dataset::open` reads `manifest.segments` (folded into W3.2 by the approved deviation) | 6 |
| Base §4 — `replay_index` and `delta_log.rs` deleted | 6, 7 |
| Base §4 — W3.2a keeps `GraphResidueGuard`, inert | 6 |
| Base §5 — failed-commit tests, three flavors, six assertions | 9 |
| Base §5 — loom Models 1 and 2 | 10 |
| Base §5 — Model 3 deferred to its own PR | 10 (recorded, not implemented) |
| Base §8 (invariants) — atomic row+index publish; nothing acknowledged before durable | 6, 9, 10 |
| W3 amendment §2 — `NodeSource::is_deleted` default | already in the tree (W3.1); `SegmentReader` uses the default (3) |
| W3 amendment §3 — relocate, don't delete: extraction, non-finite guard, dimension pre-validation | 5 |
| W3 amendment §4 — crate ownership: `crates/index` pure in-memory, no `strata-storage` edge; `bytemuck` + CRC32C added there and justified | 2, 7 |
| W3.2 amendment §1 — delete `Live`/`from_live`/`sole_live`/`live_arc`; length-independent iteration in the transient commit | 4 (`sole_live` deleted, iteration), 8 (`Live`/`from_live`/`live_arc` deleted) |
| W3.2 amendment §2 — `validate_*` off a plain `usize`; `established_dimension` over sealed parts; `Transaction.graph` and the two `Snapshot`-construction sites deleted | 5, 4, 6 |
| W3.2 amendment §3a — build via `HnswIndex`, not `Graph<L2>` | 6 |
| W3.2 amendment §3b — key by local ordinals `0..N` | 6 |
| W3.2 amendment §3c — no segment for a vector-less commit, with an explicit test | 6, 9 |
| W3.2 amendment §4 — `SegmentReader::vector` fails closed | 3 |
| W3.2 amendment §5 — `strata_storage::write_bytes` with its own checkpoint; chaos comment updated | 1 |
| W3.2 amendment §6 — the 5-file blast radius; `safe_join` doc contract | 6, 7 |
| W3.2 amendment §7 — `.claude/CLAUDE.md` and `.claude/rules/vector-index.md` | 11 |
| W3.2 amendment §8 — live-id bitset hoist already resolved | not re-planned (correct) |
| This plan's Scope decision — basic fan-out in W3.2a | 4 (mechanics), 9 (end-to-end proof) |

No gaps found.

**2. Placeholder scan.** No "TBD", "TODO", "implement later", "handle appropriately", "add validation", "similar to Task N", or "write tests for the above" appears. Every code step carries the actual code. The one place a later decision is permitted — Task 10 Step 5's loom fallback — states the exact fallback, the exact precondition for applying it (a measured budget overrun), and requires reporting rather than silent application; that is the base design's own instruction, not a deferred decision.

**3. Type consistency.** Cross-checked the names each task produces against every later use:
- `SEGMENT_FORMAT_VERSION` (T2) → `SegmentEntry.format_version` (T6). ✓
- `HnswIndex::to_segment_bytes(&self, row_ids: &[u64]) -> Result<Box<[u8]>, IndexError>` (T2) → T3 tests, T4 test helper, T6 `build_and_write_segment`. ✓ (Deliberately takes `row_ids` rather than the amendment's parameterless sketch: the working index is keyed `0..N` and therefore does not know its own global row-ids, and `NodeTable` exposes no iteration API, so node count must come from the caller either way. Recorded in T2's Interfaces block.)
- `SegmentReader::from_bytes`/`node_count`/`dimension`/`row_id_at`/`row_id_range`/`byte_len` (T3) → T4 `fan_out`/`established_dimension`, T6 `load_segments` + the debug re-read. ✓
- `SegmentSet::empty`/`from_segments`/`with_appended`/`len`/`is_empty`/`established_dimension` (T4) → T6 `create`/`open`/`commit`, T8 `snapshot.rs` helper, T9 assertions, T10 models. ✓
- `VectorInsert { row_id, vector }`, `build_vector_inserts`, `validate_vector_dimensions(&[VectorInsert], usize)` (T5) → T6 `write_phase`/`build_and_write_segment`. ✓
- `PublishedSegment { entry, reader }` (T6) → `write_phase`'s return tuple and `commit`'s two uses. ✓
- `write_bytes(path, bytes)` (T1) → T6. ✓
- `TxnError::CorruptSegment` (T6) → `load_segments` only. ✓
- `pause_before_manifest_commit` — renamed consistently in T6 (field, setter, `begin` initializer, `commit` site, and the one test that calls it). ✓
- `inject_panic_before_manifest_commit` — added wholly within T9 (field, setter, `begin` initializer, `commit` site); T6 Step 8 explicitly defers it rather than half-adding it. ✓
- `IndexError::SegmentEmpty`/`SegmentTooLarge`/`SegmentCorrupt` (T2) → T2 writer, T3 reader + tests, T6 error propagation. ✓
- `orphaned_segment_files` / `assert_failed_commit_left_no_trace` (T9) → the three flavors plus the vector-less test. ✓

One naming hazard worth flagging to the implementer rather than silently resolving: `SegmentReader` has both an inherent `dimension()` and a `NodeSource::dimension()`. The inherent method wins at any call site where the trait is not imported, and they return the same value, so this is safe — but do not "simplify" by deleting the inherent one, or `segment_set.rs` (which does not import `NodeSource`) stops compiling.

