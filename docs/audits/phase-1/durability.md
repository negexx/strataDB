# Phase 1 durability audit

**Lane:** Sol durability

**Date:** 2026-08-01

**Baseline:** Current dirty working tree, with `docs/status.md`, `docs/roadmap.md`,
`docs/architecture.md`, and this audit pack's `README.md` treated as active. No Rust, test,
dependency, configuration, or unrelated documentation file was changed by this lane.

**Scope:** Local storage writes, fsync/rename publication, manifest recovery, corruption and torn
write handling, chaos checkpoints, orphan files, `LocalFs`/`Backend`, and platform assumptions.

## Verdict

**Phase 1 durability exit is blocked.** The manifest boundary correctly prevents unreferenced row
files and vector segments from becoming logically visible, and the existing abrupt-process-crash
tests pass. The stronger active claim that every successful commit is durable across a storage
crash/power loss is not established:

1. directory-open and directory-`sync_all` failures are discarded while the commit still returns
   success;
2. newly created `data/` and `_versions/` directory entries are not made durable in their parent;
3. manifests and Arrow row files have no end-to-end integrity check, allowing some validly encoded
   corruption to be accepted and, for corrupted statistics, to suppress reads silently; and
4. the chaos suite models process abort after filesystem calls, not loss of unsynced filesystem
   metadata, and therefore cannot close the preceding gaps.

The active `Partial` status for durability/recovery is correct. At audit time, the acknowledged-write
and "durably publishes" wording in `docs/architecture.md` was treated as an implementation claim;
the active document has since been explicitly narrowed to an intended contract while the implementation
remains incomplete. The underlying fail-open behavior still blocks Phase 1.

## Classification summary

| ID | Finding | Severity | Confidence | Affected phase | Classification / disposition |
|---|---|---:|---:|---|---|
| DUR-01 | Directory fsync is fail-open although successful return is treated as the durability point | Critical | High | Phase 1 | **Blocker:** make directory durability fail closed on every supported local platform/filesystem and fault-test the error path. |
| DUR-02 | Initial dataset directories are not durably linked from their parent | High | High | Phase 1 | **Blocker:** fsync the parent after first creation of `data/` and `_versions/`, with an explicit dataset-root creation contract. |
| DUR-03 | Manifest/row-file corruption can remain validly encoded and escape detection | High | High | Phase 1 | **Blocker:** define the corruption threat model and add integrity plus semantic validation sufficient to reject covered corruption loudly. |
| DUR-04 | Chaos evidence proves process-abort atomicity, not power-loss durability; its loss budget is count-based | High | High | Phase 1 | **Blocker (verification):** add deterministic filesystem-fault coverage and exact acknowledged-ID accounting; qualify existing chaos evidence. |
| DUR-05 | Active architecture wording overstates the implemented durability guarantee | High | High | Phase 1 | **Documentation correction:** keep durability `Partial`; do not claim acknowledged power-loss durability until DUR-01/02 are closed and verified. |
| DUR-06 | Failed commits and crashes leave unreachable temp/data/segment files with no cleanup | Medium | High | Phase 3, with Phase 1 obligation | **Later work:** cleanup/vacuum is Phase 3; Phase 1 must retain the explicit growth/cleanup obligation and avoid calling orphans corruption. |
| DUR-07 | `LocalFs` has undeclared filesystem/key-platform constraints; `delete` is not durable | Medium | High | Phases 3, 4, and 6 | **Later work plus boundary documentation:** resolve before cleanup, cross-process CAS, or object-backend parity is supported. |
| DUR-08 | Independent openers can overwrite/race manifest versions | Critical if used; out of scope today | High | Phase 4 | **Intentional non-goal:** current shared-handle scope is documented correctly; do not use this to weaken Phase 1's single-handle durability requirements. |

## Detailed findings

### DUR-01 — successful commits can bypass directory durability

**Evidence**

- `crates/storage/src/datafile.rs:62-83` states directory fsync is needed for a file name to survive
  a crash, but `sync_dir` ignores both `File::open(dir)` failure and `handle.sync_all()` failure and
  always returns `Ok(())`. Its chaos checkpoint runs even when no directory sync occurred
  (`crates/storage/src/datafile.rs:78-83`).
- `LocalFs::put` fsyncs the temp file, renames it, calls that fail-open `sync_dir`, and returns
  success (`crates/storage/src/backend/local.rs:185-216`). `put_if_absent` does the same after its
  hard link (`crates/storage/src/backend/local.rs:224-263`).
- This contradicts the public backend contract that `put`/`put_if_absent` return only when durable
  and return an error when durable completion fails (`crates/storage/src/backend/mod.rs:21-24`,
  `crates/storage/src/backend/mod.rs:54-72`).
- `commit_manifest` delegates publication to `LocalFs::put` and returns success
  (`crates/storage/src/manifest.rs:193-216`). `Transaction::commit` then labels that return the
  durability point and returns `Ok(())` after installing the snapshot
  (`crates/txn/src/dataset.rs:1113-1156`).
- The architecture document records the intended durability boundary and now qualifies it as Partial
  and blocked by this audit (`docs/architecture.md`, `## Commit and snapshot lifecycle`).

**Impact**

On any platform or filesystem where opening/syncing a directory is unsupported or fails, a commit
can be acknowledged after the manifest rename without evidence that the renamed directory entry is
stable. The same applies to the preceding `data/` directory sync: after power loss, a surviving
manifest can name a row file or segment whose directory entry was not durable. This violates the
acknowledged-write invariant inside the supported shared-handle scope; it is not a cross-process
issue.

**Required disposition**

Implement a platform-specific, fail-closed directory-sync primitive for supported targets. A
location where the required guarantee cannot be provided must fail dataset creation/commit or be
explicitly unsupported; it must not return success. Add injected failures for directory open and
directory sync and assert that no write is acknowledged. Keep the chaos checkpoint after a
*successful* durability operation, not after a swallowed failure.

### DUR-02 — first creation omits parent-directory durability

**Evidence**

- `Dataset::create` creates `data/` and then publishes manifest 0
  (`crates/txn/src/dataset.rs:358-368`). It never syncs the dataset root after creating `data/`.
- `LocalFs::put` creates `_versions/` with `create_dir_all`
  (`crates/storage/src/backend/local.rs:185-190`) and later syncs `_versions/` itself
  (`crates/storage/src/backend/local.rs:200-215`), but never syncs the dataset root that gained the
  `_versions/` entry.
- `write_phase` likewise creates `data/` if needed and syncs only `data/` itself
  (`crates/txn/src/dataset.rs:1196-1212`, `crates/txn/src/dataset.rs:1269-1275`).
- The real-process chaos suite explicitly allows `NotFound` when the initial manifest rename did not
  complete (`tests/sim/tests/chaos.rs:386-405`), but does not exercise a rename that completed in
  memory and whose newly created parent entry is then lost on power failure.

**Impact**

Fsyncing a newly created child directory does not by itself establish durability of that child's
name in its parent. An acknowledged `Dataset::create`, or the first write that creates a missing
subdirectory, therefore lacks a complete persistence chain. The exact root-creation contract is
also undefined: if Strata creates the dataset root, durability may require syncing its parent too.

**Required disposition**

Define who creates the dataset root and which ancestor must already exist durably. After creating
each required child directory, sync the parent directory before relying on the child. Cover fresh
create separately from steady-state commits; existing reopen tests exercise namespace visibility,
not persistence after volatile metadata loss.

### DUR-03 — corruption detection is strong for segments but incomplete for manifests and rows

**Evidence: behavior that fails loudly**

- Manifest structs deny unknown fields (`crates/storage/src/manifest.rs:34-45`,
  `crates/storage/src/manifest.rs:63-74`, `crates/storage/src/manifest.rs:100-112`), and invalid
  JSON is mapped to `StorageError::CorruptManifest`
  (`crates/storage/src/manifest.rs:229-256`). The invalid-JSON test is
  `crates/storage/src/manifest.rs:397-416`.
- Segment loading checks manifest-recorded length and metadata
  (`crates/txn/src/dataset.rs:1696-1720`, `crates/txn/src/dataset.rs:1725-1807`). `SegmentReader`
  validates magic, format,
  endianness, header CRC, body CRC, section geometry, row-ID ordering, and graph bounds
  (`crates/index/src/segment_reader.rs:100-109`,
  `crates/index/src/segment_reader.rs:113-236`). A truncated committed segment is
  rejected by the real reopen path (`crates/txn/src/dataset.rs:4772-4810`).
- Arrow parser panics for the known malformed-schema class are converted to
  `CorruptDataFile`, with explicitly documented residual abort/stack-overflow gaps
  (`crates/storage/src/datafile.rs:86-121`, `crates/storage/src/datafile.rs:122-143`).

**Evidence: uncovered silent-corruption paths**

- Manifests are plain compact JSON without a checksum or digest
  (`crates/storage/src/manifest.rs:202-215`, `crates/storage/src/manifest.rs:542-571`).
  `read_current` selects the highest numeric
  filename, deserializes it, and does not compare the filename's selected version to
  `Manifest.version` (`crates/storage/src/manifest.rs:229-256`; note the selected version is
  discarded at line 250).
- Valid JSON mutations to tombstones, file names, counters, or statistics can therefore deserialize
  successfully. In particular, manifest statistics are trusted before any row file is opened:
  `read_surviving_files` filters entries through `should_scan_file`
  (`crates/txn/src/snapshot.rs:156-196`), and `should_scan_file` returns `false` when trusted min/max
  appear to prove absence (`crates/query/src/predicate.rs:135-180`). A validly encoded corrupted
  min/max can silently skip real rows.
- Segment zone maps are also stored in the unchecked manifest rather than covered by the segment
  body CRC, and are used to skip segments (`crates/txn/src/snapshot.rs:30-48`,
  `crates/txn/src/snapshot.rs:256-274`).
- Arrow row files are fsynced and parsed, but no Strata checksum is written or verified
  (`crates/storage/src/datafile.rs:21-36`, `crates/storage/src/datafile.rs:86-143`).
  Parser-detectable structural damage errors;
  a payload bit flip that remains valid Arrow can return altered row data. Repository search finds
  CRC handling for index segments, not manifests or row files.

**Impact**

The repository rule to reject corrupt/unknown state loudly is not met for validly encoded manifest
or row-file corruption. Corrupted pruning metadata is especially serious because it can produce an
apparently successful query with missing rows instead of a typed error. It can also desynchronize
row and vector results even though segment bytes themselves are protected.

**Required disposition**

Before Phase 1 exit, define whether the supported corruption model covers only crash-torn writes or
also latent/media corruption. For every covered class, add an integrity envelope and semantic
validation. At minimum, validate filename version versus payload version and internal manifest
invariants; protect pruning metadata and row bytes with checksums bound to the manifest or file
format; and add tests that mutate bytes while preserving valid JSON/Arrow structure. Any format
change needs an explicit compatibility/migration decision, not an accidental rewrite.

### DUR-04 — current chaos tests do not prove power-loss durability

**Evidence**

- Chaos injection aborts the process at explicit checkpoints with `process::abort`
  (`crates/storage/src/chaos.rs:23-32`). This correctly skips unwinding and destructors.
- A vector commit's six documented checkpoints are file-content syncs, a best-effort data-directory
  sync, manifest temp sync, rename, and a best-effort versions-directory sync
  (`tests/sim/tests/chaos.rs:59-115`). Because `sync_dir` checkpoints even when sync failed,
  reaching the checkpoint is not proof of directory durability.
- The harness reopens the same live filesystem after process termination
  (`tests/sim/tests/chaos.rs:191-218`, `tests/sim/tests/chaos.rs:382-405`,
  `tests/sim/tests/chaos.rs:562-615`). This checks process-crash visibility
  and parser recovery while the OS/filesystem cache survives; it does not emulate power loss that
  drops unsynced metadata.
- The 2,000-seed tier is opt-in via `STRATA_CHAOS_THOROUGH=1`
  (`tests/sim/tests/chaos.rs:617-630`). Current CI is Ubuntu-only and runs
  `cargo test --workspace` without that environment variable (`.github/workflows/ci.yml:11-27`),
  so the thorough tier returns early in ordinary CI.
- Lost/phantom allowance is a count derived from in-flight verbs, not exact target IDs
  (`tests/sim/tests/chaos.rs:117-135`, `tests/sim/tests/chaos.rs:329-362`,
  `tests/sim/tests/chaos.rs:435-517`). The source itself records that
  overlapping delete/update targets can create excess slack that hides a regression
  (`tests/sim/tests/chaos.rs:497-508`). Acknowledged tombstone resurrection is checked exactly
  (`tests/sim/tests/chaos.rs:519-531`), but acknowledged insert loss can still consume unrelated
  count slack.
- The dedicated checkpoint integration test returns early unless its opt-in environment is present
  (`crates/storage/tests/chaos_checkpoint_actually_aborts.rs:11-24`); its helper invokes a real
  manifest commit (`crates/storage/tests/bin/chaos_checkpoint_helper.rs:8-13`).

**What the evidence does prove**

It proves that configured checkpoints really abort when built and invoked with chaos injection;
that leftover temp files are excluded; and that the sampled abrupt-process-crash executions reopen
without the checked row/index/tombstone violations. It does not prove the stronger filesystem
durability contract.

**Required disposition**

Add deterministic storage fault injection around file sync, directory open/sync, rename, and parent
creation. Assert commit acknowledgement and recovered state at each outcome. Replace count-only
ambiguity with exact target/produced row-ID sets in start records. Run supported-platform filesystem
tests in CI, and treat the thorough tier as explicit evidence with preserved command/result rather
than inferring it from the skipped test target.

### DUR-05 — active claims need correction pending implementation

| Active claim | Audit result | Disposition |
|---|---|---|
| The active status ledger marks durability/recovery `Partial` and says cross-platform/lifecycle guarantees need audit | Accurate | Keep `Partial`; add the fail-open directory-sync and corruption boundaries. |
| The architecture narrative describes durability as an intended boundary but now qualifies it as Partial and blocked | The implementation does not establish power-loss durability | Keep the qualification until DUR-01/02 and verification close. |
| `docs/architecture.md:13` says segment parsing validates format and CRCs | Supported | Keep. See `crates/index/src/segment_reader.rs:100-109`, `crates/index/src/segment_reader.rs:113-236`. |
| `docs/architecture.md:16`, `docs/architecture.md:41`, `docs/architecture.md:45` say orphan cleanup/compaction are absent | Supported | Keep and cross-reference the Phase 3 lifecycle obligation. |
| The Phase 1 roadmap exit criteria require bounded, directly verified guarantees (`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`) | Not yet met | DUR-01 through DUR-04 block exit. |
| The active status ledger, architecture, and roadmap exclude independent openers | Supported boundary | Keep; cross-process conditional publication remains Phase 4. |

### DUR-06 — orphan files are safe for visibility, not managed for lifecycle

**Evidence**

- Data and segment files are written before conflict checking and manifest publication
  (`crates/txn/src/dataset.rs:915-963`, `crates/txn/src/dataset.rs:1191-1275`). Failure can
  therefore leave final-name
  `.arrow` and `.seg` files; manifest temp files can remain after pre-rename aborts
  (`crates/storage/src/manifest.rs:4-17`).
- Reads use only `Manifest.data_files` (`crates/txn/src/snapshot.rs:156-196`), while reopen loads
  only `Manifest.segments` (`crates/txn/src/dataset.rs:1696-1807`). Unreferenced files are therefore
  unreachable by the current query paths.
- Tests explicitly require an orphaned segment to remain invisible and survive reopen
  (`crates/txn/src/dataset.rs:2213-2238`, `crates/txn/src/dataset.rs:2240-2309`) and cover
  manifest-failure, conflict, and panic cases (`crates/txn/src/dataset.rs:6708`,
  `crates/txn/src/dataset.rs:6769`, `crates/txn/src/dataset.rs:6827`). Manifest tests cover both old and
  current temp-name shapes (`crates/storage/src/manifest.rs:324-395`).
- No cleanup exists; active docs place vacuum/orphan cleanup in Phase 3
  (`docs/status.md`, `Manifest/segment growth and cleanup obligations`; `docs/roadmap.md`,
  `## Phase 3 — Operational lifecycle`).

**Disposition**

This is not a Phase 1 atomic-visibility blocker: unreachable residue is the intended failure shape.
Cleanup is Phase 3. Phase 1 must nevertheless document all residue classes, measure/limit the
operational obligation, and ensure future cleanup derives reachability from retained manifests and
live snapshots. Orphans must not be mislabeled as corrupt committed state.

### DUR-07 — backend and platform assumptions

| Assumption / behavior | Exact evidence | Disposition |
|---|---|---|
| Rename temp and target are colocated, so normal publication avoids cross-filesystem rename | `crates/storage/src/backend/local.rs:67-81`, `crates/storage/src/backend/local.rs:185-199` | Sound within one ordinary local filesystem; document supported filesystems and rename semantics. |
| `put_if_absent` requires hard links and explicitly fails on exFAT/FAT32/some SMB mounts | `crates/storage/src/backend/local.rs:219-263` | Not used by current manifest commit, but must be resolved or rejected explicitly before Phase 4/6 conditional publication. |
| Backend keys are specified as `/`-delimited | `crates/storage/src/backend/mod.rs:26-39` | On Windows, `validate_key` splits only on `/` and accepts normal `Path` components (`crates/storage/src/backend/local.rs:28-64`), while listing canonicalizes components back to `/` (`crates/storage/src/backend/local.rs:132-140`). Reject `\` or otherwise preserve key identity before backend parity. |
| `Backend::delete` has no durable directory-entry step | `crates/storage/src/backend/local.rs:273-276` | Harmless while no cleanup path uses it; Phase 3 vacuum must not claim crash-safe deletion until this is fixed and tested. |
| Current manifest publication uses overwrite-capable `put`, not `put_if_absent` | `crates/storage/src/manifest.rs:202-215`; `crates/storage/src/backend/local.rs:185-216` | Correct only inside the documented single shared-handle serialization boundary. Durable CAS belongs to Phase 4 and remote conditional writes to Phase 6. |
| CI tests one OS only | `.github/workflows/ci.yml:11-27` | Add a supported-platform matrix before making cross-platform durability claims. |

### DUR-08 — cross-process publication is intentionally out of scope

Two independent `Dataset` handles do not share the commit lock, row allocator, attempt counter, or
snapshot. The current manifest primitive overwrites a version key on filesystems where rename
permits it, and Windows/filesystem collision behavior can differ. This would be a critical durability
and lost-update bug if independent openers were supported, but active documentation explicitly
excludes them (`docs/status.md`, `## Concurrency scope`; `docs/architecture.md`, `## Commit and snapshot lifecycle`;
`docs/roadmap.md`, `## Phase 4 — Cross-process coordination`).
Classify this as Phase 4, not as a reason to fail the current shared-handle scope.

## Verified existing behavior

| Behavior | Evidence | Audit assessment |
|---|---|---|
| Row and segment contents are fsynced before manifest publication | `crates/storage/src/datafile.rs:21-59`; `crates/txn/src/dataset.rs:1267-1275` | Implemented for file contents. Directory-entry durability remains blocked by DUR-01/02. |
| Manifest publication writes a colocated temp, fsyncs it, renames, then attempts directory sync | `crates/storage/src/backend/local.rs:185-215`; `crates/storage/src/manifest.rs:202-216` | Atomic namespace transition on assumed same-filesystem rename; power-loss durability is not fail closed. |
| Recovery ignores temp files and selects the highest numeric manifest filename | `crates/storage/src/manifest.rs:4-17`, `crates/storage/src/manifest.rs:219-256`, tests at `crates/storage/src/manifest.rs:324-395` and `crates/storage/src/manifest.rs:472-508` | Implemented. A corrupt highest committed manifest errors; there is no silent fallback to an older version. |
| Invalid JSON/unknown manifest fields fail | `crates/storage/src/manifest.rs:34-45`, `crates/storage/src/manifest.rs:63-74`, `crates/storage/src/manifest.rs:100-112`, `crates/storage/src/manifest.rs:253-255`, `crates/storage/src/manifest.rs:397-416` | Implemented for those syntax/schema classes only. |
| Committed segment corruption/truncation fails loudly | `crates/index/src/segment_reader.rs:100-109`, `crates/index/src/segment_reader.rs:113-236`; `crates/txn/src/dataset.rs:1696-1807`, `crates/txn/src/dataset.rs:4772-4810` | Stronger than row/manifest integrity; typed error paths are exercised. |
| Unlisted data/segments are invisible before and after reopen | `crates/txn/src/snapshot.rs:156-196`; `crates/txn/src/dataset.rs:1696-1807`, `crates/txn/src/dataset.rs:2213-2309` | Implemented within current read APIs. Cleanup remains absent. |

## Fresh verification run for this lane

Commands were run against the same dirty working tree after inspection:

- `cargo test -p strata-storage` — exit 0; 59 unit tests, the integration target, and the doctest
  passed. The checkpoint integration test's source-level opt-in means this command alone did not
  prove an abort.
- `$env:STRATA_CHAOS_TEST_HELPER_BUILT='1'; cargo test -p strata-storage --features chaos-injection
  --test chaos_checkpoint_actually_aborts -- --nocapture` — exit 0; 1/1, real configured abort.
- `cargo test -p strata-txn create_then_open_recovers_same_version` — exit 0; 1/1.
- `cargo test -p strata-txn reopening_a_dataset_with_a_truncated_segment_file_returns_corrupt_segment`
  — exit 0; 1/1.
- `cargo test -p strata-txn orphaned_segment` — exit 0; 3/3.
- `cargo test -p strata-sim fast_tier_random_seeds_survive_random_crash_points -- --nocapture` —
  exit 0 on the fresh rerun; 1/1 test covering the source-defined 30-seed fast tier
  (`tests/sim/tests/chaos.rs:562-615`).

These passing tests support the bounded behaviors listed above. None injects directory-sync
failure, loses volatile filesystem metadata, validates parent-directory persistence, mutates
manifest/row bytes while preserving valid encoding, or runs the 2,000-seed thorough tier. They do
not discharge DUR-01 through DUR-04.

## Controller handoff

Phase 1 should remain `Partial`. Treat DUR-01, DUR-02, DUR-03, and DUR-04 as closure blockers; apply
DUR-05 as the immediate claim correction; retain DUR-06 and DUR-07 as explicit lifecycle/platform
obligations; and preserve DUR-08 as a deliberate Phase 4 boundary. A fresh Sol review is required
after any durability implementation because changes to file ordering, manifest publication, or
chaos checkpoints can invalidate both atomicity and checkpoint accounting.
