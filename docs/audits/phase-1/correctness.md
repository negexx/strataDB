# Phase 1 correctness audit

**Date:** 2026-08-01

**Lane:** Sol correctness

**Scope:** Current working tree; single-process concurrency through one shared `Dataset` handle.
**Method:** Read-only review of `docs/status.md`, `docs/roadmap.md`, `docs/architecture.md`, the current transaction/storage/index/query implementation and tests, plus the crash-harness assertions relevant to recovery. The working tree was already heavily dirty across these areas; this report evaluates that exact state and changes no source, tests, dependencies, or configuration.

## Verdict

**BLOCKED — Phase 1 does not yet meet its correctness and durability exit criteria.**

The central happy path is well structured: files and immutable index segments are prepared before publication, commit validation is serialized, a fresh latest snapshot is used to construct the next manifest, and the in-memory snapshot advances only after manifest publication. Existing tests give strong evidence for valid-row conflicts, immutable snapshots, row/index publication, segment recovery, and conservative conflict-log expiry.

However, one public-API counterexample can make a successfully committed insert immediately invisible, and a related interleaving bypasses the claimed write-write conflict check. Row-ID claims can also be reused after restart following a failed commit, contrary to the repository's non-reuse invariant. Directory-fsync failures are swallowed even though `commit()` then acknowledges durability. Two further Phase 1 contract/validation gaps affect update cardinality and recovery from a self-inconsistent manifest.

The roadmap already marks Phase 1 Partial and requires update/delete semantics, crash/recovery boundaries, typed conflicts, and direct verification (`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`). The findings below are therefore not cross-process or serializability requests; they are inside the supported shared-handle boundary.

## Findings

### COR-01 — A future/nonexistent tombstone can hide a later acknowledged insert and bypass write-write conflict detection

- **Severity:** Critical
- **Confidence:** High
- **Affected phase:** Phase 1; blocks Phase 2 evidence
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - `Transaction::delete` unconditionally appends any caller-supplied `u64` to both `pending_tombstones` and `write_set`, without proving that the row exists or is visible in the transaction's base snapshot (`crates/txn/src/dataset.rs:748-755`).
  - Insert-only transactions add nothing to `write_set` (`crates/txn/src/dataset.rs:741-746`, `crates/txn/src/dataset.rs:570-573`). Conflict validation considers only that write set (`crates/txn/src/dataset.rs:948-963`).
  - A delete-only commit skips row-ID allocation (`crates/txn/src/dataset.rs:1196-1204`), but its supplied ID is still persisted as a tombstone (`crates/txn/src/dataset.rs:1069-1082`).
  - The next insert claims from the unchanged allocator high-water mark (`crates/txn/src/dataset.rs:1237-1249`), while every read path treats the matching tombstone as authoritative (`crates/txn/src/snapshot.rs:124-146`, `crates/txn/src/snapshot.rs:156-230`).
  - Current delete/update tests seed a real row first and do not cover nonexistent or future IDs (`crates/txn/src/dataset.rs:5591-5605`, `crates/txn/src/dataset.rs:5663-5680`).

**Counterexamples:**

1. On an empty dataset, `delete(0)` commits a tombstone while `next_row_id` remains 0. The next insert is assigned row ID 0, returns `Ok(())`, and is immediately filtered from scan and vector search. This violates the target rule that an acknowledged write is visible (`docs/architecture.md`, opening contract; the active document now qualifies that contract as Partial and blocked).
2. A delete transaction can begin at version 0 and target row 0 before that row exists. A concurrent insert then commits row 0 with an empty conflict write set. The stale delete subsequently passes conflict validation and tombstones row 0. This is a write/write overlap that is not reported as the typed conflict required by the intended current boundary (`docs/architecture.md`, `## Commit and snapshot lifecycle`).

Resolve the public contract for missing/already-deleted targets, reject a target not valid in the transaction's base view with a typed error, and ensure an insert's newly allocated row IDs participate in any conflict rule needed to close the stale-delete interleaving. Add direct scan and vector-search tests for both counterexamples.

### COR-02 — Failed pre-publication row-ID claims are reused after restart

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 0 invariant and Phase 1 recovery
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - `RowIdRange` documents every granted range as permanently consumed, with “gaps are safe, reuse is forbidden” (`crates/txn/src/row_id.rs:75-78`).
  - A claim advances only the in-memory allocator before any commit lock or manifest publication (`crates/txn/src/row_id.rs:137-153`, `crates/txn/src/dataset.rs:1237-1249`).
  - The new high-water mark reaches durable state only when a later manifest is built and successfully published (`crates/txn/src/dataset.rs:1036-1042`, `crates/txn/src/dataset.rs:1113-1119`).
  - `Dataset::open` reconstructs the allocator solely from the last manifest's `next_row_id` (`crates/txn/src/dataset.rs:426-445`).
  - Failed-commit tests confirm the old manifest survives and an orphan segment survives reopen, but do not commit through the reopened handle and check the next assigned ID (`crates/txn/src/dataset.rs:2240-2309`, `crates/txn/src/dataset.rs:6708-6765`).

If a transaction claims row ID 1, writes its files, and fails before manifest publication, an immediate process restart reloads the prior `next_row_id == 1`; the next insert reuses row ID 1 even though the orphaned files already contain that physical allocation value. The same-session tests do not expose this because the in-memory allocator still remembers the failed claim and a later successful commit persists the gap.

The repository targets dataset-global, monotonically allocated, never-reused row IDs (`AGENTS.md`, `Non-negotiable target invariants`; `docs/roadmap.md`, `## Phase 0 — Foundation`). Preserve reservations durably across restart, or redesign when an ID becomes an allocation value without weakening that target. Add a failed-manifest-publication → drop/reopen → insert test that asserts strict non-reuse for both row data and segment metadata.

### COR-03 — Directory-fsync failures are ignored before durability is acknowledged

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 durability and recovery
- **Disposition:** Phase 1 blocker; cross-check with the durability lane
- **Evidence:**
  - `sync_dir` explicitly tolerates both failure to open a directory and failure of `sync_all`, always returning `Ok(())` (`crates/storage/src/datafile.rs:62-83`).
  - Data/segment preparation relies on that best-effort call to make newly created directory entries durable before manifest publication (`crates/txn/src/dataset.rs:1269-1275`).
  - Manifest publication also calls the same best-effort helper after rename and then returns success (`crates/storage/src/backend/local.rs:185-216`).
  - `Transaction::commit` treats successful `commit_manifest` as the durability point, installs the snapshot, and returns `Ok(())` (`crates/txn/src/dataset.rs:1113-1156`).

On a filesystem/platform where directory opening or directory `sync_all` fails, Strata can acknowledge a commit even though the new data, segment, or manifest directory entry is not guaranteed to survive power loss. Process abort/kill tests cannot prove this property because they do not simulate loss of unflushed filesystem metadata. This conflicts with the acknowledged-write durability invariant targeted by the project contract (`docs/architecture.md`, opening contract; `AGENTS.md:16`), even though the active architecture now qualifies that invariant as Partial and blocked.

Return a typed durability error when the supported platform requires and cannot complete the directory sync, or explicitly narrow the supported filesystem/platform guarantee and enforce that precondition before acknowledging writes. Add platform-appropriate fault injection around both data-directory and manifest-directory sync.

### COR-04 — Recovery accepts a manifest whose filename version disagrees with its payload version

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 recovery/error behavior
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - `read_current` selects the highest numeric filename, but discards that parsed version before deserializing and returning the payload (`crates/storage/src/manifest.rs:229-256`).
  - `Dataset::open` trusts the payload's `manifest.version` as the snapshot version (`crates/txn/src/dataset.rs:426-472`).
  - The next commit derives its version from that trusted snapshot and publishes to the corresponding filename (`crates/txn/src/dataset.rs:945-967`, `crates/storage/src/manifest.rs:202-216`).
  - Existing recovery tests cover invalid JSON and temporary/non-numeric names, but not filename/payload disagreement (`crates/storage/src/manifest.rs:325-416`, `crates/storage/src/manifest.rs:472-508`).

A fully renamed, valid-JSON `00000000000000000005.manifest` whose payload says version 4 is accepted as current. Commit ordering and subsequent recovery then depend on platform overwrite behavior instead of rejecting the self-inconsistent state. Compare filename and payload versions and return `CorruptManifest` on mismatch. Extend recovery validation to other load-bearing manifest relationships in a separately reviewed follow-up (for example tombstones/segment row IDs versus `next_row_id`, duplicate names, and incompatible overlapping ranges).

### COR-05 — `update` permits zero or many replacement rows despite singular replacement semantics

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 update/delete identity semantics
- **Disposition:** Phase 1 contract decision required; blocker until resolved
- **Evidence:**
  - `update` is exactly `delete(row_id)` followed by unrestricted `insert(batch)` and performs no `batch.num_rows()` check (`crates/txn/src/dataset.rs:797-807`).
  - The commit path deliberately supports arbitrary batch cardinality and allocates that many new physical IDs (`crates/txn/src/dataset.rs:1237-1249`).
  - The architecture describes one old physical row being replaced by data carrying a newly allocated global row ID (`docs/architecture.md:43`), and the controlling invariant says “a replacement with a new ID” (`AGENTS.md:24-25`).
  - Existing tests exercise only one-row replacement batches (`crates/txn/src/dataset.rs:5663-5680`; `crates/txn/src/snapshot.rs:1202-1228`).

Today an empty replacement batch silently turns `update` into delete, while an N-row batch turns one row into N replacements. Enforce exactly one replacement row with a typed error, or explicitly adopt and document one-to-many replacement semantics before Phase 1 exit. The current singular contract and implementation cannot both remain.

## Strengths

- Commit ordering is easy to audit: prepare and fsync unique data/segment files, acquire the shared commit lock, reload latest state, validate conflicts and vector dimension, publish one manifest, record history, then swap the immutable snapshot (`crates/txn/src/dataset.rs:915-1156`). Row and index additions share the same manifest boundary.
- The latest manifest is cloned under the commit lock before appending new files/segments, preventing stale non-conflicting writers from dropping intervening commits (`crates/txn/src/dataset.rs:981-1035`). Tests cover version sourcing and concurrent insert preservation (`crates/txn/src/dataset.rs:5752-5812`).
- Conflict results identify contested row IDs, bounded-history loss fails conservatively for nonempty write sets, and the commit-log implementation has both targeted tests and a property comparison against a naive reference (`crates/txn/src/commit_log.rs:79-185`, `crates/txn/src/commit_log.rs:194-397`).
- Snapshot state is structurally immutable: manifests, tombstone sets, and segment sets are captured together, and old snapshots retain their own state. Scan and ANN paths both enforce snapshot-scoped tombstones (`crates/txn/src/snapshot.rs:51-63`, `crates/txn/src/snapshot.rs:124-230`, `crates/txn/tests/concurrent_snapshot_isolation.rs:22-482`).
- Segment recovery validates length, CRC-backed format, node count, dimension, row-ID range metadata, and cross-segment dimension consistency before exposing a dataset (`crates/txn/src/dataset.rs:1705-1807`; `crates/index/src/segment_reader.rs:95-373`).
- Orphan files from failed preparation are not a visibility failure: scans and searches are manifest-driven. Physical orphan cleanup remains a Phase 3 lifecycle item, consistent with `docs/roadmap.md`, `## Phase 3 — Operational lifecycle`, provided COR-02's allocation-reuse issue is fixed independently.

## Verification evidence

Fresh checks against the audited working tree:

- `cargo test -p strata-txn --quiet` — passed: 129 unit tests, 4 concurrent snapshot tests, 1 MVP test, 3 pruning tests, and 6 doc tests; 0 failures.
- `cargo test -p strata-storage --lib --tests --quiet` — passed: 59 unit tests and 1 chaos-checkpoint integration test; 0 failures.
- `cargo test -p strata-query --lib --quiet` — passed: 60 tests; 0 failures.
- `cargo test -p strata-index --lib --quiet` — passed: 149 tests; 1 ignored expensive real-thread stress test; 0 failures.

The first aggregate package command timed out after 120 seconds despite showing all package test bodies green; the individual commands above provide the authoritative successful exit codes. This lane did not rerun loom or the opt-in 2,000-seed chaos tier. Their existing models/harness were reviewed as evidence, but neither currently exercises COR-01, COR-02, COR-04, or COR-05, and process-kill chaos does not substitute for COR-03's power-loss/directory-sync failure.

## Open questions

1. Must delete/update target a row visible in the transaction's begin-time snapshot, or is re-deleting an already-tombstoned row intentionally idempotent? Define missing, already-deleted, and future-row behavior separately; they need not share one outcome.
2. Does “never reused” include IDs embedded only in failed, orphaned files? The controlling invariant and `RowIdRange` documentation say yes, while the current allocator is only session-durable until another manifest succeeds.
3. Is `update` strictly one physical row to one replacement row? If one-to-many is desired, what identity and conflict contract should callers rely on?
4. Which local filesystems and operating systems are supported for acknowledged-write durability, and how is failure to durably sync directory entries surfaced?
5. How much cross-object recovery validation belongs in Phase 1 beyond filename/payload version equality: tombstones below `next_row_id`, segment row IDs below `next_row_id`, duplicate data/segment names, overlapping segment ranges, and data-file row-ID uniqueness?
