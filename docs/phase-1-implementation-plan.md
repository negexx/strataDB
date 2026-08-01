# Phase 1 Audit Remediation Implementation Plan

> **For agentic workers:** Execute one task at a time with the approved Luna -> Sol -> Terra workflow. Production behavior changes require TDD: write the failing regression, run it and observe the expected failure, implement the smallest fix, then rerun targeted and workspace checks. Do not create or restore `docs/superpowers/`.

**Goal:** Close the approved Phase 1 correctness, durability, schema, recovery, facade, verification, and current segmented-evidence blockers within Strata's one-process/shared-`Dataset` boundary.

**Architecture:** Keep immutable manifest-listed vector segments and snapshot reads. Add a versioned, checksummed recovery catalog for the dataset schema, row-file ownership, and row/index integrity; add a separate durable row-ID high-water record; and make `Dataset` the supported strict facade for schema validation and physical-row target semantics. Directory durability fails closed, while compaction and cross-process coordination remain deferred.

**Tech Stack:** Rust 1.90, Cargo workspace, Arrow 58, serde/serde_json, CRC32C, loom, real-process chaos tests, GitHub Actions, and Criterion benchmarks.

## Global Constraints

- Supported concurrency remains one process using one shared `Dataset` handle.
- `Dataset::create` requires an explicit logical `SchemaRef`; `_row_id` and `_timestamp` remain reserved physical columns.
- Updates are exactly one live physical row to one replacement row with a new physical row ID; logical keys and schema evolution are deferred.
- Legacy datasets without the new schema/integrity metadata return typed `LegacyFormatNeedsMigration` rather than opening unverified.
- Directory synchronization errors are returned; no write is acknowledged before the ordered durability boundary succeeds.
- The manifest remains the atomic visibility boundary for row files, tombstones, and immutable vector segments.
- Cross-process publication, compaction, orphan cleanup, authenticated tamper protection, and stable client API work remain deferred.
- No new external dependency is added without recording why it is required and obtaining approval; prefer the already-resolved `crc32c` package used by `strata-index`.
- `crates/txn/src/dataset.rs`, `crates/txn/src/snapshot.rs`, and `crates/storage/src/manifest.rs` have one serialized writer at a time.
- Tasks 2 and 4 form one serialized compatibility stream, not two independently landing formats: write their failing schema/API and catalog tests together, then land schema ownership, the manifest envelope, and recovery validation as one implementation gate before migrating callers. No commit may persist a temporary schema representation that a later task replaces; the task labels only organize review scope.
- Every interleaving-sensitive transaction/index change gets a targeted loom model and normal tests.
- Do not claim universal power-loss, recall, latency, or unbounded-lifecycle guarantees; report named platforms, workloads, seeds, revisions, and measured bounds.

---

### Task 1: Fail-closed local durability primitives

**Files:**

- Modify: `crates/storage/src/datafile.rs`
- Modify: `crates/storage/src/backend/local.rs`
- Modify: `crates/storage/src/error.rs`
- Modify: `crates/storage/src/manifest.rs`
- Modify: `crates/storage/src/lib.rs`
- Modify: `crates/txn/src/dataset.rs` (dataset-creation durability integration only)
- Test: storage unit tests and `crates/storage/tests/chaos_checkpoint_actually_aborts.rs`
- Test: transaction dataset-creation durability tests

**Interfaces:**

- `sync_dir(path: &Path) -> Result<()>` must return directory-open and `sync_all` failures.
- `write_batch` and `write_bytes` return a small digest/length value used by manifest entries while retaining compatibility for callers that ignore the result.
- Add a typed `DurabilityUnsupported` storage error for a platform/filesystem that cannot provide the declared directory-sync operation.
- `Dataset::create` requires its immediate parent to pre-exist as the caller-owned durable anchor; it synchronizes only the dataset directory and that immediate parent on every attempt, making retries after a pre-publication failure safe without traversing inaccessible system ancestors.
- `LocalFs` requires its configured root to pre-exist as its durable anchor, synchronizes the owned parent chain only through that root, and rejects symlinked root/key components before filesystem access.

- [ ] **Step 1: Write failing durability tests.** Add tests proving a directory-open/sync failure is returned, and that a successful file rename is followed by a required containing-directory sync. Use a test-only fault-injection seam rather than mocking `std::fs` in production.

- [ ] **Step 2: Run the targeted tests and observe failure.** Run `cargo test -p strata-storage sync_dir -- --nocapture` and confirm the current best-effort implementation incorrectly returns `Ok(())` for the injected failure.

- [ ] **Step 3: Implement the minimal fail-closed primitive.** Remove ignored `File::open`/`sync_all` results, preserve platform-specific unsupported behavior as a typed error, and keep the existing atomic temp-write/rename ordering.

- [ ] **Step 4: Add dataset-root durability tests.** Test that creation synchronizes the newly-created dataset directory and its pre-existing immediate parent before returning, and that a retry repeats the bounded chain after an injected pre-publication failure. Keep the test portable by asserting the ordered calls through the fault-injection seam and by documenting the named local filesystem support matrix.

- [ ] **Step 5: Run targeted storage tests.** Run `cargo test -p strata-storage` and `cargo fmt --check`; record the exact test count and platform behavior.

- [ ] **Step 6: Commit.** Commit only the storage durability files and tests with `fix(storage): fail closed on directory durability errors`.

### Task 2: Versioned manifest/recovery catalog and integrity checks

This task is implemented in the shared Task 2/Task 4 compatibility stream. Its catalog validator consumes the same schema bytes written by `Dataset::create`; it does not define a second schema format or land before the schema/API tests and implementation gate.

**Files:**

- Modify: `crates/storage/Cargo.toml` only if the already-resolved `crc32c` package must become a direct dependency; otherwise reuse an existing workspace path without adding a package.
- Modify: `crates/storage/src/manifest.rs`
- Modify: `crates/storage/src/datafile.rs`
- Modify: `crates/storage/src/error.rs`
- Modify: `crates/storage/src/lib.rs`
- Modify: `crates/index/src/segment_reader.rs`
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/snapshot.rs`
- Modify: `crates/txn/src/error.rs`
- Test: storage manifest/data-file tests, index segment-reader tests, and transaction recovery tests

**Interfaces:**

- Add an explicit current manifest format constant and a required dataset-owned serialized Arrow schema.
- Extend `DataFileEntry` with byte length, CRC32C, row count, and inclusive row-ID range.
- Add a manifest checksum calculated over a canonical copy whose checksum field is zeroed.
- Add `StorageError::LegacyFormatNeedsMigration(PathBuf)` and typed corruption errors that name the violated catalog field.
- Add a checked `SegmentReader::row_ids()`/iterator accessor for recovery validation without exposing mutable index state.

The on-disk format is exact: `MANIFEST_FORMAT_VERSION` identifies the envelope format and `Manifest.version` identifies the committed snapshot. The persisted value is `ManifestEnvelope { format_version, manifest, checksum }`; checksum is CRC32C over canonical UTF-8 JSON bytes of the envelope with `checksum` set to zero. Canonical JSON recursively sorts object keys before serialization, including the existing stats maps. The logical Arrow schema is stored as `schema_ipc: Vec<u8>` containing the raw Arrow IPC schema message bytes produced by the existing `IpcDataGenerator::schema_to_bytes_with_dictionary_tracker` API with default `IpcWriteOptions`; the byte vector is represented directly as a JSON array, and recovery decodes it through the matching `arrow::ipc::convert` API. This uses the already-enabled Arrow IPC feature and adds no dependency or feature approval. Missing or undecodable schema bytes are legacy/corrupt metadata, never inferred from a data file. `DataFileEntry.row_id_range` is `Option<(u64, u64)>`, and is `None` exactly when `row_count == 0`; non-empty ranges are inclusive and validated against every physical row ID.

- [ ] **Step 1: Write failing catalog tests.** Add tests for filename/payload version mismatch, legacy metadata rejection, manifest checksum mutation, row-byte mutation, wrong row count/range, duplicate row ownership, invalid tombstones, duplicate segment row IDs, and a vector ID without a row owner.

- [ ] **Step 2: Run recovery tests to verify red.** Run the new tests individually through `cargo test -p strata-storage` and `cargo test -p strata-txn`; confirm the current reader accepts at least the filename/payload mismatch and validly-encoded metadata mutation.

- [ ] **Step 3: Implement the catalog format.** Persist the explicit format, schema, checksums, and ownership metadata. Treat missing required fields as `LegacyFormatNeedsMigration`; do not silently compute retroactive checksums for legacy bytes.

- [ ] **Step 4: Implement recovery validation.** In the single `Dataset::open` recovery path, verify manifest identity/checksum, every row file's digest/schema/row IDs, tombstone ownership, segment metadata/CRC, cross-segment dimensions, duplicate vector IDs, and vector-to-row ownership before constructing `Snapshot`.

- [ ] **Step 5: Verify row/index identity.** Add the row-ID iterator and reject any segment row ID that is absent from validated row-file ownership or appears in more than one segment. Preserve current segment CRC and byte-length checks.

- [ ] **Step 6: Run targeted recovery checks.** Run `cargo test -p strata-storage manifest`, `cargo test -p strata-txn reopening`, and the exact new corruption tests. Run `cargo fmt --check` and `cargo clippy -p strata-storage -p strata-txn --all-targets -- -D warnings`.

- [ ] **Step 7: Commit.** Commit only the catalog/recovery files and tests with `fix(storage,txn,index): validate phase one recovery identity`.

### Task 3: Durable row-ID high-water allocation

**Files:**

- Modify: `crates/txn/src/row_id.rs`
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/error.rs`
- Modify: `crates/storage/src/lib.rs` and one narrow storage metadata module if needed
- Test: row allocator unit tests, transaction restart tests, and process-level recovery tests

**Interfaces:**

- Add a checksummed `_meta/row-id-high-water/` collection of immutable records named by their claimed end, with an atomic temp-write/rename/sync protocol; never replace an existing target.
- `RowIdAllocator::claim` durably publishes the checked new end before returning `RowIdRange`.
- `Dataset::open` seeds from `max(manifest.next_row_id, durable_high_water)`; `Dataset::create` initializes the record before acknowledgement.
- A failed reservation returns a typed durability/I/O error and consumes no claim visible to the transaction.

- [ ] **Step 1: Write failing restart/non-reuse tests.** Reproduce a claim followed by manifest failure/process abort, reopen, commit a new row, and assert its row ID is greater than the abandoned range. Add an injected reservation-write failure asserting no row file is created.

- [ ] **Step 2: Run the tests to verify red.** Run the exact new tests and confirm the current allocator reuses the old manifest high-water mark after restart.

- [ ] **Step 3: Implement durable reservation.** Add the metadata record and make allocation persist-before-expose under the allocator lock. Keep the existing lock ordering: commit lock may take the allocator lock, never the reverse.

The high-water collection is monotonic across uncertain failures: persist `max(existing, requested_end)` and never overwrite a higher value with a lower one. For a new higher end, write a uniquely named immutable record; if that target already exists, verify its checksum and treat it as durable rather than replacing it. If rename succeeds but directory sync fails, return the durability error while treating the requested end as possibly visible; an in-process retry and a later restart must scan/retain that higher end and must not reuse the abandoned range. Cover failure before rename, after rename/before sync, repeated reservations, retry in-process, and restart with real filesystem fault injection/chaos; loom models the publication state machine.

- [ ] **Step 4: Add the loom model.** Add the exact model `row_id::loom_tests::concurrent_claims_publish_monotonic_high_water`, modeling two concurrent claims and a failed publication, asserting non-overlap and that no successful claim is exposed before its durable high-water transition.

- [ ] **Step 5: Run targeted verification.** Run allocator tests, the new reopen test, and the scoped transaction loom binary using the repository's `cargo rustc -p strata-txn --lib --profile test -- --cfg loom` recipe. Record any platform-specific durability limitation.

- [ ] **Step 6: Commit.** Commit the allocator and metadata files with `fix(txn): persist row id reservations before exposure`.

### Task 4: Dataset-owned schema and strict target/update contract

**Files:**

- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/snapshot.rs`
- Modify: `crates/txn/src/error.rs`
- Modify: `crates/txn/src/lib.rs`
- Modify: `crates/storage/src/manifest.rs` only through the serialized recovery stream
- Modify: `crates/chaos-worker/src/commit_ops.rs`, `crates/chaos-worker/src/reader.rs`, and fixtures
- Modify: `crates/chaos-worker/src/main.rs`, `crates/cli/src/main.rs`, `crates/txn/examples/basic_usage.rs`
- Modify: all Dataset call sites in `bench/benches/concurrent_commit_bench.rs`, `bench/benches/ef_construction_sweep_bench.rs`, `bench/benches/lifecycle_bench.rs`, `bench/benches/lockfree_vs_hnsw_rs_bench.rs`, `bench/benches/manifest_growth_bench.rs`, `bench/benches/segment_recall_bench.rs`, and `bench/benches/vector_search_bench.rs`
- Modify: `crates/txn/tests/concurrent_snapshot_isolation.rs`, `crates/txn/tests/mvp_checklist_1_to_5.rs`, `crates/txn/tests/phase_3_pruning.rs`, and `tests/sim/tests/chaos.rs`
- Test: transaction unit/integration tests, snapshot tests, CLI tests, chaos-worker tests, and bindings compile tests

**Interfaces:**

```rust
pub fn create(dir: impl Into<PathBuf>, schema: SchemaRef) -> Result<Self>;
pub fn schema(&self) -> SchemaRef;
pub fn insert(&mut self, batch: RecordBatch) -> Result<()>;
pub fn delete(&mut self, row_id: u64) -> Result<()>;
pub fn update(&mut self, row_id: u64, replacement: RecordBatch) -> Result<()>;
```

`insert` and `update` validate against the persisted schema before allocation or file I/O. `delete` and `update` validate that the target is owned and live in the transaction's base snapshot; stale concurrent targets are revalidated under the commit lock and surface as typed conflicts. `update` rejects zero- or multi-row replacements and never partially mutates the transaction on validation failure.

- [ ] **Step 1: Write failing schema/target tests.** Cover renamed/swapped fields, castable-but-different types, nullability, nested vector shape, schema persistence after reopen, empty-schema reads, future tombstones, missing targets, already-dead targets, duplicate targets, stale concurrent targets, and zero/multi-row updates.

- [ ] **Step 2: Run the targeted tests to verify red.** Run each new test by exact name and confirm current positional casts and unrestricted tombstones produce the counterexamples.

- [ ] **Step 3: Implement schema ownership.** Persist the creation schema, expose it from Dataset/Snapshot, validate every inserted physical batch against it, and make reads project by owned field names rather than allowing caller renaming.

- [ ] **Step 4: Implement target ownership and update cardinality.** Retain the base snapshot/ownership view in `Transaction`, add typed `RowNotFound`, `RowNotLive`, `InvalidUpdateShape`, and duplicate-target errors, and make target validation atomic with the transaction's buffered state.

- [ ] **Step 5: Revalidate at publication.** Keep write-write OCC for stale targets, then verify the target is still live before adding the tombstone/replacement to the latest manifest. Ensure rejected operations publish neither row files nor tombstones reachable from a later snapshot.

- [ ] **Step 6: Migrate all callers.** Update fixtures, examples, CLI, chaos-worker, tests, and benchmarks to pass explicit schemas and handle `Result` from insert/delete/update. Do not broaden the Python binding scaffold into a client API.

Use `rg -l "Dataset::create|\\.insert\\(|\\.delete\\(|\\.update\\(|\\.scan\\(" crates bench tests` as the mechanical call-site audit, then inspect each match so unrelated query/index/storage matches are not changed accidentally.

- [ ] **Step 7: Run targeted checks.** Run `cargo test -p strata-txn`, CLI/chaos-worker tests, and the exact future-tombstone and update-cardinality regressions. Run clippy for all affected crates.

- [ ] **Step 8: Commit.** Commit the facade contract and tests with `fix(txn): enforce owned schema and row targets`.

### Task 5: Preserve insufficient-history error semantics

**Files:**

- Modify: `crates/txn/src/commit_log.rs`
- Modify: `crates/txn/src/error.rs`
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/chaos-worker/src/commit_ops.rs` only where error matching requires it
- Test: commit-log and transaction history tests

- [ ] **Step 1: Write the failing error test.** Change the existing aged-out-history regression to require `TxnError::InsufficientHistory { base_version, oldest_retained_version, latest_version }`, and retain a separate genuine row-overlap assertion.

- [ ] **Step 2: Run it to verify red.** Confirm the current implementation returns `TxnError::Conflict` with the entire write set.

- [ ] **Step 3: Implement typed propagation.** Carry retained-range context through `ConflictCheck`, increment the observability counter, and return the dedicated error without invented contested IDs.

- [ ] **Step 4: Run targeted tests and commit.** Run commit-log unit tests and the exact history tests, then commit with `fix(txn): preserve insufficient history errors`.

### Task 6: Supported facade boundary and canonical status updates

**Files:**

- Modify: `crates/storage/Cargo.toml`, `crates/index/Cargo.toml`, `crates/query/Cargo.toml`, `crates/txn/Cargo.toml`, `crates/cli/Cargo.toml`, and `crates/bindings/Cargo.toml` to mark internal workspace packages non-publishable where appropriate
- Modify: package-level `lib.rs` documentation for storage/index/query/txn
- Modify: `docs/architecture.md`, `docs/design.md`, `docs/status.md`, `docs/decisions.md`, `docs/roadmap.md`, and `docs/phase-1-audit.md`
- Test: cargo metadata, rustdoc, stale-claim/link scans, and documentation checks

- [ ] **Step 1: Write documentation/config regression checks.** Add a scriptable scan or CI shell assertions that no supported guarantee is attached to direct low-level storage/index use, no deleted `docs/superpowers/` path is linked, and all internal packages report `publish = false`.

- [ ] **Step 2: Implement the narrow facade statement.** Document Dataset/Snapshot/Transaction as the supported engine surface and low-level crates as internal implementation surfaces. Do not merge crates or attempt ARCH-06/07/08.

- [ ] **Step 3: Update status without overclaiming.** Keep Phase 1 Partial until all verification evidence passes; explicitly list legacy rejection, named durability platforms, bounded performance evidence, and deferred findings.

- [ ] **Step 4: Run checks and commit.** Run `cargo metadata --no-deps`, stale-link scans, `git diff --check`, and documentation tests; commit with `docs: bound phase one facade and status claims`.

### Task 7: Regression, loom, chaos, and CI gates

**Files:**

- Modify: `.github/workflows/ci.yml`
- Modify: `crates/storage/Cargo.toml` and checkpoint tests
- Modify: `tests/sim/tests/chaos.rs`
- Modify: transaction/index/live-set-cache loom modules and their tests
- Modify: affected source comments that still reference deleted `.opencode/` guidance
- Test: CI-equivalent local commands and exact loom/chaos targets

- [ ] **Step 1: Write failing gate checks.** Add CI commands that fail when no transaction loom binary is produced, the live-set-cache model is not selected, the checkpoint helper is skipped, or the thorough chaos test silently returns success without exercising its requested seed count.

- [ ] **Step 2: Run the gate checks to verify red.** Run the current CI recipe and capture that only index loom is wired and the opt-in tests can self-skip.

- [ ] **Step 3: Add scoped loom gates.** Keep crate-scoped `cargo rustc` builds, run transaction models individually by exact name, and add a live-set-cache model command with an explicit test-discovery assertion. Do not set workspace-wide `RUSTFLAGS=--cfg loom`.

- [ ] **Step 4: Make chaos/checkpoint gates exercise.** Use Cargo required features and `#[ignore]` for the 2,000-seed thorough test; remove internal success-on-skip behavior. Keep the 30-seed fast tier on the ordinary workflow and document the long runtime.

- [ ] **Step 5: Run targeted gates.** Run the checkpoint helper test, fast chaos tier, exact transaction loom models, exact cache loom model, and the CI-equivalent index loom command. Update checkpoint thresholds only after all preceding durability/allocator changes are complete.

- [ ] **Step 6: Commit.** Commit CI and verification changes with `test(ci): gate phase one concurrency and chaos evidence`.

VER-04 (fuzz-build/discovery), VER-05 (immutable CI provenance), and VER-06 (portable benchmark provenance) remain separately tracked evidence findings in this branch. Task 7 may add reproducible local/CI assertions, but must not relabel those findings as remediated unless the required evidence is actually produced; the final status retains `Partial` and names each residual finding.

### Task 8: Current segmented performance and operating-bound evidence

**Files:**

- Modify: `bench/benches/manifest_growth_bench.rs`
- Modify: `bench/benches/lifecycle_bench.rs`
- Modify: `bench/benches/segment_recall_bench.rs`
- Add: `docs/phase-1-performance.md`
- Modify: `docs/status.md`, `docs/roadmap.md`, and `docs/phase-1-audit.md`
- Test: Criterion benchmark commands and report reproducibility checks

- [ ] **Step 1: Define the measurement matrix.** Record revision, Rust/toolchain, features, OS/filesystem, CPU/RAM, dataset recipe/hash, seeds, commands, and warm/cold-cache state for commit, manifest growth, reopen/recovery, segment fan-out, filtered search, and pinned-snapshot memory.

- [ ] **Step 2: Correct benchmark scope.** Ensure the measurements exercise the current immutable manifest-listed segment path rather than the retired shared graph. Preserve the current HNSW parameters; do not tune them in this task.

- [ ] **Step 3: Run bounded measurements.** Capture multiple points for retained versions, manifest bytes, segment count, recovery bytes/time, vector-search latency/recall, and snapshot/cache residency. Keep results as evidence, not universal guarantees.

- [ ] **Step 4: Write the current operating envelope.** Document measured ranges and known growth behavior. State that compaction, vacuum, orphan cleanup, and indefinite sustained operation remain Phase 3.

- [ ] **Step 5: Verify and commit.** Run the relevant Criterion benches, validate report commands and stale claims, then commit with `docs(bench): record current segmented phase one bounds`.

### Task 9: Whole-branch verification and fresh Sol review

**Files:**

- All intentional files changed by Tasks 1–8

- [ ] **Step 1: Inspect the complete diff.** Confirm no `docs/superpowers/` tree, credentials, build artifacts, benchmark datasets, or unrelated edits are present.

- [ ] **Step 2: Run required verification.** Run, with fresh output:

```text
cargo fmt --check
cargo build --workspace
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
cargo deny check bans sources advisories
cargo test -p strata-txn --features parallel-insert
cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings
```

Also run `cargo metadata --no-deps`, the exact recovery corruption tests, and the following named evidence gates. For transaction loom, run `cargo rustc -p strata-txn --lib --profile test --message-format=json -- --cfg loom | tee /tmp/loom-txn-build.json`, set `bin=$(jq -r 'select(.reason == "compiler-artifact" and .executable != null) | .executable' /tmp/loom-txn-build.json | tail -1)`, assert `test -n "$bin"`, then invoke `$bin --exact` once for each named model: `dataset::loom_tests::two_threads_deleting_the_same_row_exactly_one_conflicts`, `dataset::loom_tests::two_threads_deleting_disjoint_rows_both_succeed`, `dataset::loom_tests::a_failed_commits_segment_is_never_searchable_under_concurrent_commits`, `dataset::loom_tests::concurrent_first_vector_commits_at_different_dimensions_are_not_both_accepted`, `dataset::loom_tests::a_failed_commits_segment_is_never_visible_to_a_concurrent_reader`, `dataset::loom_tests::a_commits_row_and_its_segment_become_visible_as_one_atomic_step`, `dataset::loom_tests::a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`, `live_set_cache::loom_tests::two_concurrent_misses_on_the_same_key_compute_exactly_once`, and `row_id::loom_tests::concurrent_claims_publish_monotonic_high_water`. The index loom gate uses the existing `.github/workflows/ci.yml` JSON/`jq` recipe with its executable assertion. Run the checkpoint gate as `cargo test -p strata-storage --features chaos-injection --test chaos_checkpoint_actually_aborts -- --exact commit_manifest_aborts_at_the_configured_checkpoint`, the fast chaos gate as `cargo test -p strata-sim fast_tier_random_seeds_survive_random_crash_points -- --exact --nocapture`, and the thorough gate explicitly as `STRATA_CHAOS_THOROUGH=1 cargo test -p strata-sim thorough_tier_satisfies_the_phase_7_exit_criterion -- --exact --ignored --nocapture`, asserting the output reports `2000/2000` seeds (or failing if the implementation still self-skips). Run the benchmark matrix, stale-link/claim scans, and `git diff --check`.

- [ ] **Step 3: Dispatch a fresh Sol review.** Review the full branch against this plan and `docs/phase-1-audit.md`, with special attention to transaction invariants, on-disk compatibility, durability acknowledgement, schema ownership, row/vector identity, and deferred-scope claims. Sol makes no edits.

- [ ] **Step 4: Fix accepted findings with Terra.** Each accepted finding gets a failing regression first, a minimal fix, targeted verification, and a new focused commit. Re-run the affected full checks.

- [ ] **Step 5: Final verification.** Repeat all required checks after the last fix. Do not declare Phase 1 complete while any P0 blocker lacks an implementation and regression test.

- [ ] **Step 6: Commit only intentional files.** Inspect `git diff --cached`, then create focused commits for any remaining final adjustments.

### Task 10: Publish, merge, and cleanup

- [ ] **Step 1: Push the branch** `codex/phase-1-audit-implementation`.
- [ ] **Step 2: Open a ready-for-review PR** describing the supported boundary, explicit legacy rejection, named durability platform scope, verification evidence, and deferred findings.
- [ ] **Step 3: Wait for required checks** and investigate any failure with systematic debugging before changing code.
- [ ] **Step 4: Merge only after checks and fresh Sol review pass.**
- [ ] **Step 5: Confirm the merge commit on remote main.**
- [ ] **Step 6: Delete the worktree and feature branch only after merge confirmation.**
- [ ] **Step 7: Report the merged PR, exact verification output, and every deferred finding that remains.**
