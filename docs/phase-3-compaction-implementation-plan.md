# Phase 3 Compaction Implementation Plan

> **For agentic workers:** Execute each task with TDD and an independent review checkpoint. Do not implement the next task until the current task's targeted checks pass.

**Goal:** Add an explicit, crash-safe, shared-handle compaction operation that rewrites the current logical snapshot into a smaller manifest-listed row/index set and reclaims only authority-proven obsolete objects.

**Architecture:** `Dataset::compact` will acquire lifecycle exclusivity before `commit_lock`, capture one immutable snapshot, write uniquely named replacement row/index objects, publish a new durable manifest, and then delete only objects absent from the published manifest and all protected active snapshot manifests. Existing commit/recovery helpers remain the durability authority; no new commit protocol or cross-process mechanism is introduced.

**Tech Stack:** Rust 2024, Cargo workspace, Arrow 58, existing `strata-storage` manifest/datafile helpers, existing `strata-index` immutable segment writer/reader, Criterion, crate-scoped loom.

## Global Constraints

- Supported concurrency remains one process using one shared `Dataset` handle.
- The isolation ceiling remains immutable snapshot reads plus write-write OCC; no serializability.
- Row IDs remain physical, globally allocated, monotonic, and never reused.
- Row data and vector-index changes publish through one manifest boundary.
- Every deletion must be authority-checked, typed, and counted only after successful deletion and directory synchronization.
- Crash recovery must accept either the old manifest or the fully published compacted manifest; never a partially written manifest.
- No dependency additions, on-disk compatibility changes, background compaction, object storage, or arbitrary orphan sweeping.
- Every interleaving-sensitive `crates/txn` change gets a targeted loom model or an explicit reason loom is not applicable.

---

### Task 1: Establish the compaction API and failing contract tests

**Files:**
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/lib.rs`
- Modify: `crates/txn/src/retention.rs` or create `crates/txn/src/compaction.rs` for policy/report types
- Test: `crates/txn/tests/compaction.rs`
- Modify: `docs/status.md`, `docs/roadmap.md` after behavior is implemented

**Interfaces:**
- Produce `CompactionPolicy`, `CompactionReport`, and `Dataset::compact(&self, CompactionPolicy) -> Result<CompactionReport>`.
- The initial policy must expose `retain_snapshots: bool`, defaulting to preservation behavior in the test helpers.
- The report must expose source/published versions, written row-file/segment counts, and successful deletion counts/bytes.

- [ ] **Step 1: Write failing tests**

Add tests that call the intended API and assert:

```rust
let report = dataset.compact(CompactionPolicy::retain_snapshots()).unwrap();
assert!(report.published_version > report.source_version);
assert_eq!(dataset.snapshot().scan(&schema()).unwrap().num_rows(), expected_rows);
```

Cover empty datasets, two committed row batches, three vector commits, and report field invariants. Assert that the current manifest has one replacement row file and at most one replacement vector segment after compaction.

- [ ] **Step 2: Run the focused tests to verify the correct failure**

Run:

```text
cargo test -p strata-txn --no-default-features --test compaction
```

Expected: compilation fails because the public policy/report types and `Dataset::compact` do not yet exist. Do not implement production code before observing this failure.

- [ ] **Step 3: Add the minimal public types and an unimplemented typed error path**

Define the policy/report types and method signature. Until the protocol exists, return a dedicated typed `TxnError` variant such as `CompactionUnavailable`; do not silently return an empty report.

- [ ] **Step 4: Run the focused tests again**

Expected: the tests compile and fail on the explicit unavailable error. This confirms the tests exercise the intended API rather than accidentally passing through an existing path.

- [ ] **Step 5: Commit only if explicitly authorized**

```text
git add crates/txn/src/dataset.rs crates/txn/src/lib.rs crates/txn/src/compaction.rs crates/txn/tests/compaction.rs
git commit -m "feat: define phase three compaction contract"
```

---

### Task 2: Build a replacement snapshot without reclamation

**Files:**
- Modify: `crates/txn/src/dataset.rs`
- Modify: `crates/txn/src/snapshot.rs` only for a focused reusable visible-row extraction helper if required
- Modify: `crates/index/src/segment_writer.rs` only if the existing immutable writer cannot consume the compacted visible rows
- Test: `crates/txn/tests/compaction.rs`

**Interfaces:**
- Consume the captured `Arc<Snapshot>` and its schema/manifest/index.
- Produce uniquely named replacement row data and vector-segment entries plus a candidate manifest, without deleting old objects.

- [ ] **Step 1: Add failing preservation tests**

Create a dataset with inserts, update/delete tombstones, nullable values, and vectors. Capture an old snapshot, compact the dataset, then assert the current snapshot and old snapshot return their respective expected rows and vector hits. Assert physical row IDs in the compacted rows remain the original IDs and the manifest high-water fields do not regress.

- [ ] **Step 2: Run the tests and confirm failure**

Run the focused compaction test. Expected failure: the method still returns `CompactionUnavailable` or the replacement manifest is not yet published.

- [ ] **Step 3: Implement visible-row materialization**

Use the captured snapshot's visibility/tombstone rules, preserve the dataset schema and physical row-ID column, and avoid reconstructing rows from a later snapshot. Ensure empty input writes no vector segment and nonempty vector input writes exactly one replacement segment.

- [ ] **Step 4: Implement unique temporary/final object naming**

Use the existing write-attempt allocator and path validation. Write files through existing `write_batch`/segment writer durability helpers. Do not reuse prior names and do not mutate the current manifest in place.

- [ ] **Step 5: Build and publish the replacement manifest**

Copy schema, tombstones, row-ID high-water, timestamp high-water, and attempt high-water from the captured authority; replace only row-file and segment lists; allocate a checked-next manifest version; publish with `commit_manifest`; install the new in-memory snapshot only after publication succeeds.

- [ ] **Step 6: Run focused green tests**

Run:

```text
cargo test -p strata-txn --no-default-features --test compaction
```

Expected: preservation, row identity, vector parity, reopen compatibility, and empty-dataset tests pass before reclamation is enabled.

---

### Task 3: Add authority-checked reclamation

**Files:**
- Modify: `crates/txn/src/dataset.rs` or `crates/txn/src/compaction.rs`
- Modify: `crates/txn/src/retention.rs` only for shared protected-manifest authority helpers
- Test: `crates/txn/tests/compaction.rs`
- Test: `crates/storage/src/backend` tests only if deletion error injection needs a storage seam

**Interfaces:**
- Consume the published manifest and active snapshot lease versions under lifecycle exclusivity plus `commit_lock`.
- Produce deletion counts/bytes only after successful `Backend::delete` calls.

- [ ] **Step 1: Add failing reclamation tests**

Assert that old row files and segments disappear only after successful publication; an active historical snapshot keeps its referenced objects; a missing/malformed protected object fails closed; and a post-unlink directory-sync error returns an error without falsely incrementing the report.

- [ ] **Step 2: Run focused tests and observe failure**

Expected: old objects remain or the report incorrectly reports zero deletions.

- [ ] **Step 3: Implement protected-object authority**

Relist exact manifest keys under both guards, read each protected manifest, collect all row/segment object names referenced by the published manifest and active snapshot leases, and reject malformed, duplicate, unsafe, or missing authority before deletion.

- [ ] **Step 4: Delete only superseded row/segment objects**

Never delete `_versions/`, row-ID catalogs, temporary files, or arbitrary orphan candidates in this task. Delete oldest obsolete data objects through the existing backend contract and report only successful operations.

- [ ] **Step 5: Run green reclamation tests**

Run the focused compaction test plus existing lifecycle executor tests. Verify retries are safe and old snapshots remain readable.

---

### Task 4: Crash, interleaving, and recovery verification

**Files:**
- Modify: `crates/txn/src/dataset.rs` fault checkpoints only if an existing test seam cannot target compaction
- Test: `crates/txn/tests/compaction.rs`
- Test: `crates/txn/tests/compaction_loom.rs` or crate-scoped loom module
- Modify: `.github/workflows/ci.yml` if a new exact command is required

**Interfaces:**
- Exercise compaction preparation/publication/reclamation checkpoints using existing typed fault injection.
- Prove preparation and lifecycle exclusivity do not overlap with a loom model or document why the existing lifecycle loom model covers the unchanged lock protocol.

- [ ] **Step 1: Add failing crash/reopen tests**

Inject failure before manifest publication and after publication before reclamation. Reopen the dataset and assert either the old or new complete manifest is valid, no partially written manifest is selected, and the next commit continues with correct high-water state.

- [ ] **Step 2: Run the tests and verify they fail for missing seams/behavior**

Run the targeted fault-injection test with `cargo test -p strata-txn --features test-fault-injection --test compaction`.

- [ ] **Step 3: Wire minimal compaction checkpoints**

Reuse existing directory-sync fault seams and add only named compaction checkpoints required to reproduce pre-publication and reclamation failures. Keep all failures typed.

- [ ] **Step 4: Run normal and loom verification**

Run the targeted tests, existing lifecycle tests, and the exact crate-scoped loom recipe. Do not set workspace-wide `RUSTFLAGS=--cfg loom`.

---

### Task 5: Benchmark and documentation closeout

**Files:**
- Create or modify: `bench/benches/compaction_bench.rs`
- Modify: `bench/Cargo.toml`
- Modify: `.github/workflows/phase-3-lifecycle-evidence.yml` or the current CI workflow
- Modify: `docs/phase-3-lifecycle-inventory-design.md`, `docs/phase-3-manifest-retention-executor-design.md`, `docs/status.md`, `docs/roadmap.md`
- Create: `docs/phase-3-compaction-verification-report.md`

**Interfaces:**
- Benchmark before/after segment fan-out and reopen/query behavior on deterministic synthetic data and the pinned fixture where practical.
- Report measured results without converting one workload into universal latency, storage, or memory guarantees.

- [ ] **Step 1: Add a failing benchmark smoke/configuration check**

Register the benchmark target and make the CI command explicit before relying on its output.

- [ ] **Step 2: Implement the compaction benchmark**

Measure K-segment vector search and reopen behavior before compaction, invoke explicit compaction, then measure the compacted replacement. Record segment counts, wall time, query latency, file bytes, and peak RSS separately from compilation.

- [ ] **Step 3: Run cloud evidence**

Use GitHub Actions on Ubuntu with the pinned Rust toolchain, retain provenance/artifacts, and compare the same fixture/configuration.

- [ ] **Step 4: Update status and closeout docs**

Mark only the implemented bounded slice complete. Keep row/segment reclamation scope, crash behavior, and non-claims explicit. Do not mark Phase 3 complete until all tests and evidence gates pass.

---

## Final verification gate

Before claiming completion, run fresh output for:

```text
cargo test -p strata-txn --no-default-features --test compaction
cargo test -p strata-txn --features test-fault-injection --test compaction
cargo test -p strata-txn --test manifest_retention_executor
cargo check --workspace
cargo test --workspace --no-default-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo doc --workspace --no-deps
git diff --check
```

Then run the exact crate-scoped loom model and the Phase 3 cloud benchmark workflow. A local
MSVC-linker failure is an environment limitation and must be reported separately from test
assertion failures.
