# Phase 0 Audit Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix confirmed Phase 0 audit defects and evidence gaps while preserving Strata's current on-disk format and one-process/shared-`Dataset` boundary.

**Architecture:** Apply small, vertical changes at the existing CI, test, cache, and immutable-segment seams. Runtime changes preserve current APIs and file formats; performance observations that require new formats become explicit roadmap gates rather than speculative rewrites.

**Tech Stack:** Rust 1.97.1, Cargo workspace, Arrow IPC, immutable HNSW segments, cargo-fuzz, GitHub Actions, crate-scoped loom.

## Global Constraints

- Preserve the intentional dirty baseline on `codex/phase-0-audit`.
- Do not add dependencies without explicit user approval.
- Do not change the on-disk format or claim universal power-loss durability.
- Keep concurrency support limited to one process sharing one `Dataset` handle.
- Use TDD for runtime/test behavior changes and targeted loom for interleaving-sensitive `txn`/`index` changes.
- Terra implements one task at a time; a separate Terra reviewer reviews each completed task.
- Sol performs only the final complete-branch review before publication.

---

### Task 1: Align toolchain, CI evidence, and recovery documentation

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/status.md`
- Modify: `docs/phase-0-audit.md`
- Modify: `crates/txn/src/snapshot.rs`
- Modify: `crates/txn/src/live_set_cache.rs`

**Interfaces:** No Rust API changes. CI's supported toolchain becomes `1.97.1`; recovery wording describes row ownership, uniqueness, metadata, checksums, and manifest consistency rather than byte-for-byte vector identity.

- [ ] Replace every supported-job `1.90` toolchain reference with `1.97.1`, preserving the pinned nightly toolchain used only for fuzz/loom where required.
- [ ] Add the existing row-ID restart evidence command to the Windows durability job and increase evidence artifact retention to a durable period used by the repository's CI policy.
- [ ] Narrow the status/audit vector-identity claim and state the unsupported tampering scenario explicitly.
- [ ] Replace stale `docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md` links with the current performance document.
- [ ] Run YAML/config scans, `git diff --check`, and the affected documentation checks.

### Task 2: Enforce hard LiveSet cache admission

**Files:**
- Modify: `crates/txn/src/live_set_cache.rs`
- Test: the existing `LiveSetCache` unit tests in `crates/txn/src/live_set_cache.rs` or its current test module

**Interfaces:** `LiveSetCache::get_or_try_compute` continues returning the computed `Arc<LiveSet>`; an oversized result is returned but not retained and must not push the accounting counter beyond the configured budget.

- [ ] Add a failing test for a computed `LiveSet` whose byte size exceeds the remaining budget.
- [ ] Run that test and confirm it fails because the current implementation retains the result/accounting charge after computation.
- [ ] Compute the result under the existing per-key synchronization, admit it only when the complete charge fits, and release/remove the unretained slot without changing error retry behavior.
- [ ] Run the cache unit tests, the same-key loom model, and `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings`.

### Task 3: Replace fan-out sorting with bounded top-k selection

**Files:**
- Modify: `crates/index/src/segment_set.rs`
- Test: existing `SegmentSet` search tests in `crates/index/src/segment_set.rs` and any focused segment-set integration test

**Interfaces:** `SegmentSet::fan_out` keeps its signature and `VectorMatch` output. Global row-ID mapping, nearest-first order, duplicate nearest-wins behavior, filters, and zone-map pruning remain unchanged.

- [ ] Add a failing unit test for a bounded selector helper that retains at most `k` unique `(row_id, distance)` candidates and updates a duplicate when a nearer occurrence arrives later.
- [ ] Run the helper test and confirm it fails because the bounded selector is not yet implemented.
- [ ] Implement the bounded selector, then retain the duplicate-row scenario as a compatibility oracle for `fan_out` integration.
- [ ] Replace the final full sort with the selector so only the retained top-k results are fully ordered while preserving duplicate nearest-wins semantics.
- [ ] Run index unit tests, filtered search tests, the relevant loom model if the implementation changes shared state, and the existing segment recall benchmark smoke check.

### Task 4: Make lifecycle admission tests deterministic

**Files:**
- Modify: `crates/txn/src/retention_executor.rs`
- Modify: `crates/txn/src/lifecycle_coordination.rs` for a test-only queue rendezvous helper if required
- Modify: `crates/txn/src/dataset.rs` for a test-only forwarding hook if required

**Interfaces:** No production lifecycle API change. Test-only hooks may expose the coordinator queue rendezvous within the crate. The test must establish the pruning executor's queued state before releasing the first preparation or starting the later preparation.

- [ ] Add a failing synchronization assertion or rendezvous that proves pruning has entered the coordinator wait state.
- [ ] Run the focused regression repeatedly, including `cargo test -p strata-txn --features parallel-insert`, and confirm the old scheduling window is removed.
- [ ] Implement the smallest test-only handshake using existing coordinator checkpoints/notifications.
- [ ] Run the default and `parallel-insert` transaction suites repeatedly.

### Task 5: Tighten chaos ambiguity to exact target IDs

**Files:**
- Modify: `tests/sim/tests/chaos.rs`
- Modify: the existing chaos worker/run-result protocol files that emit and parse operation `starting` records
- Test: `tests/sim/tests/chaos.rs`

**Interfaces:** Preserve existing log compatibility where practical; add target row IDs to delete/update in-flight records and calculate permitted lost/phantom sets by exact row ID.

- [ ] Add a focused regression fixture with two in-flight operations targeting the same row and verify that the tolerated ambiguity set contains that row only once.
- [ ] Run the regression against the old count-based oracle and confirm it exposes the false-negative slack.
- [ ] Emit/parse target IDs and replace count budgets with exact permitted-ID sets.
- [ ] Run the fast chaos seed test and the relevant simulation test suite.

### Task 6: Add production-boundary parser fuzz targets

**Files:**
- Create: `fuzz/fuzz_targets/manifest_current_parse.rs`
- Create: `fuzz/fuzz_targets/segment_parse.rs`
- Modify: `fuzz/Cargo.toml` only if target registration is required by the existing fuzz workspace
- Modify: `.github/workflows/ci.yml` for target discovery/build/smoke registration

**Interfaces:** Fuzz targets use existing public/internal parser entry points and introduce no runtime dependency.

- [ ] Add a manifest target that exercises current-file discovery, envelope checksum/version validation, and malformed directory entries.
- [ ] Add a segment target that calls `SegmentReader::from_bytes` and bounds input sizes before parsing.
- [ ] Run target compilation and bounded smoke runs using the repository's pinned nightly recipe.
- [ ] Keep corpus/artifact paths out of the commit.

### Task 7: Make loom and platform evidence explicit

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/phase-0-audit.md`
- Modify: `docs/status.md`

**Interfaces:** No engine behavior change. CI must invoke crate-scoped loom binaries directly and record timeout/incomplete evidence honestly.

- [ ] Add named CI steps for the required `strata-index` and `strata-txn` crate-scoped loom build/run recipes.
- [ ] Ensure normal Cargo test summaries do not stand in for `#[cfg(loom)]` model evidence.
- [ ] Record the two locally slow loom models as bounded evidence gaps unless CI completes them; do not delete or weaken the models.
- [ ] Preserve Windows and Linux evidence distinctions and retain summarized provenance beyond short artifact expiry.

### Task 8: Record performance and concurrency entry gates

**Files:**
- Modify: `docs/phase-1-performance.md`
- Modify: `docs/roadmap.md`
- Modify: `docs/status.md`
- Modify: `docs/decisions.md` if an ADR is needed to record the no-format-change boundary

**Interfaces:** Documentation only.

- [ ] Add measurable gates for manifest/history growth, segment count/fan-out, cold filtered I/O, recovery latency/memory, lease cleanup, and lifecycle fairness.
- [ ] State which findings remain intentionally deferred because they require a format, migration, or operational-owner decision.
- [ ] Link the audit evidence and the new tests/benchmarks without claiming unsupported universal durability or cross-process behavior.

### Task 9: Full verification and publication

**Files:**
- Modify only files accepted by the task reviews.

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo check --workspace`.
- [ ] Run `cargo test --workspace --no-default-features`.
- [ ] Run `cargo test -p strata-txn --features parallel-insert` and targeted index/txn loom recipes.
- [ ] Run `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`.
- [ ] Run fuzz target compile/smoke commands and the fast chaos regression.
- [ ] Inspect `git diff --check`, exact file scope, and the staged diff.
- [ ] Obtain Sol's complete-branch review and resolve all critical/important findings.
- [ ] Commit the reviewed changes, push `codex/phase-0-audit`, and open the PR against `main`.
