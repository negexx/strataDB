# Phase 1 Closeout Implementation Plan

> **Execution rule:** follow this plan task-by-task. Each behavior change starts with a
> failing test or executable evidence assertion. Terra implements, a fresh independent Terra
> reviews the task, Terra applies accepted fixes, and Luna records the result. Sol is reserved
> for the final complete-branch review.

## 0. Synchronize the implementation baseline

**Files:** none in the repository.

**Objective:** make `codex/phase-1-close-all-gaps` descend from merged PR #50,
`8cd7696fdcf34f6253fb11f9e110f6632bc872de`, before any production edit.

**Actions:**

1. Confirm the isolated worktree and branch, and record its clean status.
2. Fetch or otherwise obtain the merged commit from an environment with GitHub access.
3. Verify the commit identity, PR #50 merge ancestry, and that the closeout branch is based on it.
4. Preserve the unrelated dirty root checkout; do not copy its edits into this worktree.

**Exit evidence:** `git status --short --branch`, `git merge-base`, and the ancestry check all
show the merged PR #50 baseline. If the commit cannot be obtained, stop before implementation
and report the network/repository-state blocker.

## 1. Build the finding ledger and baseline record

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `docs/phase-1-audit.md`
- `docs/status.md`
- `docs/roadmap.md`
- `docs/phase-1-closeout-ledger.md`

**Objective:** make every remaining Phase 1 finding mechanically traceable to an implementation,
regression, or evidence artifact.

**Implementation:** record the synchronized baseline, exact finding IDs, current state, controlling
source, dependency, allowed scope, acceptance assertion, and final evidence location. Include
COR-01 through COR-04, CONC-01 and CONC-03, DUR-01 through DUR-03, IDX-01 and IDX-02, ARCH-01
through ARCH-05, VER-01 through VER-06, and PERF-01 through PERF-05. Mark PERF-06/07, IDX-03,
ARCH-06/07/08, DUR-06/07/08, and later compaction/vacuum/retention/orphan-growth work as
explicitly deferred rather than silently omitting them.

**Checks:** stale-link and stale-claim scans, credential scan, `git diff --check`, and a review
that no document claims stronger guarantees than the implementation or evidence supports.

## 2. Complete named loom models and reproducible invocation

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `crates/txn/src/dataset.rs`
- `crates/txn/src/row_id.rs`
- `crates/txn/src/snapshot.rs`
- `crates/index/src/live_set_cache.rs`
- `crates/index/tests/*` only where a model fixture is required
- `crates/txn/tests/*` only where a model fixture is required
- `.github/workflows/ci.yml`
- `docs/phase-1-closeout-ledger.md`

**Objective:** make every named model runnable with the repository’s crate-scoped recipe and
close the seventh transaction model without weakening assertions or using workspace-wide
`RUSTFLAGS=--cfg loom`.

**Implementation:** first reproduce the incomplete model and identify the actual resource or
fixture cause. Add the smallest test-only/model-only correction, or a production correction only
when the model demonstrates a real interleaving defect. Preserve typed conflicts, immutable
snapshots, row/index atomicity, durable publication, and the single-process concurrency boundary.
Add an exact CI invocation that builds the relevant crate and runs the produced test binary.

**Checks:** all named transaction, cache, and index models; normal targeted tests; format,
clippy for affected crates; and fresh output identifying every model and its result.

## 3. Close checkpoint, fast-chaos, and thorough-chaos gates

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `crates/storage/tests/chaos_checkpoint_actually_aborts.rs`
- `tests/sim/tests/chaos.rs`
- `tests/sim/src/*` only for harness correctness
- `crates/chaos-worker/src/*` only for harness correctness
- `.github/workflows/ci.yml`
- `docs/phase-1-closeout-ledger.md`

**Objective:** produce explicit crash/durability evidence, including the required full 2,000-seed
thorough tier.

**Implementation:** retain the checkpoint-abort assertion, make fast chaos deterministic and
report its seed range, and make the thorough tier execute and report `2000/2000` successful
seeds. A timeout, partial run, or skipped test is an incomplete gate. Harness changes must not
turn failures into passes or suppress child exit status. Keep crash recovery, manifest
publication, row/vector identity, and non-reuse assertions intact.

**Checks:** checkpoint-abort, fast chaos, thorough chaos with the exact seed count, targeted
storage/txn tests, and CI artifact/log retention on failure.

## 4. Make fuzz and provenance verification reproducible

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `fuzz/Cargo.toml`
- `fuzz/fuzz_targets/*`
- `.github/workflows/ci.yml`
- `rust-toolchain.toml` only when required to pin the existing supported toolchain
- `docs/phase-1-closeout-ledger.md`

**Objective:** close VER-04 and VER-05 with buildable, discoverable targets and focused recovery
parser smoke coverage, while making CI provenance auditable.

**Implementation:** ensure each declared fuzz target builds in the separate fuzz workspace and
that the expected recovery/manifest parsers have deterministic smoke inputs. Pin action and
toolchain provenance to the project’s approved versions without adding dependencies. Record
runner OS/architecture, Rust/Cargo versions, lockfile hash, and command lines in retained CI
artifacts.

**Checks:** fuzz target discovery, `cargo fuzz build` or the repository-equivalent build gate,
focused parser smoke tests, workflow YAML validation, stale-link/credential scans, and
`git diff --check`.

## 5. Establish reproducible PERF-01 and PERF-02 evidence

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `bench/benches/manifest_growth_bench.rs`
- `bench/cloud-performance/run.sh`
- `bench/cloud-performance/summarize.py`
- `bench/cloud-performance/README.md`
- `.github/workflows/cloud-performance-before-after.yml`
- `docs/phase-1-performance.md`
- `docs/phase-1-closeout-ledger.md`

**Objective:** produce comparable, repeatable manifest-growth and timing evidence for the current
manifest-listed immutable-segment path.

**Implementation:** keep deterministic synthetic inputs, warmup exclusion, repeated measurements,
median/p95/variance, raw logs, revision and lockfile hashes, runner metadata, and explicit
failure recording. Measure retained-history points through the supported bounded envelope and
separate real-fixture evidence from synthetic evidence. Compare like-for-like current-path
workloads only; do not compare the retired direct `HnswIndex` path with `Dataset`/`Snapshot`.
Document bytes and timings as an observed operating envelope, not an asymptotic guarantee.

**Checks:** benchmark smoke run, parser validation, manifest points at 1/10/20/40/80/160
commits, direct before/after comparison from the synchronized revisions, and artifact integrity.

## 6. Add recovery-byte accounting for PERF-03

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `crates/storage/src/datafile.rs`
- `crates/storage/src/manifest.rs`
- `crates/storage/src/lib.rs` only for an existing public diagnostic boundary
- `crates/txn/src/dataset.rs`
- `crates/storage/tests/*` and `crates/txn/tests/*` for regression/evidence tests
- `bench/benches/lifecycle_bench.rs`
- `bench/cloud-performance/run.sh`
- `bench/cloud-performance/summarize.py`
- `docs/phase-1-performance.md`
- `docs/phase-1-closeout-ledger.md`

**Objective:** measure recovery work directly rather than using process RSS or wall-clock time
as a proxy.

**Implementation:** add typed, side-effect-free recovery accounting at the narrowest existing
diagnostic boundary. Count manifest/data/index bytes actually inspected or loaded during reopen,
and expose the measurement to tests/benchmarks without changing normal commit semantics. Add
tests for empty, small, retained-history, and bounded larger datasets, plus ingest/commit,
reopen/recovery, and concurrent-commit measurements. Do not claim a universal bound beyond the
measured supported envelope.

**Checks:** targeted storage/txn tests, lifecycle benchmark with raw outputs, recovery-byte
monotonicity/consistency assertions, workspace build/test/clippy for affected crates, and
evidence review.

## 7. Close PERF-04 with a measured supported segment envelope

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `bench/benches/segment_recall_bench.rs`
- `bench/cloud-performance/run.sh`
- `bench/cloud-performance/summarize.py`
- `bench/cloud-performance/README.md`
- `docs/phase-1-performance.md`
- `docs/phase-1-closeout-ledger.md`
- a production index/txn file only if the measured contract requires a typed operational guard

**Objective:** define and verify the supported K=1 through K=max segment envelope for recall,
latency, throughput, and filtered search.

**Implementation:** measure the current immutable segment path at the exact supported points and
record dataset shape, query count, warmup, repetitions, recall target, and filtered/unfiltered
behavior. Set a documented supported maximum when evidence establishes one. Add a typed
operational rejection/guard only if serving beyond that maximum is otherwise an undocumented
unsafe operating mode; tests must cover the boundary and rejected request. Do not change HNSW
parameters or add a monolithic comparison.

**Checks:** benchmark smoke and full bounded sweep, targeted index/txn tests, boundary regression
if a guard is added, and independent review of whether the evidence actually supports the stated
envelope.

## 8. Close PERF-05 with retained-footprint accounting

**Owner:** Terra implementation, fresh Terra review.

**Allowed files:**

- `crates/txn/src/live_set_cache.rs`
- `crates/txn/src/snapshot.rs`
- `crates/txn/src/dataset.rs`
- `crates/txn/tests/*` for residency/release regressions
- `bench/benches/lifecycle_bench.rs`
- `bench/cloud-performance/run.sh`
- `bench/cloud-performance/summarize.py`
- `docs/phase-1-performance.md`
- `docs/phase-1-closeout-ledger.md`

**Objective:** measure pinned snapshot/cache residency and retained manifest/data/segment
footprint directly, with RSS retained only as supplemental runner evidence.

**Implementation:** add or use narrow diagnostic accounting for retained snapshots, cache entries,
and manifest-listed bytes. Test pinning and release at 0/1/4/16/64 snapshots and bounded
retained-history points. Ensure eviction/release does not violate snapshot immutability or row/
index identity. Keep process-wide RSS labeled approximate.

**Checks:** targeted cache/snapshot tests, bounded residency benchmark, repeated measurements,
and review of the accounting against actual retained resources.

## 9. Consolidate canonical status after evidence exists

**Owner:** Luna consolidation, Terra documentation review.

**Allowed files:**

- `docs/phase-1-audit.md`
- `docs/status.md`
- `docs/roadmap.md`
- `docs/phase-1-performance.md`
- `docs/phase-1-closeout-ledger.md`

**Objective:** update the canonical record without overstating guarantees.

**Implementation:** mark each in-scope finding as implementation/regression-covered or
evidence-complete within its named bounded scope. Preserve explicit limitations: single process,
single node, snapshot-read/write-write-OCC API ceiling, bounded segment/history envelope, and
any named platform/fixture limitations. Keep later-phase findings deferred.

**Checks:** stale-link, stale-claim, credential, and exact-scope scans; `git diff --check`; and
manual comparison of every ledger row with fresh test/benchmark artifacts.

## 10. Final verification, Terra fixes, and Sol review

Run fresh verification on the complete branch:

- `cargo fmt --check`
- `cargo check --workspace`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo doc --workspace --no-deps`
- `cargo deny check bans sources advisories`
- metadata and parallel-insert checks
- every relevant loom model with crate-scoped builds
- checkpoint-abort, fast chaos, and full `2000/2000` thorough chaos
- fuzz discovery/build and parser smoke tests
- benchmark and artifact validation for PERF-01 through PERF-05
- stale-link/stale-claim/credential scans and `git diff --check`

Then obtain one independent final Sol review of the entire diff. Any accepted finding returns to
Terra for a focused fix and reruns the affected checks; Sol reviews the resulting complete diff
again if the fix changes semantics. Do not declare Phase 1 closed while a P0/P1 blocker lacks
fresh implementation, regression, or required evidence.

## 11. Publish and clean up only after remote confirmation

Stage only intentional files, inspect the staged diff, create focused commits, push
`codex/phase-1-close-all-gaps`, open a ready-for-review PR, wait for required checks, and merge
only after the final Sol review and all required checks pass. Confirm merged `main` remotely,
then remove the isolated worktree and branch. Report the merged PR, exact verification output,
bounded performance evidence, and every deferred finding.
