# Phase 1 Complete-All-Blockers Implementation Plan

> **For agentic workers:** Use the Luna → Terra task workflow. Every behavior change requires TDD;
> every evidence task must retain raw output and exact provenance. Terra reviews task diffs; Sol reviews
> only the complete branch at the final gate. Do not recreate the retired agent-planning tree.

**Goal:** Close every remaining Phase 1 blocker that can be closed inside the one-process/shared-
`Dataset` boundary, with a full pinned-fixture matrix and fresh native verification, while keeping
unsupported lifecycle and universal-guarantee claims explicit.

**Architecture:** Preserve the manifest-listed immutable segment path and current API boundary. Add no
arbitrary global performance cap. Extend only the evidence orchestration needed to run the complete
100K-row fixture and current bounded matrices; add a typed operational guard only if an existing
invariant requires it and measured evidence supports the exact condition.

**Tech Stack:** Rust 1.90, Cargo workspace, GitHub Actions, Bash, Python JSONL/CSV summarizer, Criterion,
loom, chaos-worker, and cargo-fuzz.

## Global constraints

- Work only in `codex/phase-1-complete-all-blockers`, never the dirty local `main` checkout.
- Keep the supported scope at one process using one shared `Dataset` handle.
- Never reintroduce the retired direct `HnswIndex` benchmark path.
- Do not add dependencies, compaction, vacuum, SQL, or new ANN families.
- Every Rust concurrency/index behavior change gets a targeted loom model and normal regression tests.
- Every evidence result names revision, runner, toolchain, filesystem, cache policy, input, seed/hash,
  warmup/repetition policy, command, and artifact.
- Phase 1 remains Partial/blocked until the final acceptance matrix is fully green.

## Task 1: Full 100K-row pinned fixture comparison

**Files:**

- Modify: `.github/workflows/cloud-performance-before-after.yml`
- Modify: `bench/cloud-performance/run.sh` only if input identity or full-row validation needs a fix
- Modify: `bench/cloud-performance/summarize.py` and `bench/cloud-performance/test_summarize.py` only
  for a demonstrated validation gap
- Modify: `bench/cloud-performance/README.md`
- Test: workflow shell/config validation and the existing Python summarizer suite

**Acceptance:** A manually dispatched Ubuntu run loads exactly 100,000 rows from the pinned fixture
for both revisions, emits one identical input hash per revision, executes K=1…64 filtered and
unfiltered searches with fixed queries and repetitions, and validates the complete artifact. Synthetic
matrix behavior remains unchanged.

## Task 2: Current bounded performance envelope

**Files:**

- Modify: `bench/cloud-performance/run.sh` only for disjoint scale/configuration support
- Modify: `docs/phase-1-performance.md`, `docs/phase-1-audit.md`,
  `docs/phase-1-closeout-ledger.md`, `docs/status.md`, and `docs/roadmap.md`

**Acceptance:** Fresh artifacts cover manifest growth, recovery accounting, segment fan-out, and
snapshot residency at named safe scales. Any unsupported maximum, full lifecycle bound, RSS bound,
or statistics/projection isolation remains explicitly open; no arbitrary cap is introduced.

## Task 3: Exact native verification and provenance

**Files:**

- Modify `.github/workflows/ci.yml` only if a current exact gate is missing or falsely reports success
- Modify canonical evidence docs after the manual run succeeds

**Acceptance:** A manual CI run on the exact branch head executes the named transaction/cache/index
loom models, checkpoint gate, fast chaos, thorough chaos with `2000/2000`, fuzz builds/smokes, and
provenance artifact. A skip or timeout is not recorded as success.

## Task 4: Final verification and closeout

**Files:** all intentional files changed by Tasks 1–3 and canonical docs only.

**Acceptance:** `cargo fmt --check`, `cargo check --workspace`, `cargo test --workspace`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo doc --workspace --no-deps`,
`cargo deny check bans sources advisories`, relevant loom/chaos/fuzz gates, stale-link/claim/credential
scans, and `git diff --check` pass. Fresh Terra reviews approve each task; final Sol approves the
complete diff; PR checks pass; remote main confirms the merge before cleanup.
