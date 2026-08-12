# Phase 1 audit findings implementation plan

> This is an audit disposition plan. It is not approval to implement fixes.

**Goal:** Close only confirmed documentation/verification gaps and preserve the existing Phase 1
boundary while keeping unresolved runtime evidence explicit.

## Task 1: Refresh toolchain and baseline provenance

Files: `AGENTS.md`, current phase plans/status references, CI artifact links.

Update active guidance from Rust 1.90 to the pinned 1.97.1 toolchain where it is not historical.
Record the exact audit branch revision and link fresh CI artifacts. Verify with `rustup show`,
`cargo metadata`, and stale-reference scans.

## Task 2: Provide a runnable native verification environment

Files: no repository code change required; CI/provenance only unless the approved environment needs a
workflow correction.

Run workspace tests, transaction feature tests, exact transaction/cache/index loom models, checkpoint
and fast/thorough chaos, fuzz smoke, and benchmarks on a host with a working linker. Retain logs with
revision, toolchain, OS, lockfile hash, exit status, and command. Do not mark the gate complete from
compilation-only output.

## Task 3: Fix rustdoc warnings

Files: `crates/query/src/predicate_key.rs`, `crates/storage/src/encoding.rs`.

Remove the redundant explicit intra-doc target and replace the public link to the private constant
with public wording or a public documentation target. Run `cargo doc --workspace --no-deps` and
`cargo fmt --check`.

## Task 4: Clarify current documentation claims

Files: `docs/status.md`, `docs/architecture.md`, `docs/phase-1-audit.md`, `docs/phase-1-performance.md`.

Separate historical remote evidence from exact-head evidence, reconcile “partial” client wording,
and preserve explicit limits for warm-cache, host-local, non-RSS, and non-universal measurements.
Verify with link/stale-reference scans and `git diff --check`.

## Task 5: Behavior-preserving refactoring review

Files: `crates/txn/src/dataset.rs` only if approved after Tasks 1–4.

Split commit/recovery orchestration only if a reviewer can preserve the exact ordering: prepare and
fsync before publication lock, conflict check before manifest mutation, manifest durability before
in-memory visibility, and no index mutation outside manifest publication. Add no dependencies and
change no format. Run targeted red/green tests, normal tests, and loom models.

## Acceptance

Phase 1 remains Partial/blocked until the runtime and exact-head evidence gates are fresh and green.
No task authorizes stronger isolation, compaction, on-disk format changes,
or a universal durability/performance claim.
