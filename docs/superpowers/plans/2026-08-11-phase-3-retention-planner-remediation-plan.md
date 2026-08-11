# Phase 3 Retention Planner Remediation Plan

> Remediate the independent Terra and Sol review findings without broadening Strata beyond
> read-only, single-process retention evidence. Do not add deletion, cleanup, compaction, or
> cross-process coordination in this slice.

## Global constraints

- Preserve the existing `Dataset::snapshot() -> Arc<Snapshot>` API and one shared-`Dataset`
  coordination boundary.
- Retention candidates are advisory evidence, never deletion authorization.
- Unknown, temporary, unsafe, duplicate, missing, malformed, or in-flight/preparation objects
  must not become eligible candidates.
- Candidate data provenance is the reachable-key union from validated older manifests outside the
  retained set, intersected with the deduplicated `data/` inventory.
- Reuse one canonical manifest decoder for both current-manifest recovery and specific-version
  reads; preserve exact selected keys and checked byte conversion.
- `commit_lock` serializes publication only; it does not protect files prepared before lock
  acquisition. Future cleanup requires preparation leases, a lifecycle epoch, or equivalent
  coordination spanning preparation through publication/abort.
- Every behavior change requires a failing test before production code, then focused green tests.
- Every interleaving-sensitive transaction change needs a targeted loom model and normal tests;
  the exact loom model must be registered in CI using the crate-scoped recipe.
- Preserve unrelated user-requested `AGENTS.md` and `rust-toolchain.toml` changes; do not modify
  them as part of this remediation.

## Task 1: Conservative provenance and canonical manifest decoding

Files: `crates/txn/src/retention.rs`, `crates/storage/src/manifest.rs`,
`crates/txn/tests/retention_plan.rs`, and focused storage/txn tests.

- Add red tests proving arbitrary unknown and temporary data files are excluded, while data keys
  referenced by validated older manifests become candidates only outside retained reachability.
- Add red regression coverage for duplicate inventory and malformed candidate-producing manifests.
- Refactor the storage reader to share one internal parse/validate/byte-count helper between
  `read_current_with_byte_count` and `read_manifest_with_byte_count`; preserve the exact key
  chosen by current-manifest recovery and use checked byte conversion.
- Compute eligible data provenance from validated older manifests minus retained keys, validate
  missing reachable objects fail closed, and use `snapshot.version` as authoritative.

## Task 2: Lease and publication concurrency evidence

Files: `crates/txn/src/retention.rs`, `crates/txn/tests/retention_plan.rs`,
`.github/workflows/ci.yml`, and only the narrowest transaction test hooks required.

- Add a loom model for registry registration, live-version scanning, final-drop races, and dead
  weak-entry pruning with explicit quiescent postconditions. Keep the model honest about the
  standard-library `Weak` implementation remaining outside loom instrumentation.
- Add the exact model name to the CI loom matrix/steps using the repository’s crate-scoped build
  and direct-binary execution recipe.
- Add a deterministic real-thread planner/commit test using the existing post-write,
  pre-`commit_lock` checkpoint: prepared files must not be candidates, and a captured plan must
  remain unchanged after commit publication.

## Task 3: Integration and pure-helper coverage

Files: `crates/txn/tests/retention_plan.rs`, `crates/txn/src/retention.rs`, and focused test
fixtures only.

- Match typed `TxnError` and nested storage errors instead of display substrings.
- Assert complete plan equality after a later commit, object key/byte maps before and after a
  planner call, and held snapshot row/segment reachability using synthetic non-cumulative
  manifests or a narrow test-only fixture seam.
- Cover unsafe inventory keys, checked count/byte overflow, duplicate listings, malformed or
  missing retained objects, duplicate reachable keys, and no mutation of durable state.

## Task 4: Documentation and plan reconciliation

Files: `docs/superpowers/specs/2026-08-11-phase-3-retention-planner-design.md`,
`docs/superpowers/plans/2026-08-11-phase-3-retention-planner-plan.md`, `docs/status.md`,
`docs/roadmap.md`, `docs/architecture.md`.

- Remove stale “implementation not started” wording.
- State in all relevant lifecycle docs that reacquiring `commit_lock` alone cannot make future
  deletion safe because prepared files are created before lock acquisition.
- Record preparation leases/lifecycle epochs/equivalent coordination as a prerequisite for any
  future cleanup executor, and keep this slice explicitly read-only.
- Reconcile architecture wording that still says no retention policy exists.

## Verification and review gate

- Run focused red/green tests per task, normal transaction tests, the exact retention loom model,
  workspace tests, clippy, format, and `git diff --check`.
- Dispatch a fresh Terra implementation worker and separate Terra reviewer for every task.
- Dispatch Sol for final complete-branch review before any commit/push/PR action.
