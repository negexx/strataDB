# Phase 3 Retention Planner Implementation Plan

> Execute this plan task-by-task in the current Strata workspace. Keep the change read-only and
> preserve the existing single-process/shared-`Dataset` boundary.

## Implementation status

Tasks 1--5 are implemented as the read-only retention-planning slice. This plan remains a record of
that implementation; lifecycle-executor remediation is pending. The delivered planner is advisory
only and does not grant deletion authority. In particular, reacquiring `commit_lock` cannot make a
previous plan authoritative: the lock serializes manifest publication but does not protect row or
segment files prepared before lock acquisition. A future executor requires preparation leases,
lifecycle epochs, or equivalent coordination that covers preparation through publication or abort.
It must remain within the supported single-process/shared-`Dataset` boundary unless a separate
cross-process design supersedes it.

## Objective

Implement the approved Phase 3 retention planner and snapshot lease registry from
`docs/superpowers/specs/2026-08-11-phase-3-retention-planner-design.md`. The result must report
retained and eligible manifests/data objects without deleting, rewriting, compacting, or
republishing anything.

## Constraints and invariants

- Do not add deletion or mutation to the planner; `Backend::delete` remains unused by this slice.
- `Dataset::snapshot() -> Arc<Snapshot>` keeps its existing public signature and semantics.
- A cloned historical `Arc<Snapshot>` keeps its lease alive; the registry stores only `Weak` leases.
- The current snapshot is always retained, and `keep_latest_versions == 0` is rejected.
- Retain the latest version window plus every active snapshot's manifest and the union of their
  referenced row/segment objects.
- Fail closed on malformed/unsafe/missing retained state, duplicate reachable keys, and arithmetic
  overflow. Never turn uncertain objects into deletion candidates.
- Preserve on-disk compatibility and existing typed error behavior.
- Do not alter unrelated dirty work. Before each implementation task, inspect the worktree and keep
  edits limited to the files named by that task.

## Task 1: Establish the retention model and lease ownership

Files:

- `crates/txn/src/retention.rs` (new)
- `crates/txn/src/lib.rs`
- `crates/txn/src/dataset.rs`
- `crates/txn/src/snapshot.rs`

Actions:

1. Add and export `RetentionPolicy`, `RetentionPlan`, and `RetentionCandidate` with documented
   deterministic ordering and advisory `eligible` semantics.
2. Add an internal `SnapshotLease` carrying a snapshot version and an
   `Arc`-owned `SnapshotLeaseRegistry` on `Dataset`. Ensure `Dataset` clones share the registry.
3. Add the lease to `Snapshot`; wire every production snapshot construction path (`create`, `open`,
   and commit publication) to register a lease. Update isolated/unit-test constructors to use an
   unregistered lease without requiring a `Dataset` registry.
4. Keep registry collection non-owning: prune dead `Weak` entries, upgrade live entries, include the
   captured current snapshot explicitly, then sort/deduplicate versions.
5. Add focused unit tests for registry registration, Arc clone behavior, last-drop removal, dead
   weak-entry pruning, and deterministic version deduplication.

Verification: compile the transaction crate and run only the new lease/unit tests before proceeding.

## Task 2: Reuse manifest validation and implement pure retention-set calculation

Files:

- `crates/txn/src/retention.rs`
- existing manifest/lifecycle/storage files only where a narrow helper is required, most likely
  `crates/txn/src/lifecycle.rs`, `crates/storage/src/manifest.rs`, or their current owners

Actions:

1. Identify the existing canonical manifest-envelope, checksum, version, path-safety, and listed
   object validation functions through the codebase knowledge graph before editing.
2. Add a narrow helper for reading a specific version manifest and obtaining its byte count only if
   the existing APIs cannot provide it. Reuse canonical parsing/checksum validation; do not create a
   second manifest format or alter `Backend` trait/delete semantics.
3. Implement pure helpers that derive the retained manifest version set, union referenced data keys,
   classify well-formed older manifests, and accumulate object counts/bytes with checked arithmetic.
4. Make helpers reject duplicate reachable keys and unsafe or malformed retained entries. Keep
   unknown/temporary objects out of eligible candidates unless the existing validation proves them
   safe and well-formed.
5. Add unit tests for latest-version windows, active historical versions, sorting/deduplication,
   duplicate keys, unsafe names, malformed manifests, and count/byte overflow.

Verification: run the pure helper tests and `cargo check -p strata-txn`.

## Task 3: Add `Dataset::retention_plan`

Files:

- `crates/txn/src/dataset.rs`
- `crates/txn/src/retention.rs`
- `crates/txn/src/error.rs` or the current `TxnError` owner

Actions:

1. Add the typed zero-policy error variant and preserve existing error conversion/display
   conventions.
2. Implement `Dataset::retention_plan(&self, RetentionPolicy) -> Result<RetentionPlan>`:
   - load the current `Arc<Snapshot>` once;
   - capture its observed version;
   - collect active lease versions without taking `commit_lock`;
   - list `_versions/` and `data/` through `LocalFs`;
   - parse safe manifest keys and read only manifests needed for the policy window or active leases;
   - retain the latest window, all active snapshot manifests, and all referenced row/segment keys;
   - return sorted/deduplicated manifest and data candidates plus checked counts/bytes.
3. Ensure storage/list/read failures and validation failures return immediately with no partial plan.
4. Keep the method observational: no `put`, `delete`, manifest publication, snapshot replacement, or
   commit-lock acquisition.

Verification: run `crates/txn/tests/retention_plan.rs` focused tests and inspect the diff for any
mutation or lock-order change.

## Task 4: Add integration coverage for lifecycle behavior

Files:

- `crates/txn/tests/retention_plan.rs` (new)
- supporting test utilities only if required

Tests:

1. Fresh dataset with policy one retains version zero and reports no eligible objects.
2. Multiple commits retain exactly the latest policy window and classify older manifests/data only
   when no live snapshot references them.
3. A held historical snapshot retains its manifest and all referenced row/segment objects; releasing
   it removes that version from the next plan.
4. Multiple clones of one snapshot deduplicate the active version and retain it until the last clone
   is dropped.
5. A commit after plan capture leaves the returned plan unchanged and exposes `observed_version` for
   staleness detection.
6. Zero policy, malformed retained manifests, unsafe keys, missing retained objects, and overflow
   return typed errors and never classify affected state as eligible.
7. The planner performs no filesystem mutation and leaves snapshots, manifests, data files, and
   segments unchanged.

Verification: run the focused integration test with the repository's native test feature flags.

## Task 5: Documentation and status update

Files:

- `docs/status.md`
- `docs/roadmap.md`

Actions:

1. After implementation tests pass, document the retention planner as an observational Phase 3
   slice, including its supported coordination boundary and explicit lack of deletion/compaction.
2. Keep Phase 3 marked partial/proposed for lifecycle execution work; do not claim cleanup,
   reclamation, or the full Phase 3 exit criterion is complete.
3. Record the new API and the fact that a future executor must capture and revalidate a plan under
   the shared commit lock. Also record that the lock serializes publication only: it cannot alone
   authorize deletion of row or segment files prepared before lock acquisition; an executor needs
   preparation-spanning coordination such as leases or lifecycle epochs.

## Task 6: Full verification and independent review checkpoint

Run fresh commands after all edits:

- `cargo fmt --check`
- `cargo check --workspace`
- `cargo test --workspace --no-default-features`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

Also run any transaction-specific test or loom command required by the touched interleaving paths,
following the exact crate-scoped recipe in `AGENTS.md`. Review the final diff and status, confirm no
unrelated files changed, and resolve every test/review finding before calling the implementation
complete. Do not commit, stage, push, or open a PR unless separately requested.

## Completion criteria

- The approved API and lease registry are implemented without changing snapshot handle semantics.
- All focused and required workspace checks pass with fresh output.
- The planner is demonstrably read-only and conservative under malformed, missing, unsafe, and
  concurrent state.
- Status and roadmap accurately describe this as a partial Phase 3 diagnostic/planning capability.
