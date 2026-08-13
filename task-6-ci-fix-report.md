# Task 6 CI fix report

## Scope

- Changed only `crates/txn/tests/compaction.rs` and this report.
- The implementation remains unchanged: commit `6a7607c` already reads active-snapshot manifests through the exact inventory key.
- Pre-existing untracked files were preserved: `docs/phase-3-closeout-remediation-plan.md`, `task-5-fix-report.md`, and `task-6-fix-report.md`.

## Root-cause evidence

GitHub Actions run `31709183628` failed only
`compaction_reads_an_unpadded_active_snapshot_manifest_key_and_preserves_its_objects` at
`crates/txn/tests/compaction.rs:160`, where `report.objects_deleted` was `2` rather than `0`.

The existing exact-key path is correct. `Dataset::compact` indexes `_versions/` with
`retention::index_manifest_objects`, retrieves each live snapshot's actual listed key, and calls
`read_manifest_at_key_with_byte_count` with that key. This matches the recovery and retention-plan
tests for unpadded numeric manifest names.

In the fixture, version 1 is an active snapshot and owns one Arrow file plus one segment; version 2
has another Arrow file plus segment but no lease. Compaction publishes version 3 and protects the
version-1 objects. It correctly reclaims the two unleased version-2 objects, so `objects_deleted == 2`.
The prior `== 0` assertion was therefore wrong; changing deletion behavior would retain superseded
objects and weaken compaction reclamation semantics.

## Regression adjustment

- Corrected the expected deletion count to the hand-derived value `2`.
- Extended the active-snapshot on-disk protection assertion from row files to both row files and
  immutable vector segments. The existing snapshot scan and vector-search assertions remain.

## Verification

- Red evidence: GitHub Actions run `31709183628` recorded the original test failure with `left: 2`,
  `right: 0`; its other six compaction tests passed.
- `cargo test -p strata-txn --no-default-features --test compaction
  compaction_reads_an_unpadded_active_snapshot_manifest_key_and_preserves_its_objects -- --exact`
  — exit 1 before test execution because this Windows host lacks MSVC `link.exe`.
- `cargo check -p strata-txn --no-default-features --test compaction` — exit 0.
- `cargo clippy -p strata-txn --no-default-features --test compaction -- -D warnings` — exit 0.
- `cargo fmt --check` — exit 0.
- `git diff --check` — exit 0.
- `git diff --cached --check` and full staged-diff inspection — exit 0.

This is a test-only correction, so no production implementation or behavior changed after the
CI red result. The green type/lint checks establish the corrected regression compiles cleanly;
an executable green result requires a host or CI runner with the MSVC linker.
