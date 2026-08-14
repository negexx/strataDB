# Task 2 execution report — versioned schema catalog and migrations

**Date:** 2026-08-14
**Branch:** `codex/agent-production-readiness`
**Assigned brief:** `.superpowers/sdd/agent-production-readiness-plan/briefs/task-2-brief.md`

## Scope reviewed

- `crates/storage/src/error.rs`
- `crates/storage/src/lib.rs`
- `crates/storage/src/manifest.rs`
- `crates/storage/src/schema.rs`
- `crates/txn/src/dataset.rs`
- `crates/txn/tests/schema_migrations.rs`
- `docs/phase-3-verification-report.md`

The pre-edit worktree contained only the six Task 2 source/test changes above. Task 1 was already
committed (`acb5f99..e536430`) and was not modified. The finisher’s only source edit was the
Rustfmt-required layout change in the Task 2 recovery test.

## Delivered behavior

- Manifests persist `schema_version`; missing field values in otherwise supported envelope manifests
  default unambiguously to catalog v1, while unknown versions return `StorageError::UnknownSchemaVersion`.
- `SchemaMigration::AddNullableColumn` is the only forward migration. It is named, deterministic,
  validates source/target versions, requires a new nullable column, and rejects unsupported,
  incompatible, reverse, stale, and lossy requests with typed errors.
- `Dataset::migrate_schema` writes replacement row files and copied immutable segment objects before
  publishing a new manifest. It preserves row IDs, tombstones, segment metadata, and old snapshots.
- Pre-migration transactions are rejected with `SchemaVersionChanged` instead of publishing v1 rows
  into a v2 manifest.
- Focused tests cover catalog round-trip/defaulting, unknown-version rejection, forward migration,
  typed rejection paths, old snapshots, reopen, stale transactions, and recovery after an injected
  pre-publication transaction failure.

## Fresh verification

Successful Cargo commands used a process-local MSVC setup: requested linker directory first on
`PATH`, MSVC x64 libraries, and Windows SDK 10.0.26100.0 UCRT/UM x64 libraries on `LIB`.

| Command | Exit | Result |
|---|---:|---|
| `cargo test -p strata-txn --no-default-features --test schema_migrations` | 0 | 3 passed, 0 failed |
| `cargo test -p strata-storage schema_catalog_version_round_trips_and_legacy_manifests_default_to_v1` | 0 | 1 passed, 0 failed |
| `cargo test -p strata-storage recovery_rejects_an_unknown_schema_catalog_version` | 0 | 1 passed, 0 failed |
| `cargo test -p strata-txn --no-default-features migration_after_a_failed_manifest_commit_uses_only_the_complete_manifest` | 0 | 1 passed, 0 failed |
| `cargo test -p strata-txn --no-default-features --features parallel-insert --lib --tests` | 0 | 296 passed, 0 failed |
| `cargo clippy -p strata-storage --all-targets -- -D warnings` | 0 | no warnings |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | 0 | no warnings |
| `cargo fmt` | 0 | formatted assigned test only |
| `cargo fmt --check` | 0 | clean after formatting |
| `git diff --check` | 0 | clean before staging |

Two preliminary broader-test invocations failed before Rust tests started because the shell did not
inherit required `LIB` entries: `LNK1104` for `msvcrt.lib`, then `LNK1181` for `kernel32.lib`.
Adding the explicit local SDK library paths resolved the linker setup; no repository configuration
was changed.

## TDD evidence and concern

The assigned implementation and focused test were already present when this finisher started. The
available handoff stated that the focused test already passed, but did not include a captured red
run. I did not delete and recreate the existing implementation solely to manufacture one. Fresh
green verification is recorded above. The direct migration failure coverage is represented by typed
pre-publication validation failures and by the existing transaction manifest-failure hook proving
that migration/reopen ignores incomplete prepared objects; the hook does not inject failure into
`Dataset::migrate_schema` itself.

No dependencies were added. No pushes, branches, or pull requests were made. The authorized commit
follows this recorded fresh verification.

## Fix round 1 (2026-08-14)

### Review findings addressed

- Added `migration_failure_before_publication_reopens_the_prior_complete_manifest` in
  `crates/txn/tests/schema_migrations.rs`. It uses the feature-gated,
  migration-specific `test_support::fail_before_migration_manifest_publication`
  seam after replacement objects are validated but before manifest publication,
  then reopens and proves the v1 complete manifest remains selected.
- Added `dataset::loom_tests::migration_exclusivity_rejects_a_stale_schema_commit_or_migrates_its_published_rows`.
  It models the actual `LifecycleCoordinator` preparation/exclusive guards with
  the schema-version publication boundary: a v1 transaction can publish before
  migration, or after v2 it observes the stale version and cannot publish.
- Re-established red evidence from parent `e536430` in detached temporary
  worktree `.worktrees/task-2-fix-round-1-red` by materializing
  `crates/txn/tests/schema_migrations.rs`. With the explicit MSVC linker and
  library paths, Cargo exited 1 after compiling the test and reported 19
  expected missing Task 2 symbols, including the new migration-fault hook.
  A prefix-only preliminary launch exited 1 at `LNK1104` (`msvcrt.lib`) before
  Rust compilation; it was environmental and not counted as red evidence.

### Fresh verification

All successful Cargo commands used the requested MSVC linker directory first
on `PATH` plus the MSVC x64 and Windows SDK 10.0.26100.0 UCRT/UM x64 library
directories on `LIB`.

| Command | Exit | Result |
|---|---:|---|
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_failure_before_publication_reopens_the_prior_complete_manifest -- --exact` | 0 | 1 passed, 0 failed |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations` | 0 | 4 passed, 0 failed |
| `cargo test -p strata-txn --no-default-features --features parallel-insert --lib --tests` | 0 | 296 passed, 0 failed |
| `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`, then the produced binary with `--exact dataset::loom_tests::migration_exclusivity_rejects_a_stale_schema_commit_or_migrates_its_published_rows --test-threads=1` | 0 | build and model passed; 1 model passed, 232 filtered; localized `linker_messages` warning only |
| `cargo clippy -p strata-txn --all-targets --features test-fault-injection -- -D warnings` | 0 | no warnings |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | 0 | no warnings |
| `cargo fmt --check` | 0 | clean |

No dependency, concurrency-scope, isolation, row/vector publication, or immutable-segment design
change was made. The initial full-`Dataset` loom attempt aborted with Windows `0xC00000FD` before
schedule output because migration's recursive Arrow/filesystem path exceeds loom's coroutine-stack
budget; the accepted model was narrowed to the real lifecycle/catalog interleaving and rerun green.
