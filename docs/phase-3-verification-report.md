# Phase 3 verification report

**Run date:** 2026-08-13
**Execution branch:** historical `codex/phase-3-vacuum`
**Verification head:** `7be77d5` (final pre-merge PR head)
**Merged implementation branch:** `main` at `65449a9` (PR #68)
**Documentation baseline:** Evidence was reviewed against baseline `main` `66408bd` before this
uncommitted documentation closeout. Final exact-head provenance must be the commit containing these
docs.

## Fresh cloud provenance

Canonical final exact-head GitHub Actions run
[31714285971](https://github.com/negexx/strataDB/actions/runs/31714285971) completed successfully
for final verification head `7be77d5`. The controller reported all
workflow jobs successful:

- `ci`;
- `phase-0-foundation-evidence`;
- `phase-3-lifecycle-evidence`;
- `fuzz-and-provenance`;
- `thorough-chaos`; and
- `windows-directory-durability`.

This is the canonical final exact-head evidence for the Task 1–6 remediation aggregate, including
manifest-publication timestamps; Task 6 exact-key protection for recovery-recognized numeric manifest
keys, with duplicate padded/unpadded aliases rejected fail-closed; constrained vacuum cleanup; lifecycle
inventory compatibility; and compaction crash/reopen coverage. It also covers the unpadded current-manifest
vacuum regression: both the protected row file and vector segment survive, and after reopening the
dataset the vector search returns physical row ID `0`. The active-snapshot compaction regression now
correctly asserts that two unprotected superseded objects are reclaimed while the historical snapshot's
row file and vector segment remain readable.

## Prior cloud provenance

The earlier exact-head GitHub Actions run
[31701969422](https://github.com/negexx/strataDB/actions/runs/31701969422) completed successfully
for remediation head `105c24f74dc68d8c4c552bfa9d4b63b1a4c79a2c`, with the same controller job set
listed above. It remains retained provenance for the earlier Task 1–4 remediation aggregate.

The still earlier exact-head GitHub Actions run
[31698710652](https://github.com/negexx/strataDB/actions/runs/31698710652) completed successfully
for remediation head `0141355d1261ee20d8128cbc11c38b23e83045c9`, with the same controller job set
listed above. It remains retained provenance for the earlier Task 1–4 remediation aggregate.

## Fresh local Windows verification

The 2026-08-13 native x64 MSVC environment was verified by explicit `PATH` setup and
`where.exe cl`/`where.exe link`. The seven targeted lifecycle test binaries passed:

| Command | Result |
|---|---|
| `cargo test -p strata-txn --no-default-features --test lifecycle_inventory --test retention_plan --test retention_age --test manifest_retention_executor --test compaction --test vacuum --test maintenance` | Exit 0; all seven targeted lifecycle test binaries passed. |
| `cargo test --workspace --no-default-features` | Exit 0. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 0. |
| `cargo fmt --check` | Exit 0. |
| `git diff --check` | Exit 0. |

These native Windows results include the workspace test suite, lint, format, and diff checks, but
make no local loom claim. The canonical exact-head cloud run above remains authoritative for the
loom and broader CI gates.

## Claim boundary

**Phase 3 is implemented within named bounds.** Its lifecycle operations apply only to one process
using one shared `Dataset` handle. Compaction preserves active snapshots, age retention protects
legacy zero timestamps, vacuum deletes only recognized unprotected objects, and
`storage_bound_met` is one completed maintenance run's final inventory observation. Active snapshots,
protected history, or unknown objects can prevent that observation from meeting a requested bound.
This evidence does not establish serializability, cross-process coordination, universal power-loss
durability, or atomic/continuing storage-bound enforcement.

## Task 1 fix round 2 verification (2026-08-14)

This round re-established the Task 1 transaction-read-view red/green evidence without changing
current-branch source, Git history, dependencies, or the transaction contract. Every command put
the requested linker directory first on `PATH`:

```text
C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\bin\Hostx64\x64
```

The invoking Windows shell had no `LIB` or `INCLUDE` environment variables. `link.exe` was found
through the required prefix, but a prefix-only preliminary invocation stopped at `LNK1181` for
`kernel32.lib`, before Rust could compile the test. For each recorded Cargo invocation, the existing
`VsDevCmd.bat -arch=x64 -host_arch=x64` environment was initialized and the installed MSVC x64
library directory (`...\VC\Tools\MSVC\14.52.36615\lib\x64`) was prepended to `LIB`; the required
linker directory was then prepended again to `PATH`. This was process-local environment setup, not
a repository change.

| Command | Result |
|---|---|
| In a detached temporary worktree at `acb5f99`, after copying the current `crates/txn/tests/transaction_read_view.rs`: `cargo test -p strata-txn --no-default-features --test transaction_read_view` | Exit 101, as expected for the red phase. Compilation reached the copied test and reported 11 E0599 errors: missing `Transaction::{scan_query, group_by_query, lookup_row, vector_search_query}` methods and missing `QueryExecutionError::UnsupportedTransactionRead`. The temporary worktree was then removed. |
| `cargo test -p strata-txn --no-default-features --test transaction_read_view` | Exit 0: 6 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features --features parallel-insert --lib --tests` | Exit 0: 292 unit/integration tests passed, 0 failed; doctests were excluded. |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | Exit 0; no warnings. |
| `cargo rustc -p strata-txn --lib --profile test --message-format=json -- --cfg loom`, followed by the produced test binary with `--exact dataset::loom_tests::transaction_read_overlay_stays_private_while_disjoint_and_contested_writes_commit --test-threads=1` | Build exit 0; model exit 0: 1 passed, 0 failed, 230 filtered out. |

The loom result is a targeted local model result, not a replacement for the broader cloud loom
provenance above. This entry records verification evidence only and makes no whole-feature or
broader Phase 3 completion claim.

## Task 2 schema-migration verification (2026-08-14)

Task 2 adds the versioned durable schema catalog and the single explicit
`add_nullable_column` migration. The migration rewrites row objects, copies immutable vector
segments to new locations, then publishes their references and the catalog version in one
manifest. Existing snapshots remain bound to their captured manifest and schema; recovery rejects
unknown catalog versions and selects only a fully published manifest.

All Cargo commands below ran in a process-local MSVC environment with the requested linker
directory first on `PATH`, plus the MSVC and Windows SDK x64 library directories on `LIB`. This is
environment setup only, not a repository change.

| Command | Result |
|---|---|
| `cargo test -p strata-txn --no-default-features --test schema_migrations` | Exit 0: 3 passed, 0 failed. |
| `cargo test -p strata-storage schema_catalog_version_round_trips_and_legacy_manifests_default_to_v1` | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-storage recovery_rejects_an_unknown_schema_catalog_version` | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features migration_after_a_failed_manifest_commit_uses_only_the_complete_manifest` | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features --features parallel-insert --lib --tests` | Exit 0: 296 passed, 0 failed. |
| `cargo clippy -p strata-storage --all-targets -- -D warnings` | Exit 0; no warnings. |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | Exit 0; no warnings. |
| `cargo fmt --check` after `cargo fmt` | Exit 0. |

The first two broader test-launch attempts exited 1 before a Rust test ran: first `LNK1104`
(`msvcrt.lib`), then `LNK1181` (`kernel32.lib`). Supplying the explicit Windows SDK x64 `LIB`
entries fixed the machine-local linker configuration; the successful commands above are the fresh
verification evidence. This Task 2 entry is scoped to the documented single-process/shared-handle
and local-filesystem boundary. It does not claim distributed coordination, serializability, or
universal power-loss durability.
