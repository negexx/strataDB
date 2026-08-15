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

## Task 2 fix round 1 verification (2026-08-14)

This review-fix round adds a migration-only, pre-publication fault seam. The
fault runs only with `test-fault-injection`, after replacement row/immutable
segment objects have been validated and immediately before `commit_manifest_with`.
The new regression proves a returned typed I/O error leaves the v1 manifest as
the complete manifest selected after reopen. It also adds a crate-scoped loom
model of `migrate_schema` lifecycle exclusivity versus a stale v1 transaction
publication lease: the stale transaction can publish only before migration, or
otherwise observes v2 and is rejected by the existing typed schema-version
guard. The model intentionally scopes loom to that coordinator/catalog state
boundary; migration's Arrow/filesystem rewrite path is covered by the concrete
reopen test rather than loom's bounded coroutine stack.

Every Cargo command below put
`C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\bin\Hostx64\x64`
first on `PATH` and prepended the MSVC x64, Windows SDK UCRT x64, and Windows
SDK UM x64 directories to `LIB`; this was process-local environment setup.

| Command | Result |
|---|---|
| Detached temporary worktree at `e536430`, after materializing the current `crates/txn/tests/schema_migrations.rs`: `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_failure_before_publication_reopens_the_prior_complete_manifest -- --exact` | Exit 1, expected red phase. Rust compiled the test and reported 19 missing Task 2 symbols, including `SchemaMigration`, `Dataset::migrate_schema`, `Dataset::schema_version`, migration error variants, and the new `fail_before_migration_manifest_publication` hook. A prefix-only preliminary launch exited 1 at `LNK1104` for `msvcrt.lib` before Rust compilation; the explicit `LIB` setup above produced the recorded red result. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_failure_before_publication_reopens_the_prior_complete_manifest -- --exact` | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations` | Exit 0: 4 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features --features parallel-insert --lib --tests` | Exit 0: all 296 unit/integration tests passed, 0 failed. |
| `cargo rustc -p strata-txn --lib --profile test -- --cfg loom`, then the produced binary with `--exact dataset::loom_tests::migration_exclusivity_rejects_a_stale_schema_commit_or_migrates_its_published_rows --test-threads=1` | Build exit 0 (one localized `linker_messages` warning from `link.exe`); model exit 0: 1 passed, 0 failed, 232 filtered out. |
| `cargo clippy -p strata-txn --all-targets --features test-fault-injection -- -D warnings` | Exit 0; no warnings. |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | Exit 0; no warnings. |
| `cargo fmt --check` | Exit 0. |

The detached temporary worktree was used only for red evidence and was removed
after the final diff checks. These results remain limited to the
documented local, one-process/shared-`Dataset` boundary and do not claim
serializability, distributed coordination, or universal power-loss durability.

## Task 2 fix round 2 evidence (2026-08-15)

This is evidence-only. A detached temporary worktree at `80b4fe8` (the initial
Task 2 implementation) received the final
`crates/txn/tests/schema_migrations.rs` test from `2f81581`; no current source
or history was modified. Unlike the earlier base used for the first red run,
this base already supplies `SchemaMigration`, `Dataset::migrate_schema`, and
the schema-version API. The test therefore isolates the migration-specific
pre-publication seam introduced by `2f81581`.

The process-local MSVC environment set the following explicit directories:

```powershell
$env:PATH = 'C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\bin\Hostx64\x64;' + $env:PATH
$env:LIB = 'C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\lib\x64;C:\Program Files (x86)\Windows Kits\10\Lib\10.0.26100.0\ucrt\x64;C:\Program Files (x86)\Windows Kits\10\Lib\10.0.26100.0\um\x64;' + $env:LIB
$env:INCLUDE = 'C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\include;C:\Program Files (x86)\Windows Kits\10\Include\10.0.26100.0\ucrt;C:\Program Files (x86)\Windows Kits\10\Include\10.0.26100.0\shared;C:\Program Files (x86)\Windows Kits\10\Include\10.0.26100.0\um;C:\Program Files (x86)\Windows Kits\10\Include\10.0.26100.0\winrt;' + $env:INCLUDE
Set-Location 'C:\Users\dagda\Downloads\nex\strataDB\.worktrees\task-2-fix-round-2-red'
cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_failure_before_publication_reopens_the_prior_complete_manifest -- --exact
```

| Command | Result |
|---|---|
| The command above, in the detached `80b4fe8` worktree after materializing the final test | Exit 101, expected red phase. The test crate compiled against the initial Task 2 migration API and failed only because the migration-specific recovery seam was absent; no Rust test executed. |

Relevant final Cargo output was:

```text
   Compiling strata-storage v0.1.0 (C:\Users\dagda\Downloads\nex\strataDB\.worktrees\task-2-fix-round-2-red\crates\storage)
   Compiling strata-index v0.1.0 (C:\Users\dagda\Downloads\nex\strataDB\.worktrees\task-2-fix-round-2-red\crates\index)
   Compiling strata-query v0.1.0 (C:\Users\dagda\Downloads\nex\strataDB\.worktrees\task-2-fix-round-2-red\crates\query)
   Compiling strata-txn v0.1.0 (C:\Users\dagda\Downloads\nex\strataDB\.worktrees\task-2-fix-round-2-red\crates\txn)
error[E0425]: cannot find function `fail_before_migration_manifest_publication` in module `strata_txn::dataset::test_support`
   --> crates\txn\tests\schema_migrations.rs:212:53
    |
212 |     let _fault = strata_txn::dataset::test_support::fail_before_migration_manifest_publication();
    |                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |
   ::: crates\txn\src\dataset.rs:395:5
    |
395 |     pub fn fail_after_compaction_manifest_publication() -> PostPublicationFaultGuard {
    |     -------------------------------------------------------------------------------- similarly named function `fail_after_compaction_manifest_publication` defined here

For more information about this error, try `rustc --explain E0425`.
error: could not compile `strata-txn` (test "schema_migrations") due to 1 previous error
```

The temporary worktree was then removed. This result demonstrates the missing
migration fault seam/recovery coverage specifically; it is not the earlier
missing-migration-API failure and makes no broader completion claim.

## Task 3 query-planning evidence (2026-08-15)

Task 3 adds a small logical/physical planner for the existing immutable
snapshot query primitives. Explain observations are captured snapshot facts,
not cost or cardinality guarantees. Planned scan, group-by, and vector-search
entry points validate and select a path, then use the same direct snapshot
operators, preserving the existing snapshot, tombstone, null, projection-order,
and typed-error contracts.

All commands below used a process-local native x64 MSVC environment: first
`VsDevCmd.bat -arch=x64 -host_arch=x64`, then the MSVC linker directory first on
`PATH`, and the MSVC 14.52.36615 plus Windows SDK 10.0.26100.0 UCRT/UM x64
directories on `LIB` and the corresponding MSVC/Windows SDK include directories
on `INCLUDE`.

| Command | Result |
|---|---|
| `cargo test -p strata-query planner_tests::planner_rejects_a_predicate_after_a_result_operator -- --exact` before the ordering guard | Exit 101, expected red phase: `LogicalPlan::new` accepted `Source -> Projection -> Predicate -> Materialize`. |
| The same focused test after the ordering guard | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-query` | Exit 0: 63 passed, 0 failed; 0 doctests. |
| `cargo test -p strata-txn --no-default-features --test query_planner --test phase_3_pruning` | Exit 0: 4 planner-equivalence tests and 3 Phase 3 pruning tests passed. |
| `cargo bench -p strata-bench --bench query_planner_bench` | Exit 0. Criterion executed every case below. The initial two attempts exited 1 during benchmark compilation (the local `fixture` value shadowed Criterion's setup function, then `RecordBatch::try_new` was passed without unwrapping); both benchmark-only defects were corrected before this successful measurement. |

Criterion used its default 3 s warmup, 100 samples, and approximately 5 s
measurement target. The fixture was four committed 64-row batches (256 rows),
with a 2-dimensional vector column; the predicate was `id >= 192`, pruning the
first three row files. Direct denotes the established snapshot facade, and
planned includes validation/explain selection before delegating to that same
operator path.

| Workload | Direct 95% interval | Planned 95% interval | Observed comparison |
|---|---:|---:|---|
| Projection scan (`id,category`) | 577.39–581.76 µs | 567.16–575.79 µs | Planned interval was lower on this local rerun. |
| Selective predicate scan (`id,amount`, `id >= 192`) | 136.70–137.28 µs | 138.81–140.99 µs | Planned path was slightly slower on this fixture. |
| Grouped aggregation (`sum(amount)` by `category`, same predicate) | 150.72–152.72 µs | 151.74–152.36 µs | Intervals overlap closely. |
| Filtered vector search (top 10, hydrate `id`, same predicate) | 1.3549–1.3710 ms | 1.3599–1.3729 ms | Intervals overlap closely. |
| Shared-handle transaction commit (two concurrent one-row commits) | 51.992–53.022 ms | n/a | Commit baseline only; it does not claim planner cost. |

Criterion emitted its standard advisory that the default 5-second target could
not fit 100 vector-search or shared-handle-commit samples; it still collected
and analyzed all 100 samples for each case. These results are local workload
evidence only and do not establish universal performance, a cost model,
serializability, cross-process coordination, or a broader Phase 3 completion
claim.

## Task 4 Python API verification (2026-08-15)

The PyO3 facade now exposes the versioned Dataset/Snapshot/Transaction lifecycle, Arrow IPC
transaction scan overlay, explicit nullable-column migration result, and stable explain dictionaries.
All operations remain limited to a single process sharing one Dataset handle; transaction vector
search with staged data remains a typed unsupported operation.

| Command | Result |
|---|---|
| `cargo test -p strata-bindings --no-default-features transaction_wrapper_reads` before schema capture | Exit 101, expected red: `PyTransaction::scan` referenced an uncaptured `schema` and the PyO3 helper methods were incorrectly exported. |
| `cargo test -p strata-bindings --no-default-features transaction_wrapper_reads` after capture/helper separation | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-bindings --no-default-features stable_python_contract_commits_migrates_and_explains_without_rust_layouts` before wrappers | Exit 101, expected red: missing commit, migration, explain, and schema-version façade methods. |
| `cargo test -p strata-bindings --no-default-features` | Exit 0: 15 passed, 0 failed. |
| `cargo clippy -p strata-bindings --no-default-features --all-targets -- -D warnings` | Exit 0; no warnings. |
| `cargo fmt --check` | Exit 0. |

All native bindings commands used the process-local MSVC x64 linker and SDK library directories;
no repository configuration changed for that environment.

## Task 4 fix round 1 verification (2026-08-15)

The Python facade now exposes stable exception subclasses for conflict, schema/migration, invalid
query, unsupported transaction reads, storage/durability, and corruption. `ConflictError` carries a
`contested_row_ids` attribute. `PyDataset.begin()` takes the wrapper schema from the same transaction
base snapshot, and `Transaction.vector_search` delegates to the bounded Rust contract: it searches an
unchanged base snapshot and raises `UnsupportedTransactionReadError` when staged writes would make
base-index results stale. Batch-schema mismatches now render stable human-readable schema context
instead of Rust `Debug` output.

| Command | Result |
|---|---|
| `cargo test -p strata-bindings --no-default-features tests::transaction_schema_accessor_stays_bound_to_its_base_snapshot -- --exact` before the fix | Exit 101, expected red: the five stable exception classes, `Transaction::schema`, and `PyTransaction::vector_search` were absent. |
| `cargo test -p strata-txn --no-default-features error::tests::new_error_variants_format_with_their_context -- --exact` before the fix | Exit 101, expected red: `BatchSchemaMismatch` quoted the schema strings through Rust `Debug` formatting. |
| `cargo test -p strata-bindings --no-default-features` | Exit 0: 17 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features dataset::tests::dataset_schema_is_persisted_and_rejects_renamed_or_castable_batches -- --exact` | Exit 0: 1 passed, 0 failed. |
| `cargo test -p strata-txn --no-default-features error::tests::new_error_variants_format_with_their_context -- --exact` | Exit 0: 1 passed, 0 failed. |
| `cargo build --workspace` | Exit 0. The Windows linker emitted its existing import-library informational warning for `strata-bindings`; no Rust source warning was emitted. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 0; no warnings. |
| `maturin 1.9.4 build --release --locked`, then temporary-wheel installation/import | Exit 0. The CPython 3.14 wheel imported and asserted `Dataset`, `Snapshot`, `Transaction`, and every stable exception export. |

The package-smoke workflow retains the same locked-wheel convention and now checks the new
`Transaction` and exception exports.

## Task 3 fix round 1 evidence (2026-08-15)

This evidence-and-documentation round did not change planner source, dependencies, or planner
semantics. It used a detached worktree at the pre-Task-3 base `6f4d2c0`, then materialized the final
`crates/txn/tests/query_planner.rs` integration test and the final planner construction/invalid-plan
test bodies from `crates/query/src/lib.rs`. The worktree contained only those test copies and was
removed after its status/diff check.

Each focused command initialized the full local x64 MSVC environment with
`VsDevCmd.bat -arch=x64 -host_arch=x64`, then put
`C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\bin\Hostx64\x64`
first on `PATH`; it prepended MSVC `14.52.36615` x64 plus Windows SDK
`10.0.26100.0` UCRT/UM x64 directories to `LIB`, and the corresponding
MSVC/Windows SDK directories to `INCLUDE`. `where cl` and `where link` resolved to that MSVC
x64 directory before the first Cargo run.

| Discriminator and exact focused command | Result |
|---|---|
| Construction/invalid logical plan: `cargo test -p strata-query planner_red_tests::planner_rejects_a_predicate_after_a_result_operator -- --exact` | Exit 101, expected red. The materialized final test could not compile because `strata_query` at `6f4d2c0` exported no `LogicalOperator`, `LogicalPlan`, or `PlanError`; no test body ran. This is direct missing-planner-API evidence, not a claim about a later assertion result. |
| Explain fields/operators: `cargo test -p strata-txn --no-default-features --test query_planner unfiltered_plans_do_not_claim_a_row_filter_or_zone_map_path -- --exact` | Exit 101, expected red. The materialized final integration crate could not compile: `strata_query::{LogicalOperator, PhysicalOperator}` and `Snapshot::explain_scan_query`/`explain_vector_search_query` were absent. Cargo compiles the full test crate before applying the filter, so zero test bodies ran. |
| Explain fields plus direct/planned scan, group, and vector equivalence: `cargo test -p strata-txn --no-default-features --test query_planner planned_queries_match_direct_snapshot_operators_and_report_selection_evidence -- --exact` | Exit 101, expected red. The same missing planner exports, `explain_*`, and `execute_planned_{scan,group_by,vector_search}_query` APIs produced 12 compiler errors; no assertion result is claimed. |
| Direct/planned equivalence with tombstones, nulls, projection ordering, and invalid requests: `cargo test -p strata-txn --no-default-features --test query_planner planned_paths_preserve_tombstones_nulls_projection_order_and_invalid_request_errors -- --exact` | Exit 101, expected red. The full materialized integration crate again stopped at the same 12 missing planner API errors before this filtered test could run. |

The first integration compile reached `strata-txn` and reported exactly the absent public API expected
at this pre-Task-3 base: two unresolved planner operator exports; three missing `explain_*` methods;
and seven missing `execute_planned_*` method uses. This establishes a red precondition for each
discriminator, but it does not manufacture a runtime failure where compilation prevented one. The
current Task 3 green evidence and measured benchmark output remain the preceding section; neither
set of results claims SQL support, a cost model, serializability, cross-process coordination, or
universal performance.

## Task 6 final branch verification (2026-08-15)

This is a bounded, local Windows record for `codex/agent-production-readiness` at
`HEAD` `36a3faa` (merge-base with `origin/main` `f362e54`). It preserves the historical
evidence above and records only the commands actually run against that head; it is not a
claim that every complete-branch gate passed.

Each Cargo command used a process-local native x64 MSVC/Windows SDK environment. `PATH`
began with `C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.52.36615\bin\Hostx64\x64`;
`LIB` began with the corresponding MSVC x64 directory and Windows SDK `10.0.26100.0`
UCRT/UM x64 directories; and `INCLUDE` began with the corresponding MSVC and Windows SDK
UCRT/shared/UM/winrt directories. `where.exe cl` and `where.exe link` both resolved to that
MSVC x64 directory.

| Command | Result |
|---|---|
| `cargo fmt --check` | Exit 0. |
| `cargo metadata --no-deps --format-version 1` | Exit 0. |
| `cargo check --workspace` | Exit 0. |
| `cargo build --workspace` | Exit 0. The existing `strata-bindings` `linker_messages` warning remained: MSVC reported creation of `strata_ext.dll.lib` and `strata_ext.dll.exp`; no Rust-source warning is claimed. |
| `cargo test --workspace --no-default-features` | Exit 0. The bindings unit binary passed 17/17; `admin_cli` and `phase_2_cli` each passed 12/12. |
| `cargo test -p strata-txn --no-default-features --test schema_migrations --quiet` | Exit 0: 3 passed, 0 failed, 0 ignored. This separately records the migration-test count also included in the successful workspace run. |
| `git diff --check` | Exit 0 after this evidence-only edit. |

The normal workspace test selection retained its intentionally ignored real-thread HNSW stress
test and thorough 2,000-seed chaos/recovery test. Neither ignored test was force-run here.

### Unrun and not claimed

The following Task 6 gates were not run in this local evidence pass and are not claimed: workspace
clippy, `cargo doc --workspace --no-deps`, `cargo deny check bans sources advisories`, the
`parallel-insert` test/clippy checks, crate-scoped loom builds and binaries, and the
fault-injection recovery gates. The native workspace test command does not enable the
fault-injection feature and is not evidence for those recovery gates. `maturin` was absent, so no
packaging/wheel-import smoke ran. No Criterion benchmark command ran, so this section adds no
performance measurement or comparison. These omissions, the intentionally ignored tests, and the
existing linker informational warning mean this record must not be read as a full-branch-green or
complete-feature claim.

## Task 6 exact-HEAD partial Windows verification (2026-08-15)

This later run is a partial exact-HEAD record for `29c70e3f2f6a7886cf108985fa323b2ffd4aafae`.
It used a process-local x64 environment with MSVC `14.52.36615` and Windows SDK
`10.0.26100.0`: `PATH` began with `...\\VC\\Tools\\MSVC\\14.52.36615\\bin\\Hostx64\\x64`,
`LIB` began with the MSVC x64, SDK UCRT x64, and SDK UM x64 directories, and `INCLUDE`
began with the matching MSVC and SDK UCRT/shared/UM/winrt directories. In that environment,
`link.exe` resolved from the requested MSVC directory and the SDK x64 `kernel32.lib` existed.

| Command | Result |
|---|---|
| `cargo fmt --check` | Exit 0. |
| `cargo metadata --locked --no-deps` | Exit 0; Cargo emitted its compatibility warning requesting an explicit `--format-version`. |
| `cargo check --workspace` | Exit 0. |
| `cargo build --workspace` | Exit 0; existing `strata-bindings` `linker_messages` warning reported MSVC creation of `strata_ext.dll.lib` and `strata_ext.dll.exp`. |
| `cargo test --workspace --no-default-features` | Exit 0; all executed unit, integration, and doctest binaries passed. The normal ignored HNSW real-thread stress test and thorough 2,000-seed chaos test remained ignored. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 0. |
| `cargo doc --workspace --no-deps` | Exit 0; existing rustdoc warning: unresolved intra-doc link `crate::backend::LocalFs::put` in `crates/storage/src/manifest.rs`. |
| `cargo deny check bans sources advisories` | Initial sandbox attempt exited 1 because the advisory DB lock path was read-only. The required rerun with advisory-cache write access exited 0: advisories, bans, and sources were OK; cargo-deny warned about duplicate lock entries for `getrandom`, `hashbrown`, `r-efi`, `thiserror`, and `thiserror-impl`. |
| `cargo test -p strata-txn --features parallel-insert` | Exit 0; all executed tests and doctests passed, with 0 failures. |
| `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings` | Exit 0. |
| `cargo test -p strata-txn --features test-fault-injection` | Exit 0; all executed tests and doctests passed, with 0 failures. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_failure_before_publication_reopens_the_prior_complete_manifest -- --exact` | Exit 0: 1 passed, 0 failed, 5 filtered out. |
| `cargo test -p strata-txn --features chaos-injection --test manifest_retention_executor -- --exact manifest_prune_abort_before_directory_sync_preserves_latest_recovery --nocapture` | Exit 0: 1 passed, 0 failed, 5 filtered out. |

The txn loom gate was started with the CI-style crate-scoped recipe
`cargo rustc -p strata-txn --lib --profile test --message-format=json -- --cfg loom`, then the
produced binary was invoked with `--exact <model> --test-threads=1`. The build exited 0 and
emitted the existing localized MSVC `linker_messages` import-library warning. These two models
passed: `one_writer_store_races_safely_with_many_readers_load` and
`two_threads_deleting_the_same_row_exactly_one_conflicts`. The next exact model,
`dataset::loom_tests::a_failed_commits_segment_is_never_searchable_under_concurrent_commits`,
did not complete or emit a failure line within the practical run window. At user direction it was
terminated together with its owning PowerShell process; it is **aborted**, not passed.

### Unrun and not claimed

Because the run stopped at that aborted txn loom model, the remaining txn loom models, all
live-set-cache loom models, and all `strata-index` loom models were unrun. Criterion benchmark
compile/run was unrun. `maturin` was unavailable on PATH, so the bindings wheel/import smoke was
unrun. No additional standalone CLI integration invocation, fault-injection clippy invocation,
thorough-chaos run, independent Terra review, or Sol final review was run in this partial pass.
The only pre-existing worktree change was
`.superpowers/sdd/agent-production-readiness-plan/reports/task-2-report.md`; it was preserved.
This section records partial local evidence only and makes no whole-feature, complete-branch, or
unrun-gate-green claim.
