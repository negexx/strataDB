# Agent Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to implement this plan task-by-task, with an independent review after every task.

**Goal:** Deliver stable Python and CLI contracts, read/write snapshot transactions, explicit schema migrations, a query planner with evidence, and operational tooling for multiple agents sharing one local `Dataset` handle.

**Architecture:** Extend the existing `txn` transaction/snapshot model with a transaction-local overlay, retain write-write OCC and immutable manifest publication, add versioned schema metadata in `storage`, and place logical/physical planning in `query`. Bindings and CLI expose only the stable contracts built on those primitives.

**Tech Stack:** Rust 2024, Cargo workspace, Arrow IPC/data files, PyO3, existing CLI parser, Criterion benchmarks, loom models, and the repository’s current test/chaos infrastructure.

## Global Constraints

- Supported concurrency remains one process with multiple agents sharing one `Dataset` handle.
- Snapshot isolation is the ceiling; do not implement or claim full serializability.
- Do not add Phase 4 cross-process coordination, IPC/RPC, leases, or distributed transactions.
- Row data and vector-index changes publish through one durable manifest boundary.
- Vector indexes remain immutable per-commit segments.
- Conflicts remain typed and identify contested physical row IDs.
- Physical row IDs remain monotonically allocated and never reused.
- Do not add dependencies without explicit approval.
- Every interleaving-sensitive `txn`/`index` change gets a targeted loom model and normal tests.
- Use `--no-default-features` for native workspace Rust tests and preserve the bindings feature rule.
- Do not claim unrelated dirty Rust work is green after a documentation-only or isolated task.

## File map

- `crates/txn/src/dataset.rs`: transaction creation, staged writes, commit validation, and publication integration.
- `crates/txn/src/snapshot.rs`: immutable snapshot read primitives and the transaction read-overlay implementation or its shared helpers.
- `crates/txn/src/query.rs`: query contracts and typed unsupported/validation errors used by planned and direct execution.
- `crates/txn/src/lib.rs`: public transaction/snapshot exports.
- `crates/storage/src/manifest.rs`: durable manifest schema-version metadata and atomic publication.
- `crates/storage/src/schema.rs` or the existing schema module selected after symbol inspection: catalog version and migration representation.
- `crates/query/src/lib.rs` and adjacent existing query modules: logical/physical plan types, planner, and explain output.
- `crates/bindings/src/lib.rs`: stable PyO3 dataset/snapshot/transaction/result/error surface.
- `crates/cli/src/main.rs`: stable admin commands, output modes, and exit categories.
- Existing focused test files beside each crate, plus `crates/txn/tests`, `crates/cli/tests`, and `bench/benches` where current conventions place them.
- `docs/status.md`, `docs/roadmap.md`, and relevant API/operations docs: evidence and boundary refresh after implementation.

### Task 1: Transaction read view and overlay

**Files:**
- Modify: `crates/txn/src/dataset.rs` around `Dataset::begin`, `Transaction::insert`, update/delete methods, and `Transaction::commit`.
- Modify: `crates/txn/src/snapshot.rs` around `Snapshot::lookup_row`, `scan_query`, `group_by_query`, and vector query methods.
- Modify: `crates/txn/src/query.rs` for typed transaction-read unsupported errors and shared query contracts.
- Modify: `crates/txn/src/lib.rs` for public exports.
- Test: existing transaction/snapshot test modules and a focused new test module under `crates/txn/tests` if the crate convention supports it.
- Test: crate-scoped loom model location identified from the existing txn loom recipe.

**Interfaces:**
- Consumes: current `Dataset::begin`, immutable `Snapshot`, staged write structures, conflict history, and `Transaction::commit`.
- Produces: a public transaction read view with read-your-writes for lookup/scan/predicate/group operations that can be merged correctly; typed unsupported behavior for operations whose overlay cannot be represented; unchanged write-write OCC semantics.

- [ ] **Step 1: Write failing tests for the contract.** Cover transaction lookup of an inserted row, replacement visibility, delete invisibility, scan/predicate results, grouped results, abort/drop non-publication, isolation from another transaction, and typed unsupported vector-overlay behavior if vector reads cannot yet merge staged values.
- [ ] **Step 2: Run the focused tests and confirm failure.** Run `cargo test -p strata-txn --no-default-features <focused-filter>`; expected failures must identify missing transaction read methods or stale snapshot results.
- [ ] **Step 3: Implement the minimal overlay.** Capture the base snapshot at begin, represent staged insert/replace/delete states by physical row ID, and make each supported read resolve overlay state before applying projection/filter/group logic. Preserve the base snapshot for untouched rows and never expose staged state through `Dataset::snapshot`.
- [ ] **Step 4: Preserve OCC and publication behavior.** Route commit through the existing typed conflict validation and manifest publication path. Do not broaden conflict detection to read sets, predicates, or serializability.
- [ ] **Step 5: Add the targeted loom model.** Model two shared-handle transactions with disjoint and contested writes plus one transaction read view; assert no uncommitted state escapes and contested physical row IDs produce the typed conflict.
- [ ] **Step 6: Run focused verification.** Run the focused tests, loom binary using the crate-scoped `cargo rustc` recipe, `cargo test -p strata-txn --features parallel-insert`, and relevant clippy checks.
- [ ] **Step 7: Commit the slice.** Stage only transaction source/tests and commit `feat: add transaction read views`.

### Task 2: Versioned schema catalog and migrations

**Files:**
- Modify: `crates/storage/src/manifest.rs` for schema-version references and backward-compatible manifest decoding.
- Modify or create: the existing storage schema module (`crates/storage/src/schema.rs` if present; otherwise the narrow module selected by graph inspection) for catalog versions, migration descriptors, and typed migration errors.
- Modify: `crates/txn/src/dataset.rs` for migration entry points and atomic dataset publication.
- Modify: `crates/txn/src/snapshot.rs` for schema-version-bound reads where required.
- Test: storage manifest compatibility tests and transaction reopen/recovery tests.
- Test: migration integration tests covering success, unsupported transition, corruption, partial publication, and old snapshot readability.

**Interfaces:**
- Consumes: Task 1’s transaction/publication boundary and existing `DatasetSchema` validation.
- Produces: explicit schema version metadata, deterministic named migration execution, atomic version publication, migration status/error results, and old-snapshot compatibility.

- [ ] **Step 1: Write failing catalog tests.** Assert a fresh dataset has an explicit schema version, manifests round-trip the version, unknown versions reject loudly, and old snapshots retain their captured schema after a later migration.
- [ ] **Step 2: Write failing migration tests.** Define the smallest supported deterministic transition already expressible by the current schema model; test forward success, wrong source version, reverse/unsupported transition, lossy conversion rejection, and migration failure without changing the current manifest.
- [ ] **Step 3: Implement versioned manifest metadata.** Add the schema reference using the current manifest compatibility/versioning pattern. Older supported manifests must receive only a documented default; ambiguous or unknown state must return a typed error.
- [ ] **Step 4: Implement one explicit migration path.** Validate source/target versions, write transformed row/vector objects to new durable locations, publish schema and object references atomically, and make recovery select only a complete manifest.
- [ ] **Step 5: Add fault/reopen coverage.** Exercise migration interruption points using existing fault-injection hooks where available, reopen the dataset, and assert the prior complete manifest remains usable when the new one was not fully published.
- [ ] **Step 6: Run storage/txn verification.** Run focused tests, workspace checks for affected crates, format, clippy, and the relevant crash/recovery tests.
- [ ] **Step 7: Commit the slice.** Stage only schema/storage/txn migration files and tests and commit `feat: add versioned schema migrations`.

### Task 3: Query planner, explain, and benchmarks

**Files:**
- Modify/create: `crates/query/src/lib.rs` and adjacent existing query modules for logical plans, physical plans, planner selection, and explain serialization.
- Modify: `crates/txn/src/snapshot.rs` to execute planned reads through existing scan, pruning, grouping, and vector operators without changing result semantics.
- Modify: `crates/txn/src/query.rs` for planner-facing validation and execution errors.
- Modify: `crates/cli/src/main.rs` to use the planner for explain and supported query commands.
- Create/modify: focused query equivalence tests under `crates/query` or `crates/txn` following current conventions.
- Create/modify: Criterion benches under `bench/benches` for projection scan, selective predicate, grouped aggregation, vector search, and shared-handle transaction commit.
- Modify: benchmark evidence documentation under `docs/` after measurements are collected.

**Interfaces:**
- Consumes: current direct `Snapshot` query primitives, Task 1 transaction read view, and existing explain command behavior.
- Produces: logical plan, physical plan, planner entry point, stable explain representation, and benchmark cases comparing planned execution with direct operators.

- [ ] **Step 1: Write failing plan/explain tests.** Build plans for source→projection, source→predicate→projection, source→group, and vector search; assert explain includes operator names, pruning choice, cardinality information only when available, and overlay involvement.
- [ ] **Step 2: Write equivalence tests.** Execute each plan against a fixture and compare rows/groups/vector matches to the existing direct operator results, including empty input, tombstones, nulls, and invalid projections.
- [ ] **Step 3: Implement logical and physical plan types.** Keep the AST limited to supported operations and make invalid combinations return typed query errors. Do not add a SQL parser or unsupported optimizer claims.
- [ ] **Step 4: Implement physical selection.** Reuse current pruning, scan, grouping, and immutable vector segment paths; preserve snapshot identity, row ordering guarantees already tested, and transaction overlay semantics.
- [ ] **Step 5: Integrate explain and CLI execution.** Keep existing compatibility output stable while adding the planner details needed by the approved admin contract.
- [ ] **Step 6: Add and run Criterion workloads.** Record command, fixture size, baseline direct path, planned path, and observed results for each workload. Do not change HNSW parameters without separate evidence and approval.
- [ ] **Step 7: Commit the slice.** Stage planner/query/benchmark files and focused tests/evidence and commit `feat: add query planning evidence`.

### Task 4: Stable Python contract

**Files:**
- Modify: `crates/bindings/src/lib.rs` around `PyDataset`, `PySnapshot`, conversion helpers, and error mapping.
- Modify: `crates/txn/src/lib.rs` or the narrow public API module for binding-facing transaction types.
- Test: binding unit/integration tests in the existing bindings test location.
- Test: Python smoke fixture or packaging test only where the repository already supports it.
- Modify: Python API documentation under `docs/`.

**Interfaces:**
- Consumes: Task 1 transaction read view, Task 2 migration results, Task 3 plan/explain result, and existing PyO3 conversions.
- Produces: explicit Python API version marker; stable dataset/snapshot/transaction lifecycle; stable row/group/vector/explain result shapes; categorized Python exceptions for conflict, schema, invalid query, unsupported operation, storage/durability, and corruption.

- [ ] **Step 1: Write failing binding tests.** Cover import/build smoke, API version, transaction context/lifecycle, read-your-writes, abort, typed conflict mapping, migration result/error mapping, and explain/result shape stability.
- [ ] **Step 2: Implement stable wrappers.** Add only wrappers around existing Rust contracts, avoid exposing internal structs/debug text, and release the GIL around blocking open/read/commit/migration operations.
- [ ] **Step 3: Preserve existing compatibility.** Run current binding tests and retain existing snapshot methods and Arrow IPC/result behavior unless the new stable type is explicitly versioned.
- [ ] **Step 4: Run binding verification.** Use the project’s documented feature configuration, run native tests with `--no-default-features`, and perform the packaging/import smoke test.
- [ ] **Step 5: Commit the slice.** Stage binding source/tests/docs and commit `feat: stabilize python api contracts`.

### Task 5: Administration CLI and operational evidence

**Files:**
- Modify: `crates/cli/src/main.rs` around command parsing, `CliError`, handlers, output formatting, and `handle_explain`.
- Modify: `crates/cli/tests/phase_2_cli.rs` or create the narrowly named next-phase integration test file following current fixture helpers.
- Modify: `crates/txn/src/dataset.rs` or the narrow operational API module for inspect/schema/migration status operations.
- Modify: `docs/status.md`, `docs/roadmap.md`, and relevant CLI/API documentation with final evidence and named limits.

**Interfaces:**
- Consumes: versioned schema/migration API, planner/explain, stable error categories, and existing typed CLI rendering.
- Produces: stable inspect/schema, explain, migration validate/run/status, recovery/manifest status, and benchmark/evidence commands with human and JSON output plus stable exit categories.

- [ ] **Step 1: Write failing CLI integration tests.** Cover help/usage, inspect schema, explain planned operators, migration validation, explicit migration execution, status after reopen, JSON output parsing, and distinct exit behavior for usage/conflict/unsupported/corruption/operational errors.
- [ ] **Step 2: Implement read-only admin commands.** Add inspect/schema/status and machine-readable output without mutating data; preserve current compatibility commands and their tested output.
- [ ] **Step 3: Implement explicit migration commands.** Require an explicit named migration and target dataset, reject ambiguity, report resulting schema/manifest version, and leave the prior manifest unchanged on failure.
- [ ] **Step 4: Add operational benchmark/evidence command.** Make it invoke the supported benchmark fixture or report how to run the Criterion evidence; avoid pretending a CLI timing is a durable benchmark result.
- [ ] **Step 5: Run CLI verification.** Run focused integration tests, current Phase 2 CLI tests, format, clippy, and representative command smoke checks.
- [ ] **Step 6: Refresh status and roadmap.** Mark only verified capabilities complete, retain explicit limits around local/shared-handle concurrency and snapshot isolation, and record benchmark/loom/migration evidence paths.
- [ ] **Step 7: Commit the slice.** Stage CLI/docs/tests and commit `feat: add stable administration tooling`.

### Task 6: Complete-branch verification and independent review

**Files:**
- Modify only verification evidence/docs required by fresh results.
- Review all changed files from Tasks 1–5.

**Interfaces:**
- Consumes: all implemented slices and their focused evidence.
- Produces: complete verification record and accepted/rejected review findings; no unsupported completion claim.

- [ ] **Step 1: Run the full required checks.** Run `cargo fmt --check`, `cargo check --workspace`, `cargo test --workspace --no-default-features`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo doc --workspace --no-deps`, `cargo deny check bans sources advisories`, targeted parallel-insert checks, loom binaries, bindings smoke, CLI integration, migration recovery, and Criterion benchmark commands.
- [ ] **Step 2: Inspect the final diff.** Run `git diff --check`, verify exact file scope, scan for accidental compatibility changes, unsupported isolation claims, credentials, build artifacts, and stale docs.
- [ ] **Step 3: Perform separate Terra review.** Review each slice independently for invariants, tests, error behavior, manifest durability, and API stability; record concrete findings before acceptance.
- [ ] **Step 4: Resolve accepted findings.** Terra fixes only approved findings in bounded commits and reruns affected checks.
- [ ] **Step 5: Perform Sol final branch review.** Check architecture, concurrency boundary, schema compatibility, planner semantics, and evidence against the approved design.
- [ ] **Step 6: Report final evidence.** Include branch, commits, files changed, exact commands/results, measured benchmark comparisons, remaining limitations, and any blocked checks.
