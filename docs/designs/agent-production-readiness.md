# Agent Production Readiness Design

Status: implemented within named bounds; Tasks 1 through 5 are accepted within
named bounds. The active contract and final evidence are maintained by the
Phase 3 audit records and the current verification report.

## Objective

Make Strata practical for multiple orchestration agents sharing one embedded `Dataset` handle. The work extends the existing local, single-process engine without changing its concurrency boundary or claiming stronger isolation than the current design supports.

The deliverable consists of five coordinated capabilities:

1. a stable Python API;
2. read/write transactions with snapshot reads, read-your-writes, and write-write OCC;
3. explicit schema evolution and migrations;
4. a query planner with explain output and measured performance evidence; and
5. stable administration and operational tooling through the CLI.

## Scope and non-goals

The supported deployment remains one process with multiple agents sharing a `Dataset` handle. A transaction captures an immutable base snapshot and may stage writes against it. Reads in that transaction observe the base snapshot plus its own pending writes. Commit remains durable, conflict-checked, and manifest-visible within the documented local durability boundary.

This design does not add cross-process coordination, leases, IPC/RPC, distributed transactions, full SQL, automatic conflict resolution, additional ANN index families, or full serializability. Snapshot isolation is the ceiling. The transaction API must not imply prevention of read skew, predicate phantoms, or write skew. A new isolation level would require a separate decision record.

## Invariants

- A successful commit is acknowledged only after durable row and vector state, write-write conflict validation, and manifest publication.
- Row data and vector-index changes are one commit boundary.
- Vector indexes remain immutable per-commit segments listed by the manifest.
- Conflicts are typed errors and identify contested physical row IDs.
- Physical row IDs are monotonically allocated, never reused, and may contain gaps.
- Existing snapshots remain readable against their captured schema and manifest.
- Unsupported or ambiguous schema changes fail closed rather than being silently coerced.
- Public Python and CLI behavior is versioned and maps engine errors without losing their category.
- Query planning is an execution arrangement, not a new consistency model.

## Architecture

The existing `Dataset`, `Snapshot`, storage manifest, query operators, bindings, and CLI remain the primary layers. The transaction layer owns the transaction read view and write overlay. Storage owns versioned schema metadata and migration artifacts. Query owns logical and physical plan types and optimization decisions. Bindings and CLI expose stable, typed contracts over those primitives.

The intended flow is:

```text
begin -> immutable base snapshot + transaction overlay
          |                         |
          +--> planned reads -------+
          +--> staged row/vector writes
                         |
             validate write-write OCC
                         |
              durable objects + manifest publication
```

The overlay must be explicit enough that a read can distinguish a staged replacement, staged insertion, staged deletion, and an unchanged row. It must not mutate the base snapshot or make uncommitted state visible to another handle. Query execution over a transaction may use the same logical plan types as snapshot execution, but its source must merge the base snapshot with the transaction overlay before projection, filtering, grouping, or vector result materialization.

## Read/write transaction contract

The public Rust API should expose a transaction read view alongside existing write methods. The exact names should follow current crate conventions, but the contract is fixed:

- `begin` captures the base snapshot and transaction identity.
- Transaction reads support the documented query primitives needed by agents: row lookup/scan, predicates, projection, grouping, and vector search where the overlay can be represented correctly.
- A transaction reads its own inserts, replacements, and deletes.
- A transaction never reads another transaction's uncommitted writes.
- Commit validates only write-write conflicts against the engine's recent committed history.
- A conflict returns the existing typed conflict error with row IDs; it never silently retries or resolves.
- Abort/drop discards staged state without publishing it.

Read-your-writes behavior must be tested for each supported read primitive. Where a primitive cannot safely merge staged data—for example, an index operation whose overlay semantics are not yet expressible—the API must return a typed unsupported-operation error rather than silently reading a stale view. This keeps the implementation honest and preserves the snapshot-isolation ceiling.

## Schema catalog and migrations

Schema metadata becomes an explicit versioned part of the durable dataset state. A manifest references the schema version used by its row and vector objects. A migration is a named, deterministic, explicitly requested transformation from one supported version to another.

Migration requirements:

- validate source version and migration direction before writing;
- write transformed objects to new durable locations;
- publish the new schema and object references atomically in one manifest update;
- preserve old snapshots and their schema references;
- reject unknown versions, incompatible types, lossy implicit conversions, and partial migration state;
- make reopen/recovery select only a complete published version;
- expose migration status and failure details through the admin API and CLI.

The first implementation should support the smallest useful evolution set already expressible by the storage schema model. It must not introduce a general-purpose arbitrary code execution hook into the database. Migration registration and execution must have deterministic error reporting and a compatibility test for every supported transition.

## Query planner and evidence

The planner should introduce a small logical plan representation for the supported operations: source, projection, predicate, grouping, vector search, and result materialization. A physical planning step may choose existing scan, pruning, vector, and aggregation operators. It must preserve row ordering and result semantics currently promised by the relevant API.

`explain` must show the logical shape, selected physical operators, predicate/pruning decisions, estimated or measured cardinalities when available, and whether a transaction overlay is involved. It must not present unsupported cost estimates as guarantees.

Performance work is evidence-driven. Add focused Criterion benchmarks for representative agent workloads: snapshot scan with projection, selective predicate scan, grouped aggregation, vector search, and read/write transaction commit under shared-handle contention. Record baselines and compare planner paths against the existing direct operators. Any HNSW change requires separate parameter evidence; this design does not require changing HNSW parameters.

## Stable Python API

The PyO3 facade should expose a deliberately small, documented surface around dataset open/create, snapshots, transactions, query results, migrations, and typed errors. Python methods must not leak internal Rust layout or unstable debug strings. Errors should preserve categories such as conflict, schema/migration, invalid query, unsupported operation, durability/storage, and corruption.

The API should include an explicit version marker and stable result representations for rows, groups, vector matches, explain output, and transaction state. Blocking storage and lock operations must release the GIL according to the existing binding conventions. Python tests must cover import/build smoke behavior, lifecycle, typed errors, read-your-writes, migration/reopen, and representative query results.

## Administration and CLI

The CLI should provide stable commands for the operational tasks required by the new capabilities: inspect dataset/schema, explain a query, run or validate a named migration, report migration/recovery state, and run the supported benchmark/evidence command. Existing compatibility commands may remain, but new commands must have documented arguments, exit-code behavior, and machine-readable JSON output in addition to human-readable output where practical.

Administrative commands must not mutate data by default. Mutating operations require an explicit command and arguments, fail closed on ambiguity, and report the resulting schema/manifest version. The CLI must distinguish usage errors, conflicts, unsupported operations, corruption, and operational failures through stable exit categories.

## Verification strategy

Implementation proceeds in bounded vertical slices with TDD:

1. transaction read overlay and typed API errors, with normal tests and a targeted loom model for interleavings;
2. schema catalog versioning and one minimal migration path, with migration, reopen, crash-boundary, and corruption tests;
3. planner/explain integration and Criterion baselines, with equivalence tests against direct operators;
4. Python contract and binding tests;
5. CLI/admin integration tests and documentation/evidence refresh.

Every slice needs focused red/green tests before broad verification. Affected Rust changes require format, check, tests, clippy, and documentation checks. Transaction/index interleavings require the crate-scoped loom recipe from `AGENTS.md`. Bindings use the documented native test feature configuration. Benchmarks must report command, workload, and comparison results rather than only compiling.

The final branch gate includes workspace tests with `--no-default-features`, clippy with warnings denied, format check, docs build, deny checks where available, targeted parallel-insert checks, loom output, CLI/Python smoke tests, migration recovery tests, benchmark output, `git diff --check`, and a complete diff/evidence review. A separate Terra reviewer reviews each implementation slice; Sol performs the final complete-branch review.

## Dependency order and likely file scope

The implementation order is transaction contract, schema/migrations, planner/benchmarks, then Python and CLI/operations. This avoids exposing client contracts over primitives whose semantics are still moving.

Likely files are confined to the relevant existing modules under `crates/txn`, `crates/storage`, `crates/query`, `crates/bindings`, `crates/cli`, focused tests, benchmarks, and current docs. New dependencies are forbidden unless separately justified and explicitly approved. The implementation plan must name exact files after inspecting current symbols and tests; broad rewrites are out of scope.

## Decisions and deviations

This design intentionally leaves Phase 4 cross-process coordination skipped. It also preserves the prior decision against full serializability and does not create a hidden coordination layer through Python or the CLI. If implementation discovers that a requested read or migration guarantee requires a stronger isolation or durability model, the work must stop at a typed limitation and return for a new architecture decision rather than weakening the documented boundary.
