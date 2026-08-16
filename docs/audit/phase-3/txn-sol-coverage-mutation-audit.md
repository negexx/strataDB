# Strata-Txn Sol Structural, Behavioral, and Mutation Coverage Audit

Date: 2026-08-15  
Scope: `crates/txn`, transaction tests, CI recipes, loom, fuzz, chaos, and
benchmark evidence  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 was found. One P1 correctness-critical coverage gap and
several P2/P3 mutation gaps prevent approval.

No coverage percentages are claimed: test volume is not statement, branch, or
mutation coverage.

## Findings

### [P1] Post-publication sync failure has no transaction-level behavioral coverage

Locations:

- [`crates/storage/src/backend/local.rs:344`](../../../../crates/storage/src/backend/local.rs:344)
- [`crates/storage/src/backend/local.rs:359`](../../../../crates/storage/src/backend/local.rs:359)
- [`crates/txn/src/dataset.rs:2231`](../../../../crates/txn/src/dataset.rs:2231)
- [`crates/txn/src/dataset.rs:2239`](../../../../crates/txn/src/dataset.rs:2239)

Storage tests prove that a manifest can become visible before directory sync
returns an error, but no transaction test verifies reopen/retry behavior,
stale-handle reconciliation, version uniqueness, or row/index state after that
outcome. Existing tests model failures before publication only. This is the
same durability gap identified by the correctness audit and requires a Sol
design before implementation.

### [P2] No measurable statement, branch, or mutation coverage gate

The graph indexes 246 unit tests and 89 `crates/txn/tests` integration tests,
but no `cargo-llvm-cov`, grcov, tarpaulin, cargo-mutants configuration,
baseline, report, or CI threshold was found. CI runs normal/feature tests,
loom, and fuzz smoke, but does not record line/branch coverage or mutation
results.

### [P2] Legacy zero-timestamp retention protection lacks a branch test

The destructive authority branch protecting manifests with
`committed_at_us == 0` is at
[`retention.rs:261`](../../../../crates/txn/src/retention.rs:261). The age
retention test creates only positively timestamped manifests at
[`retention_age.rs:7`](../../../../crates/txn/tests/retention_age.rs:7).
Removing the zero-timestamp guard would likely survive existing tests.

### [P2] Maintenance policy validation lacks invalid-branch tests

Three zero-value conditions are joined by `||` at
[`maintenance.rs:63`](../../../../crates/txn/src/maintenance.rs:63), while the
tests use valid policies at
[`maintenance.rs:7`](../../../../crates/txn/tests/maintenance.rs:7) and
[`maintenance.rs:38`](../../../../crates/txn/tests/maintenance.rs:38).
An `||` to `&&` mutation or removal of an individual guard would likely
survive.

### [P3] Some fault tests verify failure but not error identity

For example, injected manifest failures use generic `is_err()` assertions at
[`dataset.rs:8687`](../../../../crates/txn/src/dataset.rs:8687). Changing the
error category could survive. Conflict, target, unsupported-read, and many
lifecycle tests do assert exact variants, so this gap is localized.

## Strong behavioral/state evidence

Existing tests substantively verify:

- conflict row IDs and OCC boundaries, including a 2,000-case property test;
- tombstones, replacement IDs, and reopen persistence;
- immutable snapshots;
- restart non-reuse after process abort;
- lifecycle protection of active snapshots;
- query result equivalence, tombstones, nulls, projection order, and typed
  invalid requests;
- row/index visibility interleavings through loom models.

Representative evidence includes
[`dataset.rs:8224`](../../../../crates/txn/src/dataset.rs:8224),
[`commit_log.rs:278`](../../../../crates/txn/src/commit_log.rs:278),
[`dataset.rs:8110`](../../../../crates/txn/src/dataset.rs:8110),
[`concurrent_snapshot_isolation.rs:24`](../../../../crates/txn/tests/concurrent_snapshot_isolation.rs:24),
[`row_id_reservation_restart.rs:37`](../../../../crates/txn/tests/row_id_reservation_restart.rs:37),
[`compaction.rs:84`](../../../../crates/txn/tests/compaction.rs:84),
[`vacuum.rs:68`](../../../../crates/txn/tests/vacuum.rs:68), and
[`query_planner.rs:475`](../../../../crates/txn/tests/query_planner.rs:475).

## Representative mutation assessment

| Mutation | Static assessment |
|---|---|
| Swap conflict version-range operators | Expected killed by boundary-directed property tests |
| Remove conflict checks | Expected killed by contested-row tests and loom |
| Skip tombstone publication | Expected killed by visibility, reopen, update, and query tests |
| Reuse abandoned row IDs | Expected killed by restart tests |
| Remove active-snapshot lifecycle protection | Expected killed by retention/compaction/vacuum tests |
| Bypass manifest publication | Expected killed by reopen and row/index atomicity tests |
| Mishandle post-rename sync failure | Likely survives |
| Remove legacy zero-timestamp protection | Likely survives |
| Change maintenance validation `||` to `&&` | Likely survives |
| Change injected manifest-failure error category | Likely survives generic `is_err()` assertions |

These are static predictions, not executed mutation results.

## Tool and evidence blockers

- Native transaction tests cannot run because `link.exe` is unavailable.
- `cargo-llvm-cov`, grcov, cargo-mutants, and cargo-tarpaulin are unavailable.
- Fuzz CI runs deterministic smoke inputs, not a sustained transaction-state
  fuzz campaign.
- Thorough chaos runs are scheduled/manual rather than part of every CI run.
- Criterion benchmarks provide performance measurements, not mutation or
  correctness assertions.

No files were edited by the Sol reviewer. This document is an audit record;
the identified coverage gaps should be addressed through a Sol plan before
Terra implementation.

