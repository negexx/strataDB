# Strata-Txn Sol Structural, Behavioral, and Mutation Coverage Audit

Date: 2026-08-27
Scope: `crates/txn`, transaction tests, CI recipes, loom, fuzz, chaos, and
benchmark evidence  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 2 head `d08f5d6`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The correctness-critical publication
scenario and the practical normal-code mutation gaps identified by the audit
were closed with focused behavioral tests and independently reviewed
remediation slices. Measured coverage and mutation evidence is recorded below;
these results are not a universal release threshold or a claim of exhaustive
state-space coverage.

Coverage percentages and mutation outcomes are recorded in the execution
addenda below; they are evidence for this bounded audit, not a universal
quality SLO.

## Findings

### [Resolved P1] Post-publication sync failure has transaction-level behavioral coverage

Locations:

- [`crates/storage/src/backend/local.rs:344`](../../../crates/storage/src/backend/local.rs#L344)
- [`crates/storage/src/backend/local.rs:359`](../../../crates/storage/src/backend/local.rs#L359)
- [`crates/txn/src/dataset.rs:2231`](../../../crates/txn/src/dataset.rs#L2231)
- [`crates/txn/src/dataset.rs:2239`](../../../crates/txn/src/dataset.rs#L2239)

Transaction-level tests now verify indeterminate publication reconciliation,
reopen visibility, unique subsequent publication, and row/index state after
the outcome. Compaction and schema-migration publication barriers are covered
as well. Blind replay remains prohibited by the documented typed error
contract.

### [Resolved P2] Measurable statement, branch, and mutation evidence recorded

The execution addenda record reproducible `cargo-llvm-cov` line/function/branch
results and a `cargo-mutants` campaign with its denominator and outcome
classification. The repository does not impose a universal percentage gate;
coverage is evidence for this bounded audit rather than an SLO.

### [Resolved P2] Legacy zero-timestamp retention protection has a branch test

The destructive authority branch protecting manifests with
`committed_at_us == 0` is at
[`retention.rs:261`](../../../crates/txn/src/retention.rs#L261). The age
retention test creates only positively timestamped manifests at
[`retention_age.rs:7`](../../../crates/txn/tests/retention_age.rs#L7).
The retention suite now covers the zero-timestamp protection branch,
including the never-age-prune behavior.

### [Resolved P2] Maintenance policy validation has invalid-branch tests

Three zero-value conditions are joined by `||` at
[`maintenance.rs:63`](../../../crates/txn/src/maintenance.rs#L63), while the
tests use valid policies at
[`maintenance.rs:7`](../../../crates/txn/tests/maintenance.rs#L7) and
[`maintenance.rs:38`](../../../crates/txn/tests/maintenance.rs#L38).
The maintenance suite covers each invalid zero-value policy condition and the
valid paths, preventing the identified guard mutations from silently passing.

### [Named P3] Some fault tests verify failure but not error identity

For example, injected manifest failures use generic `is_err()` assertions at
[`dataset.rs:8687`](../../../crates/txn/src/dataset.rs#L8687). Changing the
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
[`dataset.rs:8224`](../../../crates/txn/src/dataset.rs#L8224),
[`commit_log.rs:278`](../../../crates/txn/src/commit_log.rs#L278),
[`dataset.rs:8110`](../../../crates/txn/src/dataset.rs#L8110),
[`concurrent_snapshot_isolation.rs:24`](../../../crates/txn/tests/concurrent_snapshot_isolation.rs#L24),
[`row_id_reservation_restart.rs:37`](../../../crates/txn/tests/row_id_reservation_restart.rs#L37),
[`compaction.rs:84`](../../../crates/txn/tests/compaction.rs#L84),
[`vacuum.rs:68`](../../../crates/txn/tests/vacuum.rs#L68), and
[`query_planner.rs:475`](../../../crates/txn/tests/query_planner.rs#L475).

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

The initial table was a static prediction. The execution and remediation
addenda below supersede it with measured campaign outcomes and explicit
classification of survivors.

## Tool and evidence blockers

- Native transaction tests now run with the configured MSVC and Windows SDK
  environment.
- `cargo-llvm-cov` and `cargo-mutants` were installed and executed; grcov and
  cargo-tarpaulin were not required for the recorded evidence.
- Fuzz CI runs deterministic smoke inputs, not a sustained transaction-state
  fuzz campaign.
- Thorough chaos runs are scheduled/manual rather than part of every CI run.
- Criterion benchmarks provide performance measurements, not mutation or
  correctness assertions.

The original Sol review did not edit source. Subsequent Terra remediation and
verification are recorded in the dated addenda; the remaining limits are
explicitly classified rather than presented as green universal gates.

## 2026-08-16 execution addendum

The previously unavailable coverage and mutation tools were installed and run
against `strata-txn` with `--no-default-features --features
test-fault-injection`.

### Coverage evidence

All 245 unit and integration tests passed. Coverage across the 17 transaction
source files was:

| Metric | Covered | Total | Result |
|---|---:|---:|---:|
| Lines | 10,433 | 11,027 | 94.61% |
| Functions | 780 | 842 | 92.64% |
| Branches | 488 | 664 | 73.49% |

The branch report required the isolated nightly toolchain because LLVM branch
instrumentation is unstable/nightly-only. The repository's pinned stable
toolchain was not changed.

Lowest substantive branch areas were `retention_executor` (50.00%),
`dataset` (67.81%), `live_set_cache` (66.67%), `retention` (69.64%), and
`snapshot` (75.56%). Files reporting `0/0` branches had no instrumented branch
records and are not interpreted as zero coverage.

### Mutation evidence

`cargo-mutants` generated 972 mutants for `strata-txn`. The completed campaign
reported:

| Outcome | Count |
|---|---:|
| Caught | 464 |
| Missed | 152 |
| Timed out | 13 |
| Unviable | 343 |

The baseline passed. Among executable mutants with a conclusive test outcome,
464 of 629 were caught (73.77%); 152 survived and 13 timed out. Unviable
mutants are excluded from that denominator because they could not compile or
were outside the normal build configuration, including many `cfg(loom)`-only
helpers.

The surviving set is not a bug count. It contains likely test gaps in conflict
boundary handling, attempt-ID migration, compaction, zone-map merging,
filter-key sizing, query validation, retention boundaries, and row-ID
persistence, plus Loom-only/configuration-specific mutations requiring a
separate Loom-scoped campaign or classification.

The campaign results are recorded in the local, untracked `mutants.out/`
artifact directory and were not added to the repository.

### Loom follow-up evidence

The previously unbounded committer-vs-committer model
(`a_failed_commits_segment_is_never_searchable_under_concurrent_commits`) was
converted to a source-explicit `preemption_bound = Some(3)` gate after the
unbounded run exhausted Windows resources without completing. The exact model
passed at bounds 1, 2, and 3; those prior runs took approximately 2.04s,
13.81s, and 58.65s respectively. Subsequent bound-3 runs took approximately
59.50s, 79.06s, and 81.03s. The test now witnesses both post-commit
observation orders—failing committer first and succeeding committer first—
across Loom executions, and asserts that both were observed. Each marker is
placed after its `commit()` returns, so this does not claim to record internal
commit completion or lock-acquisition order. The complementary
reader-vs-failed-committer model was run with `LOOM_MAX_PREEMPTIONS=3` and
passed, but its source still uses default `loom::model(...)`; the environment,
rather than a source-assigned bound, supplied that run's limit. These records
describe those model-specific runs and do not make universal claims.

The independent Terra review found and the follow-up resolved the stale Loom
bound documentation and the missing order-coverage witness. The current
verification evidence is: the complete `strata-txn` suite passed (233 unit
tests, all integration tests, and 6 doctests), workspace check and clippy
passed with `-D warnings`, and the exact bounded Loom model passed.

## 2026-08-17 remediation addendum

The Sol remediation plan was executed as seven bounded Terra test slices. The
changes are test-only; no production behavior was changed.

| Slice | Scope | Focused evidence |
|---|---|---|
| 1 | Attempt-ID migration | 9/9 selected mutants caught; persisted counters, malformed names, highest-prefix selection, and overflow are covered. |
| 2 | Recovery and physical integrity | 43/45 selected mutants caught; 2 were unviable. Capacity, physical schema, manifest paths, row ranges, and segment ownership are covered. |
| 3 | Retention, row IDs, and vacuum | 10 caught; 2 Loom-only survivors, 2 unviable mutants, and 2 guard-path timeouts remain classified rather than treated as normal misses. |
| 4 | Compaction and zone maps | 36 caught; the genuine empty-schema compaction survivor was killed by a reopen-and-scan test. Remaining misses are equivalent, unreachable, or tuning/run-index behavior; one mutation timed out. |
| 5 | Filter identity and cache budget | All 21 meaningful selected mutants caught; one whole-method replacement was unviable. |
| 6 | Query validation | Independent tests kill both original `columns` and `logical_type` survivors; production implementations remain unchanged. |
| 7 | Snapshot semantics | Independent tests cover version/ownership, segment inventory, lookup, vector type rejection, aggregates, three-valued filters, and comparison boundaries. |

The 152 original missed mutants were therefore not 152 bugs. The practical
normal-code gaps were addressed with behavior-level tests; configuration-only
Loom helpers, equivalent mutations, unreachable guards, and mutation timeouts
remain explicitly classified and are not claimed killed by the normal
campaign.

Fresh full `strata-txn` verification passed: 280 instrumented tests under the
nightly branch-coverage run, 268 tests in the pinned stable no-default-feature
run, all integration tests, and 6 doctests. Nightly LLVM coverage is now:

| Metric | Covered | Total | Result |
|---|---:|---:|---:|
| Lines | 11,839 | 12,357 | 95.62% |
| Functions | 827 | 884 | 93.55% |
| Branches | 513 | 680 | 75.44% |

Compared with the initial addendum, this is +1.01 percentage points in lines,
+0.91 in functions, and +1.95 in branches.

