# Strata-Txn Sol Deterministic Simulation and State-Space Audit

Date: 2026-08-27
Scope: `crates/txn`, its direct concurrency/fault harness boundary, loom,
chaos, and simulation CI recipes  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 7 mainline at `d36f9f1`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The bounded deterministic state-space
evidence required for the supported single-process/shared-`Dataset` contract
is implemented and retained in CI. Audit 7's indeterminate-publication model,
the transaction-overlay and migration-exclusivity models, direct model
enumeration, pinned toolchain, and retained logs close the previously missing
transaction state transitions. This is not a claim of a full DST hypervisor,
complete OS-schedule replay, universal power-loss simulation, or
cross-process coordination.

## Findings

### [Named limit] A seed does not replay the complete OS execution

Locations:

- [`chaos-worker/src/main.rs:1`](../../../crates/chaos-worker/src/main.rs#L1)
- [`chaos-worker/src/main.rs:209`](../../../crates/chaos-worker/src/main.rs#L209)
- [`chaos-worker/src/reader.rs:195`](../../../crates/chaos-worker/src/reader.rs#L195)
- [`storage/src/chaos.rs:23`](../../../crates/storage/src/chaos.rs#L23)
- [`tests/sim/tests/chaos.rs:648`](../../../tests/sim/tests/chaos.rs#L648)

Chaos seeds control operation data, but OS scheduling remains nondeterministic.
Schedule-dependent live-row selection and the process-global crash-checkpoint
ordinal can change between identical runs. Time and filesystem behavior are
real; there is no virtual clock, scheduler, or environment controller.

Chaos supports seed and abort-ordinal reruns, but not complete schedule replay.
The OS scheduler, wall clock, and filesystem are not virtualized, so a seed is
a reproducible scenario input and crash checkpoint, not a complete schedule
transcript. Full DST scheduler replay is outside the embedded single-node
product boundary.

### [Resolved P1] Critical post-publication failure is deterministically modeled

Locations:

- [`storage/src/backend/local.rs:368`](../../../crates/storage/src/backend/local.rs#L368)
- [`storage/src/manifest.rs:734`](../../../crates/storage/src/manifest.rs#L734)
- [`txn/src/dataset.rs:456`](../../../crates/txn/src/dataset.rs#L456)

The final-name hard-link publication path can report an uncertain directory
synchronization result. `Transaction::commit` reconciles the verified-visible
candidate's commit-log entry and current snapshot before returning the typed
indeterminate error. The Loom model at
`dataset::loom_tests::indeterminate_reconciliation_precedes_readers_and_a_subsequent_publisher`
invokes the same `complete_visible_publication` helper and represents the
separate commit-log mutex, snapshot atomic, error-return boundary, reader, and
following publisher. Its rendezvous is blocking rather than a polling loop.
Focused fault-injection tests additionally cover storage, transaction,
compaction, and migration.

### [Named limit] Loom models synchronization state, not external I/O

Locations:

- [`txn/src/dataset.rs:107`](../../../crates/txn/src/dataset.rs#L107)
- [`txn/src/dataset.rs:10556`](../../../crates/txn/src/dataset.rs#L10556)
- [`txn/src/row_id.rs:366`](../../../crates/txn/src/row_id.rs#L366)
- [`txn/src/lifecycle_coordination.rs:271`](../../../crates/txn/src/lifecycle_coordination.rs#L271)

Under Loom, production snapshot publication is represented by the repository's
documented `SnapshotCell` shim and instrumented synchronization primitives.
Filesystem, Arrow, and index operations remain real and uninstrumented. The
models therefore prove ordering and invariants at the transaction boundary;
they do not claim to simulate arbitrary external I/O.

### [Resolved P2] Required transaction models and state-space bounds are retained

CI directly builds the crate-scoped `strata-txn` test binary with `--cfg loom`
and enumerates exact model names before executing each with one test thread.
The retained list includes transaction overlay privacy, migration exclusivity,
indeterminate reconciliation, atomic row/index visibility, row-ID monotonicity,
retention, lifecycle coordination, and the existing conflict models. Explicit
preemption bounds remain documented in the source; they are finite safety
models, not exhaustive exploration of all thread counts. The pinned toolchain
and 360-minute CI timeout make this gate repeatable and diagnosable.

### [Named limit] Full deterministic replay and virtual fault environment remain out of scope

Chaos child processes now have a 60-second per-worker deadline; a timed-out
worker is killed, its pipes are drained, and the failure includes the exact
seed/abort rerun diagnostics. CI retains Loom/chaos evidence. There is still no
virtual
clock, deterministic scheduler, virtual WAL, arbitrary volume-prefix rollback
simulator, or replayable checkpoint format. Those capabilities are outside the
current embedded local contract.

### [Resolved P3] Coverage comments describe the finite model bounds

The Loom module now documents each explicit preemption bound and why the
full-stack model is finite. See
[`dataset.rs:10600`](../../../crates/txn/src/dataset.rs#L10600).

## Positive evidence

- CI enumerates targeted transaction Loom models for same-row conflicts,
  disjoint commits, failed publication, atomic row/index visibility, row-ID
  allocation, retention, and lifecycle coordination.
- Chaos uses real child processes, eight writers plus a reader, reopen
  validation, tombstone checks, lost/phantom accounting, and row/index checks.
- Deterministic RNG streams have golden tests; chaos pre-draws `(seed,
  abort_at)` pairs and provides single-seed reruns.
- Commit-log property testing runs 2,000 generated boundary cases at
  [`commit_log.rs:278`](../../../crates/txn/src/commit_log.rs#L278).
- CI retains Loom and chaos logs for 90 days.

The exact Loom model list is executed by the current CI workflow, which retains
the model and chaos logs for 90 days. The expensive thorough-chaos sweep remains
an explicitly scheduled gate rather than a claim attached to every pull request.

## Verification status

The current CI recipe builds the crate-scoped test binary with:

```text
cargo rustc -p strata-txn --lib --profile test --message-format=json -- --cfg loom
```

It then verifies every named model is present and executes it with
`--exact --test-threads=1`, including:

```text
dataset::loom_tests::transaction_read_overlay_stays_private_while_disjoint_and_contested_writes_commit
dataset::loom_tests::migration_exclusivity_rejects_a_stale_schema_commit_or_migrates_its_published_rows
dataset::loom_tests::indeterminate_reconciliation_precedes_readers_and_a_subsequent_publisher
```

The same workflow retains the Loom log, chaos log, and provenance artifacts for
90 days. Focused recovery-test results are recorded in the Audit 7 report.
These checks establish the finite, supported state-space contract; they do not
turn the explicitly named scheduler and virtual-I/O limits into claims.

Fresh local verification on this branch:

| Command | Result |
|---|---|
| `cargo fmt --check` | Exit 0 |
| `git diff --check` | Exit 0 |
| `cargo test -p strata-txn --no-default-features` | Exit 0; 269 unit tests passed, 1 scheduled stress test ignored; all integration tests and 6 doctests passed |
| `cargo test -p strata-sim --no-default-features fast_tier_random_seeds_survive_random_crash_points -- --exact --nocapture` | Exit 0; 1 test passed, 30 crash-seed iterations completed |

## Representative mutation/state-space assessment

Covered by the current targeted models and tests:

- removal of same-row conflict detection;
- split row/index publication;
- failed pre-manifest segment leakage;
- row-ID overlap;
- lifecycle exclusivity violations.
- exposure of staged transaction reads;
- stale schema publication during migration;
- publication before in-memory reconciliation.

Remaining named-limit scenarios:

- a complete OS schedule transcript from one chaos seed;
- filesystem and Arrow behavior inside the Loom state space;
- executions requiring more than the configured finite preemption bounds;
- schedule-dependent crash-checkpoint remapping.

Strata's immutable files plus versioned manifest are a functional WAL
equivalent for publication, but there is no explicit virtual WAL/storage model
enumerating publication and durability failures.

Cross-process correctness, power-loss semantics, object storage, and
scheduled-only exhaustive chaos remain explicit scope limits and must not be
described as covered.

The bounded state-space and deterministic-input evidence is implemented within
the supported product boundary. A full DST scheduler, virtual filesystem, or
cross-process coordinator would require a separate design and is not implied by
this audit closure.

