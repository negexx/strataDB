# Strata-Txn Sol Deterministic Simulation and State-Space Audit

Date: 2026-08-15  
Scope: `crates/txn`, its direct concurrency/fault harness boundary, loom,
chaos, and simulation CI recipes  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 was found, but two P1 gaps prevent claiming deterministic
simulation or adequate crash-publication state-space coverage. Current testing
is useful targeted concurrency testing, not a full DST system.

## Findings

### [P1] A seed does not replay the complete execution

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

### [P1] The critical post-publication failure transition is not deterministically modeled

Locations:

- [`storage/src/backend/local.rs:330`](../../../crates/storage/src/backend/local.rs#L330)
- [`storage/src/manifest.rs:370`](../../../crates/storage/src/manifest.rs#L370)
- [`txn/src/dataset.rs:2230`](../../../crates/txn/src/dataset.rs#L2230)

A manifest rename can succeed and directory synchronization then fail. The
transaction returns before updating its in-memory snapshot/OCC log, leaving an
uncertain durable publication. No deterministic model exercises retry,
stale-handle visibility, or version uniqueness after this transition.

### [P2] Loom models abstractions, not the complete transaction state machine

Locations:

- [`txn/src/dataset.rs:105`](../../../crates/txn/src/dataset.rs#L105)
- [`txn/src/dataset.rs:9580`](../../../crates/txn/src/dataset.rs#L9580)
- [`txn/src/row_id.rs:136`](../../../crates/txn/src/row_id.rs#L136)
- [`txn/src/dataset.rs:10781`](../../../crates/txn/src/dataset.rs#L10781)

Under Loom, `ArcSwap` is replaced with a mutex-backed cell. Filesystem, Arrow,
and index operations remain real and uninstrumented. Row-ID and semantic
models explicitly omit durable storage or production commit behavior.

### [P2] State-space bounds and recurring coverage are incomplete

Models at [`dataset.rs:9922`](../../../crates/txn/src/dataset.rs#L9922),
[`dataset.rs:10831`](../../../crates/txn/src/dataset.rs#L10831), and
[`dataset.rs:10942`](../../../crates/txn/src/dataset.rs#L10942) use preemption
bounds of 2, 2, and 3. CI does not pin or archive Loom state-space limits or
checkpoints and omits transaction-overlay and migration-exclusivity models.

### [P2] Replay and hang diagnostics are insufficient

Worker execution uses blocking `Command::output()` without a per-process
timeout. Loom artifacts are logs rather than replayable checkpoints. Chaos
provides seed/abort reruns but not schedule replay.

### [P3] Coverage comments overstate model bounds

The Loom module says two models are bounded and the remainder unbounded, while
three source models have explicit preemption bounds. See
[`dataset.rs:9725`](../../../crates/txn/src/dataset.rs#L9725).

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

Historical reports—not fresh current-head evidence—record expensive Loom
models passing and a 2,000/2,000 thorough chaos run.

## Representative mutation/state-space assessment

Likely killed by existing targeted tests:

- removal of same-row conflict detection;
- split row/index publication;
- failed pre-manifest segment leakage;
- row-ID overlap;
- lifecycle exclusivity violations.

Likely to survive current modeling:

- post-rename sync-failure handling;
- production `ArcSwap` ordering defects;
- durable row-ID storage races;
- executions requiring more than configured preemption bounds;
- schedule-dependent crash-checkpoint remapping.

Strata's immutable files plus versioned manifest are a functional WAL
equivalent for publication, but there is no explicit virtual WAL/storage model
enumerating publication and durability failures.

Cross-process correctness, power-loss semantics, object storage, and
scheduled-only exhaustive chaos remain explicit scope limits and must not be
described as covered.

No files were edited by the Sol reviewer. A complete DST architecture or
uncertain-publication fix requires new Sol design work before Terra
implementation.

