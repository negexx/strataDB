# Strata-Txn Sol Operational Event Auditing and Observability Audit

Date: 2026-08-15  
Scope: `crates/txn` runtime operations, lifecycle/recovery events, and direct
CLI/bindings observability surfaces  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 data-integrity defect was found, but operational database
event auditing and observability are not implemented.

## Findings

### [P1] No operational audit pipeline exists

There is no audit-event type, centralized emitter, operation filter, ring
buffer, asynchronous offload worker, sink, or flush/backpressure policy.
Critical paths have no audit records:

- Dataset create/open: [`dataset.rs:818`](../../../crates/txn/src/dataset.rs:818),
  [`dataset.rs:945`](../../../crates/txn/src/dataset.rs:945)
- Transaction begin/commit/conflict/failure:
  [`dataset.rs:1347`](../../../crates/txn/src/dataset.rs:1347),
  [`dataset.rs:2004`](../../../crates/txn/src/dataset.rs:2004),
  [`dataset.rs:2052`](../../../crates/txn/src/dataset.rs:2052)
- Migration and lifecycle operations:
  [`dataset.rs:1192`](../../../crates/txn/src/dataset.rs:1192),
  [`maintenance.rs:59`](../../../crates/txn/src/maintenance.rs:59),
  [`vacuum.rs:29`](../../../crates/txn/src/vacuum.rs:29)
- Row-ID reservation and recovery:
  [`row_id.rs:189`](../../../crates/txn/src/row_id.rs#L189),
  [`dataset.rs:945`](../../../crates/txn/src/dataset.rs:945)

Failures, conflicts, abandoned ranges, corruption detections, recovery
outcomes, and lifecycle mutations are visible only to the immediate caller.

### [P1] Partial lifecycle effects can lose terminal evidence

[`maintenance.rs:69`](../../../crates/txn/src/maintenance.rs:69) executes
compaction, retention, vacuum, and inventory using `?`. If an earlier phase
publishes successfully and a later phase fails, the method returns only an
error and does not preserve a durable partial-work record.

### [P2] `CommitLog` is not an audit log

[`commit_log.rs:1`](../../../crates/txn/src/commit_log.rs:1) records only
successful commit versions/write sets after manifest durability. It omits
begin, failure, conflict, recovery, migration, and lifecycle events, evicts
entries, and is recreated empty on create/open. The only explicit metric is the
handle-local insufficient-history counter.

### [P2] No audit identity, ordering, durability, or privacy contract

Manifest versions order successful publications but provide no stable dataset
or transaction identity, actor/correlation ID, operation kind, failure record,
global lifecycle sequence, or delivery acknowledgement. No configuration
defines filtering, capacity, overflow, retention, sink, shutdown flushing,
redaction, or ownership. Caller-visible errors can expose paths, schemas,
object names, and contested row IDs.

### [P3] CLI, bindings, and CI expose status, not event history

The CLI exposes lifecycle/recovery snapshots, not historical events. Python
exposes transaction APIs and typed exceptions, not lifecycle counters or event
subscription. CI has no audit completeness, ordering, loss, bypass, privacy,
or backpressure gate.

## Requested pipeline comparison

`operation -> filter -> memory/ring buffer -> async offload` is absent:

- Operation hooks: absent.
- Filter: absent.
- Memory buffer: OCC `CommitLog` exists but is semantically unsuitable.
- Async offload: absent.
- Durable/exported sink: absent.

There is currently no observability I/O overhead because no observability sink
exists. Enterprise authentication, SQL DDL/DML audit semantics, network
sessions, and cross-process event ordering are outside the embedded scope.

## Mutation assessment

- Removing a conflict event is undetectable because no conflict event exists.
- Hiding a failed commit is not observable by the audit layer.
- Dropping a recovery event is undetectable; recovery tests validate state only.
- Reordering lifecycle events is inapplicable because only synchronous reports
  exist.
- Removing the insufficient-history counter increment is detected by its
  focused test.

## Positive evidence

Typed conflicts, checksummed successful manifest versions, recovery validation,
and synchronous lifecycle/compaction/retention/vacuum reports provide useful
caller evidence. Scope documentation correctly limits Strata to embedded,
local, one-process/shared-handle operation.

No files were edited by the Sol reviewer. Implementing audit events requires a
Sol design for event schema, identity/order, durability, partial-success
semantics, filtering, overflow, privacy, ownership, configuration, and CLI/
binding exposure.

