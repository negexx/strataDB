# Strata-Txn Sol Operational Event Auditing and Observability Audit

Date: 2026-08-27
Scope: `crates/txn` runtime operations, lifecycle/recovery events, and direct
CLI/bindings observability surfaces  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 9 head `eb19c7d`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** A bounded, redacted, shared-handle event
journal records transaction and lifecycle outcomes and exposes filtered
snapshot/drain hand-off APIs. It is intentionally in-memory and
application-exported; it is not a durable security audit log or a
cross-process event bus.

## Findings

### [Resolved P1] A bounded operational event pipeline now exists

`OperationalEventLog` provides an event type, allow-listed operation/outcome
taxonomy, bounded ring, filters, snapshot/drain hand-off, and atomic overflow
accounting. Transaction begin/commit/conflict/failure and handle-owned
lifecycle outcomes are recorded without putting an external sink on a commit
path. The application owns asynchronous offload after `drain`.

The journal is shared by clones of one `Dataset`, resets on `open`, and
contains no paths, schemas, row IDs, or caller strings. Critical paths are
hooked at:

- Dataset create/open: [`dataset.rs:818`](../../../crates/txn/src/dataset.rs#L818),
  [`dataset.rs:945`](../../../crates/txn/src/dataset.rs#L945)
- Transaction begin/commit/conflict/failure:
  [`dataset.rs:1347`](../../../crates/txn/src/dataset.rs#L1347),
  [`dataset.rs:2004`](../../../crates/txn/src/dataset.rs#L2004),
  [`dataset.rs:2052`](../../../crates/txn/src/dataset.rs#L2052)
- Migration and lifecycle operations:
  [`dataset.rs:1192`](../../../crates/txn/src/dataset.rs#L1192),
  [`maintenance.rs:59`](../../../crates/txn/src/maintenance.rs#L59),
  [`vacuum.rs:29`](../../../crates/txn/src/vacuum.rs#L29)
- Row-ID reservation and recovery:
  [`row_id.rs:189`](../../../crates/txn/src/row_id.rs#L189),
  [`dataset.rs:945`](../../../crates/txn/src/dataset.rs#L945)

Failures and conflicts are visible through the bounded journal as typed
categories. Evictions are observable through `operational_events_dropped`.

### [Resolved P1] Lifecycle terminal outcomes are observable

Lifecycle methods emit a success or failure category at their handle-owned API
boundary. This records the terminal outcome of the call, not a durable
per-suboperation journal and not a claim that a partially completed maintenance
run is atomic.

### [Resolved P2] `CommitLog` remains separate from the operational journal

`CommitLog` continues to serve OCC only. The operational journal owns event
history and does not expose write sets or contested row IDs.

### [Resolved P2] Identity, ordering, capacity, and privacy are explicit

Per-handle sequence IDs order events. Fixed capacity, eviction accounting,
filtering, drain ownership, and redaction are part of the public contract;
there is no durable delivery acknowledgement. Application exporters own
retention, shutdown, and transport policy.

### [Named limit] CLI and bindings remain status surfaces

The Rust transaction crate exposes the event journal. CLI/Python event export is
not added in this slice; they retain status/error surfaces. Rust tests cover
ordering, filtering, loss accounting, redaction, clone sharing, and hooks.

## Requested pipeline comparison

`operation -> filter -> memory/ring buffer -> application async offload` is
implemented:

- Operation hooks: transaction and handle-owned lifecycle hooks.
- Filter: allow-listed kind/outcome filter.
- Memory buffer: fixed-capacity event ring with drop accounting.
- Async offload: caller-owned after filtered drain.
- Durable/exported sink: intentionally caller-owned and outside this crate.

The journal adds bounded in-process synchronization and allocation work on
event-producing paths; no filesystem I/O occurs. Enterprise authentication,
SQL DDL/DML audit semantics, network sessions, durable compliance logs, and
cross-process event ordering remain outside the embedded scope.

## Mutation assessment

- Removing a conflict event is covered by the conflict-hook regression.
- Hiding a failed commit is covered by the failure-hook regression.
- Ring overflow is observable through the dropped counter.
- Filtered drain preserves sequence order and removes only returned events.
- Removing the insufficient-history counter increment is detected by its
  focused test.

## Positive evidence

Typed conflicts, checksummed successful manifest versions, recovery validation,
and synchronous lifecycle/compaction/retention/vacuum reports provide useful
caller evidence. Scope documentation correctly limits Strata to embedded,
local, one-process/shared-handle operation.

## Verification status

Fresh local verification on this branch:

| Command | Result |
|---|---|
| `cargo fmt --check` | Exit 0 |
| `git diff --check` | Exit 0 |
| `cargo test -p strata-txn --test operational_observability --no-default-features` | Exit 0; 2 tests passed |
| `cargo clippy -p strata-txn --all-targets --no-default-features -- -D warnings` | Exit 0 |

The bounded design and implementation are recorded in
[`operational-observability.md`](../../designs/phase-3/operational-observability.md).
Durable export, cross-process ordering, and CLI/binding event subscription
remain deliberate follow-on decisions.

