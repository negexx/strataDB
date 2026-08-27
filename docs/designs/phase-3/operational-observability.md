# Operational observability design

Status: implemented within named local/shared-handle bounds

## Scope

Strata is an embedded, single-process engine. Operational observability
provides a bounded in-memory event journal attached to a shared `Dataset`
handle. It is intended for an application-owned exporter, metrics adapter, or
test harness; it is not a durable security log and does not coordinate
independent openers of the same path.

## Contract

- Events have a monotonic per-handle sequence number and a small allow-listed
  kind/outcome pair. They contain no paths, schemas, row IDs, credentials, or
  caller-provided strings.
- The journal has a fixed capacity. When full, the oldest event is evicted and
  an atomic dropped counter increments; recording never blocks engine progress
  on an external sink.
- Callers can take a filtered snapshot or drain a filtered batch. Drain is the
  hand-off boundary for an application-owned asynchronous exporter.
- Transaction events are emitted for begin, commit, conflict, and failure.
  Dataset creation/open and lifecycle operations emit success/failure events
  where a handle exists to receive them.
- The journal is shared by clones of one `Dataset` and is intentionally reset
  by `Dataset::open`; it is not a cross-process or durable history.

## Rejected alternatives

An internal background thread and a durable audit sink were rejected for this
slice: they would add shutdown/backpressure/error-delivery semantics and a
cross-process ordering expectation without a product requirement. The drain
API lets the embedding application choose its own asynchronous transport.

## Verification obligations

Tests cover sequence monotonicity, filtering, bounded eviction and drop
accounting, drain semantics, clone sharing, and transaction success/conflict/
failure hooks. The audit report records the remaining local-only limits.
