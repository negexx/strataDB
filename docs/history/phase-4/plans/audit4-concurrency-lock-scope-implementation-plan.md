# Audit 4 Concurrency and Thread-Safety Implementation Plan

## Objective

Close the remaining indeterminate manifest-publication defect in compaction and schema migration,
then add exact-production concurrency evidence without expanding Strata's supported boundary.

## Task 1 — reconciliation tests and helper

- Add failing fault-injection tests for compaction and migration when the final manifest name is
  visible but directory synchronization reports an indeterminate error.
- Add one private publication/reconciliation helper in `crates/txn/src/dataset.rs` and route
  compaction and migration through it.
- Install the candidate commit-log entry and snapshot before returning the typed indeterminate error.
- Do not run reclamation after indeterminate publication; leaving old objects is safe for retry.

Files: `crates/txn/src/dataset.rs`, `crates/txn/tests/compaction.rs`,
`crates/txn/tests/schema_migrations.rs`.

## Task 2 — production-primitive concurrency stress

Create `crates/txn/tests/concurrency_stress.rs` with an ignored, configurable test using one shared
`Dataset`, one writer, and at least four readers. Readers must perform non-vacuous checks that
snapshot versions never decrease and each observed snapshot is complete. Use
`STRATA_CONCURRENCY_STRESS_COMMITS` with a bounded default and emit a stable completion sentinel.
No sleeps may establish correctness.

## Task 3 — retained evidence lanes

Update `.github/workflows/ci.yml` with scheduled/manual ARM64 and ThreadSanitizer lanes. Each lane
must pin the relevant toolchain, run the targeted isolation/stress commands, and retain the commit
revision, runner architecture, versions, command, counts, and completion sentinel. Configuration is
not evidence of a pass; unavailable or failing lanes remain explicitly reported.

## Task 4 — independent review and verification

A separate Terra review checks candidate installation exactly once, no publication-before-durability,
no stale snapshot, and no claims beyond the shared-handle boundary. Then run focused fault-injection
tests, workspace tests, clippy, fmt, docs, and diff checks. Refresh the Audit 4 report only from fresh
outputs.

## Non-goals

Do not narrow lifecycle lock scope, add cross-process coordination, add serializability, alter the
on-disk format, add dependencies, or claim FIFO/independent-opener/weak-memory guarantees.
