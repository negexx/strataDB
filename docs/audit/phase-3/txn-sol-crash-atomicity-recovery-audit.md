# Strata-Txn Sol Crash Atomicity, Fault Injection, and Recovery Audit

Date: 2026-08-27
Scope: `crates/txn` and the storage publication/recovery APIs it directly
depends on  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 6 mainline at `3f9d5e6`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The former P1 recovery-state defect is
resolved in the current transaction and storage publication paths, with
focused fault-injection and recovery regressions. The remaining P2/P3 items
are explicit product boundaries rather than unaddressed crash-atomicity
defects.

Strata does not use a WAL in this path. Its durability model is based on
immutable Arrow row files, immutable HNSW segments, versioned manifests, and a
separate immutable row-ID high-water collection.

## Findings

### [Resolved P1] Final-name publication sync failure is reconciled before returning

Locations:

- [`crates/storage/src/backend/local.rs:368`](../../../crates/storage/src/backend/local.rs#L368)
- [`crates/txn/src/dataset.rs:2230`](../../../crates/txn/src/dataset.rs#L2230)
- [`crates/storage/src/manifest.rs:502`](../../../crates/storage/src/manifest.rs#L502)

The local backend publishes the immutable manifest by linking its temporary
file to the final name with `put_if_absent`/`fs::hard_link`, then synchronizes
the owned directory chain, so the sync result can be uncertain. `commit_manifest_with`
now verifies the exact final-name bytes and returns a typed
`StorageError::PublicationIndeterminate` only when those bytes are readable.
`Transaction::commit` installs the verified candidate snapshot and commit-log
entry while holding `commit_lock` before returning the typed indeterminate
error. A same-handle retry therefore cannot overwrite the immutable candidate
or derive an unrecorded version.

The behavior is regression-covered by
`dataset::tests::indeterminate_manifest_publication_installs_the_candidate_before_reporting_it`,
`storage::manifest::tests::commit_manifest_reports_indeterminate_after_final_name_creation_sync_failure`,
and the compaction/schema-migration equivalents. The API requires callers to
use different recovery rules for dataset creation and ordinary transactions.
`Dataset::create` returns no handle on indeterminate initial publication, so
callers must use `Dataset::open` before retrying creation. An ordinary
`Transaction::commit` makes that transaction terminal, installs its candidate
and write-set into the existing shared handle, and must not replay the same
transaction; subsequent transactions may continue on that handle subject to
normal OCC. Dataset-level compaction and schema migration likewise reconcile
their candidate before returning the typed error, after which callers should
drop/reopen to inspect the recovered version and schema. These are typed,
bounded local-filesystem contracts, not universal power-loss proofs.

### [P2] Arbitrary byte-prefix or volume rollback is unsupported

Recovery has no external monotonic epoch, WAL, undo/redo log, or double-write
area. If rollback removes the newest manifest, recovery may select an older
valid manifest; if rollback leaves truncated or corrupt current objects,
recovery fails closed. Detecting every valid older-state rollback is outside
the documented local ordered-operation guarantee and is not currently tested.

### [P3] Failed preparation intentionally leaves physical orphans

Locations:

- [`crates/txn/src/dataset.rs:2312`](../../../crates/txn/src/dataset.rs#L2312)
- [`crates/txn/src/row_id.rs:189`](../../../crates/txn/src/row_id.rs#L189)

Row IDs are reserved and row/segment files are written before conflict
checking and manifest publication. Conflicts, injected failures, and panics
can leave unreachable files and permanent row-ID gaps. Manifest reads do not
expose them; recognized objects can later be inventoried/vacuumed. Arbitrary
orphan cleanup and bounded growth remain explicit limits.

## Confirmed recovery mechanisms

- Row IDs are durably reserved before row-file writes; uncertain reservation
  publication consumes the range.
- Conflict and live-target checks precede manifest publication
  ([`dataset.rs:2049`](../../../crates/txn/src/dataset.rs#L2049)).
- One manifest contains both new row-file and segment references, providing the
  shared visibility boundary
  ([`dataset.rs:2098`](../../../crates/txn/src/dataset.rs#L2098)).
- Recovery validates manifest checksum/version, row-file length/CRC/schema/
  ownership, tombstones, segments, and row-ID high-water state
  ([`manifest.rs:252`](../../../crates/storage/src/manifest.rs#L252),
  [`dataset.rs:941`](../../../crates/txn/src/dataset.rs#L941)).
- Temporary manifests are ignored; a malformed highest renamed manifest fails
  closed instead of silently falling back.

## Representative mutation assessment

| Mutation | Static assessment |
|---|---|
| Skip manifest directory synchronization | Covered by the final-name publication sync-failure regression and typed indeterminate result |
| Publish before conflict check | Covered by conflict/orphan regressions |
| Expose uncommitted rows or segments | Rejected by manifest-only visibility and reopen tests |
| Reuse abandoned row IDs | Covered by reservation-failure and process-restart tests |
| Accept CRC mismatch | Covered for manifests, row-ID records, row files, and truncated segments |
| Recover arbitrary byte-prefix rollback | Survives as an unsupported scenario |

## Verification status

The current branch has fresh focused verification for the publication and
recovery paths. The report does not claim arbitrary volume rollback, universal
power-loss behavior, or cross-process coordination.

| Command | Result |
|---|---|
| `cargo test -p strata-storage --no-default-features --features test-fault-injection --lib manifest::tests::commit_manifest_reports_indeterminate_after_final_name_creation_sync_failure -- --exact` | Exit 0; 1 test passed; the post-rename sync fault is classified as indeterminate and the final manifest remains readable. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --lib dataset::tests::indeterminate_manifest_publication_installs_the_candidate_before_reporting_it -- --exact` | Exit 0; 1 test passed; the shared handle installs the verified candidate before returning the typed error. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test compaction compaction_indeterminate_publication_installs_candidate_before_returning_error -- --exact` | Exit 0; 1 test passed; compaction applies the same reconciliation boundary. |
| `cargo test -p strata-txn --no-default-features --features test-fault-injection --test schema_migrations migration_indeterminate_publication_installs_schema_before_returning_error -- --exact` | Exit 0; 1 test passed; schema publication applies the same reconciliation boundary. |

The exact hosted CI evidence for this branch is recorded by its pull request;
local results above are the focused behavioral evidence for the audit finding.

