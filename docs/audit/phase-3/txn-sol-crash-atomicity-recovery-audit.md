# Strata-Txn Sol Crash Atomicity, Fault Injection, and Recovery Audit

Date: 2026-08-15  
Scope: `crates/txn` and the storage publication/recovery APIs it directly
depends on  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 was found, but one P1 recovery-state defect and missing fresh
verification prevent approval.

Strata does not use a WAL in this path. Its durability model is based on
immutable Arrow row files, immutable HNSW segments, versioned manifests, and a
separate immutable row-ID high-water collection.

## Findings

### [P1] Post-rename manifest-sync failure can leave the live handle behind disk

Locations:

- [`crates/storage/src/backend/local.rs:330`](../../../crates/storage/src/backend/local.rs:330)
- [`crates/txn/src/dataset.rs:2230`](../../../crates/txn/src/dataset.rs:2230)
- [`crates/storage/src/manifest.rs:502`](../../../crates/storage/src/manifest.rs:502)

The local backend renames the manifest into its final name before synchronizing
the directory chain. That later sync can fail. `Transaction::commit` returns
before updating the OCC log or in-memory snapshot, while recovery selects the
highest renamed manifest. After an uncertain sync error, `Dataset::open` may
see the commit while the existing handle does not. A same-handle retry can
derive the same next version and overwrite the manifest key.

Ordinary commits lack typed uncertainty, handle poisoning/refresh, retry
guidance, and a deterministic post-rename regression. Sol must define the
uncertain-publication semantics before Terra changes implementation.

### [P2] Arbitrary byte-prefix or volume rollback is unsupported

Recovery has no external monotonic epoch, WAL, undo/redo log, or double-write
area. If rollback removes the newest manifest, recovery may select an older
valid manifest; if rollback leaves truncated or corrupt current objects,
recovery fails closed. Detecting every valid older-state rollback is outside
the documented local ordered-operation guarantee and is not currently tested.

### [P3] Failed preparation intentionally leaves physical orphans

Locations:

- [`crates/txn/src/dataset.rs:2312`](../../../crates/txn/src/dataset.rs:2312)
- [`crates/txn/src/row_id.rs:182`](../../../crates/txn/src/row_id.rs:182)

Row IDs are reserved and row/segment files are written before conflict
checking and manifest publication. Conflicts, injected failures, and panics
can leave unreachable files and permanent row-ID gaps. Manifest reads do not
expose them; recognized objects can later be inventoried/vacuumed. Arbitrary
orphan cleanup and bounded growth remain explicit limits.

## Confirmed recovery mechanisms

- Row IDs are durably reserved before row-file writes; uncertain reservation
  publication consumes the range.
- Conflict and live-target checks precede manifest publication
  ([`dataset.rs:2049`](../../../crates/txn/src/dataset.rs:2049)).
- One manifest contains both new row-file and segment references, providing the
  shared visibility boundary
  ([`dataset.rs:2098`](../../../crates/txn/src/dataset.rs:2098)).
- Recovery validates manifest checksum/version, row-file length/CRC/schema/
  ownership, tombstones, segments, and row-ID high-water state
  ([`manifest.rs:252`](../../../crates/storage/src/manifest.rs:252),
  [`dataset.rs:941`](../../../crates/txn/src/dataset.rs:941)).
- Temporary manifests are ignored; a malformed highest renamed manifest fails
  closed instead of silently falling back.

## Representative mutation assessment

| Mutation | Static assessment |
|---|---|
| Skip manifest fsync | Existing sync-order/failure tests should detect it, but the ordinary post-rename case is missing |
| Publish before conflict check | Covered by conflict/orphan regressions |
| Expose uncommitted rows or segments | Rejected by manifest-only visibility and reopen tests |
| Reuse abandoned row IDs | Covered by reservation-failure and process-restart tests |
| Accept CRC mismatch | Covered for manifests, row-ID records, row files, and truncated segments |
| Recover arbitrary byte-prefix rollback | Survives as an unsupported scenario |

## Verification status

The Sol review did not complete a fresh Cargo command before its requested
early return, so no fresh pass is claimed in its report. CI recipes and
historical evidence were inspected for fault injection, row-ID restart, fast
chaos, loom publication models, and scheduled 2,000-seed chaos. Historical
evidence is not current-head evidence.

No files were edited by the Sol reviewer. Implementation requires a focused Sol
design for ordinary-commit uncertain-publication semantics, followed by a
Terra plan and regression coverage.

