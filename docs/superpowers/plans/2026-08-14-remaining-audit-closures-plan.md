# Implementation plan: remaining audit closures

Status: DUR-07, PERF-06/07, ARCH-06, and owner-backed ARCH-07 routing are implemented within named
bounds. The row-group format is additive and opt-in; legacy Arrow IPC remains unchanged.

## Scope gate

Do not touch the pre-existing dirty files listed by `git status`, except files explicitly assigned below. ARCH-08 files are preserved and are not part of these tasks.

## Acceptance checklist

- [x] DUR-07 key validation and bounded delete-sync semantics implemented and focused native tests passed.
- [x] PERF-06 projection accounting implemented behind the opt-in test feature and focused native test passed.
- [x] PERF-06 Criterion projected-read benchmark recorded; it showed no speed win on the current IPC fixture.
- [x] PERF-07 additive indexed row-group format, selective projection reads, tests, and benchmark evidence.
- [x] ARCH-06 additive facade DTOs implemented; no breaking API change claimed.
- [x] ARCH-07 owner routing covers manifest, row data, segments, reservations, lifecycle, retention, vacuum, and compaction paths.
- [x] `cargo check --workspace --no-default-features`, workspace clippy, `cargo fmt --check`, and `git diff --check` passed on native Windows with the configured VS 2026 x64 toolchain.

## Terra sequence

1. DUR-07 — `crates/storage/src/backend/mod.rs`, `crates/storage/src/backend/local.rs`, conformance tests, and the narrow durability documentation. Add/adjust red tests for key validation, post-unlink sync error semantics, and the local contract; implement only missing contract behavior.
2. PERF-06 — `crates/storage/src/datafile.rs`, `crates/txn/src/snapshot.rs`, focused txn tests, and an existing benchmark file or a new narrowly scoped benchmark. Add read accounting without changing public result shapes; prove projection plus filter-column loading.
3. PERF-07 — inspect the current datafile format first. If safe row-group metadata and selective reads already exist, add the missing pruning path and tests. Otherwise stop at the current safe boundary and document PERF-07 as still deferred; do not redesign the on-disk format in this slice.
4. ARCH-06 — implemented additively: facade-owned DTO methods sit beside legacy `Snapshot` metadata accessors; removal/deprecation remains a separate compatibility decision.
5. ARCH-07 — implemented as the owner/key-layout seam. A future bounded task must port manifest/datafile/lifecycle I/O through it and add backend-spy coverage.

## Superseding implementation decision

The originally deferred PERF-07 format boundary was subsequently authorized and implemented as
an additive `STRARGR1` indexed row-group container. It does not rewrite legacy Arrow IPC files,
and automatic predicate-to-group pruning is not claimed. ARCH-07 routing was also completed
through `StorageOwner` for manifest, datafile, segment, reservation, lifecycle, retention, vacuum,
and compaction paths. The old execution notes above are historical; this decision controls the
current implementation and verification.

## Review gates

Each Terra task must report files changed, red/green commands, deviations, and blockers. A separate Terra reviewer checks the task diff and tests. Sol performs a final complete-branch review. Luna runs fresh native verification and updates audit evidence only from command output.
