# Remaining audit closures: bounded durability, query I/O, and architecture

## Objective

Close the implementable portions of DUR-07, PERF-06, PERF-07, ARCH-06, and ARCH-07 while preserving Strata's supported boundary: local single-node storage, one process, and shared `Dataset` handles.

## Decisions

- DUR-07 is closed as a named local contract: `Backend::delete` removes only a validated backend key and, for `LocalFs`, synchronizes the owned containing-directory chain. A post-unlink sync error remains an error and does not claim the delete did not happen. No universal power-loss guarantee is claimed.
- PERF-06 uses the existing projection path and adds observable read accounting plus benchmark evidence. It must prove requested columns and predicate columns are the only data columns loaded; correctness remains unchanged.
- PERF-07 is limited to a format-supported row-group/sub-file slice. If the current Arrow writer does not expose safe row-group metadata, do not fake pruning: retain the existing file-level zone-map pruning and record the format boundary as deferred rather than claiming closure.
- ARCH-06 is partially bounded in this slice. Additive facade DTO views are implemented while existing public low-level types remain source-compatible; deprecation/removal is a separate decision.
- ARCH-07 is partially bounded in this slice. `Dataset` owns a validated `StorageOwner` with canonical key helpers; full routing of manifest/datafile/lifecycle I/O remains a follow-up slice, and no object-storage implementation is claimed.

## Superseding implementation decision

The row-group slice is additive and opt-in, preserving legacy Arrow IPC compatibility. Dataset
owner routing now covers manifest, datafile, segment, reservation, lifecycle, retention, vacuum,
and compaction paths within the existing local/backend capability boundary.

## Invariants and exclusions

- No cross-process coordination, serializability, universal durability, or remote backend is introduced.
- Every behavior change gets a red test first, then implementation, then targeted and workspace verification.
- Loom is required for concurrency-sensitive transaction changes; pure storage/query accounting and API additions do not need a new loom model, with that rationale recorded in the Terra handoff.
- Existing dirty files and the uncommitted ARCH-08 slice are out of scope and must not be reformatted, reverted, or staged.

## Acceptance evidence

- Focused native Windows tests with the repository's no-default-features recipe.
- Workspace tests, clippy, format, and diff checks for the final scoped diff.
- Criterion benchmark output and focused projection counters are required for PERF-06; the current native result is evidence only and showed no speed win. Evidence must state that it is a measurement, not an SLO.
- Audit/ledger/status documentation updated only after code and evidence pass.
