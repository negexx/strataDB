# Audit 4: Concurrency and Thread-Safety

**Status:** Sol-reviewed implementation scope; work in progress.

## Decision

Audit 4 does not narrow lifecycle maintenance's `commit_lock` scope in this slice. Lifecycle
exclusivity intentionally remains a stop-the-world writer barrier: releasing the publication mutex
while compaction, migration, pruning, or vacuum performs I/O would not let a normal commit publish,
because commit preparation is blocked by lifecycle exclusivity. A two-phase lifecycle protocol would
require new stale-authority, deletion, retry, and loom semantics and is outside this bounded audit.

## Scope

The actionable correctness defect is the residual indeterminate-manifest-publication path in
`Dataset::compact` and `Dataset::migrate_schema`. If the final directory synchronization fails after
the manifest is visible, the method must install that already-built candidate into commit history and
the current snapshot before returning the typed indeterminate error. Ordinary transaction commit
already has this reconciliation behavior.

Audit 4 also adds evidence for the exact production `ArcSwap` path: a sustained shared-handle
publication stress test plus retained scheduled/manual ARM64 and ThreadSanitizer lanes. Loom remains
an abstract observable-snapshot model, not a model of ArcSwap's internal algorithm.

## Invariants

- A verified-visible manifest candidate is installed exactly once before returning an indeterminate
  publication error.
- A pre-publication or unverified candidate is never installed.
- Row files and immutable vector segments remain one manifest/snapshot publication boundary.
- Lifecycle exclusivity and the existing lock order remain unchanged.
- The supported boundary remains one process using a shared `Dataset` handle; no FIFO, independent
  opener, cross-process, serializability, ARM64, or TSan guarantee is inferred from local tests.

## Verification

Add compaction and migration fault-injection regressions, an ignored non-vacuous ArcSwap stress test,
and hosted evidence lanes that record revision, architecture, toolchain, command, test counts, and a
completion sentinel. The final audit report must distinguish completed reconciliation from evidence
that remains unavailable or failed.
