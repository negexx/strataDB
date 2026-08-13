# Phase 6 Concurrent-Writes Ideas

Status: retained ideas only; no implementation plan is approved by this note.

The historical concurrent-writes work identified useful correctness questions, but its
implementation is superseded by the current `main` architecture. Current work must preserve the
write phase outside `commit_lock`, immutable vector segments, persisted attempt allocation,
bounded commit history, typed insufficient-history errors, manifest validation, and lifecycle
coordination. Do not rebase or merge the historical implementation wholesale.

## Ideas worth preserving

1. **Deterministic concurrent-writer interleavings.** Add barrier-controlled tests for disjoint
   commits, same-row conflicts, and a commit that loses publication authority. Tests should exercise
   the current preparation/write/publication path rather than serializing all work under
   `commit_lock`.
2. **Typed conflict evidence.** Conflicts should identify the contested physical row IDs and
   distinguish a genuine write-write conflict from insufficient commit history or invalid target
   state.
3. **Target-state validation.** Delete and update paths should reject stale, tombstoned, or
   never-published targets against the transaction's base snapshot with typed errors.
4. **Attempt-identity durability.** Keep regression coverage proving that prepared filenames and
   row-ID reservations remain unique across concurrent attempts and restart. Use the persisted
   allocator already owned by current `main`; do not reintroduce process-local timestamp/PID names.
5. **Tombstone recovery integrity.** Reopen tests should prove that committed tombstones remain
   durable, malformed or orphan tombstone IDs fail closed, and older snapshots retain their
   documented visibility.
6. **Retry and history boundaries.** Exercise real contention where possible, verify bounded retry
   behavior, and prove that an evicted history range returns the distinct typed insufficient-history
   error. Test-only decoy snapshots are not sufficient evidence of concurrent CAS loss.
7. **Publication and index ordering.** Keep tests that prove a failed or unpublished commit cannot
   become visible through scans or vector search, and that index mutation cannot leave a published
   snapshot in an inconsistent state.

## Explicitly do not port

- A `CommitHistory` implementation that replaces the current bounded history model.
- The historical `ArcSwap`/delta-log replay architecture.
- An unvalidated `HashSet` tombstone representation.
- A global lock held across filesystem-heavy preparation, which would remove the concurrency the
  tests are intended to measure.
- Process-local timestamp/PID/counter filename allocation.
- Transaction read APIs or a new isolation contract without a superseding design decision.

## Suggested next step

Sol should first produce a current-`main` concurrency audit centered on `Transaction::write_phase`,
the publication CAS, commit-history retention, and lifecycle coordination. Terra can then add one
deterministic regression slice at a time, with a separate review and loom evidence for every
interleaving-sensitive change.
