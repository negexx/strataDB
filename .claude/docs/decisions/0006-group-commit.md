# ADR 0006 — Group commit for the transaction layer

**Status:** Proposed — needs a human decision. Not implemented.
**Date:** 2026-07-23

## Context

`Transaction::commit` serializes every committer through one `commit_lock`, and the critical
section ends with `commit_manifest`, which writes a temp file, **fsyncs it**, renames it into
place, and then fsyncs the directory. Two fsyncs per commit, inside a global lock.

That makes single-writer throughput a function of fsync latency, not of core count or of how much
work a transaction actually does. Measured on the dev machine with
`bench/benches/manifest_growth_bench.rs` (sequential commits, one small data file each, no vector
column so no HNSW insert is involved):

| Accumulated data files | Mean commit |
|---|---|
| 0–299 | 12.2 ms |
| 1200–1499 | 17.8 ms |
| 3000–3299 | 30.5 ms |
| 5700–5999 | 39.5 ms |

The floor — ~12 ms with an essentially empty manifest — is close to pure fsync cost, i.e. roughly
**80 commits/sec regardless of concurrency**. Everything above that floor is the separate O(F)
manifest-size problem (see `.claude/docs/analysis/2026-07-23-complexity-audit.md`); this ADR is
about the floor, which is present at *every* scale including an empty dataset.

The existing exit-evidence benchmark measured concurrent commits ~18% faster than a sequential
baseline. That is consistent with the above: the parallelizable part (data-file writes, which
already happen *outside* the lock) is real but small next to a serialized ~12 ms fsync.

Relevant constraint, from `.claude/rules/concurrency-txn-layer.md`:

> No write is acknowledged to the caller until it is fsynced, conflict-checked, and durably
> committed. Never add a buffering/batching path that acknowledges before that point, even for
> throughput. If throughput needs improving, that's a design conversation, not a quiet code change.

This ADR is that conversation.

## Decision

**Proposed:** adopt pipelined group commit — batch the *manifest durability step* across
concurrently-committing transactions so that N transactions share one fsync, while every caller
still blocks until the fsync covering **its own** commit has completed.

Sketch:

1. A committer takes `commit_lock`, conflict-checks, and builds its new manifest state exactly as
   today. This part stays serialized; it is cheap.
2. Instead of each committer calling `commit_manifest` under the lock, validated commits are
   appended to a durability queue and the lock is released.
3. One committer acts as group leader: it merges the queued manifest updates, performs a single
   write + fsync + rename + directory fsync, and then signals every participant.
4. Each caller returns `Ok(())` only after the fsync that made *its* commit durable has succeeded.

**This does not weaken the no-silent-buffering invariant.** Nothing is acknowledged before it is
durable. The change is that durability work is *shared*, not *deferred* — callers block for at
least as long as correctness requires, and strictly longer in the leader-waits case. That is the
difference between batching and buffering, and it is the whole reason this is proposable at all.

## Alternatives considered

- **Do nothing.** Defensible today: ~80 commits/sec may be well past what a handful of local agent
  processes need, and the O(F) manifest growth is the larger effect at realistic dataset sizes.
  This is the honest default if the workload never approaches the floor. Rejected as a *permanent*
  answer only because the floor is scale-independent — it does not improve as the implementation
  gets better anywhere else.
- **Relax fsync (e.g. fsync every N commits, or rely on rename atomicity alone).** Rejected
  outright: directly violates the durability invariant, and would make the flagship "no write
  acknowledged until durable" claim false.
- **Move `commit_manifest` outside the lock without batching.** Rejected: two committers both
  reading version N would both write `{N+1}.manifest` through the same temp path, so the loser's
  rename can clobber an already-acknowledged manifest, and a crash can leave a durable,
  never-conflict-checked version that `read_current` adopts (highest-version-wins). Breaks the
  single-CAS commit rule.
- **Shrink what gets fsynced (incremental manifests).** Complementary, not an alternative — it
  attacks the O(F) growth *above* the floor, not the floor itself. Should be decided separately.
- **Optimistic lock coupling / speculative snapshot construction.** Rejected as a throughput fix:
  it moves the manifest *clone* out of the lock but not the fsync, which is the dominant term. It
  also cannot move the HNSW graph application out, since that is shared mutable state with no undo.

## Consequences

- Positive: write throughput decouples from per-commit fsync latency and scales with batch size;
  the improvement applies at every dataset size, including an empty one.
- Positive: no change to the conflict-detection model, isolation level, or the single atomic rename
  that publishes a version.
- Negative: **one manifest version no longer corresponds to one transaction.** `CommitLog` is keyed
  by version and `conflicts_with` reasons over version ranges; a group sharing a version breaks that
  mapping, and the loom tests encode the one-writer-one-version model too.
- Negative: **version allocation must be decoupled from the published snapshot.** Once validated
  transactions release the lock before publishing, the next committer reading
  `latest_snapshot.version` sees a version that queued-but-unpublished commits have already claimed.
- Negative: **failure becomes a cascade.** If the group's fsync fails, every participant must fail,
  and any transaction that validated against queued-but-not-yet-durable state must fail with it.
  Today a failed commit affects exactly one transaction.
- Negative: more moving parts in the subsystem whose entire purpose is being obviously correct.
- Neutral: the data-file writes that already happen outside the lock are unaffected.

## Open questions — must be answered before this is accepted

1. Does one version cover a whole group, or does each transaction keep its own version with the
   group sharing only the fsync? The second preserves `CommitLog`'s keying but complicates the
   on-disk manifest sequence.
2. How is a group's fsync failure surfaced, and what is the recovery state on disk?
3. What is the batching policy (leader waits a bounded interval? drains whatever is queued?), and
   how is added latency for a lone committer bounded?
4. What does the loom model look like once one lock acquisition no longer implies one published
   version? The existing three loom tests all assume it.
5. Is the workload anywhere near 80 commits/sec? If not, this should stay proposed and unbuilt —
   measure the real agent workload first.

## How to revisit

ADRs are immutable once committed. If this is accepted and later proves wrong, supersede it with a
new ADR rather than editing this one. If it is rejected, record that as a status change plus the
measurement that justified rejecting it, so the next person does not re-derive the same analysis.
