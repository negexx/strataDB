# Phase 6 — Multi-Agent Concurrent Write Engine — Design

**Date:** 2026-07-21
**Status:** Approved for implementation planning

## 1. Goal and scope

Phase 6 is Strata's flagship subsystem (`.claude/docs/architecture.md`'s roadmap):
replace Phase 5's single-writer assumption with real optimistic concurrency
control so concurrent transactions detect genuine conflicts, commit atomically,
and never corrupt shared state. Exit criterion is the "v0.3 concurrent
multi-agent write slice" described in architecture.md: N concurrent
transactions (some conflicting, some not) against one shared dataset; every
acknowledged write is durable and visible to the next reader; conflicting
transactions get a typed error naming contested rows; a transaction's row
write and index update commit atomically or neither; a reader's open snapshot
never sees a partial write.

**In scope for this slice:**
- In-process concurrency: multiple threads/tasks sharing one `Dataset` handle.
  Matches architecture.md's own framing ("multiple concurrent AI agents, or
  concurrent tool calls from one agent") and Strata's nature as an embedded
  library, not a server.
- Write-write conflict detection only, keyed by row-id.
- A public `Transaction::update`/`delete` API (doesn't exist today — see §2).
- A tightly-scoped `Mutex`-guarded commit critical section (Approach A below).

**Explicitly out of scope for this slice** (not abandoned — deferred with a
reason, per Design Principle 5's "cut ruthlessly, document the cut"):
- **Cross-process visibility.** `Dataset::snapshot()` is a pure in-memory
  `ArcSwap::load_full()` today — a second OS process has no mechanism at all
  to notice another process's commits. Building one (polling or a filesystem
  watcher) is a distinct subsystem, not a fix to existing code, and the
  concern re-appears naturally in Phase 9 (object storage), where multiple
  independent writers hitting the same store is the baseline scenario Lance's
  own mechanism already targets. Solving it twice, once prematurely, isn't
  worth it.
- **Read-set / read-write conflict detection.** Phase-0 spec §1's conflict
  definition is adopted directly from FoundationDB, which does include
  read-set-vs-write-set overlap — but FDB populates a read-conflict-range
  automatically from reads made *through its transactional API*, and Strata's
  `Transaction` has no read method today (reads happen via
  `Dataset::snapshot()`, entirely outside any transaction). There's nothing to
  auto-populate from yet. This is reinforced, not just excused, by Strata's
  own named reference systems: Lance and DuckDB both independently chose
  write-write-only conflict detection over FDB's fuller model, and both are
  closer analogues to Strata's table-shaped workload than a general-purpose
  distributed KV store is. If a future phase adds transactional reads,
  read-conflict-ranges can slot into the same already-written definition
  without redefining it.
- **Automatic retry / conflict resolution.** Already an explicit Non-Goal in
  architecture.md ("Automatic/implicit conflict resolution... silent
  resolution hides bugs"). `commit()` returns the typed error; the caller
  decides whether to retry.

## 2. What exists today, and two bugs this design fixes

`crates/txn/src/dataset.rs`'s `Transaction::commit()` (Phase 5) has **zero**
conflict detection: it unconditionally computes `new_version =
base_manifest.version + 1` and calls `self.current.store(...)` — an `ArcSwap`
*store*, not a compare-and-swap. `commit_manifest` (`crates/storage/src/manifest.rs`)
writes to a tmp file then `fs::rename`s it into place — `rename()` silently
overwrites on both POSIX and NTFS if the destination already exists, so two
concurrent commits computing the same version number today would race, and
whichever's rename lands last wins with no error, no detection, no signal.

Two more bugs surfaced while tracing this path, both fixed by this design
rather than called out as pre-existing known issues, since they're directly
relevant to what "conflict" means once concurrency is real:

1. **Graph-mutation-ordering hazard.** `commit()` applies this transaction's
   deltas to the shared, ever-growing `HnswIndex` graph *before*
   `commit_manifest`. Harmless today (nothing can abort for conflict reasons),
   but under real conflict detection a transaction that *should* abort would
   already have mutated a graph that has no node-removal API to undo it.
   Conflict detection must run before any graph mutation, not after.
2. **Row-id allocation race.** Row-ids are assigned from `manifest.next_row_id`
   as read from the transaction's `base_manifest` — which can be stale under
   concurrency. Two concurrent inserting transactions reading the same stale
   `next_row_id` would allocate overlapping id ranges. This needs its own
   fix independent of conflict detection (§3).

Additionally, only `Transaction::insert` is public today. Row-ids for inserts
are assigned monotonically at commit time, so two concurrent *insert*
transactions can never conflict by construction — there is currently no
operation where two transactions can legitimately target the same row, which
is why this design adds `update`/`delete` (§4).

## 3. Mechanism: tightly-scoped mutex (Approach A)

Considered and rejected: a fully lock-free design (`ArcSwap::compare_and_swap`
+ a disk-level exclusive claim such as `File::create_new` in place of the
overwrite-prone rename). More literally matches this codebase's "prefer
lock-free" convention, but the retry loop still needs the same commit-log
structure below, now accessed lock-free, and unbounded retry interleavings are
meaningfully harder to exhaustively `loom`-test than one mutex's
happy/contended paths. `concurrency-txn-layer.md` doesn't ban locks outright —
it asks for lock order to be documented when "genuinely required" — and this
project already has a precedent for shipping the simpler, provably-correct
version first and revisiting with a dedicated rewrite only once a benchmark
justifies it (Phase 4's HNSW: wrap-first, lock-free-rewrite-later).

**Key scoping insight:** the expensive part of a commit — writing and
`fsync`ing new data files (`write_pending_batches`) — touches only files
unique to this transaction, so it never needs to be inside any lock. Only the
small part needs serializing: conflict check, graph delta application, the
manifest write (a small JSON file, cheap `fsync`), and the `ArcSwap` swap.
Scoped that tightly, the throughput gap against a fully lock-free design is an
empirical question, not an assumed one — see §6's benchmark requirement.

## 4. Data structures

**`Transaction` gains:**
```rust
write_set: Vec<u64>  // row-ids this transaction tombstones (update/delete). Inserts never add to this.
```

**`Dataset` gains:**
```rust
commit_lock: Mutex<CommitLog>
next_row_id_counter: AtomicU64
```

- `CommitLog` — a small bounded ring buffer of `(version: u64, write_set:
  Vec<u64>)` for recently-committed transactions. Needed regardless of
  locking strategy: `Snapshot`s don't retain write-set history once
  unreferenced, so answering "did anything land between my read-version and
  now touch my rows" needs its own structure. If a transaction's read-version
  predates the oldest surviving entry (the log wrapped), that's treated as a
  conservative conflict — can't prove innocence, so don't assume it. Documented
  as a known limitation (same pattern as Phase 5 spec §12), revisit if it
  proves too aggressive under real workloads.
- `next_row_id_counter` — decoupled from `commit_lock` on purpose. Every
  inserting transaction reserves a non-colliding id range via `fetch_add`
  before writing its data files, which is what makes it safe to keep the
  expensive fsync fully outside the commit mutex (§3) while still preventing
  the row-id collision race (§2's second bug). The persisted
  `manifest.next_row_id` only needs to be a safe upper bound — since Strata
  never reuses ids, it's fine for it to reflect the counter's current value
  even if that's ahead of what's strictly been committed so far.

## 5. Commit flow

1. **Before the lock** (unchanged): reserve row-ids from
   `next_row_id_counter`, write data files, `fsync` them and the directory
   entry.
2. **Acquire `commit_lock`.**
3. **Re-read actual latest state** via `self.current.load()` — not the
   transaction's possibly-stale `base_manifest`. Call its version
   `latest_version`.
4. **If `latest_version == base_manifest.version`:** nothing committed since
   this transaction began; skip to step 6.
5. **Else, walk `CommitLog` entries with version in
   `(base_manifest.version, latest_version]`:**
   - Insufficient log coverage (gap) → conservative conflict.
   - Any entry's `write_set` intersects this transaction's `write_set` →
     **abort**: return `TxnError::Conflict { contested_row_ids }`, release the
     lock, discard buffered state. Nothing has been mutated (§2's first bug,
     fixed).
6. **Clean — apply:** validate delta dimensions (existing), apply deltas to
   the graph, `commit_manifest` at `latest_version + 1` (not
   `base_manifest.version + 1`), swap the `ArcSwap`, push
   `(new_version, write_set)` onto `CommitLog`.
7. **Release `commit_lock`** (implicit on drop).

This also satisfies phase-0 spec §3 step 5 (a CAS failure caused by an
unrelated non-conflicting commit should transparently retry, not error out) for
free: under a single mutex there is no separate "spurious CAS failure" case to
handle — step 3 always observes truly-current state, so the
retry-vs-real-conflict distinction a lock-free design would need to handle
explicitly doesn't arise here.

## 6. API surface & error handling

```rust
impl Transaction {
    pub fn update(&mut self, row_id: u64, batch: RecordBatch); // tombstone(row_id) + insert(batch)
    pub fn delete(&mut self, row_id: u64);                      // tombstone(row_id) only
}
```
Both push `row_id` onto `write_set` in addition to existing buffering.

```rust
// New TxnError variant:
#[error("conflict: {contested_row_ids:?} were modified by another transaction")]
Conflict { contested_row_ids: Vec<u64> },
```

No automatic retry (§1). `commit()` returns `Err(TxnError::Conflict { .. })`
and stops; the caller decides whether to `dataset.begin()` again.

## 7. Testing strategy

Per `concurrency-txn-layer.md`'s mandatory rule, this needs `loom` coverage,
not just happy-path `#[test]`s — and per that same doc, loom-gated tests must
be run scoped to `strata-txn` only (`cargo rustc -p strata-txn --lib --profile
test -- --cfg loom`), never a workspace-wide `RUSTFLAGS` invocation.

**Loom interleavings** (`crates/txn/src/dataset.rs`):
- Two concurrent transactions tombstoning the *same* row-id → exactly one
  commits, the other gets `TxnError::Conflict` naming that row-id.
- Two concurrent transactions touching *disjoint* row-ids (including plain
  inserts) → both commit, both visible, no spurious conflict.
- A transaction that conflicts and aborts → the shared graph shows zero trace
  of its deltas (regression test for §2's first bug).

**Unit/integration tests:**
- `update`/`delete` correctness in isolation (tombstone + optional re-insert,
  visibility via `Snapshot::is_visible`).
- `CommitLog` boundary: a transaction whose read-version has aged out of the
  ring buffer gets the conservative conflict, not a false negative.
- New manifest version is taken from `latest_version + 1` at commit time, not
  the transaction's stale `base_manifest.version` (regression test for §2's
  framing of the original unconditional-store bug).

**Benchmark (exit evidence for §3's mutex-scoping decision):** a new
`bench/benches/` case measuring commit throughput under concurrent
non-conflicting writers vs. today's single-writer baseline, and under a
high-conflict-rate workload. This is the number that either validates the
tightly-scoped mutex or motivates revisiting toward a lock-free rewrite later
— the same path already taken with HNSW itself.

## 8. References

- `.claude/docs/architecture.md` — Phase 6 roadmap entry and the v0.3 exit
  criterion this design targets.
- `.claude/docs/design/phase-0-transaction-and-format-spec.md` — precise
  definitions of "conflict," "transaction boundary," and the commit protocol
  this design implements (§§1-4 especially).
- `.claude/rules/concurrency-txn-layer.md` — invariants and the loom-scoping
  gotcha this design must not violate.
- `.claude/docs/decisions/0003-snapshot-isolation-not-serializability.md` —
  why write-skew (and, by extension, full read-set tracking) is an accepted
  non-goal, not an oversight.
- `.claude/docs/design/phase-5-mvcc-snapshot-isolation-spec.md` — the
  `Arc<ArcSwap<Snapshot>>` foundation this design builds on.
- Prior art consulted during design: [Lance transactions](https://lance.org/format/table/transaction/)
  (write-write-only conflict detection, no read-set tracking — this project's
  own storage-format reference), [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency)
  (optimistic, write-write-only, "appends never conflict" — this project's own
  vectorized-execution reference), and FoundationDB's read/write
  conflict-range model (adopted directly by phase-0 spec §1, but not fully
  applicable yet — see §1's scope notes).
