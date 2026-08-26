# Strata-Txn Sol Concurrency, Synchronization, and Thread-Safety Audit

Date: 2026-08-15  
Scope: `crates/txn` concurrency paths, synchronization primitives, atomics,
loom/chaos evidence, and the shared-handle boundary  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 defect or circular-wait path was confirmed. The normal
successful commit path preserves immutable snapshot and row/index atomicity
for one process sharing one `Dataset`, but the P1 indeterminate-publication
defect remains blocking.

## Findings

### [P1] Indeterminate manifest publication leaves shared state stale

Locations:

- [`crates/txn/src/dataset.rs:2231`](../../../crates/txn/src/dataset.rs:2231)
- [`crates/storage/src/backend/local.rs:344`](../../../crates/storage/src/backend/local.rs:344)
- [`crates/storage/src/backend/local.rs:359`](../../../crates/storage/src/backend/local.rs:359)
- [`crates/txn/src/dataset.rs:598`](../../../crates/txn/src/dataset.rs:598)
- [`crates/txn/src/dataset.rs:1290`](../../../crates/txn/src/dataset.rs:1290)

Manifest `N+1` can become visible by rename before directory synchronization
fails. `Transaction::commit` returns before updating the commit log or current
snapshot, leaving memory at `N` while disk exposes `N+1`. The next shared-handle
operation can reuse the version or operate from stale authority. Compaction and
schema migration have the same publish-before-in-memory-install structure.

This requires Sol design for immutable version publication, indeterminate
outcomes, shared-handle reconciliation, and retry/reopen semantics.

### [P2] Loom does not model the exact production publication primitive

Locations:

- [`crates/txn/src/dataset.rs:105`](../../../crates/txn/src/dataset.rs:105)
- [`crates/txn/src/dataset.rs:124`](../../../crates/txn/src/dataset.rs:124)

Production uses `ArcSwap`; loom substitutes a mutex-backed `SnapshotCell`
because ArcSwap is not loom-instrumented. The existing models test old-or-new
snapshot behavior but do not model ArcSwap's actual lock-free algorithm or
catch a production-only publication ordering regression.

### [P2] Lifecycle operations hold publication locks across whole-dataset I/O

Locations:

- [`crates/txn/src/dataset.rs:446`](../../../crates/txn/src/dataset.rs:446)
- [`crates/txn/src/dataset.rs:1193`](../../../crates/txn/src/dataset.rs:1193)
- [`crates/txn/src/vacuum.rs:29`](../../../crates/txn/src/vacuum.rs:29)

Compaction, migration, vacuum, and manifest pruning acquire lifecycle
exclusivity and retain `commit_lock` across filesystem reads, rewrites,
validation, synchronization, and sometimes deletion. No deadlock was found,
but publication can be suspended for the duration of whole-dataset I/O.

### [P3] Concurrency design limits and missing platform evidence

These are documented limits or evidence gaps, not confirmed defects:

- No FIFO guarantee among competing lifecycle executors; queued preparations
  can be delayed.
- Independent `Dataset::open` handles and separate processes do not share
  locks, row-ID allocation, OCC history, snapshot leases, or publication
  authority.
- No native ARM64 run, ThreadSanitizer run, or sustained data-race stress run
  is part of current evidence.

## Lock-order evidence

The reviewed hierarchy is:

1. Lifecycle coordinator state mutex, held only while changing counters or
   waiting.
2. Lifecycle-exclusive guard.
3. `commit_lock`.
4. Row-ID allocator mutex when reading the high-water mark.
5. Snapshot-lease registry mutex.
6. Snapshot publication through ArcSwap.

Normal commit claims row IDs before taking `commit_lock`, releases the allocator
guard, and only later reads allocator state under `commit_lock`. No nested
row-ID-to-commit cycle was found. Condition-variable waits use predicate loops,
`notify_all`, and writer preference. The live-set cache releases its global
slots lock before taking a per-key slot and drops the slot guard before
reacquiring slots for eviction; recursive compute callbacks are prohibited.

## Atomic-order evidence

- Timestamp high-water uses `fetch_max(SeqCst)`.
- Attempt IDs use `fetch_add(SeqCst)` before preparation and `load(SeqCst)` under
  `commit_lock`.
- Insufficient-history telemetry uses Relaxed ordering and is non-authoritative.
- Cache byte reservations use Relaxed atomic updates for the numerical cap,
  while slot mutexes publish cache values.
- No direct production Acquire/Release calls were found in `crates/txn`;
  visibility relies on mutex/condition-variable synchronization and ArcSwap's
  safe publication API. No invalid weak-memory ordering was established, but
  native ARM64 evidence is absent.

## Successful publication behavior

On the successful path, row files and the immutable vector segment enter one
manifest. Only after manifest durability returns does the code append OCC
history and store one complete replacement snapshot. Readers therefore see an
immutable old or new row/index view. The P1 failure creates disk/in-memory
divergence, not a half-constructed in-memory snapshot.

## Verification and mutation assessment

The Sol reviewer found no circular-wait path and identified existing loom
coverage for transaction, lifecycle, row-ID, and cache models. Static mutation
assessment indicates existing tests likely kill removed conflict serialization,
same-row double success, disjoint-write loss, lifecycle/preparation overlap,
writer-preference removal, split row/index publication, cache double-compute,
and over-budget concurrent admission.

The following are not proven killed by executed mutation testing:

- post-rename synchronization failure;
- stale-handle reconciliation;
- production ArcSwap ordering changes;
- durable filesystem reservation changes;
- cross-process races;
- ARM64-specific behavior.

After the Sol review, the parent workspace configured the installed MSVC and
Windows SDK environment and freshly verified:

```text
cargo check -p strata-txn --no-default-features       passed
cargo test -p strata-txn --no-default-features        312 passed, 0 failed
```

No loom model, TSan run, or mutation campaign was executed in this audit.

