# Chaos-Worker: Real Concurrent Agents and Zone-Map-Merge Verification

**Status:** Design — closes the two gaps PR #47
(`feat/chaos-worker-workload-extension`) documented as known limitations
rather than fixed.

## Context

PR #47 extended `crates/chaos-worker`'s Phase 7 workload from insert-only to
the full op mix (insert/delete/update/multibatch-insert/conflict-drop) and
closed 3 of the 5 gaps named in the S1 closure note
(`.claude/docs/design/phase-s1-segmented-index-spec.md` §9): delete, update,
and predicate-filtered search under zone-map pruning, all under real crash
injection. It explicitly documented — not silently claimed fixed — two
remaining gaps:

1. **Genuine write-write conflicts are unreachable.** The scheduler
   simulates `NUM_AGENTS` agents sequentially on one thread (`main.rs`'s
   `loop { ... }` picks one live agent per iteration, runs its op to
   completion, then picks the next). `Transaction::commit()`'s conflict
   check (`CommitLog::conflicts_with`) can only ever see `Clean`, because no
   two commits ever overlap in time. `commit_with_retry_once`,
   `ExecOutcome::Dropped`, and the "no resurrected tombstones" invariant's
   crash-tolerance logic are all real, reviewed, and unit-tested via mock
   closures — but none of it has ever executed against a genuine
   `TxnError::Conflict` in this harness. The 2000-seed thorough-tier
   zero-violation run is evidence the drop path never ran, not that it ran
   and passed.
2. **Multi-batch zone-map-merge correctness is not verified.** The reader's
   live check (`reader.rs`) applies `live_set` as an exact post-hoc row
   filter after pruning, so `pruned ⊆ reference` holds by construction
   regardless of whether a segment's merged zone map is actually correct.
   The unimplemented reverse direction (design doc §3.3: "every reference
   row with a genuinely nearest vector must be findable through the pruned
   path too") is the only direction that could catch a real over-pruning
   bug — and even with it built, `execute_multi_batch_insert`'s two rows
   currently share one `name` value, which is the only column the reader
   predicates on, so the merged zone map for that column is degenerate
   (min == max on both sides of the merge) and never exercises real
   cross-batch min/max merge logic.

This design closes both, on this same branch/PR, before merge.

## Goal

- Real, live-coverage evidence that `TxnError::Conflict` and the
  retry-then-drop path work correctly under actual concurrent commits —
  not mock-closure coverage alone.
- Real, live-coverage evidence that zone-map merging across multiple
  batches/segments is correct — not "structurally cannot fail" coverage
  alone.

## Non-Goals

- Exact interleaving reproducibility from a chaos seed. This design
  explicitly trades that away (see "Reproducibility tradeoff" below) — the
  op *sequence* and abort-*checkpoint-count* stay deterministic; the precise
  moment-by-moment thread interleaving does not, because real OS threads are
  now involved. This project's own methodology note
  (`.claude/CLAUDE.md`'s Phase 7 bullet) already commits to "Jepsen's
  methodology... real process spawn... seed-reproducible scenarios" —
  genuine Jepsen tests use real concurrent client threads too; today's
  single-threaded simulation was a simplification, not the target design.
- Changing `MultiBatchInsert`'s `name` semantics (both rows in one
  multi-batch commit keep the same `name`). Gap 2 is closed via a second,
  `id`-based predicate check instead — see below — so the existing
  `Registry`/tombstone-tracking invariants (which assume an agent's own
  rows all share one `name`) are untouched.
- Any change to `tests/sim/tests/chaos.rs`'s invariants or ack-line parser.
  The ack protocol (`print_outcome`'s line formats) is unchanged; only
  *which thread* prints them changes. `Dropped` acks were already a
  parsed, handled case (they just never fired) — no orchestrator change
  needed for real drops to start appearing.

## Part 1: Real concurrent agents

### Architecture change

Replace the single global scheduler loop with `NUM_AGENTS` real OS threads,
each running its own agent's full, pre-generated op sequence
(`agent_vectors[i]`, `agent_verbs[i]`) to completion, sequentially within
that thread, against the shared `Arc<Dataset>` (already `Arc`-cloneable and
thread-safe — this is exactly `crates/txn`'s designed-for scenario).

This *simplifies* `main()`: the `live_agents`/`pick`/`scheduler_rng`/
`next_op`/`remaining` global-scheduler bookkeeping is deleted entirely.
Each agent thread independently walks its own op list from index 0 to
`ops_per_agent - 1` (with `resolve_slot_consumption`'s existing downgrade
logic still applying per-thread), and the *only* remaining source of
interleaving randomness is genuine OS thread scheduling — which is the
actual thing this design exists to exercise.

```rust
let dataset = Arc::new(/* ...unchanged... */);
let registry = Arc::new(Mutex::new(Registry::new(num_agents_usize)));
// setup_contested_pool unchanged in its own logic, called once before
// any agent thread spawns (still establishes the pool sequentially).
setup_contested_pool(&dataset, seed, &mut registry.lock().unwrap(), &mut out);

let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset));

let agent_handles: Vec<_> = (0..num_agents)
    .map(|agent| {
        let dataset = Arc::clone(&dataset);
        let registry = Arc::clone(&registry);
        let vectors = agent_vectors[agent as usize].clone();
        let verbs = agent_verbs[agent as usize].clone();
        let mut target_rng = ChaCha8Rng::seed_from_u64(seed ^ agent ^ TARGET_STREAM);
        std::thread::spawn(move || {
            let mut op = 0u64;
            let mut remaining = ops_per_agent;
            while remaining > 0 {
                // ...existing per-op dispatch logic, operating on `agent`
                // (captured), `op`, `remaining`, `target_rng`, `registry`
                // (locked only around resolve_target and the post-commit
                // record/remove -- see "Registry" below)...
            }
        })
    })
    .collect();
for handle in agent_handles {
    handle.join().expect("agent thread panicked without going through the failure hook -- this should be unreachable");
}

reader_done.store(true, Ordering::SeqCst);
reader_handle.join().expect(/* unchanged */);
```

The exact per-op dispatch body (the `match verb { OpVerb::Insert => ..., ... }`
block) is unchanged logic, just moved from the shared loop into each
thread's closure, reading from that thread's own captured `vectors`/`verbs`
instead of indexing a shared `Vec<Vec<_>>` by `pick`.

### `Registry` becomes `Arc<Mutex<Registry>>`

`Registry`'s own methods (`record_pool_row`, `record_own_row`, `remove`,
`pool_rows`, `own_rows`) are unchanged — only the *access pattern* changes.
Two separate, short lock scopes per op (never held across a `commit()`
call, which is the expensive, blocking part):

1. **Before dispatch:** for `Delete`/`Update`, lock, read `pool_rows()`/
   `own_rows(agent)` (clone the needed slice contents out, since
   `resolve_target` just needs a snapshot to draw from), unlock, then call
   `resolve_target` unlocked.
2. **After a successful commit:** lock, call `record_own_row`/`remove` per
   `ExecOutcome`'s existing match arms (unchanged), unlock.

At `NUM_AGENTS × OPS_PER_AGENT` scale (tens of ops, not thousands), a plain
`std::sync::Mutex` is the correct, YAGNI choice — no lock-free structure is
justified here.

### Ack-line printing: a real correctness fix, not just a nice-to-have

**Finding, confirmed against `std`'s `Write::write_fmt` default
implementation:** `writeln!(&mut stdout, "agent {agent} committed insert op {op} row_id {row_id}")`
does **not** write the whole formatted line in one call. The default
`write_fmt` decomposes a format string into its literal fragments and
interpolated values, calling the destination's `write_str`
(→ `write_all`) once per fragment. For a bare `std::io::Stdout` (not a
locked `StdoutLock`), *each* of those calls independently acquires and
releases `Stdout`'s internal `ReentrantLock`.

This was harmless before this design: only the single main thread ever
printed ack lines (the reader thread never prints unless panicking, and
`install_failure_hook`'s own panic message uses a lock held for its one
call). It stops being harmless the moment multiple agent threads print
concurrently — two threads' ack lines could interleave mid-line, corrupting
`tests/sim`'s orchestrator parse (`run_worker`'s `match words.as_slice()`
would panic on `"unrecognized chaos-worker stdout line"` or, worse, parse a
corrupted line into wrong data silently).

**Fix:** build the complete line into an owned `String` via `format!()`
first (no I/O), then write it as a single `write_all`/`writeln!` call while
holding a lock acquired *just* for that one call:

```rust
fn print_line(line: &str) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
} // lock dropped here, immediately -- never held across a blocking op
```

`print_outcome` (in `commit_ops.rs`) and `setup_contested_pool`'s own
`writeln!` call both move to this pattern: format first, then one locked
write. This preserves the existing "never hold the lock across a blocking
operation" property (each lock acquisition is bounded to one `String`'s
worth of bytes) while *adding* the atomicity multi-threaded printing now
needs.

### Reproducibility tradeoff (stated plainly)

With real OS threads, the exact sequence of *which agent's op lands at
which `chaos_checkpoint` count* is no longer deterministic from the seed
alone — OS thread scheduling decides that, not this program. What **stays**
deterministic: each agent's own op sequence (verbs, targets-drawn-from,
vectors) is still fully seed-derived, and `STRATA_CHAOS_ABORT_AT`'s
checkpoint count is still a single global atomic counter in
`strata-storage`, unaffected by which thread increments it. A failing seed
may no longer reproduce the *exact* same failure on every run — but it will
reproduce the same *op content*, and `tests/sim`'s existing
`STRATA_CHAOS_ONLY_SEED` reproduction workflow still narrows a failure to
one seed; only the guarantee of *exact* re-hit on retry loosens, to
"likely, not certain," matching how real Jepsen-style tools already work.

### Interaction with existing invariants

No change needed to `tests/sim/tests/chaos.rs`. Real conflicts becoming
reachable means:
- `ExecOutcome::Dropped` / `"dropped op {op} (conflict)"` ack lines can now
  actually appear (previously only ever tested via mocks) — already parsed
  by the orchestrator's existing `[.., "dropped", "op", _, "(conflict)"]`
  arm.
- The joint lost+phantom tolerance bound and the "no resurrected
  tombstones" invariant were already designed for a world with real
  concurrent conflicts (their crash-tolerance reasoning explicitly
  considered "the one op that may have durably committed without its ack
  line reaching stdout" — this design doesn't change that reasoning, it
  makes the harness the *first* one to actually exercise it under a
  non-crash conflict, not just a crash-induced ambiguous outcome).

## Part 2: Zone-map-merge verification

Two additions to `reader.rs`, both run from the existing reader thread, no
new thread:

### 2a. Reverse-direction check (design doc §3.3's original ask)

For each row currently in the reference set (matches the `name` predicate,
per the existing unpruned scan), query `vector_search` using **that row's
own vector** as the query point, under the same `name` predicate, `k=1`.
Assert the result is that row itself (`found_own_point`-style: top-1 hit at
squared distance ≈ 0). If zone-map pruning ever wrongly excludes the
segment holding that row (a merged range too narrow), this row would come
back missing or replaced by a farther point — the failure mode the
subset-only check structurally cannot see.

**Cost control:** the reference set can grow to `NUM_AGENTS × OPS_PER_AGENT`
rows over a run, and this reader polls every 1ms. Checking every reference
row every poll would multiply an already-flagged hot-loop cost
(`.claude/docs/architecture.md`'s prior note on this reader's O(files)
per-tick cost). Check **one pseudo-randomly chosen reference row per poll**
(seeded from the same `seed` the worker was given, so the choice is
deterministic across runs even though which rows exist at poll time
isn't) instead of all of them — still statistically covers the reference
set across a run's many poll iterations, without materially changing the
loop's per-tick cost.

### 2b. `id`-range compound-predicate check (the actual zone-map-merge exerciser)

`name` is constant per agent, so cross-batch/cross-segment `name` zone-map
merging is always degenerate (merging two identical single-value ranges
proves nothing about merge *logic*). The business `id` column genuinely
differs per row (each op's `global_id` is unique), so it's the column where
merge correctness has real min/max arithmetic to get right — and this is
already a `Predicate::And(GtEq, Lt)` compound predicate, already supported
(`strata_query::Predicate`, S1 W1).

Add a second check per poll, with the exact range pinned down (no "roughly"
left to the implementer). This check has different scope than the `name`
check above: it runs an **unrelated, separate unpruned scan with no
predicate** (the full table — every agent's rows plus pool rows, not just
`name == "agent0"`), reading just the `id` column, to compute `min_id`/
`max_id` across ALL currently-visible rows (pool ids are negative,
`-1..-POOL_SIZE`; agent ids are `0..NUM_AGENTS*OPS_PER_AGENT`; the full
visible range spans both). Skip this check entirely if fewer than 2
distinct ids are visible (nothing to split). Otherwise:
`lo = min_id`, `hi = min_id + (max_id - min_id) / 2 + 1` (integer division;
the `+ 1` guarantees `hi > lo` even when `max_id - min_id == 1`, so the
range is never empty and always strictly smaller than the full set,
covering the lower half). Build
`Predicate::And(Predicate::GtEq("id", Value::Int64(lo)), Predicate::Lt("id", Value::Int64(hi)))`,
run the same pruned-vs-reference subset check `check_once` already does for
`name`, generalized to take a predicate parameter instead of hardcoding
`name`. This exercises the `And` pruning path (`should_scan_file`'s
`And(l, r) => should_scan_file(stats, l) && should_scan_file(stats, r)`)
against genuinely-varying per-row values, across both single-batch and
multi-batch commits, which the existing `name`-only check never did.

### Implementation shape

`check_once` generalizes from a hardcoded `name == "agent0"` predicate to
accept a `&Predicate` and a closure describing how to build the reference
set from a scanned batch (since `name` and `id` need different column
extraction). Two call sites in the reader's loop: the existing `name`
check (unchanged behavior) and the new `id`-range check, both against the
same snapshot per poll tick (still one `dataset.snapshot()` call feeding
both, preserving the existing same-snapshot discipline).

## Testing plan

- Existing `reader.rs`/`commit_ops.rs` unit tests updated for the
  generalized `check_once` signature; new unit tests for the `id`-range
  check and the reverse-direction check, each against a real multi-batch
  commit (2+ rows, one segment) to make the "merge, not just store"
  distinction load-bearing.
- New unit test(s) for `Registry` under concurrent access (a handful of
  threads hammering `record_own_row`/`remove`/`pool_rows` concurrently,
  asserting no panic and a consistent final count) — this is ordinary
  `Mutex` usage, not `crates/txn`/`crates/index` internals, so no `loom`
  test is required (per this project's own scoping: loom is for
  `crates/txn`/`crates/index`'s lock-free/OCC logic specifically).
- Fast tier (30 seeds) re-run; **expect to actually observe `dropped`
  ack lines in at least some seeds now** — if 30 seeds never produce one,
  that's a signal worth investigating (either the pool is too small
  relative to agent count, or timing means threads rarely truly overlap)
  before declaring gap 1 closed.
- Thorough tier (2000 seeds) re-run; the "5 invariants, zero violations"
  bar still applies, now under a run where `Dropped`/conflict paths are
  live rather than theoretical.
- Full `cargo build --workspace` / `clippy --all-targets -D warnings` /
  `fmt --check` / `cargo test --workspace`.

## Open question flagged for the plan (not blocking design approval)

Whether `POOL_SIZE = 6` and the 50/50 pool-vs-own-row target split give
real threads enough *actual* row-level contention to reliably produce
conflicts within `OPS_PER_AGENT = 5` ops per agent — if fast-tier seeds
consistently show zero drops, tuning either constant (not the mechanism)
is the likely fix. Left for the implementation plan to measure and decide,
not pre-specified here.
