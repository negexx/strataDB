# Chaos-Worker: Real Concurrent Agents and Zone-Map-Merge Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close PR #47's two documented gaps for real: make genuine `TxnError::Conflict`/drop paths reachable by replacing the sequential single-thread agent scheduler with real OS threads, and make zone-map-merge correctness (not just pruned-subset-of-reference) actually exercised by the reader.

**Architecture:** `crates/chaos-worker/src/main.rs`'s scheduler loop is replaced by `NUM_AGENTS` real OS threads, each running its own agent's pre-generated op sequence to completion against the shared `Arc<Dataset>`. `Registry` becomes `Arc<Mutex<Registry>>` with two short lock scopes per op (never held across `commit()`). All worker-printed lines move through one atomic `print_line` helper. `reader.rs`'s `check_once` gains a reverse-direction check (own-vector round-trip) and an `id`-range compound-predicate check, both run every poll alongside the existing `name`-predicate subset check.

**Tech Stack:** Rust (edition 2024), `arrow`, `strata_txn`/`strata_query`/`strata_storage`, `rand`/`rand_chacha` (`ChaCha8Rng`), `std::sync::{Arc, Mutex}`, `std::thread`.

## Global Constraints

- Never share one `ChaCha8Rng` across two threads. Each agent thread builds its own target-selection RNG via `ChaCha8Rng::seed_from_u64(seed ^ agent ^ TARGET_STREAM)` — the exact formula the old shared loop used per agent, just moved to per-thread ownership, so per-agent op-target sequences stay byte-identical to before.
- `Registry`'s lock (`Arc<Mutex<Registry>>`) must never be held across a `Transaction::commit()` call or any other blocking operation. Two short scopes per op only: once before dispatch (to snapshot `pool_rows()`/`own_rows(agent)` into owned `Vec<u64>`s for `resolve_target`), once after a successful commit (to `record_own_row`/`remove`).
- Every line this worker prints (pool-setup acks, per-op acks) must go through the single `print_line` helper in `commit_ops.rs`, which locks real stdout once per call and drops the lock immediately after — never a bare unlocked `Stdout` write once more than one thread prints.
- No change to `tests/sim/tests/chaos.rs`'s five invariants or its ack-line parser (`run_worker`'s `match words.as_slice()`). Ack-line **formats** (`format_outcome_line`'s output strings) must stay byte-identical to today's — only *which thread* calls the printer changes. Tuning `NUM_AGENTS`/`OPS_PER_AGENT`/`POOL_SIZE` *values* (not formats or invariants) is allowed in Task 6 only, if measurement shows real conflicts don't occur.
- The `id`-range check's range is exact, not approximate: scan the full `id` column across every currently-visible row (pool rows included), compute `min_id`/`max_id`, skip entirely if fewer than 2 distinct ids are visible, otherwise `lo = min_id`, `hi = min_id + (max_id - min_id) / 2 + 1`.
- Every task in this plan gets an Opus review before being marked done, per this project's `CLAUDE.md` (non-negotiable, not an escalation).
- Design doc: `docs/superpowers/specs/2026-07-30-chaos-worker-real-concurrency-and-zonemap-verification-design.md`. If anything in this plan and that doc ever disagree, the design doc wins — flag it rather than silently picking one.

---

### Task 1: `Registry` becomes `Arc<Mutex<Registry>>` (no behavior change)

De-risks the concurrency-enabling data-structure change independently of introducing real threads (Task 3) — after this task, the worker is still single-threaded and must produce byte-identical output to before, but every `Registry` access already goes through the lock pattern Task 3 will reuse unchanged.

**Files:**
- Modify: `crates/chaos-worker/src/main.rs`
- Modify: `crates/chaos-worker/src/commit_ops.rs` (new test only)

**Interfaces:**
- Consumes: `Registry::{new, record_pool_row, record_own_row, remove, pool_rows, own_rows}` (all unchanged signatures, from `commit_ops.rs`).
- Produces: no new public interface — `registry` in `main()` changes type from `Registry` to `Arc<Mutex<Registry>>`, consumed by Task 3.

- [ ] **Step 1: Add the `Mutex` import to `main.rs`**

In `crates/chaos-worker/src/main.rs`, change:

```rust
use std::sync::Arc;
use std::sync::atomic::Ordering;
```

to:

```rust
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
```

- [ ] **Step 2: Wrap `registry` in `Arc<Mutex<>>` and update the pool-setup call site**

In `main()`, change:

```rust
    let num_agents_usize = usize::try_from(num_agents).unwrap();
    let mut registry = Registry::new(num_agents_usize);
    // Deliberately `std::io::stdout()`, NOT `.lock()`'d: `Stdout` itself
    // implements `Write` by locking/unlocking internally on every single
    // call, rather than holding the lock across this whole function --
    // see the comment on the scheduler loop's `out` below for why holding
    // it for any extended span is unsafe here.
    let mut out = std::io::stdout();
    setup_contested_pool(&dataset, seed, &mut registry, &mut out);
```

to:

```rust
    let num_agents_usize = usize::try_from(num_agents).unwrap();
    // Pool setup runs before any agent thread exists and before the
    // registry is wrapped for shared access -- taking `&mut Registry`
    // directly here (rather than `registry.lock().unwrap()`) avoids
    // holding the mutex across `setup_contested_pool`'s POOL_SIZE commits
    // and stdout flushes, which would otherwise violate this file's own
    // "never hold the registry lock across a commit or blocking I/O"
    // discipline -- harmless today (nothing else can contend yet), but
    // Task 3's real agent threads inherit this exact pattern, so it must
    // not establish the one counter-example to it.
    let mut registry = Registry::new(num_agents_usize);
    // Deliberately `std::io::stdout()`, NOT `.lock()`'d: `Stdout` itself
    // implements `Write` by locking/unlocking internally on every single
    // call, rather than holding the lock across this whole function --
    // see the comment on the scheduler loop's `out` below for why holding
    // it for any extended span is unsafe here.
    let mut out = std::io::stdout();
    setup_contested_pool(&dataset, seed, &mut registry, &mut out);
    let registry = Arc::new(Mutex::new(registry));
```

(`out`/`print_outcome`'s signature is untouched in this task — that's Task 2. This step changes `registry`'s type — wrapped in `Arc<Mutex<>>` only AFTER pool setup completes, so the lock is never held across pool setup's own commits/flushes — and the one call site that used it directly.)

- [ ] **Step 3: Update the scheduler loop's target-resolution call sites to lock, snapshot, unlock**

In `main()`'s `loop { ... }`, change the `OpVerb::Delete` arm from:

```rust
            OpVerb::Delete => {
                if let Some(target_row_id) = resolve_target(
                    &mut agent_target_rngs[pick],
                    registry.pool_rows(),
                    registry.own_rows(pick),
                ) {
                    execute_delete(&dataset, target_row_id)
                } else {
```

to:

```rust
            OpVerb::Delete => {
                let (pool_rows, own_rows) = {
                    let guard = registry.lock().unwrap();
                    (guard.pool_rows().to_vec(), guard.own_rows(pick).to_vec())
                };
                if let Some(target_row_id) =
                    resolve_target(&mut agent_target_rngs[pick], &pool_rows, &own_rows)
                {
                    execute_delete(&dataset, target_row_id)
                } else {
```

and the `OpVerb::Update` arm from:

```rust
            OpVerb::Update => {
                if let Some(target_row_id) = resolve_target(
                    &mut agent_target_rngs[pick],
                    registry.pool_rows(),
                    registry.own_rows(pick),
                ) {
```

to:

```rust
            OpVerb::Update => {
                let (pool_rows, own_rows) = {
                    let guard = registry.lock().unwrap();
                    (guard.pool_rows().to_vec(), guard.own_rows(pick).to_vec())
                };
                if let Some(target_row_id) =
                    resolve_target(&mut agent_target_rngs[pick], &pool_rows, &own_rows)
                {
```

- [ ] **Step 4: Update the post-commit registry-update match block to lock per arm**

Change:

```rust
        match &outcome {
            ExecOutcome::CommittedInsert { row_id, .. } => registry.record_own_row(pick, *row_id),
            ExecOutcome::CommittedUpdate {
                target_row_id,
                row_id,
            } => {
                registry.remove(pick, *target_row_id);
                registry.record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedDelete { target_row_id } => registry.remove(pick, *target_row_id),
            ExecOutcome::CommittedMultiBatch { row_ids } => {
                registry.record_own_row(pick, row_ids[0]);
                registry.record_own_row(pick, row_ids[1]);
            }
            ExecOutcome::Dropped => {}
        }
```

to:

```rust
        match &outcome {
            ExecOutcome::CommittedInsert { row_id, .. } => {
                registry.lock().unwrap().record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedUpdate {
                target_row_id,
                row_id,
            } => {
                let mut guard = registry.lock().unwrap();
                guard.remove(pick, *target_row_id);
                guard.record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedDelete { target_row_id } => {
                registry.lock().unwrap().remove(pick, *target_row_id);
            }
            ExecOutcome::CommittedMultiBatch { row_ids } => {
                let mut guard = registry.lock().unwrap();
                guard.record_own_row(pick, row_ids[0]);
                guard.record_own_row(pick, row_ids[1]);
            }
            ExecOutcome::Dropped => {}
        }
```

- [ ] **Step 5: Write the failing concurrent-access test in `commit_ops.rs`**

Add to `commit_ops.rs`'s `#[cfg(test)] mod tests` (near `registry_record_and_remove_round_trip`):

```rust
    #[test]
    fn registry_survives_concurrent_record_and_remove_from_multiple_threads() {
        use std::sync::{Arc, Mutex};
        let registry = Arc::new(Mutex::new(Registry::new(4)));
        let handles: Vec<_> = (0..4u64)
            .map(|agent| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    let agent_idx = usize::try_from(agent).unwrap();
                    for i in 0..50u64 {
                        registry
                            .lock()
                            .unwrap()
                            .record_own_row(agent_idx, agent * 1000 + i);
                    }
                    for i in (0..50u64).step_by(2) {
                        registry.lock().unwrap().remove(agent_idx, agent * 1000 + i);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let guard = registry.lock().unwrap();
        for agent_idx in 0..4usize {
            assert_eq!(
                guard.own_rows(agent_idx).len(),
                25,
                "agent {agent_idx} should have exactly the 25 odd-indexed rows left \
                 (0..50 step 2 removes the 25 even-indexed ones)"
            );
        }
    }
```

This test doesn't depend on `main.rs`'s changes (it exercises `Registry` directly), so it can be written and passed before Steps 1-4, or after — order doesn't matter for correctness, but write it now so the whole task lands in one commit.

- [ ] **Step 6: Run the tests**

```bash
cargo test -p strata-chaos-worker
```

Expected: all pass, including the new `registry_survives_concurrent_record_and_remove_from_multiple_threads`.

- [ ] **Step 7: Run the existing chaos fast tier to confirm zero behavior change**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` (in `tests/sim/tests/chaos.rs`) still passes — this task changes `Registry`'s access pattern only, not scheduling, so output must be byte-identical to before.

- [ ] **Step 8: Commit**

```bash
git add crates/chaos-worker/src/main.rs crates/chaos-worker/src/commit_ops.rs
git commit -m "refactor(chaos-worker): wrap Registry in Arc<Mutex<>> ahead of real concurrency"
```

---

### Task 2: Atomic ack-line printing (`print_line`)

Fixes the printing-atomicity bug the design doc identifies: `Write::write_fmt`'s default implementation decomposes a multi-fragment format string into multiple separate locking `Stdout` calls, which risks interleaved/corrupted lines once more than one thread prints (Task 3). The fix: acquire the stdout lock once per line, and write through that single already-held lock.

**Files:**
- Modify: `crates/chaos-worker/src/commit_ops.rs`
- Modify: `crates/chaos-worker/src/main.rs`

**Interfaces:**
- Produces: `pub(crate) fn print_line(line: &str)` and `pub(crate) fn format_outcome_line(agent: u64, op: u64, outcome: &ExecOutcome) -> String` in `commit_ops.rs`. `print_outcome`'s signature changes from `(out: &mut impl Write, agent: u64, op: u64, outcome: &ExecOutcome)` to `(agent: u64, op: u64, outcome: &ExecOutcome)` — Task 3's `run_agent` calls the new 3-arg form.

- [ ] **Step 1: Write the failing test for `format_outcome_line`**

In `commit_ops.rs`'s `#[cfg(test)] mod tests`, replace the existing `print_outcome_matches_the_documented_ack_line_format_for_every_variant` test with:

```rust
    #[test]
    fn format_outcome_line_matches_the_documented_ack_line_format_for_every_variant() {
        // Task 7's orchestrator parses these lines by exact format -- a
        // typo here (a missing token, "multi_batch" instead of
        // "multibatch", a space instead of a comma) would compile and pass
        // every execute_* test while silently breaking the orchestrator's
        // parser.
        assert_eq!(
            format_outcome_line(
                1,
                2,
                &ExecOutcome::CommittedInsert {
                    business_id: 99,
                    row_id: 5
                }
            ),
            "agent 1 committed insert op 2 row_id 5"
        );
        assert_eq!(
            format_outcome_line(1, 3, &ExecOutcome::CommittedDelete { target_row_id: 5 }),
            "agent 1 committed delete op 3 target_row_id 5"
        );
        assert_eq!(
            format_outcome_line(
                1,
                4,
                &ExecOutcome::CommittedUpdate {
                    target_row_id: 5,
                    row_id: 6
                }
            ),
            "agent 1 committed update op 4 target_row_id 5 row_id 6"
        );
        assert_eq!(
            format_outcome_line(1, 5, &ExecOutcome::CommittedMultiBatch { row_ids: [7, 8] }),
            "agent 1 committed multibatch op 5 row_ids 7,8"
        );
        assert_eq!(
            format_outcome_line(1, 6, &ExecOutcome::Dropped),
            "agent 1 dropped op 6 (conflict)"
        );
    }
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cargo test -p strata-chaos-worker format_outcome_line_matches
```

Expected: FAIL — `format_outcome_line` doesn't exist yet.

- [ ] **Step 3: Implement `print_line`, `format_outcome_line`, and the new `print_outcome`**

In `commit_ops.rs`, replace the existing `print_outcome` function with:

```rust
/// Writes one already-fully-formatted line to real stdout, locking it
/// exactly once for this one write+flush and dropping the lock
/// immediately after — never across a blocking operation. `Write::write_fmt`'s
/// default implementation decomposes a format string into one `write_str`
/// call per literal fragment/interpolated value, and a bare unlocked
/// `Stdout` re-locks internally on every one of those calls — harmless
/// with a single printer, but it risks two threads' lines interleaving
/// mid-line once multiple agent threads print concurrently. Locking once
/// here, before any of those internal `write_str` calls happen, means they
/// all run under the SAME held lock, so no other thread's `print_line`
/// call can interleave until this one's guard drops. See
/// `docs/superpowers/specs/2026-07-30-chaos-worker-real-concurrency-and-zonemap-verification-design.md`.
pub(crate) fn print_line(line: &str) {
    let stdout = std::io::stdout();
    let mut locked = stdout.lock();
    let _ = writeln!(locked, "{line}");
    let _ = locked.flush();
}

/// Builds the acknowledgment line text for one op outcome, matching the
/// design doc §3.5 protocol — separated from [`print_line`] so the exact
/// format stays unit-testable without going through real stdout.
pub(crate) fn format_outcome_line(agent: u64, op: u64, outcome: &ExecOutcome) -> String {
    match outcome {
        ExecOutcome::CommittedInsert { row_id, .. } => {
            format!("agent {agent} committed insert op {op} row_id {row_id}")
        }
        ExecOutcome::CommittedDelete { target_row_id } => {
            format!("agent {agent} committed delete op {op} target_row_id {target_row_id}")
        }
        ExecOutcome::CommittedUpdate {
            target_row_id,
            row_id,
        } => format!(
            "agent {agent} committed update op {op} target_row_id {target_row_id} row_id {row_id}"
        ),
        ExecOutcome::CommittedMultiBatch { row_ids } => format!(
            "agent {agent} committed multibatch op {op} row_ids {},{}",
            row_ids[0], row_ids[1]
        ),
        ExecOutcome::Dropped => format!("agent {agent} dropped op {op} (conflict)"),
    }
}

/// Prints one acknowledgment line and flushes immediately (`tests/sim`'s
/// orchestrator reads this over a pipe and needs each line as soon as it's
/// written).
pub(crate) fn print_outcome(agent: u64, op: u64, outcome: &ExecOutcome) {
    print_line(&format_outcome_line(agent, op, outcome));
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cargo test -p strata-chaos-worker format_outcome_line_matches
```

Expected: PASS.

- [ ] **Step 5: Update `setup_contested_pool` and its call site in `main.rs`**

In `main.rs`, change `setup_contested_pool`'s signature and body from:

```rust
fn setup_contested_pool(
    dataset: &strata_txn::Dataset,
    seed: u64,
    registry: &mut Registry,
    out: &mut impl std::io::Write,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ POOL_STREAM);
    for i in 0..POOL_SIZE {
        let business_id = -1 - i64::try_from(i).unwrap();
        let vector = [
            rng.random::<f32>(),
            rng.random::<f32>(),
            rng.random::<f32>(),
        ];
        let outcome = execute_insert(dataset, business_id, "pool", vector);
        let ExecOutcome::CommittedInsert { row_id, .. } = outcome else {
            panic!("pool setup insert must always commit cleanly: {outcome:?}");
        };
        registry.record_pool_row(row_id);
        writeln!(out, "pool committed insert row_id {row_id}").unwrap();
        out.flush().unwrap();
    }
}
```

to:

```rust
fn setup_contested_pool(dataset: &strata_txn::Dataset, seed: u64, registry: &mut Registry) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ POOL_STREAM);
    for i in 0..POOL_SIZE {
        let business_id = -1 - i64::try_from(i).unwrap();
        let vector = [
            rng.random::<f32>(),
            rng.random::<f32>(),
            rng.random::<f32>(),
        ];
        let outcome = execute_insert(dataset, business_id, "pool", vector);
        let ExecOutcome::CommittedInsert { row_id, .. } = outcome else {
            panic!("pool setup insert must always commit cleanly: {outcome:?}");
        };
        registry.record_pool_row(row_id);
        commit_ops::print_line(&format!("pool committed insert row_id {row_id}"));
    }
}
```

Then change its call site — this is Task 1's Step 2 pattern (kept as a plain `Registry` through setup, wrapped in `Arc<Mutex<>>` only after, so the lock is never held across setup's commits) — from:

```rust
    let mut registry = Registry::new(num_agents_usize);
    // Deliberately `std::io::stdout()`, NOT `.lock()`'d: `Stdout` itself
    // implements `Write` by locking/unlocking internally on every single
    // call, rather than holding the lock across this whole function --
    // see the comment on the scheduler loop's `out` below for why holding
    // it for any extended span is unsafe here.
    let mut out = std::io::stdout();
    setup_contested_pool(&dataset, seed, &mut registry, &mut out);
    let registry = Arc::new(Mutex::new(registry));
```

to:

```rust
    let mut registry = Registry::new(num_agents_usize);
    setup_contested_pool(&dataset, seed, &mut registry);
    let registry = Arc::new(Mutex::new(registry));
```

(The `let mut out = std::io::stdout();` binding and its doc comment are deleted entirely — nothing in `main()` needs an injected writer anymore now that both `setup_contested_pool` and `print_outcome` print via `commit_ops::print_line` internally. `registry` is still wrapped in `Arc<Mutex<>>` only AFTER `setup_contested_pool` returns — that ordering is load-bearing, not incidental: see Task 1's Step 2 comment on why.)

- [ ] **Step 6: Update the scheduler loop's `print_outcome` call site**

Change:

```rust
        commit_ops::print_outcome(&mut out, agent, op, &outcome);
```

to:

```rust
        commit_ops::print_outcome(agent, op, &outcome);
```

- [ ] **Step 7: Update `stdout_lock_discipline_tests`'s doc comment**

In `main.rs`'s `stdout_lock_discipline_tests` module, the comment referencing "`main()` and `setup_contested_pool` print through a plain `std::io::Stdout` handle, never a `StdoutLock`" is now stale — both print through `commit_ops::print_line`, which DOES use a `StdoutLock`, briefly, once per line. Update the comment block to:

```rust
    // A characterization test of a ReentrantLock cross-thread-blocking
    // property this crate deliberately never triggers, not a test of
    // main() itself (it never calls main() or anything in it, so it
    // cannot detect a regression there). The property: Stdout's lock is
    // reentrant PER THREAD ONLY, so a second thread's lock() call blocks
    // while a first thread still holds it, and unblocks once that first
    // thread drops its guard.
    //
    // Every line this worker prints goes through commit_ops::print_line,
    // which acquires a StdoutLock for exactly one write+flush and drops it
    // immediately -- never held across a blocking operation (an agent
    // thread's commit(), or main()'s join() on the agent/reader threads).
    // That matters because install_failure_hook's hook calls
    // stdout().lock() from whichever thread panics: if some other thread
    // instead held a StdoutLock across a blocking call, a panicking
    // thread's own lock() call would block forever on it, and whatever
    // that other thread was blocked joining could never return either.
    // This test exists to document that hazard precisely, so nobody
    // "optimizes" print_line back into a lock held across a wider span
    // without understanding what it would reintroduce.
```

- [ ] **Step 8: Run the tests**

```bash
cargo test -p strata-chaos-worker
```

Expected: all pass.

- [ ] **Step 9: Run the full workspace test suite**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` still passes — no scheduling change yet, only how lines are printed.

- [ ] **Step 10: Commit**

```bash
git add crates/chaos-worker/src/commit_ops.rs crates/chaos-worker/src/main.rs
git commit -m "fix(chaos-worker): print every ack line through one atomic, single-lock write"
```

---

### Task 3: Real per-agent-thread scheduler

The architecture change itself: replaces the single shared scheduler loop with `NUM_AGENTS` real OS threads, each running its own agent's op sequence to completion. Depends on Task 1 (`Registry` already `Arc<Mutex<>>`-wrapped with the target lock-scoping pattern) and Task 2 (`print_outcome`/`print_line` already safe for concurrent callers).

**Files:**
- Modify: `crates/chaos-worker/src/main.rs`

**Interfaces:**
- Produces: `fn run_agent(dataset: &strata_txn::Dataset, registry: &Mutex<Registry>, agent: u64, ops_per_agent: u64, vectors: &[[f32; 3]], verbs: &[OpVerb], seed: u64)` — one agent's full op sequence, run to completion on whatever thread calls it.
- Consumes: `Registry`, `ops::{OpVerb, resolve_slot_consumption, resolve_target}`, `commit_ops::{execute_insert, execute_delete, execute_update, execute_multi_batch_insert, print_outcome}` (all unchanged from Tasks 1-2).

- [ ] **Step 1: Replace `main()`'s scheduler loop with per-agent thread spawning**

In `main.rs`, replace everything from `let mut next_op: Vec<u64> = vec![0; num_agents_usize];` through the end of the `loop { ... }` block (i.e. replace the old scheduler's bookkeeping and loop) — and remove the `#[allow(clippy::too_many_lines)]` attribute above `fn main()` along with its comment, since the rewritten `main()` is short — with:

```rust
    let agent_handles: Vec<std::thread::JoinHandle<()>> = (0..num_agents)
        .map(|agent| {
            let dataset = Arc::clone(&dataset);
            let registry = Arc::clone(&registry);
            let vectors = agent_vectors[usize::try_from(agent).unwrap()].clone();
            let verbs = agent_verbs[usize::try_from(agent).unwrap()].clone();
            std::thread::spawn(move || {
                run_agent(&dataset, &registry, agent, ops_per_agent, &vectors, &verbs, seed);
            })
        })
        .collect();
    for handle in agent_handles {
        handle.join().expect(
            "agent thread panicked without going through the failure hook -- this should be unreachable",
        );
    }
```

`agent_vectors` and `agent_verbs` (built just above, unchanged) stay as-is — only the `agent_target_rngs`/`next_op`/`remaining`/`scheduler_rng` locals and the `loop { ... }` that consumed them are deleted, replaced by the block above.

The full `fn main()` should now read:

```rust
fn main() {
    install_failure_hook();

    let args: Vec<String> = std::env::args().collect();
    let dir = args
        .get(1)
        .expect("usage: chaos-worker <dir> <seed> <num_agents> <ops_per_agent>");
    let seed: u64 = args
        .get(2)
        .expect("missing <seed>")
        .parse()
        .expect("seed must be a u64");
    let num_agents: u64 = args
        .get(3)
        .expect("missing <num_agents>")
        .parse()
        .expect("num_agents must be a u64");
    let ops_per_agent: u64 = args
        .get(4)
        .expect("missing <ops_per_agent>")
        .parse()
        .expect("ops_per_agent must be a u64");

    let dataset = Arc::new(
        strata_txn::Dataset::open(dir)
            .or_else(|_| strata_txn::Dataset::create(dir))
            .expect("failed to open or create dataset"),
    );

    let num_agents_usize = usize::try_from(num_agents).unwrap();
    // Pool setup runs before any agent thread exists and before the
    // registry is wrapped for shared access -- taking `&mut Registry`
    // directly here (rather than `registry.lock().unwrap()`) avoids
    // holding the mutex across `setup_contested_pool`'s POOL_SIZE commits,
    // which would otherwise violate this file's own "never hold the
    // registry lock across a commit or blocking op" discipline -- the
    // exact discipline the agent threads below depend on.
    let mut registry = Registry::new(num_agents_usize);
    setup_contested_pool(&dataset, seed, &mut registry);
    let registry = Arc::new(Mutex::new(registry));

    let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset));

    // Per-agent vector generation (unchanged from the original insert-only
    // worker) and verb generation (ops.rs), both pre-generated up front.
    // Target resolution for Delete/Update stays just-in-time -- see
    // resolve_target's own doc comment for why -- and now happens inside
    // each agent's own thread (run_agent), not a shared scheduler.
    let agent_vectors: Vec<Vec<[f32; 3]>> = (0..num_agents)
        .map(|agent| {
            let mut agent_rng = ChaCha8Rng::seed_from_u64(seed ^ agent);
            (0..ops_per_agent)
                .map(|op| {
                    let global_id = agent * ops_per_agent + op;
                    #[allow(clippy::cast_precision_loss)]
                    let v = global_id as f32;
                    [
                        v + agent_rng.random::<f32>(),
                        v + agent_rng.random::<f32>(),
                        v + agent_rng.random::<f32>(),
                    ]
                })
                .collect()
        })
        .collect();
    let agent_verbs: Vec<Vec<OpVerb>> = (0..num_agents)
        .map(|agent| generate_verb_sequence(seed, agent, ops_per_agent))
        .collect();

    let agent_handles: Vec<std::thread::JoinHandle<()>> = (0..num_agents)
        .map(|agent| {
            let dataset = Arc::clone(&dataset);
            let registry = Arc::clone(&registry);
            let vectors = agent_vectors[usize::try_from(agent).unwrap()].clone();
            let verbs = agent_verbs[usize::try_from(agent).unwrap()].clone();
            std::thread::spawn(move || {
                run_agent(&dataset, &registry, agent, ops_per_agent, &vectors, &verbs, seed);
            })
        })
        .collect();
    for handle in agent_handles {
        handle.join().expect(
            "agent thread panicked without going through the failure hook -- this should be unreachable",
        );
    }

    reader_done.store(true, Ordering::SeqCst);
    reader_handle.join().expect("reader thread panicked without going through the failure hook -- this should be unreachable");
}
```

(The `reader::spawn(Arc::clone(&dataset))` call site is untouched in this task — Task 4 changes its signature.)

- [ ] **Step 2: Add `run_agent`**

Add this function after `main()`:

```rust
/// Runs one agent's full, pre-generated op sequence to completion,
/// sequentially within this thread, against the shared `dataset`/
/// `registry` — see design doc Part 1. `resolve_slot_consumption`'s
/// downgrade logic still applies per-thread; the only remaining source of
/// interleaving randomness across agents is genuine OS thread scheduling,
/// which is the actual thing this design exists to exercise.
#[allow(clippy::too_many_arguments)]
fn run_agent(
    dataset: &strata_txn::Dataset,
    registry: &Mutex<Registry>,
    agent: u64,
    ops_per_agent: u64,
    vectors: &[[f32; 3]],
    verbs: &[OpVerb],
    seed: u64,
) {
    let pick = usize::try_from(agent).unwrap();
    let mut target_rng = ChaCha8Rng::seed_from_u64(seed ^ agent ^ TARGET_STREAM);
    let mut op = 0u64;
    let mut remaining = ops_per_agent;

    while remaining > 0 {
        let drawn_verb = verbs[usize::try_from(op).unwrap()];
        let (verb, slots_consumed) = resolve_slot_consumption(drawn_verb, remaining);

        let outcome = match verb {
            OpVerb::Insert => {
                let global_id = agent * ops_per_agent + op;
                let vector = vectors[usize::try_from(op).unwrap()];
                execute_insert(
                    dataset,
                    i64::try_from(global_id).unwrap(),
                    &format!("agent{agent}"),
                    vector,
                )
            }
            OpVerb::MultiBatchInsert => {
                let global_id_0 = agent * ops_per_agent + op;
                let global_id_1 = agent * ops_per_agent + op + 1;
                let vector_0 = vectors[usize::try_from(op).unwrap()];
                let vector_1 = vectors[usize::try_from(op + 1).unwrap()];
                execute_multi_batch_insert(
                    dataset,
                    [
                        i64::try_from(global_id_0).unwrap(),
                        i64::try_from(global_id_1).unwrap(),
                    ],
                    &format!("agent{agent}"),
                    [vector_0, vector_1],
                )
            }
            OpVerb::Delete => {
                let (pool_rows, own_rows) = {
                    let guard = registry.lock().unwrap();
                    (guard.pool_rows().to_vec(), guard.own_rows(pick).to_vec())
                };
                if let Some(target_row_id) = resolve_target(&mut target_rng, &pool_rows, &own_rows)
                {
                    execute_delete(dataset, target_row_id)
                } else {
                    // No eligible target yet -- downgrade to Insert per design doc §3.1.
                    let global_id = agent * ops_per_agent + op;
                    let vector = vectors[usize::try_from(op).unwrap()];
                    execute_insert(
                        dataset,
                        i64::try_from(global_id).unwrap(),
                        &format!("agent{agent}"),
                        vector,
                    )
                }
            }
            OpVerb::Update => {
                let (pool_rows, own_rows) = {
                    let guard = registry.lock().unwrap();
                    (guard.pool_rows().to_vec(), guard.own_rows(pick).to_vec())
                };
                if let Some(target_row_id) = resolve_target(&mut target_rng, &pool_rows, &own_rows)
                {
                    let global_id = agent * ops_per_agent + op;
                    let vector = vectors[usize::try_from(op).unwrap()];
                    execute_update(
                        dataset,
                        target_row_id,
                        i64::try_from(global_id).unwrap(),
                        &format!("agent{agent}"),
                        vector,
                    )
                } else {
                    let global_id = agent * ops_per_agent + op;
                    let vector = vectors[usize::try_from(op).unwrap()];
                    execute_insert(
                        dataset,
                        i64::try_from(global_id).unwrap(),
                        &format!("agent{agent}"),
                        vector,
                    )
                }
            }
        };

        match &outcome {
            ExecOutcome::CommittedInsert { row_id, .. } => {
                registry.lock().unwrap().record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedUpdate {
                target_row_id,
                row_id,
            } => {
                let mut guard = registry.lock().unwrap();
                guard.remove(pick, *target_row_id);
                guard.record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedDelete { target_row_id } => {
                registry.lock().unwrap().remove(pick, *target_row_id);
            }
            ExecOutcome::CommittedMultiBatch { row_ids } => {
                let mut guard = registry.lock().unwrap();
                guard.record_own_row(pick, row_ids[0]);
                guard.record_own_row(pick, row_ids[1]);
            }
            ExecOutcome::Dropped => {}
        }
        commit_ops::print_outcome(agent, op, &outcome);

        op += slots_consumed;
        remaining -= slots_consumed;
    }
}
```

- [ ] **Step 3: Build and run the unit tests**

```bash
cargo build -p strata-chaos-worker
cargo test -p strata-chaos-worker
```

Expected: builds clean, all unit tests pass. (`failure_hook_tests` and `stdout_lock_discipline_tests` are unaffected by this task's changes.)

- [ ] **Step 4: Run the chaos fast tier**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` passes. Op *content* per agent (which verbs, which vectors, which targets) is unchanged from before — only *which thread* runs each agent's sequence, and the exact interleaving of `chaos_checkpoint` counts across agents, is now nondeterministic. This is the reproducibility tradeoff the design doc states explicitly; it does not mean invariants can fail, since the invariants never depended on exact interleaving in the first place.

- [ ] **Step 5: Commit**

```bash
git add crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): replace the sequential scheduler with real per-agent threads"
```

---

### Task 4: Reader — generalize `check_once` and add the reverse-direction check (design doc §2a)

**Files:**
- Modify: `crates/chaos-worker/src/reader.rs`
- Modify: `crates/chaos-worker/src/main.rs` (one call-site update)

**Interfaces:**
- Produces: `pub(crate) fn spawn(dataset: Arc<Dataset>, seed: u64) -> (JoinHandle<()>, Arc<AtomicBool>)` (signature gains `seed`). `fn check_once(dataset: &Dataset, name_predicate: &Predicate, reverse_rng: &mut ChaCha8Rng)` (signature gains `reverse_rng`).
- Consumes: `schema::schema_with_row_id`, `strata_query::Predicate`, `strata_storage::Value`, `strata_txn::{Dataset, ROW_ID_COLUMN}` (all unchanged).

- [ ] **Step 1: Write the failing tests for the new signatures**

In `reader.rs`'s `#[cfg(test)] mod tests`, update `spawn_and_stop_against_a_real_but_empty_dataset_does_not_panic`'s call from:

```rust
        let (handle, done) = spawn(Arc::clone(&dataset));
```

to:

```rust
        let (handle, done) = spawn(Arc::clone(&dataset), 42);
```

Update `check_once_passes_against_real_committed_rows`'s final section from:

```rust
        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        // Must not panic: the pruned agent0-only search result must be a
        // subset of the unpruned reference scan's agent0 rows.
        check_once(&dataset, &predicate);
```

to:

```rust
        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        // Must not panic: the pruned agent0-only search result must be a
        // subset of the unpruned reference scan's agent0 rows.
        check_once(&dataset, &predicate, &mut reverse_rng);
```

Add a new test right after it:

```rust
    #[test]
    fn check_once_passes_against_a_real_multi_batch_commit() {
        // Two rows from the SAME multi-batch commit (one segment, one
        // merged zone map spanning both) -- exercises the
        // reverse-direction check's own-vector round-trip against a real
        // committed row, not just the subset check. Real cross-SEGMENT
        // merge coverage still needs multiple segments, which only a real
        // chaos run produces (see the module doc).
        let dir = temp_dir("check-once-multi-batch");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "agent0", [0.0, 1.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        // Run several times: the reverse check picks a pseudo-random
        // reference row each call, and with 2 candidate rows this
        // exercises both across repeated calls.
        for _ in 0..10 {
            check_once(&dataset, &predicate, &mut reverse_rng);
        }

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p strata-chaos-worker --lib reader::
```

Expected: FAIL to compile — `spawn`/`check_once` don't accept the new parameters yet.

- [ ] **Step 3: Implement the generalized `check_once` and updated `spawn`**

Replace `reader.rs`'s imports (the `use` block) with:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt64Array};
use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

use crate::schema::schema_with_row_id;
```

Add this constant next to the existing ones:

```rust
/// Distinct RNG stream for the reverse-direction check's per-poll
/// reference-row pick — see design doc Part 2 §2a. Seeded once from the
/// worker's own `seed` at thread-spawn time, then threaded through every
/// poll iteration so the pick sequence is deterministic across runs even
/// though which rows exist at poll time isn't.
const READER_REVERSE_STREAM: u64 = 0xB33F_ACE5_0000_0003;
```

Add this helper right after `disagreement`:

```rust
/// Asserts every id in `pruned_row_ids` is present in `reference_row_ids`
/// — shared by every predicate this reader checks (`name`, and later the
/// `id`-range compound predicate). `predicate_description` names the
/// predicate for the panic message only.
fn assert_pruned_is_subset_of_reference(
    pruned_row_ids: &[u64],
    reference_row_ids: &HashSet<u64>,
    predicate_description: &str,
) {
    let bad = disagreement(pruned_row_ids, reference_row_ids);
    assert!(
        bad.is_empty(),
        "predicate-pruning disagreement: vector_search with {predicate_description} returned \
         row-ids {bad:?}, which the unpruned reference scan does not have — zone-map pruning \
         (or its merge across multi-batch commits) returned a wrong result"
    );
}

/// Downcasts a named column from an unpruned reference scan to its
/// expected arrow array type — every column this reader reads
/// (`schema_with_row_id`'s fields) is required, so a missing column or a
/// type mismatch is a genuine bug, not a recoverable condition.
fn required_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column(
            batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|_| panic!("schema_with_row_id must include column {name}")),
        )
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} must downcast to the expected arrow array type"))
}
```

Replace `spawn` with:

```rust
/// Spawns the reader thread. The caller must set the returned
/// `Arc<AtomicBool>` (via `Ordering::SeqCst`) once every agent has
/// finished, then join the handle — the thread has no other stop signal
/// (a genuine chaos-induced crash kills it along with the whole process,
/// which needs no explicit signaling at all). `seed` seeds the
/// reverse-direction check's reference-row pick (see
/// `READER_REVERSE_STREAM`) — the same `seed` the worker itself was
/// invoked with.
pub(crate) fn spawn(dataset: Arc<Dataset>, seed: u64) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let done = Arc::new(AtomicBool::new(false));
    let done_for_thread = Arc::clone(&done);
    let handle = std::thread::spawn(move || {
        let name_predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(seed ^ READER_REVERSE_STREAM);
        while !done_for_thread.load(Ordering::SeqCst) {
            check_once(&dataset, &name_predicate, &mut reverse_rng);
            std::thread::sleep(READER_POLL_INTERVAL);
        }
        // One final check so the last batch of commits (landed between
        // the reader's last loop iteration and the writer setting `done`)
        // is still checked at least once.
        check_once(&dataset, &name_predicate, &mut reverse_rng);
    });
    (handle, done)
}
```

Replace `check_once` with:

```rust
fn check_once(dataset: &Dataset, name_predicate: &Predicate, reverse_rng: &mut ChaCha8Rng) {
    let snapshot = dataset.snapshot();
    let schema = schema_with_row_id();

    // Neither call legitimately errors here: an empty snapshot (zero
    // committed files) resolves to Ok(vec![]) on both paths, and this
    // codebase has no compaction/GC that could make a manifest-listed
    // file vanish out from under a live snapshot. A real Err is therefore
    // always a genuine bug -- exactly the class of failure this reader
    // thread exists to surface via the global panic hook (design doc
    // §3.4), so it must panic here, not silently skip the check.
    let pruned = snapshot
        .vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(name_predicate))
        .expect("vector_search must succeed against a live snapshot");
    let pruned_row_ids: Vec<u64> = pruned.into_iter().map(|m| m.row_id).collect();

    let all_rows = snapshot
        .scan(&schema)
        .expect("scan must succeed against a live snapshot");
    let name_col = required_column::<StringArray>(&all_rows, "name");
    let row_id_col = required_column::<UInt64Array>(&all_rows, strata_txn::ROW_ID_COLUMN);
    let vector_col = required_column::<FixedSizeListArray>(&all_rows, "vector");

    let name_reference_indices: Vec<usize> = (0..all_rows.num_rows())
        .filter(|&i| name_col.value(i) == READER_PREDICATE_NAME)
        .collect();
    let name_reference: HashSet<u64> = name_reference_indices
        .iter()
        .map(|&i| row_id_col.value(i))
        .collect();
    assert_pruned_is_subset_of_reference(
        &pruned_row_ids,
        &name_reference,
        &format!("Eq(name, {READER_PREDICATE_NAME:?})"),
    );

    // Reverse-direction check (design doc Part 2 §2a): one pseudo-randomly
    // chosen name-scoped reference row per poll, queried by its OWN
    // vector under the same predicate. If zone-map pruning ever wrongly
    // excludes the segment holding that row, it would come back missing
    // or replaced by a farther point -- the failure mode the subset-only
    // check above structurally cannot see. Skipped when there's no
    // reference row yet.
    if !name_reference_indices.is_empty() {
        let idx =
            name_reference_indices[reverse_rng.random_range(0..name_reference_indices.len())];
        let expected_row_id = row_id_col.value(idx);
        let vector_value = vector_col.value(idx);
        let vector_values: &Float32Array = vector_value
            .as_any()
            .downcast_ref()
            .expect("vector column elements must be Float32");
        let query: Vec<f32> = (0..vector_values.len())
            .map(|i| vector_values.value(i))
            .collect();
        let hits = snapshot
            .vector_search(&query, 1, Some(name_predicate))
            .expect("vector_search must succeed against a live snapshot");
        assert!(
            hits.first()
                .is_some_and(|h| h.row_id == expected_row_id && h.squared_distance < 1.0),
            "reverse-direction disagreement: row {expected_row_id}'s own vector, queried under \
             Eq(name, {READER_PREDICATE_NAME:?}), did not come back as the top-1 pruned hit \
             (got {hits:?}) — zone-map pruning wrongly excluded the segment holding this row"
        );
    }
}
```

- [ ] **Step 4: Update `main.rs`'s call site**

Change:

```rust
    let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset));
```

to:

```rust
    let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset), seed);
```

- [ ] **Step 5: Update the module doc comment's gap description**

`reader.rs`'s module doc (top of file) currently states this module "does NOT verify zone-map-merge correctness" and describes the reverse direction as "unimplemented." Update the doc comment to reflect that the reverse direction is now implemented (this task) while the `id`-range check is not yet (Task 5 adds it) — replace the whole module doc comment with:

```rust
//! The live predicate-pruning correctness check — see design doc §3.3 and
//! `2026-07-30-chaos-worker-real-concurrency-and-zonemap-verification-design.md`
//! Part 2. Runs on its own thread for the whole worker process lifetime,
//! concurrently with the agent threads, comparing zone-map-pruned
//! predicate queries against unpruned references on the SAME snapshot.
//!
//! Three checks run every poll: (1) the original pruned-subset-of-reference
//! check for `Eq(name, "agent0")` (§3.3's first direction — pruned ⊆
//! reference holds by construction regardless of zone-map correctness,
//! since `vector_search`'s pruned path applies `live_set` as an exact
//! per-row membership filter after pruning); (2) a reverse-direction check
//! (one pseudo-randomly chosen name-scoped reference row per poll, queried
//! by its own vector) that CAN catch a merged zone-map range too narrow,
//! wrongly pruning a segment out; (3) [not yet implemented — see Task 5]
//! an `id`-range compound-predicate subset check, which exercises real
//! cross-batch/cross-segment min/max merge arithmetic, unlike `name`
//! (constant per agent, hence a degenerate merge for that column alone).
```

- [ ] **Step 6: Run the tests**

```bash
cargo test -p strata-chaos-worker
```

Expected: all pass, including the new multi-batch test.

- [ ] **Step 7: Run the full workspace test suite**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` still passes.

- [ ] **Step 8: Commit**

```bash
git add crates/chaos-worker/src/reader.rs crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): add reverse-direction zone-map check to the reader"
```

---

### Task 5: Reader — `id`-range compound-predicate check (design doc §2b)

**Files:**
- Modify: `crates/chaos-worker/src/reader.rs`

**Interfaces:**
- Consumes: `required_column`, `assert_pruned_is_subset_of_reference` (from Task 4). `strata_query::Predicate::{And, GtEq, Lt}`.

- [ ] **Step 1: Write the failing tests**

Add to `reader.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn check_once_passes_against_rows_spanning_a_real_id_range() {
        // Two rows with distinct business ids from one multi-batch commit
        // -- makes the id-range split's lo/hi arithmetic load-bearing
        // (min=1, max=2 -> lo=1, hi=2, so exactly one row falls in
        // [lo, hi)).
        let dir = temp_dir("check-once-id-range");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "agent0", [0.0, 1.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        check_once(&dataset, &predicate, &mut reverse_rng);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_once_skips_the_id_range_check_with_fewer_than_two_distinct_ids() {
        // A single row -- distinct_ids.len() == 1, so the id-range block
        // must not run (and, in particular, must not construct an empty
        // or inverted range). Passing at all is the assertion: an
        // off-by-one in the skip condition would panic here.
        let dir = temp_dir("check-once-single-row");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        let mut reverse_rng = ChaCha8Rng::seed_from_u64(1);
        check_once(&dataset, &predicate, &mut reverse_rng);

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run them to verify the current behavior**

```bash
cargo test -p strata-chaos-worker --lib reader::tests::check_once_passes_against_rows_spanning_a_real_id_range
cargo test -p strata-chaos-worker --lib reader::tests::check_once_skips_the_id_range_check_with_fewer_than_two_distinct_ids
```

Expected: both PASS already (the id-range check doesn't exist yet, so there's nothing to fail against) — these are regression tests for the implementation added next, not red/green TDD in the strict sense, since there's no way to make "check_once panics because the id-range check is missing" a meaningful red state. Proceed to Step 3 regardless.

- [ ] **Step 3: Add the `id`-range check to `check_once`**

Add `Int64Array` to `reader.rs`'s arrow import:

```rust
use arrow::array::{Array, FixedSizeListArray, Float32Array, Int64Array, StringArray, UInt64Array};
```

At the end of `check_once` (after the reverse-direction check block), add:

```rust

    // id-range compound-predicate check (design doc Part 2 §2b): `name`
    // is constant per agent (a degenerate zone-map merge); the business
    // `id` column genuinely varies per row, so this is the actual
    // zone-map-merge exerciser. Scoped to the FULL visible table (every
    // agent's rows plus pool rows), not just the name-scoped reference set
    // above -- deliberately broader scope than the name check. Skipped
    // when fewer than 2 distinct ids are visible (nothing to split).
    let id_col = required_column::<Int64Array>(&all_rows, "id");
    let distinct_ids: HashSet<i64> = (0..all_rows.num_rows()).map(|i| id_col.value(i)).collect();
    if distinct_ids.len() >= 2 {
        let min_id = *distinct_ids.iter().min().unwrap();
        let max_id = *distinct_ids.iter().max().unwrap();
        let lo = min_id;
        let hi = min_id + (max_id - min_id) / 2 + 1;
        let id_predicate = Predicate::And(
            Box::new(Predicate::GtEq("id".to_string(), Value::Int64(lo))),
            Box::new(Predicate::Lt("id".to_string(), Value::Int64(hi))),
        );
        let id_pruned = snapshot
            .vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(&id_predicate))
            .expect("vector_search must succeed against a live snapshot");
        let id_pruned_row_ids: Vec<u64> = id_pruned.into_iter().map(|m| m.row_id).collect();
        let id_reference: HashSet<u64> = (0..all_rows.num_rows())
            .filter(|&i| {
                let id = id_col.value(i);
                id >= lo && id < hi
            })
            .map(|i| row_id_col.value(i))
            .collect();
        assert_pruned_is_subset_of_reference(
            &id_pruned_row_ids,
            &id_reference,
            &format!("And(GtEq(id,{lo}), Lt(id,{hi}))"),
        );
    }
```

- [ ] **Step 4: Update the module doc comment**

Change the `[not yet implemented — see Task 5]` line from Task 4's Step 5 to describe the check as implemented:

```rust
//! (3) an `id`-range compound-predicate subset check (`Predicate::And(GtEq,
//! Lt)` over the full visible table's `id` column, split at its
//! midpoint), which exercises real cross-batch/cross-segment min/max merge
//! arithmetic, unlike `name` (constant per agent, hence a degenerate merge
//! for that column alone).
```

- [ ] **Step 5: Run the tests**

```bash
cargo test -p strata-chaos-worker
```

Expected: all pass.

- [ ] **Step 6: Run the full workspace test suite**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` still passes.

- [ ] **Step 7: Commit**

```bash
git add crates/chaos-worker/src/reader.rs
git commit -m "feat(chaos-worker): add id-range zone-map-merge check to the reader"
```

---

### Task 6: Full validation — measure real contention, tune if needed, thorough tier

Confirms the two gaps are actually closed: real `dropped` (conflict) ack lines must now be observable, and the reader's new checks must survive real chaos-tier volume. Per the design doc's flagged open question, `POOL_SIZE`/`NUM_AGENTS`/`OPS_PER_AGENT` may need tuning if real threads rarely overlap enough to produce genuine conflicts.

**Files:**
- Possibly modify: `crates/chaos-worker/src/main.rs` (`POOL_SIZE`) and/or `tests/sim/tests/chaos.rs` (`NUM_AGENTS`/`OPS_PER_AGENT`) — only if Step 2 measures zero drops.

- [ ] **Step 1: Full workspace gate**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo test --workspace
```

Expected: all clean. Fix anything that doesn't pass before proceeding — do not tune constants to paper over a build/lint/test failure.

- [ ] **Step 2: Measure real conflict-drop frequency**

Build the worker in release mode once, then invoke it directly (bypassing `tests/sim`'s crash injection entirely — `STRATA_CHAOS_ABORT_AT` unset) across a range of seeds, with the SAME `NUM_AGENTS`/`OPS_PER_AGENT` values `tests/sim/tests/chaos.rs` uses (3 and 5), to get a clean measurement of how often real thread interleaving produces at least one genuine conflict:

```bash
cargo build --release -p strata-chaos-worker
```

Then, from a shell, run 50 seeds and count how many produce a `dropped` line (adjust the binary path for your platform if `cargo build`'s output location differs):

```bash
for seed in $(seq 0 49); do
  dir="/tmp/strata-chaos-measure-$seed"
  rm -rf "$dir"
  target/release/chaos-worker "$dir" "$seed" 3 5 > /tmp/chaos-measure-out-$seed.txt
  rm -rf "$dir"
done
grep -l "dropped" /tmp/chaos-measure-out-*.txt | wc -l
```

(On Windows, run the equivalent loop in PowerShell or via the Bash tool's git-bash shell — the binary is `target\release\chaos-worker.exe`.)

- [ ] **Step 3: Tune if zero (or very few) drops occurred**

If 50 seeds produce zero `dropped` lines: real threads aren't overlapping enough within `OPS_PER_AGENT = 5` ops to collide on the shared pool. Apply, in order, whichever of these is needed (re-measuring with Step 2's loop after each change, stopping once drops occur across a meaningful fraction of seeds):

1. First, reduce `crates/chaos-worker/src/main.rs`'s `POOL_SIZE` constant from `6` to `3` — a smaller contested pool concentrates the 50%-pool-targeting probability onto fewer rows, upping same-row collision odds without touching `tests/sim`.
2. If still zero, raise `tests/sim/tests/chaos.rs`'s `OPS_PER_AGENT` from `5` to `10` — more ops per agent means more wall-clock time for threads to genuinely overlap.

If either constant is changed, update `tests/sim/tests/chaos.rs`'s `MAX_ABORT_THRESHOLD` comment/value only if the true checkpoint-count range actually shifts enough to matter (re-run the binary-search approach the existing comment describes, or reason from the existing formula in that comment) — don't bump it speculatively.

Record the measured before/after drop rate directly in the commit message for whichever change was needed (see Step 6) — no new design doc for this.

- [ ] **Step 4: Re-run the fast tier**

```bash
cargo test --workspace
```

Expected: `fast_tier_random_seeds_survive_random_crash_points` passes, and (per Step 2/3's measurement) real seeds now genuinely exercise the `Dropped`/conflict-retry path — this is the evidence gap 1 asked for.

- [ ] **Step 5: Run the thorough tier**

```bash
STRATA_CHAOS_THOROUGH=1 cargo test --workspace --test chaos thorough_tier_satisfies_the_phase_7_exit_criterion -- --nocapture
```

Expected: "thorough tier: 2000/2000 seeds checked, zero violations so far" (or the final matching line), confirming all five invariants still hold under a run where real conflict/drop and the reader's two new checks are genuinely live, not theoretical.

- [ ] **Step 6: Commit (only if Step 3 required a tuning change)**

```bash
git add crates/chaos-worker/src/main.rs tests/sim/tests/chaos.rs
git commit -m "$(cat <<'EOF'
tune(chaos-worker): <describe the exact constant change>

Measured <N>/50 seeds producing a genuine dropped (conflict) ack line
before this change, <M>/50 after -- real per-agent OS threads need
<the tuned constant> to reliably overlap enough for
Transaction::commit's OCC conflict path to actually fire.
EOF
)"
```

If Step 2 already showed real drops with no tuning needed, skip this step (no commit — nothing changed).

- [ ] **Step 7: Report the measured evidence**

Before requesting final review, summarize (for the PR description / final reviewer): the measured conflict-drop rate, whether tuning was needed, and confirmation that the thorough tier's 2000-seed run passed with zero violations. This is the direct evidence PR #47's gap 1 and gap 2 are closed, not just structurally unreachable-to-fail.

---

## After all tasks: whole-branch review and PR update

Once Tasks 1-6 are complete and reviewed individually, dispatch a final whole-branch code review (Opus) over the full diff since `origin/main` (this branch already has PR #47 open against `main`), per `superpowers:subagent-driven-development`'s process. Push the resulting commits to `feat/chaos-worker-workload-extension` — no new PR needed, since #47 is still open and tracks this branch.
