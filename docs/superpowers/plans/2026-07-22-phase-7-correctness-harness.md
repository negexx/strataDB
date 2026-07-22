# Phase 7 Correctness Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, seed-reproducible chaos-testing harness that proves strataDB's concurrent-write guarantees survive a real process crash at an arbitrary point, closing the one gap nothing built so far covers (Phase 1 proved single-writer crash-recovery; Phase 6 proved concurrent-write correctness; nothing has tested both together).

**Architecture:** A feature-gated checkpoint counter in `crates/storage` calls `std::process::abort()` at instrumented durability boundaries when an env var threshold is hit. A new `crates/chaos-worker` binary deterministically interleaves simulated concurrent agents (seeded PRNG picks turn order, not real OS threads) against a real `Dataset` in a real process. A `tests/sim` orchestrator spawns the worker with randomized thresholds, then reopens and checks four invariants.

**Tech Stack:** Rust, existing `strata-txn`/`strata-storage`/`strata-index` crates, `rand`/`rand_chacha` (new direct dependency — seeded, reproducible PRNG; already present transitively via `hnsw_rs`, so no new supply-chain surface).

## Global Constraints

- `unwrap()`/`expect()` are `clippy::warn`, not banned — fine in tests/chaos-worker (a throwaway binary, not a library), never in `crates/storage`'s production, non-test code.
- Any `unsafe` needs a `// SAFETY:` comment; this plan introduces none.
- `chaos-injection` is off by default for every crate — must never affect the real `strata` binary, `strata-txn`, or any published-consumer build.
- Full workspace gate before any task is done: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
- Every task gets reviewed by the Opus reviewer subagent before being marked done, per this project's CLAUDE.md — not optional, regardless of which model implemented it.
- This is the flagship correctness-proof subsystem; escalate to Fable 5/Opus 4.8 for the architecturally significant tasks (the checkpoint mechanism, the orchestrator's invariant logic).

---

### Task 1: `chaos-injection` feature + checkpoint counter primitive

**Files:**
- Modify: `crates/storage/Cargo.toml`
- Create: `crates/storage/src/chaos.rs`
- Modify: `crates/storage/src/lib.rs`

**Interfaces:**
- Produces: `strata_storage::chaos::chaos_checkpoint() -> ()` — public function, real body under the `chaos-injection` feature, an empty inlined no-op otherwise. Later tasks call this unconditionally from `crates/storage/src/datafile.rs` and `crates/storage/src/manifest.rs`.

- [ ] **Step 1: Write the failing test**

Add to `crates/storage/src/chaos.rs` (new file):

```rust
//! Deterministic crash injection for Phase 7's correctness harness. Entirely
//! inert unless compiled with the `chaos-injection` feature AND the
//! `STRATA_CHAOS_ABORT_AT` env var is set — zero cost, zero behavior change
//! for the real `strata` binary or any other consumer. See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md` §3.

#[cfg(feature = "chaos-injection")]
mod real {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CHECKPOINT_COUNT: AtomicU64 = AtomicU64::new(0);

    fn abort_at() -> Option<u64> {
        static ABORT_AT: OnceLock<Option<u64>> = OnceLock::new();
        *ABORT_AT.get_or_init(|| {
            std::env::var("STRATA_CHAOS_ABORT_AT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
    }

    /// Call at each durability-boundary point in the commit protocol.
    /// Increments a process-global counter; if `STRATA_CHAOS_ABORT_AT` is
    /// set and this call is exactly the Nth checkpoint since process start,
    /// aborts immediately — no unwinding, no destructors, exactly what a
    /// real crash at this instant would leave on disk.
    pub fn chaos_checkpoint() {
        let n = CHECKPOINT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        if abort_at() == Some(n) {
            std::process::abort();
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn checkpoint_count_increments_each_call() {
            // CHECKPOINT_COUNT is process-global and shared across tests
            // run in the same binary, so assert on the *delta*, not an
            // absolute value.
            let before = CHECKPOINT_COUNT.load(Ordering::SeqCst);
            chaos_checkpoint();
            chaos_checkpoint();
            let after = CHECKPOINT_COUNT.load(Ordering::SeqCst);
            assert_eq!(after - before, 2);
        }

        #[test]
        fn no_abort_when_env_var_unset() {
            // Absence of STRATA_CHAOS_ABORT_AT (the default, and true in
            // this test process) must never abort no matter how many
            // checkpoints pass.
            for _ in 0..50 {
                chaos_checkpoint();
            }
            // Reaching this line at all is the assertion — an abort would
            // have killed the test process.
        }
    }
}

#[cfg(feature = "chaos-injection")]
pub use real::chaos_checkpoint;

/// No-op, inlined away entirely, when the crate isn't built with
/// `chaos-injection` — the real `strata` binary and every other consumer
/// never pays for this at all.
#[cfg(not(feature = "chaos-injection"))]
#[inline(always)]
pub fn chaos_checkpoint() {}
```

In `crates/storage/Cargo.toml`, add (after the existing `[dependencies]` table, as a new top-level table):

```toml
[features]
chaos-injection = []
```

In `crates/storage/src/lib.rs`, add near the other `pub mod` declarations:

```rust
pub mod chaos;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-storage --features chaos-injection chaos::real::tests -- --nocapture`
Expected: FAIL — compile error, `crate::chaos` module doesn't exist yet / `lib.rs` doesn't declare it. (This is one commit's worth of setup; RED here means "doesn't compile," which is the expected failure mode for a brand-new module before Step 1's file exists — if you're implementing Step 1's code first and only then running this, you'll see it pass immediately, which is fine: the file above already contains both the implementation and its tests together, since a bare checkpoint counter has no meaningful "add test then add empty impl" split.)

- [ ] **Step 3: Confirm the feature-off path compiles and stays silent**

Run: `cargo test -p strata-storage 2>&1 | grep chaos`
Expected: no output — without `--features chaos-injection`, `chaos::real` doesn't exist, so `chaos::real::tests` never even compiles. This is the actual proof the module is inert by default.

- [ ] **Step 4: Run the feature-on tests to verify they pass**

Run: `cargo test -p strata-storage --features chaos-injection chaos:: -- --nocapture`
Expected: PASS — `checkpoint_count_increments_each_call` and `no_abort_when_env_var_unset` both `ok`.

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean. Note `cargo clippy --workspace --all-targets` does NOT enable `chaos-injection` (it's off by default) — also separately run `cargo clippy -p strata-storage --all-targets --features chaos-injection -- -D warnings` and confirm that's clean too, since the feature-on code path needs its own clippy pass.

- [ ] **Step 6: Commit**

```bash
git add crates/storage/Cargo.toml crates/storage/src/chaos.rs crates/storage/src/lib.rs
git commit -m "feat(storage): add chaos-injection feature and checkpoint counter primitive"
```

---

### Task 2: Wire checkpoints into the four durability boundaries

**Files:**
- Modify: `crates/storage/src/datafile.rs`
- Modify: `crates/storage/src/manifest.rs`
- Test: `crates/storage/tests/chaos_checkpoint_actually_aborts.rs` (new)

**Interfaces:**
- Consumes: `crate::chaos::chaos_checkpoint()` from Task 1.
- Produces: nothing new for later tasks — this task's deliverable is the wiring itself, verified by an end-to-end subprocess test (calling `chaos_checkpoint()` in-process inside a unit test would abort the test runner itself, so verification must spawn a real child process).

- [ ] **Step 1: Write the failing test**

Create `crates/storage/tests/chaos_checkpoint_actually_aborts.rs`:

```rust
//! Proves `chaos_checkpoint()` is actually wired into a real durability
//! path, not just present as an unused function. Must run in a subprocess:
//! calling an aborting function in-process would kill this very test
//! binary. This is a `tests/` integration test (not a unit test in
//! `chaos.rs`) specifically so it can build itself as its own tiny binary
//! and exec that binary as a child — see Step 3's helper.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

#[test]
fn commit_manifest_aborts_at_the_configured_checkpoint() {
    // This test only makes sense when strata-storage was built with
    // chaos-injection — if the feature is off, commit_manifest's
    // checkpoints are no-ops and nothing would ever abort, which would
    // make this test either fail confusingly or pass for the wrong
    // reason. Skip cleanly instead of asserting the wrong thing.
    if std::env::var("STRATA_CHAOS_TEST_HELPER_BUILT").is_err() {
        eprintln!(
            "skipping: run via `cargo test -p strata-storage --features chaos-injection \
             --test chaos_checkpoint_actually_aborts`"
        );
        return;
    }

    let helper_bin = env!("CARGO_BIN_EXE_chaos_checkpoint_helper");
    let output = Command::new(helper_bin)
        .env("STRATA_CHAOS_ABORT_AT", "1")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "helper should have aborted at checkpoint 1, but exited normally"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(6), // SIGABRT
            "expected SIGABRT (process::abort), got: {:?}",
            output.status
        );
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-storage --test chaos_checkpoint_actually_aborts`
Expected: FAIL — `CARGO_BIN_EXE_chaos_checkpoint_helper` doesn't exist (no such binary target defined yet), compile error.

- [ ] **Step 3: Add the helper binary and wire the checkpoints**

Add to `crates/storage/Cargo.toml` (a new `[[bin]]` section, test-only in spirit but Cargo doesn't have a "test-only binary" concept — gate it so it only builds with the feature):

```toml
[[bin]]
name = "chaos_checkpoint_helper"
path = "tests/chaos_checkpoint_helper.rs"
required-features = ["chaos-injection"]
```

Create `crates/storage/tests/chaos_checkpoint_helper.rs`:

```rust
//! Standalone helper binary for
//! `tests/chaos_checkpoint_actually_aborts.rs` — performs one real
//! `commit_manifest` call so the test can observe whether the configured
//! checkpoint actually aborts it. Only built with `chaos-injection`
//! (see `required-features` in Cargo.toml).
#![allow(clippy::unwrap_used, clippy::expect_used)]

fn main() {
    let dir = std::env::temp_dir().join(format!("strata-chaos-helper-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let manifest = strata_storage::Manifest::empty();
    strata_storage::commit_manifest(&dir, &manifest).unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
```

Now wire the four checkpoints. In `crates/storage/src/datafile.rs`, modify `write_batch`:

```rust
pub fn write_batch(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    crate::chaos::chaos_checkpoint(); // data-file content is now durable
    Ok(())
}
```

And `sync_dir`:

```rust
pub fn sync_dir(dir: &Path) -> Result<()> {
    if let Ok(handle) = File::open(dir) {
        let _ = handle.sync_all();
    }
    crate::chaos::chaos_checkpoint(); // directory entries are now durable (best-effort per-platform, see doc comment above)
    Ok(())
}
```

In `crates/storage/src/manifest.rs`, modify `commit_manifest`:

```rust
pub fn commit_manifest(dataset_dir: &Path, manifest: &Manifest) -> Result<()> {
    let versions = versions_dir(dataset_dir);
    fs::create_dir_all(&versions)?;

    let final_path = manifest_path(dataset_dir, manifest.version);
    let tmp_path = versions.join(format!(".tmp-{}", manifest.version));

    let json = serde_json::to_vec_pretty(manifest)?;
    {
        let mut tmp_file = File::create(&tmp_path)?;
        tmp_file.write_all(&json)?;
        tmp_file.sync_all()?;
        crate::chaos::chaos_checkpoint(); // tmp manifest is durable, about to rename
    }
    fs::rename(&tmp_path, &final_path)?;
    crate::chaos::chaos_checkpoint(); // renamed into place, about to fsync the directory entry

    crate::datafile::sync_dir(&versions)?;

    Ok(())
}
```

(`sync_dir`'s own checkpoint, added above, covers the final directory-fsync boundary — no separate call needed after it here.)

- [ ] **Step 4: Run test to verify it passes**

Run: `STRATA_CHAOS_TEST_HELPER_BUILT=1 cargo test -p strata-storage --features chaos-injection --test chaos_checkpoint_actually_aborts -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Also run `cargo clippy -p strata-storage --all-targets --features chaos-injection -- -D warnings` separately (the new `[[bin]]` only builds with the feature on).

- [ ] **Step 6: Commit**

```bash
git add crates/storage/Cargo.toml crates/storage/src/datafile.rs crates/storage/src/manifest.rs crates/storage/tests/chaos_checkpoint_actually_aborts.rs crates/storage/tests/chaos_checkpoint_helper.rs
git commit -m "feat(storage): wire chaos_checkpoint into the four commit-protocol durability boundaries"
```

---

### Task 3: `crates/chaos-worker` — basic sequential worker

**Files:**
- Create: `crates/chaos-worker/Cargo.toml`
- Create: `crates/chaos-worker/src/main.rs`
- Modify: `Cargo.toml` (workspace root — add member)

**Interfaces:**
- Produces: a `chaos-worker` binary, CLI usage `chaos-worker <dir> <seed> <num_agents> <ops_per_agent>`. Prints `agent <A> committed op <K> row_id <R>\n` (flushed immediately) after every successful commit. Exits 0 if every agent finishes; if `chaos_checkpoint()` fires, the process aborts (non-zero exit / signal) with whatever it had already printed and flushed still readable by a parent capturing stdout.
- This task's version has NO interleaving yet — each agent runs to completion before the next starts. Task 4 adds real interleaving. This keeps the vertical slice small: prove the worker/ack-log/seed-derived-ops pattern works before adding the harder scheduling logic.

- [ ] **Step 1: Write the failing test**

Create `crates/chaos-worker/Cargo.toml`:

```toml
[package]
name = "strata-chaos-worker"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = false

[[bin]]
name = "chaos-worker"
path = "src/main.rs"

[lints]
workspace = true

[dependencies]
strata-txn = { path = "../txn", features = ["chaos-injection"] }
strata-storage = { path = "../storage", features = ["chaos-injection"] }
rand = "0.9"
rand_chacha = "0.9"
```

(Note: this requires `strata-txn` to also expose a `chaos-injection` feature that forwards to `strata-storage`'s — added in this step below, since `strata-txn` is the crate `chaos-worker` actually calls `Dataset`/`Transaction` through, and Cargo features must be declared on every crate in the dependency chain that needs to pass them through, even if `strata-txn` itself has no chaos code of its own.)

Add to `crates/txn/Cargo.toml`:

```toml
[features]
chaos-injection = ["strata-storage/chaos-injection"]
```

Create `crates/chaos-worker/src/main.rs`:

```rust
//! Chaos-testing worker: deterministically commits a seed-derived sequence
//! of operations against a real `Dataset`, printing and flushing an
//! acknowledgment after every successful commit. Meant to be spawned as a
//! child process by `tests/sim`'s orchestrator with `STRATA_CHAOS_ABORT_AT`
//! set, so it may be aborted mid-run by `strata_storage::chaos`. See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("usage: chaos-worker <dir> <seed> <num_agents> <ops_per_agent>");
    let seed: u64 = args.get(2).expect("missing <seed>").parse().expect("seed must be a u64");
    let num_agents: u64 = args.get(3).expect("missing <num_agents>").parse().expect("num_agents must be a u64");
    let ops_per_agent: u64 = args.get(4).expect("missing <ops_per_agent>").parse().expect("ops_per_agent must be a u64");

    let dataset = strata_txn::Dataset::open(dir)
        .or_else(|_| strata_txn::Dataset::create(dir))
        .expect("failed to open or create dataset");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    for agent in 0..num_agents {
        // Each agent's own RNG is seeded from (global seed, agent index),
        // so its operation sequence is fully determined by the top-level
        // seed regardless of how many agents run before it — required for
        // Task 4's interleaving to reorder agents without changing what
        // each one individually does.
        let mut agent_rng = ChaCha8Rng::seed_from_u64(seed ^ agent);
        for op in 0..ops_per_agent {
            let global_id = agent * ops_per_agent + op;
            #[allow(clippy::cast_precision_loss)]
            let v = global_id as f32;
            use rand::Rng as _;
            let vector = [
                v + agent_rng.random::<f32>(),
                v + agent_rng.random::<f32>(),
                v + agent_rng.random::<f32>(),
            ];
            let batch = strata_txn::mvp_fixtures::mvp_row(
                i64::try_from(global_id).unwrap(),
                &format!("agent{agent}"),
                vector,
            )
            .unwrap();

            let mut txn = dataset.begin();
            txn.insert(batch);
            match txn.commit() {
                Ok(()) => {
                    writeln!(out, "agent {agent} committed op {op} row_id {global_id}").unwrap();
                    out.flush().unwrap();
                }
                Err(e) => {
                    // A pure-insert-only worker (this task) can only ever
                    // get Clean commits — fresh monotonic row-ids never
                    // conflict (design doc §1's "appends never conflict").
                    // A real error here means something is genuinely
                    // broken, not a chaos scenario to tolerate silently.
                    panic!("unexpected commit error: {e}");
                }
            }
        }
    }
}
```

Add `"crates/chaos-worker"` to the root `Cargo.toml`'s `[workspace] members` list, alongside the existing entries.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo build -p strata-chaos-worker`
Expected: FAIL until the files above exist — this is a from-scratch binary, so "the test" for this task is the binary running correctly end-to-end (Step 4), not a unit test; Steps 2-3 here are the build/compile gate instead.

- [ ] **Step 3: Build it**

Run: `cargo build -p strata-chaos-worker`
Expected: builds clean.

- [ ] **Step 4: Run it and verify the acknowledgment output**

Run:
```bash
rm -rf /tmp/chaos-worker-smoke-test
cargo run -p strata-chaos-worker -- /tmp/chaos-worker-smoke-test 42 3 5
```
Expected: 15 lines of `agent A committed op K row_id R` (agents 0-2, ops 0-4 each), exit code 0. Run it a second time with the same args against a fresh directory and confirm the output is byte-for-byte identical — this is the actual proof of determinism for this task.

```bash
rm -rf /tmp/chaos-worker-smoke-test-2
cargo run -p strata-chaos-worker -- /tmp/chaos-worker-smoke-test-2 42 3 5 > /tmp/run1.txt
rm -rf /tmp/chaos-worker-smoke-test-2
cargo run -p strata-chaos-worker -- /tmp/chaos-worker-smoke-test-2 42 3 5 > /tmp/run2.txt
diff /tmp/run1.txt /tmp/run2.txt
```
Expected: no diff output (files identical).

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/chaos-worker/ Cargo.toml crates/txn/Cargo.toml
git commit -m "feat(chaos-worker): add sequential chaos-testing worker binary"
```

---

### Task 4: Deterministic seeded interleaving

**Files:**
- Modify: `crates/chaos-worker/src/main.rs`

**Interfaces:**
- Consumes: Task 3's per-agent seeded operation generation (unchanged — only the *order* operations execute in changes, not what each agent does).
- Produces: same CLI/output contract as Task 3, but agents now genuinely interleave (agent A can buffer/commit an op, then agent B does one, then A again) instead of running to completion one at a time.

- [ ] **Step 1: Write the failing test**

This task changes runtime behavior with no new public interface, so its own "test" is a behavioral property check via the CLI, same pattern as Task 3's Step 4. First, capture Task 3's current (sequential) output shape as the "before":

Run:
```bash
rm -rf /tmp/chaos-worker-interleave-before
cargo run -p strata-chaos-worker -- /tmp/chaos-worker-interleave-before 7 3 4 > /tmp/before.txt
cat /tmp/before.txt
```
Expected: agent 0's four ops complete, then agent 1's four, then agent 2's four (strictly grouped by agent — the current sequential behavior).

- [ ] **Step 2: Confirm the current (sequential) shape, which Step 3 will change**

Run: `awk '{print $2}' /tmp/before.txt`
Expected: `0 0 0 0 1 1 1 1 2 2 2 2` — four `0`s, then four `1`s, then four `2`s. This confirms the grouped-by-agent baseline before interleaving is added.

- [ ] **Step 3: Implement interleaving**

Replace `main`'s per-agent loop in `crates/chaos-worker/src/main.rs` with:

```rust
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("usage: chaos-worker <dir> <seed> <num_agents> <ops_per_agent>");
    let seed: u64 = args.get(2).expect("missing <seed>").parse().expect("seed must be a u64");
    let num_agents: u64 = args.get(3).expect("missing <num_agents>").parse().expect("num_agents must be a u64");
    let ops_per_agent: u64 = args.get(4).expect("missing <ops_per_agent>").parse().expect("ops_per_agent must be a u64");

    let dataset = strata_txn::Dataset::open(dir)
        .or_else(|_| strata_txn::Dataset::create(dir))
        .expect("failed to open or create dataset");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    // Each agent's full operation sequence (what vector/name it will use
    // for each of its ops) is generated up front from (seed, agent index)
    // — unchanged from Task 3 — so interleaving order below only changes
    // *when* an already-fully-determined op happens, never *what* it is.
    use rand::Rng as _;
    let agent_ops: Vec<Vec<[f32; 3]>> = (0..num_agents)
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

    let mut next_op: Vec<u64> = vec![0; num_agents as usize];
    let mut remaining: Vec<u64> = vec![ops_per_agent; num_agents as usize];

    // A single scheduler RNG, seeded from the same top-level seed but a
    // distinct stream (via a fixed XOR constant) from any individual
    // agent's RNG, picks which not-yet-finished agent goes next at each
    // step — this is the actual interleaving driver.
    let mut scheduler_rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA9E1_C0DE_u64);

    loop {
        let live_agents: Vec<usize> = (0..num_agents as usize)
            .filter(|&a| remaining[a] > 0)
            .collect();
        if live_agents.is_empty() {
            break;
        }
        let pick = live_agents[scheduler_rng.random_range(0..live_agents.len())];
        let agent = pick as u64;
        let op = next_op[pick];
        let global_id = agent * ops_per_agent + op;
        let vector = agent_ops[pick][op as usize];

        let batch = strata_txn::mvp_fixtures::mvp_row(
            i64::try_from(global_id).unwrap(),
            &format!("agent{agent}"),
            vector,
        )
        .unwrap();

        let mut txn = dataset.begin();
        txn.insert(batch);
        match txn.commit() {
            Ok(()) => {
                writeln!(out, "agent {agent} committed op {op} row_id {global_id}").unwrap();
                out.flush().unwrap();
            }
            Err(e) => panic!("unexpected commit error: {e}"),
        }

        next_op[pick] += 1;
        remaining[pick] -= 1;
    }
}
```

Add `use rand_chacha::ChaCha8Rng;` and `use rand::SeedableRng;` to the top of the file if not already present from Task 3.

- [ ] **Step 4: Run it and verify interleaving actually happens**

Run:
```bash
rm -rf /tmp/chaos-worker-interleave-after
cargo run -p strata-chaos-worker -- /tmp/chaos-worker-interleave-after 7 3 4 > /tmp/after.txt
awk '{print $2}' /tmp/after.txt
```
Expected: agent indices are now mixed, not grouped (e.g. `1 0 2 0 1 2 ...` rather than `0 0 0 0 1 1 1 1 2 2 2 2`) — confirms real interleaving. Re-run the determinism check from Task 3 Step 4 (same seed twice, diff the output) — must still produce byte-identical output both times, since the scheduler RNG is seeded too.

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): interleave agents via a seeded scheduler instead of running them sequentially"
```

---

### Task 5: `tests/sim` orchestrator — fast tier, all four invariants

**Files:**
- Create: `tests/sim/Cargo.toml`
- Create: `tests/sim/tests/chaos.rs`
- Modify: `Cargo.toml` (workspace root — add member)

**Interfaces:**
- Consumes: the `chaos-worker` binary (Tasks 3-4), located and built via the `escargot` crate (`escargot::CargoBuild::new().bin("chaos-worker").package("strata-chaos-worker").current_release().run()`), NOT via `env!("CARGO_BIN_EXE_...")`. **Correction from an earlier draft of this plan, found during Task 5's implementation:** `CARGO_BIN_EXE_<name>` is only ever defined for binary targets of the *same package* whose integration test is being compiled — it is never set for a dependency's binaries on stable Cargo (cross-package binary artifact access needs the unstabilized `-Z bindeps`/RFC 3028, nightly-only). `escargot` is the standard, well-maintained crate for exactly this "locate/build a sibling workspace binary from an integration test" problem — it shells out to `cargo build --message-format=json` and parses the resulting artifact path via `cargo metadata`, which is more robust than a hand-rolled relative-path guess from `std::env::current_exe()`. This keeps the orchestrator at its architecture-doc-reserved `tests/sim` location (rejected alternative: moving the test into `crates/chaos-worker/tests/`, which would work via the same-package `CARGO_BIN_EXE_` mechanism, but would also pull `chaos-injection` into the orchestrator's own build via Cargo's feature unification — silently breaking the design doc's explicit "verification is a plain read path" contract). Justification for the new dependency (per this project's "don't add dependencies without justifying" rule): `escargot` is dev-only (never ships in the built `strata` binary or any published crate), has no further transitive runtime surface, and is the standard mitigation for this specific, well-known Cargo limitation.
- Consumes: `strata_txn::Dataset` (no `chaos-injection` feature enabled — verification is a plain read path, now genuinely isolated from the worker's build since `tests/sim` no longer depends on `strata-chaos-worker` or its `chaos-injection`-enabled `strata-txn`/`strata-storage` edges at all).

- [ ] **Step 1: Write the failing test**

Create `tests/sim/Cargo.toml`:

```toml
[package]
name = "strata-sim"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = false

[lints]
workspace = true

[dev-dependencies]
strata-txn = { path = "../../crates/txn" }
arrow.workspace = true
rand = "0.9"
rand_chacha = "0.9"
escargot = "0.5"
```

(`strata-chaos-worker` is deliberately NOT a dependency here — see the Interfaces section above on why, and how `escargot` builds/locates its binary instead without pulling its `chaos-injection`-enabled dependency edges into this crate's own build.)

Create `tests/sim/tests/chaos.rs`:

```rust
//! Phase 7 correctness harness: spawns `chaos-worker` as a real child
//! process with a randomized crash checkpoint, then reopens the dataset
//! and checks four invariants. See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::process::Command;

use rand::Rng as _;
use rand::SeedableRng as _;

const NUM_AGENTS: u64 = 3;
const OPS_PER_AGENT: u64 = 5;
/// Comfortably above the total number of checkpoints one full run
/// produces (each commit passes through write_batch's fsync,
/// sync_dir's data-dir fsync, commit_manifest's tmp-sync, rename, and
/// sync_dir's versions-dir fsync — 5 per commit, 15 ops max here — so a
/// threshold in this range can land anywhere from "crash on the very
/// first commit" to "never crashes, all ops complete").
const MAX_ABORT_THRESHOLD: u64 = 200;

struct RunResult {
    acknowledged_row_ids: HashSet<u64>,
    crashed: bool,
}

/// Builds (once, lazily — `OnceLock` caches the result across every
/// `run_worker` call in this test binary rather than re-invoking `cargo
/// build` per iteration) and locates the `chaos-worker` binary. Uses
/// `escargot` instead of `env!("CARGO_BIN_EXE_...")` because that macro is
/// only ever defined for a package's OWN binary targets, never a
/// dependency's — see this task's Interfaces note above.
fn worker_bin_path() -> &'static std::path::Path {
    static WORKER_BIN: std::sync::OnceLock<escargot::CargoRun> = std::sync::OnceLock::new();
    WORKER_BIN
        .get_or_init(|| {
            escargot::CargoBuild::new()
                .bin("chaos-worker")
                .package("strata-chaos-worker")
                .current_release()
                .run()
                .expect("failed to build chaos-worker via escargot")
        })
        .path()
}

fn run_worker(dir: &std::path::Path, seed: u64, abort_at: Option<u64>) -> RunResult {
    let mut cmd = Command::new(worker_bin_path());
    cmd.args([
        dir.to_str().unwrap(),
        &seed.to_string(),
        &NUM_AGENTS.to_string(),
        &OPS_PER_AGENT.to_string(),
    ]);
    if let Some(n) = abort_at {
        cmd.env("STRATA_CHAOS_ABORT_AT", n.to_string());
    }
    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    let acknowledged_row_ids: HashSet<u64> = stdout
        .lines()
        .filter_map(|line| line.rsplit(' ').next())
        .filter_map(|s| s.parse().ok())
        .collect();

    RunResult {
        acknowledged_row_ids,
        crashed: !output.status.success(),
    }
}

fn check_invariants(dir: &std::path::Path, acknowledged: &HashSet<u64>, crashed: bool) {
    // Invariant 1: no corruption. A crash mid-write must never leave an
    // EXISTING dataset unable to open. One narrow, precisely-scoped
    // exception (found during implementation): if the crash landed before
    // the dataset's very first commit_manifest call ever completed its
    // rename (e.g. abort_at=1, landing inside Dataset::create's own
    // initial-manifest write), no manifest file -- not even the initial
    // empty one -- was ever durably created, so NotFound is the correct,
    // expected response to "nothing exists yet," not corruption. This can
    // only be legitimate when nothing was ever acknowledged either
    // (acknowledged non-empty would mean at least one commit fully
    // landed, which requires a manifest to already exist) -- any other
    // open failure, or a NotFound with a non-empty acknowledged set, is
    // genuine corruption and must still fail loudly. All four invariants
    // are trivially satisfied when nothing was ever created, so return
    // early rather than trying to scan a dataset that doesn't exist.
    let dataset = match strata_txn::Dataset::open(dir) {
        Ok(ds) => ds,
        Err(strata_txn::TxnError::NotFound(_)) if acknowledged.is_empty() => return,
        Err(e) => panic!("dataset failed to reopen after crash — corruption: {e}"),
    };

    let schema = strata_txn::mvp_fixtures::mvp_schema();
    let batch = dataset.snapshot().scan(&schema).expect("scan failed after reopen");
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let visible_row_ids: HashSet<u64> = (0..batch.num_rows())
        .map(|i| u64::try_from(id_col.value(i)).unwrap())
        .collect();

    // Invariant 2: no lost commits. Everything acknowledged must be visible.
    let lost: Vec<&u64> = acknowledged.difference(&visible_row_ids).collect();
    assert!(
        lost.is_empty(),
        "lost commits: acknowledged but not visible after reopen: {lost:?}"
    );

    // Invariant 3: no phantom commits. Everything visible must trace back
    // to an acknowledgment, with one narrow, provably-bounded exception
    // (found during implementation): a CRASHED run may have exactly one
    // row that completed commit_manifest's rename (and is therefore
    // genuinely, correctly durable) but whose worker process died before
    // it could print the acknowledgment line -- the classic Jepsen
    // "info"/ambiguous-outcome case (a write can succeed on the server
    // while the client's own confirmation is lost), not a storage-layer
    // bug. The worker is single-threaded and fully completes (or fully
    // fails) each op before starting the next, so at most one op can ever
    // be "in flight" at abort time -- more than one phantom row would
    // indicate a real bug, not this narrow race, so the tolerance stays
    // tight and only applies to crashed runs (a clean exit had time to
    // print every acknowledgment, so it must have zero).
    let phantom: Vec<&u64> = visible_row_ids.difference(acknowledged).collect();
    let max_tolerated_phantoms = usize::from(crashed);
    assert!(
        phantom.len() <= max_tolerated_phantoms,
        "phantom commits: visible after reopen but never acknowledged: {phantom:?} \
         (tolerated at most {max_tolerated_phantoms} for this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    // Invariant 4: row + index consistency. Every acknowledged (and
    // therefore visible) row's own vector must be findable in the HNSW
    // graph — same pattern Phase 6's own
    // losing_transactions_graph_insert_never_lands_when_it_conflicts test
    // used: a near-zero squared_distance on a self-query proves the
    // row's vector is genuinely indexed, not just present in the row
    // store. NOTE (correction from an earlier draft of this plan, found
    // during implementation): this deliberately does NOT also assert
    // `results[0].row_id == row_id` — `VectorMatch::row_id` is the
    // dataset's own internal monotonic row-id (assigned by commit order),
    // a completely different namespace from this workload's `id` schema
    // column (`global_id`, the value checked against `acknowledged`
    // here). Since chaos-worker's whole point is randomized interleaving,
    // a row's commit-order position is scrambled relative to its
    // `global_id` on almost every seed — asserting the two are equal
    // would fail constantly for reasons unrelated to any real bug. The
    // near-zero `squared_distance` on a self-query is what actually
    // proves row+index consistency; which internal row-id it resolves to
    // is irrelevant to this invariant, and the workload's per-row random
    // vector jitter makes cross-row collisions within this distance
    // essentially impossible.
    for &row_id in acknowledged {
        let row_idx = (0..batch.num_rows())
            .find(|&i| u64::try_from(id_col.value(i)).unwrap() == row_id)
            .expect("acknowledged row must be in the scanned batch (invariant 2 already checked this)");
        let vector_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
            .unwrap();
        let values = vector_col.value(row_idx);
        let values: &arrow::array::Float32Array = values.as_any().downcast_ref().unwrap();
        let query: Vec<f32> = (0..values.len()).map(|i| values.value(i)).collect();

        let results = dataset
            .snapshot()
            .vector_search(&query, 1, None)
            .expect("vector_search failed");
        assert!(
            !results.is_empty() && results[0].squared_distance < 0.001,
            "row {row_id} is visible in the row store but not findable in the HNSW graph \
             (row+index consistency violated) — got {results:?}"
        );
    }
}

#[test]
fn fast_tier_random_seeds_survive_random_crash_points() {
    const NUM_SEEDS: u64 = 30;
    let mut master_rng = rand_chacha::ChaCha8Rng::seed_from_u64(0xF457_7E57);

    for seed in 0..NUM_SEEDS {
        let dir = std::env::temp_dir().join(format!("strata-chaos-fast-{}-{seed}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();

        let abort_at = master_rng.random_range(1..MAX_ABORT_THRESHOLD);
        let result = run_worker(&dir, seed, Some(abort_at));

        // Give the OS a moment to fully release file handles after an
        // abort, same precaution the existing Phase 1 crash-recovery test
        // already takes.
        std::thread::sleep(std::time::Duration::from_millis(50));

        check_invariants(&dir, &result.acknowledged_row_ids, result.crashed);

        if !result.crashed {
            // The randomly-picked threshold happened to exceed the total
            // checkpoint count for this seed — the run completed cleanly.
            // Still a valid, still-checked iteration; not a bug.
            assert_eq!(
                result.acknowledged_row_ids.len(),
                (NUM_AGENTS * OPS_PER_AGENT) as usize,
                "worker exited successfully but didn't acknowledge every op"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }
}
```

Add `"tests/sim"` to the root `Cargo.toml`'s `[workspace] members` list.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-sim`
Expected: FAIL — compile error until the `tests/sim/Cargo.toml` (with `escargot` as a dev-dependency) and workspace-member wiring above are in place.

- [ ] **Step 3: Fix wiring issues found**

Confirm `escargot`'s exact API (method names/signatures can drift slightly by version — check the installed `escargot` crate's docs if `CargoBuild`/`CargoRun`'s methods don't match this brief's code exactly) and that `.bin("chaos-worker").package("strata-chaos-worker")` correctly identifies Task 3's binary (bin name `chaos-worker` per its `[[bin]] name`, package name `strata-chaos-worker` per its `[package] name`). Minor API-shape fixes here are expected and don't need escalation; anything that changes the architecture (dependency direction, `tests/sim`'s location, or the `chaos-injection` feature-isolation contract) does.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-sim -- --nocapture`
Expected: PASS — `fast_tier_random_seeds_survive_random_crash_points` completes, `ok`. This run also validates all four invariants held across 30 random seeds and random crash points, each including data-file-content, directory-entry, manifest-tmp, and manifest-rename crash boundaries.

If a real invariant violation is found, STOP — do not weaken the assertion or the check. Report back with the specific seed/threshold that reproduces it (both the seed and the `abort_at` threshold are encoded in the test's own `dir` path — `strata-chaos-fast-{pid}-{seed}-{abort_at}` — so the failing directory name alone is enough to reconstruct the `run_worker(&dir, seed, Some(abort_at))` call that reproduces it) and escalate; this would be an actual correctness bug in Phase 6's implementation being caught by this exact harness doing its job.

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Note this fast tier is now part of default `cargo test --workspace` — confirm its wall-clock cost is genuinely low tens of seconds, not minutes; if it's meaningfully slower, that's worth flagging in the task report even though it's not necessarily a blocker (30 seeds × up to ~15 real fsyncs each is the expected cost).

- [ ] **Step 6: Commit**

```bash
git add tests/sim/ Cargo.toml
git commit -m "test(sim): add Phase 7 chaos orchestrator with all four invariant checks"
```

---

### Task 6: Thorough tier

**Files:**
- Modify: `tests/sim/tests/chaos.rs`

**Interfaces:**
- Consumes: `run_worker`/`check_invariants` from Task 5 (unchanged, reused as-is).
- Produces: a second test, opt-in via env var, that runs the same logic at the "thousands of randomized runs" scale the Phase 7 exit criterion actually asks for.

- [ ] **Step 1: Write the failing test**

Add to `tests/sim/tests/chaos.rs`:

```rust
/// The actual Phase 7 exit criterion: "thousands of randomized
/// concurrent-agent runs, zero invariant violations." Opt-in via
/// `STRATA_CHAOS_THOROUGH=1` — NOT part of default `cargo test --workspace`
/// (see the design doc §5 for why: each iteration's real process spawn +
/// real fsyncs make thousands of them too slow for the normal dev loop).
/// Intended for a scheduled/on-demand CI job.
#[test]
fn thorough_tier_satisfies_the_phase_7_exit_criterion() {
    if std::env::var("STRATA_CHAOS_THOROUGH").is_err() {
        eprintln!("skipping thorough tier: set STRATA_CHAOS_THOROUGH=1 to run it");
        return;
    }

    const NUM_SEEDS: u64 = 2000;
    let mut master_rng = rand_chacha::ChaCha8Rng::seed_from_u64(0x7040_0060_5EED);

    for seed in 0..NUM_SEEDS {
        let dir = std::env::temp_dir().join(format!("strata-chaos-thorough-{}-{seed}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();

        let abort_at = master_rng.random_range(1..MAX_ABORT_THRESHOLD);
        let result = run_worker(&dir, seed, Some(abort_at));

        std::thread::sleep(std::time::Duration::from_millis(50));

        check_invariants(&dir, &result.acknowledged_row_ids, result.crashed);

        std::fs::remove_dir_all(&dir).ok();

        if seed % 100 == 0 {
            eprintln!("thorough tier: {seed}/{NUM_SEEDS} seeds checked, zero violations so far");
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-sim thorough_tier -- --nocapture`
Expected: prints "skipping thorough tier..." and passes trivially without `STRATA_CHAOS_THOROUGH` set — confirm this is the observed behavior (this is the "RED" step in the sense that it proves the opt-out gate works before proving the opt-in path does).

- [ ] **Step 3: Run it for real**

Run: `STRATA_CHAOS_THOROUGH=1 cargo test -p strata-sim thorough_tier --release -- --nocapture`
Expected: runs to completion (this will take real wall-clock time — budget accordingly, and prefer `--release` for this tier specifically since 2000 iterations of debug-build fsync-heavy work is unnecessarily slow), printing progress every 100 seeds, ending in `ok` with zero invariant violations. This run is the actual evidence the Phase 7 exit criterion is met — capture its output for the task report.

- [ ] **Step 4: Run the full workspace gate**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: clean (the thorough tier's default-skip behavior means it doesn't affect this gate's runtime).

- [ ] **Step 5: Commit**

```bash
git add tests/sim/tests/chaos.rs
git commit -m "test(sim): add thorough tier (2000 seeds) satisfying the Phase 7 exit criterion"
```

---

### Task 7: Update CLAUDE.md's stack line

**Files:**
- Modify: `.claude/CLAUDE.md`

**Interfaces:**
- None — documentation only.

- [ ] **Step 1: Make the edit**

In `.claude/CLAUDE.md`'s Stack section, find the line:

```
- **Concurrency correctness:** `loom` (exhaustive interleaving testing of locks/atomics/CAS loops — this is the whole reason Rust was the original recommendation) + `madsim`/`turmoil` for FoundationDB-style deterministic simulation (Phase 7). Unlike C++, both are real, maintained, reusable crates — no bespoke VOPR-style simulator has to be built from scratch.
```

Replace with:

```
- **Concurrency correctness:** `loom` (exhaustive interleaving testing of locks/atomics/CAS loops — this is the whole reason Rust was the original recommendation) for `crates/txn`/`crates/index`. Phase 7's correctness harness (`tests/sim`, `crates/chaos-worker`) does NOT use `madsim`/`turmoil` as originally planned here — both were found to be async/tokio-shaped and a poor fit for this codebase's entirely synchronous production code (see `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md` §2). Phase 7 instead follows Jepsen's methodology: real process spawn, real `std::process::abort()` at instrumented checkpoints, seed-reproducible scenarios.
```

- [ ] **Step 2: Verify the file still reads sensibly**

Run: `grep -n -A3 "Concurrency correctness" .claude/CLAUDE.md`
Expected: the new text reads correctly in context, no dangling references to the old wording elsewhere in the file (check with `grep -n "madsim\|turmoil" .claude/CLAUDE.md` — should show only the updated line, no other stale mentions).

- [ ] **Step 3: Commit**

```bash
git add .claude/CLAUDE.md
git commit -m "docs: update CLAUDE.md's stack line to reflect the madsim/turmoil to real-subprocess pivot"
```
