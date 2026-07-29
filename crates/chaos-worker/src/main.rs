//! Chaos-testing worker: deterministically commits a seed-derived sequence
//! of operations against a real `Dataset`, printing and flushing an
//! acknowledgment after every successful commit. Meant to be spawned as a
//! child process by `tests/sim`'s orchestrator with `STRATA_CHAOS_ABORT_AT`
//! set, so it may be aborted mid-run by `strata_storage::chaos`. See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;

use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;

// Not yet called from main() -- wired in by Task 6 of the workload-extension
// plan, which removes this attribute once it is.
#[allow(dead_code)]
mod ops;
// Not yet called from main() -- wired in by Task 6, which removes these
// attributes once they are.
#[allow(dead_code)]
mod commit_ops;
#[allow(dead_code)]
mod schema;
// Not yet called from main() -- wired in by Task 6, which removes this
// attribute once it is.
#[allow(dead_code)]
mod reader;

/// Installs a global panic hook: any panic, on the main thread or the
/// reader thread, prints a `GENUINE_FAILURE: <message>` line and exits
/// with a reserved code — distinct from `chaos_checkpoint`'s
/// `std::process::abort()` (an entirely different termination mechanism,
/// not a panic) — so `tests/sim`'s orchestrator can tell a genuine bug
/// apart from an expected chaos-induced crash without decoding
/// OS-specific exit signals. See design doc §3.4.
#[allow(dead_code)]
const GENUINE_FAILURE_EXIT_CODE: i32 = 2;

#[allow(dead_code)]
fn install_failure_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        // Best-effort: if even this write fails, still exit with the
        // reserved code below so the orchestrator's exit-code check still
        // fires (it does not require the message line to be present).
        let _ = writeln!(out, "GENUINE_FAILURE: {message}");
        let _ = out.flush();
        std::process::exit(GENUINE_FAILURE_EXIT_CODE);
    }));
}

fn main() {
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

    let dataset = strata_txn::Dataset::open(dir)
        .or_else(|_| strata_txn::Dataset::create(dir))
        .expect("failed to open or create dataset");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    // Each agent's full operation sequence (what vector/name it will use
    // for each of its ops) is generated up front from (seed, agent index)
    // — unchanged from Task 3 — so interleaving order below only changes
    // *when* an already-fully-determined op happens, never *what* it is.
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

    let num_agents_usize = usize::try_from(num_agents).unwrap();
    let mut next_op: Vec<u64> = vec![0; num_agents_usize];
    let mut remaining: Vec<u64> = vec![ops_per_agent; num_agents_usize];

    // A single scheduler RNG, seeded from the same top-level seed but a
    // distinct stream (via a fixed XOR constant) from any individual
    // agent's RNG, picks which not-yet-finished agent goes next at each
    // step — this is the actual interleaving driver.
    let mut scheduler_rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA9E1_C0DE_u64);

    loop {
        let live_agents: Vec<usize> = (0..num_agents_usize)
            .filter(|&a| remaining[a] > 0)
            .collect();
        if live_agents.is_empty() {
            break;
        }
        let pick = live_agents[scheduler_rng.random_range(0..live_agents.len())];
        let agent = pick as u64;
        let op = next_op[pick];
        let global_id = agent * ops_per_agent + op;
        let vector = agent_ops[pick][usize::try_from(op).unwrap()];

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

        next_op[pick] += 1;
        remaining[pick] -= 1;
    }
}

#[cfg(test)]
mod failure_hook_tests {
    use super::install_failure_hook;

    /// Env var gating this test's actual body -- unset (the normal case,
    /// when this test runs as part of the full suite) it's a no-op, so it
    /// never installs the global panic hook and never disturbs any other
    /// test sharing this process. Only the subprocess spawned by
    /// `a_panic_after_the_hook_is_installed_exits_with_the_reserved_code`,
    /// which sets this var and filters to run ONLY this one test, ever
    /// exercises the real body.
    const TRIGGER_ENV_VAR: &str = "CHAOS_WORKER_TEST_TRIGGER_GENUINE_FAILURE";

    #[test]
    fn inner_trigger_genuine_failure() {
        if std::env::var(TRIGGER_ENV_VAR).is_err() {
            return;
        }
        install_failure_hook();
        panic!("deliberate test panic");
    }

    // Spawns THIS SAME test binary (via current_exe()), filtered with
    // libtest's own --exact to run ONLY inner_trigger_genuine_failure,
    // with the trigger env var set -- the only way to observe a global
    // panic hook's effect without corrupting other tests sharing this
    // process. A bare CLI flag checked in fn main() does NOT work here:
    // current_exe() under `cargo test` is the libtest-harness binary,
    // not this crate's real fn main() entry point, so an arbitrary flag
    // reaches libtest's own parser (which rejects it), not fn main().
    #[test]
    fn a_panic_after_the_hook_is_installed_exits_with_the_reserved_code() {
        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "failure_hook_tests::inner_trigger_genuine_failure",
                "--exact",
                "--nocapture",
            ])
            .env(TRIGGER_ENV_VAR, "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("GENUINE_FAILURE: deliberate test panic"),
            "expected the marker line in stdout, got: {stdout}"
        );
    }
}
