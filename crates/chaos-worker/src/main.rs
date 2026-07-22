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
