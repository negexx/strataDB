//! Chaos-testing worker: runs `NUM_AGENTS` real OS threads (`run_agent`),
//! each committing its own seed-derived op sequence against a shared,
//! real `Dataset`, printing a `starting` ack line immediately before and a
//! `committed`/`dropped` ack line immediately after every op's commit
//! attempt. Meant to be spawned as a child process by `tests/sim`'s
//! orchestrator with `STRATA_CHAOS_ABORT_AT` set, so it may be aborted
//! mid-run by `strata_storage::chaos`. Per-agent op *content* (which verb,
//! which vector, which target) is fully seed-derived and reproducible;
//! cross-agent *interleaving* is genuine OS thread scheduling and is NOT
//! reproducible from the seed alone. See `docs/audit/phase-1/audit.md` for the
//! current verification boundary.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod commit_ops;
mod ops;
mod reader;
mod schema;

use std::io::Write as _;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;

use commit_ops::{
    ExecOutcome, Registry, execute_delete, execute_insert, execute_multi_batch_insert,
    execute_update, print_line,
};
use ops::{OpVerb, generate_verb_sequence, resolve_slot_consumption, resolve_target};

/// Reserved exit code [`install_failure_hook`]'s hook uses for a genuine
/// panic, distinct from `chaos_checkpoint`'s `std::process::abort()` (an
/// entirely different termination mechanism, not a panic) — so
/// `tests/sim`'s orchestrator can tell a genuine bug apart from an
/// expected chaos-induced crash without decoding OS-specific exit
/// signals. See design doc §3.4.
const GENUINE_FAILURE_EXIT_CODE: i32 = 2;
const POOL_SIZE: u64 = 6;
const POOL_STREAM: u64 = 0x9001_5EED_0000_0001;
const TARGET_STREAM: u64 = 0x7A46_E7D0_0000_0002;

/// Installs a global panic hook: any panic, on the main thread or the
/// reader thread, prints a `GENUINE_FAILURE: <message>` line and exits
/// with [`GENUINE_FAILURE_EXIT_CODE`]. See design doc §3.4.
///
/// This unconditionally exits the process, which preempts
/// `crates/storage`'s `catch_unwind`-based recovery for a corrupt data
/// file (`crates/storage/src/datafile.rs`'s `read_batch`/
/// `read_batch_columns`) — a panic hook runs at the panic site, before
/// unwinding begins, so `catch_unwind` never gets a chance to observe and
/// convert the payload. Accepted deliberately, not overlooked: design doc
/// §3.4 mandates surfacing *any* panic as a genuine failure for this
/// harness, and a data file this worker itself just wrote and fsynced
/// before the manifest CAS that publishes it cannot be the truncated/
/// corrupt file that recovery path exists for (that path is for
/// untrusted/externally-corrupted input, not this worker's own writes) —
/// so the recovery path this preempts should not be reachable from a
/// chaos run in practice, and would be a design bug if it ever were.
fn install_failure_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let location = info
            .location()
            .map(|l| format!(" at {}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        // Best-effort: if even this write fails, still exit with the
        // reserved code below so the orchestrator's exit-code check still
        // fires (it does not require the message line to be present).
        let _ = writeln!(out, "GENUINE_FAILURE: {message}{location}");
        let _ = out.flush();
        std::process::exit(GENUINE_FAILURE_EXIT_CODE);
    }));
}

/// Commits `POOL_SIZE` individual single-row inserts before the
/// interleaved phase starts, establishing the shared contested row-id
/// pool — see design doc §3.1. Business ids are negative (`-1..-POOL_SIZE`)
/// so they can never collide with an agent's own `global_id` business ids
/// (always >= 0). Each insert prints a `"pool starting insert row {i}"`
/// line immediately before `execute_insert` -- part of the same ack
/// protocol `run_agent` uses; see that function's doc for the contract.
fn setup_contested_pool(dataset: &strata_txn::Dataset, seed: u64, registry: &mut Registry) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ POOL_STREAM);
    for i in 0..POOL_SIZE {
        let business_id = -1 - i64::try_from(i).unwrap();
        let vector = [
            rng.random::<f32>(),
            rng.random::<f32>(),
            rng.random::<f32>(),
        ];
        print_line(&format!("pool starting insert row {i}"));
        let outcome = execute_insert(dataset, business_id, "pool", vector);
        let ExecOutcome::CommittedInsert { row_id, .. } = outcome else {
            panic!("pool setup insert must always commit cleanly: {outcome:?}");
        };
        registry.record_pool_row(row_id);
        print_line(&format!("pool committed insert row_id {row_id}"));
    }
}

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
            .or_else(|_| strata_txn::Dataset::create(dir, strata_txn::mvp_fixtures::mvp_schema()))
            .expect("failed to open or create dataset"),
    );

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
    setup_contested_pool(&dataset, seed, &mut registry);
    let registry = Arc::new(Mutex::new(registry));

    let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset), seed);

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
                run_agent(
                    &dataset,
                    &registry,
                    agent,
                    ops_per_agent,
                    &vectors,
                    &verbs,
                    seed,
                );
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

/// Runs one agent's full, pre-generated op sequence to completion,
/// sequentially within this thread, against the shared `dataset`/
/// `registry` — see design doc Part 1. `resolve_slot_consumption`'s
/// downgrade logic still applies per-thread; the only remaining source of
/// interleaving randomness across agents is genuine OS thread scheduling,
/// which is the actual thing this design exists to exercise. Op-target
/// *values* (which pool/own row gets picked) are consequently no longer
/// diffable run-to-run the way the RNG *stream* still is -- two runs of
/// the same seed draw the same targets-to-choose-from in the same order,
/// but which ones actually exist at each draw depends on cross-thread
/// timing, so the resolved values themselves can differ.
///
/// **The `starting`/`committed`/`dropped` ack-line contract is
/// load-bearing, not just logging.** Every op prints exactly one
/// `"agent {agent} starting {verb} op {op}"` line, with
/// `"target_row_id {id}"` appended for resolved Delete/Update targets,
/// via [`print_line`], IMMEDIATELY before its `execute_*` call -- using
/// the FINAL resolved verb (after any Delete/Update-to-Insert downgrade),
/// never the originally-drawn one -- and exactly one `committed`/`dropped`
/// completion line (via [`commit_ops::print_outcome`]) immediately after.
/// `tests/sim/tests/chaos.rs`'s entire crash-tolerance budget for the
/// lost/phantom-commit invariants is computed by matching these two
/// lines: any `starting` line with no matching completion line by the end
/// of a run's captured stdout is treated as the op that was genuinely in
/// flight when a chaos abort fired. Its verb controls phantom tolerance;
/// a Delete/Update target controls the exact lost-ID tolerance (see
/// `chaos.rs`'s `run_worker`/`crash_tolerance`). Moving a `starting` print after its `execute_*`
/// call, adding a new op path that skips it, or printing the
/// pre-downgrade verb instead of the resolved one would silently corrupt
/// that budget without failing to compile or necessarily failing any
/// single test run.
#[allow(clippy::too_many_lines)]
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
                print_line(&format!("agent {agent} starting insert op {op}"));
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
                print_line(&format!("agent {agent} starting multibatch op {op}"));
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
                    print_line(&format!(
                        "agent {agent} starting delete op {op} target_row_id {target_row_id}"
                    ));
                    execute_delete(dataset, target_row_id)
                } else {
                    // No eligible target yet -- downgrade to Insert per design doc §3.1.
                    let global_id = agent * ops_per_agent + op;
                    let vector = vectors[usize::try_from(op).unwrap()];
                    print_line(&format!("agent {agent} starting insert op {op}"));
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
                    print_line(&format!(
                        "agent {agent} starting update op {op} target_row_id {target_row_id}"
                    ));
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
                    print_line(&format!("agent {agent} starting insert op {op}"));
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

#[cfg(test)]
mod stdout_lock_discipline_tests {
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
    #[test]
    fn a_thread_holding_the_stdout_lock_blocks_another_threads_lock_until_it_is_dropped() {
        let stdout = std::io::stdout();
        let out = stdout.lock(); // Simulates main() still holding its lock.

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            // Simulates the panic hook's own stdout().lock() call from a
            // different thread while main's lock is still held. Signals
            // "about to call lock()" first so the parent's 200ms window
            // below measures actual blocking, not thread-spawn latency.
            let _ = started_tx.send(());
            let stdout = std::io::stdout();
            let _out = stdout.lock();
            let _ = tx.send(());
        });

        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("spawned thread should signal it's about to call lock() promptly");
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_millis(200)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "a second thread's stdout lock() must block while the first thread still holds \
             it -- if this doesn't block, the assertion below (lock released -> unblocks) \
             can't prove anything, since there'd be no contention to release in the first place"
        );

        drop(out); // Mirrors main()'s drop(out) before reader_handle.join().

        rx.recv_timeout(std::time::Duration::from_secs(5)).expect(
            "the spawned thread's stdout lock acquisition should complete promptly once the \
             holding thread's lock is dropped -- a timeout here means something is still \
             holding the stdout lock, reintroducing the deadlock this test guards against",
        );
        handle.join().unwrap();
    }
}
