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
/// produces (each commit passes through `write_batch`'s fsync,
/// `sync_dir`'s data-dir fsync, `commit_manifest`'s tmp-sync, rename, and
/// `sync_dir`'s versions-dir fsync — 5 per commit, 15 ops max here — so a
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
/// dependency's, on stable Cargo — confirmed during this task's
/// implementation (cross-package binary artifact access needs the
/// unstabilized `-Z bindeps`). See this task's plan-doc Interfaces note.
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
    let batch = dataset
        .snapshot()
        .scan(&schema)
        .expect("scan failed after reopen");
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

    // Invariant 4: row + index consistency. Every visible row's own vector
    // must be findable in the HNSW graph — same pattern Phase 6's own
    // losing_transactions_graph_insert_never_lands_when_it_conflicts test
    // used: a near-zero squared_distance on a self-query proves the
    // row's vector is genuinely indexed, not just present in the row
    // store. Deliberately iterates `visible_row_ids`, not `acknowledged`:
    // the one tolerated phantom row from invariant 3 (durably committed
    // but never acknowledged, because the worker died before it could
    // print) is still genuinely visible and durable, so it should still
    // be checked for row+index consistency — this is a strict superset
    // of the acknowledged set at no extra cost.
    //
    // NOTE: this deliberately does NOT compare `results[0].row_id` against
    // `row_id` here. `row_id` in this loop is the "id" *data* column value
    // chaos-worker printed (mvp_row's caller-supplied id, e.g. its
    // agent/op-derived global_id) — a value chosen by the workload, not by
    // the storage engine. `VectorMatch::row_id` is a completely different,
    // internal identifier: the dataset's own monotonic row-id counter,
    // assigned by *commit order* (`row_id_base` in
    // `crates/txn/src/dataset.rs`'s `build_delta_entries`), which is
    // scrambled relative to the "id" column's values by chaos-worker's
    // whole point — randomized agent interleaving. Confirmed empirically
    // with a standalone probe: inserting id=999 then id=5 as two separate
    // commits returns `VectorMatch { row_id: 0, .. }` for the first and
    // `VectorMatch { row_id: 1, .. }` for the second — commit order, not
    // the id column. Comparing the two would assert two unrelated
    // namespaces are equal and fail spuriously on almost every seed with
    // more than one agent, not catch a real bug. The near-zero distance
    // check alone is what actually proves this row's vector reached the
    // graph — the exact standard the existing sibling test above uses.
    for &row_id in &visible_row_ids {
        let row_idx = (0..batch.num_rows())
            .find(|&i| u64::try_from(id_col.value(i)).unwrap() == row_id)
            .expect("visible row must be in the scanned batch (it was just derived from it)");
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
        let abort_at = master_rng.random_range(1..MAX_ABORT_THRESHOLD);
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-fast-{}-{seed}-{abort_at}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();

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
                usize::try_from(NUM_AGENTS * OPS_PER_AGENT).unwrap(),
                "worker exited successfully but didn't acknowledge every op"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }
}

/// The actual Phase 7 exit criterion: "thousands of randomized
/// concurrent-agent runs, zero invariant violations." Opt-in via
/// `STRATA_CHAOS_THOROUGH=1` — NOT part of default `cargo test --workspace`
/// (see the design doc §5 for why: each iteration's real process spawn +
/// real fsyncs make thousands of them too slow for the normal dev loop).
/// Intended for a scheduled/on-demand CI job.
#[test]
fn thorough_tier_satisfies_the_phase_7_exit_criterion() {
    const NUM_SEEDS: u64 = 2000;

    if std::env::var("STRATA_CHAOS_THOROUGH").is_err() {
        eprintln!("skipping thorough tier: set STRATA_CHAOS_THOROUGH=1 to run it");
        return;
    }

    let mut master_rng = rand_chacha::ChaCha8Rng::seed_from_u64(0x7040_0060_5EED);

    for seed in 0..NUM_SEEDS {
        let abort_at = master_rng.random_range(1..MAX_ABORT_THRESHOLD);
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-thorough-{}-{seed}-{abort_at}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();

        let result = run_worker(&dir, seed, Some(abort_at));

        std::thread::sleep(std::time::Duration::from_millis(50));

        check_invariants(&dir, &result.acknowledged_row_ids, result.crashed);

        std::fs::remove_dir_all(&dir).ok();

        if seed % 100 == 0 {
            eprintln!("thorough tier: {seed}/{NUM_SEEDS} seeds checked, zero violations so far");
        }
    }
}
