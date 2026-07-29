//! Phase 7 correctness harness: spawns `chaos-worker` as a real child
//! process with a randomized crash checkpoint, then reopens the dataset
//! and checks five invariants (no corruption, no lost commits, no phantom
//! commits, no resurrected tombstones, row+index consistency). See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::process::Command;

use rand::Rng as _;
use rand::SeedableRng as _;

const NUM_AGENTS: u64 = 3;
const OPS_PER_AGENT: u64 = 5;
/// Locally duplicated from `crates/chaos-worker/src/main.rs`'s private
/// `POOL_SIZE` (design doc §3.1) -- that constant is not reachable from
/// here, and this file needs its exact value for the clean-run slot-count
/// assertion below and for this threshold's own upper-bound estimate.
const POOL_SIZE: u64 = 6;
/// Comfortably above the total number of checkpoints one full run
/// produces -- an ESTIMATE with real margin built in, not an exact count
/// (the true total is workload-mix-dependent, since Insert/Update/Delete/
/// `MultiBatchInsert` each touch a different number of checkpoint sites).
/// A vector-carrying single-row commit (a plain Insert, or Update's insert
/// half) passes through six: `write_batch`'s data-file fsync,
/// `write_bytes`'s segment fsync (added by S1 W3.2a), `sync_dir`'s
/// data-dir fsync, `commit_manifest`'s tmp-sync, its rename, and
/// `sync_dir`'s versions-dir fsync -- confirmed against the actual call
/// sites in `crates/storage/src/{datafile,manifest}.rs`. A `MultiBatchInsert`
/// commit calls `write_batch` once per pending row (2, not 1) but still
/// only one `write_bytes`/`commit_manifest` (one segment, one manifest
/// entry, built from both batches together) -- 7 checkpoints for that one
/// op, not 6. A delete-only commit writes no new data file or segment at
/// all, so it has fewer (this workload has had deletes since Task 6,
/// unlike when this comment previously claimed otherwise). Per-AGENT
/// worst case is still all-Insert (Insert's 6/slot is the single highest
/// per-slot cost -- `MultiBatchInsert`'s 7 checkpoints buys 2 slots, or
/// 3.5/slot): `OPS_PER_AGENT` * 6 * `NUM_AGENTS` = 90. `setup_contested_pool`
/// (Task 6) adds `POOL_SIZE` more single-row inserts, UNCONDITIONALLY, on
/// every run, before any agent op: `POOL_SIZE` * 6 = 36 more. Plus
/// `Dataset::create`'s own initial `commit_manifest` call (3 more, for a
/// freshly-created dataset). Conservative total estimate: 90 + 36 + 3 =
/// 129. Empirically confirmed by binary-searching the true checkpoint
/// total on 12 real seeds: 95-119, comfortably under this estimate. A
/// higher threshold than needed isn't free: `abort_at` is drawn uniformly
/// from `1..MAX_ABORT_THRESHOLD`, so a threshold much above the true
/// total wastes iterations on clean (non-crashing) runs instead of
/// exercising a crash -- Task 8's thorough tier is the actual Phase 7
/// exit criterion, and every non-crashing iteration there is one fewer
/// crash-path check out of its fixed seed budget. Kept at 200, not
/// pushed up to a larger "safe" round number, for exactly that reason.
///
/// `STRATA_CHAOS_ABORT_AT` counts checkpoints since **process start** off
/// one process-global counter, so this constant must be re-checked
/// whenever a checkpoint site is added or removed anywhere in
/// `strata-storage`.
const MAX_ABORT_THRESHOLD: u64 = 200;

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

struct RunResult {
    acknowledged_inserts: HashSet<u64>,
    acknowledged_tombstones: HashSet<u64>,
    /// Sum of op-SLOTS acknowledged (pool lines excluded, tracked
    /// separately by `pool_acks` below): insert/delete/update/dropped
    /// each consume 1 slot, multibatch consumes 2 -- matches
    /// `resolve_slot_consumption`'s own accounting in
    /// `crates/chaos-worker/src/ops.rs`. Lets a clean run assert an EXACT
    /// `NUM_AGENTS * OPS_PER_AGENT` total, restoring the coverage the
    /// original insert-only exact-count check had, without assuming every
    /// slot grows `acknowledged_inserts` by exactly one the way a pure
    /// Insert workload did.
    total_op_slots: u64,
    /// Count of `pool committed insert` lines. Always exactly `POOL_SIZE`
    /// on a clean run (`setup_contested_pool` is unconditional and
    /// single-threaded, so either all `POOL_SIZE` lines print before the
    /// scheduler loop starts, or the run crashed before finishing setup).
    pool_acks: u64,
    crashed: bool,
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

    // Exit code 2 is the reserved genuine-failure signal (design doc
    // §3.4) -- a real bug, never an expected chaos-abort. Fail the test
    // immediately with the worker's own printed message, before even
    // attempting to reopen the dataset.
    if output.status.code() == Some(2) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let message = stdout
            .lines()
            .find(|l| l.starts_with("GENUINE_FAILURE:"))
            .unwrap_or("GENUINE_FAILURE: <no message line found in stdout>");
        panic!(
            "chaos-worker reported a genuine failure at seed={seed} abort_at={abort_at:?}: {message}"
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut acknowledged_inserts = HashSet::new();
    let mut acknowledged_tombstones = HashSet::new();
    let mut total_op_slots: u64 = 0;
    let mut pool_acks: u64 = 0;
    for line in stdout.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        match words.as_slice() {
            ["pool", "committed", "insert", "row_id", row_id] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
                pool_acks += 1;
            }
            [.., "committed", "insert", "op", _, "row_id", row_id] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
                total_op_slots += 1;
            }
            [
                ..,
                "committed",
                "delete",
                "op",
                _,
                "target_row_id",
                target_row_id,
            ] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
                total_op_slots += 1;
            }
            [
                ..,
                "committed",
                "update",
                "op",
                _,
                "target_row_id",
                target_row_id,
                "row_id",
                row_id,
            ] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
                acknowledged_inserts.insert(row_id.parse().unwrap());
                total_op_slots += 1;
            }
            [.., "committed", "multibatch", "op", _, "row_ids", row_ids] => {
                for id in row_ids.split(',') {
                    acknowledged_inserts.insert(id.parse().unwrap());
                }
                total_op_slots += 2;
            }
            [.., "dropped", "op", _, "(conflict)"] => {
                // The dropped line carries no verb, so this hardcodes 1
                // slot -- correct only because a droppable op always costs
                // exactly 1 slot today: execute_delete/execute_update are
                // the only ops that can return Dropped (commit_ops.rs's
                // commit_with_retry_once), and both are 1-slot ops.
                // execute_insert/execute_multi_batch_insert panic on any
                // commit error instead of returning Dropped, so a 2-slot
                // MultiBatchInsert can never reach this arm. If that ever
                // changes, this hardcoded 1 would silently desync
                // total_op_slots from the real slot count on a clean run,
                // and the exact-count assertion below would start failing
                // with a misleading "didn't account for every op slot"
                // message instead of pointing at this arm.
                total_op_slots += 1;
            }
            _ => panic!("unrecognized chaos-worker stdout line: {line:?}"),
        }
    }

    RunResult {
        acknowledged_inserts,
        acknowledged_tombstones,
        total_op_slots,
        pool_acks,
        crashed: !output.status.success(),
    }
}

/// Builds the same schema shape as `crates/chaos-worker/src/schema.rs`'s
/// `schema_with_row_id` (that function is `pub(crate)` to chaos-worker,
/// not reachable from here, so this is a small intentional duplicate) --
/// `mvp_schema()`'s fields plus the hidden internal row-id column,
/// appended last.
fn schema_with_row_id() -> arrow::datatypes::SchemaRef {
    let mvp = strata_txn::mvp_fixtures::mvp_schema();
    let mut fields: Vec<arrow::datatypes::Field> =
        mvp.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(arrow::datatypes::Field::new(
        strata_txn::ROW_ID_COLUMN,
        arrow::datatypes::DataType::UInt64,
        false,
    ));
    std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
}

fn check_invariants(dir: &std::path::Path, result: &RunResult) {
    let acknowledged = &result.acknowledged_inserts;
    let crashed = result.crashed;

    // Invariant 1: no corruption. A crash mid-write must never leave an
    // EXISTING dataset unable to open. One narrow, precisely-scoped
    // exception: if the crash landed before the dataset's very first
    // commit_manifest call ever completed its rename (e.g. abort_at=1,
    // landing inside Dataset::create's own initial-manifest write), no
    // manifest file -- not even the initial empty one -- was ever durably
    // created, so NotFound is the correct, expected response to "nothing
    // exists yet," not corruption. This can only be legitimate when
    // nothing was ever acknowledged either (a non-empty acknowledged set
    // would mean at least one commit fully landed, which requires a
    // manifest to already exist) -- any other open failure, or a NotFound
    // with a non-empty acknowledged set, is genuine corruption and must
    // still fail loudly. All invariants below are trivially satisfied when
    // nothing was ever created, so return early rather than trying to scan
    // a dataset that doesn't exist.
    let dataset = match strata_txn::Dataset::open(dir) {
        Ok(ds) => ds,
        Err(strata_txn::TxnError::NotFound(_)) if acknowledged.is_empty() => return,
        Err(e) => panic!("dataset failed to reopen after crash — corruption: {e}"),
    };

    // Reads the internal system row-id, NOT the business `id` column.
    // print_outcome's ack lines (Task 3) print the internal row-id
    // returned by lookup_row_id -- e.g. an insert's ack line is
    // `row_id 6`, not the business id used to construct that row. The
    // business `id` column is also unusable directly here regardless:
    // setup_contested_pool (Task 6) commits pool rows with NEGATIVE
    // business ids, so casting that column to u64 (as an earlier version
    // of this function did) panics with a TryFromIntError on every run
    // that includes the pool -- i.e. every run, since pool setup is
    // unconditional.
    let schema = schema_with_row_id();
    let batch = dataset
        .snapshot()
        .scan(&schema)
        .expect("scan failed after reopen");
    let row_id_col = batch
        .column(
            batch
                .schema_ref()
                .index_of(strata_txn::ROW_ID_COLUMN)
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let visible_row_ids: HashSet<u64> =
        (0..batch.num_rows()).map(|i| row_id_col.value(i)).collect();

    // Invariant 2: no lost commits. Two deliberate deviations from this
    // task's brief, whose given invariant-2 code predates Task 6's
    // Delete/Update workload and doesn't account for it -- both found by
    // running the fast tier and inspecting the actual failures below, not
    // guessed:
    //
    // 1. A row that was legitimately tombstoned later (its row-id also
    //    appears in `acknowledged_tombstones`, from a subsequent Delete or
    //    Update targeting it) is correctly absent from `visible_row_ids`
    //    -- that's not a lost commit, it's Delete/Update doing its job.
    //    Verified empirically at seed=0, abort_at=166 (a clean/non-crashed
    //    run): every id the un-adjusted check reported as "lost" was also
    //    present in `acknowledged_tombstones`.
    // 2. The same single-in-flight-op ambiguous-outcome case invariant 3
    //    already tolerates for phantoms applies symmetrically here for a
    //    CRASHED run: the one op that may have durably committed without
    //    printing its ack line could be a Delete/Update whose *target*
    //    row was acknowledged as inserted earlier -- that target now
    //    reads as "acknowledged-insert, not tombstoned, not visible" (a
    //    lost commit by the naive check) even though it was legitimately
    //    superseded, purely because the one ack line that would have
    //    recorded the tombstone never made it to stdout. Verified
    //    empirically at seed=1, abort_at=115 (crashed=true): the single
    //    reported "lost" row-id had no corresponding tombstone ack, with
    //    every other row-id accounted for.
    let lost: Vec<&u64> = acknowledged
        .difference(&visible_row_ids)
        .filter(|id| !result.acknowledged_tombstones.contains(id))
        .collect();
    let max_tolerated_lost = usize::from(crashed);
    assert!(
        lost.len() <= max_tolerated_lost,
        "lost commits: acknowledged but not visible after reopen: {lost:?} \
         (tolerated at most {max_tolerated_lost} for this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    // Invariant 3: no phantom commits (up to two tolerated for a crashed
    // run — the single-in-flight-op ambiguous-outcome case, widened from
    // the brief's given "at most one" for the same Task-6-shaped reason as
    // invariant 2 above: MultiBatchInsert bundles 2 row-level commits
    // behind ONE op-slot and ONE ack line, so the single ambiguous op at
    // abort time can be a MultiBatchInsert whose commit durably landed
    // both rows but whose one ack line never made it to stdout, producing
    // two phantom row-ids at once, not one. Verified empirically: a
    // crashed run surfaced exactly two consecutive phantom row-ids with no
    // other invariant violated once this widening was applied.
    let phantom: Vec<&u64> = visible_row_ids.difference(acknowledged).collect();
    let max_tolerated_phantoms = if crashed { 2 } else { 0 };
    assert!(
        phantom.len() <= max_tolerated_phantoms,
        "phantom commits: visible after reopen but never acknowledged: {phantom:?} \
         (tolerated at most {max_tolerated_phantoms} for this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    // Joint bound on invariants 2+3 together: the individual per-invariant
    // tolerances above (lost <= 1, phantom <= 2) are each individually
    // tight, but only ONE op is ever ambiguous at abort time (the scheduler
    // is single-threaded and every chaos_checkpoint site lives inside the
    // one commit() in flight), so the two bounds can never BOTH max out
    // from the same run. Enumerating every op's ambiguous-outcome shape as
    // (lost, phantom): Insert->(0,1), MultiBatchInsert->(0,2),
    // Delete->(1,0), Update->(1,1) -- the worst combined total across all
    // of them is 2, not 1+2=3. Without this check, a genuine lost commit
    // occurring alongside an unrelated genuine 2-row phantom would pass
    // silently, since each individually stays within its own bound.
    let max_tolerated_combined = if crashed { 2 } else { 0 };
    assert!(
        lost.len() + phantom.len() <= max_tolerated_combined,
        "combined lost+phantom count {} exceeds what any single ambiguous op can produce \
         (tolerated at most {max_tolerated_combined} for this {} run) -- lost: {lost:?}, \
         phantom: {phantom:?}",
        lost.len() + phantom.len(),
        if crashed { "crashed" } else { "clean" }
    );

    // New invariant: no resurrected tombstones. Unlike an ambiguous
    // insert outcome, there is no legitimate scenario where a durably
    // tombstoned row should still be visible -- Snapshot::is_visible is a
    // pure `!tombstones.contains` check with no timing window, so this
    // gets NO crash-tolerance carve-out.
    let resurrected: Vec<&u64> = result
        .acknowledged_tombstones
        .intersection(&visible_row_ids)
        .collect();
    assert!(
        resurrected.is_empty(),
        "resurrected tombstones: acknowledged as deleted but visible after reopen: {resurrected:?}"
    );

    // Invariant 4: row + index consistency (still iterates every
    // currently-visible row; row_idx lookup and the vector column index
    // both updated for the row-id-column schema above).
    let vector_col_idx = batch.schema_ref().index_of("vector").unwrap();
    for &row_id in &visible_row_ids {
        let row_idx = (0..batch.num_rows())
            .find(|&i| row_id_col.value(i) == row_id)
            .expect("visible row must be in the scanned batch (it was just derived from it)");
        let vector_col = batch
            .column(vector_col_idx)
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

        check_invariants(&dir, &result);

        if !result.crashed {
            // The randomly-picked threshold happened to exceed the total
            // checkpoint count for this seed — the run completed cleanly.
            // Still a valid, still-checked iteration; not a bug.
            //
            // Unlike the insert-only workload, a clean run's total
            // acknowledged ROW-ID count is no longer a fixed function of
            // NUM_AGENTS * OPS_PER_AGENT alone (MultiBatchInsert grows
            // acknowledged_inserts by 2 per slot-pair, Delete/Update don't
            // grow it at all, and a dropped conflict acknowledges nothing).
            // But op-SLOT count IS still exact regardless of verb mix --
            // every slot prints exactly one ack line, and total_op_slots
            // counts insert/delete/update/dropped as 1 and multibatch as 2,
            // mirroring resolve_slot_consumption's own accounting -- so
            // this restores the precision the original insert-only exact
            // count had (a merely-nonempty check would pass even if every
            // single agent op silently vanished, since setup_contested_pool
            // unconditionally acks POOL_SIZE rows before any agent op runs).
            assert_eq!(
                result.total_op_slots,
                NUM_AGENTS * OPS_PER_AGENT,
                "worker exited successfully but didn't account for every op slot"
            );
            assert_eq!(
                result.pool_acks, POOL_SIZE,
                "worker exited successfully but didn't acknowledge every pool row"
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

    // Bounded concurrency: run seeds in fixed-size batches of scoped
    // threads. Deliberately NOT unbounded rayon-across-all-cores — dozens
    // of concurrent aborting child processes all fsyncing the same volume
    // is exactly the environment the 50ms handle-release sleep below
    // exists to protect against, and a spurious Dataset::open failure here
    // would misreport as corruption. Default 8, validated clean (zero
    // spurious failures across 8,200+ concurrent seed-checks at
    // concurrency 4/8/16 on a 12-logical-core machine, with 16 the
    // fastest observed on that hardware) -- 8 is the safer portable
    // default since 16's edge was only confirmed on that specific core
    // count; override via STRATA_CHAOS_CONCURRENCY (1 = the original
    // sequential behavior) for CI hardware with more cores, or to
    // reproduce a specific failure in isolation.
    let concurrency: usize = std::env::var("STRATA_CHAOS_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(8);
    // Debugging knobs: cap the seed count (partial runs) or run exactly
    // one seed in isolation (spurious-failure reproduction — see the
    // panic message below).
    let num_seeds: u64 = std::env::var("STRATA_CHAOS_NUM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(NUM_SEEDS);
    let only_seed: Option<u64> = std::env::var("STRATA_CHAOS_ONLY_SEED")
        .ok()
        .and_then(|s| s.parse().ok());

    let mut master_rng = rand_chacha::ChaCha8Rng::seed_from_u64(0x7040_0060_5EED);

    // Pre-draw every (seed, abort_at) pair from the master RNG up front so
    // the schedule is byte-identical to the sequential version regardless
    // of concurrency level or chunking.
    let pairs: Vec<(u64, u64)> = (0..NUM_SEEDS)
        .map(|seed| (seed, master_rng.random_range(1..MAX_ABORT_THRESHOLD)))
        .filter(|&(seed, _)| seed < num_seeds && only_seed.is_none_or(|s| s == seed))
        .collect();

    // Force the one-time escargot build before any threads spawn, so the
    // first batch's threads don't all block inside OnceLock::get_or_init
    // and the reported timings measure seed throughput, not compilation.
    let _ = worker_bin_path();

    let mut checked: u64 = 0;
    for chunk in pairs.chunks(concurrency) {
        std::thread::scope(|s| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|&(seed, abort_at)| {
                    s.spawn(move || {
                        let dir = std::env::temp_dir().join(format!(
                            "strata-chaos-thorough-{}-{seed}-{abort_at}",
                            std::process::id()
                        ));
                        std::fs::remove_dir_all(&dir).ok();

                        let result = run_worker(&dir, seed, Some(abort_at));

                        // Give the OS a moment to fully release file
                        // handles after an abort — same precaution as the
                        // fast tier and the Phase 1 crash-recovery test.
                        std::thread::sleep(std::time::Duration::from_millis(50));

                        check_invariants(&dir, &result);

                        std::fs::remove_dir_all(&dir).ok();
                    })
                })
                .collect();
            for (handle, &(seed, abort_at)) in handles.into_iter().zip(chunk) {
                assert!(
                    handle.join().is_ok(),
                    "invariant violation at seed={seed} abort_at={abort_at} \
                     (panic message above has the details); reproduce alone with \
                     STRATA_CHAOS_ONLY_SEED={seed} STRATA_CHAOS_CONCURRENCY=1"
                );
            }
        });
        checked += u64::try_from(chunk.len()).unwrap();
        if checked.is_multiple_of(100) || checked == u64::try_from(pairs.len()).unwrap() {
            eprintln!(
                "thorough tier: {checked}/{} seeds checked, zero violations so far \
                 (concurrency={concurrency})",
                pairs.len()
            );
        }
    }
}
