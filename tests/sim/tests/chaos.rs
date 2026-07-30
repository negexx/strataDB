//! Phase 7 correctness harness: spawns `chaos-worker` as a real child
//! process with a randomized crash checkpoint, then reopens the dataset
//! and checks five invariants (no corruption, no lost commits, no phantom
//! commits, no resurrected tombstones, row+index consistency). See
//! `docs/superpowers/specs/2026-07-22-phase-7-correctness-harness-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::process::Command;

use rand::Rng as _;
use rand::SeedableRng as _;

/// Bumped from 3 (Task 3.5 and earlier) per Task 6's real-concurrency
/// closure work: measured empirically (direct chaos-worker invocations,
/// `STRATA_CHAOS_ABORT_AT` unset) that 3 real agent threads at
/// `OPS_PER_AGENT = 5` never produced a genuine `TxnError::Conflict`/
/// dropped-op ack line in 50 seeds -- commits finish fast enough relative
/// to OS thread scheduling that 3 short-lived threads rarely have
/// overlapping commit windows. Full sweep (30 seeds/config unless noted;
/// `pool` defaults to `POOL_SIZE`'s real value, 6, unless stated):
/// 3 agents/5 ops -> 0/50, 3 agents/20 ops -> 1/30, 6 agents/5 ops -> 0/30,
/// 6 agents/8 ops -> 1/30, 6 agents/10 ops -> 2/30, 4 agents/8 ops -> 0/30,
/// 8 agents/5 ops -> 3/30 (~10%), 8 agents/8 ops -> 7/30 (~23%). Also
/// tested `POOL_SIZE = 3` (a smaller contested pool, concentrating target
/// collisions) at 3 agents/5 ops -> 0/30 and at 8 agents/5 ops -> 2/30 --
/// both are AT OR BELOW the matching `POOL_SIZE = 6` runs -- no measurable
/// improvement at the sizings tested (n=30/config, so 2-vs-3 is within
/// noise; not a claim shrinking the pool never helps at any scale).
/// Plausible reading: a conflict needs
/// overlapping COMMIT WINDOWS, not just overlapping op targets in general;
/// concentrating targets doesn't help if two threads' commits essentially
/// never overlap in wall-clock time to begin with). `NUM_AGENTS` is the
/// dominant lever, not `OPS_PER_AGENT` or `POOL_SIZE`: more concurrent
/// threads raises the chance several are mid-commit at the same instant far
/// more than lengthening any one thread's op sequence or narrowing its
/// target pool does. 8 agents/8 ops is the highest-yield configuration
/// tested (not the smallest -- 8/5, 6/10, and 3/20 are all cheaper by total
/// op-slot count and were tried first) -- kept over the cheaper 8/5 (~10%)
/// because the actual measured thorough-tier cost at 8/8 was acceptable
/// (2000/2000 seeds, zero violations, ~1175s/~20min at concurrency=8) and
/// the higher yield means more of the 2000-seed budget
/// actually exercises the conflict/drop path this whole task exists to
/// close.
const NUM_AGENTS: u64 = 8;
/// See `NUM_AGENTS`'s doc comment for the measurement this was tuned
/// alongside.
const OPS_PER_AGENT: u64 = 8;
/// Locally duplicated from `crates/chaos-worker/src/main.rs`'s private
/// `POOL_SIZE` (design doc §3.1) -- that constant is not reachable from
/// here, and this file needs its exact value for the clean-run slot-count
/// assertion below and for this threshold's own upper-bound estimate. Left
/// at 6 -- see `NUM_AGENTS`'s doc comment for why shrinking it doesn't
/// meaningfully raise the conflict rate here.
const POOL_SIZE: u64 = 6;
/// Comfortably above the total number of checkpoints one full run
/// produces -- an ESTIMATE with real margin built in, not an exact count
/// (the true total is workload-mix-dependent, since Insert/Update/Delete/
/// `MultiBatchInsert` each touch a different number of checkpoint sites,
/// AND -- now that real conflicts are reachable, see `NUM_AGENTS`'s doc
/// comment -- retry-dependent too, see below). A vector-carrying
/// single-row commit (a plain Insert, or Update's insert half) passes
/// through six on a clean attempt: `write_batch`'s data-file fsync,
/// `write_bytes`'s segment fsync (added by S1 W3.2a), `sync_dir`'s
/// data-dir fsync, `commit_manifest`'s tmp-sync, its rename, and
/// `sync_dir`'s versions-dir fsync -- confirmed against the actual call
/// sites in `crates/storage/src/{datafile,manifest}.rs`. A `MultiBatchInsert`
/// commit calls `write_batch` once per pending row (2, not 1) but still
/// only one `write_bytes`/`commit_manifest` (one segment, one manifest
/// entry, built from both batches together) -- 7 checkpoints for that one
/// op, not 6. A delete-only commit writes no new data file or segment at
/// all on either a clean OR a conflicting attempt (`write_phase` returns
/// early with no pending batches), so it costs 3 (just the manifest-side
/// checkpoints) or 0 if the attempt conflicts before even reaching those.
///
/// The RETRY path (new since real concurrency landed) can make a single
/// 1-slot Update MORE expensive than a clean Insert: `Transaction::commit`
/// runs `write_phase` (the first three checkpoints above) BEFORE the
/// conflict check, so a conflicting first attempt still burns those three
/// checkpoints before `commit_with_retry_once`
/// (`crates/chaos-worker/src/commit_ops.rs`) retries with a fresh
/// transaction. A conflict-then-succeed Update therefore costs 3 + 6 = 9
/// checkpoints for one op-slot, not 6 -- the true analytic worst case is
/// `OPS_PER_AGENT * 9 * NUM_AGENTS` = 8 * 9 * 8 = 576, not the 384 a
/// clean-attempt-only accounting gives. Plus `setup_contested_pool`'s
/// `POOL_SIZE` unconditional single-row inserts (`POOL_SIZE * 6` = 36) and
/// `Dataset::create`'s own initial `commit_manifest` call (3): worst-case
/// analytic total 576 + 36 + 3 = 615. Reaching that in one real run would
/// need essentially every agent's every op to conflict-then-succeed, which
/// the measured ~10-23% any-drop-at-all rate above makes very unlikely --
/// empirically binary-searching the true checkpoint total on 8 real seeds
/// (0-7) found every one in (350, 400]. Set `MAX_ABORT_THRESHOLD` to 450
/// (~12% above the highest observed real total, not the 615 analytic
/// worst case) as a deliberate, informed bet on the empirical distribution
/// over the theoretical maximum -- the failure mode if a rare run's true
/// total DID exceed 450 is benign, not a false failure: `abort_at` would
/// simply always land inside that run (guaranteeing `crashed = true`),
/// which only means that seed's clean-run-only assertions (the fast
/// tier's exact slot-count/pool-ack checks, and the two `!crashed`
/// zero-budget asserts in `check_invariants`) don't get exercised, not
/// that any invariant check produces a wrong answer. A higher threshold than needed isn't free
/// either way: `abort_at` is drawn uniformly from `1..MAX_ABORT_THRESHOLD`,
/// so a threshold much above the true total wastes iterations on clean
/// (non-crashing) runs instead of exercising a crash -- the thorough tier
/// is the actual Phase 7 exit criterion, and every non-crashing iteration
/// there is one fewer crash-path check out of its fixed seed budget.
///
/// `STRATA_CHAOS_ABORT_AT` counts checkpoints since **process start** off
/// one process-global counter, so this constant must be re-checked
/// whenever a checkpoint site is added or removed anywhere in
/// `strata-storage`, or whenever `NUM_AGENTS`/`OPS_PER_AGENT` change.
const MAX_ABORT_THRESHOLD: u64 = 450;

/// The (lost, phantom) tolerance a single op of this shape contributes if
/// it was genuinely in flight — mid-commit, ack line not yet printed —
/// when a chaos abort fired. Mirrors `crates/chaos-worker`'s own
/// `resolve_slot_consumption`/`commit_ops.rs` op semantics: a pure insert
/// (or a Delete/Update downgraded to one) can only ever produce a phantom
/// (the row may have committed durably with no ack); a delete can only
/// ever produce a lost commit (the tombstone may have committed durably
/// with no ack, hiding a row `acknowledged_inserts` still expects to see);
/// an update does both (it is a delete-of-old plus insert-of-new); a
/// multi-batch insert can phantom BOTH of its rows behind the one ack line
/// that never printed.
fn ambiguity_shape(verb: &str) -> (usize, usize) {
    match verb {
        "insert" => (0, 1),
        "delete" => (1, 0),
        "update" => (1, 1),
        "multibatch" => (0, 2),
        other => panic!("unrecognized verb in a 'starting' ack line: {other:?}"),
    }
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
    /// Sum of `ambiguity_shape(verb).0` over every op that printed a
    /// "starting" line but never printed its matching completion line in
    /// this specific run -- computed from the ACTUAL observed in-flight
    /// set, not a flat per-run constant. Zero on a run that exited
    /// cleanly (every started op's thread runs its own commit loop to
    /// completion before the process exits 0, so an unmatched start is
    /// only possible when the process was aborted mid-commit).
    max_tolerated_lost: usize,
    /// Sum of `ambiguity_shape(verb).1` over the same in-flight set.
    max_tolerated_phantoms: usize,
}

#[allow(clippy::too_many_lines)]
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
    let mut pool_starts: u64 = 0;
    // Keyed by (agent, op) -- an agent's own ops run sequentially within
    // one thread, so at most one entry per agent can ever be present at
    // the end of parsing (the one truly in flight when the process
    // aborted, if it aborted mid-op).
    let mut started_agent_ops: HashMap<(u64, u64), String> = HashMap::new();
    for line in stdout.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        match words.as_slice() {
            ["pool", "starting", "insert", "row", _] => {
                pool_starts += 1;
            }
            ["pool", "committed", "insert", "row_id", row_id] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
                pool_acks += 1;
            }
            ["agent", agent, "starting", verb, "op", op] => {
                started_agent_ops.insert(
                    (agent.parse().unwrap(), op.parse().unwrap()),
                    (*verb).to_string(),
                );
            }
            [
                "agent",
                agent,
                "committed",
                "insert",
                "op",
                op,
                "row_id",
                row_id,
            ] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
                total_op_slots += 1;
                started_agent_ops.remove(&(agent.parse().unwrap(), op.parse().unwrap()));
            }
            [
                "agent",
                agent,
                "committed",
                "delete",
                "op",
                op,
                "target_row_id",
                target_row_id,
            ] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
                total_op_slots += 1;
                started_agent_ops.remove(&(agent.parse().unwrap(), op.parse().unwrap()));
            }
            [
                "agent",
                agent,
                "committed",
                "update",
                "op",
                op,
                "target_row_id",
                target_row_id,
                "row_id",
                row_id,
            ] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
                acknowledged_inserts.insert(row_id.parse().unwrap());
                total_op_slots += 1;
                started_agent_ops.remove(&(agent.parse().unwrap(), op.parse().unwrap()));
            }
            [
                "agent",
                agent,
                "committed",
                "multibatch",
                "op",
                op,
                "row_ids",
                row_ids,
            ] => {
                for id in row_ids.split(',') {
                    acknowledged_inserts.insert(id.parse().unwrap());
                }
                total_op_slots += 2;
                started_agent_ops.remove(&(agent.parse().unwrap(), op.parse().unwrap()));
            }
            ["agent", agent, "dropped", "op", op, "(conflict)"] => {
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
                started_agent_ops.remove(&(agent.parse().unwrap(), op.parse().unwrap()));
            }
            _ => panic!("unrecognized chaos-worker stdout line: {line:?}"),
        }
    }

    // Precise crash-tolerance budget: sum ambiguity_shape over exactly the
    // ops that started but never completed in THIS run, not a flat
    // per-run constant. At most one pool insert can be in flight (pool
    // setup is strictly sequential, single-threaded, and runs to
    // completion before any agent thread spawns) and at most one op per
    // agent (each agent's own ops run sequentially within its thread) --
    // see design doc Part 1 and this task's own rationale.
    let mut max_tolerated_lost = 0usize;
    let mut max_tolerated_phantoms = 0usize;
    if pool_starts > pool_acks {
        debug_assert_eq!(
            pool_starts - pool_acks,
            1,
            "pool setup is strictly sequential; at most one pool insert can ever be in flight"
        );
        let (lost, phantom) = ambiguity_shape("insert");
        max_tolerated_lost += lost;
        max_tolerated_phantoms += phantom;
    }
    for verb in started_agent_ops.values() {
        let (lost, phantom) = ambiguity_shape(verb);
        max_tolerated_lost += lost;
        max_tolerated_phantoms += phantom;
    }

    RunResult {
        acknowledged_inserts,
        acknowledged_tombstones,
        total_op_slots,
        pool_acks,
        crashed: !output.status.success(),
        max_tolerated_lost,
        max_tolerated_phantoms,
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

    // Invariants 2+3 (no lost commits, no phantom commits): crash-tolerance
    // budgets are computed above from Task 3.5's precise in-flight-op
    // tracking (see RunResult's own doc comments and this file's
    // ambiguity_shape) -- NOT a flat per-run constant. A row also present
    // in acknowledged_tombstones is correctly excluded from "lost" (that's
    // Delete/Update doing its job, not ambiguity).
    let lost: Vec<&u64> = acknowledged
        .difference(&visible_row_ids)
        .filter(|id| !result.acknowledged_tombstones.contains(id))
        .collect();
    let max_tolerated_lost = result.max_tolerated_lost;
    if !crashed {
        assert_eq!(
            max_tolerated_lost, 0,
            "a clean (non-crashed) run must never have an op that started but never completed -- \
             a nonzero budget here means the 'starting'/'committed'/'dropped' ack-line protocol \
             itself is broken, not a legitimate ambiguity"
        );
    }
    assert!(
        lost.len() <= max_tolerated_lost,
        "lost commits: acknowledged but not visible after reopen: {lost:?} \
         (tolerated at most {max_tolerated_lost}, computed from the ops genuinely in flight \
         at abort time in this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    let phantom: Vec<&u64> = visible_row_ids.difference(acknowledged).collect();
    let max_tolerated_phantoms = result.max_tolerated_phantoms;
    if !crashed {
        assert_eq!(
            max_tolerated_phantoms, 0,
            "a clean (non-crashed) run must never have an op that started but never completed"
        );
    }
    assert!(
        phantom.len() <= max_tolerated_phantoms,
        "phantom commits: visible after reopen but never acknowledged: {phantom:?} \
         (tolerated at most {max_tolerated_phantoms}, computed from the ops genuinely in flight \
         at abort time in this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    // The joint bound is exactly the sum of the two individual budgets --
    // both are sums of ambiguity_shape(verb) over the SAME set of
    // genuinely-in-flight ops, so no separate enumeration/reasoning is
    // needed here (unlike the old flat-constant version, which had to
    // reason about the worst case across UNKNOWN verbs; this version
    // knows each in-flight op's actual verb from its "starting" line).
    //
    // WHY THE SUM, NOT A MAX: `commit_lock` (crates/txn/src/dataset.rs)
    // does NOT bound this to one ambiguous op. `write_phase` runs OUTSIDE
    // commit_lock, and each op's ack line prints AFTER commit() returns --
    // i.e. after commit_lock is already released -- so arbitrarily many
    // agent threads can each be sitting in their own post-commit,
    // pre-ack-line window at the same instant when a chaos abort fires.
    // Every op with a "starting" line and no matching completion line by
    // the end of the run is independently ambiguous; this is why the
    // budget is a genuine sum over the whole in-flight set (up to
    // NUM_AGENTS entries), not a single-op cap the way it correctly was
    // when the scheduler was sequential.
    //
    // KNOWN RESIDUAL IMPRECISION (false-negative only, never a false
    // failure): if 2+ in-flight ops are Delete/Update targeting the SAME
    // pool row, this sums a `lost` budget of 2+ for that row even though
    // only 1 row can actually go missing -- a real regression there could
    // hide behind the extra slack. The collision probability scales with
    // NUM_AGENTS/POOL_SIZE, so it's non-trivially higher now (8/6) than
    // when this was first measured Minor (3/6). Inherent to the
    // per-verb-shape model (the alternative -- printing each delete/
    // update's target row-id in its "starting" line and tolerating an
    // exact id-set instead of a count -- is a real fix, just not done
    // here) and still strictly narrower than the flat constant it
    // replaced.
    let max_tolerated_combined = max_tolerated_lost + max_tolerated_phantoms;
    assert!(
        lost.len() + phantom.len() <= max_tolerated_combined,
        "combined lost+phantom count {} exceeds the budget computed from the ops genuinely in \
         flight at abort time in this {} run (tolerated at most {max_tolerated_combined}) -- \
         lost: {lost:?}, phantom: {phantom:?}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ambiguity_shape_matches_the_documented_per_verb_table() {
        assert_eq!(ambiguity_shape("insert"), (0, 1));
        assert_eq!(ambiguity_shape("delete"), (1, 0));
        assert_eq!(ambiguity_shape("update"), (1, 1));
        assert_eq!(ambiguity_shape("multibatch"), (0, 2));
    }

    #[test]
    #[should_panic(expected = "unrecognized verb")]
    fn ambiguity_shape_panics_on_an_unknown_verb() {
        ambiguity_shape("bogus");
    }
}
