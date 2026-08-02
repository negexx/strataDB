//! Commit execution, conflict retry, and the contested-row-id registry
//! for the chaos workload. See
//! `docs/phase-1-audit.md` for the current verification boundary.

use std::io::Write as _;

use arrow::array::UInt64Array;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::{Dataset, ROW_ID_COLUMN, TxnError};

use crate::schema::schema_with_row_id;

/// Looks up the internal system row-id assigned to the row whose business
/// `id` column equals `business_id`. Called immediately after a
/// successful insert-type commit — `Transaction::commit` returns only
/// `Result<()>`, never the row-id(s) it assigned, and `Transaction::delete`/
/// `update` need the internal row-id, not the business `id` column value.
pub(crate) fn lookup_row_id(dataset: &Dataset, business_id: i64) -> u64 {
    let predicate = Predicate::Eq("id".to_string(), Value::Int64(business_id));
    let batch = dataset
        .snapshot()
        .scan_with_predicate(&schema_with_row_id(), &predicate)
        .expect("scan_with_predicate must succeed for a row this worker just committed");
    assert_eq!(
        batch.num_rows(),
        1,
        "business id {business_id} must resolve to exactly one row right after its own insert"
    );
    let row_id_col = batch
        .column(batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    row_id_col.value(0)
}

/// Tracks which row-ids are eligible Delete/Update targets: the shared
/// contested pool, and each agent's own live (not-yet-tombstoned) rows.
/// Updated only after a commit actually durably succeeds — never
/// speculatively.
///
/// Every method taking an `agent` index requires `agent < num_agents` (the
/// value passed to [`Registry::new`]) — callers are all in-crate and
/// already have a valid agent index in hand from the scheduler loop, so
/// this is an internal invariant, not a checked precondition.
pub(crate) struct Registry {
    pool_live: Vec<u64>,
    own_live: Vec<Vec<u64>>,
}

impl Registry {
    pub(crate) fn new(num_agents: usize) -> Self {
        Self {
            pool_live: Vec::new(),
            own_live: vec![Vec::new(); num_agents],
        }
    }

    pub(crate) fn record_pool_row(&mut self, row_id: u64) {
        self.pool_live.push(row_id);
    }

    pub(crate) fn record_own_row(&mut self, agent: usize, row_id: u64) {
        self.own_live[agent].push(row_id);
    }

    /// Removes `row_id` from wherever it currently lives (the pool, or
    /// some agent's own-row list) after it's been tombstoned. A target is
    /// always either a pool row or the ACTING agent's own row (never
    /// another agent's), so checking the pool then this agent's own list
    /// is exhaustive. Since real concurrent agent threads landed, `row_id`
    /// may already be gone by the time this runs -- e.g. two agents both
    /// resolved the SAME pool row as their target before either committed,
    /// and the other one's delete/update already removed it here first.
    /// This is silently correct in that case: `position` returns `None`
    /// in both branches below, so the `if let`/`else if let` is a
    /// harmless no-op rather than a panic or an incorrect removal from
    /// the wrong list -- the row genuinely is gone, just not because of
    /// THIS call.
    pub(crate) fn remove(&mut self, agent: usize, row_id: u64) {
        if let Some(pos) = self.pool_live.iter().position(|&r| r == row_id) {
            self.pool_live.swap_remove(pos);
        } else if let Some(pos) = self.own_live[agent].iter().position(|&r| r == row_id) {
            self.own_live[agent].swap_remove(pos);
        }
    }

    pub(crate) fn pool_rows(&self) -> &[u64] {
        &self.pool_live
    }

    pub(crate) fn own_rows(&self, agent: usize) -> &[u64] {
        &self.own_live[agent]
    }
}

/// The result of attempting one op. `Dropped` means a conflict occurred
/// twice in a row (the original attempt and one retry) — see the module
/// doc comment and design doc §3.2.
#[derive(Debug)]
pub(crate) enum ExecOutcome {
    CommittedInsert {
        // Retained for the `Debug` impl and test assertions (e.g.
        // commit_ops::tests' `matches!` patterns) -- `main`'s scheduler
        // loop and `print_outcome` only ever need `row_id`, so this field
        // is otherwise unread in a non-test build.
        #[allow(dead_code)]
        business_id: i64,
        row_id: u64,
    },
    CommittedDelete {
        target_row_id: u64,
    },
    CommittedUpdate {
        target_row_id: u64,
        row_id: u64,
    },
    CommittedMultiBatch {
        row_ids: [u64; 2],
    },
    Dropped,
}

/// A pure insert has an empty write-set (`Transaction::insert` never
/// touches it), so it structurally cannot conflict — any error here is a
/// genuine, unexpected bug, exactly like today's insert-only worker.
pub(crate) fn execute_insert(
    dataset: &Dataset,
    business_id: i64,
    name: &str,
    vector: [f32; 3],
) -> ExecOutcome {
    let batch = strata_txn::mvp_fixtures::mvp_row(business_id, name, vector)
        .expect("mvp_row must succeed for a well-formed insert");
    let mut txn = dataset.begin();
    txn.insert(batch)
        .expect("mvp_row must match the dataset schema");
    match txn.commit() {
        Ok(()) => ExecOutcome::CommittedInsert {
            business_id,
            row_id: lookup_row_id(dataset, business_id),
        },
        Err(e) => panic!("unexpected commit error on a pure insert (inserts cannot conflict): {e}"),
    }
}

/// Whether [`commit_with_retry_once`]'s two-attempt policy ended in a
/// commit or a drop. Deliberately not `Result` -- neither outcome is an
/// error at this layer, and folding "dropped" into an `Err` variant would
/// invite a stray `?` to silently propagate it as one.
#[derive(Debug, PartialEq, Eq)]
enum RetryOutcome {
    Committed,
    Dropped,
}

/// The retry-once-then-drop policy shared by [`execute_delete`] and
/// [`execute_update`] -- see design doc §3.2. `attempt` must build and
/// commit a FRESH transaction on every call (never reuse transaction
/// state across calls), since a retry needs a `dataset.begin()` at the
/// post-winner version to have any chance of succeeding. `context` names
/// the operation for the panic message on an unexpected error. A dead
/// target is a normal terminal drop under the strict target contract, while
/// a typed OCC conflict gets the one retry.
fn commit_with_retry_once(
    mut attempt: impl FnMut() -> Result<(), TxnError>,
    context: &str,
) -> RetryOutcome {
    match attempt() {
        Ok(()) => RetryOutcome::Committed,
        Err(TxnError::RowNotLive { .. }) => RetryOutcome::Dropped,
        Err(TxnError::Conflict { .. } | TxnError::InsufficientHistory { .. }) => match attempt() {
            Ok(()) => RetryOutcome::Committed,
            Err(TxnError::RowNotLive { .. }) => RetryOutcome::Dropped,
            Err(TxnError::Conflict { .. } | TxnError::InsufficientHistory { .. }) => {
                RetryOutcome::Dropped
            }
            Err(e) => panic!("unexpected commit error on {context} retry: {e}"),
        },
        Err(e) => panic!("unexpected commit error on {context}: {e}"),
    }
}

/// See design doc §3.2 and [`commit_with_retry_once`] for the retry
/// policy.
pub(crate) fn execute_delete(dataset: &Dataset, target_row_id: u64) -> ExecOutcome {
    let attempt = || {
        let mut txn = dataset.begin();
        txn.delete(target_row_id)?;
        txn.commit()
    };
    match commit_with_retry_once(attempt, "delete") {
        RetryOutcome::Committed => ExecOutcome::CommittedDelete { target_row_id },
        RetryOutcome::Dropped => ExecOutcome::Dropped,
    }
}

/// See design doc §3.2 and [`commit_with_retry_once`] for the retry
/// policy.
pub(crate) fn execute_update(
    dataset: &Dataset,
    target_row_id: u64,
    business_id: i64,
    name: &str,
    vector: [f32; 3],
) -> ExecOutcome {
    let attempt = || {
        let batch = strata_txn::mvp_fixtures::mvp_row(business_id, name, vector)
            .expect("mvp_row must succeed for a well-formed update");
        let mut txn = dataset.begin();
        txn.update(target_row_id, batch)?;
        txn.commit()
    };
    match commit_with_retry_once(attempt, "update") {
        RetryOutcome::Committed => ExecOutcome::CommittedUpdate {
            target_row_id,
            row_id: lookup_row_id(dataset, business_id),
        },
        RetryOutcome::Dropped => ExecOutcome::Dropped,
    }
}

/// Bundles 2 separate `Transaction::insert()` calls into one `commit()` —
/// the multi-batch shape that exercises `merge_zone_map_stats` across
/// batches. Like [`execute_insert`], a pure insert cannot conflict.
pub(crate) fn execute_multi_batch_insert(
    dataset: &Dataset,
    business_ids: [i64; 2],
    name: &str,
    vectors: [[f32; 3]; 2],
) -> ExecOutcome {
    let batch0 = strata_txn::mvp_fixtures::mvp_row(business_ids[0], name, vectors[0])
        .expect("mvp_row must succeed for a well-formed multi-batch insert");
    let batch1 = strata_txn::mvp_fixtures::mvp_row(business_ids[1], name, vectors[1])
        .expect("mvp_row must succeed for a well-formed multi-batch insert");
    let mut txn = dataset.begin();
    txn.insert(batch0)
        .expect("mvp_row must match the dataset schema");
    txn.insert(batch1)
        .expect("mvp_row must match the dataset schema");
    match txn.commit() {
        Ok(()) => ExecOutcome::CommittedMultiBatch {
            row_ids: [
                lookup_row_id(dataset, business_ids[0]),
                lookup_row_id(dataset, business_ids[1]),
            ],
        },
        Err(e) => {
            panic!("unexpected commit error on multi-batch insert (inserts cannot conflict): {e}")
        }
    }
}

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
/// `docs/phase-1-audit.md`.
pub(crate) fn print_line(line: &str) {
    let stdout = std::io::stdout();
    let mut locked = stdout.lock();
    // Unwrap, not `let _ =`: this worker's entire contract with
    // `tests/sim`'s orchestrator IS the ack stream, so a write/flush
    // failure (e.g. the orchestrator's read end closing) must panic and
    // go through `install_failure_hook`, not be silently swallowed while
    // this thread keeps committing rows nobody will ever hear about --
    // the exact false "lost write" signature this harness exists to rule
    // out. Safe to unwrap from inside a held `StdoutLock`: the hook's own
    // `stdout().lock()` call reacquires the SAME thread's reentrant lock,
    // never another thread's (see `stdout_lock_discipline_tests`).
    writeln!(locked, "{line}").unwrap();
    locked.flush().unwrap();
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn dataset(dir: &std::path::Path) -> Dataset {
        Dataset::create(dir, strata_txn::mvp_fixtures::mvp_schema()).unwrap()
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-worker-test-{label}-{}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn lookup_row_id_finds_the_row_just_inserted() {
        // Two rows with distinct business ids, not one -- with only one
        // row present, this test can't tell "matched business id 42" from
        // "returned whatever row happened to be there," since both would
        // return the dataset's only row-id. Two rows makes the predicate
        // itself load-bearing: a lookup_row_id that filtered on the wrong
        // column, the wrong Value variant, or ignored the predicate
        // entirely would still pass a single-row version of this test.
        let dir = temp_dir("lookup-row-id");
        let dataset = dataset(&dir);
        let mut first = dataset.begin();
        first
            .insert(strata_txn::mvp_fixtures::mvp_row(42, "agent0", [1.0, 2.0, 3.0]).unwrap())
            .unwrap();
        first.commit().unwrap();
        let mut second = dataset.begin();
        second
            .insert(strata_txn::mvp_fixtures::mvp_row(7, "agent0", [4.0, 5.0, 6.0]).unwrap())
            .unwrap();
        second.commit().unwrap();

        // First-ever commit in a fresh dataset always claims row-id 0; the
        // second commit claims row-id 1.
        assert_eq!(lookup_row_id(&dataset, 42), 0);
        assert_eq!(lookup_row_id(&dataset, 7), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn registry_record_and_remove_round_trip() {
        let mut registry = Registry::new(2);
        registry.record_pool_row(100);
        registry.record_own_row(0, 200);
        registry.record_own_row(1, 300);

        assert_eq!(registry.pool_rows(), &[100]);
        assert_eq!(registry.own_rows(0), &[200]);
        assert_eq!(registry.own_rows(1), &[300]);

        registry.remove(0, 100); // a pool row, removed via agent 0's delete
        assert_eq!(registry.pool_rows(), &[] as &[u64]);

        registry.remove(0, 200); // agent 0's own row
        assert_eq!(registry.own_rows(0), &[] as &[u64]);
        // Untouched: agent 1's own row.
        assert_eq!(registry.own_rows(1), &[300]);
    }

    #[test]
    fn registry_remove_of_an_unknown_row_id_is_a_harmless_no_op() {
        let mut registry = Registry::new(1);
        registry.record_own_row(0, 1);
        registry.remove(0, 999);
        assert_eq!(registry.own_rows(0), &[1]);
    }

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

    #[test]
    fn commit_with_retry_once_commits_on_first_success() {
        let outcome = commit_with_retry_once(|| Ok(()), "test");
        assert_eq!(outcome, RetryOutcome::Committed);
    }

    #[test]
    fn commit_with_retry_once_retries_and_commits_after_one_conflict() {
        let mut calls = 0;
        let outcome = commit_with_retry_once(
            || {
                calls += 1;
                if calls == 1 {
                    Err(TxnError::Conflict {
                        contested_row_ids: vec![1],
                    })
                } else {
                    Ok(())
                }
            },
            "test",
        );
        assert_eq!(outcome, RetryOutcome::Committed);
        assert_eq!(calls, 2, "must retry exactly once, not more");
    }

    #[test]
    fn commit_with_retry_once_retries_and_commits_after_insufficient_history() {
        let mut calls = 0;
        let outcome = commit_with_retry_once(
            || {
                calls += 1;
                if calls == 1 {
                    Err(TxnError::InsufficientHistory {
                        base_version: 3,
                        oldest_retained_version: 5,
                        latest_version: 12,
                    })
                } else {
                    Ok(())
                }
            },
            "test",
        );
        assert_eq!(outcome, RetryOutcome::Committed);
        assert_eq!(calls, 2, "must retry exactly once, not more");
    }

    #[test]
    fn commit_with_retry_once_drops_after_a_second_conflict() {
        let mut calls = 0;
        let outcome = commit_with_retry_once(
            || {
                calls += 1;
                Err(TxnError::Conflict {
                    contested_row_ids: vec![1],
                })
            },
            "test",
        );
        assert_eq!(outcome, RetryOutcome::Dropped);
        assert_eq!(calls, 2, "must attempt exactly twice, never a third time");
    }

    #[test]
    #[should_panic(expected = "unexpected commit error on test:")]
    fn commit_with_retry_once_panics_on_a_non_conflict_error_on_the_first_attempt() {
        commit_with_retry_once(
            || Err(TxnError::NotFound(std::path::PathBuf::from("x"))),
            "test",
        );
    }

    #[test]
    #[should_panic(expected = "unexpected commit error on test retry:")]
    fn commit_with_retry_once_panics_on_a_non_conflict_error_on_the_retry() {
        // Distinct from the first-attempt panic test above: the message
        // prefix "unexpected commit error on test" is a substring of both
        // panic sites, so without asserting the ":" vs " retry:" suffix
        // (and without a first attempt that actually conflicts) this
        // would pass even if the retry arm's panic message were wrong.
        let mut calls = 0;
        commit_with_retry_once(
            || {
                calls += 1;
                if calls == 1 {
                    Err(TxnError::Conflict {
                        contested_row_ids: vec![1],
                    })
                } else {
                    Err(TxnError::NotFound(std::path::PathBuf::from("x")))
                }
            },
            "test",
        );
    }

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

    #[test]
    fn execute_insert_returns_the_looked_up_row_id() {
        let dir = temp_dir("execute-insert");
        let dataset = dataset(&dir);
        let outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        assert!(matches!(
            outcome,
            ExecOutcome::CommittedInsert {
                business_id: 1,
                row_id: 0
            }
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_delete_tombstones_the_target_row() {
        let dir = temp_dir("execute-delete");
        let dataset = dataset(&dir);
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert { row_id, .. } = insert_outcome else {
            panic!("expected CommittedInsert");
        };

        let delete_outcome = execute_delete(&dataset, row_id);
        assert!(matches!(
            delete_outcome,
            ExecOutcome::CommittedDelete { target_row_id } if target_row_id == row_id
        ));

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(
            visible.num_rows(),
            0,
            "the deleted row must no longer be visible"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_update_tombstones_the_old_row_and_makes_the_new_one_visible() {
        let dir = temp_dir("execute-update");
        let dataset = dataset(&dir);
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert {
            row_id: old_row_id, ..
        } = insert_outcome
        else {
            panic!("expected CommittedInsert");
        };

        let update_outcome = execute_update(&dataset, old_row_id, 2, "agent0", [9.0, 9.0, 9.0]);
        let ExecOutcome::CommittedUpdate {
            target_row_id,
            row_id: new_row_id,
        } = update_outcome
        else {
            panic!("expected CommittedUpdate, got {update_outcome:?}");
        };
        assert_eq!(target_row_id, old_row_id);
        assert_ne!(
            new_row_id, old_row_id,
            "the replacement insert must get a fresh row-id"
        );

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(visible.num_rows(), 1, "exactly the new row must be visible");
        let id_col = visible
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            id_col.value(0),
            2,
            "the new row's business id must be the update's, not the old one's"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_multi_batch_insert_commits_both_rows_in_one_transaction() {
        let dir = temp_dir("execute-multibatch");
        let dataset = dataset(&dir);
        let outcome = execute_multi_batch_insert(
            &dataset,
            [1, 2],
            "agent0",
            [[1.0, 1.0, 1.0], [2.0, 2.0, 2.0]],
        );
        let ExecOutcome::CommittedMultiBatch { row_ids } = outcome else {
            panic!("expected CommittedMultiBatch, got {outcome:?}");
        };
        assert_ne!(row_ids[0], row_ids[1]);

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(
            visible.num_rows(),
            2,
            "both rows from the one multi-batch commit must be visible"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deleting_an_already_tombstoned_row_is_a_harmless_idempotent_commit() {
        // NOT a test of the retry-then-drop path: this single-threaded
        // scenario has no second concurrent transaction, so
        // execute_delete's second call here cannot produce an actual
        // TxnError::Conflict -- it just re-tombstones an already-dead
        // row-id, which the design doc's own note says is harmless. This
        // pins that specific claim down. Genuine conflict-drop coverage
        // (a real TxnError::Conflict, retried once, then dropped) can
        // only come from real concurrent interleaving -- that's exercised
        // by Task 8's chaos-tier runs (many agents, real scheduling, a
        // shared contested pool), not by a unit test here.
        let dir = temp_dir("execute-delete-idempotent-retombstone");
        let dataset = dataset(&dir);
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert { row_id, .. } = insert_outcome else {
            panic!("expected CommittedInsert");
        };

        let first = execute_delete(&dataset, row_id);
        assert!(matches!(first, ExecOutcome::CommittedDelete { .. }));

        let second = execute_delete(&dataset, row_id);
        assert!(
            matches!(second, ExecOutcome::CommittedDelete { .. }),
            "re-deleting an already-tombstoned row-id must commit cleanly, not error or drop: {second:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
