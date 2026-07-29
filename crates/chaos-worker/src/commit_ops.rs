//! Commit execution, conflict retry, and the contested-row-id registry
//! for the chaos workload. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! §3.1-§3.2.

use arrow::array::UInt64Array;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::{Dataset, ROW_ID_COLUMN};

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
    /// is exhaustive.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
        let dataset = Dataset::create(&dir).unwrap();
        let mut first = dataset.begin();
        first.insert(strata_txn::mvp_fixtures::mvp_row(42, "agent0", [1.0, 2.0, 3.0]).unwrap());
        first.commit().unwrap();
        let mut second = dataset.begin();
        second.insert(strata_txn::mvp_fixtures::mvp_row(7, "agent0", [4.0, 5.0, 6.0]).unwrap());
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
}
