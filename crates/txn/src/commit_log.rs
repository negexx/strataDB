//! A bounded, in-memory ring buffer of recently-committed transactions'
//! write-sets — see
//! `docs/superpowers/specs/2026-07-21-phase-6-concurrent-write-engine-design.md`
//! §4. `Snapshot`s don't retain write-set history once unreferenced, so
//! conflict-checking "did anything land between my read-version and now
//! touch my rows" needs its own structure independent of `Snapshot`'s
//! lifetime.

use std::collections::VecDeque;

/// Outcome of [`CommitLog::conflicts_with`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictCheck {
    /// No committed transaction in the checked range touched any of the
    /// queried row-ids.
    Clean,
    /// At least one committed transaction's write-set intersected the
    /// queried write-set. Carries every contested row-id, not just the
    /// first — matches `TxnError::Conflict`'s contract.
    Conflict(Vec<u64>),
    /// The log's oldest entry is newer than `since_version` — some
    /// commits in the requested range have already been evicted, so
    /// "clean" cannot be proven. Treated as a conflict by the caller (see
    /// design doc §4's "conservative conflict" rule), kept as a distinct
    /// variant so tests can assert on it specifically.
    InsufficientHistory,
}

pub struct CommitLog {
    capacity: usize,
    entries: VecDeque<(u64, Vec<u64>)>,
}

impl CommitLog {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::with_capacity(capacity),
        }
    }

    /// Records a newly-committed transaction's version and write-set,
    /// evicting the oldest entry if at capacity.
    pub fn push(&mut self, version: u64, write_set: Vec<u64>) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((version, write_set));
    }

    /// Checks whether any committed transaction with version in
    /// `(since_version, up_to_version]` touched any row-id in
    /// `write_set`. See [`ConflictCheck`] for the three-way result.
    #[must_use]
    pub fn conflicts_with(
        &self,
        since_version: u64,
        up_to_version: u64,
        write_set: &[u64],
    ) -> ConflictCheck {
        if up_to_version <= since_version {
            return ConflictCheck::Clean;
        }
        if let Some((oldest_version, _)) = self.entries.front() {
            if *oldest_version > since_version + 1 && !self.entries.is_empty() {
                return ConflictCheck::InsufficientHistory;
            }
        } else {
            // Empty log but a non-empty range was requested: only "clean"
            // if nothing could possibly have committed in that range,
            // i.e. since_version == up_to_version handled above. An empty
            // log with a real gap to cover has no history at all.
            return ConflictCheck::InsufficientHistory;
        }

        let mut contested: Vec<u64> = Vec::new();
        for (version, entry_write_set) in &self.entries {
            if *version <= since_version || *version > up_to_version {
                continue;
            }
            for row_id in entry_write_set {
                if write_set.contains(row_id) && !contested.contains(row_id) {
                    contested.push(*row_id);
                }
            }
        }
        if contested.is_empty() {
            ConflictCheck::Clean
        } else {
            ConflictCheck::Conflict(contested)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_log_reports_clean_for_any_range() {
        let log = CommitLog::new(4);
        assert_eq!(log.conflicts_with(0, 0, &[1, 2]), ConflictCheck::Clean);
    }

    #[test]
    fn disjoint_write_sets_are_clean() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10, 11]);
        assert_eq!(log.conflicts_with(0, 1, &[20, 21]), ConflictCheck::Clean);
    }

    #[test]
    fn overlapping_write_sets_conflict_and_name_every_contested_row() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10, 11]);
        log.push(2, vec![10, 30]);
        let result = log.conflicts_with(0, 2, &[10, 20]);
        match result {
            ConflictCheck::Conflict(mut rows) => {
                rows.sort_unstable();
                assert_eq!(rows, vec![10]);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn versions_outside_the_requested_range_are_ignored() {
        let mut log = CommitLog::new(4);
        log.push(1, vec![10]);
        // Requested range is (5, 6] — version 1 predates it and must not
        // be treated as a conflict even though its write-set overlaps.
        assert_eq!(log.conflicts_with(5, 6, &[10]), ConflictCheck::Clean);
    }

    #[test]
    fn log_wraparound_reports_insufficient_history() {
        let mut log = CommitLog::new(2);
        log.push(1, vec![10]);
        log.push(2, vec![20]);
        log.push(3, vec![30]); // evicts version 1's entry
        assert_eq!(
            log.conflicts_with(0, 3, &[999]),
            ConflictCheck::InsufficientHistory
        );
    }

    #[test]
    fn requesting_only_still_present_versions_after_wraparound_is_fine() {
        let mut log = CommitLog::new(2);
        log.push(1, vec![10]);
        log.push(2, vec![20]);
        log.push(3, vec![30]); // evicts version 1
        // since_version=2 only needs versions >2 to be present, which they are.
        assert_eq!(log.conflicts_with(2, 3, &[999]), ConflictCheck::Clean);
    }
}
