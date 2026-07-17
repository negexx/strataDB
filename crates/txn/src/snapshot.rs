//! A point-in-time, immutable view of a [`Dataset`](crate::Dataset) — see
//! `docs/superpowers/specs/2026-07-17-phase-5-mvcc-snapshot-isolation-design.md`.
//! Every field is either `Copy` or `Arc`-wrapped, so cloning a `Snapshot` is
//! cheap and never touches the data it points to.

use std::path::PathBuf;
use std::sync::Arc;

use strata_index::HnswIndex;
use strata_storage::Manifest;

pub struct Snapshot {
    pub(crate) dir: PathBuf,
    pub(crate) version: u64,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) graph: Arc<HnswIndex>,
    pub(crate) watermark: u64,
    pub(crate) tombstones: Arc<im::HashSet<u64>>,
}

impl Snapshot {
    /// Whether `row_id` is visible under this snapshot: committed at or
    /// before this snapshot's version, and not tombstoned as of this
    /// snapshot's version. No delta-log schema change is needed for this
    /// to be correct — the version boundary comes from *when* a `Snapshot`
    /// was built (immediately after the commit that produced it), not from
    /// a stored version per tombstone entry. See the design doc's
    /// "Tombstone mechanism" section.
    pub(crate) fn is_visible(&self, row_id: u64) -> bool {
        row_id <= self.watermark && !self.tombstones.contains(&row_id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_snapshot(watermark: u64, tombstoned: &[u64]) -> Snapshot {
        Snapshot {
            dir: PathBuf::from("unused-in-these-tests"),
            version: 1,
            manifest: Arc::new(Manifest::empty()),
            graph: Arc::new(HnswIndex::new(16, 100, 16, 200).unwrap()),
            watermark,
            tombstones: Arc::new(tombstoned.iter().copied().collect()),
        }
    }

    #[test]
    fn row_at_or_below_watermark_and_not_tombstoned_is_visible() {
        let snapshot = test_snapshot(10, &[]);
        assert!(snapshot.is_visible(0));
        assert!(snapshot.is_visible(10));
    }

    #[test]
    fn row_above_watermark_is_not_visible() {
        let snapshot = test_snapshot(10, &[]);
        assert!(!snapshot.is_visible(11));
    }

    #[test]
    fn tombstoned_row_at_or_below_watermark_is_not_visible() {
        let snapshot = test_snapshot(10, &[5]);
        assert!(!snapshot.is_visible(5));
        assert!(snapshot.is_visible(6));
    }
}
