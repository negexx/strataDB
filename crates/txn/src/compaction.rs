//! Explicit lifecycle compaction policy and result types.

/// Controls which snapshots compaction must preserve while reclaiming old
/// physical row files and immutable vector segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// Keep every active historical snapshot owned by this `Dataset` handle
    /// readable after compaction.
    pub retain_snapshots: bool,
}

impl CompactionPolicy {
    /// Returns the initial safe policy: active snapshots remain valid.
    #[must_use]
    pub const fn retain_snapshots() -> Self {
        Self {
            retain_snapshots: true,
        }
    }
}

/// Counts the work performed by one compaction operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
    pub source_version: u64,
    pub published_version: u64,
    pub row_files_written: u64,
    pub segments_written: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
}
