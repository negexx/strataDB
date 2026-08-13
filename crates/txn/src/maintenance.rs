//! Coordinated lifecycle maintenance and conditional storage bounds.

use crate::{
    CompactionPolicy, CompactionReport, Dataset, ManifestPruneReport, Result, VacuumReport,
};

/// Bounds used by [`Dataset::maintain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleMaintenancePolicy {
    /// Number of newest manifests to retain.
    pub keep_latest_versions: u64,
    /// Maximum age of eligible historical manifests, in microseconds.
    pub max_age_us: u64,
    /// Maximum number of data objects allowed after maintenance.
    pub max_data_objects: u64,
    /// Maximum number of segments reachable from the current snapshot.
    pub max_segments: u64,
}

/// Evidence from one coordinated lifecycle maintenance run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleMaintenanceReport {
    pub retention: ManifestPruneReport,
    pub compaction: CompactionReport,
    pub vacuum: VacuumReport,
    pub inventory: crate::LifecycleReport,
    /// False when active snapshots or other protected durable history prevent
    /// the requested physical bounds from being met.
    pub storage_bound_met: bool,
}

impl Dataset {
    /// Compacts the current snapshot, applies age retention, and vacuums
    /// recognized unprotected objects, then reports whether the requested
    /// physical bounds were met.
    ///
    /// The bound is conditional on the shared handle's active snapshots and
    /// the policy's retention window. The report is authoritative for the
    /// inventory captured at the end of this run; callers must treat a false
    /// bound as an operational signal rather than silently deleting protected
    /// history.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the policy is invalid or any compaction,
    /// retention, vacuum, or inventory operation fails.
    pub fn maintain(
        &self,
        policy: LifecycleMaintenancePolicy,
    ) -> Result<LifecycleMaintenanceReport> {
        if policy.keep_latest_versions == 0
            || policy.max_data_objects == 0
            || policy.max_segments == 0
        {
            return Err(crate::TxnError::InvalidRetentionPolicy);
        }
        let compaction = self.compact(CompactionPolicy::retain_snapshots())?;
        let retention = self.prune_manifests_by_age(crate::AgeRetentionPolicy {
            keep_latest_versions: policy.keep_latest_versions,
            max_age_us: policy.max_age_us,
        })?;
        let vacuum = self.vacuum()?;
        let inventory = self.lifecycle_report()?;
        let storage_bound_met = inventory.data_object_count() <= policy.max_data_objects
            && inventory.reachable_segment_count() <= policy.max_segments;
        Ok(LifecycleMaintenanceReport {
            retention,
            compaction,
            vacuum,
            inventory,
            storage_bound_met,
        })
    }
}
