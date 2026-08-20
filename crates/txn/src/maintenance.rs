//! Coordinated lifecycle maintenance and conditional storage bounds.
//!
//! On one shared `Dataset` handle, mutating lifecycle execution takes
//! lifecycle exclusivity and then `commit_lock`, stopping write preparation,
//! publication, migration, and other lifecycle work while live data is
//! materialized, published, protected history validated, and eligible objects
//! reclaimed. Immutable snapshot reads continue; cost scales with live data
//! and retained/protected history. Independent handles are not coordinated.

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

/// Evidence from one explicit lifecycle maintenance run.
///
/// The report describes the final inventory observed by this invocation. It is
/// not an atomic or continuously enforced storage bound. Active snapshots,
/// protected history, unknown objects, and noncontiguous physical row IDs can
/// keep the final inventory above a requested bound. This API does not provide
/// cross-process quota or SLO semantics. Its physical accounting excludes
/// `_meta/row-id-high-water` reservation metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleMaintenanceReport {
    pub retention: ManifestPruneReport,
    pub compaction: CompactionReport,
    pub vacuum: VacuumReport,
    pub inventory: crate::LifecycleReport,
    /// Whether this run's final inventory was within the requested bounds.
    ///
    /// This is an observation, not atomic or continuous enforcement. It is
    /// false when active snapshots, protected history, unknown objects, or
    /// noncontiguous physical row IDs keep the final inventory above a bound.
    /// It does not provide cross-process quota or SLO semantics, and excludes
    /// `_meta/row-id-high-water` reservation metadata from accounting.
    pub storage_bound_met: bool,
}

impl Dataset {
    /// Compacts the current snapshot, applies age retention, and vacuums
    /// recognized unprotected objects, then reports whether the requested
    /// physical bounds were met.
    ///
    /// `storage_bound_met` is the final inventory observation from this one
    /// explicit run, not atomic or continuously enforced storage-bound
    /// enforcement. Active snapshots, protected history, unknown objects, and
    /// noncontiguous physical row IDs can prevent the bound. This shared-handle
    /// API does not provide cross-process quota or SLO semantics; callers must
    /// treat a false result as an operational signal rather than silently
    /// deleting protected history.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the policy is invalid or any compaction,
    /// retention, vacuum, or inventory operation fails. If one of the
    /// publishing lifecycle operations reports `TxnError::Storage` wrapping
    /// `StorageError::PublicationIndeterminate`, it can do so before this
    /// handle is updated; stop using it, drop/reopen, and inspect recovered
    /// version/schema. Reopen proves visibility, not durability.
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
