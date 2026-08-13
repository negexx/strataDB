//! Explicit, bounded cleanup of recognized temporary lifecycle objects.

use std::collections::HashSet;

use strata_storage::{
    Backend, LocalFs, read_manifest_at_key_with_byte_count, read_manifest_with_byte_count,
};

use crate::dataset::{Dataset, load_segments, validate_data_files, validate_tombstones};
use crate::error::{Result, TxnError};

/// Result of one vacuum operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumReport {
    pub observed_version: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
}

impl Dataset {
    /// Deletes only recognized temporary objects that are not protected by a
    /// manifest reachable from the current or active snapshot state.
    ///
    /// # Errors
    ///
    /// Returns a typed storage or manifest error when protected state cannot
    /// be validated or a deletion fails.
    pub fn vacuum(&self) -> Result<VacuumReport> {
        let _lifecycle_guard = self.lifecycle_coordinator.acquire_exclusive();
        let _commit_guard = self
            .commit_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let current = self.snapshot();
        let observed_version = current.version;
        let mut protected = HashSet::new();
        let mut protected_versions = self.live_snapshot_versions();
        protected_versions.push(observed_version);
        protected_versions.sort_unstable();
        protected_versions.dedup();

        for version in protected_versions {
            let (manifest, _) = read_manifest_with_byte_count(self.retention_dir(), version)?;
            let owned_rows =
                validate_data_files(self.retention_dir(), &manifest, &current.schema, None)?;
            validate_tombstones(self.retention_dir(), &manifest, &owned_rows)?;
            let _ = load_segments(self.retention_dir(), &manifest, &owned_rows, None)?;
            protected.extend(
                manifest
                    .data_files
                    .into_iter()
                    .map(|entry| format!("data/{}", entry.name)),
            );
            protected.extend(
                manifest
                    .segments
                    .into_iter()
                    .map(|entry| format!("data/{}", entry.name)),
            );
        }

        let backend = LocalFs::new(self.retention_dir());
        for object in backend.list("_versions/")? {
            let Some(stem) = object
                .key
                .strip_prefix("_versions/")
                .and_then(|key| key.strip_suffix(".manifest"))
            else {
                continue;
            };
            let Ok(version) = stem.parse::<u64>() else {
                continue;
            };
            let (manifest, _) =
                read_manifest_at_key_with_byte_count(self.retention_dir(), &object.key, version)?;
            let reachable = crate::lifecycle::reachable_keys(&manifest)?;
            protected.extend(reachable.data_files.into_iter().chain(reachable.segments));
        }
        let mut objects_deleted = 0_u64;
        let mut bytes_deleted = 0_u64;
        for object in backend.list("data/")? {
            let Some(name) = object.key.strip_prefix("data/") else {
                continue;
            };
            let recognized_orphan = is_strata_temporary_name(name)
                || std::path::Path::new(name)
                    .extension()
                    .is_some_and(|extension| {
                        extension.eq_ignore_ascii_case("arrow")
                            || extension.eq_ignore_ascii_case("seg")
                    });
            if !recognized_orphan || protected.contains(&object.key) {
                continue;
            }
            backend.delete(&object.key)?;
            objects_deleted = objects_deleted
                .checked_add(1)
                .ok_or_else(|| TxnError::ManifestOverflow("objects_deleted".to_owned()))?;
            bytes_deleted = bytes_deleted
                .checked_add(object.size)
                .ok_or_else(|| TxnError::ManifestOverflow("bytes_deleted".to_owned()))?;
        }

        Ok(VacuumReport {
            observed_version,
            objects_deleted,
            bytes_deleted,
        })
    }
}

fn is_strata_temporary_name(name: &str) -> bool {
    let Some(name) = std::path::Path::new(name)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return false;
    };
    let Some(name) = name.strip_prefix(".tmp-") else {
        return false;
    };
    let mut parts = name.splitn(3, '-');
    let (Some(process_id), Some(counter), Some(final_name)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };

    !final_name.is_empty()
        && process_id
            .parse::<u32>()
            .is_ok_and(|value| value.to_string() == process_id)
        && counter
            .parse::<u64>()
            .is_ok_and(|value| value.to_string() == counter)
}
