//! Explicit, bounded cleanup of recognized temporary lifecycle objects.

use std::collections::HashSet;

use strata_storage::read_manifest_at_key_with_byte_count_and_size_with;

use crate::dataset::{
    Dataset, load_segments_with_owner, validate_data_files_with_owner, validate_tombstones,
};
use crate::error::{Result, TxnError};
use crate::retention::index_manifest_objects;

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
        let result = self.vacuum_inner();
        self.record_lifecycle_result(&result);
        result
    }

    fn vacuum_inner(&self) -> Result<VacuumReport> {
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
        let storage = self.storage();
        let manifest_keys = index_manifest_objects(&storage.list("_versions")?)?;

        for version in protected_versions {
            let key = manifest_keys.get(&version).ok_or_else(|| {
                TxnError::Storage(strata_storage::StorageError::CorruptManifest(
                    self.retention_dir()
                        .join(format!("_versions/{version:020}.manifest")),
                    "protected manifest is missing from inventory".to_owned(),
                ))
            })?;
            let (manifest, _) = read_manifest_at_key_with_byte_count_and_size_with(
                &storage, &key.key, version, key.bytes,
            )?;
            let owned_rows = validate_data_files_with_owner(
                &storage,
                self.retention_dir(),
                &manifest,
                &current.schema,
                None,
            )
            .map_err(|error| match error {
                TxnError::Storage(strata_storage::StorageError::Io(source)) => TxnError::Io(source),
                other => other,
            })?;
            validate_tombstones(self.retention_dir(), &manifest, &owned_rows)?;
            let _ = load_segments_with_owner(&storage, &manifest, &owned_rows, None)?;
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

        for (version, key) in manifest_keys {
            let (manifest, _) = read_manifest_at_key_with_byte_count_and_size_with(
                &storage, &key.key, version, key.bytes,
            )?;
            let reachable = crate::lifecycle::reachable_keys(&manifest)?;
            protected.extend(reachable.data_files.into_iter().chain(reachable.segments));
        }
        let mut objects_deleted = 0_u64;
        let mut bytes_deleted = 0_u64;
        for object in storage.list("data")? {
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
            let key = strata_storage::DatasetKey::new(&object.key).map_err(TxnError::Storage)?;
            storage.delete(&key)?;
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
