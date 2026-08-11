//! Read-only retention planning for Phase 3 lifecycle work.
//!
//! A retention plan is an advisory observation. It never deletes, rewrites,
//! compacts, or republishes an object, and it only tracks snapshots created by
//! the shared in-process [`Dataset`](crate::Dataset) handle.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;

use strata_storage::{Backend, LocalFs, ObjectMeta, StorageError, read_manifest_with_byte_count};

use crate::dataset::Dataset;
use crate::error::{Result, TxnError};
use crate::lifecycle::{checked_add, manifest_object_key, reachable_keys, validate_listed_key};

/// Latest-version retention window supplied to [`Dataset::retention_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Number of newest durable manifest versions to retain. Must be non-zero.
    pub keep_latest_versions: u64,
}

/// Read-only retention evidence captured from one filesystem observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    pub observed_version: u64,
    pub active_snapshot_versions: Vec<u64>,
    pub retained_manifest_versions: Vec<u64>,
    pub retained_data_object_count: u64,
    pub retained_data_bytes: u64,
    pub eligible_manifest_versions: Vec<u64>,
    pub eligible_data_objects: Vec<RetentionCandidate>,
}

/// A well-formed listed data object outside the retained reachability set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionCandidate {
    pub key: String,
    pub bytes: u64,
}

/// A lease held by every production-created [`Snapshot`](crate::Snapshot).
/// The lease is intentionally separate from the snapshot's manifest so the
/// registry can observe Arc lifetime without keeping the snapshot alive.
#[derive(Debug)]
pub(crate) struct SnapshotLease {
    pub(crate) version: u64,
}

impl SnapshotLease {
    #[cfg(test)]
    pub(crate) fn unregistered(version: u64) -> Arc<Self> {
        Arc::new(Self { version })
    }
}

/// Non-owning registry of leases created by one shared `Dataset` handle.
#[derive(Debug, Default)]
pub(crate) struct SnapshotLeaseRegistry {
    leases: Mutex<Vec<Weak<SnapshotLease>>>,
}

impl SnapshotLeaseRegistry {
    pub(crate) fn register(&self, version: u64) -> Arc<SnapshotLease> {
        let lease = Arc::new(SnapshotLease { version });
        #[cfg(loom)]
        let mut leases = self.leases.lock().unwrap();
        #[cfg(not(loom))]
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        leases.push(Arc::downgrade(&lease));
        lease
    }

    pub(crate) fn live_versions(&self) -> Vec<u64> {
        #[cfg(loom)]
        let mut leases = self.leases.lock().unwrap();
        #[cfg(not(loom))]
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut versions = BTreeSet::new();
        leases.retain(|weak| {
            let Some(lease) = weak.upgrade() else {
                return false;
            };
            versions.insert(lease.version);
            true
        });
        versions.into_iter().collect()
    }
}

pub(crate) fn build_plan(dataset: &Dataset, policy: RetentionPolicy) -> Result<RetentionPlan> {
    if policy.keep_latest_versions == 0 {
        return Err(TxnError::InvalidRetentionPolicy);
    }

    let snapshot = dataset.snapshot();
    debug_assert_eq!(snapshot.version, snapshot.lease.version);
    let observed_version = snapshot.version;
    let mut active_snapshot_versions = dataset.live_snapshot_versions();
    active_snapshot_versions.push(observed_version);
    active_snapshot_versions.sort_unstable();
    active_snapshot_versions.dedup();

    let backend = LocalFs::new(dataset.retention_dir());
    let manifest_objects = backend.list("_versions/")?;
    let data_objects = backend.list("data/")?;
    let manifest_sizes = index_manifest_objects(&manifest_objects)?;

    let mut retained_manifest_versions =
        latest_versions(manifest_sizes.keys().copied(), policy.keep_latest_versions);
    retained_manifest_versions.extend(active_snapshot_versions.iter().copied());
    retained_manifest_versions.sort_unstable();
    retained_manifest_versions.dedup();

    let mut retained_data_keys = BTreeSet::new();
    for &version in &retained_manifest_versions {
        let (manifest, _) = read_manifest_with_byte_count(dataset.retention_dir(), version)?;
        let reachable = reachable_keys(&manifest)?;
        retained_data_keys.extend(reachable.data_files);
        retained_data_keys.extend(reachable.segments);
    }

    let (eligible_manifest_versions, older_data_keys) = older_manifest_data_keys(
        dataset.retention_dir(),
        &manifest_sizes,
        &retained_manifest_versions,
        observed_version,
    )?;
    let (retained_data_object_count, retained_data_bytes, eligible_data_objects) =
        classify_data_objects(&data_objects, &retained_data_keys, &older_data_keys)?;
    let mut reachable_data_keys = retained_data_keys.clone();
    reachable_data_keys.extend(older_data_keys);
    ensure_reachable_objects_are_listed(&data_objects, &reachable_data_keys)?;

    Ok(RetentionPlan {
        observed_version,
        active_snapshot_versions,
        retained_manifest_versions,
        retained_data_object_count,
        retained_data_bytes,
        eligible_manifest_versions,
        eligible_data_objects,
    })
}

fn index_manifest_objects(objects: &[ObjectMeta]) -> Result<BTreeMap<u64, u64>> {
    let mut versions = BTreeMap::new();
    for object in objects {
        validate_listed_key(&object.key, "_versions/")?;
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
        if versions.insert(version, object.size).is_some() {
            return Err(TxnError::Storage(StorageError::CorruptManifest(
                PathBuf::from(manifest_object_key(version)),
                "duplicate listed manifest version".to_string(),
            )));
        }
    }
    Ok(versions)
}

fn latest_versions(versions: impl Iterator<Item = u64>, keep: u64) -> Vec<u64> {
    let mut versions: Vec<_> = versions.collect();
    versions.sort_unstable();
    let keep = usize::try_from(keep).unwrap_or(usize::MAX);
    let start = versions.len().saturating_sub(keep);
    versions.drain(start..).collect()
}

fn classify_data_objects(
    objects: &[ObjectMeta],
    retained_keys: &BTreeSet<String>,
    older_keys: &BTreeSet<String>,
) -> Result<(u64, u64, Vec<RetentionCandidate>)> {
    let mut seen = BTreeSet::new();
    let mut retained_count = 0;
    let mut retained_bytes = 0;
    let mut eligible = Vec::new();
    for object in objects {
        validate_listed_key(&object.key, "data/")?;
        if !seen.insert(object.key.clone()) {
            continue;
        }
        if retained_keys.contains(&object.key) {
            retained_count = checked_add("retained_data_object_count", retained_count, 1)?;
            retained_bytes = checked_add("retained_data_bytes", retained_bytes, object.size)?;
        } else if older_keys.contains(&object.key) && !is_temporary_data_object(&object.key) {
            eligible.push(RetentionCandidate {
                key: object.key.clone(),
                bytes: object.size,
            });
        }
    }
    eligible.sort_by(|left, right| left.key.cmp(&right.key));
    Ok((retained_count, retained_bytes, eligible))
}

fn ensure_reachable_objects_are_listed(
    objects: &[ObjectMeta],
    retained_keys: &BTreeSet<String>,
) -> Result<()> {
    let listed: BTreeSet<_> = objects.iter().map(|object| object.key.as_str()).collect();
    if let Some(missing) = retained_keys
        .iter()
        .find(|key| !listed.contains(key.as_str()))
    {
        return Err(TxnError::Storage(StorageError::CorruptManifest(
            PathBuf::from("_versions"),
            format!("reachable object is missing from inventory: {missing}"),
        )));
    }
    Ok(())
}

fn older_manifest_data_keys(
    dataset_dir: &std::path::Path,
    manifest_sizes: &BTreeMap<u64, u64>,
    retained_versions: &[u64],
    observed_version: u64,
) -> Result<(Vec<u64>, BTreeSet<String>)> {
    let retained: BTreeSet<_> = retained_versions.iter().copied().collect();
    let mut eligible_versions = Vec::new();
    let mut older_data_keys = BTreeSet::new();
    for &version in manifest_sizes
        .keys()
        .filter(|version| **version < observed_version && !retained.contains(version))
    {
        let (manifest, _) = read_manifest_with_byte_count(dataset_dir, version)?;
        let reachable = reachable_keys(&manifest)?;
        older_data_keys.extend(reachable.data_files);
        older_data_keys.extend(reachable.segments);
        eligible_versions.push(version);
    }
    Ok((eligible_versions, older_data_keys))
}

fn is_temporary_data_object(key: &str) -> bool {
    key.strip_prefix("data/")
        .is_some_and(|name| name.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use strata_storage::ObjectMeta;

    use super::*;

    #[test]
    fn registry_deduplicates_versions_and_releases_dead_leases() {
        let registry = SnapshotLeaseRegistry::default();
        let first = registry.register(3);
        let second = registry.register(3);
        assert_eq!(registry.live_versions(), vec![3]);
        drop(first);
        assert_eq!(registry.live_versions(), vec![3]);
        drop(second);
        assert!(registry.live_versions().is_empty());
    }

    #[test]
    fn latest_versions_keeps_the_newest_window() {
        assert_eq!(latest_versions([0, 1, 2, 3].into_iter(), 2), vec![2, 3]);
    }

    #[test]
    fn data_candidates_reject_unsafe_keys() {
        let result = classify_data_objects(
            &[ObjectMeta {
                key: "data/../escape".to_string(),
                size: 1,
            }],
            &BTreeSet::new(),
            &BTreeSet::new(),
        );

        assert!(matches!(result, Err(TxnError::UnsafeManifestPath(path)) if path == "../escape"));
    }

    #[test]
    fn duplicate_inventory_key_is_counted_once() {
        let older = BTreeSet::from(["data/older.bin".to_string()]);
        let result = classify_data_objects(
            &[
                ObjectMeta {
                    key: "data/older.bin".to_string(),
                    size: 7,
                },
                ObjectMeta {
                    key: "data/older.bin".to_string(),
                    size: 7,
                },
            ],
            &BTreeSet::new(),
            &older,
        )
        .unwrap();

        assert_eq!(
            result.2,
            vec![RetentionCandidate {
                key: "data/older.bin".to_string(),
                bytes: 7,
            }]
        );
    }

    #[test]
    fn retained_data_bytes_use_checked_arithmetic() {
        let retained = BTreeSet::from(["data/a".to_string(), "data/b".to_string()]);
        let result = classify_data_objects(
            &[
                ObjectMeta {
                    key: "data/a".to_string(),
                    size: u64::MAX,
                },
                ObjectMeta {
                    key: "data/b".to_string(),
                    size: 1,
                },
            ],
            &retained,
            &BTreeSet::new(),
        );

        assert!(matches!(
            result,
            Err(TxnError::ManifestOverflow(total)) if total == "retained_data_bytes"
        ));
    }
}
