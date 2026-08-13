//! Read-only inventory helpers for lifecycle diagnostics.
//!
//! These helpers classify one captured manifest against backend object metadata;
//! they never mutate storage, manifests, or snapshots.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use strata_storage::{Manifest, ObjectMeta, StorageError};

use crate::error::{Result, TxnError};

/// A read-only, snapshot-anchored storage inventory.
///
/// `orphan_candidate_*` values identify objects not referenced by the captured
/// manifest. They are diagnostic evidence only: an orphan candidate is not a
/// safe-to-delete claim because another live snapshot or a later lifecycle
/// policy may still require that object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleReport {
    observed_version: u64,
    manifest_object_count: u64,
    manifest_bytes: u64,
    current_manifest_bytes: Option<u64>,
    data_object_count: u64,
    data_bytes: u64,
    reachable_data_file_count: u64,
    reachable_data_file_bytes: u64,
    reachable_segment_count: u64,
    reachable_segment_bytes: u64,
    orphan_candidate_count: u64,
    orphan_candidate_bytes: u64,
    tombstone_count: u64,
    physical_row_count: u64,
}

impl LifecycleReport {
    #[must_use]
    pub const fn observed_version(&self) -> u64 {
        self.observed_version
    }

    #[must_use]
    pub const fn manifest_object_count(&self) -> u64 {
        self.manifest_object_count
    }

    #[must_use]
    pub const fn manifest_bytes(&self) -> u64 {
        self.manifest_bytes
    }

    #[must_use]
    pub const fn current_manifest_bytes(&self) -> Option<u64> {
        self.current_manifest_bytes
    }

    #[must_use]
    pub const fn data_object_count(&self) -> u64 {
        self.data_object_count
    }

    #[must_use]
    pub const fn data_bytes(&self) -> u64 {
        self.data_bytes
    }

    #[must_use]
    pub const fn reachable_data_file_count(&self) -> u64 {
        self.reachable_data_file_count
    }

    #[must_use]
    pub const fn reachable_data_file_bytes(&self) -> u64 {
        self.reachable_data_file_bytes
    }

    #[must_use]
    pub const fn reachable_segment_count(&self) -> u64 {
        self.reachable_segment_count
    }

    #[must_use]
    pub const fn reachable_segment_bytes(&self) -> u64 {
        self.reachable_segment_bytes
    }

    #[must_use]
    pub const fn orphan_candidate_count(&self) -> u64 {
        self.orphan_candidate_count
    }

    #[must_use]
    pub const fn orphan_candidate_bytes(&self) -> u64 {
        self.orphan_candidate_bytes
    }

    #[must_use]
    pub const fn tombstone_count(&self) -> u64 {
        self.tombstone_count
    }

    #[must_use]
    pub const fn physical_row_count(&self) -> u64 {
        self.physical_row_count
    }
}

pub(crate) struct ReachableKeys {
    pub(crate) data_files: BTreeSet<String>,
    pub(crate) segments: BTreeSet<String>,
}

/// Joins captured backend listings with a captured manifest without touching
/// the filesystem.
///
/// The future `Dataset::lifecycle_report` entry point supplies listings rooted
/// at `_versions/` and `data/` from exactly one captured snapshot.
pub(crate) fn collect(
    manifest_objects: &[ObjectMeta],
    data_objects: &[ObjectMeta],
    manifest: &Manifest,
) -> Result<LifecycleReport> {
    let reachable = reachable_keys(manifest)?;

    let (manifest_object_count, manifest_bytes, current_manifest_bytes) =
        collect_manifest_objects(manifest_objects, manifest)?;

    let mut data_object_count = 0;
    let mut data_bytes = 0;
    let mut reachable_data_file_count = 0;
    let mut reachable_data_file_bytes = 0;
    let mut reachable_segment_count = 0;
    let mut reachable_segment_bytes = 0;
    let mut orphan_candidate_count = 0;
    let mut orphan_candidate_bytes = 0;
    let mut listed_data_keys = BTreeSet::new();
    let mut seen_reachable_keys = BTreeSet::new();
    for object in data_objects {
        validate_listed_key(&object.key, "data/")?;
        if !listed_data_keys.insert(object.key.clone()) {
            return Err(corrupt_manifest(
                manifest,
                format!("duplicate listed data object key: {}", object.key),
            ));
        }
        data_object_count = checked_add("data_object_count", data_object_count, 1)?;
        data_bytes = checked_add("data_bytes", data_bytes, object.size)?;

        if reachable.data_files.contains(&object.key) {
            seen_reachable_keys.insert(object.key.clone());
            reachable_data_file_count =
                checked_add("reachable_data_file_count", reachable_data_file_count, 1)?;
            reachable_data_file_bytes = checked_add(
                "reachable_data_file_bytes",
                reachable_data_file_bytes,
                object.size,
            )?;
        } else if reachable.segments.contains(&object.key) {
            seen_reachable_keys.insert(object.key.clone());
            reachable_segment_count =
                checked_add("reachable_segment_count", reachable_segment_count, 1)?;
            reachable_segment_bytes = checked_add(
                "reachable_segment_bytes",
                reachable_segment_bytes,
                object.size,
            )?;
        } else {
            orphan_candidate_count =
                checked_add("orphan_candidate_count", orphan_candidate_count, 1)?;
            orphan_candidate_bytes = checked_add(
                "orphan_candidate_bytes",
                orphan_candidate_bytes,
                object.size,
            )?;
        }
    }

    for key in reachable.data_files.iter().chain(reachable.segments.iter()) {
        if !seen_reachable_keys.contains(key) {
            return Err(corrupt_manifest(
                manifest,
                format!("reachable object is missing from inventory: {key}"),
            ));
        }
    }

    let (tombstone_count, physical_row_count) = manifest_totals(manifest)?;

    Ok(LifecycleReport {
        observed_version: manifest.version,
        manifest_object_count,
        manifest_bytes,
        current_manifest_bytes,
        data_object_count,
        data_bytes,
        reachable_data_file_count,
        reachable_data_file_bytes,
        reachable_segment_count,
        reachable_segment_bytes,
        orphan_candidate_count,
        orphan_candidate_bytes,
        tombstone_count,
        physical_row_count,
    })
}

fn collect_manifest_objects(
    manifest_objects: &[ObjectMeta],
    manifest: &Manifest,
) -> Result<(u64, u64, Option<u64>)> {
    let mut manifest_object_count = 0;
    let mut manifest_bytes = 0;
    let mut current_manifest_bytes = None;
    let mut listed_manifest_keys = BTreeSet::new();
    let mut listed_manifest_versions = BTreeSet::new();
    for object in manifest_objects {
        validate_listed_key(&object.key, "_versions/")?;
        if !listed_manifest_keys.insert(object.key.clone()) {
            return Err(corrupt_manifest(
                manifest,
                format!("duplicate listed manifest object key: {}", object.key),
            ));
        }
        manifest_object_count = checked_add("manifest_object_count", manifest_object_count, 1)?;
        manifest_bytes = checked_add("manifest_bytes", manifest_bytes, object.size)?;
        let Some(version) = listed_manifest_version(&object.key) else {
            continue;
        };
        if !listed_manifest_versions.insert(version) {
            return Err(TxnError::Storage(StorageError::CorruptManifest(
                PathBuf::from(manifest_object_key(version)),
                "duplicate listed manifest version".to_string(),
            )));
        }
        if version == manifest.version {
            current_manifest_bytes = Some(object.size);
        }
    }
    Ok((
        manifest_object_count,
        manifest_bytes,
        current_manifest_bytes,
    ))
}

fn listed_manifest_version(key: &str) -> Option<u64> {
    key.strip_prefix("_versions/")
        .and_then(|name| name.strip_suffix(".manifest"))
        .and_then(|stem| stem.parse().ok())
}

fn manifest_totals(manifest: &Manifest) -> Result<(u64, u64)> {
    let mut tombstone_count = 0;
    for _ in &manifest.tombstones {
        tombstone_count = checked_add("tombstone_count", tombstone_count, 1)?;
    }
    let mut physical_row_count = 0;
    for data_file in &manifest.data_files {
        physical_row_count = checked_add(
            "physical_row_count",
            physical_row_count,
            data_file.row_count,
        )?;
    }
    Ok((tombstone_count, physical_row_count))
}

pub(crate) fn checked_add(total: &str, current: u64, value: u64) -> Result<u64> {
    current
        .checked_add(value)
        .ok_or_else(|| TxnError::ManifestOverflow(total.to_string()))
}

pub(crate) fn reachable_keys(manifest: &Manifest) -> Result<ReachableKeys> {
    let mut data_files = BTreeSet::new();
    let mut segments = BTreeSet::new();

    for entry in &manifest.data_files {
        insert_reachable_key(&mut data_files, entry.name.as_str(), manifest)?;
    }
    for entry in &manifest.segments {
        let key = manifest_data_key(entry.name.as_str())?;
        if data_files.contains(&key) || !segments.insert(key.clone()) {
            return Err(corrupt_manifest(
                manifest,
                format!("duplicate reachable object key: {key}"),
            ));
        }
    }

    Ok(ReachableKeys {
        data_files,
        segments,
    })
}

fn insert_reachable_key(
    data_files: &mut BTreeSet<String>,
    name: &str,
    manifest: &Manifest,
) -> Result<()> {
    let key = manifest_data_key(name)?;
    if !data_files.insert(key.clone()) {
        return Err(corrupt_manifest(
            manifest,
            format!("duplicate reachable object key: {key}"),
        ));
    }
    Ok(())
}

pub(crate) fn manifest_data_key(name: &str) -> Result<String> {
    validate_manifest_relative_name(name)?;
    Ok(format!("data/{name}"))
}

pub(crate) fn validate_listed_key(key: &str, prefix: &str) -> Result<()> {
    let Some(relative_name) = key.strip_prefix(prefix) else {
        return Err(TxnError::UnsafeManifestPath(key.to_string()));
    };
    validate_manifest_relative_name(relative_name)
}

fn validate_manifest_relative_name(name: &str) -> Result<()> {
    let path = Path::new(name);
    if name.is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(TxnError::UnsafeManifestPath(name.to_string()));
    }
    Ok(())
}

pub(crate) fn manifest_object_key(version: u64) -> String {
    format!("_versions/{version:020}.manifest")
}

fn corrupt_manifest(manifest: &Manifest, reason: String) -> TxnError {
    TxnError::Storage(StorageError::CorruptManifest(
        PathBuf::from(manifest_object_key(manifest.version)),
        reason,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use strata_storage::{DataFileEntry, Manifest, ObjectMeta, SegmentEntry, StorageError};

    use super::*;
    use crate::TxnError;

    #[test]
    fn checked_add_rejects_byte_total_overflow() {
        // Break caught: wrapping a lifecycle byte total would under-report
        // physical storage when listed object sizes exceed u64.
        let result = checked_add("manifest_bytes", u64::MAX, 1);

        assert!(matches!(
            result,
            Err(TxnError::ManifestOverflow(total)) if total == "manifest_bytes"
        ));
    }

    #[test]
    fn checked_add_rejects_count_total_overflow() {
        // Break caught: wrapping an object count would under-report the
        // number of listed data objects when the count exceeds u64.
        let result = checked_add("data_object_count", u64::MAX, 1);

        assert!(matches!(
            result,
            Err(TxnError::ManifestOverflow(total)) if total == "data_object_count"
        ));
    }

    #[test]
    fn reachable_keys_reject_duplicate_manifest_object_keys() {
        // Break caught: accepting two manifest entries for the same physical
        // object would double-count a reachable object and hide corruption.
        let mut manifest = Manifest::empty();
        manifest.data_files = vec![data_file("shared.bin")];
        manifest.segments = vec![segment("shared.bin")];

        let result = reachable_keys(&manifest);

        assert!(matches!(
            result,
            Err(TxnError::Storage(StorageError::CorruptManifest(path, reason)))
                if path.as_path() == Path::new("_versions/00000000000000000000.manifest")
                    && reason.contains("duplicate reachable object key: data/shared.bin")
        ));
    }

    #[test]
    fn collect_classifies_current_manifest_objects_without_mutation() -> crate::Result<()> {
        // Break caught: treating a captured manifest's row file or segment as
        // an orphan candidate would make diagnostic inventory misleading.
        let mut manifest = Manifest::empty();
        manifest.version = 7;
        manifest.data_files = vec![data_file("rows.arrow")];
        manifest.data_files[0].row_count = 3;
        manifest.segments = vec![segment("vectors.seg")];
        manifest.tombstones = vec![1, 2];
        let manifests = vec![
            object("_versions/00000000000000000006.manifest", 11),
            object("_versions/00000000000000000007.manifest", 13),
        ];
        let data = vec![
            object("data/rows.arrow", 17),
            object("data/vectors.seg", 19),
            object("data/left-over.tmp", 23),
        ];

        let report = collect(&manifests, &data, &manifest)?;

        assert_eq!(report.observed_version(), 7);
        assert_eq!(report.manifest_object_count(), 2);
        assert_eq!(report.manifest_bytes(), 24);
        assert_eq!(report.current_manifest_bytes(), Some(13));
        assert_eq!(report.data_object_count(), 3);
        assert_eq!(report.data_bytes(), 59);
        assert_eq!(report.reachable_data_file_count(), 1);
        assert_eq!(report.reachable_data_file_bytes(), 17);
        assert_eq!(report.reachable_segment_count(), 1);
        assert_eq!(report.reachable_segment_bytes(), 19);
        assert_eq!(report.orphan_candidate_count(), 1);
        assert_eq!(report.orphan_candidate_bytes(), 23);
        assert_eq!(report.tombstone_count(), 2);
        assert_eq!(report.physical_row_count(), 3);
        Ok(())
    }

    fn object(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size,
        }
    }

    fn data_file(name: &str) -> DataFileEntry {
        DataFileEntry {
            name: name.to_string(),
            byte_len: 0,
            crc32c: 0,
            row_count: 0,
            row_id_range: None,
            stats: HashMap::default(),
        }
    }

    fn segment(name: &str) -> SegmentEntry {
        SegmentEntry {
            name: name.to_string(),
            format_version: 1,
            vector_count: 0,
            dimension: 0,
            row_id_min: 0,
            row_id_max: 0,
            byte_len: 0,
            zone_map: HashMap::default(),
        }
    }
}
