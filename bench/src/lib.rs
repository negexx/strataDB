//! Shared, deterministic accounting helpers for Strata benchmark reports.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestListedFile {
    pub name: String,
    pub byte_len: u64,
}

impl ManifestListedFile {
    #[must_use]
    pub fn new(name: impl Into<String>, byte_len: u64) -> Self {
        Self {
            name: name.into(),
            byte_len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFootprint {
    pub version: u64,
    pub manifest_payload_bytes: u64,
    pub row_data_files: Vec<ManifestListedFile>,
    pub immutable_segment_files: Vec<ManifestListedFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedSnapshotFootprintDiagnostics {
    pub logical_manifest_payload: u128,
    pub unique_manifest_payload: u128,
    pub logical_row_data: u128,
    pub unique_row_data: u128,
    pub logical_immutable_segments: u128,
    pub unique_immutable_segments: u128,
}

fn unique_file_bytes<'a>(files: impl Iterator<Item = &'a ManifestListedFile>) -> u128 {
    let mut unique = BTreeMap::new();
    for file in files {
        unique.entry(&file.name).or_insert(file.byte_len);
    }
    unique.values().map(|bytes| u128::from(*bytes)).sum()
}

/// Reports the retained set as both per-handle logical references and unique
/// physical manifest/file payloads. It deliberately performs no allocator or
/// process-residency measurement.
#[must_use]
pub fn pinned_snapshot_footprint_diagnostics(
    pinned: &[SnapshotFootprint],
) -> PinnedSnapshotFootprintDiagnostics {
    PinnedSnapshotFootprintDiagnostics {
        logical_manifest_payload: pinned
            .iter()
            .map(|snapshot| u128::from(snapshot.manifest_payload_bytes))
            .sum(),
        unique_manifest_payload: {
            let mut unique = BTreeMap::new();
            for snapshot in pinned {
                unique
                    .entry(snapshot.version)
                    .or_insert(snapshot.manifest_payload_bytes);
            }
            unique.values().map(|bytes| u128::from(*bytes)).sum()
        },
        logical_row_data: pinned
            .iter()
            .flat_map(|snapshot| snapshot.row_data_files.iter())
            .map(|file| u128::from(file.byte_len))
            .sum(),
        unique_row_data: unique_file_bytes(
            pinned
                .iter()
                .flat_map(|snapshot| snapshot.row_data_files.iter()),
        ),
        logical_immutable_segments: pinned
            .iter()
            .flat_map(|snapshot| snapshot.immutable_segment_files.iter())
            .map(|file| u128::from(file.byte_len))
            .sum(),
        unique_immutable_segments: unique_file_bytes(
            pinned
                .iter()
                .flat_map(|snapshot| snapshot.immutable_segment_files.iter()),
        ),
    }
}
