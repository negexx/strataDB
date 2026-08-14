//! Facade-owned metadata views.
//!
//! These DTOs keep the supported transaction API independent from the
//! storage crate's manifest representation. The legacy raw accessors remain
//! available for compatibility, but new callers should use these views.

use strata_storage::{DataFileEntry, SegmentEntry};

/// Stable, read-only description of one committed row-data file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileInfo {
    pub name: String,
    pub byte_len: u64,
    pub crc32c: u32,
    pub row_count: u64,
    pub row_id_range: Option<(u64, u64)>,
}

/// Stable, read-only description of one immutable vector segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfo {
    pub name: String,
    pub format_version: u32,
    pub vector_count: u64,
    pub dimension: u32,
    pub row_id_min: u64,
    pub row_id_max: u64,
    pub byte_len: u64,
}

impl DataFileInfo {
    pub(crate) fn from_entry(entry: &DataFileEntry) -> Self {
        Self {
            name: entry.name.clone(),
            byte_len: entry.byte_len,
            crc32c: entry.crc32c,
            row_count: entry.row_count,
            row_id_range: entry.row_id_range,
        }
    }
}

impl SegmentInfo {
    pub(crate) fn from_entry(entry: &SegmentEntry) -> Self {
        Self {
            name: entry.name.clone(),
            format_version: entry.format_version,
            vector_count: entry.vector_count,
            dimension: entry.dimension,
            row_id_min: entry.row_id_min,
            row_id_max: entry.row_id_max,
            byte_len: entry.byte_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn dto_conversion_does_not_expose_storage_stats() {
        let entry = DataFileEntry {
            name: "0001.arrow".to_owned(),
            byte_len: 42,
            crc32c: 7,
            row_count: 3,
            row_id_range: Some((10, 12)),
            stats: HashMap::new(),
        };
        assert_eq!(
            DataFileInfo::from_entry(&entry),
            DataFileInfo {
                name: "0001.arrow".to_owned(),
                byte_len: 42,
                crc32c: 7,
                row_count: 3,
                row_id_range: Some((10, 12)),
            }
        );
    }
}
