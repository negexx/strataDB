//! Append-only delta log for vector-index mutations. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8 and
//! `.claude/rules/vector-index.md`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::hnsw::IndexError;

/// One vector-index mutation. `Insert` enters a row-id's embedding into the
/// graph for the first time; `Tombstone` logically removes it (used for
/// DELETE and as half of an UPDATE — see the Phase 0 spec §8; no
/// `Tombstone` entries are produced by Phase 4's write path, but the type
/// and the read/replay path support it so Phase 5/6 don't need to touch
/// this module).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeltaEntry {
    Insert { row_id: u64, vector: Vec<f32> },
    Tombstone { row_id: u64 },
}

/// Writes `entries` to `path` as newline-delimited JSON (one `DeltaEntry`
/// per line) — matches this project's existing JSON-based durable format
/// (the manifest, `crates/storage::manifest`), so no new serialization
/// dependency (e.g. bincode) is introduced for a format nothing else needs
/// to be maximally compact.
///
/// # Errors
///
/// Returns an [`IndexError::Io`] if `path` can't be written, or
/// [`IndexError::Serde`] if an entry fails to serialize.
pub fn write_delta_log(path: &Path, entries: &[DeltaEntry]) -> Result<(), IndexError> {
    use std::io::Write as _;
    let mut file = std::fs::File::create(path)?;
    for entry in entries {
        let line = serde_json::to_string(entry)?;
        writeln!(file, "{line}")?;
    }
    file.sync_all()?;
    Ok(())
}

/// Reads back every entry written by [`write_delta_log`], in order.
///
/// # Errors
///
/// Returns an [`IndexError::Io`] if `path` can't be read, or
/// [`IndexError::Serde`] if a line fails to parse.
pub fn read_delta_log(path: &Path) -> Result<Vec<DeltaEntry>, IndexError> {
    let content = std::fs::read_to_string(path)?;
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(IndexError::from))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "strata-delta-log-test-{label}-{}.jsonl",
            std::process::id()
        ))
    }

    #[test]
    fn write_then_read_round_trips_insert_and_tombstone_entries() {
        let path = temp_path("round-trip");
        let entries = vec![
            DeltaEntry::Insert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            DeltaEntry::Insert {
                row_id: 1,
                vector: vec![4.0, 5.0, 6.0],
            },
            DeltaEntry::Tombstone { row_id: 0 },
        ];

        write_delta_log(&path, &entries).unwrap();
        let read_back = read_delta_log(&path).unwrap();

        assert_eq!(read_back, entries);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_of_missing_file_errors_instead_of_panicking() {
        let path = temp_path("missing");
        let result = read_delta_log(&path);
        assert!(result.is_err());
    }
}
