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
/// Returns [`IndexError::Io`] if `path` can't be created, `sync_all`-ed, or
/// if `write_all`'s newline write fails outright. Note that a write failure
/// that occurs *while `serde_json` is writing an entry* (e.g. disk full
/// partway through a flush the serializer triggers internally) surfaces as
/// [`IndexError::Serde`] instead, since `serde_json::to_writer` wraps the
/// underlying `io::Error` in its own `Error` type — not a bare I/O failure
/// from this function's own perspective, even though the root cause is I/O.
/// Either error type is handled identically by every caller today (both
/// convert to `TxnError::Index` in `crates/txn`), so this only matters if a
/// caller ever starts branching on the specific variant.
pub fn write_delta_log(path: &Path, entries: &[DeltaEntry]) -> Result<(), IndexError> {
    use std::io::Write as _;
    let file = std::fs::File::create(path)?;
    // One writeln! per entry against a raw File is one syscall per entry; a
    // BufWriter coalesces them into 64 KiB writes.
    let mut writer = std::io::BufWriter::with_capacity(64 * 1024, file);
    for entry in entries {
        serde_json::to_writer(&mut writer, entry)?;
        writer.write_all(b"\n")?;
    }
    // Ordering is load-bearing: `into_inner` flushes the userspace buffer
    // into the OS *before* the fsync — `sync_all()` only flushes OS-level
    // buffers and would silently miss unflushed userspace bytes otherwise.
    let file = writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?;
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
        tempfile::Builder::new()
            .prefix(&format!("strata-delta-log-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
            .join("delta.jsonl")
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
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn write_delta_log_produces_compact_newline_delimited_json_with_no_trailing_whitespace() {
        // The round-trip test alone can't catch a whitespace-level format
        // regression: read_delta_log's serde_json::from_str tolerates
        // trailing whitespace on a line, so e.g. an accidental
        // `{...}   \n` would still round-trip green. This asserts the exact
        // on-disk bytes instead, mirroring the sibling byte-format check in
        // crates/storage/src/manifest.rs
        // (commit_manifest_writes_compact_json_not_pretty_printed).
        let path = temp_path("byte-format");
        let entries = vec![
            DeltaEntry::Insert {
                row_id: 0,
                vector: vec![1.0, 2.0, 3.0],
            },
            DeltaEntry::Tombstone { row_id: 1 },
        ];

        write_delta_log(&path, &entries).unwrap();
        let bytes = std::fs::read(&path).unwrap();

        let expected = format!(
            "{}\n{}\n",
            serde_json::to_string(&entries[0]).unwrap(),
            serde_json::to_string(&entries[1]).unwrap(),
        );
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            expected,
            "expected compact JSON, one entry per line, no extra whitespace"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn read_of_missing_file_errors_instead_of_panicking() {
        let path = temp_path("missing");
        let result = read_delta_log(&path);
        assert!(result.is_err());
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }
}
