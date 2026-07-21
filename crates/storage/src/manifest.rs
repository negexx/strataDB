//! Manifest & versioning, per
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md` §3.4 and §6.
//!
//! A manifest is one immutable file per version, named so lexicographic
//! order equals numeric order (`{version:020}.manifest`, following Lance's
//! own convention). Commit is: write to a temp name, fsync, atomically
//! rename into place. A crash mid-write leaves only a `.tmp-*` file, which
//! never matches the `*.manifest` glob `read_current` looks for — so a
//! reader can never observe a partially-written version. This *is* the
//! mechanism the Phase 1 "kill -9 mid-write, restart, recover last
//! committed version" MVP checklist item tests.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::stats::ColumnStats;

/// One committed data file's name and the per-column statistics computed
/// for it at commit time — see
/// `.claude/docs/design/phase-3-query-refinement-spec.md` §1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
    /// Relative to the dataset's `data/` directory. This commit's vector-
    /// index delta-log entries — see
    /// `.claude/docs/design/phase-4-vector-index-spec.md` §2.
    pub delta_log: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Accumulated across every committed version so far.
    pub data_files: Vec<DataFileEntry>,
    /// The row-id to assign to the next inserted row, dataset-wide. Never
    /// resets, never reused — see
    /// `.claude/docs/design/phase-0-transaction-and-format-spec.md` §8.
    pub next_row_id: u64,
    /// Row-ids tombstoned (deleted, or superseded by `update`) as of this
    /// version. Accumulated across every committed version, same as
    /// `data_files` — see Phase 6's design doc for why this lives directly
    /// in the manifest rather than a delta-log file: a delete-only
    /// transaction has no data file to attach one to, since there is no
    /// dataset-wide fixed schema to fabricate an empty batch from.
    #[serde(default)]
    pub tombstones: Vec<u64>,
    /// The next filename-uniqueness "attempt id" to hand out for data/
    /// delta-log filenames — see `strata_txn::Dataset.write_attempt_counter`.
    /// Persisted (rather than always restarting at 0) so that
    /// `Dataset::open` never regenerates a filename a prior session already
    /// committed: `write_batch` truncates via `File::create`, so a filename
    /// collision across sessions would silently destroy already-durable
    /// data. Analogous to `next_row_id` (never resets, never reused), but
    /// this counter identifies filename-uniqueness attempts rather than row
    /// identity — see `.claude/docs/design/phase-0-transaction-and-format-spec.md`
    /// §8 for `next_row_id`'s parallel contract.
    ///
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones` above.
    #[serde(default)]
    pub next_attempt_id: u64,
}

impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        }
    }
}

fn versions_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join("_versions")
}

fn manifest_path(dataset_dir: &Path, version: u64) -> PathBuf {
    versions_dir(dataset_dir).join(format!("{version:020}.manifest"))
}

/// Durably and atomically commits `manifest` as the new current version.
/// Never call this twice concurrently for the same `dataset_dir` from
/// separate writers in Phase 1 — there is no conflict detection yet (single
/// writer only); see `crates/txn`.
///
/// # Errors
///
/// Returns an error if the `_versions/` directory can't be created, if the
/// manifest can't be serialized or written, or if the atomic rename fails.
pub fn commit_manifest(dataset_dir: &Path, manifest: &Manifest) -> Result<()> {
    let versions = versions_dir(dataset_dir);
    fs::create_dir_all(&versions)?;

    let final_path = manifest_path(dataset_dir, manifest.version);
    let tmp_path = versions.join(format!(".tmp-{}", manifest.version));

    let json = serde_json::to_vec_pretty(manifest)?;
    {
        let mut tmp_file = File::create(&tmp_path)?;
        tmp_file.write_all(&json)?;
        tmp_file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;

    // fsync the containing directory so the rename itself survives a crash,
    // not just the file content — see `crate::datafile::sync_dir`. Not fatal
    // if unsupported on this platform, since `rename()` on both POSIX and
    // NTFS is itself atomic; the worst case without this is a rename that
    // completed but whose *durability* is unconfirmed on an immediate power
    // loss, not a torn or partially-visible write.
    crate::datafile::sync_dir(&versions)?;

    Ok(())
}

/// Returns the highest committed version's manifest, or `None` if the
/// dataset has never been committed to. This is the entire crash-recovery
/// mechanism: it only ever sees fully-renamed `*.manifest` files.
///
/// # Errors
///
/// Returns an error if `_versions/` can't be listed, or if the highest
/// numbered `*.manifest` file exists but fails to read or parse — a
/// genuinely corrupt manifest, not a crash-in-progress one (see the module
/// doc comment for why those are distinguishable).
pub fn read_current(dataset_dir: &Path) -> Result<Option<Manifest>> {
    let versions = versions_dir(dataset_dir);
    if !versions.exists() {
        return Ok(None);
    }

    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(&versions)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".manifest") else {
            continue;
        };
        let Ok(version) = stem.parse::<u64>() else {
            continue;
        };
        let is_newer = best.as_ref().is_none_or(|(v, _)| version > *v);
        if is_newer {
            best = Some((version, path));
        }
    }

    let Some((_, path)) = best else {
        return Ok(None);
    };
    let bytes = fs::read(&path)?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|e| StorageError::CorruptManifest(path.clone(), e.to_string()))?;
    Ok(Some(manifest))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::stats::Value;

    fn temp_dataset_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "strata-manifest-test-{label}-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn read_current_is_none_for_fresh_dataset() {
        let dir = temp_dataset_dir("fresh");
        assert!(read_current(&dir).unwrap().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_then_read_current_round_trips() {
        let dir = temp_dataset_dir("roundtrip");
        let m0 = Manifest {
            version: 0,
            data_files: vec![DataFileEntry {
                name: "a.arrow".to_string(),
                stats: HashMap::new(),
                delta_log: "d.deltalog".to_string(),
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        commit_manifest(&dir, &m0).unwrap();
        let m1 = Manifest {
            version: 1,
            data_files: vec![
                DataFileEntry {
                    name: "a.arrow".to_string(),
                    stats: HashMap::new(),
                    delta_log: "d.deltalog".to_string(),
                },
                DataFileEntry {
                    name: "b.arrow".to_string(),
                    stats: HashMap::new(),
                    delta_log: "d.deltalog".to_string(),
                },
            ],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        commit_manifest(&dir, &m1).unwrap();

        let current = read_current(&dir).unwrap().unwrap();
        assert_eq!(current, m1);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn leftover_tmp_file_is_never_picked_up_as_current() {
        // Simulates a crash mid-commit: a .tmp-* file exists but was never
        // renamed into place. This is the actual crash-safety property the
        // MVP's kill-9 checklist item depends on.
        let dir = temp_dataset_dir("crash-sim");
        let m0 = Manifest {
            version: 0,
            data_files: vec![DataFileEntry {
                name: "a.arrow".to_string(),
                stats: HashMap::new(),
                delta_log: "d.deltalog".to_string(),
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };
        commit_manifest(&dir, &m0).unwrap();

        let versions = versions_dir(&dir);
        let mut tmp = File::create(versions.join(".tmp-1")).unwrap();
        tmp.write_all(b"{ incomplete json").unwrap();

        let current = read_current(&dir).unwrap().unwrap();
        assert_eq!(
            current, m0,
            "leftover .tmp file must not be treated as current"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn genuinely_corrupt_manifest_errors_instead_of_panicking() {
        // Unlike the .tmp-* case above, this simulates real on-disk
        // corruption: a *fully-renamed* manifest (so it matches the
        // `*.manifest` glob `read_current` looks for) whose content is
        // invalid JSON. This must surface as a typed error, not a panic or
        // a silently-wrong "current" version.
        let dir = temp_dataset_dir("corrupt");
        let versions = versions_dir(&dir);
        fs::create_dir_all(&versions).unwrap();
        let mut file = File::create(manifest_path(&dir, 0)).unwrap();
        file.write_all(b"not valid json").unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, _))),
            "expected a CorruptManifest error, got {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_then_read_current_with_populated_stats() {
        // Exercises serde round-trip of populated ColumnStats (all three Value
        // variants: Int64, Float64, Utf8) through the actual file-based
        // commit_manifest/read_current path — not just in-memory equality.
        let dir = temp_dataset_dir("stats-roundtrip");

        // Build stats with one entry per Value variant.
        let mut stats = HashMap::new();
        stats.insert(
            "id_col".to_string(),
            ColumnStats {
                min: Value::Int64(100),
                max: Value::Int64(500),
            },
        );
        stats.insert(
            "price_col".to_string(),
            ColumnStats {
                min: Value::Float64(9.99),
                max: Value::Float64(99.99),
            },
        );
        stats.insert(
            "name_col".to_string(),
            ColumnStats {
                min: Value::Utf8("alice".to_string()),
                max: Value::Utf8("zoe".to_string()),
            },
        );

        let m0 = Manifest {
            version: 0,
            data_files: vec![DataFileEntry {
                name: "data.arrow".to_string(),
                stats,
                delta_log: "d.deltalog".to_string(),
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
        };

        commit_manifest(&dir, &m0).unwrap();
        let current = read_current(&dir).unwrap().unwrap();
        assert_eq!(
            current, m0,
            "populated stats must round-trip correctly through commit/read"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_skips_a_manifest_suffixed_file_with_a_non_numeric_stem() {
        let dir = temp_dataset_dir("garbage-stem");
        let versions = versions_dir(&dir);
        fs::create_dir_all(&versions).unwrap();
        let mut garbage = File::create(versions.join("not-a-number.manifest")).unwrap();
        garbage.write_all(b"{}").unwrap();

        // No real manifest exists at all - the garbage-stemmed file must be
        // silently skipped, not picked as current and not erroring.
        let current = read_current(&dir).unwrap();
        assert!(
            current.is_none(),
            "a garbage-stemmed *.manifest file must never be treated as current"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_is_none_when_versions_dir_has_only_a_leftover_tmp_file() {
        // Simulates a crash during the *very first* commit, before any
        // version was ever successfully renamed into place - unlike
        // leftover_tmp_file_is_never_picked_up_as_current, `best` must stay
        // None all the way through, not just fall back to an earlier real
        // version.
        let dir = temp_dataset_dir("only-tmp-file");
        let versions = versions_dir(&dir);
        fs::create_dir_all(&versions).unwrap();
        let mut tmp = File::create(versions.join(".tmp-0")).unwrap();
        tmp.write_all(b"{ incomplete json").unwrap();

        let current = read_current(&dir).unwrap();
        assert!(
            current.is_none(),
            "a versions/ directory containing only a leftover .tmp file must read as fresh, not current"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_manifest_has_no_tombstones() {
        let manifest = Manifest::empty();
        assert!(manifest.tombstones.is_empty());
    }

    #[test]
    fn manifest_with_tombstones_round_trips_through_json() {
        let mut manifest = Manifest::empty();
        manifest.tombstones = vec![3, 7, 12];
        let json = serde_json::to_vec(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.tombstones, vec![3, 7, 12]);
    }

    #[test]
    fn empty_manifest_has_zero_next_attempt_id() {
        let manifest = Manifest::empty();
        assert_eq!(manifest.next_attempt_id, 0);
    }

    #[test]
    fn manifest_with_next_attempt_id_round_trips_through_json() {
        let mut manifest = Manifest::empty();
        manifest.next_attempt_id = 42;
        let json = serde_json::to_vec(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.next_attempt_id, 42);
    }

    #[test]
    fn manifest_without_next_attempt_id_field_deserializes_with_default_zero() {
        // Simulates a manifest written to disk before `next_attempt_id`
        // existed — must still deserialize, defaulting to 0, same as
        // `tombstones` does for pre-tombstone manifests.
        let old_json = serde_json::json!({
            "version": 0,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert_eq!(deserialized.next_attempt_id, 0);
    }
}
