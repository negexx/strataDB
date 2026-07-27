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
///
/// `#[serde(deny_unknown_fields)]`: a pre-S1-W3.2 manifest's `DataFileEntry`
/// still carries a `delta_log` field (removed with no compatibility shim,
/// per the design doc §0.3 cut). Without this, `delta_log` would be silently
/// dropped by serde's default "ignore unknown fields" behavior and
/// `Manifest.segments` would default to empty via its own
/// `#[serde(default)]` — the dataset would *open*, `scan()` would return
/// rows correctly, and `vector_search()` would silently return `Ok(vec![])`
/// forever. Denying unknown fields turns that into a loud deserialization
/// error at `read_current`/`Dataset::open`, which is what the design doc
/// actually promises: a pre-migration dataset does not open.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataFileEntry {
    /// Relative to the dataset's `data/` directory.
    pub name: String,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
}

/// One immutable index segment listed in the manifest — see
/// `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
/// §3. Always an empty `Vec` on `Manifest` until S1 W3.2 starts writing
/// segments; `#[serde(default)]` on the field below and on `zone_map` here
/// both make "field absent" (a manifest written before this existed) and
/// "field present but empty" indistinguishable, which is required: an
/// absent/empty `zone_map` must always mean "must scan," never "may prune"
/// (binding invariant, see the design doc §3).
///
/// `#[serde(deny_unknown_fields)]`: unlike `DataFileEntry`, `SegmentEntry`
/// was introduced in S1 W3.1 with exactly its current field set and has
/// never had a field removed since — so no manifest this crate has ever
/// written can carry a segment entry with a field this code doesn't know
/// about, and denying unknown fields cannot reject any of them. It only
/// rejects a manifest carrying a field this code has never heard of (a
/// future field written by a newer version and then rolled back to this
/// one, or on-disk corruption/hand-editing) — the same "fail loudly instead
/// of silently dropping data" reasoning as on `DataFileEntry` above.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentEntry {
    /// Relative to the dataset's `data/` directory, e.g. `"{attempt_id:020}.seg"`.
    pub name: String,
    /// Per-segment, not per-dataset — segments are immutable and never
    /// rewritten, so a future writer must still be able to read an older
    /// segment's format.
    pub format_version: u32,
    pub vector_count: u64,
    pub dimension: u32,
    /// Inclusive.
    pub row_id_min: u64,
    /// Inclusive.
    pub row_id_max: u64,
    pub byte_len: u64,
    /// Computed and populated at commit time since S1 W4a — each commit's
    /// per-batch `ColumnStats` merged into one map covering every batch in
    /// that commit (`strata_txn::dataset::merge_zone_map_stats`); see the S1
    /// W4 design amendment §5 for the merge rule. Consumed for pruning since
    /// S1 W4b by `strata_txn::snapshot::zone_map_permits_scan` (which feeds
    /// `strata_query::should_scan_file`), called from `Snapshot::vector_search`'s
    /// predicate path via `SegmentSet::search_filtered_pruned`. An absent or
    /// empty map must still always fail safe to "must scan".
    #[serde(default)]
    pub zone_map: HashMap<String, ColumnStats>,
}

/// `#[serde(deny_unknown_fields)]`: every field `Manifest` has ever gained
/// (`tombstones`, `next_attempt_id`, `commit_time_high_water`, `segments`)
/// was added with `#[serde(default)]` and no top-level field has ever been
/// *removed* the way `DataFileEntry.delta_log` was — so every manifest this
/// crate has ever written is a subset of today's field set, and denying
/// unknown fields cannot reject any of them. It only rejects a manifest
/// carrying a field this code has never heard of (a future field written by
/// a newer version and then rolled back to this one, or on-disk
/// corruption/hand-editing) — the same "fail loudly instead of silently
/// dropping data" reasoning as on `DataFileEntry` above.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// in the manifest rather than in some per-commit artifact: a
    /// delete-only transaction has no data file (no dataset-wide fixed
    /// schema to fabricate an empty batch from) and no index segment (no
    /// vectors to build one from) to attach a tombstone record to.
    #[serde(default)]
    pub tombstones: Vec<u64>,
    /// The next filename-uniqueness "attempt id" to hand out for data/
    /// segment filenames — see `strata_txn::Dataset.write_attempt_counter`.
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
    /// The commit-order-monotone envelope of every commit's captured
    /// timestamp so far — **not** necessarily equal to the max `_timestamp`
    /// any individual row carries (see
    /// `docs/superpowers/specs/2026-07-25-s1-w2-timestamp-column-design.md`
    /// §4 for why: `write_phase` runs outside `commit_lock`, so a row's own
    /// timestamp capture and its eventual commit order can diverge under
    /// concurrency). Updated as `.max()` against each commit's own captured
    /// timestamp, inside the commit lock — which is what makes *this* field
    /// non-decreasing across versions by construction, even when a specific
    /// row's own value isn't.
    ///
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones`/`next_attempt_id`.
    #[serde(default)]
    pub commit_time_high_water: i64,
    /// Immutable index segments as of this version — see
    /// `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
    /// §3. Empty only for a dataset that has never committed a vector.
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones`/`next_attempt_id`.
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
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
            commit_time_high_water: 0,
            segments: Vec::new(),
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

    let json = serde_json::to_vec(manifest)?;
    {
        let mut tmp_file = File::create(&tmp_path)?;
        tmp_file.write_all(&json)?;
        tmp_file.sync_all()?;
        crate::chaos::chaos_checkpoint(); // tmp manifest is durable, about to rename
    }
    fs::rename(&tmp_path, &final_path)?;
    crate::chaos::chaos_checkpoint(); // renamed into place, about to fsync the directory entry

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
        tempfile::Builder::new()
            .prefix(&format!("strata-manifest-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
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
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
        };
        commit_manifest(&dir, &m0).unwrap();
        let m1 = Manifest {
            version: 1,
            data_files: vec![
                DataFileEntry {
                    name: "a.arrow".to_string(),
                    stats: HashMap::new(),
                },
                DataFileEntry {
                    name: "b.arrow".to_string(),
                    stats: HashMap::new(),
                },
            ],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
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
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
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
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
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
    fn commit_manifest_writes_compact_json_not_pretty_printed() {
        // The manifest is cumulative and re-serialized+fsynced on every
        // commit, so it's written compact (no indentation/newlines) rather
        // than pretty-printed — smaller on disk, faster to (de)serialize.
        // JSON is JSON either way, so this is purely a byte-format check.
        let dir = temp_dataset_dir("compact-json");
        let m0 = Manifest {
            version: 0,
            data_files: vec![DataFileEntry {
                name: "a.arrow".to_string(),
                stats: HashMap::new(),
            }],
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
        };
        commit_manifest(&dir, &m0).unwrap();

        let bytes = fs::read(manifest_path(&dir, 0)).unwrap();
        assert_eq!(
            bytes,
            serde_json::to_vec(&m0).unwrap(),
            "on-disk manifest bytes must match serde_json::to_vec's compact output exactly"
        );
        assert!(
            !bytes.contains(&b'\n'),
            "compact JSON must not contain newlines"
        );
        fs::remove_dir_all(&dir).ok();
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

    #[test]
    fn manifest_without_commit_time_high_water_field_deserializes_with_default_zero() {
        // Simulates a manifest written before `commit_time_high_water` existed
        // — must still deserialize, defaulting to 0, same as `next_attempt_id`
        // does for pre-that-field manifests.
        let old_json = serde_json::json!({
            "version": 0,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert_eq!(deserialized.commit_time_high_water, 0);
    }

    #[test]
    fn empty_manifest_starts_with_zero_commit_time_high_water() {
        assert_eq!(Manifest::empty().commit_time_high_water, 0);
    }

    #[test]
    fn manifest_without_segments_field_deserializes_with_default_empty() {
        let old_json = serde_json::json!({
            "version": 0,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert!(deserialized.segments.is_empty());
    }

    #[test]
    fn empty_manifest_has_no_segments() {
        assert!(Manifest::empty().segments.is_empty());
    }

    #[test]
    fn legacy_data_file_entry_with_delta_log_field_fails_to_deserialize() {
        // Simulates a manifest written before S1 W3.2 removed
        // `DataFileEntry.delta_log` (no compatibility shim, per the design
        // doc §0.3 cut). Before `deny_unknown_fields`, `delta_log` would be
        // silently dropped and `Manifest.segments` would quietly default to
        // empty via its own `#[serde(default)]` — the dataset would open,
        // `scan()` would work, and `vector_search()` would silently return
        // `Ok(vec![])` forever. This must instead be a loud deserialization
        // error.
        let legacy_json = serde_json::json!({
            "name": "a.arrow",
            "stats": {},
            "delta_log": "d.deltalog",
        });
        let result: std::result::Result<DataFileEntry, _> = serde_json::from_value(legacy_json);
        assert!(
            result.is_err(),
            "a legacy DataFileEntry with a delta_log field must fail to deserialize, not silently drop it"
        );
    }
}
