//! Manifest & versioning, per
//! `docs/design.md`.
//!
//! A manifest is one immutable file per version, named so lexicographic
//! order equals numeric order (`{version:020}.manifest`, following Lance's
//! own convention). Commit is: write to a temp name (via
//! [`crate::backend::LocalFs::put`]), fsync, atomically rename into place.
//! A crash mid-write leaves only a `.tmp-*` file behind. Its stem (the
//! part before `.manifest`) always starts with a `.` from the temp-name
//! prefix, so it can never parse as a `u64` version — `read_current`
//! excludes it on that basis. The leftover tmp file's name does still end
//! in `.manifest` (it's derived from the target filename), so this is a
//! single-guarded exclusion (numeric-parse failure), not a
//! `*.manifest`-glob mismatch — but a reader still can never observe a
//! partially-written version either way. This *is* the mechanism the
//! Phase 1 "kill -9 mid-write, restart, recover last committed version"
//! MVP checklist item tests.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, LocalFs};
use crate::error::{Result, StorageError};
use crate::stats::ColumnStats;

/// The version of the manifest envelope, deliberately distinct from a
/// manifest's commit [`Manifest::version`].
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// One committed data file's name and the per-column statistics computed
/// for it at commit time — see `docs/design.md`.
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
    /// Length of the complete Arrow IPC file, in bytes.
    pub byte_len: u64,
    /// CRC32C of the complete Arrow IPC file.
    pub crc32c: u32,
    /// Number of physical rows in this file.
    pub row_count: u64,
    /// Inclusive physical row-id range, absent exactly for an empty file.
    pub row_id_range: Option<(u64, u64)>,
    /// Column name -> stats. Absent key means "no stats for this column in
    /// this file" (non-orderable type, or all-null) — never a wrong entry.
    pub stats: HashMap<String, ColumnStats>,
}

/// One immutable index segment listed in the manifest — see
/// `docs/design.md`. The segment list is empty only when no vector-bearing commit is
/// visible; `#[serde(default)]` on the field below and on `zone_map` here
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
    /// Raw Arrow IPC schema-message bytes for the dataset's owned logical
    /// schema. The physical `_row_id` and `_timestamp` columns are not part
    /// of this schema.
    pub schema_ipc: Vec<u8>,
    /// Accumulated across every committed version so far.
    pub data_files: Vec<DataFileEntry>,
    /// The row-id to assign to the next inserted row, dataset-wide. Never
    /// resets, never reused — see
    /// `docs/design.md`.
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
    /// identity — see `docs/design.md` for `next_row_id`'s parallel
    /// contract.
    ///
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones` above.
    #[serde(default)]
    pub next_attempt_id: u64,
    /// The commit-order-monotone envelope of every commit's captured
    /// timestamp so far — **not** necessarily equal to the max `_timestamp`
    /// any individual row carries (see
    /// `docs/design.md` for why: `write_phase` runs outside `commit_lock`, so a row's own
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
    /// `docs/design.md`. Empty only when no vector-bearing commit is visible.
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones`/`next_attempt_id`.
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    #[must_use]
    pub fn empty() -> Self {
        Self::empty_with_schema(&Schema::empty())
    }

    #[must_use]
    pub fn empty_with_schema(schema: &Schema) -> Self {
        Self {
            version: 0,
            schema_ipc: encode_schema(schema),
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            segments: Vec::new(),
        }
    }

    /// Decodes the dataset-owned logical schema from its persisted IPC
    /// message bytes. The caller supplies the manifest path so corruption is
    /// reported against the durable artifact that carried it.
    /// Decodes the Arrow schema owned by this manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted IPC schema bytes are malformed.
    pub fn schema(&self, manifest_path: &Path) -> Result<SchemaRef> {
        if self.schema_ipc.is_empty() {
            return Err(StorageError::LegacyFormatNeedsMigration(
                manifest_path.to_path_buf(),
            ));
        }
        let schema = try_schema_from_flatbuffer_bytes(&self.schema_ipc).map_err(|error| {
            StorageError::CorruptManifest(
                manifest_path.to_path_buf(),
                format!("schema_ipc cannot be decoded: {error}"),
            )
        })?;
        Ok(std::sync::Arc::new(schema))
    }
}

/// The exact durable representation of a manifest file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestEnvelope {
    pub format_version: u32,
    pub manifest: Manifest,
    pub checksum: u32,
}

impl ManifestEnvelope {
    fn new(manifest: Manifest) -> Result<Self> {
        let mut envelope = Self {
            format_version: MANIFEST_FORMAT_VERSION,
            manifest,
            checksum: 0,
        };
        envelope.checksum = envelope.canonical_checksum()?;
        Ok(envelope)
    }

    fn canonical_checksum(&self) -> Result<u32> {
        Ok(crc32c::crc32c(&canonical_envelope_bytes(self)?))
    }

    fn validate(&self, path: &Path, filename_version: u64) -> Result<()> {
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(StorageError::CorruptManifest(
                path.to_path_buf(),
                format!(
                    "format_version {} is unsupported; expected {MANIFEST_FORMAT_VERSION}",
                    self.format_version
                ),
            ));
        }
        let expected_checksum = self.canonical_checksum()?;
        if self.checksum != expected_checksum {
            return Err(StorageError::CorruptManifest(
                path.to_path_buf(),
                format!(
                    "checksum {} does not match canonical payload checksum {expected_checksum}",
                    self.checksum
                ),
            ));
        }
        if self.manifest.version != filename_version {
            return Err(StorageError::CorruptManifest(
                path.to_path_buf(),
                format!(
                    "filename version {filename_version} does not match payload version {}",
                    self.manifest.version
                ),
            ));
        }
        Ok(())
    }
}

fn encode_schema(schema: &Schema) -> Vec<u8> {
    let generator = IpcDataGenerator::default();
    let mut dictionaries = DictionaryTracker::new(true);
    generator
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionaries,
            &IpcWriteOptions::default(),
        )
        .ipc_message
}

fn canonical_envelope_bytes(envelope: &ManifestEnvelope) -> Result<Vec<u8>> {
    let mut zeroed = envelope.clone();
    zeroed.checksum = 0;
    let value = serde_json::to_value(zeroed)?;
    serde_json::to_vec(&canonicalize_json(value)).map_err(StorageError::from)
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(values) => {
            let sorted: BTreeMap<_, _> = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        other => other,
    }
}

#[cfg(test)]
fn versions_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join("_versions")
}

#[cfg(test)]
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
    let backend = LocalFs::new(dataset_dir);
    let key = format!("_versions/{:020}.manifest", manifest.version);
    let json = serde_json::to_vec(&ManifestEnvelope::new(manifest.clone())?)?;
    // `LocalFs::put` fsyncs the containing directory internally (see Task
    // 1), so there is no separate `sync_dir` call here the way the
    // pre-Backend code had one -- folding that step into `put` itself
    // (rather than leaving it a caller-remembered step) is what makes
    // `Backend::put`'s durability contract self-contained. Do not add a
    // second explicit `sync_dir` call here: `versions_dir(dataset_dir)` is
    // exactly the directory `put` already fsyncs for this key, so a second
    // call would double a chaos checkpoint and break the "checkpoint count
    // unchanged" global constraint below.
    backend.put(&key, &json)?;
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
    Ok(read_current_with_byte_count(dataset_dir)?.map(|(manifest, _)| manifest))
}

/// Reads and validates one durable manifest version, returning its payload
/// and exact serialized byte count.
///
/// # Errors
///
/// Returns an error if the versioned manifest is missing, malformed, has an
/// unsupported format/checksum/version, or carries an invalid schema.
pub fn read_manifest_with_byte_count(dataset_dir: &Path, version: u64) -> Result<(Manifest, u64)> {
    let backend = LocalFs::new(dataset_dir);
    let key = format!("_versions/{version:020}.manifest");
    let bytes = backend.get(&key)?;
    decode_manifest_with_byte_count(dataset_dir, &key, version, &bytes)
}

fn decode_manifest_with_byte_count(
    dataset_dir: &Path,
    key: &str,
    version: u64,
    bytes: &[u8],
) -> Result<(Manifest, u64)> {
    let path = dataset_dir.join(key);
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| StorageError::CorruptManifest(path.clone(), error.to_string()))?;
    if !value
        .as_object()
        .is_some_and(|object| object.contains_key("format_version"))
    {
        return Err(StorageError::LegacyFormatNeedsMigration(path));
    }
    let envelope: ManifestEnvelope = serde_json::from_value(value)
        .map_err(|error| StorageError::CorruptManifest(path.clone(), error.to_string()))?;
    envelope.validate(&path, version)?;
    envelope.manifest.schema(&path)?;
    let byte_count = u64::try_from(bytes.len()).map_err(|error| {
        StorageError::CorruptManifest(
            path.clone(),
            format!("manifest byte count overflowed u64: {error}"),
        )
    })?;
    Ok((envelope.manifest, byte_count))
}

/// Returns the highest committed manifest together with the exact number of
/// manifest bytes loaded to validate it.
///
/// This is a side-effect-free diagnostic companion to [`read_current`]. The
/// count covers the one fully-renamed current manifest selected by recovery,
/// not directory listings or older retained manifests that are not loaded.
///
/// # Errors
///
/// As [`read_current`], if recovery cannot list, load, parse, or validate the
/// selected manifest.
pub fn read_current_with_byte_count(dataset_dir: &Path) -> Result<Option<(Manifest, u64)>> {
    let backend = LocalFs::new(dataset_dir);

    let mut best: Option<(u64, String)> = None;
    for meta in backend.list("_versions/")? {
        let Some(stem) = meta
            .key
            .strip_prefix("_versions/")
            .and_then(|s| s.strip_suffix(".manifest"))
        else {
            continue;
        };
        let Ok(version) = stem.parse::<u64>() else {
            continue;
        };
        let is_newer = best.as_ref().is_none_or(|(v, _)| version > *v);
        if is_newer {
            best = Some((version, meta.key.clone()));
        }
    }

    let Some((filename_version, key)) = best else {
        return Ok(None);
    };
    let bytes = backend.get(&key)?;
    decode_manifest_with_byte_count(dataset_dir, &key, filename_version, &bytes).map(Some)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write as _;

    use crate::stats::Value;

    fn temp_dataset_dir(label: &str) -> PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-manifest-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    fn data_file(name: &str) -> DataFileEntry {
        DataFileEntry {
            name: name.to_string(),
            byte_len: 0,
            crc32c: 0,
            row_count: 0,
            row_id_range: None,
            stats: HashMap::new(),
        }
    }

    fn manifest(version: u64, data_files: Vec<DataFileEntry>) -> Manifest {
        let mut manifest = Manifest::empty();
        manifest.version = version;
        manifest.data_files = data_files;
        manifest
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
        let m0 = manifest(0, vec![data_file("a.arrow")]);
        commit_manifest(&dir, &m0).unwrap();
        let m1 = manifest(1, vec![data_file("a.arrow"), data_file("b.arrow")]);
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
        let m0 = manifest(0, vec![data_file("a.arrow")]);
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
    fn leftover_tmp_file_with_the_real_localfs_naming_shape_is_never_picked_up_as_current() {
        // Unlike `leftover_tmp_file_is_never_picked_up_as_current` above
        // (which uses the pre-Backend `.tmp-{version}` naming and so only
        // exercises the suffix-mismatch path), this uses the actual shape
        // `LocalFs::tmp_path_for` produces: `.tmp-{pid}-{n}-{file_name}`,
        // where `file_name` is the target's own filename -- so this tmp
        // file's name *does* end in `.manifest`. Exclusion here rests
        // entirely on the stem (`.tmp-1234-0-...`) never parsing as a u64.
        let dir = temp_dataset_dir("real-tmp-shape");
        let m0 = manifest(0, vec![data_file("a.arrow")]);
        commit_manifest(&dir, &m0).unwrap();

        let versions = versions_dir(&dir);
        let mut tmp = File::create(versions.join(format!(
            ".tmp-{}-0-{:020}.manifest",
            std::process::id(),
            1
        )))
        .unwrap();
        tmp.write_all(b"{ incomplete json").unwrap();

        let current = read_current(&dir).unwrap().unwrap();
        assert_eq!(
            current, m0,
            "a leftover tmp file using LocalFs's real naming shape must not be treated as current"
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

        let m0 = manifest(
            0,
            vec![DataFileEntry {
                stats,
                ..data_file("data.arrow")
            }],
        );

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
        let m0 = manifest(0, vec![data_file("a.arrow")]);
        commit_manifest(&dir, &m0).unwrap();

        let bytes = fs::read(manifest_path(&dir, 0)).unwrap();
        assert_eq!(
            bytes,
            serde_json::to_vec(&ManifestEnvelope::new(m0.clone()).unwrap()).unwrap(),
            "on-disk manifest bytes must match the compact checksum envelope exactly"
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
            "schema_ipc": Manifest::empty().schema_ipc,
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
            "schema_ipc": Manifest::empty().schema_ipc,
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
            "schema_ipc": Manifest::empty().schema_ipc,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert!(deserialized.segments.is_empty());
    }

    #[test]
    fn committed_manifest_uses_the_versioned_checksum_envelope() {
        let dir = temp_dataset_dir("versioned-envelope");
        let manifest = Manifest::empty();

        commit_manifest(&dir, &manifest).unwrap();

        let bytes = fs::read(manifest_path(&dir, manifest.version)).unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            envelope
                .get("format_version")
                .and_then(serde_json::Value::as_u64),
            Some(u64::from(MANIFEST_FORMAT_VERSION)),
            "a manifest file must carry the distinct on-disk format version"
        );
        assert!(
            envelope.get("manifest").is_some(),
            "the payload must be nested in the envelope so its checksum covers it"
        );
        assert!(
            envelope.get("checksum").is_some(),
            "the envelope must carry its canonical JSON checksum"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_a_manifest_with_a_mutated_checksum() {
        // Break caught: accepting a manifest after its envelope checksum has
        // changed would make recovery trust an unverified catalog payload.
        let dir = temp_dataset_dir("mutated-checksum");
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest).unwrap();
        let path = manifest_path(&dir, manifest.version);
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let checksum = envelope["checksum"].as_u64().unwrap();
        envelope["checksum"] = serde_json::Value::from(checksum + 1);
        fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref reason)) if reason.contains("checksum")),
            "recovery must reject a manifest whose checksum no longer matches its canonical payload: {result:?}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_an_unsupported_manifest_format_version() {
        // Break caught: opening a newer envelope as if it were this format
        // could silently reinterpret fields that this reader does not know.
        let dir = temp_dataset_dir("unsupported-format-version");
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest).unwrap();
        let path = manifest_path(&dir, manifest.version);
        let mut envelope: ManifestEnvelope =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        envelope.format_version = MANIFEST_FORMAT_VERSION + 1;
        envelope.checksum = 0;
        envelope.checksum = envelope.canonical_checksum().unwrap();
        fs::write(path, serde_json::to_vec(&envelope).unwrap()).unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref reason)) if reason.contains("format_version") && reason.contains("unsupported")),
            "recovery must reject an unsupported manifest envelope version: {result:?}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn manifest_checksum_is_independent_of_map_insertion_order() {
        // Break caught: serializing HashMap iteration order directly would
        // make otherwise identical manifests produce unstable checksums.
        let mut left_stats = HashMap::new();
        left_stats.insert(
            "z".to_string(),
            ColumnStats {
                min: Value::Int64(1),
                max: Value::Int64(2),
            },
        );
        left_stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(3),
                max: Value::Int64(4),
            },
        );
        let mut right_stats = HashMap::new();
        right_stats.insert(
            "a".to_string(),
            ColumnStats {
                min: Value::Int64(3),
                max: Value::Int64(4),
            },
        );
        right_stats.insert(
            "z".to_string(),
            ColumnStats {
                min: Value::Int64(1),
                max: Value::Int64(2),
            },
        );

        let left = manifest(
            0,
            vec![DataFileEntry {
                stats: left_stats,
                ..data_file("rows.arrow")
            }],
        );
        let right = manifest(
            0,
            vec![DataFileEntry {
                stats: right_stats,
                ..data_file("rows.arrow")
            }],
        );
        let left_envelope = ManifestEnvelope::new(left).unwrap();
        let right_envelope = ManifestEnvelope::new(right).unwrap();

        assert_eq!(left_envelope.checksum, right_envelope.checksum);
        assert_eq!(
            canonical_envelope_bytes(&left_envelope).unwrap(),
            canonical_envelope_bytes(&right_envelope).unwrap(),
            "canonical JSON must recursively sort map keys before checksumming"
        );
    }

    #[test]
    fn read_current_rejects_a_filename_payload_version_mismatch() {
        let dir = temp_dataset_dir("filename-payload-mismatch");
        let manifest = Manifest::empty();
        commit_manifest(&dir, &manifest).unwrap();
        fs::copy(manifest_path(&dir, 0), manifest_path(&dir, 1)).unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref reason)) if reason.contains("filename version")),
            "recovery must reject a manifest whose filename and payload version disagree: {result:?}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_a_legacy_unenveloped_manifest() {
        let dir = temp_dataset_dir("legacy-unenveloped");
        let versions = versions_dir(&dir);
        fs::create_dir_all(&versions).unwrap();
        fs::write(
            manifest_path(&dir, 0),
            serde_json::to_vec(&Manifest::empty()).unwrap(),
        )
        .unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(result, Err(StorageError::LegacyFormatNeedsMigration(_))),
            "the old direct-manifest representation must not open as a verified dataset: {result:?}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_manifest_has_no_segments() {
        assert!(Manifest::empty().segments.is_empty());
    }

    #[test]
    fn current_recovery_decodes_the_exact_selected_manifest_key() {
        let dir = temp_dataset_dir("exact-selected-key");
        let manifest = manifest(7, vec![data_file("rows.arrow")]);
        commit_manifest(&dir, &manifest).unwrap();
        fs::rename(
            manifest_path(&dir, 7),
            versions_dir(&dir).join("7.manifest"),
        )
        .unwrap();

        let (recovered, bytes) = read_current_with_byte_count(&dir).unwrap().unwrap();

        assert_eq!(recovered, manifest);
        assert_eq!(
            bytes,
            fs::metadata(versions_dir(&dir).join("7.manifest"))
                .unwrap()
                .len()
        );
        fs::remove_dir_all(&dir).ok();
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
