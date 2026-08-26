//! Manifest & versioning, per
//! `docs/design.md`.
//!
//! A manifest is one immutable file per version, named so lexicographic
//! order equals numeric order (`{version:020}.manifest`, following Lance's
//! own convention). Commit is: write to a temp name, fsync it, publish the
//! final name with `StorageOwner::put_if_absent` (the local backend uses a
//! hard link), then synchronize the bounded directory chain from `_versions`
//! through the dataset root. A final-name candidate can be readable if a
//! directory sync fails; this is a verified-visible indeterminate
//! publication, not a durability acknowledgement.
//! A crash mid-write leaves only a `.tmp-*` file behind. Its stem (the
//! part before `.manifest`) always starts with a `.` from the temp-name
//! prefix, so it can never parse as a `u64` version — `read_current`
//! excludes it on that basis. The leftover tmp file's name does still end
//! in `.manifest` (it's derived from the target filename), so this is a
//! single-guarded exclusion (numeric-parse failure), not a
//! `*.manifest`-glob mismatch — but a reader still can never observe a
//! partially-written version either way. This *is* the mechanism the
//! Phase 1 "kill -9 mid-write, restart, recover the newest readable final-name
//! version" MVP checklist item tests.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::schema::{INITIAL_SCHEMA_VERSION, validate_schema_version};
use crate::stats::ColumnStats;

/// The version of the manifest envelope, deliberately distinct from a
/// manifest's commit [`Manifest::version`].
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Maximum encoded manifest size accepted during recovery. This bounds the
/// first allocation made by JSON parsing for untrusted on-disk input.
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
/// `serde_json`'s default recursion guard is intentionally made explicit here
/// so the format's recovery bound is visible and testable.
const MAX_MANIFEST_JSON_DEPTH: usize = 128;
const MAX_MANIFEST_OBJECT_FIELDS: usize = 4_096;
const MAX_MANIFEST_STRING_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_ARRAY_ITEMS: usize = 1_000_000;
const MAX_MANIFEST_DATA_FILES: usize = 900_000;
const MAX_MANIFEST_SEGMENTS: usize = 1_000_000;
const MAX_MANIFEST_TOMBSTONES: usize = 10_000_000;
const MAX_MANIFEST_SCHEMA_IPC_BYTES: usize = 16 * 1024 * 1024;

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
    /// Version of the durable logical schema catalog used by these row and
    /// vector object references. `None` denotes a format-v1 envelope written
    /// before schema catalog metadata existed; it is interpreted as the
    /// initial catalog without changing that envelope's checksum bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
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
    /// Timestamp captured for this manifest publication, in Unix microseconds.
    /// Legacy manifests default to zero and are retained conservatively by
    /// age-based cleanup.
    #[serde(default)]
    pub committed_at_us: i64,
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
            schema_version: Some(INITIAL_SCHEMA_VERSION),
            schema_ipc: encode_schema(schema),
            data_files: Vec::new(),
            next_row_id: 0,
            tombstones: Vec::new(),
            next_attempt_id: 0,
            commit_time_high_water: 0,
            committed_at_us: 0,
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
        validate_schema_version(self.schema_version(), Some(manifest_path))?;
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

    /// Replaces the logical schema bytes after a validated catalog migration.
    pub fn set_schema(&mut self, schema: &Schema) {
        self.schema_ipc = encode_schema(schema);
    }

    /// Returns the effective schema catalog version for this manifest.
    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.schema_version.unwrap_or(INITIAL_SCHEMA_VERSION)
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
        if self.manifest.version != filename_version {
            return Err(StorageError::CorruptManifest(
                path.to_path_buf(),
                format!(
                    "filename version {filename_version} does not match payload version {}",
                    self.manifest.version
                ),
            ));
        }
        validate_schema_version(self.manifest.schema_version(), Some(path))?;
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

fn manifest_limit_error(path: &Path, detail: impl Into<String>) -> StorageError {
    StorageError::CorruptManifest(path.to_path_buf(), detail.into())
}

fn validate_json_limits(
    value: &serde_json::Value,
    path: &Path,
    depth: usize,
    array_limit: usize,
) -> Result<()> {
    if depth > MAX_MANIFEST_JSON_DEPTH {
        return Err(manifest_limit_error(
            path,
            format!("JSON nesting exceeds maximum depth {MAX_MANIFEST_JSON_DEPTH}"),
        ));
    }

    match value {
        serde_json::Value::Array(values) => {
            if values.len() > array_limit {
                return Err(manifest_limit_error(
                    path,
                    format!(
                        "JSON array contains {} items; maximum is {array_limit}",
                        values.len(),
                    ),
                ));
            }
            for value in values {
                validate_json_limits(value, path, depth + 1, array_limit)?;
            }
        }
        serde_json::Value::Object(values) => {
            if values.len() > MAX_MANIFEST_OBJECT_FIELDS {
                return Err(manifest_limit_error(
                    path,
                    format!(
                        "JSON object contains {} fields; maximum is {MAX_MANIFEST_OBJECT_FIELDS}",
                        values.len()
                    ),
                ));
            }
            for (key, value) in values {
                if key.len() > MAX_MANIFEST_STRING_BYTES {
                    return Err(manifest_limit_error(
                        path,
                        format!(
                            "JSON object key exceeds maximum length {MAX_MANIFEST_STRING_BYTES}"
                        ),
                    ));
                }
                let child_array_limit = match key.as_str() {
                    "tombstones" => MAX_MANIFEST_TOMBSTONES,
                    "schema_ipc" => MAX_MANIFEST_SCHEMA_IPC_BYTES,
                    "data_files" => MAX_MANIFEST_DATA_FILES,
                    "segments" => MAX_MANIFEST_SEGMENTS,
                    _ => MAX_MANIFEST_ARRAY_ITEMS,
                };
                validate_json_limits(value, path, depth + 1, child_array_limit)?;
            }
        }
        serde_json::Value::String(value) if value.len() > MAX_MANIFEST_STRING_BYTES => {
            return Err(manifest_limit_error(
                path,
                format!("JSON string exceeds maximum length {MAX_MANIFEST_STRING_BYTES}"),
            ));
        }
        _ => {}
    }

    Ok(())
}

fn validate_manifest_collection_limits(value: &serde_json::Value, path: &Path) -> Result<()> {
    let Some(manifest) = value.get("manifest").and_then(serde_json::Value::as_object) else {
        return Err(manifest_limit_error(
            path,
            "manifest envelope payload is not an object",
        ));
    };

    for (field, limit) in [
        ("data_files", MAX_MANIFEST_DATA_FILES),
        ("segments", MAX_MANIFEST_SEGMENTS),
        ("tombstones", MAX_MANIFEST_TOMBSTONES),
    ] {
        if let Some(items) = manifest.get(field).and_then(serde_json::Value::as_array)
            && items.len() > limit
        {
            return Err(manifest_limit_error(
                path,
                format!(
                    "manifest field '{field}' contains {} items; maximum is {limit}",
                    items.len()
                ),
            ));
        }
    }

    if let Some(schema_ipc) = manifest
        .get("schema_ipc")
        .and_then(serde_json::Value::as_array)
        && schema_ipc.len() > MAX_MANIFEST_SCHEMA_IPC_BYTES
    {
        return Err(manifest_limit_error(
            path,
            format!(
                "schema_ipc contains {} bytes; maximum is {MAX_MANIFEST_SCHEMA_IPC_BYTES}",
                schema_ipc.len()
            ),
        ));
    }

    Ok(())
}

fn raw_envelope_checksum(value: &serde_json::Value, path: &Path) -> Result<(u32, u32, u32)> {
    let object = value
        .as_object()
        .ok_or_else(|| manifest_limit_error(path, "manifest envelope is not a JSON object"))?;
    let format_version = object
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            manifest_limit_error(path, "manifest format_version is missing or invalid")
        })?;
    let stored_checksum = object
        .get("checksum")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| manifest_limit_error(path, "manifest checksum is missing or invalid"))?;

    let mut zeroed = value.clone();
    let Some(zeroed_object) = zeroed.as_object_mut() else {
        return Err(manifest_limit_error(
            path,
            "manifest envelope is not a JSON object",
        ));
    };
    zeroed_object.insert("checksum".to_string(), serde_json::Value::from(0));
    let canonical = serde_json::to_vec(&canonicalize_json(zeroed))?;
    Ok((format_version, stored_checksum, crc32c::crc32c(&canonical)))
}

#[cfg(test)]
fn versions_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join("_versions")
}

#[cfg(test)]
fn manifest_path(dataset_dir: &Path, version: u64) -> PathBuf {
    versions_dir(dataset_dir).join(format!("{version:020}.manifest"))
}

/// Publishes `manifest` atomically as a new immutable current-version candidate.
/// A successful return is the local durability acknowledgement; a
/// `PublicationIndeterminate` error can still leave the exact final-name
/// candidate readable without that acknowledgement.
/// Never call this twice concurrently for the same `dataset_dir` from
/// separate writers in Phase 1 — there is no conflict detection yet (single
/// writer only); see `crates/txn`.
///
/// # Errors
///
/// Returns an error if the `_versions/` directory can't be created, if the
/// manifest can't be serialized or written, or if its version already exists.
/// Those are definite failures when they occur before final-name publication.
/// If the immutable final-name candidate is readable after an error but its
/// directory sync failed, returns `StorageError::PublicationIndeterminate`:
/// visibility may exist, but no durability acknowledgement is reported.
pub fn commit_manifest(dataset_dir: &Path, manifest: &Manifest) -> Result<()> {
    commit_manifest_with(&crate::backend::StorageOwner::local(dataset_dir), manifest)
}

/// Publishes a manifest through a dataset-owned backend capability.
#[allow(clippy::missing_errors_doc)]
pub fn commit_manifest_with(
    owner: &crate::backend::StorageOwner,
    manifest: &Manifest,
) -> Result<()> {
    let key = owner.manifest_object_key(manifest.version);
    let json = serde_json::to_vec(&ManifestEnvelope::new(manifest.clone())?)?;
    // Validate the exact serialized bytes before publication. Recovery applies
    // the same bounds and raw-checksum rules, so a successful commit must never
    // create a final-name manifest that the next reopen would reject.
    decode_manifest_with_byte_count(owner.root(), key.as_str(), manifest.version, &json)?;
    // `put_if_absent` makes a manifest version immutable: a retry can never
    // overwrite the bytes recovery may already select. `LocalFs` publishes
    // its final hard-link name before synchronizing the directory, so a
    // sync failure may leave these exact bytes observable. Classify only
    // that verified post-publication state as indeterminate; pre-publication
    // failures and duplicate-version collisions retain their original errors.
    match owner.put_if_absent(&key, &json) {
        Ok(()) => Ok(()),
        Err(error) if !matches!(error, StorageError::AlreadyExists(_)) => match owner.get(&key) {
            Ok(published) if published == json => Err(StorageError::PublicationIndeterminate(
                key.as_str().to_owned(),
            )),
            _ => Err(error),
        },
        Err(error) => Err(error),
    }
}

/// Returns the highest readable final-name version's manifest, or `None` if
/// the dataset has no final-name manifest. This is the crash-recovery selection
/// mechanism: it only ever sees fully-published final-name `*.manifest` files, including a
/// candidate whose publication outcome was indeterminate.
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

/// Reads the newest manifest through a dataset-owned backend capability.
#[allow(clippy::missing_errors_doc)]
pub fn read_current_with(owner: &crate::backend::StorageOwner) -> Result<Option<Manifest>> {
    Ok(read_current_with_byte_count_with(owner)?.map(|(manifest, _)| manifest))
}

/// Reads and validates one durable manifest version, returning its payload
/// and exact serialized byte count.
///
/// # Errors
///
/// Returns an error if the versioned manifest is missing, malformed, has an
/// unsupported format/checksum/version, or carries an invalid schema.
pub fn read_manifest_with_byte_count(dataset_dir: &Path, version: u64) -> Result<(Manifest, u64)> {
    let key = format!("_versions/{version:020}.manifest");
    read_manifest_at_key_with_byte_count(dataset_dir, &key, version)
}

/// Reads and validates a manifest at its exact durable inventory key.
///
/// Callers that select a manifest key from a backend inventory must retain
/// that key rather than reconstructing a canonical filename: recovery accepts
/// valid numeric filenames such as `_versions/7.manifest` as well as the
/// writer's padded form. The decoded envelope is still required to carry the
/// supplied version.
///
/// # Errors
///
/// Returns an error if the exact manifest key is missing, malformed, has an
/// unsupported format/checksum/version, or carries an invalid schema.
pub fn read_manifest_at_key_with_byte_count(
    dataset_dir: &Path,
    key: &str,
    version: u64,
) -> Result<(Manifest, u64)> {
    read_manifest_at_key_with_byte_count_with(
        &crate::backend::StorageOwner::local(dataset_dir),
        key,
        version,
    )
}

/// Reads one exact manifest key through a dataset-owned backend capability.
#[allow(clippy::missing_errors_doc)]
pub fn read_manifest_at_key_with_byte_count_with(
    owner: &crate::backend::StorageOwner,
    key: &str,
    version: u64,
) -> Result<(Manifest, u64)> {
    let key = crate::backend::DatasetKey::new(key)?;
    let bytes = read_bounded_manifest_bytes(owner, &key, None)?;
    decode_manifest_with_byte_count(owner.root(), key.as_str(), version, &bytes)
}

/// Reads one exact manifest key when the caller already has its inventory size.
///
/// Lifecycle callers commonly enumerate `_versions/` once and then inspect
/// several manifests from that inventory. Supplying the observed size avoids
/// re-enumerating the namespace for every manifest while preserving the same
/// pre-allocation byte bound as the standalone reader.
#[allow(clippy::missing_errors_doc)]
pub fn read_manifest_at_key_with_byte_count_and_size_with(
    owner: &crate::backend::StorageOwner,
    key: &str,
    version: u64,
    size: u64,
) -> Result<(Manifest, u64)> {
    let key = crate::backend::DatasetKey::new(key)?;
    let bytes = read_bounded_manifest_bytes(owner, &key, Some(size))?;
    decode_manifest_with_byte_count(owner.root(), key.as_str(), version, &bytes)
}

fn read_bounded_manifest_bytes(
    owner: &crate::backend::StorageOwner,
    key: &crate::backend::DatasetKey,
    known_size: Option<u64>,
) -> Result<Vec<u8>> {
    let size = if let Some(size) = known_size {
        size
    } else {
        let prefix = key
            .as_str()
            .rsplit_once('/')
            .map_or("", |(prefix, _)| prefix);
        owner
            .list(prefix)?
            .into_iter()
            .find(|meta| meta.key == key.as_str())
            .ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("manifest object '{}' is missing", key.as_str()),
                ))
            })?
            .size
    };
    if size > MAX_MANIFEST_BYTES as u64 {
        return Err(manifest_limit_error(
            &owner.root().join(key.as_str()),
            format!("manifest contains {size} bytes; maximum is {MAX_MANIFEST_BYTES}"),
        ));
    }
    owner.get_range(key, 0..size)
}

fn decode_manifest_with_byte_count(
    dataset_dir: &Path,
    key: &str,
    version: u64,
    bytes: &[u8],
) -> Result<(Manifest, u64)> {
    let path = dataset_dir.join(key);
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(manifest_limit_error(
            &path,
            format!(
                "manifest contains {} bytes; maximum is {MAX_MANIFEST_BYTES}",
                bytes.len()
            ),
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| StorageError::CorruptManifest(path.clone(), error.to_string()))?;
    if !value
        .as_object()
        .is_some_and(|object| object.contains_key("format_version"))
    {
        return Err(StorageError::LegacyFormatNeedsMigration(path));
    }
    validate_json_limits(&value, &path, 0, MAX_MANIFEST_ARRAY_ITEMS)?;
    validate_manifest_collection_limits(&value, &path)?;
    let (raw_format_version, stored_checksum, expected_checksum) =
        raw_envelope_checksum(&value, &path)?;
    if raw_format_version != MANIFEST_FORMAT_VERSION {
        return Err(StorageError::CorruptManifest(
            path.clone(),
            format!(
                "format_version {raw_format_version} is unsupported; expected {MANIFEST_FORMAT_VERSION}"
            ),
        ));
    }
    if stored_checksum != expected_checksum {
        return Err(StorageError::CorruptManifest(
            path.clone(),
            format!(
                "checksum {stored_checksum} does not match canonical payload checksum {expected_checksum}"
            ),
        ));
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
    read_current_with_byte_count_with(&crate::backend::StorageOwner::local(dataset_dir))
}

/// Returns the newest manifest and loaded-byte count through an owner.
#[allow(clippy::missing_errors_doc)]
pub fn read_current_with_byte_count_with(
    owner: &crate::backend::StorageOwner,
) -> Result<Option<(Manifest, u64)>> {
    let mut best: Option<(u64, String, u64)> = None;
    for meta in owner.list("_versions")? {
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
        let is_newer = best.as_ref().is_none_or(|(v, _, _)| version > *v);
        if is_newer {
            best = Some((version, meta.key.clone(), meta.size));
        }
    }

    let Some((filename_version, key, size)) = best else {
        return Ok(None);
    };
    let manifest_key = crate::backend::DatasetKey::new(&key)?;
    let bytes = read_bounded_manifest_bytes(owner, &manifest_key, Some(size))?;
    decode_manifest_with_byte_count(owner.root(), &key, filename_version, &bytes).map(Some)
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
    fn commit_manifest_reports_indeterminate_after_final_name_creation_sync_failure() {
        // Break caught: a generic directory-sync error after a manifest has
        // reached its final name leaves callers unable to distinguish an
        // unpublished version from one recovery can already observe.
        let dir = temp_dataset_dir("indeterminate-publication");
        let m0 = manifest(0, vec![data_file("a.arrow")]);
        let _fault = crate::datafile::test_support::fail_directory_sync_on_call(
            1,
            std::io::ErrorKind::Other,
        );

        let error = commit_manifest(&dir, &m0).unwrap_err();

        assert_eq!(
            error.to_string(),
            "manifest publication is indeterminate after final-name creation: _versions/00000000000000000000.manifest",
            "a post-publication directory-sync failure needs its own outcome"
        );
        assert_eq!(
            fs::read(manifest_path(&dir, m0.version)).unwrap(),
            serde_json::to_vec(&ManifestEnvelope::new(m0.clone()).unwrap()).unwrap(),
            "the final-name manifest must exist when publication is indeterminate"
        );
        assert_eq!(
            read_current(&dir).unwrap(),
            Some(m0),
            "recovery must already be able to observe the final-name manifest"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_manifest_rejects_a_duplicate_version_without_changing_existing_bytes() {
        // Break caught: overwriting a version after a retry can replace the
        // manifest recovery selects for a version that was already published.
        let dir = temp_dataset_dir("immutable-version-collision");
        let original = manifest(0, vec![data_file("original.arrow")]);
        commit_manifest(&dir, &original).unwrap();
        let path = manifest_path(&dir, original.version);
        let original_bytes = fs::read(&path).unwrap();
        let replacement = manifest(0, vec![data_file("replacement.arrow")]);

        let result = commit_manifest(&dir, &replacement);

        assert!(
            matches!(result, Err(StorageError::AlreadyExists(ref key)) if key == "_versions/00000000000000000000.manifest"),
            "a duplicate manifest version must report the typed collision, got {result:?}"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            original_bytes,
            "a rejected duplicate manifest publication must not overwrite or delete existing bytes"
        );
        assert_eq!(read_current(&dir).unwrap(), Some(original));
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
    fn commit_manifest_rejects_output_that_recovery_would_bound() {
        let dir = temp_dataset_dir("writer-bound");
        let manifest = manifest(
            0,
            vec![data_file(&"x".repeat(MAX_MANIFEST_STRING_BYTES + 1))],
        );

        let result = commit_manifest(&dir, &manifest);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("JSON string")),
            "expected the writer-side bound, got {result:?}"
        );
        assert!(!versions_dir(&dir).exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_accepts_a_checksum_valid_manifest_without_committed_at_us() {
        // This is a real historical shape: format-v1 writers predated the
        // `committed_at_us` field. The checksum must cover the bytes that
        // were actually written, not a typed struct with today's defaults
        // inserted into it during deserialization.
        let dir = temp_dataset_dir("historical-committed-at");
        let m0 = manifest(0, vec![data_file("legacy.arrow")]);
        let mut value = serde_json::to_value(ManifestEnvelope::new(m0).unwrap()).unwrap();
        {
            let object = value.as_object_mut().unwrap();
            let manifest_value = object
                .get_mut("manifest")
                .and_then(serde_json::Value::as_object_mut)
                .unwrap();
            manifest_value.remove("committed_at_us");
            object.insert("checksum".to_string(), serde_json::json!(0));
        }
        let checksum_bytes = serde_json::to_vec(&canonicalize_json(value.clone())).unwrap();
        let checksum = crc32c::crc32c(&checksum_bytes);
        value
            .as_object_mut()
            .unwrap()
            .insert("checksum".to_string(), serde_json::json!(checksum));

        fs::create_dir_all(versions_dir(&dir)).unwrap();
        fs::write(manifest_path(&dir, 0), serde_json::to_vec(&value).unwrap()).unwrap();

        let recovered = read_current(&dir).unwrap().unwrap();

        assert_eq!(recovered.version, 0);
        assert_eq!(recovered.committed_at_us, 0);
        fs::remove_dir_all(&dir).ok();
    }

    fn write_raw_manifest_bytes(dir: &Path, bytes: &[u8]) {
        fs::create_dir_all(versions_dir(dir)).unwrap();
        fs::write(manifest_path(dir, 0), bytes).unwrap();
    }

    fn raw_manifest_value() -> serde_json::Value {
        serde_json::to_value(ManifestEnvelope::new(manifest(0, Vec::new())).unwrap()).unwrap()
    }

    #[test]
    fn read_current_rejects_a_manifest_over_the_encoded_byte_limit() {
        let dir = temp_dataset_dir("manifest-byte-limit");
        write_raw_manifest_bytes(&dir, &vec![b'x'; MAX_MANIFEST_BYTES + 1]);

        let result = read_current(&dir);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("maximum is 67108864")),
            "expected the encoded manifest bound, got {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_manifest_json_that_is_too_deep() {
        let dir = temp_dataset_dir("manifest-depth-limit");
        let mut bytes = Vec::with_capacity((MAX_MANIFEST_JSON_DEPTH + 1) * 2 + 1);
        bytes.extend(std::iter::repeat_n(b'[', MAX_MANIFEST_JSON_DEPTH + 1));
        bytes.push(b'0');
        bytes.extend(std::iter::repeat_n(b']', MAX_MANIFEST_JSON_DEPTH + 1));
        write_raw_manifest_bytes(&dir, &bytes);

        let result = read_current(&dir);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("JSON nesting") || detail.contains("recursion limit")),
            "expected the JSON depth bound, got {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_manifest_json_with_too_many_object_fields() {
        let dir = temp_dataset_dir("manifest-object-limit");
        let mut value = raw_manifest_value();
        let object = value
            .get_mut("manifest")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        for index in 0..=MAX_MANIFEST_OBJECT_FIELDS {
            object.insert(format!("unknown_{index}"), serde_json::Value::Null);
        }
        write_raw_manifest_bytes(&dir, &serde_json::to_vec(&value).unwrap());

        let result = read_current(&dir);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("JSON object")),
            "expected the JSON object-field bound, got {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_current_rejects_manifest_with_too_many_data_files() {
        let dir = temp_dataset_dir("manifest-data-file-limit");
        let mut value = raw_manifest_value();
        value["manifest"]["data_files"] = serde_json::Value::Array(
            std::iter::repeat_n(serde_json::Value::Null, MAX_MANIFEST_DATA_FILES + 1).collect(),
        );
        write_raw_manifest_bytes(&dir, &serde_json::to_vec(&value).unwrap());

        let result = read_current(&dir);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("data_files") || detail.contains("maximum is 900000")),
            "expected the data-file bound, got {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn field_specific_array_limits_are_not_shadowed_by_generic_limit() {
        let value = serde_json::json!({
            "manifest": {
                "tombstones": std::iter::repeat_n(serde_json::Value::from(0),
                    MAX_MANIFEST_ARRAY_ITEMS + 1).collect::<Vec<_>>()
            }
        });

        assert!(
            validate_json_limits(&value, Path::new("manifest"), 0, MAX_MANIFEST_ARRAY_ITEMS)
                .is_ok()
        );
    }

    #[test]
    fn read_current_rejects_manifest_with_an_oversized_string() {
        let dir = temp_dataset_dir("manifest-string-limit");
        let mut value = raw_manifest_value();
        value["manifest"]["schema_ipc"] = serde_json::json!([1]);
        value["manifest"]["data_files"] = serde_json::json!([{
            "name": "x".repeat(MAX_MANIFEST_STRING_BYTES + 1),
            "byte_len": 0,
            "crc32c": 0,
            "row_count": 0,
            "row_id_range": null,
            "stats": {}
        }]);
        write_raw_manifest_bytes(&dir, &serde_json::to_vec(&value).unwrap());

        let result = read_current(&dir);

        assert!(
            matches!(result, Err(StorageError::CorruptManifest(_, ref detail)) if detail.contains("JSON string")),
            "expected the JSON string bound, got {result:?}"
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
    fn manifest_without_committed_at_field_deserializes_with_default_zero() {
        let old_json = serde_json::json!({
            "version": 7,
            "schema_ipc": Manifest::empty().schema_ipc,
            "data_files": [],
            "next_row_id": 0,
            "tombstones": [],
            "next_attempt_id": 0,
            "commit_time_high_water": 0,
            "segments": []
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert_eq!(deserialized.committed_at_us, 0);
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
    fn schema_catalog_version_round_trips_and_legacy_manifests_default_to_v1() {
        // Break caught: a manifest that silently drops its catalog version
        // cannot distinguish v1 data from a future incompatible schema.
        let mut manifest = Manifest::empty();
        manifest.schema_version = Some(2);
        let json = serde_json::to_vec(&manifest).unwrap();
        let round_tripped: Manifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(round_tripped.schema_version, Some(2));

        let legacy_json = serde_json::json!({
            "version": 0,
            "schema_ipc": Manifest::empty().schema_ipc,
            "data_files": [],
            "next_row_id": 0,
        });
        let legacy: Manifest = serde_json::from_value(legacy_json).unwrap();
        assert_eq!(legacy.schema_version, None);
        assert_eq!(legacy.schema_version(), 1);
    }

    #[test]
    fn read_current_accepts_a_pre_schema_version_v1_envelope_without_rewriting_it() {
        // Break caught: treating an absent catalog version as an explicit v1
        // during checksum validation changes the canonical envelope bytes and
        // makes all format-v1 envelopes written before this field unreadable.
        //
        // This is a pinned historical v1 envelope. It intentionally uses
        // literal bytes and a hand-pinned checksum rather than `Manifest`,
        // `ManifestEnvelope`, or canonicalization helpers, so a change to
        // the current serializer cannot manufacture its own compatibility
        // fixture.
        const HISTORICAL_V1_ENVELOPE: &[u8] = br#"{"format_version":1,"manifest":{"version":0,"schema_ipc":[16,0,0,0,0,0,10,0,12,0,10,0,9,0,4,0,10,0,0,0,16,0,0,0,0,1,4,0,8,0,8,0,0,0,4,0,8,0,0,0,4,0,0,0,3,0,0,0,196,0,0,0,132,0,0,0,4,0,0,0,88,255,255,255,32,0,0,0,12,0,0,0,0,0,0,16,96,0,0,0,1,0,0,0,36,0,0,0,0,0,6,0,8,0,4,0,6,0,0,0,3,0,0,0,16,0,22,0,16,0,0,0,15,0,4,0,0,0,8,0,16,0,0,0,24,0,0,0,28,0,0,0,0,0,0,3,24,0,0,0,0,0,6,0,8,0,6,0,6,0,0,0,0,0,1,0,0,0,0,0,4,0,0,0,105,116,101,109,0,0,0,0,6,0,0,0,118,101,99,116,111,114,0,0,212,255,255,255,24,0,0,0,12,0,0,0,0,0,0,5,16,0,0,0,0,0,0,0,4,0,4,0,4,0,0,0,4,0,0,0,110,97,109,101,0,0,0,0,16,0,20,0,16,0,0,0,15,0,4,0,0,0,8,0,16,0,0,0,24,0,0,0,32,0,0,0,0,0,0,2,28,0,0,0,8,0,12,0,4,0,11,0,8,0,0,0,64,0,0,0,0,0,0,1,0,0,0,0,2,0,0,0,105,100,0,0],"data_files":[],"next_row_id":0,"tombstones":[],"next_attempt_id":0,"commit_time_high_water":0,"committed_at_us":1786760837484914,"segments":[]},"checksum":2945473902}"#;
        let dir = temp_dataset_dir("legacy-schema-version-envelope");
        let path = manifest_path(&dir, 0);
        fs::create_dir_all(versions_dir(&dir)).unwrap();
        fs::write(&path, HISTORICAL_V1_ENVELOPE).unwrap();

        let recovered = read_current(&dir).unwrap().unwrap();

        assert_eq!(recovered.schema_version, None);
        assert_eq!(recovered.schema_version(), INITIAL_SCHEMA_VERSION);
        let recovered_schema = recovered.schema(&path).unwrap();
        assert_eq!(recovered_schema.field(0).name(), "id");
        assert_eq!(recovered_schema.field(1).name(), "name");
        assert_eq!(recovered_schema.field(2).name(), "vector");
        assert_eq!(
            fs::read(path).unwrap(),
            HISTORICAL_V1_ENVELOPE,
            "recovery must not rewrite the fixture"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_rejects_an_unknown_schema_catalog_version() {
        // Break caught: treating a newer catalog version as a known schema
        // could reinterpret rows and vector segments without a migration.
        let dir = temp_dataset_dir("unknown-schema-version");
        let mut value =
            serde_json::to_value(ManifestEnvelope::new(Manifest::empty()).unwrap()).unwrap();
        value["manifest"]["schema_version"] = serde_json::json!(99);
        value["checksum"] = serde_json::json!(0);
        let checksum =
            crc32c::crc32c(&serde_json::to_vec(&canonicalize_json(value.clone())).unwrap());
        value["checksum"] = serde_json::json!(checksum);
        fs::create_dir_all(versions_dir(&dir)).unwrap();
        fs::write(manifest_path(&dir, 0), serde_json::to_vec(&value).unwrap()).unwrap();

        let result = read_current(&dir);
        assert!(
            matches!(
                result,
                Err(StorageError::UnknownSchemaVersion { version: 99, .. })
            ),
            "unknown catalog versions must fail closed: {result:?}"
        );
        fs::remove_dir_all(&dir).ok();
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
    fn exact_key_manifest_reader_decodes_an_unpadded_key_and_validates_its_version() {
        let dir = temp_dataset_dir("exact-key-reader");
        let manifest = manifest(7, vec![data_file("rows.arrow")]);
        commit_manifest(&dir, &manifest).unwrap();
        fs::rename(
            manifest_path(&dir, 7),
            versions_dir(&dir).join("7.manifest"),
        )
        .unwrap();

        let (decoded, bytes) =
            read_manifest_at_key_with_byte_count(&dir, "_versions/7.manifest", 7).unwrap();

        assert_eq!(decoded, manifest);
        assert_eq!(
            bytes,
            fs::metadata(versions_dir(&dir).join("7.manifest"))
                .unwrap()
                .len()
        );
        let mismatch = read_manifest_at_key_with_byte_count(&dir, "_versions/7.manifest", 8);
        assert!(matches!(
            mismatch,
            Err(StorageError::CorruptManifest(_, reason)) if reason.contains("filename version")
        ));
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
