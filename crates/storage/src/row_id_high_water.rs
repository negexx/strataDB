//! Durable, immutable row-id allocation reservations.
//!
//! A manifest is the visibility boundary for rows, but a transaction claims
//! its physical row ids before it writes the files later named by that
//! manifest. This collection keeps those pre-publication claims monotonic
//! across a restart. Each record is immutable and named by its exclusive
//! high-water end, so no retry ever overwrites a possibly visible record.

use std::path::Path;

use crate::{Backend, LocalFs, Result, StorageError, sync_dir};

/// The object-store-style prefix containing immutable reservation records.
pub const ROW_ID_HIGH_WATER_PREFIX: &str = "_meta/row-id-high-water/";

const RECORD_SUFFIX: &str = ".reservation";
const RECORD_LEN: usize = 12;

/// A reservation write that failed after its immutable record became visible.
///
/// The caller must advance its in-memory allocation floor through `end`
/// before returning `source`: another claim must never reuse a range whose
/// record can survive the failed directory sync.
#[derive(Debug)]
pub enum HighWaterPersistenceError {
    /// No matching immutable record was observable after the failed write.
    Definite(StorageError),
    /// A matching immutable record was observable after the failed write,
    /// but the directory durability confirmation failed.
    PossiblyPublished { end: u64, source: StorageError },
}

/// A decoded row-ID high-water value together with the reservation payload
/// bytes read to validate it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowIdHighWaterRead {
    /// Greatest valid durable reservation record, when one was present.
    pub high_water: Option<u64>,
    /// Exact bytes returned by `Backend::get` for decoded reservation records.
    pub bytes_read: u128,
}

impl HighWaterPersistenceError {
    /// Returns the durable-or-possibly-durable floor that must not be reused.
    #[must_use]
    pub fn possibly_published_end(&self) -> Option<u64> {
        match self {
            Self::Definite(_) => None,
            Self::PossiblyPublished { end, .. } => Some(*end),
        }
    }

    /// Consumes the wrapper and returns the storage error that caused it.
    #[must_use]
    pub fn into_storage_error(self) -> StorageError {
        match self {
            Self::Definite(source) | Self::PossiblyPublished { source, .. } => source,
        }
    }
}

/// Writes the initial zero floor during dataset creation.
///
/// A successful return means the immutable record is durably published. A
/// failure after publication is returned as its underlying durability error;
/// callers creating a dataset must not acknowledge creation in that case.
///
/// # Errors
///
/// Returns the underlying storage error when the record cannot be published
/// and durably confirmed.
pub fn initialize_row_id_high_water(dataset_dir: &Path) -> Result<()> {
    match persist_row_id_high_water_at_least(dataset_dir, 0) {
        Ok(_) => Ok(()),
        Err(error) => Err(error.into_storage_error()),
    }
}

/// Initializes the row-id reservation catalog through an owner.
#[allow(clippy::missing_errors_doc)]
pub fn initialize_row_id_high_water_with(owner: &crate::backend::StorageOwner) -> Result<()> {
    match persist_row_id_high_water_at_least_with(owner, 0) {
        Ok(_) => Ok(()),
        Err(error) => Err(error.into_storage_error()),
    }
}

/// Returns the greatest valid durable reservation record, if the collection
/// has not yet been initialized for this dataset.
///
/// Temporary files left by an interrupted write are deliberately ignored:
/// they were never linked into the immutable record namespace. Any malformed
/// named record is corruption, not a value that can be silently skipped.
///
/// # Errors
///
/// Returns a storage error when the collection cannot be read or a named
/// record is malformed or fails its checksum validation.
pub fn read_row_id_high_water(dataset_dir: &Path) -> Result<Option<u64>> {
    Ok(read_row_id_high_water_with_byte_count(dataset_dir)?.high_water)
}

/// Reads row-id reservations through a dataset-owned backend capability.
#[allow(clippy::missing_errors_doc)]
pub fn read_row_id_high_water_with(owner: &crate::backend::StorageOwner) -> Result<Option<u64>> {
    Ok(read_row_id_high_water_with_byte_count_with(owner)?.high_water)
}

/// Reads row-id reservations and byte accounting through an owner.
#[allow(clippy::missing_errors_doc)]
pub fn read_row_id_high_water_with_byte_count_with(
    owner: &crate::backend::StorageOwner,
) -> Result<RowIdHighWaterRead> {
    let mut greatest = None;
    let mut bytes_read = 0_u128;
    for meta in owner.list(ROW_ID_HIGH_WATER_PREFIX.trim_end_matches('/'))? {
        let Some(end) = end_from_key(&meta.key) else {
            if temporary_key(&meta.key) {
                continue;
            }
            return Err(corrupt_record(
                &owner.root().join(&meta.key),
                "record name does not encode an immutable row-id high-water end",
            ));
        };
        let key = crate::backend::DatasetKey::new(&meta.key)?;
        let bytes = owner.get(&key)?;
        bytes_read += u128::from(bytes.len() as u64);
        let path = owner.root().join(&meta.key);
        let recorded_end = decode_record(&path, &bytes)?;
        if recorded_end != end {
            return Err(corrupt_record(
                &path,
                format!(
                    "record filename end {end} does not match checksummed payload end {recorded_end}"
                ),
            ));
        }
        greatest = Some(greatest.map_or(end, |current: u64| current.max(end)));
    }
    Ok(RowIdHighWaterRead {
        high_water: greatest,
        bytes_read,
    })
}

/// Returns the greatest valid durable reservation record and the exact bytes
/// read to decode and validate the reservation records.
///
/// Temporary files left by an interrupted write are deliberately ignored:
/// they were never linked into the immutable record namespace. Any malformed
/// named record is corruption, not a value that can be silently skipped.
///
/// # Errors
///
/// Returns a storage error when the collection cannot be read or a named
/// record is malformed or fails its checksum validation.
pub fn read_row_id_high_water_with_byte_count(dataset_dir: &Path) -> Result<RowIdHighWaterRead> {
    let backend = LocalFs::new(dataset_dir);
    let mut greatest = None;
    let mut bytes_read = 0;

    for meta in backend.list(ROW_ID_HIGH_WATER_PREFIX)? {
        let Some(end) = end_from_key(&meta.key) else {
            if temporary_key(&meta.key) {
                continue;
            }
            return Err(corrupt_record(
                &dataset_dir.join(&meta.key),
                "record name does not encode an immutable row-id high-water end",
            ));
        };

        let path = dataset_dir.join(&meta.key);
        let bytes = backend.get(&meta.key)?;
        #[cfg(feature = "test-fault-injection")]
        test_support::run_after_row_id_read_hook(&bytes);
        bytes_read += u128::from(bytes.len() as u64);
        let recorded_end = decode_record(&path, &bytes)?;
        if recorded_end != end {
            return Err(corrupt_record(
                &path,
                format!(
                    "record filename end {end} does not match checksummed payload end {recorded_end}"
                ),
            ));
        }
        greatest = Some(greatest.map_or(end, |current: u64| current.max(end)));
    }

    Ok(RowIdHighWaterRead {
        high_water: greatest,
        bytes_read,
    })
}

/// Persists a high-water mark no lower than `requested_end`.
///
/// A new immutable target is created with `Backend::put_if_absent`, whose
/// local implementation performs temp-write, content sync, atomic link into
/// the no-replace name, and directory sync. The `AlreadyExists` branch is a
/// retry/concurrent-writer recovery path: it verifies the immutable bytes and
/// re-syncs the owned metadata chain before treating the record as durable.
///
/// If publication returns an error but the named record is now observable,
/// this returns [`HighWaterPersistenceError::PossiblyPublished`]. The
/// allocator must consume that floor without handing the failed claim to the
/// transaction that received the error.
///
/// # Errors
///
/// Returns [`HighWaterPersistenceError::Definite`] when no record is visible
/// after the failed operation, or [`HighWaterPersistenceError::PossiblyPublished`]
/// when a record may survive but its directory durability confirmation failed.
pub fn persist_row_id_high_water_at_least(
    dataset_dir: &Path,
    requested_end: u64,
) -> std::result::Result<u64, HighWaterPersistenceError> {
    let observed =
        read_row_id_high_water(dataset_dir).map_err(HighWaterPersistenceError::Definite)?;
    if let Some(end) = observed.filter(|end| *end >= requested_end) {
        sync_existing_record_chain(dataset_dir)
            .map_err(|source| HighWaterPersistenceError::PossiblyPublished { end, source })?;
        return Ok(end);
    }

    #[cfg(any(test, feature = "test-fault-injection"))]
    if let Some(source) = test_support::before_publish_error() {
        return Err(HighWaterPersistenceError::Definite(source.into()));
    }

    let end = requested_end;
    let key = record_key(end);
    let backend = LocalFs::new(dataset_dir);
    let bytes = encode_record(end);
    match backend.put_if_absent(&key, &bytes) {
        Ok(()) => Ok(end),
        Err(StorageError::AlreadyExists(_)) => {
            let observed =
                read_row_id_high_water(dataset_dir).map_err(HighWaterPersistenceError::Definite)?;
            let Some(observed_end) = observed.filter(|observed_end| *observed_end >= end) else {
                return Err(HighWaterPersistenceError::Definite(corrupt_record(
                    &dataset_dir.join(&key),
                    "immutable row-id high-water target existed without a valid matching record",
                )));
            };
            sync_existing_record_chain(dataset_dir).map_err(|source| {
                HighWaterPersistenceError::PossiblyPublished {
                    end: observed_end,
                    source,
                }
            })?;
            Ok(observed_end)
        }
        Err(source) => match read_row_id_high_water(dataset_dir) {
            Ok(Some(observed_end)) if observed_end >= end => {
                Err(HighWaterPersistenceError::PossiblyPublished {
                    end: observed_end,
                    source,
                })
            }
            Ok(_) => Err(HighWaterPersistenceError::Definite(source)),
            Err(observation_error) => Err(HighWaterPersistenceError::Definite(observation_error)),
        },
    }
}

/// Persists a row-id reservation through an owner. Backend publication is
/// already the durability boundary, so this path does not require local
/// directory handles and makes no cross-process allocation claim.
#[allow(clippy::missing_errors_doc)]
pub fn persist_row_id_high_water_at_least_with(
    owner: &crate::backend::StorageOwner,
    requested_end: u64,
) -> std::result::Result<u64, HighWaterPersistenceError> {
    let observed =
        read_row_id_high_water_with(owner).map_err(HighWaterPersistenceError::Definite)?;
    if let Some(end) = observed.filter(|end| *end >= requested_end) {
        return Ok(end);
    }
    let end = requested_end;
    let key = crate::backend::DatasetKey::new(record_key(end))
        .map_err(HighWaterPersistenceError::Definite)?;
    let bytes = encode_record(end);
    match owner.put_if_absent(&key, &bytes) {
        Ok(()) => Ok(end),
        Err(StorageError::AlreadyExists(_)) => {
            let observed =
                read_row_id_high_water_with(owner).map_err(HighWaterPersistenceError::Definite)?;
            match observed.filter(|observed_end| *observed_end >= end) {
                Some(observed_end) => Ok(observed_end),
                None => Err(HighWaterPersistenceError::Definite(corrupt_record(
                    &owner.root().join(key.as_str()),
                    "immutable row-id high-water target existed without a valid matching record",
                ))),
            }
        }
        Err(source) => match read_row_id_high_water_with(owner) {
            Ok(Some(observed_end)) if observed_end >= end => {
                Err(HighWaterPersistenceError::PossiblyPublished {
                    end: observed_end,
                    source,
                })
            }
            Ok(_) => Err(HighWaterPersistenceError::Definite(source)),
            Err(observation_error) => Err(HighWaterPersistenceError::Definite(observation_error)),
        },
    }
}

fn record_key(end: u64) -> String {
    format!("{ROW_ID_HIGH_WATER_PREFIX}{end:020}{RECORD_SUFFIX}")
}

fn end_from_key(key: &str) -> Option<u64> {
    let name = key.strip_prefix(ROW_ID_HIGH_WATER_PREFIX)?;
    let end = name.strip_suffix(RECORD_SUFFIX)?;
    if end.len() != 20 || !end.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    end.parse().ok()
}

fn temporary_key(key: &str) -> bool {
    key.strip_prefix(ROW_ID_HIGH_WATER_PREFIX)
        .is_some_and(|name| name.starts_with(".tmp-"))
}

fn encode_record(end: u64) -> [u8; RECORD_LEN] {
    let mut bytes = [0; RECORD_LEN];
    let end_bytes = end.to_le_bytes();
    let checksum = crc32c::crc32c(&end_bytes).to_le_bytes();
    bytes[..8].copy_from_slice(&end_bytes);
    bytes[8..].copy_from_slice(&checksum);
    bytes
}

fn decode_record(path: &Path, bytes: &[u8]) -> Result<u64> {
    if bytes.len() != RECORD_LEN {
        return Err(corrupt_record(
            path,
            format!("record length {} does not equal {RECORD_LEN}", bytes.len()),
        ));
    }
    let mut end_bytes = [0; 8];
    end_bytes.copy_from_slice(&bytes[..8]);
    let end = u64::from_le_bytes(end_bytes);
    let mut checksum_bytes = [0; 4];
    checksum_bytes.copy_from_slice(&bytes[8..]);
    let found_checksum = u32::from_le_bytes(checksum_bytes);
    let expected_checksum = crc32c::crc32c(&bytes[..8]);
    if found_checksum != expected_checksum {
        return Err(corrupt_record(
            path,
            format!(
                "checksum {found_checksum} does not match payload checksum {expected_checksum}"
            ),
        ));
    }
    Ok(end)
}

/// Repeats the local backend's bounded collection -> `_meta` -> dataset-root
/// synchronization path after discovering an immutable target left by an
/// uncertain earlier publication. `LocalFs::put_if_absent` already performs
/// this chain for a newly-created target; this helper only covers the
/// existing-target branch, where the backend intentionally returns
/// `AlreadyExists` without changing the filesystem.
fn sync_existing_record_chain(dataset_dir: &Path) -> Result<()> {
    let collection_dir = dataset_dir.join("_meta").join("row-id-high-water");
    let metadata_dir = collection_dir.parent().ok_or_else(|| {
        corrupt_record(
            &collection_dir,
            "row-id high-water collection has no metadata parent",
        )
    })?;
    sync_dir(&collection_dir)?;
    sync_dir(metadata_dir)?;
    sync_dir(dataset_dir)?;
    Ok(())
}

fn corrupt_record(path: &Path, detail: impl Into<String>) -> StorageError {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "corrupt row-id high-water record at {}: {}",
            path.display(),
            detail.into()
        ),
    )
    .into()
}

#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
pub mod test_support {
    use std::cell::RefCell;
    use std::io;
    use std::marker::PhantomData;
    use std::rc::Rc;

    #[cfg(feature = "test-fault-injection")]
    type AfterRowIdReadHook = Box<dyn FnOnce(&[u8])>;

    thread_local! {
        static BEFORE_PUBLISH_FAILURE: RefCell<Option<io::ErrorKind>> = const { RefCell::new(None) };
    }

    #[cfg(feature = "test-fault-injection")]
    thread_local! {
        static AFTER_ROW_ID_READ_HOOK: RefCell<Option<AfterRowIdReadHook>> = const { RefCell::new(None) };
    }

    /// Installs a one-shot, thread-local hook that runs after a row-ID
    /// reservation payload has been read from the backend.
    ///
    /// This seam exists only for dependent-crate tests that need to mutate
    /// the catalog after recovery has received bytes but before it returns
    /// its decoded accounting.
    #[cfg(feature = "test-fault-injection")]
    pub fn set_after_row_id_read_hook(hook: impl FnOnce(&[u8]) + 'static) {
        AFTER_ROW_ID_READ_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    #[cfg(feature = "test-fault-injection")]
    pub(super) fn run_after_row_id_read_hook(bytes: &[u8]) {
        let hook = AFTER_ROW_ID_READ_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(bytes);
        }
    }

    /// Thread-affine scoped pre-publication failure injection for the real
    /// immutable record path. It fires immediately before `put_if_absent`,
    /// so no target or data file can have been published yet.
    #[must_use]
    pub struct ReservationPublishGuard {
        previous: Option<io::ErrorKind>,
        _thread_affine: PhantomData<Rc<()>>,
    }

    impl Drop for ReservationPublishGuard {
        fn drop(&mut self) {
            BEFORE_PUBLISH_FAILURE.with(|failure| *failure.borrow_mut() = self.previous);
        }
    }

    pub fn fail_reservation_before_publish(kind: io::ErrorKind) -> ReservationPublishGuard {
        BEFORE_PUBLISH_FAILURE.with(|failure| {
            let previous = failure.replace(Some(kind));
            ReservationPublishGuard {
                previous,
                _thread_affine: PhantomData,
            }
        })
    }

    pub(super) fn before_publish_error() -> Option<io::Error> {
        BEFORE_PUBLISH_FAILURE.with(|failure| {
            failure
                .borrow_mut()
                .take()
                .map(|kind| io::Error::new(kind, "injected row-id reservation publication failure"))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn dataset_dir(label: &str) -> PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-row-id-high-water-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    #[test]
    fn immutable_records_round_trip_and_select_the_greatest_end() {
        let dir = dataset_dir("round-trip");

        assert_eq!(persist_row_id_high_water_at_least(&dir, 3).unwrap(), 3);
        assert_eq!(persist_row_id_high_water_at_least(&dir, 9).unwrap(), 9);
        assert_eq!(persist_row_id_high_water_at_least(&dir, 5).unwrap(), 9);
        assert_eq!(read_row_id_high_water(&dir).unwrap(), Some(9));

        let records = LocalFs::new(&dir).list(ROW_ID_HIGH_WATER_PREFIX).unwrap();
        assert_eq!(records.len(), 2, "lower requests must not replace records");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_with_byte_count_reports_loaded_records_without_changing_legacy_value() {
        // Break caught: recovery accounting derived from a second catalog
        // listing could report metadata that was not part of the loader's
        // actual decoded reservation payloads.
        let dir = dataset_dir("read-with-byte-count");

        assert_eq!(persist_row_id_high_water_at_least(&dir, 3).unwrap(), 3);
        assert_eq!(persist_row_id_high_water_at_least(&dir, 9).unwrap(), 9);

        let loaded = read_row_id_high_water_with_byte_count(&dir).unwrap();

        assert_eq!(loaded.high_water, Some(9));
        assert_eq!(loaded.bytes_read, 24);
        assert_eq!(read_row_id_high_water(&dir).unwrap(), loaded.high_water);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pre_publish_failure_leaves_no_record() {
        let dir = dataset_dir("before-publish");
        let _fault = test_support::fail_reservation_before_publish(std::io::ErrorKind::Other);

        let result = persist_row_id_high_water_at_least(&dir, 1);

        assert!(
            matches!(result, Err(HighWaterPersistenceError::Definite(StorageError::Io(ref error))) if error.kind() == std::io::ErrorKind::Other)
        );
        assert_eq!(read_row_id_high_water(&dir).unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn post_publish_sync_failure_keeps_the_observable_end_for_retry() {
        let dir = dataset_dir("post-publish");
        let fault = crate::datafile::test_support::fail_directory_sync_on_call(
            1,
            std::io::ErrorKind::Other,
        );

        let result = persist_row_id_high_water_at_least(&dir, 7);

        assert!(
            matches!(result, Err(HighWaterPersistenceError::PossiblyPublished { end: 7, source: StorageError::Io(ref error) }) if error.kind() == std::io::ErrorKind::Other)
        );
        assert_eq!(read_row_id_high_water(&dir).unwrap(), Some(7));
        drop(fault);
        assert_eq!(persist_row_id_high_water_at_least(&dir, 7).unwrap(), 7);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn checksum_corruption_is_not_silently_accepted() {
        let dir = dataset_dir("checksum");
        persist_row_id_high_water_at_least(&dir, 4).unwrap();
        let path = dir
            .join("_meta")
            .join("row-id-high-water")
            .join("00000000000000000004.reservation");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let result = read_row_id_high_water(&dir);

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::InvalidData)
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
