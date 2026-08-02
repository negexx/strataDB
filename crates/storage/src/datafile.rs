//! Row data files. Uses Arrow's own IPC file format directly rather than a
//! hand-rolled encoding. Dictionary encoding (Phase 2, `crate::encoding`)
//! runs upstream of this module, before `write_batch` is ever called — the
//! files this module reads/writes may carry `Dictionary`-typed columns, but
//! this module itself has no encoding-specific logic. Strata's own custom
//! column-chunk/RLE format (`docs/design/phase-0-transaction-and-format-spec.md`
//! §6) remains a later, possibly-unnecessary decision — see
//! `docs/design/phase-2-encodings-and-groupby-spec.md`'s
//! "Alternatives considered" section.

use std::fs::File;
use std::io::Write as _;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use crate::error::{Result, StorageError};

/// Integrity metadata for bytes durably written by [`write_batch`] or
/// [`write_bytes`].
///
/// The digest is CRC32C (Castagnoli), matching the checksum already used for
/// immutable vector segments. It detects accidental corruption; it is not a
/// cryptographic authenticity boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteMetadata {
    /// Number of bytes in the durable file.
    pub byte_len: u64,
    /// CRC32C of those exact bytes.
    pub crc32c: u32,
}

impl WriteMetadata {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            byte_len: bytes.len() as u64,
            crc32c: crc32c_checksum(bytes),
        }
    }
}

/// CRC32C of durable catalog content. This is shared with transaction
/// recovery so manifest metadata and the inspected bytes use one algorithm.
#[must_use]
pub fn crc32c_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Test-only controls for modelling directory-sync outcomes without mocking
/// `std::fs` or depending on filesystem-specific permission behavior.
///
/// The controls are thread-local so parallel tests cannot affect another
/// test's durability path. They are compiled only for this crate's tests or
/// when a dependent crate explicitly enables `test-fault-injection`.
#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
pub mod test_support {
    use std::cell::RefCell;
    use std::io;
    use std::marker::PhantomData;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct DirectorySyncState {
        // Production behavior is always the default, including when this
        // test-only feature is compiled. A test guard must opt in to
        // intercepting a directory sync.
        force_success: bool,
        fail_on_call: Option<(usize, io::ErrorKind)>,
        calls: Vec<PathBuf>,
    }

    thread_local! {
        static DIRECTORY_SYNC_STATE: RefCell<DirectorySyncState> = RefCell::new(DirectorySyncState::default());
    }

    /// Restores the calling test thread's previous directory-sync state.
    pub struct DirectorySyncGuard {
        previous: DirectorySyncState,
        // The state is thread-local, so the guard must be dropped on the
        // thread that installed it. Rc makes this guard !Send and !Sync.
        _thread_affine: PhantomData<Rc<()>>,
    }

    impl Drop for DirectorySyncGuard {
        fn drop(&mut self) {
            DIRECTORY_SYNC_STATE.with(|state| *state.borrow_mut() = self.previous.clone());
        }
    }

    /// Causes the selected directory-sync invocation to fail on this test
    /// thread. The production implementation consumes this only when it
    /// reaches its real directory-sync boundary.
    #[must_use]
    pub fn fail_directory_sync_on_call(call: usize, kind: io::ErrorKind) -> DirectorySyncGuard {
        assert!(call > 0, "directory sync calls are one-based");
        DIRECTORY_SYNC_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let previous = state.clone();
            *state = DirectorySyncState {
                force_success: true,
                fail_on_call: Some((call, kind)),
                calls: Vec::new(),
            };
            DirectorySyncGuard {
                previous,
                _thread_affine: PhantomData,
            }
        })
    }

    /// Records directory-sync paths while making those test invocations
    /// succeed, even on a platform that cannot fsync directories.
    #[must_use]
    pub fn record_directory_syncs() -> DirectorySyncGuard {
        DIRECTORY_SYNC_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let previous = state.clone();
            *state = DirectorySyncState {
                force_success: true,
                fail_on_call: None,
                calls: Vec::new(),
            };
            DirectorySyncGuard {
                previous,
                _thread_affine: PhantomData,
            }
        })
    }

    /// Makes the next directory-sync calls use the host filesystem. Storage
    /// tests use this for the narrow cases that assert native error mapping.
    #[must_use]
    pub fn use_real_directory_syncs() -> DirectorySyncGuard {
        DIRECTORY_SYNC_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let previous = state.clone();
            *state = DirectorySyncState {
                force_success: false,
                fail_on_call: None,
                calls: Vec::new(),
            };
            DirectorySyncGuard {
                previous,
                _thread_affine: PhantomData,
            }
        })
    }

    impl DirectorySyncGuard {
        #[must_use]
        pub fn calls(&self) -> Vec<PathBuf> {
            DIRECTORY_SYNC_STATE.with(|state| state.borrow().calls.clone())
        }
    }

    #[allow(dead_code)]
    pub(crate) fn outcome(path: &Path) -> Option<io::Result<()>> {
        DIRECTORY_SYNC_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if !state.force_success && state.fail_on_call.is_none() {
                return None;
            }
            state.calls.push(path.to_path_buf());
            let call = state.calls.len();
            if let Some((failure_call, kind)) = state.fail_on_call
                && call == failure_call
            {
                return Some(Err(io::Error::from(kind)));
            }
            state.force_success.then_some(Ok(()))
        })
    }
}

/// Writes a single `RecordBatch` to `path` as an Arrow IPC file, fsyncing
/// before returning so the caller can rely on durability once this returns.
///
/// # Errors
///
/// Returns an error if `path` can't be created/written, or if Arrow's IPC
/// writer fails to serialize `batch`.
pub fn write_batch(path: &Path, batch: &RecordBatch) -> Result<WriteMetadata> {
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    crate::chaos::chaos_checkpoint(); // data-file content is now durable
    Ok(WriteMetadata::from_bytes(&std::fs::read(path)?))
}

/// Writes `bytes` to `path` verbatim, fsyncing before returning so the
/// caller can rely on durability once this returns — the raw-byte twin of
/// [`write_batch`], for payloads that are already a finished on-disk format
/// rather than an Arrow batch (today: `crates/index`'s `.seg` segments).
///
/// Lives here, not in the caller, so **every** chaos checkpoint in the
/// commit protocol stays inside `strata-storage` — see
/// `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §5.
/// Truncating (`File::create`) exactly like [`write_batch`]: callers derive
/// their filenames from a collision-free attempt id, so an existing file at
/// `path` is a bug to overwrite, not content to append to.
///
/// # Errors
///
/// Returns an error if `path` can't be created, written, or fsynced.
pub fn write_bytes(path: &Path, bytes: &[u8]) -> Result<WriteMetadata> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    crate::chaos::chaos_checkpoint(); // segment content is now durable
    Ok(WriteMetadata::from_bytes(bytes))
}

/// Fsyncs `dir` itself, not just files within it.
///
/// On POSIX filesystems, fsyncing a file's contents does not guarantee its
/// directory entry (the name→inode link) survives a crash — the containing
/// directory must be fsynced too, or a real power-loss crash can leave the
/// file's bytes durable on disk while the file itself is simply absent.
/// On Windows, this opens a native directory handle with
/// `FILE_FLAG_BACKUP_SEMANTICS` before calling [`File::sync_all`]. On POSIX,
/// it uses the ordinary directory file handle. A platform or filesystem that
/// reports that directory flushing is unsupported fails closed with
/// [`StorageError::DurabilityUnsupported`]; access, path, and other I/O
/// errors remain their original typed I/O errors.
///
/// # Errors
///
/// Returns an error when the directory cannot be opened or flushed. A
/// successful return means the local filesystem accepted the requested
/// directory-entry durability operation.
pub fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(any(test, feature = "test-fault-injection"))]
    if let Some(outcome) = test_support::outcome(dir) {
        outcome.map_err(|error| directory_sync_error(dir, error))?;
        crate::chaos::chaos_checkpoint(); // directory entries are now durable
        return Ok(());
    }

    let handle = open_directory_for_sync(dir).map_err(|error| directory_sync_error(dir, error))?;
    handle
        .sync_all()
        .map_err(|error| directory_sync_error(dir, error))?;
    crate::chaos::chaos_checkpoint(); // directory entries are now durable
    Ok(())
}

#[cfg(windows)]
fn open_directory_for_sync(dir: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;

    // `FILE_FLAG_BACKUP_SEMANTICS` asks CreateFileW (used by OpenOptions) to
    // return a handle for a directory. `File::open` omits it and therefore
    // fails on normal Windows directories before `sync_all` can flush the
    // handle.
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        // FlushFileBuffers requires a write-capable handle on Windows.
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)
}

#[cfg(not(windows))]
fn open_directory_for_sync(dir: &Path) -> std::io::Result<File> {
    File::open(dir)
}

fn directory_sync_error(dir: &Path, error: std::io::Error) -> StorageError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::Unsupported | std::io::ErrorKind::InvalidInput
    ) {
        return StorageError::DurabilityUnsupported(dir.to_path_buf());
    }

    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(1 | 50 | 87)) {
        return StorageError::DurabilityUnsupported(dir.to_path_buf());
    }

    #[cfg(not(windows))]
    if matches!(error.raw_os_error(), Some(22)) {
        return StorageError::DurabilityUnsupported(dir.to_path_buf());
    }

    StorageError::Io(error)
}

/// Reads the first (and, for Phase 1, only) `RecordBatch` from an Arrow IPC
/// file written by [`write_batch`].
///
/// # Errors
///
/// Returns an error if `path` can't be opened/read, if it isn't a valid
/// Arrow IPC file, or if it contains no record batch at all. Also returns
/// [`StorageError::CorruptDataFile`] — rather than panicking — for a
/// structurally-plausible but semantically-malformed schema that trips one
/// of several `panic!`/`unimplemented!()` sites in arrow-ipc's own schema
/// parser (confirmed at arrow-ipc 58.3.0, this crate's pinned version, via
/// a real fuzzing find — see this module's own regression tests, and the
/// upstream report filed at
/// <https://github.com/apache/arrow-rs/issues/10437>). Unlike this project's
/// other panic-recovery paths (`crates/index`/`crates/txn`'s worker-thread
/// panics, which are deliberately re-raised via `resume_unwind` once
/// residue bookkeeping is recorded, per those crates' own doc comments),
/// a panic caught here is fully swallowed and converted, never re-raised —
/// the right call for untrusted external input, but a real divergence from
/// that convention worth knowing about if this code is ever used as a
/// template elsewhere. Two residual gaps `catch_unwind` cannot close:
/// allocation failure on an attacker-controlled length aborts rather than
/// unwinds, and `get_data_type` recurses through nested field types
/// (`List`/`LargeList`/`Map`/`Struct`) with no depth bound, so a
/// sufficiently deeply-nested schema is an uncatchable stack overflow —
/// this closes the panic class fuzzing actually found, not every way a
/// malformed file could crash the process. `catch_unwind` is also inert
/// under `panic = "abort"`; nothing in this workspace sets that today, but
/// it would silently defeat this guarantee if a release profile ever does.
/// Caller-visible cost: the default panic hook still runs before
/// `catch_unwind` observes anything, so a corrupt file still prints a full
/// panic message and backtrace hint to stderr even though this function
/// returns cleanly — for an embedded engine (especially loaded into a host
/// process via the `PyO3` bindings) that can read as a crash in logs even
/// when it isn't one. Not suppressed here: `std::panic::set_hook` is
/// process-global and would race any other thread's panics.
pub fn read_batch(path: &Path) -> Result<RecordBatch> {
    let file = File::open(path)?;
    // `catch_unwind` is sound here: everything in the closure (`file`,
    // `reader`, `batch`) is local to this call and touches no state
    // observable outside it (`file` is dropped, closing the fd, whether
    // this returns normally or via the caught panic), so a panic partway
    // through leaves nothing for a caller who catches this error and
    // retries to observe as torn.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut reader = FileReader::try_new(file, None)?;
        let batch = reader
            .next()
            .ok_or_else(|| StorageError::EmptyDataFile(path.to_path_buf()))??;
        Ok(batch)
    }))
    .unwrap_or_else(|payload| {
        Err(StorageError::CorruptDataFile(
            path.to_path_buf(),
            panic_message(&*payload),
        ))
    })
}

/// Extracts a printable message from a caught panic's payload, falling
/// back to a generic description for anything that isn't a `String`/`&str`
/// (the two types `panic!`/`unimplemented!()` actually produce).
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// Reads the first `RecordBatch` from an Arrow IPC file, decoding **only**
/// the named columns.
///
/// Arrow IPC lays each column's buffers out separately, so a projected read
/// never touches the bodies of the columns it wasn't asked for. For a table
/// carrying a wide embedding column that is the difference between decoding
/// the entire dataset and decoding a couple of scalar columns — at 100k rows
/// of 512-dim `f32`, ~204MB versus ~1.6MB.
///
/// [`read_batch`] reads everything and stays the right default; this is for
/// callers that provably need a subset, such as resolving the row-ids
/// matching a predicate without materialising the vectors alongside them.
///
/// The footer is read twice — once to resolve names to indices, once to apply
/// the projection — but the footer is metadata only, so the first open
/// decodes no record-batch body.
///
/// # Errors
///
/// As [`read_batch`], plus an error if any name in `columns` is not a field
/// of this file's schema.
pub fn read_batch_columns(path: &Path, columns: &[&str]) -> Result<RecordBatch> {
    // See `read_batch`'s doc comment on why `catch_unwind` is needed and
    // sound here — same arrow-ipc panic surface, reached from either of
    // this function's two `FileReader::try_new` call sites.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let schema = FileReader::try_new(File::open(path)?, None)?.schema();
        let projection = columns
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut reader = FileReader::try_new(File::open(path)?, Some(projection))?;
        let batch = reader
            .next()
            .ok_or_else(|| StorageError::EmptyDataFile(path.to_path_buf()))??;
        Ok(batch)
    }))
    .unwrap_or_else(|payload| {
        Err(StorageError::CorruptDataFile(
            path.to_path_buf(),
            panic_message(&*payload),
        ))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn write_then_read_round_trips() {
        let dir = tempfile::Builder::new()
            .prefix("strata-datafile-test-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("test.arrow");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        write_batch(&path, &batch).unwrap();
        let read_back = read_batch(&path).unwrap();

        assert_eq!(batch, read_back);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_batch_returns_durable_length_and_checksum() {
        let dir = tempfile::Builder::new()
            .prefix("strata-datafile-metadata-test-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("test.arrow");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let metadata = write_batch(&path, &batch).unwrap();
        let persisted = std::fs::read(&path).unwrap();

        assert_eq!(metadata.byte_len, persisted.len() as u64);
        assert_eq!(metadata.crc32c, crc32c::crc32c(&persisted));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_bytes_then_read_round_trips_exactly() {
        let dir = tempfile::Builder::new()
            .prefix("strata-write-bytes-test-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("blob.seg");

        // Deliberately includes a zero byte and a high byte: this is a raw
        // binary writer, not a text one, and must not transform anything.
        let payload: Vec<u8> = vec![0x00, 0x53, 0x54, 0xFF, 0x01, 0x00, 0x00, 0x00];
        write_bytes(&path, &payload).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), payload);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_bytes_returns_durable_length_and_checksum() {
        let dir = tempfile::Builder::new()
            .prefix("strata-write-bytes-metadata-test-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("blob.seg");
        let payload = [0x00, 0x53, 0x54, 0xFF, 0x01];

        let metadata = write_bytes(&path, &payload).unwrap();

        assert_eq!(metadata.byte_len, payload.len() as u64);
        assert_eq!(metadata.crc32c, crc32c::crc32c(&payload));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn directory_sync_is_not_intercepted_without_an_active_guard() {
        let dir = tempfile::Builder::new()
            .prefix("strata-directory-sync-unguarded-")
            .tempdir()
            .unwrap()
            .keep();

        assert!(
            test_support::outcome(&dir).is_none(),
            "only an active test guard may intercept directory syncs"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(windows)]
    #[test]
    fn sync_dir_uses_a_native_directory_handle_on_windows() {
        let dir = tempfile::Builder::new()
            .prefix("strata-native-directory-sync-")
            .tempdir()
            .unwrap()
            .keep();
        let _real_sync = test_support::use_real_directory_syncs();

        sync_dir(&dir).unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_bytes_truncates_an_existing_file_rather_than_appending() {
        // `File::create` semantics, matching `write_batch`. Asserted
        // explicitly because a segment filename is derived from a unique
        // attempt id and must never be reused -- if it ever were, silent
        // appending would produce a file that still passes its own header
        // CRC while carrying trailing garbage.
        let dir = tempfile::Builder::new()
            .prefix("strata-write-bytes-truncate-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("blob.seg");

        write_bytes(&path, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        write_bytes(&path, &[9, 9]).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), vec![9, 9]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sync_dir_returns_the_directory_open_failure() {
        // Break caught: suppressing File::open errors would acknowledge a
        // rename whose containing directory could not be made durable.
        let parent = tempfile::Builder::new()
            .prefix("strata-sync-dir-missing-")
            .tempdir()
            .unwrap()
            .keep();
        let missing = parent.join("does-not-exist");
        let _real_sync = test_support::use_real_directory_syncs();

        let result = sync_dir(&missing);

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::NotFound),
            "expected the missing directory's open failure, got {result:?}"
        );
        std::fs::remove_dir_all(&parent).ok();
    }

    #[test]
    fn sync_dir_returns_an_injected_sync_failure() {
        // Break caught: ignoring sync_all errors after a rename would report
        // a write as durable even though its directory entry was not.
        let dir = tempfile::Builder::new()
            .prefix("strata-sync-dir-fault-")
            .tempdir()
            .unwrap()
            .keep();
        let _fault = test_support::fail_directory_sync_on_call(1, std::io::ErrorKind::Other);

        let result = sync_dir(&dir);

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected the injected directory-sync failure, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sync_dir_maps_unsupported_directory_sync_to_a_typed_error() {
        // Break caught: returning a generic I/O error for an unsupported
        // directory fsync would hide that this filesystem cannot meet the
        // engine's declared acknowledgement boundary.
        let dir = tempfile::Builder::new()
            .prefix("strata-sync-dir-unsupported-")
            .tempdir()
            .unwrap()
            .keep();
        let _fault = test_support::fail_directory_sync_on_call(1, std::io::ErrorKind::Unsupported);

        let error = sync_dir(&dir).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("directory durability is unsupported for {}", dir.display())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sync_dir_maps_invalid_input_to_a_typed_durability_error() {
        // Break caught: filesystems that report an EINVAL-like directory
        // flush rejection must not be presented as an ordinary, retryable
        // data-path I/O failure.
        let dir = tempfile::Builder::new()
            .prefix("strata-sync-dir-invalid-input-")
            .tempdir()
            .unwrap()
            .keep();
        let _fault = test_support::fail_directory_sync_on_call(1, std::io::ErrorKind::InvalidInput);

        let error = sync_dir(&dir).unwrap_err();

        assert!(
            matches!(error, StorageError::DurabilityUnsupported(ref path) if path == &dir),
            "expected an invalid directory-flush operation to be typed as unsupported, got {error:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(not(windows))]
    #[test]
    fn directory_sync_error_maps_posix_einval_to_a_typed_durability_error() {
        // Break caught: platforms may preserve raw EINVAL without mapping it
        // to ErrorKind::InvalidInput, so the raw OS error must be covered too.
        let dir = PathBuf::from("posix-einval-directory");

        let error = directory_sync_error(&dir, std::io::Error::from_raw_os_error(22));

        assert!(
            matches!(error, StorageError::DurabilityUnsupported(ref path) if path == &dir),
            "expected EINVAL to be typed as unsupported directory durability, got {error:?}"
        );
    }

    #[test]
    fn nested_directory_sync_guards_restore_the_outer_interceptor() {
        // Break caught: an inner test guard that overwrites the TLS state and
        // fails to restore it makes the enclosing guard silently stop
        // recording its durability boundary.
        let first = PathBuf::from("outer-first");
        let second = PathBuf::from("inner");
        let third = PathBuf::from("outer-third");
        let outer = test_support::record_directory_syncs();
        assert!(test_support::outcome(&first).unwrap().is_ok());

        {
            let _inner = test_support::fail_directory_sync_on_call(1, std::io::ErrorKind::Other);
            assert!(test_support::outcome(&second).unwrap().is_err());
        }

        assert_eq!(outer.calls(), vec![first.clone()]);
        assert!(test_support::outcome(&third).unwrap().is_ok());
        assert_eq!(outer.calls(), vec![first, third]);
    }

    #[test]
    fn directory_sync_guard_restores_passthrough_after_drop() {
        // Break caught: leaking an injected outcome after its guard drops can
        // cause later tests or production-like paths to acknowledge a sync
        // they never performed.
        let path = PathBuf::from("restored-passthrough");
        {
            let _guard = test_support::record_directory_syncs();
            assert!(test_support::outcome(&path).unwrap().is_ok());
        }

        assert!(
            test_support::outcome(&path).is_none(),
            "dropping the final guard must restore the real filesystem path"
        );
    }

    #[test]
    fn directory_sync_guard_restores_passthrough_during_unwind() {
        // Break caught: a panicking test must not leave its thread's
        // durability seam enabled for the next test.
        let path = PathBuf::from("unwind-passthrough");
        let unwound = std::panic::catch_unwind(|| {
            let _guard = test_support::record_directory_syncs();
            assert!(test_support::outcome(&path).unwrap().is_ok());
            panic!("intentional guard-unwind regression");
        });

        assert!(unwound.is_err());
        assert!(
            test_support::outcome(&path).is_none(),
            "unwinding must restore the real filesystem path"
        );
    }

    #[test]
    fn read_batch_errors_instead_of_panicking_on_a_malformed_ipc_schema() {
        // Regression test for a real find, not a hypothetical: fuzzing
        // `crates/storage`'s `datafile_parse` cargo-fuzz target hit this
        // exact input in under 30 seconds. A structurally-plausible but
        // semantically-malformed Arrow IPC schema (a field's flatbuffer
        // `Type` union set to `NONE`) makes arrow-ipc's own
        // `arrow_ipc::convert::get_data_type` panic via `unimplemented!()`
        // instead of returning a `Result::Err` -- confirmed present at
        // `arrow-ipc-58.3.0/src/convert.rs:514`, the exact version this
        // crate depends on (not just a newer one). This is exactly the
        // untrusted-input surface `read_batch` exists to guard: a
        // corrupted disk, a downgraded binary, or a hostile actor with
        // filesystem access could all hand a reader exactly this file.
        // Fixture is the literal bytes libFuzzer minimized the crash down
        // to (`crates/storage/testdata/malformed_ipc_type_none.arrow`).
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/malformed_ipc_type_none.arrow");
        let err = read_batch(&path).unwrap_err();
        // Pinned to the actual panic message, not just the variant: this
        // fixture must be proven to still exercise the specific arrow-ipc
        // bug being regression-tested, not just any panic that happens to
        // land in `CorruptDataFile` (e.g. if a future arrow-ipc upgrade
        // changed the panic site or message but happened to still panic
        // somewhere else in the same call).
        let StorageError::CorruptDataFile(_, message) = &err else {
            panic!("expected CorruptDataFile, got a different error: {err}");
        };
        assert!(
            message.contains("Type NONE"),
            "expected the arrow-ipc \"Type NONE not supported\" panic specifically, got: {message}"
        );
    }

    #[test]
    fn read_batch_columns_errors_instead_of_panicking_on_a_malformed_ipc_schema() {
        // Same fixture and same underlying arrow-ipc bug as
        // `read_batch_errors_instead_of_panicking_on_a_malformed_ipc_schema`
        // above, but for `read_batch_columns`'s own `catch_unwind` --
        // a distinct code path with two `FileReader::try_new` call sites
        // (schema resolution, then the projected read) rather than one,
        // so it isn't provably covered by the sibling test alone.
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/malformed_ipc_type_none.arrow");
        let err = read_batch_columns(&path, &["id"]).unwrap_err();
        let StorageError::CorruptDataFile(_, message) = &err else {
            panic!("expected CorruptDataFile, got a different error: {err}");
        };
        assert!(
            message.contains("Type NONE"),
            "expected the arrow-ipc \"Type NONE not supported\" panic specifically, got: {message}"
        );
    }

    #[test]
    fn read_batch_errors_on_an_ipc_file_with_zero_record_batches() {
        let dir = tempfile::Builder::new()
            .prefix("strata-datafile-empty-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("empty.arrow");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        writer.finish().unwrap(); // no batches written, just the header/footer

        let result = read_batch(&path);
        assert!(
            matches!(result, Err(StorageError::EmptyDataFile(_))),
            "expected EmptyDataFile, got {result:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_batch_errors_on_a_non_ipc_file() {
        let dir = tempfile::Builder::new()
            .prefix("strata-datafile-garbage-")
            .tempdir()
            .unwrap()
            .keep();
        let path = dir.join("garbage.arrow");
        std::fs::write(&path, b"not an arrow ipc file").unwrap();

        let result = read_batch(&path);
        assert!(result.is_err(), "expected an error, got {result:?}");
        std::fs::remove_dir_all(&dir).ok();
    }
}
