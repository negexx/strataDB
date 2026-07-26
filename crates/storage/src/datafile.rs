//! Row data files. Uses Arrow's own IPC file format directly rather than a
//! hand-rolled encoding. Dictionary encoding (Phase 2, `crate::encoding`)
//! runs upstream of this module, before `write_batch` is ever called — the
//! files this module reads/writes may carry `Dictionary`-typed columns, but
//! this module itself has no encoding-specific logic. Strata's own custom
//! column-chunk/RLE format (`.claude/docs/design/phase-0-transaction-and-format-spec.md`
//! §6) remains a later, possibly-unnecessary decision — see
//! `.claude/docs/design/phase-2-encodings-and-groupby-spec.md`'s
//! "Alternatives considered" section.

use std::fs::File;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use crate::error::{Result, StorageError};

/// Writes a single `RecordBatch` to `path` as an Arrow IPC file, fsyncing
/// before returning so the caller can rely on durability once this returns.
///
/// # Errors
///
/// Returns an error if `path` can't be created/written, or if Arrow's IPC
/// writer fails to serialize `batch`.
pub fn write_batch(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    crate::chaos::chaos_checkpoint(); // data-file content is now durable
    Ok(())
}

/// Fsyncs `dir` itself, not just files within it.
///
/// On POSIX filesystems, fsyncing a file's contents does not guarantee its
/// directory entry (the name→inode link) survives a crash — the containing
/// directory must be fsynced too, or a real power-loss crash can leave the
/// file's bytes durable on disk while the file itself is simply absent.
/// Best-effort: not supported uniformly across platforms (notably Windows),
/// so a failure to open/sync the directory is tolerated rather than
/// propagated — this mirrors the durability caveat already documented on
/// [`crate::manifest::commit_manifest`]'s directory-fsync step.
///
/// # Errors
///
/// This function does not currently return an error; it always returns
/// `Ok(())`. It is fallible in signature so a future platform-specific
/// failure mode can be surfaced without an API break.
pub fn sync_dir(dir: &Path) -> Result<()> {
    if let Ok(handle) = File::open(dir) {
        let _ = handle.sync_all();
    }
    crate::chaos::chaos_checkpoint(); // directory entries are now durable (best-effort per-platform, see doc comment above)
    Ok(())
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
/// other
/// panic-recovery paths (`crates/index`/`crates/txn`'s worker-thread
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
        assert!(
            matches!(err, StorageError::CorruptDataFile(_, _)),
            "expected CorruptDataFile, got a different error: {err}"
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
