//! Row data files. Phase 1 uses Arrow's own IPC file format directly rather
//! than a hand-rolled encoding — the custom column-chunk/dictionary/RLE
//! format described in `.claude/docs/design/phase-0-transaction-and-format-spec.md`
//! §6 is real "Phase 2: Real encodings" work, not part of the MVP vertical
//! slice (see the roadmap in `.claude/docs/architecture.md`).

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
    Ok(())
}

/// Reads the first (and, for Phase 1, only) `RecordBatch` from an Arrow IPC
/// file written by [`write_batch`].
///
/// # Errors
///
/// Returns an error if `path` can't be opened/read, if it isn't a valid
/// Arrow IPC file, or if it contains no record batch at all.
pub fn read_batch(path: &Path) -> Result<RecordBatch> {
    let file = File::open(path)?;
    let mut reader = FileReader::try_new(file, None)?;
    let batch = reader
        .next()
        .ok_or_else(|| StorageError::EmptyDataFile(path.to_path_buf()))??;
    Ok(batch)
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
        let dir = std::env::temp_dir().join(format!("strata-datafile-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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
    fn read_batch_errors_on_an_ipc_file_with_zero_record_batches() {
        let dir =
            std::env::temp_dir().join(format!("strata-datafile-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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
        let dir =
            std::env::temp_dir().join(format!("strata-datafile-garbage-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("garbage.arrow");
        std::fs::write(&path, b"not an arrow ipc file").unwrap();

        let result = read_batch(&path);
        assert!(result.is_err(), "expected an error, got {result:?}");
        std::fs::remove_dir_all(&dir).ok();
    }
}
