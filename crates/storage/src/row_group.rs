//! Bounded row-group storage for selective reads.
//!
//! This format is intentionally additive: existing Arrow IPC files remain
//! valid and continue through the legacy APIs. New callers can opt into a
//! small indexed container whose independent IPC payloads can be skipped
//! before decoding.

use std::io::Cursor;
use std::path::Path;
use std::{fmt::Display, fs::File, io::Write};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use crate::datafile::{WriteMetadata, crc32c_checksum, read_batch_from_bytes};
use crate::error::{Result, StorageError};

const MAGIC: &[u8; 8] = b"STRARGR1";
const ENTRY_BYTES: usize = 32;

fn checked<T, E: Display>(value: std::result::Result<T, E>) -> Result<T> {
    value.map_err(|error| StorageError::Io(std::io::Error::other(error.to_string())))
}

/// Index entry for one independently decodable row group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowGroupEntry {
    pub byte_offset: u64,
    pub byte_len: u64,
    pub row_start: u64,
    pub row_end: u64,
}

/// Writes an indexed row-group container. `group_rows` controls the maximum
/// number of rows in each independent Arrow IPC payload.
#[allow(clippy::missing_errors_doc)]
pub fn write_row_groups(
    path: &Path,
    batch: &RecordBatch,
    group_rows: usize,
) -> Result<WriteMetadata> {
    if group_rows == 0 {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "row-group size must be non-zero",
        )));
    }
    let mut payloads = Vec::new();
    let mut start = 0_u64;
    while checked(usize::try_from(start))? < batch.num_rows() {
        let start_usize = checked(usize::try_from(start))?;
        let len = group_rows.min(batch.num_rows() - start_usize);
        let group = batch.slice(start_usize, len);
        let mut writer = FileWriter::try_new(Cursor::new(Vec::new()), &group.schema())?;
        writer.write(&group)?;
        writer.finish()?;
        payloads.push(writer.into_inner()?.into_inner());
        start = start
            .checked_add(checked(u64::try_from(len))?)
            .ok_or_else(|| {
                StorageError::Io(std::io::Error::other("row-group row count overflow"))
            })?;
    }
    if payloads.is_empty() {
        let mut writer = FileWriter::try_new(Cursor::new(Vec::new()), &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
        payloads.push(writer.into_inner()?.into_inner());
    }

    let group_count = checked(u32::try_from(payloads.len()))?;
    let header_len =
        16usize
            .checked_add(ENTRY_BYTES.checked_mul(payloads.len()).ok_or_else(|| {
                StorageError::Io(std::io::Error::other("row-group header overflow"))
            })?)
            .ok_or_else(|| StorageError::Io(std::io::Error::other("row-group header overflow")))?;
    let mut bytes = Vec::with_capacity(header_len + payloads.iter().map(Vec::len).sum::<usize>());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&group_count.to_le_bytes());
    let mut offset = checked(u64::try_from(header_len))?;
    let mut row_start = 0_u64;
    for payload in &payloads {
        let len = checked(u64::try_from(payload.len()))?;
        let row_end = row_start
            .checked_add(checked(u64::try_from(if payloads.len() == 1 {
                batch.num_rows()
            } else {
                group_rows.min(batch.num_rows() - checked(usize::try_from(row_start))?)
            }))?)
            .ok_or_else(|| StorageError::Io(std::io::Error::other("row-group range overflow")))?;
        for value in [offset, len, row_start, row_end] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        offset = offset
            .checked_add(len)
            .ok_or_else(|| StorageError::Io(std::io::Error::other("row-group offset overflow")))?;
        row_start = row_end;
    }
    for payload in payloads {
        bytes.extend_from_slice(&payload);
    }
    let metadata = WriteMetadata {
        byte_len: checked(u64::try_from(bytes.len()))?,
        crc32c: crc32c_checksum(&bytes),
    };
    let mut file = File::create(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(metadata)
}

/// Reads only the requested row groups and projects the requested columns.
#[allow(clippy::missing_errors_doc)]
pub fn read_row_groups(
    path: &Path,
    columns: &[&str],
    groups: std::ops::Range<usize>,
) -> Result<RecordBatch> {
    let bytes = std::fs::read(path)?;
    let entries = parse_index(path, &bytes)?;
    let selected = groups
        .filter_map(|index| entries.get(index))
        .collect::<Vec<_>>();
    let mut batches = Vec::with_capacity(selected.len());
    for entry in selected {
        let start = checked(usize::try_from(entry.byte_offset))?;
        let end = start
            .checked_add(checked(usize::try_from(entry.byte_len))?)
            .ok_or_else(|| {
                StorageError::CorruptDataFile(
                    path.to_path_buf(),
                    "row-group payload overflow".to_owned(),
                )
            })?;
        let payload = bytes.get(start..end).ok_or_else(|| {
            StorageError::CorruptDataFile(
                path.to_path_buf(),
                "row-group payload outside file".to_owned(),
            )
        })?;
        batches.push(if columns.is_empty() {
            read_batch_from_bytes(path, payload)?
        } else {
            read_projected(path, payload, columns)?
        });
    }
    if batches.is_empty() {
        return Err(StorageError::EmptyDataFile(path.to_path_buf()));
    }
    let schema = batches[0].schema();
    Ok(concat_batches(&schema, &batches)?)
}

/// Returns the indexed row-group metadata without decoding any Arrow payload.
#[allow(clippy::missing_errors_doc)]
pub fn row_group_index(path: &Path) -> Result<Vec<RowGroupEntry>> {
    parse_index(path, &std::fs::read(path)?)
}

fn parse_index(path: &Path, bytes: &[u8]) -> Result<Vec<RowGroupEntry>> {
    if bytes.len() < 16
        || &bytes[..8] != MAGIC
        || u32::from_le_bytes(checked(bytes[8..12].try_into())?) != 1
    {
        return Err(StorageError::CorruptDataFile(
            path.to_path_buf(),
            "invalid row-group header".to_owned(),
        ));
    }
    let count = checked(usize::try_from(u32::from_le_bytes(checked(
        bytes[12..16].try_into(),
    )?)))?;
    let index_end = 16usize
        .checked_add(ENTRY_BYTES.checked_mul(count).ok_or_else(|| {
            StorageError::CorruptDataFile(path.to_path_buf(), "row-group index overflow".to_owned())
        })?)
        .ok_or_else(|| {
            StorageError::CorruptDataFile(path.to_path_buf(), "row-group index overflow".to_owned())
        })?;
    if index_end > bytes.len() {
        return Err(StorageError::CorruptDataFile(
            path.to_path_buf(),
            "row-group index outside file".to_owned(),
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut previous_offset = checked(u64::try_from(index_end))?;
    let mut previous_row_end = 0_u64;
    for chunk in bytes[16..index_end].chunks_exact(ENTRY_BYTES) {
        let mut values = [0_u64; 4];
        for (value, encoded) in values.iter_mut().zip(chunk.chunks_exact(8)) {
            *value = u64::from_le_bytes(checked(encoded.try_into())?);
        }
        let payload_end = values[0].checked_add(values[1]).ok_or_else(|| {
            StorageError::CorruptDataFile(
                path.to_path_buf(),
                "row-group payload offset overflow".to_owned(),
            )
        })?;
        let file_len = checked(u64::try_from(bytes.len()))?;
        if values[0] != previous_offset
            || payload_end > file_len
            || values[2] != previous_row_end
            || values[3] < values[2]
            || (values[3] == values[2] && !(count == 1 && values[2] == 0))
        {
            return Err(StorageError::CorruptDataFile(
                path.to_path_buf(),
                "row-group index is not contiguous and monotonic".to_owned(),
            ));
        }
        previous_offset = payload_end;
        previous_row_end = values[3];
        entries.push(RowGroupEntry {
            byte_offset: values[0],
            byte_len: values[1],
            row_start: values[2],
            row_end: values[3],
        });
    }
    if previous_offset != checked(u64::try_from(bytes.len()))? {
        return Err(StorageError::CorruptDataFile(
            path.to_path_buf(),
            "row-group index does not cover the payload".to_owned(),
        ));
    }
    Ok(entries)
}

fn read_projected(path: &Path, bytes: &[u8], columns: &[&str]) -> Result<RecordBatch> {
    let schema = FileReader::try_new(Cursor::new(bytes), None)?.schema();
    let projection = columns
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), Some(projection))?;
    reader
        .next()
        .ok_or_else(|| StorageError::EmptyDataFile(path.to_path_buf()))?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn indexed_groups_skip_unrequested_rows_and_project_columns() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let path = dir.path().join("groups.arrow");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
            ],
        )
        .unwrap_or_else(|_| unreachable!());
        write_row_groups(&path, &batch, 2).unwrap_or_else(|_| unreachable!());
        let index = row_group_index(&path).unwrap_or_else(|_| unreachable!());
        assert_eq!(index.len(), 3);
        assert_eq!((index[1].row_start, index[1].row_end), (2, 4));
        let selected = read_row_groups(&path, &["id"], 1..2).unwrap_or_else(|_| unreachable!());
        assert_eq!(selected.num_rows(), 2);
        assert_eq!(selected.num_columns(), 1);
        let ids = selected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| unreachable!());
        assert_eq!(ids.values(), &[2, 3]);
    }

    #[test]
    fn indexed_groups_reject_noncontiguous_or_overlapping_index() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let path = dir.path().join("groups.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))])
                .unwrap_or_else(|_| unreachable!());
        write_row_groups(&path, &batch, 2).unwrap_or_else(|_| unreachable!());
        let mut bytes = std::fs::read(&path).unwrap_or_else(|_| unreachable!());
        bytes[16..24].fill(0);
        assert!(parse_index(&path, &bytes).is_err());
    }

    #[test]
    fn indexed_groups_round_trip_empty_batch() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let path = dir.path().join("empty.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        write_row_groups(&path, &batch, 2).unwrap_or_else(|_| unreachable!());
        let index = row_group_index(&path).unwrap_or_else(|_| unreachable!());
        assert_eq!((index[0].row_start, index[0].row_end), (0, 0));
        let loaded = read_row_groups(&path, &["id"], 0..1).unwrap_or_else(|_| unreachable!());
        assert_eq!(loaded.num_rows(), 0);
    }
}
