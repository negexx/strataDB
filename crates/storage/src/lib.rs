//! Columnar file format, manifest/versioning. See
//! `docs/design/phase-0-transaction-and-format-spec.md`.

pub mod backend;
pub mod chaos;
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod stats;

pub use arrow;
pub use backend::{Backend, LocalFs, ObjectMeta};
pub use datafile::{
    WriteMetadata, crc32c_checksum, read_batch, read_batch_columns, sync_dir, write_batch,
    write_bytes,
};
pub use encoding::encode_batch;
pub use error::{Result, StorageError};
pub use manifest::{
    DataFileEntry, MANIFEST_FORMAT_VERSION, Manifest, ManifestEnvelope, SegmentEntry,
    commit_manifest, read_current,
};
pub use stats::{ColumnStats, Value, compute_stats};
