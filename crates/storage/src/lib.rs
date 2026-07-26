//! Columnar file format, manifest/versioning. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md`.

pub mod chaos;
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod stats;

pub use arrow;
pub use datafile::{read_batch, read_batch_columns, sync_dir, write_batch, write_bytes};
pub use encoding::encode_batch;
pub use error::{Result, StorageError};
pub use manifest::{DataFileEntry, Manifest, SegmentEntry, commit_manifest, read_current};
pub use stats::{ColumnStats, Value, compute_stats};
