//! Columnar file format, manifest/versioning. See
//! `.claude/docs/design/phase-0-transaction-and-format-spec.md`.

pub mod datafile;
pub mod error;
pub mod manifest;

pub use datafile::{read_batch, sync_dir, write_batch};
pub use error::{Result, StorageError};
pub use manifest::{Manifest, commit_manifest, read_current};
