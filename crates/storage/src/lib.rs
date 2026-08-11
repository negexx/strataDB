//! Internal columnar storage, manifest/versioning, and local durability.
//!
//! `Dataset`, `Snapshot`, and `Transaction` in `strata-txn` are the supported
//! engine facade. Direct storage use is an implementation surface and does
//! not carry the facade's schema, conflict, or recovery guarantees.

pub mod backend;
pub mod chaos;
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod row_id_high_water;
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
    commit_manifest, read_current, read_current_with_byte_count,
    read_manifest_at_key_with_byte_count, read_manifest_with_byte_count,
};
#[cfg(feature = "test-fault-injection")]
pub use row_id_high_water::test_support::set_after_row_id_read_hook;
pub use row_id_high_water::{
    HighWaterPersistenceError, ROW_ID_HIGH_WATER_PREFIX, RowIdHighWaterRead,
    initialize_row_id_high_water, persist_row_id_high_water_at_least, read_row_id_high_water,
    read_row_id_high_water_with_byte_count,
};
pub use stats::{ColumnStats, Value, compute_stats};
