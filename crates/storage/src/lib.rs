//! Internal columnar storage, manifest/versioning, and local durability.
//!
//! `Dataset`, `Snapshot`, and `Transaction` in `strata-txn` are the supported
//! engine facade. Direct storage use is an implementation surface and does
//! not carry the facade's schema, conflict, or recovery guarantees.

#![forbid(unsafe_code)]

pub mod backend;
pub mod chaos;
pub mod datafile;
pub mod encoding;
pub mod error;
pub mod manifest;
pub mod row_group;
pub mod row_id_high_water;
pub mod schema;
pub mod stats;

pub use arrow;
pub use backend::{Backend, DatasetKey, DatasetPrefix, LocalFs, ObjectMeta, StorageOwner};
pub use datafile::{
    WriteMetadata, crc32c_checksum, read_batch, read_batch_columns, read_batch_columns_with,
    read_batch_with, sync_dir, write_batch, write_batch_with, write_bytes, write_bytes_with,
};
pub use encoding::encode_batch;
pub use error::{Result, StorageError};
pub use manifest::{
    DataFileEntry, MANIFEST_FORMAT_VERSION, Manifest, ManifestEnvelope, SegmentEntry,
    commit_manifest, commit_manifest_with, read_current, read_current_with,
    read_current_with_byte_count, read_current_with_byte_count_with,
    read_manifest_at_key_with_byte_count, read_manifest_at_key_with_byte_count_and_size_with,
    read_manifest_at_key_with_byte_count_with, read_manifest_with_byte_count,
};
pub use row_group::{RowGroupEntry, read_row_groups, row_group_index, write_row_groups};
#[cfg(feature = "test-fault-injection")]
pub use row_id_high_water::test_support::set_after_row_id_read_hook;
pub use row_id_high_water::{
    HighWaterPersistenceError, ROW_ID_HIGH_WATER_PREFIX, RowIdHighWaterRead,
    initialize_row_id_high_water, initialize_row_id_high_water_with,
    persist_row_id_high_water_at_least, persist_row_id_high_water_at_least_with,
    read_row_id_high_water, read_row_id_high_water_with, read_row_id_high_water_with_byte_count,
    read_row_id_high_water_with_byte_count_with,
};
pub use schema::{
    ADD_NULLABLE_COLUMN_SCHEMA_VERSION, INITIAL_SCHEMA_VERSION, SchemaMigration,
    SchemaMigrationResult, validate_schema_version,
};
pub use stats::{ColumnStats, Value, compute_stats};
