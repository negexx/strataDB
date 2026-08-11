use std::path::PathBuf;

use thiserror::Error;

/// # Examples
///
/// ```
/// use strata_txn::TxnError;
///
/// let err = TxnError::SchemaMismatch { expected: 3, actual: 2 };
/// assert_eq!(
///     err.to_string(),
///     "schema mismatch casting a data file: expected 3 columns, found 2"
/// );
/// ```
#[derive(Debug, Error)]
pub enum TxnError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] strata_storage::StorageError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("a dataset already exists at {0} — use Dataset::open instead")]
    AlreadyExists(PathBuf),
    #[error("no dataset found at {0} — call Dataset::create first")]
    NotFound(PathBuf),
    #[error("row count overflowed u64: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Index(#[from] strata_index::IndexError),
    #[error(
        "row {row_id}'s vector contains a non-finite component (NaN or Infinity) — cannot be committed"
    )]
    NonFiniteVectorComponent { row_id: u64 },
    #[error("manifest arithmetic would overflow: {0}")]
    ManifestOverflow(String),
    #[error(
        "row-id reservation through {end} may be visible but its durable confirmation failed: {source}"
    )]
    RowIdReservationDurability {
        end: u64,
        #[source]
        source: strata_storage::StorageError,
    },
    #[error(
        "manifest declares an unreasonably large row-id capacity ({0}); maximum allowed is {1}"
    )]
    UnreasonableCapacity(u64, u64),
    #[error("manifest references an unsafe file path: {0:?}")]
    UnsafeManifestPath(String),
    #[error("segment listed in the manifest is unusable: {0}")]
    CorruptSegment(String),
    #[error("schema mismatch casting a data file: expected {expected} columns, found {actual}")]
    SchemaMismatch { expected: usize, actual: usize },
    #[error(
        "batch schema does not match the dataset-owned schema: expected {expected:?}, found {actual:?}"
    )]
    BatchSchemaMismatch { expected: String, actual: String },
    #[error("row {row_id} is not owned by the transaction's base snapshot")]
    RowNotFound { row_id: u64 },
    #[error("row {row_id} is already tombstoned in the transaction's base snapshot")]
    RowNotLive { row_id: u64 },
    #[error("row {row_id} is already targeted by this transaction")]
    DuplicateTarget { row_id: u64 },
    #[error("an update replacement must contain exactly one row, found {actual_rows}")]
    InvalidUpdateShape { actual_rows: usize },
    #[error("conflict: {contested_row_ids:?} were modified by another transaction")]
    Conflict { contested_row_ids: Vec<u64> },
    #[error(
        "commit history for base version {base_version} was evicted; retained history starts at version {oldest_retained_version} and the latest version is {latest_version}"
    )]
    InsufficientHistory {
        base_version: u64,
        oldest_retained_version: u64,
        latest_version: u64,
    },
    /// The row-id range a transaction claimed does not match the rows it
    /// actually laid out — spec §8's "gaps are safe, reuse is forbidden"
    /// invariant. `claimed_end` is one past the last claimed row-id
    /// (`base + len`); `actual_end` is one past the last row-id the
    /// transaction wrote. A divergence means the transaction handed out
    /// row-ids *past* its claim — ids some other transaction's claim may
    /// already cover — so this is surfaced as a hard error rather than left
    /// to a `debug_assert` that release builds silently drop.
    #[error(
        "row-id range mismatch: the claim ended at row-id {claimed_end} but the transaction \
         laid out rows through row-id {actual_end} — spec §8 forbids row-id reuse (gaps are \
         safe, reuse is forbidden)"
    )]
    RowIdRangeMismatch { claimed_end: u64, actual_end: u64 },
    #[error(
        "column name '{0}' is reserved for internal use and cannot appear in an inserted batch's schema"
    )]
    ReservedColumnName(String),
    #[error("system clock error: {0}")]
    Clock(String),
    #[error("keep_latest_versions must be at least one")]
    InvalidRetentionPolicy,
}

pub type Result<T> = std::result::Result<T, TxnError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_error_variants_format_with_their_context() {
        assert_eq!(
            TxnError::ManifestOverflow("next_row_id".to_string()).to_string(),
            "manifest arithmetic would overflow: next_row_id"
        );
        assert_eq!(
            TxnError::UnreasonableCapacity(5_000_000_000, 1_000_000_000).to_string(),
            "manifest declares an unreasonably large row-id capacity (5000000000); maximum allowed is 1000000000"
        );
        assert_eq!(
            TxnError::RowIdReservationDurability {
                end: 7,
                source: strata_storage::StorageError::Io(std::io::Error::other("sync failed")),
            }
            .to_string(),
            "row-id reservation through 7 may be visible but its durable confirmation failed: I/O error: sync failed"
        );
        assert_eq!(
            TxnError::UnsafeManifestPath("../escape".to_string()).to_string(),
            "manifest references an unsafe file path: \"../escape\""
        );
        assert_eq!(
            TxnError::SchemaMismatch {
                expected: 3,
                actual: 2
            }
            .to_string(),
            "schema mismatch casting a data file: expected 3 columns, found 2"
        );
        assert_eq!(
            TxnError::Clock("second time provided was later than self".to_string()).to_string(),
            "system clock error: second time provided was later than self"
        );
        assert_eq!(
            TxnError::ReservedColumnName("_row_id".to_string()).to_string(),
            "column name '_row_id' is reserved for internal use and cannot appear in an inserted batch's schema"
        );
        assert_eq!(
            TxnError::RowIdRangeMismatch {
                claimed_end: 5,
                actual_end: 8,
            }
            .to_string(),
            "row-id range mismatch: the claim ended at row-id 5 but the transaction laid out \
             rows through row-id 8 — spec §8 forbids row-id reuse (gaps are safe, reuse is \
             forbidden)"
        );
    }

    #[test]
    fn conflict_error_names_contested_rows() {
        let err = TxnError::Conflict {
            contested_row_ids: vec![5, 9],
        };
        assert_eq!(
            err.to_string(),
            "conflict: [5, 9] were modified by another transaction"
        );
    }
}
