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
        "manifest declares an unreasonably large row-id capacity ({0}); maximum allowed is {1}"
    )]
    UnreasonableCapacity(u64, u64),
    #[error("manifest references an unsafe file path: {0:?}")]
    UnsafeManifestPath(String),
    #[error("segment listed in the manifest is unusable: {0}")]
    CorruptSegment(String),
    #[error("schema mismatch casting a data file: expected {expected} columns, found {actual}")]
    SchemaMismatch { expected: usize, actual: usize },
    #[error("conflict: {contested_row_ids:?} were modified by another transaction")]
    Conflict { contested_row_ids: Vec<u64> },
    #[error(
        "column name '{0}' is reserved for internal use and cannot appear in an inserted batch's schema"
    )]
    ReservedColumnName(String),
    #[error("system clock error: {0}")]
    Clock(String),
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
