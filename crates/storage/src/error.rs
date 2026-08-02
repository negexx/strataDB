use std::path::PathBuf;

use thiserror::Error;

/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use strata_storage::StorageError;
///
/// let err = StorageError::EmptyDataFile(PathBuf::from("data/0001.arrow"));
/// assert_eq!(
///     err.to_string(),
///     "data file at data/0001.arrow contains no record batch"
/// );
/// ```
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("manifest serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("data file at {0} contains no record batch")]
    EmptyDataFile(PathBuf),
    #[error("corrupt manifest at {0}: {1}")]
    CorruptManifest(PathBuf, String),
    #[error("dataset at {0} uses a legacy manifest format and requires migration")]
    LegacyFormatNeedsMigration(PathBuf),
    #[error("dataset at {0} is missing its durable row-id high-water catalog")]
    MissingRowIdHighWater(PathBuf),
    #[error("corrupt data file at {0}: {1}")]
    CorruptDataFile(PathBuf, String),
    #[error("directory durability is unsupported for {0}")]
    DurabilityUnsupported(PathBuf),
    #[error("key already exists: {0}")]
    AlreadyExists(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
