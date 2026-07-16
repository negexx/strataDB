use std::path::PathBuf;

use thiserror::Error;

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
}

pub type Result<T> = std::result::Result<T, StorageError>;
