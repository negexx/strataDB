use std::path::PathBuf;

use thiserror::Error;

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
}

pub type Result<T> = std::result::Result<T, TxnError>;
