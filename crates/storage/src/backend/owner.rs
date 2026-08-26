//! Dataset storage ownership and key layout.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Backend, LocalFs, ObjectMeta};
use crate::error::{Result, StorageError};

/// Validated namespace prefix for an object-backed dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetPrefix(String);

/// Validated backend key owned by a dataset layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetKey(String);

impl DatasetPrefix {
    /// # Errors
    /// Returns an invalid-input error when the prefix contains unsafe path components.
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return Ok(Self(prefix));
        }
        validate_key_components(&prefix, true)?;
        Ok(Self(prefix))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl DatasetKey {
    /// # Errors
    /// Returns an invalid-input error when the key contains unsafe path components.
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        validate_key_components(&key, false)?;
        Ok(Self(key))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_key_components(key: &str, allow_empty: bool) -> Result<()> {
    if (!allow_empty && key.is_empty()) || key.starts_with('/') || key.contains('\\') {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid storage key",
        )));
    }
    if key
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid storage key component",
        )));
    }
    Ok(())
}

/// Owns the backend capability and the dataset root used to interpret keys.
///
/// Keeping these together prevents transaction code from independently
/// reconstructing backend roots and object keys. Existing path-based APIs
/// remain available; new lifecycle code can carry this owner as one value.
#[derive(Clone)]
pub struct StorageOwner {
    root: PathBuf,
    backend: Arc<dyn Backend>,
    prefix: DatasetPrefix,
}

impl StorageOwner {
    /// Creates an owner backed by the local filesystem.
    #[must_use]
    pub fn local(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            backend: Arc::new(LocalFs::new(&root)),
            root,
            prefix: DatasetPrefix(String::new()),
        }
    }

    /// Creates an owner for a backend namespace that is not local-path rooted.
    pub fn from_backend(backend: Arc<dyn Backend>, prefix: DatasetPrefix) -> Self {
        Self {
            root: PathBuf::new(),
            backend,
            prefix,
        }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns whether this owner has a local filesystem root. Object-backed
    /// owners intentionally have no path that callers may use for I/O.
    #[must_use]
    pub fn is_local(&self) -> bool {
        !self.root.as_os_str().is_empty()
    }

    #[must_use]
    pub fn backend(&self) -> &dyn Backend {
        self.backend.as_ref()
    }

    #[must_use]
    pub fn backend_arc(&self) -> Arc<dyn Backend> {
        Arc::clone(&self.backend)
    }

    #[must_use]
    pub fn manifest_object_key(&self, version: u64) -> DatasetKey {
        Self::object_key(&format!("_versions/{version:020}.manifest"))
    }

    /// # Errors
    /// Returns an invalid-input error when `name` is not a relative object name.
    pub fn data_object_key(&self, name: &str) -> Result<DatasetKey> {
        validate_key_components(name, false)?;
        Ok(Self::object_key(&format!("data/{name}")))
    }

    #[must_use]
    pub fn row_id_high_water_key(&self, end: u64) -> DatasetKey {
        Self::object_key(&format!("_meta/row-id-high-water/{end:020}.reservation"))
    }

    fn object_key(suffix: &str) -> DatasetKey {
        DatasetKey(suffix.to_owned())
    }

    fn prefixed(&self, suffix: &str) -> String {
        if self.prefix.as_str().is_empty() {
            suffix.to_owned()
        } else if suffix.is_empty() {
            self.prefix.as_str().to_owned()
        } else {
            format!("{}/{}", self.prefix.as_str(), suffix)
        }
    }

    /// # Errors
    /// Propagates the backend read error.
    pub fn get(&self, key: &DatasetKey) -> Result<Vec<u8>> {
        self.backend.get(&self.prefixed(key.as_str()))
    }
    /// # Errors
    /// Propagates the backend range-read error.
    pub fn get_range(&self, key: &DatasetKey, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        self.backend.get_range(&self.prefixed(key.as_str()), range)
    }
    /// # Errors
    /// Propagates the backend durable-write error.
    pub fn put(&self, key: &DatasetKey, bytes: &[u8]) -> Result<()> {
        self.backend.put(&self.prefixed(key.as_str()), bytes)
    }
    /// # Errors
    /// Propagates the backend deletion error.
    pub fn delete(&self, key: &DatasetKey) -> Result<()> {
        self.backend.delete(&self.prefixed(key.as_str()))
    }

    /// # Errors
    /// Propagates backend listing errors and rejects objects outside the owner namespace.
    pub fn list(&self, suffix: &str) -> Result<Vec<ObjectMeta>> {
        if !suffix.is_empty() {
            validate_key_components(suffix, true)?;
        }
        let namespace = self.prefix.as_str();
        self.backend
            .list(&self.prefixed(suffix))?
            .into_iter()
            .map(|object| {
                let key = if namespace.is_empty() {
                    object.key
                } else {
                    object
                        .key
                        .strip_prefix(namespace)
                        .and_then(|key| key.strip_prefix('/'))
                        .ok_or_else(|| {
                            StorageError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "backend returned an object outside the owner namespace",
                            ))
                        })?
                        .to_owned()
                };
                Ok(ObjectMeta {
                    key,
                    size: object.size,
                })
            })
            .collect()
    }

    /// # Errors
    /// Propagates the backend immutable-publication error.
    pub fn put_if_absent(&self, key: &DatasetKey, bytes: &[u8]) -> Result<()> {
        self.backend
            .put_if_absent(&self.prefixed(key.as_str()), bytes)
    }

    /// Returns the local path used by the compatibility path-based APIs.
    /// # Errors
    /// Returns an invalid-input error for traversal or absolute keys.
    pub fn local_path(&self, key: &str) -> Result<PathBuf> {
        validate_key_components(key, false)?;
        if !self.is_local() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "owner has no local filesystem path",
            )));
        }
        Ok(self
            .root
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_keeps_root_and_canonical_key_layout_together() {
        let owner = StorageOwner::local("dataset-root");
        assert_eq!(owner.root(), Path::new("dataset-root"));
        let data_key = owner
            .data_object_key("7.seg")
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(data_key.as_str(), "data/7.seg");
        assert_eq!(
            owner.manifest_object_key(7).as_str(),
            "_versions/00000000000000000007.manifest"
        );
        let data_path = owner
            .local_path("data/7.seg")
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(data_path, PathBuf::from("dataset-root/data/7.seg"));
        assert_eq!(
            owner.manifest_object_key(7).as_str(),
            "_versions/00000000000000000007.manifest"
        );
        assert!(DatasetPrefix::new("a/../b").is_err());
        assert!(owner.local_path("../outside").is_err());
    }

    #[test]
    fn key_types_reject_unsafe_or_ambiguous_components() {
        for key in ["", "/absolute", "a//b", "a/./b", "a/../b", "a\\b", "a/"] {
            assert!(
                DatasetKey::new(key).is_err(),
                "key {key:?} must be rejected"
            );
        }
        for prefix in ["/absolute", "a//b", "a/./b", "a/../b", "a\\b", "a/"] {
            assert!(
                DatasetPrefix::new(prefix).is_err(),
                "prefix {prefix:?} must be rejected"
            );
        }
        let key = DatasetKey::new("data/part.arrow").unwrap_or_else(|_| unreachable!());
        assert_eq!(key.as_str(), "data/part.arrow");
        let prefix = DatasetPrefix::new("tenant/dataset").unwrap_or_else(|_| unreachable!());
        assert_eq!(prefix.as_str(), "tenant/dataset");
    }
}
