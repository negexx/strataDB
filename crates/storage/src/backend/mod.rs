//! The storage-backend abstraction. `Backend` is the seam between
//! `crates/storage`'s file-format/manifest logic and where bytes actually
//! live — local disk (`local::LocalFs`, this milestone) or, in a later
//! milestone, S3-compatible object storage. See
//! `docs/roadmap.md`.

pub mod local;
pub mod owner;

#[cfg(test)]
mod conformance;

use crate::error::Result;

/// One object's key and size, as returned by [`Backend::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
}

/// Fully synchronous, object-safe. `put`/`put_if_absent` return only once
/// the write is durable — no async buffering, ever, matching
/// `docs/design.md`'s durability invariant
/// regardless of which `Backend` impl is in play.
///
/// # Key contract
///
/// A `key` is a relative, `/`-delimited, non-empty string. It must not
/// contain a `.` or `..` path component. No normalization is performed —
/// an implementation rejects a key that violates this rather than
/// silently reinterpreting it (e.g. `LocalFs` must not silently collapse
/// `"a/../b"` to `"b"`, or let an absolute-looking key escape its root).
/// This keeps key identity consistent across backends: a real object
/// store (e.g. S3) treats these as literal, distinct key strings with no
/// path semantics at all.
///
/// Note: [`Backend::list`]'s `prefix` parameter is exempt from the
/// "non-empty" and "no normalization" rules — `list("")` is valid and
/// means "everything."
pub trait Backend: Send + Sync {
    /// Reads the full contents of `key`.
    ///
    /// # Errors
    /// Returns an error if `key` does not exist or can't be read.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Reads `range` (byte offsets, exclusive end) from `key`.
    ///
    /// # Errors
    /// Returns an error if `key` does not exist, or `range` extends past
    /// its length.
    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>>;

    /// Writes `bytes` to `key`, durably, overwriting any existing content —
    /// truncate-and-replace semantics, matching today's `File::create`-based
    /// writers.
    ///
    /// # Errors
    /// Returns an error if the write can't complete durably.
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Writes `bytes` to `key` only if `key` does not already exist —
    /// atomic create-if-absent, the primitive Strata's manifest commit path
    /// uses in place of the `rename`-based CAS local disk doesn't need but
    /// object storage does. See
    /// `docs/roadmap.md`.
    ///
    /// # Errors
    /// Returns [`crate::error::StorageError::AlreadyExists`] if `key`
    /// already exists; any other error if the write can't complete.
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Lists every object whose key starts with `prefix`, sorted
    /// lexicographically by key. Recurses into nested keys the way an
    /// object store's flat, `/`-delimited namespace does — there is no
    /// concept of "one directory level" here.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be listed.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Removes a `key` that satisfies the [key contract](Backend#key-contract).
    ///
    /// # Errors
    ///
    /// Returns an error when `key` violates the key contract. `LocalFs`
    /// reports that case as [`crate::error::StorageError::Io`] with
    /// [`std::io::ErrorKind::InvalidInput`].
    ///
    /// Whether deleting an already-missing key errors is
    /// implementation-defined: `LocalFs` errors (POSIX `unlink` semantics),
    /// but an S3-compatible backend's `DeleteObject` is idempotent and
    /// would return success. Callers must not depend on either behavior.
    ///
    /// `LocalFs` returns only after unlinking the validated key and
    /// synchronizing its owned containing-directory chain through the
    /// configured backend root. A directory-sync error after unlink remains
    /// an error: the key may already be absent, but deletion durability is
    /// uncertain, so callers must not treat it as either acknowledged durable
    /// completion or a failed unlink. This is a bounded local-filesystem
    /// contract, not a cross-process or universal power-loss guarantee.
    fn delete(&self, key: &str) -> Result<()>;
}

pub use local::LocalFs;
pub use owner::{DatasetKey, DatasetPrefix, StorageOwner};
