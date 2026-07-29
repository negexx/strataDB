//! The storage-backend abstraction. `Backend` is the seam between
//! `crates/storage`'s file-format/manifest logic and where bytes actually
//! live — local disk (`local::LocalFs`, this milestone) or, in a later
//! milestone, S3-compatible object storage. See
//! `docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md`.

pub mod local;

use crate::error::Result;

/// One object's key and size, as returned by [`Backend::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
}

/// Fully synchronous, object-safe. `put`/`put_if_absent` return only once
/// the write is durable — no async buffering, ever, matching
/// `.claude/rules/concurrency-txn-layer.md`'s durability invariant
/// regardless of which `Backend` impl is in play.
pub trait Backend: Send + Sync {
    /// Reads the full contents of `key`.
    ///
    /// # Errors
    /// Returns an error if `key` does not exist or can't be read.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Writes `bytes` to `key`, durably, overwriting any existing content —
    /// truncate-and-replace semantics, matching today's `File::create`-based
    /// writers.
    ///
    /// # Errors
    /// Returns an error if the write can't complete durably.
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
}

pub use local::LocalFs;
