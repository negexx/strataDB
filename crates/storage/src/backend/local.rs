//! `LocalFs`: a [`crate::backend::Backend`] implementation rooted at a local
//! directory. Reproduces today's `write_bytes`/`commit_manifest`
//! tmp-write-then-fsync-then-rename durability dance behind the `Backend`
//! trait's `key: &str` / `bytes: &[u8]` interface.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::Backend;
use crate::error::Result;

pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// A unique, colocated temp filename for `final_path` — colocated so
    /// the eventual `rename` stays within one filesystem/volume. Uses a
    /// process-global counter plus the process id rather than a new
    /// dependency (no `rand`/`uuid`), matching this crate's existing
    /// `.tmp-{version}` naming convention (see `manifest.rs`).
    fn tmp_path_for(final_path: &Path) -> PathBuf {
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let file_name = final_path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("tmp");
        final_path.with_file_name(format!(".tmp-{pid}-{n}-{file_name}"))
    }
}

impl Backend for LocalFs {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(fs::read(self.resolve(key))?)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let final_path = self.resolve(key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = Self::tmp_path_for(&final_path);
        {
            let mut tmp_file = File::create(&tmp_path)?;
            tmp_file.write_all(bytes)?;
            tmp_file.sync_all()?;
        }
        crate::chaos::chaos_checkpoint(); // tmp object is durable, about to rename into place
        fs::rename(&tmp_path, &final_path)?;
        crate::chaos::chaos_checkpoint(); // renamed into place; now discoverable by content
        // Fsync the containing directory so the rename itself survives a
        // crash, not just the file content -- matching `commit_manifest`'s
        // existing `sync_dir` step today. Folded in here (rather than left
        // as a separate caller-side step, as `commit_manifest` used to do)
        // so `Backend::put`'s durability contract is self-contained and
        // uniform across backends: S3 has no analogous directory-entry
        // step, so a caller shouldn't need backend-specific knowledge to
        // get full durability out of `put` alone. `sync_dir` performs its
        // own chaos checkpoint internally (see `datafile.rs`), so this
        // contributes the 3rd checkpoint here, matching the 3 checkpoints
        // `commit_manifest` + its old explicit `sync_dir` call produced
        // together today -- see Task 6, which removes that now-redundant
        // explicit call.
        if let Some(parent) = final_path.parent() {
            crate::datafile::sync_dir(parent)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        tempfile::Builder::new()
            .prefix(&format!("strata-localfs-test-{label}-"))
            .tempdir()
            .unwrap()
            .keep()
    }

    #[test]
    fn put_then_get_round_trips_bytes_exactly() {
        let root = temp_root("roundtrip");
        let backend = LocalFs::new(&root);

        backend.put("a.bin", b"hello world").unwrap();
        let read_back = backend.get("a.bin").unwrap();

        assert_eq!(read_back, b"hello world");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_creates_parent_directories_for_a_nested_key() {
        let root = temp_root("nested");
        let backend = LocalFs::new(&root);

        backend
            .put("_versions/00000000000000000001.manifest", b"{}")
            .unwrap();
        let read_back = backend
            .get("_versions/00000000000000000001.manifest")
            .unwrap();

        assert_eq!(read_back, b"{}");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_overwrites_an_existing_key() {
        let root = temp_root("overwrite");
        let backend = LocalFs::new(&root);

        backend.put("a.bin", b"first").unwrap();
        backend.put("a.bin", b"second").unwrap();

        assert_eq!(backend.get("a.bin").unwrap(), b"second");
        fs::remove_dir_all(&root).ok();
    }
}
