//! `LocalFs`: a [`crate::backend::Backend`] implementation rooted at a local
//! directory. Reproduces today's `write_bytes`/`commit_manifest`
//! tmp-write-then-fsync-then-rename durability dance behind the `Backend`
//! trait's `key: &str` / `bytes: &[u8]` interface.

use std::fs::{self, File};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
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

    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let resolved = self.resolve(key);
        let mut file = File::open(&resolved)?;

        // Validate before allocating: `range` may come from on-disk data
        // (a footer/manifest offset) rather than a trusted caller, so a
        // reversed or past-EOF range must become a typed error here, not
        // a raw `u64` subtraction underflow (panics in debug, wraps in
        // release) or an allocation sized from that wrapped value.
        let file_len = file.metadata()?.len();
        let span_len = range
            .end
            .checked_sub(range.start)
            .filter(|_| range.end <= file_len)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "range {}..{} is invalid or extends past {}'s length ({file_len})",
                        range.start,
                        range.end,
                        resolved.display()
                    ),
                )
            })?;
        let len = usize::try_from(span_len)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        file.seek(SeekFrom::Start(range.start))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
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

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
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
        crate::chaos::chaos_checkpoint(); // tmp object is durable, about to link into place

        // `hard_link` is atomic w.r.t. the existence check on both POSIX
        // (link(2)) and Windows (CreateHardLinkW): it never exposes
        // partial content at `final_path`, and fails with `AlreadyExists`
        // rather than silently overwriting -- unlike `rename`, which
        // always overwrites unconditionally. The tmp file is removed
        // either way; on success it was only ever a second name for the
        // same durable content, never the only copy.
        let link_result = fs::hard_link(&tmp_path, &final_path);
        let _ = fs::remove_file(&tmp_path);
        match link_result {
            Ok(()) => {
                crate::chaos::chaos_checkpoint(); // linked into place; now discoverable
                // See `put`'s matching comment: fsync the containing
                // directory so the new hard link survives a crash, not
                // just the file content. `sync_dir` performs its own
                // chaos checkpoint internally.
                if let Some(parent) = final_path.parent() {
                    crate::datafile::sync_dir(parent)?;
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(crate::error::StorageError::AlreadyExists(key.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::error::StorageError;

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

    #[test]
    fn get_range_reads_only_the_requested_byte_span() {
        let root = temp_root("range");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"0123456789").unwrap();

        let slice = backend.get_range("a.bin", 3..6).unwrap();

        assert_eq!(slice, b"345");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn get_range_errors_on_reversed_range() {
        let root = temp_root("range-reversed");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"0123456789").unwrap();

        let result = backend.get_range("a.bin", 6..3);

        assert!(result.is_err());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn get_range_errors_on_range_extending_past_eof() {
        let root = temp_root("range-eof");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"0123456789").unwrap();

        let result = backend.get_range("a.bin", 5..15);

        assert!(result.is_err());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_if_absent_succeeds_on_a_fresh_key() {
        let root = temp_root("if-absent-fresh");
        let backend = LocalFs::new(&root);

        backend.put_if_absent("a.bin", b"first").unwrap();

        assert_eq!(backend.get("a.bin").unwrap(), b"first");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_if_absent_errors_on_a_key_that_already_exists() {
        let root = temp_root("if-absent-collision");
        let backend = LocalFs::new(&root);
        backend.put_if_absent("a.bin", b"first").unwrap();

        let result = backend.put_if_absent("a.bin", b"second");

        assert!(
            matches!(result, Err(StorageError::AlreadyExists(ref k)) if k == "a.bin"),
            "expected AlreadyExists(\"a.bin\"), got {result:?}"
        );
        // The original content must be untouched by the failed attempt.
        assert_eq!(backend.get("a.bin").unwrap(), b"first");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_if_absent_under_concurrent_racers_exactly_one_wins() {
        let root = temp_root("if-absent-race");
        let backend = LocalFs::new(&root);

        let (ok_count, winner_bytes) = std::thread::scope(|scope| {
            let backend_ref = &backend;
            let h1 = scope.spawn(move || backend_ref.put_if_absent("a.bin", b"writer-one"));
            let h2 = scope.spawn(move || backend_ref.put_if_absent("a.bin", b"writer-two"));
            let r1 = h1.join().unwrap();
            let r2 = h2.join().unwrap();

            let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
            let winner_bytes = backend_ref.get("a.bin").unwrap();
            (ok_count, winner_bytes)
        });

        assert_eq!(ok_count, 1, "exactly one racer must succeed");
        assert!(
            winner_bytes == b"writer-one" || winner_bytes == b"writer-two",
            "final content must be exactly one racer's payload, got {winner_bytes:?}"
        );
        fs::remove_dir_all(&root).ok();
    }
}
