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

    /// Verifies the configured root is an existing directory owned by this
    /// backend. `LocalFs` deliberately does not create it: its parent is the
    /// caller's durable anchor, outside this backend's bounded sync scope.
    fn validate_root(&self) -> Result<bool> {
        let metadata = match fs::symlink_metadata(&self.root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() {
            return Err(Self::invalid_input(format!(
                "LocalFs root {} must not be a symlink",
                self.root.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(Self::invalid_input(format!(
                "LocalFs root {} must be a directory",
                self.root.display()
            )));
        }
        Ok(true)
    }

    fn invalid_input(message: String) -> crate::error::StorageError {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into()
    }

    /// Validates that `key` matches the [`Backend`] trait's key contract:
    /// relative, non-empty, no `.`/`..` component. Called as the first
    /// step of every method taking a single `key: &str` (not `list`,
    /// whose `prefix` may legitimately be empty or partial) so a key that
    /// would otherwise be silently normalized or escape `root` via
    /// `resolve`'s plain `Path::join` is rejected instead.
    fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "key must not be empty",
            )
            .into());
        }
        // Check the raw string for empty segments, `.`, `..`, and doubled/trailing
        // separators that `Path::components()` would silently normalize away.
        if key
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("key {key:?} must not contain empty, '.', or '..' segments"),
            )
            .into());
        }
        let has_invalid_component = Path::new(key)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)));
        if has_invalid_component {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("key {key:?} must be relative with no '.'/'..' components"),
            )
            .into());
        }
        Ok(())
    }

    /// Checks the root and every existing component in `key` without ever
    /// following a symlink. A missing suffix is allowed for `put` and
    /// `put_if_absent`, which will create it after this validation.
    fn validate_existing_key_path(&self, key: &str, require_root: bool) -> Result<()> {
        if !self.validate_root()? {
            if require_root {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("LocalFs root {} does not exist", self.root.display()),
                )
                .into());
            }
            return Ok(());
        }
        let mut current = self.root.clone();
        for component in Path::new(key).components() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Self::invalid_input(format!(
                        "LocalFs key path component {} must not be a symlink",
                        current.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// `list` accepts an empty or partial prefix, so it cannot use the
    /// stricter key validator. It still validates every existing component
    /// the prefix addresses before walking the tree.
    fn validate_list_prefix(&self, prefix: &str) -> Result<bool> {
        if !self.validate_root()? {
            return Ok(false);
        }
        let mut current = self.root.clone();
        for component in Path::new(prefix).components() {
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(Self::invalid_input(format!(
                    "list prefix {prefix:?} must not contain path navigation components"
                )));
            }
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Self::invalid_input(format!(
                        "LocalFs list prefix component {} must not be a symlink",
                        current.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
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

    /// Converts a possibly-relative path to an absolute path without
    /// resolving symlinks. Both `root` and `final_path` use this helper so
    /// their lexical ancestry can be compared consistently.
    fn absolute_path(path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            Ok(std::env::current_dir()?.join(path))
        }
    }

    /// Synchronizes every directory containing `final_path`, from the
    /// immediate parent to this backend's configured `root`, inclusive.
    /// The leaf sync makes the object publication durable; each later sync
    /// makes the directory entry that led to the previous directory durable.
    /// `LocalFs` owns this bounded tree only, so it must never attempt to
    /// synchronize a parent of `root`.
    fn sync_containing_directory_chain(&self, final_path: &Path) -> Result<()> {
        let root = Self::absolute_path(&self.root)?;
        let final_path = Self::absolute_path(final_path)?;
        let mut current = final_path
            .parent()
            .ok_or_else(|| crate::error::StorageError::DurabilityUnsupported(final_path.clone()))?;

        if !current.starts_with(&root) {
            return Err(crate::error::StorageError::DurabilityUnsupported(
                final_path,
            ));
        }

        loop {
            crate::datafile::sync_dir(current)?;
            if current == root {
                return Ok(());
            }
            current = current
                .parent()
                .ok_or_else(|| crate::error::StorageError::DurabilityUnsupported(root.clone()))?;
        }
    }

    /// Recursively walks `dir` (relative to `root`), collecting every file
    /// whose `root`-relative, `/`-joined key starts with `prefix`. A
    /// missing `dir` (e.g. `root` itself never created because nothing has
    /// been `put` yet) is not an error — it just contributes no entries,
    /// matching an object store's "empty prefix" behavior.
    fn walk(
        root: &Path,
        dir: &Path,
        prefix: &str,
        out: &mut Vec<crate::backend::ObjectMeta>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            // `file_type()` reads the type from the readdir record itself
            // on Windows/Linux (no extra syscall), unlike `path.is_dir()`
            // which stats through symlinks -- this also stops an unbounded
            // recursion if a symlink cycle ever exists under `root`.
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(Self::invalid_input(format!(
                    "LocalFs key path component {} must not be a symlink",
                    path.display()
                )));
            }
            if file_type.is_dir() {
                let sub_key = Self::key_for(root, &path);
                // Prune: only descend if this subtree could contain
                // something matching `prefix` -- either the subtree is
                // already inside the target scope (`sub_key` starts with
                // `prefix`) or the target scope reaches further into this
                // subtree (`prefix` starts with `sub_key`). A sibling
                // directory that merely shares a textual prefix with
                // `prefix` (e.g. `_versions_backup` vs `_versions/`)
                // matches neither check and is skipped without a single
                // stat inside it -- this is what turns `list("_versions/")`
                // from O(every file in the dataset) into O(files actually
                // under `_versions/`).
                if sub_key.starts_with(prefix) || prefix.starts_with(&sub_key) {
                    Self::walk(root, &path, prefix, out)?;
                }
            } else {
                let key = Self::key_for(root, &path);
                if key.starts_with(prefix) {
                    let size = entry.metadata()?.len();
                    out.push(crate::backend::ObjectMeta { key, size });
                }
            }
        }
        Ok(())
    }

    /// Converts an absolute path under `root` into a `/`-joined key,
    /// regardless of platform path-separator conventions.
    fn key_for(root: &Path, path: &Path) -> String {
        path.strip_prefix(root)
            .unwrap_or(path)
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }
}

impl Backend for LocalFs {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Self::validate_key(key)?;
        self.validate_existing_key_path(key, false)?;
        Ok(fs::read(self.resolve(key))?)
    }

    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        Self::validate_key(key)?;
        self.validate_existing_key_path(key, false)?;
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
        Self::validate_key(key)?;
        self.validate_existing_key_path(key, true)?;
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
        self.sync_containing_directory_chain(&final_path)?;
        Ok(())
    }

    // Requires a filesystem that supports hard links (most POSIX
    // filesystems, NTFS) -- fails on exFAT/FAT32 and some SMB mounts, not
    // just on a genuine key collision. A future caller on such a
    // filesystem should expect every `put_if_absent` call to fail, not
    // just colliding ones.
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        Self::validate_key(key)?;
        self.validate_existing_key_path(key, true)?;
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
                self.sync_containing_directory_chain(&final_path)?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(crate::error::StorageError::AlreadyExists(key.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<crate::backend::ObjectMeta>> {
        if !self.validate_list_prefix(prefix)? {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        Self::walk(&self.root, &self.root, prefix, &mut results)?;
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    fn delete(&self, key: &str) -> Result<()> {
        Self::validate_key(key)?;
        self.validate_existing_key_path(key, false)?;
        let final_path = self.resolve(key);
        fs::remove_file(&final_path)?;
        crate::chaos::chaos_checkpoint(); // unlinked; no longer discoverable by content
        // The namespace change is visible before its directories are synced.
        // A sync error therefore leaves deletion durability uncertain: callers
        // must re-list and retry rather than treating the failed delete as
        // either durable completion or a failed unlink.
        self.sync_containing_directory_chain(&final_path)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::backend::ObjectMeta;
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
    fn put_and_put_if_absent_require_a_preexisting_root() {
        // Break caught: creating the configured root makes LocalFs silently
        // assume durability responsibility for its caller-owned parent.
        let parent = temp_root("missing-root-anchor");
        let root = parent.join("not-created");
        let backend = LocalFs::new(&root);

        for result in [
            backend.put("a.bin", b"payload"),
            backend.put_if_absent("b.bin", b"payload"),
        ] {
            assert!(
                matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::NotFound),
                "a missing LocalFs root must fail before writing, got {result:?}"
            );
        }
        assert!(
            !root.exists(),
            "a rejected backend write must not create the configured root"
        );
        fs::remove_dir_all(&parent).ok();
    }

    #[test]
    fn list_on_a_missing_root_returns_an_empty_result() {
        // Break caught: Dataset::create probes its not-yet-created directory
        // through read_current, which relies on an empty list rather than an
        // error before the first manifest exists.
        let parent = temp_root("missing-root-list");
        let root = parent.join("not-created");

        let listed = LocalFs::new(&root).list("").unwrap();

        assert!(listed.is_empty());
        assert!(!root.exists(), "listing must not create the backend root");
        fs::remove_dir_all(&parent).ok();
    }

    #[test]
    fn local_fs_rejects_symlinked_root_and_nested_key_components() {
        // Break caught: lexical starts_with checks accept a key such as
        // linked/escaped.bin even when linked escapes the physical root.
        let parent = temp_root("symlink-containment");
        let root = parent.join("root");
        let target = parent.join("target");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&target).unwrap();

        let nested_link = root.join("linked");
        if let Err(error) = create_directory_symlink(&target, &nested_link) {
            if symlink_creation_is_unavailable(&error) {
                // Windows requires developer mode or the symlink privilege;
                // this is the only platform skip for this regression.
                fs::remove_dir_all(&parent).ok();
                return;
            }
            panic!("creating a test symlink failed unexpectedly: {error}");
        }

        fs::write(target.join("existing.bin"), b"outside").unwrap();
        let backend = LocalFs::new(&root);
        for result in [
            backend
                .put("linked/escaped.bin", b"payload")
                .map(|()| Vec::new()),
            backend.get("linked/existing.bin"),
            backend.list("linked/").map(|_| Vec::new()),
        ] {
            assert!(
                matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::InvalidInput),
                "a symlinked key component must be rejected, got {result:?}"
            );
        }
        assert!(
            !target.join("escaped.bin").exists(),
            "a rejected write must not escape through the symlink"
        );

        let symlinked_root = parent.join("symlinked-root");
        create_directory_symlink(&target, &symlinked_root).unwrap();
        let result = LocalFs::new(&symlinked_root).list("");
        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::InvalidInput),
            "a symlinked LocalFs root must be rejected, got {result:?}"
        );
        fs::remove_dir_all(&parent).ok();
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
    fn put_returns_the_directory_sync_failure_after_renaming_the_file() {
        // Break caught: returning success after the rename but before a
        // successful parent-directory sync would acknowledge non-durable
        // object publication.
        let root = temp_root("put-directory-sync-failure");
        let backend = LocalFs::new(&root);
        let _fault = crate::datafile::test_support::fail_directory_sync_on_call(
            1,
            std::io::ErrorKind::Other,
        );

        let result = backend.put("a.bin", b"durable payload");

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected parent-directory sync failure after rename, got {result:?}"
        );
        assert_eq!(
            backend.get("a.bin").unwrap(),
            b"durable payload",
            "the test must prove the error happened after the atomic rename"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_returns_an_error_when_a_nested_parent_sync_fails() {
        // Break caught: syncing only the leaf directory lets `put` report
        // success while a newly-created ancestor directory entry remains
        // non-durable.
        let root = temp_root("put-nested-ancestor-sync-failure");
        let backend = LocalFs::new(&root);
        let _fault = crate::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        let result = backend.put("one/two/a.bin", b"durable payload");

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected nested ancestor directory-sync failure, got {result:?}"
        );
        assert_eq!(
            backend.get("one/two/a.bin").unwrap(),
            b"durable payload",
            "the failure must occur after atomic file publication"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_syncs_the_owned_parent_chain_and_never_its_ancestor() {
        // Break caught: walking above LocalFs::root makes a backend write
        // depend on directory handles outside its configured ownership.
        let root = temp_root("put-owned-sync-chain");
        let backend = LocalFs::new(&root);
        let recorder = crate::datafile::test_support::record_directory_syncs();

        backend.put("one/two/a.bin", b"durable payload").unwrap();

        assert_eq!(
            recorder.calls(),
            vec![root.join("one").join("two"), root.join("one"), root.clone()],
            "put must synchronize leaf-to-root inclusive and never above LocalFs::root"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_retry_resyncs_the_complete_owned_parent_chain() {
        // Break caught: after a sync failure, directories remain visible;
        // the next write must still sync every owned directory on the path.
        let root = temp_root("put-owned-sync-chain-retry");
        let backend = LocalFs::new(&root);
        let fault = crate::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        assert!(backend.put("one/two/a.bin", b"first").is_err());
        drop(fault);

        let recorder = crate::datafile::test_support::record_directory_syncs();
        backend.put("one/two/b.bin", b"second").unwrap();

        assert_eq!(
            recorder.calls(),
            vec![root.join("one").join("two"), root.join("one"), root.clone()],
            "a retry must re-sync the complete leaf-to-root owned chain"
        );
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
    fn put_if_absent_returns_an_error_when_a_nested_parent_sync_fails() {
        // Break caught: `put_if_absent` has the same durable-completion
        // obligation as `put`; a successful hard link alone cannot make a
        // recursively-created parent chain durable.
        let root = temp_root("if-absent-nested-ancestor-sync-failure");
        let backend = LocalFs::new(&root);
        let _fault = crate::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        let result = backend.put_if_absent("one/two/a.bin", b"durable payload");

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected nested ancestor directory-sync failure, got {result:?}"
        );
        assert_eq!(
            backend.get("one/two/a.bin").unwrap(),
            b"durable payload",
            "the failure must occur after atomic file publication"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_if_absent_syncs_the_owned_parent_chain_and_never_its_ancestor() {
        // Break caught: `put_if_absent` must hold the same bounded
        // durability boundary as `put` after atomically linking its object.
        let root = temp_root("if-absent-owned-sync-chain");
        let backend = LocalFs::new(&root);
        let recorder = crate::datafile::test_support::record_directory_syncs();

        backend
            .put_if_absent("one/two/a.bin", b"durable payload")
            .unwrap();

        assert_eq!(
            recorder.calls(),
            vec![root.join("one").join("two"), root.join("one"), root.clone()],
            "put_if_absent must synchronize leaf-to-root inclusive and never above LocalFs::root"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_if_absent_retry_resyncs_the_complete_owned_parent_chain() {
        // Break caught: a failed initial link publication can leave its
        // parent tree visible but uncertain; a later distinct publication
        // under that tree must re-sync the full owned chain.
        let root = temp_root("if-absent-owned-sync-chain-retry");
        let backend = LocalFs::new(&root);
        let fault = crate::datafile::test_support::fail_directory_sync_on_call(
            2,
            std::io::ErrorKind::Other,
        );

        assert!(backend.put_if_absent("one/two/a.bin", b"first").is_err());
        drop(fault);

        let recorder = crate::datafile::test_support::record_directory_syncs();
        backend.put_if_absent("one/two/b.bin", b"second").unwrap();

        assert_eq!(
            recorder.calls(),
            vec![root.join("one").join("two"), root.join("one"), root.clone()],
            "a retry must re-sync the complete leaf-to-root owned chain"
        );
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

    #[test]
    fn list_returns_only_keys_matching_the_prefix_sorted() {
        let root = temp_root("list");
        let backend = LocalFs::new(&root);
        backend
            .put("_versions/00000000000000000002.manifest", b"{}")
            .unwrap();
        backend
            .put("_versions/00000000000000000001.manifest", b"{}")
            .unwrap();
        backend.put("data/a.arrow", b"irrelevant").unwrap();

        let listed = backend.list("_versions/").unwrap();

        let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "_versions/00000000000000000001.manifest",
                "_versions/00000000000000000002.manifest",
            ]
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn list_reports_the_correct_size_for_each_object() {
        let root = temp_root("list-size");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"12345").unwrap();

        let listed = backend.list("a").unwrap();

        assert_eq!(
            listed,
            vec![ObjectMeta {
                key: "a.bin".to_string(),
                size: 5
            }]
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn list_on_a_prefix_with_no_matches_returns_empty() {
        let root = temp_root("list-empty");
        let backend = LocalFs::new(&root);

        let listed = backend.list("_versions/").unwrap();

        assert!(listed.is_empty());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn delete_removes_the_key_and_a_second_delete_errors() {
        let root = temp_root("delete");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"content").unwrap();

        backend.delete("a.bin").unwrap();

        assert!(backend.get("a.bin").is_err());
        assert!(backend.delete("a.bin").is_err());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn delete_syncs_the_owned_parent_chain() {
        // Break caught: unlinking a nested key without synchronizing its
        // containing directories can report deletion before the namespace
        // change is durable.
        let root = temp_root("delete-owned-sync-chain");
        let backend = LocalFs::new(&root);
        let setup_recorder = crate::datafile::test_support::record_directory_syncs();
        backend.put("one/two/a.bin", b"content").unwrap();
        drop(setup_recorder);
        let recorder = crate::datafile::test_support::record_directory_syncs();

        backend.delete("one/two/a.bin").unwrap();

        assert_eq!(
            recorder.calls(),
            vec![root.join("one").join("two"), root.join("one"), root.clone()],
            "delete must synchronize leaf-to-root inclusive and never above LocalFs::root"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn delete_returns_the_directory_sync_failure_after_unlink() {
        // Break caught: returning success after unlink but before the
        // directory sync acknowledges a deletion whose durability remains
        // uncertain.
        let root = temp_root("delete-directory-sync-failure");
        let backend = LocalFs::new(&root);
        let setup_recorder = crate::datafile::test_support::record_directory_syncs();
        backend.put("a.bin", b"content").unwrap();
        drop(setup_recorder);
        let _fault = crate::datafile::test_support::fail_directory_sync_on_call(
            1,
            std::io::ErrorKind::Other,
        );

        let result = backend.delete("a.bin");

        assert!(
            matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::Other),
            "expected parent-directory sync failure after unlink, got {result:?}"
        );
        assert!(
            backend.get("a.bin").is_err(),
            "the test must prove the error happened after unlink"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_rejects_a_key_containing_a_parent_dir_component() {
        let root = temp_root("key-validation");
        let backend = LocalFs::new(&root);

        let result = backend.put("../escape.bin", b"x");

        assert!(
            result.is_err(),
            "a key with a '..' component must be rejected, not silently escape root"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_rejects_a_key_with_an_interior_dot_segment() {
        let root = temp_root("key-validation-dot");
        let backend = LocalFs::new(&root);

        let result = backend.put("a/./b", b"x");

        assert!(
            result.is_err(),
            "a key with an interior '.' segment must be rejected, not silently normalized"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_rejects_a_key_with_a_doubled_separator() {
        let root = temp_root("key-validation-double-slash");
        let backend = LocalFs::new(&root);

        let result = backend.put("a//b", b"x");

        assert!(
            result.is_err(),
            "a key with a doubled separator must be rejected, not silently normalized"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_rejects_a_key_with_a_trailing_separator() {
        let root = temp_root("key-validation-trailing-slash");
        let backend = LocalFs::new(&root);

        let result = backend.put("a/b/", b"x");

        assert!(
            result.is_err(),
            "a key with a trailing separator must be rejected, not silently normalized"
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_fs_passes_the_backend_conformance_suite() {
        let root = temp_root("conformance");
        let root_for_closure = root.clone();
        // `conformance::run` calls `make_backend` once per assertion group
        // (5 times) and its own doc comment promises each group "starts
        // from an empty backend" -- a counter-derived, per-call unique
        // subdirectory is what actually makes that true, mirroring the
        // `tmp_path_for` counter pattern already used in this file. Without
        // it, every group would share one store (same directory), isolated
        // only incidentally by using disjoint keys.
        let call_counter = std::sync::atomic::AtomicU64::new(0);
        crate::backend::conformance::run(move || {
            let n = call_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let group_root = root_for_closure.join(format!("group-{n}"));
            fs::create_dir(&group_root).unwrap();
            Box::new(LocalFs::new(group_root)) as Box<dyn Backend>
        });
        fs::remove_dir_all(&root).ok();
    }

    #[cfg(unix)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(not(any(unix, windows)))]
    fn create_directory_symlink(_target: &Path, _link: &Path) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "directory symlinks are unsupported on this platform",
        ))
    }

    fn symlink_creation_is_unavailable(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
        ) || cfg!(windows) && error.raw_os_error() == Some(1314)
    }
}
