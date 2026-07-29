# Phase 9 M0: Backend Trait (Local-Disk Only) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a synchronous `Backend` trait in `crates/storage`, with a `LocalFs` implementation, and migrate `manifest.rs`'s `commit_manifest`/`read_current` onto it — with zero new dependencies and zero behavior change, so the existing test suite is a genuine regression gate for the abstraction.

**Architecture:** New `crates/storage/src/backend/` module: `mod.rs` (the `Backend` trait + `ObjectMeta`), `local.rs` (`LocalFs`, a `Backend` impl that reproduces today's tmp-write+fsync+rename durability dance behind a `key: &str` / `bytes: &[u8]` interface instead of `&Path`/`File`), `conformance.rs` (a reusable, backend-agnostic test suite that will be run again in a later milestone against `S3Backend`). `manifest.rs`'s `commit_manifest`/`read_current` are rewritten to delegate to a `LocalFs` rooted at the dataset directory, with **exactly the same chaos-checkpoint count and order** as today (2 checkpoints, both relocated from `manifest.rs` into `backend/local.rs`, per `.claude/docs/design/phase-0-transaction-and-format-spec.md`'s durability protocol and `crates/storage/src/chaos.rs`'s "every checkpoint stays inside strata-storage" rule). `datafile.rs`'s `write_batch`/`write_bytes`/`read_batch`/`read_batch_columns`/`sync_dir` are **explicitly out of scope for this plan** — see Global Constraints.

**Tech Stack:** Rust (existing `std::fs`, `arrow`, `serde_json`, `thiserror` — no new crates).

## Global Constraints

- **Zero new dependencies.** No `Cargo.toml` changes in this plan.
- **Zero changes outside `crates/storage`.** In particular, do not touch `crates/txn/src/dataset.rs`, `crates/index/`, or `tests/sim/` — another workstream (PR #47, the chaos-worker workload extension) is actively working in `crates/txn`/`tests/sim`, and this plan's own design doc (`docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md` §5, M0) scopes this milestone to `crates/storage` only.
- **Do not touch `datafile.rs`'s `write_batch`/`write_bytes`/`read_batch`/`read_batch_columns`/`sync_dir`.** Their migration onto `Backend` is deferred to a later milestone (M2/M3) specifically because `write_batch`/`write_bytes` each currently produce exactly one chaos checkpoint (a direct `File::create`, no tmp+rename — they're safe to write directly because nothing references a data/segment file's final path until the manifest that names it commits), and `tests/sim/tests/chaos.rs`'s `MAX_ABORT_THRESHOLD` constant (currently 200, computed from an explicit 6-checkpoints-per-commit accounting documented in that file's own comment) would need re-deriving if that checkpoint count changed. Changing it is real, coordinated work for a later milestone, not this one.
- **`commit_manifest`'s chaos-checkpoint count and order must not change.** Today it performs exactly 2 checkpoints (tmp-file fsync, then rename) inline; after this plan, `LocalFs::put` performs the same 2 checkpoints internally, in the same order, and `commit_manifest` calls it once. The total system-wide checkpoint count stays at 6 per commit, unchanged — do not add or remove a checkpoint anywhere as part of this plan.
- **Existing test files (`manifest.rs`'s and `datafile.rs`'s `#[cfg(test)] mod tests`) must keep passing unmodified** except for import adjustments strictly required by the refactor (see Task 6).
- Workspace lints: `clippy::pedantic` + `unwrap_used`/`expect_used` at warn. Match the existing convention of `#[allow(clippy::unwrap_used, clippy::expect_used)]` on `#[cfg(test)] mod tests` blocks; production code must not use `unwrap()`/`expect()`.
- Any `unsafe` block needs a `// SAFETY:` comment (not expected to be needed in this plan — flag it if it turns out to be).

---

### Task 1: `Backend` trait, `ObjectMeta`, and `LocalFs::new`/`put`/`get`

**Files:**
- Create: `crates/storage/src/backend/mod.rs`
- Create: `crates/storage/src/backend/local.rs`
- Modify: `crates/storage/src/lib.rs`

**Interfaces:**
- Produces: `pub trait Backend: Send + Sync { fn get(&self, key: &str) -> Result<Vec<u8>>; fn put(&self, key: &str, bytes: &[u8]) -> Result<()>; /* more methods added in later tasks */ }`, `pub struct ObjectMeta { pub key: String, pub size: u64 }`, `pub struct LocalFs { /* private */ }` with `pub fn new(root: impl Into<PathBuf>) -> Self`.

- [ ] **Step 1: Write the failing test for `LocalFs::put`/`get` round-trip**

Create `crates/storage/src/backend/local.rs`:

```rust
//! `LocalFs`: a [`crate::backend::Backend`] implementation rooted at a local
//! directory. Reproduces today's `write_bytes`/`commit_manifest`
//! tmp-write-then-fsync-then-rename durability dance behind the `Backend`
//! trait's `key: &str` / `bytes: &[u8]` interface.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::{Backend, ObjectMeta};
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
}

impl Backend for LocalFs {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(fs::read(self.resolve(key))?)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        todo!()
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

        backend.put("_versions/00000000000000000001.manifest", b"{}").unwrap();
        let read_back = backend.get("_versions/00000000000000000001.manifest").unwrap();

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
```

Create `crates/storage/src/backend/mod.rs`:

```rust
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
```

Modify `crates/storage/src/lib.rs` — add the new module and export:

```rust
pub mod backend;
```

(insert alphabetically among the existing `pub mod` lines, i.e. between `pub mod chaos;` and `pub mod datafile;`)

```rust
pub use backend::{Backend, LocalFs, ObjectMeta};
```

(insert alphabetically among the existing `pub use` lines, i.e. immediately after `pub use arrow;`)

- [ ] **Step 2: Run tests to verify the round-trip tests fail (todo! panics)**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: FAIL — `put_then_get_round_trips_bytes_exactly`, `put_creates_parent_directories_for_a_nested_key`, and `put_overwrites_an_existing_key` all panic with `not yet implemented`.

- [ ] **Step 3: Implement `LocalFs::put`**

Replace the `todo!()` body in `crates/storage/src/backend/local.rs`'s `impl Backend for LocalFs` with:

```rust
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
```

Add the tmp-path helper as an associated function on `LocalFs` (below `resolve`):

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: PASS — all 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/storage/src/backend/mod.rs crates/storage/src/backend/local.rs crates/storage/src/lib.rs
git commit -m "feat(storage): add Backend trait and LocalFs put/get"
```

---

### Task 2: `LocalFs::get_range`

**Files:**
- Modify: `crates/storage/src/backend/mod.rs`
- Modify: `crates/storage/src/backend/local.rs`

**Interfaces:**
- Consumes: `LocalFs::resolve` from Task 1.
- Produces: `Backend::get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>>`.

- [ ] **Step 1: Write the failing test**

Add to `crates/storage/src/backend/local.rs`'s test module:

```rust
    #[test]
    fn get_range_reads_only_the_requested_byte_span() {
        let root = temp_root("range");
        let backend = LocalFs::new(&root);
        backend.put("a.bin", b"0123456789").unwrap();

        let slice = backend.get_range("a.bin", 3..6).unwrap();

        assert_eq!(slice, b"345");
        fs::remove_dir_all(&root).ok();
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-storage backend::local::tests::get_range_reads_only_the_requested_byte_span -- --nocapture`
Expected: FAIL with a compile error (`get_range` not a method on `Backend`/`LocalFs`).

- [ ] **Step 3: Add `get_range` to the trait and implement it**

In `crates/storage/src/backend/mod.rs`, add to the `Backend` trait (after `get`):

```rust
    /// Reads `range` (byte offsets, exclusive end) from `key`.
    ///
    /// # Errors
    /// Returns an error if `key` does not exist, or `range` extends past
    /// its length.
    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>>;
```

In `crates/storage/src/backend/local.rs`, add `use std::io::{Read as _, Seek as _, SeekFrom};` to the existing `use` block, and add to `impl Backend for LocalFs` (after `get`):

```rust
    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let mut file = File::open(self.resolve(key))?;
        file.seek(SeekFrom::Start(range.start))?;
        let len = usize::try_from(range.end - range.start)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: PASS — all 4 tests green (the 3 from Task 1 plus this one).

- [ ] **Step 5: Commit**

```bash
git add crates/storage/src/backend/mod.rs crates/storage/src/backend/local.rs
git commit -m "feat(storage): add Backend::get_range and LocalFs impl"
```

---

### Task 3: `LocalFs::put_if_absent` and `StorageError::AlreadyExists`

**Files:**
- Modify: `crates/storage/src/error.rs`
- Modify: `crates/storage/src/backend/mod.rs`
- Modify: `crates/storage/src/backend/local.rs`

**Interfaces:**
- Consumes: `LocalFs::tmp_path_for` from Task 1.
- Produces: `StorageError::AlreadyExists(String)`, `Backend::put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>`.

- [ ] **Step 1: Write the failing test**

Add to `crates/storage/src/backend/local.rs`'s test module:

```rust
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
```

Add `use crate::error::StorageError;` to the top of `crates/storage/src/backend/local.rs`'s existing `use` block (needed for the `matches!` in the second test; the `Result` alias already imports the `Ok`/`Err` variants transparently, but the concrete error type needs its own import).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-storage backend::local::tests::put_if_absent -- --nocapture`
Expected: FAIL with a compile error (`put_if_absent` not a method; `StorageError::AlreadyExists` doesn't exist).

- [ ] **Step 3: Add the `AlreadyExists` error variant**

In `crates/storage/src/error.rs`, add a new variant to `StorageError` (after `CorruptDataFile`):

```rust
    #[error("key already exists: {0}")]
    AlreadyExists(String),
```

- [ ] **Step 4: Add `put_if_absent` to the trait and implement it**

In `crates/storage/src/backend/mod.rs`, add to the `Backend` trait (after `put`):

```rust
    /// Writes `bytes` to `key` only if `key` does not already exist —
    /// atomic create-if-absent, the primitive Strata's manifest commit path
    /// uses in place of the `rename`-based CAS local disk doesn't need but
    /// object storage does. See
    /// `docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md`
    /// §3.3.
    ///
    /// # Errors
    /// Returns [`crate::error::StorageError::AlreadyExists`] if `key`
    /// already exists; any other error if the write can't complete.
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>;
```

In `crates/storage/src/backend/local.rs`, add to `impl Backend for LocalFs` (after `put`):

```rust
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
                Err(StorageError::AlreadyExists(key.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: PASS — all 6 tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/storage/src/error.rs crates/storage/src/backend/mod.rs crates/storage/src/backend/local.rs
git commit -m "feat(storage): add Backend::put_if_absent and StorageError::AlreadyExists"
```

---

### Task 4: `LocalFs::list` and `LocalFs::delete`

**Files:**
- Modify: `crates/storage/src/backend/mod.rs`
- Modify: `crates/storage/src/backend/local.rs`

**Interfaces:**
- Consumes: `ObjectMeta` from Task 1.
- Produces: `Backend::list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>`, `Backend::delete(&self, key: &str) -> Result<()>`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/storage/src/backend/local.rs`'s test module:

```rust
    #[test]
    fn list_returns_only_keys_matching_the_prefix_sorted() {
        let root = temp_root("list");
        let backend = LocalFs::new(&root);
        backend.put("_versions/00000000000000000002.manifest", b"{}").unwrap();
        backend.put("_versions/00000000000000000001.manifest", b"{}").unwrap();
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

        assert_eq!(listed, vec![ObjectMeta { key: "a.bin".to_string(), size: 5 }]);
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: FAIL with compile errors (`list`/`delete` not methods on `Backend`/`LocalFs`).

- [ ] **Step 3: Add `list` and `delete` to the trait and implement them**

In `crates/storage/src/backend/mod.rs`, add to the `Backend` trait (after `put_if_absent`):

```rust
    /// Lists every object whose key starts with `prefix`, sorted
    /// lexicographically by key. Recurses into nested keys the way an
    /// object store's flat, `/`-delimited namespace does — there is no
    /// concept of "one directory level" here.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be listed.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Removes `key`.
    ///
    /// # Errors
    /// Returns an error if `key` does not exist or can't be removed.
    fn delete(&self, key: &str) -> Result<()>;
```

In `crates/storage/src/backend/local.rs`, add to `impl Backend for LocalFs` (after `put_if_absent`):

```rust
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let mut results = Vec::new();
        Self::walk(&self.root, &self.root, prefix, &mut results)?;
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    fn delete(&self, key: &str) -> Result<()> {
        fs::remove_file(self.resolve(key))?;
        Ok(())
    }
```

Add the recursive walk helper as an associated function on `LocalFs` (below `tmp_path_for`):

```rust
    /// Recursively walks `dir` (relative to `root`), collecting every file
    /// whose `root`-relative, `/`-joined key starts with `prefix`. A
    /// missing `dir` (e.g. `root` itself never created because nothing has
    /// been `put` yet) is not an error — it just contributes no entries,
    /// matching an object store's "empty prefix" behavior.
    fn walk(
        root: &Path,
        dir: &Path,
        prefix: &str,
        out: &mut Vec<ObjectMeta>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::walk(root, &path, prefix, out)?;
            } else {
                let key = Self::key_for(root, &path);
                if key.starts_with(prefix) {
                    let size = entry.metadata()?.len();
                    out.push(ObjectMeta { key, size });
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-storage backend::local::tests -- --nocapture`
Expected: PASS — all 10 tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/storage/src/backend/mod.rs crates/storage/src/backend/local.rs
git commit -m "feat(storage): add Backend::list and Backend::delete, LocalFs impl"
```

---

### Task 5: Backend conformance suite

**Files:**
- Create: `crates/storage/src/backend/conformance.rs`
- Modify: `crates/storage/src/backend/mod.rs`

**Interfaces:**
- Consumes: `Backend`, `LocalFs` from Tasks 1-4.
- Produces: `pub(crate) fn run(make_backend: impl Fn() -> Box<dyn Backend>)` (test-only) — reused unmodified in a later milestone against `S3Backend`/`InMemory`, per the design doc's testing strategy.

- [ ] **Step 1: Write the conformance suite (this task is the test — there is no separate "implementation" step, since it exercises Tasks 1-4's already-passing code)**

Create `crates/storage/src/backend/conformance.rs`:

```rust
//! A backend-agnostic conformance suite for any [`crate::backend::Backend`]
//! impl. Run against `LocalFs` in this milestone; run again unmodified
//! against `S3Backend`/`InMemory` in a later milestone — see
//! `docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md`
//! §4. Not a `#[cfg(test)] mod tests` itself: it's a reusable function
//! `mod tests` blocks in this crate call into, kept in its own file because
//! it's meant to outlive this milestone's own tests.

#![cfg(test)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::backend::{Backend, ObjectMeta};
use crate::error::StorageError;

/// Runs the full conformance suite against a fresh backend from
/// `make_backend`. Called once per assertion group so each group starts
/// from an empty backend, rather than sharing state across assertions.
pub(crate) fn run(make_backend: impl Fn() -> Box<dyn Backend>) {
    put_then_get_round_trips(&make_backend());
    get_range_reads_a_byte_span(&make_backend());
    put_if_absent_succeeds_once_then_collides(&make_backend());
    list_finds_a_put_key_under_its_prefix(&make_backend());
    delete_removes_a_key(&make_backend());
}

fn put_then_get_round_trips(backend: &dyn Backend) {
    backend.put("conformance/a.bin", b"hello").unwrap();
    assert_eq!(backend.get("conformance/a.bin").unwrap(), b"hello");
}

fn get_range_reads_a_byte_span(backend: &dyn Backend) {
    backend.put("conformance/range.bin", b"0123456789").unwrap();
    assert_eq!(backend.get_range("conformance/range.bin", 2..5).unwrap(), b"234");
}

fn put_if_absent_succeeds_once_then_collides(backend: &dyn Backend) {
    backend.put_if_absent("conformance/once.bin", b"first").unwrap();
    let result = backend.put_if_absent("conformance/once.bin", b"second");
    assert!(
        matches!(result, Err(StorageError::AlreadyExists(_))),
        "expected AlreadyExists, got {result:?}"
    );
    assert_eq!(backend.get("conformance/once.bin").unwrap(), b"first");
}

fn list_finds_a_put_key_under_its_prefix(backend: &dyn Backend) {
    backend.put("conformance/listed/x.bin", b"x").unwrap();
    let listed = backend.list("conformance/listed/").unwrap();
    assert_eq!(
        listed,
        vec![ObjectMeta { key: "conformance/listed/x.bin".to_string(), size: 1 }]
    );
}

fn delete_removes_a_key(backend: &dyn Backend) {
    backend.put("conformance/gone.bin", b"x").unwrap();
    backend.delete("conformance/gone.bin").unwrap();
    assert!(backend.get("conformance/gone.bin").is_err());
}
```

In `crates/storage/src/backend/mod.rs`, add near the top (after `pub mod local;`):

```rust
#[cfg(test)]
mod conformance;
```

Add a test invoking it in `crates/storage/src/backend/local.rs`'s test module:

```rust
    #[test]
    fn local_fs_passes_the_backend_conformance_suite() {
        let root = temp_root("conformance");
        let root_for_closure = root.clone();
        crate::backend::conformance::run(move || {
            Box::new(LocalFs::new(root_for_closure.clone())) as Box<dyn Backend>
        });
        fs::remove_dir_all(&root).ok();
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p strata-storage backend -- --nocapture`
Expected: PASS — all `backend::local::tests` (11 now) and no separate `conformance` test binary (it's a helper module, not its own test target).

- [ ] **Step 3: Commit**

```bash
git add crates/storage/src/backend/conformance.rs crates/storage/src/backend/mod.rs crates/storage/src/backend/local.rs
git commit -m "test(storage): add reusable Backend conformance suite"
```

---

### Task 6: Migrate `manifest.rs`'s `commit_manifest`/`read_current` onto `Backend`

**Files:**
- Modify: `crates/storage/src/manifest.rs`

**Interfaces:**
- Consumes: `Backend::{put, get, list}` from Tasks 1 and 4, `LocalFs::new` from Task 1.
- Produces: `commit_manifest`/`read_current` keep their exact existing signatures (`pub fn commit_manifest(dataset_dir: &Path, manifest: &Manifest) -> Result<()>`, `pub fn read_current(dataset_dir: &Path) -> Result<Option<Manifest>>`) — no caller anywhere in the workspace needs to change.

- [ ] **Step 1: Confirm the existing test suite is green before touching production code (characterization baseline)**

Run: `cargo test -p strata-storage manifest:: -- --nocapture`
Expected: PASS — all existing tests in `crates/storage/src/manifest.rs`'s `mod tests` (13 tests) green, unmodified. This is the baseline this task must not regress.

- [ ] **Step 2: Replace `commit_manifest`'s implementation**

In `crates/storage/src/manifest.rs`, replace the body of `commit_manifest`:

```rust
pub fn commit_manifest(dataset_dir: &Path, manifest: &Manifest) -> Result<()> {
    let backend = LocalFs::new(dataset_dir);
    let key = format!("_versions/{:020}.manifest", manifest.version);
    let json = serde_json::to_vec(manifest)?;
    // `LocalFs::put` fsyncs the containing directory internally (see Task
    // 1), so there is no separate `sync_dir` call here the way the
    // pre-Backend code had one -- folding that step into `put` itself
    // (rather than leaving it a caller-remembered step) is what makes
    // `Backend::put`'s durability contract self-contained. Do not add a
    // second explicit `sync_dir` call here: `versions_dir(dataset_dir)` is
    // exactly the directory `put` already fsyncs for this key, so a second
    // call would double a chaos checkpoint and break the "checkpoint count
    // unchanged" global constraint below.
    backend.put(&key, &json)?;
    Ok(())
}
```

- [ ] **Step 3: Replace `read_current`'s implementation**

Replace the body of `read_current`:

```rust
pub fn read_current(dataset_dir: &Path) -> Result<Option<Manifest>> {
    let backend = LocalFs::new(dataset_dir);

    let mut best: Option<(u64, String)> = None;
    for meta in backend.list("_versions/")? {
        let Some(stem) = meta
            .key
            .strip_prefix("_versions/")
            .and_then(|s| s.strip_suffix(".manifest"))
        else {
            continue;
        };
        let Ok(version) = stem.parse::<u64>() else {
            continue;
        };
        let is_newer = best.as_ref().is_none_or(|(v, _)| version > *v);
        if is_newer {
            best = Some((version, meta.key.clone()));
        }
    }

    let Some((_, key)) = best else {
        return Ok(None);
    };
    let bytes = backend.get(&key)?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|e| StorageError::CorruptManifest(dataset_dir.join(&key), e.to_string()))?;
    Ok(Some(manifest))
}
```

- [ ] **Step 4: Fix imports**

At the top of `crates/storage/src/manifest.rs`, the production code (outside `mod tests`) no longer calls `std::fs`/`std::fs::File`/`std::io::Write` directly — only `versions_dir`/`manifest_path` (pure `PathBuf` construction, unaffected) and the two rewritten functions remain in that scope, and the test module still needs those std items for its own direct-filesystem assertions (e.g. reading back raw bytes to check exact JSON formatting). Change:

```rust
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::stats::ColumnStats;
```

to:

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::backend::LocalFs;
use crate::error::{Result, StorageError};
use crate::stats::ColumnStats;
```

Then, inside `#[cfg(test)] mod tests` (which currently starts with `use super::*;`), add the imports the test module's direct-filesystem assertions still need:

```rust
    use std::fs::{self, File};
    use std::io::Write as _;
```

(insert directly after the existing `use super::*;` line in `mod tests`)

- [ ] **Step 5: Mark `versions_dir`/`manifest_path` test-only**

Neither `versions_dir` nor `manifest_path` (both existing helper functions,
unchanged in body) is called by the new `commit_manifest`/`read_current`
implementations above — `commit_manifest` now inlines its key as a format
string, and `read_current` lists by a literal prefix. Both helpers are
still used, but only by `mod tests`'s direct-filesystem assertions (e.g.
`manifest_path(&dir, 0)` to read back raw bytes). Left as plain functions,
a non-test `cargo build` would flag both as unused (`mod tests` doesn't
exist in that compilation). Mark them `#[cfg(test)]` so they're compiled
exactly when something uses them:

```rust
#[cfg(test)]
fn versions_dir(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join("_versions")
}

#[cfg(test)]
fn manifest_path(dataset_dir: &Path, version: u64) -> PathBuf {
    versions_dir(dataset_dir).join(format!("{version:020}.manifest"))
}
```

- [ ] **Step 6: Run the full manifest test suite to verify no regression**

Run: `cargo test -p strata-storage manifest:: -- --nocapture`
Expected: PASS — all 13 tests green, unmodified, including
`leftover_tmp_file_is_never_picked_up_as_current`,
`read_current_is_none_when_versions_dir_has_only_a_leftover_tmp_file`,
`commit_manifest_writes_compact_json_not_pretty_printed`, and
`read_current_skips_a_manifest_suffixed_file_with_a_non_numeric_stem` — these
specifically exercise the tmp-file/rename/listing behavior this task
relocated into `LocalFs`, so they're the real regression check.

- [ ] **Step 7: Run the full storage crate's test suite**

Run: `cargo test -p strata-storage -- --nocapture`
Expected: PASS — every test in `strata-storage` (datafile, manifest, backend, encoding, stats) green.

- [ ] **Step 8: Commit**

```bash
git add crates/storage/src/manifest.rs
git commit -m "refactor(storage): migrate commit_manifest/read_current onto Backend/LocalFs"
```

---

### Task 7: Full verification gate

**Files:** none (verification only)

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: succeeds, no warnings.

- [ ] **Step 2: Full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass, including `tests/sim`'s chaos harness — confirming this task's claim that the total chaos-checkpoint count is unchanged (6 per commit) holds in practice, not just by inspection. If `tests/sim/tests/chaos.rs` fails or its invariant checks report a mismatch versus `MAX_ABORT_THRESHOLD`'s documented accounting, stop and re-examine Task 6's `LocalFs::put` checkpoint placement before proceeding — do not adjust `MAX_ABORT_THRESHOLD` as a fix, since per this plan's Global Constraints that file is out of scope for this plan.

- [ ] **Step 3: Lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean, no warnings.

- [ ] **Step 4: Format check**

Run: `cargo fmt --check`
Expected: clean. If not, run `cargo fmt` and re-stage.

- [ ] **Step 5: Commit (if `cargo fmt` produced changes)**

```bash
git add -u
git commit -m "chore(storage): cargo fmt"
```

---

## What comes after M0

This plan intentionally stops here. Per
`docs/superpowers/specs/2026-07-30-phase9-object-storage-backend-design.md`
§5, the next milestone (M1) is a throwaway conditional-PUT probe against
MinIO/R2/real S3 — an exploratory spike, not a TDD-shaped plan — followed by
M2 (`S3Backend` + the async/tokio bridge, which is where the `object_store`
dependency actually gets added and its own ADR gets written) and M3-M5.
Write M1's findings up and turn M2 into its own plan once M1's probe result
is known — the design doc already flags that a non-uniform conditional-PUT
result across vendors would change M2's fail-closed-probe details, so
front-loading M2's plan before M1 runs risks writing tasks against untested
assumptions about the `object_store` API's exact `PutMode`/error-mapping
behavior. Before starting M2's plan, also re-check PR #47's status, since
M2/M3 will eventually touch `crates/txn/src/dataset.rs` (URI dispatch at
`open`/`create`) and `tests/sim` (the new chaos fault class in M4) — the
two areas this plan deliberately avoided.
