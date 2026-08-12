#![no_main]

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 64 * 1024;
static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDatasetDir {
    path: PathBuf,
    owns_directory: bool,
}

impl TempDatasetDir {
    fn new() -> Option<Self> {
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "strata-fuzz-manifest-current-{}-{}-{sequence}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_nanos(),
        ));
        Self::create_at(path)
    }

    fn create_at(path: PathBuf) -> Option<Self> {
        match std::fs::create_dir(&path) {
            Ok(()) => Some(Self {
                path,
                owns_directory: true,
            }),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => None,
            Err(_) => None,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDatasetDir {
    fn drop(&mut self) {
        if self.owns_directory {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some(dataset_dir) = TempDatasetDir::new() else {
        return;
    };
    let versions_dir = dataset_dir.path().join("_versions");
    let bytes = &data[..data.len().min(MAX_INPUT_BYTES)];

    if std::fs::create_dir_all(&versions_dir).is_err() {
        return;
    }

    let manifest = versions_dir.join("00000000000000000000.manifest");
    let malformed_name = versions_dir.join("not-a-version.manifest");
    let temporary_file = versions_dir.join("00000000000000000001.manifest.tmp");
    if std::fs::write(manifest, bytes).is_ok() {
        let _ = std::fs::write(malformed_name, bytes);
        let _ = std::fs::write(temporary_file, bytes);
        let _ = strata_storage::read_current(dataset_dir.path());
    }
});
