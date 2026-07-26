#![no_main]

use std::sync::atomic::{AtomicU64, Ordering};

use libfuzzer_sys::fuzz_target;

// Calls `strata_storage::read_batch` itself, not a hand-rolled
// `arrow::ipc::reader::FileReader::try_new` over an in-memory `Cursor`
// (this target's original form). That distinction used to be cosmetic; it
// no longer is. This target's own first run found a real crash (arrow-ipc
// panicking on a malformed schema instead of returning a `Result` --
// confirmed at arrow-ipc 58.3.0, reported upstream at
// https://github.com/apache/arrow-rs/issues/10437), and `read_batch` was
// fixed in response to catch exactly that class of panic and convert it
// into a typed `StorageError`. A `Cursor`-based version bypasses that
// fix entirely: it would immediately and permanently re-discover the same
// already-fixed, already-reported crash on every run (the committed seed
// corpus sits bytes away from it) instead of ever exploring past it. This
// is the real untrusted-input surface for data files: a corrupted disk or
// a partially-written file after a crash mid-write (exactly what this
// project's Phase 7 chaos harness injects) could hand a reader exactly
// this. `read_batch` only accepts a `&Path` (real disk I/O, not what
// libFuzzer drives directly), so `data` is written to a uniquely-named
// temp file each run instead of an in-memory buffer. Must never panic;
// returning an error for garbage input is correct and expected -- that's
// exactly what's now being verified, not merely hoped for.
fuzz_target!(|data: &[u8]| {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "strata-fuzz-datafile-{}-{n}.arrow",
        std::process::id()
    ));
    if std::fs::write(&path, data).is_ok() {
        let _ = strata_storage::read_batch(&path);
        let _ = std::fs::remove_file(&path);
    }
});
