#![no_main]

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use libfuzzer_sys::fuzz_target;

// Fuzzes the same Arrow IPC parsing path `strata_storage::read_batch`
// wraps (`crates/storage/src/datafile.rs`), fed from an in-memory buffer
// instead of a real file so libFuzzer can drive it directly against
// arbitrary bytes. This is the real untrusted-input surface for data files:
// a corrupted disk or a partially-written file after a crash mid-write
// (exactly what this project's Phase 7 chaos harness injects) could hand a
// reader exactly this. Must never panic; returning an error for garbage
// input is correct and expected.
fuzz_target!(|data: &[u8]| {
    if let Ok(mut reader) = FileReader::try_new(Cursor::new(data), None) {
        for batch in reader.by_ref() {
            let _ = batch;
        }
    }
});
