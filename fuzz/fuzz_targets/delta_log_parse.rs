#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzzes the actual delta-log entry deserialization step
// (`strata_index::delta_log::read_delta_log`'s internal
// `serde_json::from_str::<DeltaEntry>(line)` call, exercised here via
// `from_slice` since fuzz input is raw bytes, not necessarily valid UTF-8)
// directly against arbitrary bytes -- this is the real untrusted-input
// surface: a corrupted disk, a downgraded binary writing an older delta-log
// shape, or a hand-edited/pre-fix log entry could all hand a reader exactly
// this. Must never panic; returning an error for garbage input is correct
// and expected.
fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<strata_index::DeltaEntry>(data);
});
