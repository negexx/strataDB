#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzzes the actual on-disk manifest deserialization step
// (`strata_storage::manifest::read_current`'s internal
// `serde_json::from_slice::<Manifest>(&bytes)` call) directly against
// arbitrary bytes — this is the real untrusted-input surface: a corrupted
// disk, a downgraded binary writing an older manifest shape, or a hostile
// actor with filesystem access could all hand a reader exactly this.
fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<strata_storage::Manifest>(data);
});
