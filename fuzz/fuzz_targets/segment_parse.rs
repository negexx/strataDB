#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_BYTES {
        let _ = strata_index::SegmentReader::from_bytes(data);
    }
});
