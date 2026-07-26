#![no_main]

use libfuzzer_sys::fuzz_target;

// Mirrors `strata_index::delta_log::read_delta_log`'s actual shape (line-
// split, skip empty lines, parse each line, short-circuit on the first
// error) against arbitrary bytes, rather than treating the whole input as
// one JSON value -- a single `serde_json::from_slice` call over the whole
// input (this target's original form) never exercises the line-splitting
// step at all, so a valid entry followed by garbage on a later line, CRLF
// vs LF, or an embedded blank line were all untested. `read_delta_log`
// itself takes a `&Path` (real disk I/O, not what libFuzzer drives), so
// this reimplements its exact logic rather than calling it directly --
// `std::fs::read_to_string`'s only fuzzable-relevant behavior is the UTF-8
// validation `str::from_utf8` already gives us here for free.
//
// This is the real untrusted-input surface: a corrupted disk, a downgraded
// binary writing an older delta-log shape, or a hand-edited/pre-fix log
// entry could all hand a reader exactly this. Must never panic; returning
// an error for garbage input is correct and expected. Seeded corpus
// (`fuzz/corpus/delta_log_parse/`) includes real `Insert`/`Tombstone`
// entries and the zero-length-vector shape this project previously found
// and fixed a real poisoning bug around (see
// `.claude/docs/analysis/2026-07-25-filtered-vector-search-memory-audit.md`
// and `crates/txn/src/dataset.rs`'s `ZeroLengthVectorColumn`/
// `IndexError::ZeroLengthVector` guards) -- the dimension-validation logic
// itself lives in `crates/txn` (private to that crate), one layer above
// what this target can reach without new public API surface; this target's
// job is the parsing layer beneath it, not a substitute for that guard.
fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _: Result<Vec<strata_index::DeltaEntry>, _> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(serde_json::from_str::<strata_index::DeltaEntry>)
        .collect();
});
