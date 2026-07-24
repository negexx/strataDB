//! Standalone helper binary for
//! `tests/chaos_checkpoint_actually_aborts.rs` — performs one real
//! `commit_manifest` call so the test can observe whether the configured
//! checkpoint actually aborts it. Only built with `chaos-injection`
//! (see `required-features` in Cargo.toml).
#![allow(clippy::unwrap_used, clippy::expect_used)]

fn main() {
    let dir = tempfile::Builder::new()
        .prefix("strata-chaos-helper-")
        .tempdir()
        .unwrap()
        .keep();
    let manifest = strata_storage::Manifest::empty();
    strata_storage::commit_manifest(&dir, &manifest).unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
