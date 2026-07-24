//! Standalone helper binary for
//! `tests/chaos_checkpoint_actually_aborts.rs` — performs one real
//! `commit_manifest` call so the test can observe whether the configured
//! checkpoint actually aborts it. Only built with `chaos-injection`
//! (see `required-features` in Cargo.toml).
#![allow(clippy::unwrap_used, clippy::expect_used)]

fn main() {
    let dir = std::env::temp_dir().join(format!("strata-chaos-helper-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let manifest = strata_storage::Manifest::empty();
    strata_storage::commit_manifest(&dir, &manifest).unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
