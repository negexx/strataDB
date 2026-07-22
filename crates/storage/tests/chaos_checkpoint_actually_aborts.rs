//! Proves `chaos_checkpoint()` is actually wired into a real durability
//! path, not just present as an unused function. Must run in a subprocess:
//! calling an aborting function in-process would kill this very test
//! binary. This is a `tests/` integration test (not a unit test in
//! `chaos.rs`) specifically so it can build itself as its own tiny binary
//! and exec that binary as a child — see Step 3's helper.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

#[test]
fn commit_manifest_aborts_at_the_configured_checkpoint() {
    // This test only makes sense when strata-storage was built with
    // chaos-injection — if the feature is off, commit_manifest's
    // checkpoints are no-ops and nothing would ever abort, which would
    // make this test either fail confusingly or pass for the wrong
    // reason. Skip cleanly instead of asserting the wrong thing.
    if std::env::var("STRATA_CHAOS_TEST_HELPER_BUILT").is_err() {
        eprintln!(
            "skipping: run via `cargo test -p strata-storage --features chaos-injection \
             --test chaos_checkpoint_actually_aborts`"
        );
        return;
    }

    let helper_bin = env!("CARGO_BIN_EXE_chaos_checkpoint_helper");
    let output = Command::new(helper_bin)
        .env("STRATA_CHAOS_ABORT_AT", "1")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "helper should have aborted at checkpoint 1, but exited normally"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(6), // SIGABRT
            "expected SIGABRT (process::abort), got: {:?}",
            output.status
        );
    }
}
