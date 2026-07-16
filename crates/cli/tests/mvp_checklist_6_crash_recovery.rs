//! Phase 1 MVP checklist step 6: "Kill the process mid-write, restart,
//! confirm the dataset opens and returns the last successfully committed
//! version." This is the one checklist item that genuinely needs a real OS
//! process to kill — nothing in-process can exercise actual crash safety,
//! since nothing actually crashes.
//!
//! `Child::kill()` is `SIGKILL` on Unix and `TerminateProcess` on Windows —
//! both are hard, immediate termination with no graceful shutdown, no
//! destructors, no final flush beyond what the child already forced itself
//! (see `crates/cli/src/main.rs`'s `crash-loop`, which flushes stdout after
//! every commit specifically so this test can observe progress reliably).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

const COMMITS_TO_WAIT_FOR: u64 = 20;

/// Extracts the `u64` following `key` (e.g. `"version="`) out of
/// `strata inspect`'s single-line `version=N row_count=M` output.
fn field(text: &str, key: &str) -> Option<u64> {
    text.split_whitespace()
        .find_map(|token| token.strip_prefix(key))
        .and_then(|s| s.parse().ok())
}

#[test]
fn crash_mid_write_recovers_last_committed_version() {
    let dir = std::env::temp_dir().join(format!("strata-crash-test-{}", std::process::id()));
    let strata_bin = env!("CARGO_BIN_EXE_strata");
    let dir_str = dir.to_str().expect("temp dir path must be valid UTF-8");

    let status = Command::new(strata_bin)
        .args(["create", dir_str])
        .status()
        .unwrap();
    assert!(status.success(), "create failed");

    // A huge commit count, so the child can never naturally finish before
    // we kill it — the kill always lands genuinely mid-run, not after.
    let mut child = Command::new(strata_bin)
        .args(["crash-loop", dir_str, "1000000"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().expect("child stdout was piped");
    let mut reader = BufReader::new(stdout);
    let mut last_committed: u64 = 0;

    let mut line = String::new();
    while last_committed < COMMITS_TO_WAIT_FOR {
        line.clear();
        let bytes_read = reader.read_line(&mut line).unwrap();
        assert!(
            bytes_read > 0,
            "child exited before producing {COMMITS_TO_WAIT_FOR} commits"
        );
        if let Some(n) = line.trim().strip_prefix("committed ") {
            last_committed = n.parse().expect("commit count should be a valid u64");
        }
    }

    child.kill().unwrap();
    child.wait().unwrap();

    // Give the OS a moment to fully release file handles before reopening.
    std::thread::sleep(Duration::from_millis(200));

    // Restart: open the dataset fresh, exactly as a new process would after
    // an actual crash.
    let output = Command::new(strata_bin)
        .args(["inspect", dir_str])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "inspect failed after crash — dataset did not recover cleanly: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let version: u64 = field(&stdout, "version=").expect("inspect output should contain version=N");
    let row_count: u64 =
        field(&stdout, "row_count=").expect("inspect output should contain row_count=N");

    // The recovered version can be >= last_committed (the kill may have
    // landed after more commits than we happened to observe) but never
    // less — and critically, `inspect` succeeding at all (not erroring on a
    // corrupt/torn manifest) is itself the main proof of crash safety: the
    // atomic-rename commit protocol means a killed process can only ever
    // leave a leftover `.tmp-*` file behind, never a partially-written
    // `*.manifest` that `read_current` would try to parse.
    assert!(
        version >= last_committed,
        "recovered version {version} is behind the last confirmed commit {last_committed} \
         — a committed write was lost on crash"
    );

    // A stronger check than the manifest pointer alone: crash-loop inserts
    // exactly one row per commit, starting from an empty dataset, so the
    // actual scanned row count must equal the recovered version exactly —
    // proving the *data*, not just the manifest, survived intact.
    assert_eq!(
        row_count, version,
        "row_count ({row_count}) should equal version ({version}) — crash-loop inserts one row \
         per commit, so a mismatch means data desynced from the manifest on recovery"
    );

    std::fs::remove_dir_all(&dir).ok();
}
