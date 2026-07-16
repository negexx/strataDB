---
name: verify
description: Run the project's verification gate (build + tests + lint) and report results. Required before claiming work is complete.
argument-hint: "[--loom to also run loom interleaving tests for concurrency-touching changes]"
---

# /verify — Pre-completion gate

Run the standard verification commands for **Strata** and report results honestly.

## Commands to run (in this order)

1. **Build (= typecheck):** `cargo check --workspace` (fast pass), then `cargo build --workspace`
2. **Tests:** `cargo test --workspace` (or `cargo nextest run` if installed)
3. **Lint:** `cargo clippy --workspace --all-targets -- -D warnings`
4. **Format check:** `cargo fmt --check`

Run them sequentially — a build failure invalidates the test results, so don't waste time running tests if step 1 fails. Report each one's exit status before moving on.

If `--loom` is passed, or the change touches `crates/txn/` or `crates/index/`:

5. **Loom interleaving tests:** run the crate's loom-gated tests with `RUSTFLAGS="--cfg loom" cargo test --release -p <crate>` for the specific interleavings that matter. The borrow checker rules out data races in safe code — it does not prove your OCC/conflict-detection logic is correct under every interleaving. This is a genuinely separate pass, not implied by step 2's normal `cargo test`.

## Output format

```
✓ Check     — passed (2s)
✓ Build     — passed (9s)
✓ Tests     — 42 passed, 0 failed (4s)
✗ Clippy    — 2 warnings in crates/txn/src/manifest.rs
   <paste relevant snippets>
✓ Format    — clean
✓ Loom      — all interleavings verified (run required: crates/txn/ touched)

Next: fix clippy warnings before claiming this task done.
```

## Rules

- Don't paper over failures. A red is a red.
- Don't claim "passed" without showing the output. Evidence before assertions.
- If a test was flaky, investigate root cause — don't retry until green. Flaky concurrency tests are especially suspect: a flake in `crates/txn/` or `crates/index/` tests is more likely a real race than test infrastructure noise.
- For anything touching the transaction layer or vector index, a green `cargo test` without a loom pass is not sufficient — loom is required, not optional, for interleaving-sensitive logic.
- `unwrap()`/`expect()` are `clippy::warn`, not `clippy::deny` — don't treat a clean clippy run as license to reach for them anyway; they're warned on for a reason.

## When verification fails

Surface the actual error verbatim. Don't summarize away the useful detail. The user needs the real output to debug.

## One more gate after this passes

Passing `/verify` means the code works — it doesn't mean the task is done. Every task also needs an Opus 4.8 review (the `reviewer` subagent) before it's marked complete; `/ship` runs that automatically, but if you're not shipping yet, dispatch it yourself.
