# Task 3 report — checkpoint and chaos verification gates

## Scope and decision

Worktree: `C:\Users\dagda\Downloads\strataDB\.worktrees\codex-phase-1-close-all-gaps` on
`codex/phase-1-close-all-gaps`, starting from `db9b4d3c9fb26a061e5accfcb82314fe03cf39dc` with a
clean task scope.

No harness or CI source was changed. The checkpoint gate passed; the fast chaos gate passed; and
the thorough gate neither self-skipped nor hid a child failure. Its CI workflow has
`set -euo pipefail`, streams Cargo output with `tee`, and requires the exact final marker with
`grep`. The test itself rejects a missing `STRATA_CHAOS_THOROUGH=1` gate. Therefore changing the
harness or workflow solely because this Windows host did not finish 2,000 seeds within the bounded
15-minute run would not be justified and would not close the required evidence gap.

## Reproduction evidence

### Checkpoint abort

Command (exit 0; 58.5 s wall time):

```text
cargo test -p strata-storage --features chaos-injection --test chaos_checkpoint_actually_aborts -- --exact commit_manifest_aborts_at_the_configured_checkpoint
```

Output:

```text
running 1 test
test commit_manifest_aborts_at_the_configured_checkpoint ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.67s
```

### Fast chaos tier

The first two invocations reached the harness after an initial build but were cut off by the
60-second command-wrapper limit (exit 124). The final unchanged command used a 300-second wrapper
limit and completed (exit 0; 63.6 s wall time):

```text
cargo test -p strata-sim fast_tier_random_seeds_survive_random_crash_points -- --exact --nocapture

running 1 test
test fast_tier_random_seeds_survive_random_crash_points has been running for over 60 seconds
test fast_tier_random_seeds_survive_random_crash_points ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 62.47s
```

### Thorough chaos tier

The POSIX spelling required by CI could not be launched literally because `bash` is absent on this
Windows host (exit 1 before Cargo). The process-local PowerShell equivalent set the same required
environment value and used the exact Cargo arguments:

```text
$env:STRATA_CHAOS_THOROUGH = '1'; cargo test -p strata-sim thorough_tier_satisfies_the_phase_7_exit_criterion -- --exact --ignored --nocapture
```

It timed out at the bounded 900-second host limit (exit 124; 900.6 s). Exact emitted output was:

```text
running 1 test
test thorough_tier_satisfies_the_phase_7_exit_criterion has been running for over 60 seconds
thorough tier: 200/2000 seeds checked, zero violations so far (concurrency=8)
thorough tier: 400/2000 seeds checked, zero violations so far (concurrency=8)
thorough tier: 600/2000 seeds checked, zero violations so far (concurrency=8)
```

The final `thorough tier: 2000/2000 seeds checked` marker did not occur. The recorded partial count
is therefore exactly 600 completed seeds, with zero violations reported so far; this is incomplete
Phase 1 evidence and is not claimed as closure.

## Static verification

- Test discovery (exit 0): `cargo test -p strata-sim --test chaos -- --list` listed the fast and
  ignored thorough tests; the storage command listed the named checkpoint test.
- YAML parse (exit 0): `npx --yes prettier@3 --parser yaml .github/workflows/ci.yml | Out-Null`.
- Exact-marker/environment discovery (exit 0): CI contains
  `STRATA_CHAOS_THOROUGH=1 cargo test ... -- --exact --ignored --nocapture`,
  `set -euo pipefail`, `tee /tmp/strata-chaos-thorough.log`, and
  `grep -F "thorough tier: 2000/2000 seeds checked"`.
- Stale-reference scan (exit 0): the configured `NUM_SEEDS` is `2000`; the sole `450` match is a
  regression-test comment describing the retired cap, not active configuration.
- Credential scan (exit 0): no credential material found. The only broad-pattern match was the
  ordinary word `token` in a `commit_ops.rs` source comment.
- Pre-edit `git diff --check` and scoped diff were clean (exit 0).

## Files and commits

- Changed: `.superpowers/sdd/phase-1-closeout-plan/task-3-report.md` only.
- No harness, chaos-worker, CI, ledger, assertion, or seed-count files changed.
- Baseline commit: `db9b4d3c9fb26a061e5accfcb82314fe03cf39dc`.
- The intentional report-only commit ID is recorded in the Terra handoff because a Git commit cannot
  contain its own final object ID.

## Remaining limitation

This host could not complete the required 2,000-seed thorough tier within 15 minutes. CI remains
fail-closed: a skipped test, child failure, timeout, partial output, or absent exact final marker
cannot pass the scheduled/manual thorough workflow. Fresh successful 2,000/2,000 evidence from a
suitable host or CI remains required.
