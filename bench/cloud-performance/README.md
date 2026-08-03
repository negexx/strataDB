# Cloud before/after performance evidence

This harness compares the same deterministic Strata workloads at two Git revisions. It is
evidence-only: it does not declare a performance fix successful, and it does not claim a universal
bound from synthetic data.

The comparison runs:

- manifest growth at 1, 10, 20, 40, 80, and 160 sequential id-only commits, with one excluded
  warmup and five measured repetitions at every point;
- segmented vector search at 256 synthetic 512-dimensional rows, 16 queries, K=1…64, one excluded
  warmup, and five measured repetitions;
- the lifecycle benchmark at 64 synthetic 512-dimensional rows committed one row at a time, five
  retained-snapshot points (0/1/4/16/64 distinct snapshots), one excluded warmup, and five measured
  repetitions per point.

Each run records the revision, lockfile hash, runner OS/architecture, toolchain, raw benchmark
output, exact command/configuration provenance, and a GNU time report. `summarize.py` emits
machine-readable JSONL and CSV deltas only after verifying the complete like-for-like matrix.

## Local use

```bash
bash bench/cloud-performance/run.sh <before-commit> <after-commit> /tmp/strata-performance-artifacts
python3 bench/cloud-performance/summarize.py /tmp/strata-performance-artifacts \
  --jsonl /tmp/strata-performance-artifacts/summary.jsonl \
  --csv /tmp/strata-performance-artifacts/deltas.csv \
  --validate
```

The runner creates temporary linked worktrees for both revisions and removes them on exit, so the
caller's branch and working tree are not detached or rewritten. It uses a separate Cargo target
directory for each revision so compiled artifacts are not mixed across the comparison. The
benchmark filesystem and OS caches are not forcibly flushed; that policy is recorded in the
provenance log.

## GitHub Actions

The `Cloud performance before/after` workflow is manually dispatchable. Provide full commit SHAs
for `before_revision` and `after_revision`; the generated artifact is
`cloud-performance-before-after-<run-id>-attempt-<attempt>`. The workflow retains raw logs,
provenance, JSONL, CSV, and the exact command output for 14 days.
