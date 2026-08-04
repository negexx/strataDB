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
output, exact command/configuration provenance, fixture revision/size/SHA-256, seed, cache policy,
repetitions, and a GNU time report. `summarize.py` emits machine-readable JSONL and CSV deltas only
after verifying the complete like-for-like matrix. Fixture records are rejected unless they match the
pinned Qdrant revision and SHA-256 exactly.

The fixture identity is `Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K`, revision
`56e6849a3d0f7913e56b475bf92c0064c93b576d`, file `data/train-00000-of-00001.parquet`, exactly
363758493 bytes, SHA-256
`5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d`.

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

Set `STRATA_REAL_FIXTURE=1` and `STRATA_BENCH_FIXTURE=/path/to/train-00000-of-00001.parquet` to
retain the pinned fixture identity and add a real-fixture segmented `Dataset`/`Snapshot` smoke run.
The fixture is copied into each disposable worktree at the benchmark path expected by the current
benchmarks; it is never added to Git. Synthetic benchmark behavior remains unchanged.

## GitHub Actions

`Phase 1 portability evidence` is manually dispatchable and runs an Ubuntu/Windows matrix. Both
runners retain raw native evidence and provenance (OS, architecture, filesystem, CPU/RAM observation,
toolchain, source revision, seed, cache policy, and repetitions). Ubuntu can additionally download,
verify, and smoke-test the pinned real fixture; Windows records that this optional measurement is
skipped rather than presenting a synthetic result as fixture evidence. Artifacts are retained for 14
days. This infrastructure records bounded observations only; it makes no production-limit or
performance claim.
