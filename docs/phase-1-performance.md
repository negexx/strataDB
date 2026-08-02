# Phase 1 performance evidence

## Task 5 manifest-growth evidence (fresh, bounded)

**Run date:** 2026-08-02
**Source revision:** `472368e0d5c6119e46c41f11840ed0f6772c52e7` (committed Task 5
benchmark reporting revision).
**Lockfile SHA-256:** `2e6dfa6a8a1c8afd17085660894361256c319f876f5440e19b2902d9d336bb39`.
**Runner:** Microsoft Windows `10.0.26200`, `x86_64-pc-windows-msvc`.
**Toolchain:** `rustc 1.90.0 (1159e78c4 2025-09-14)`; Cargo `1.90.0
(840b83a10 2025-07-30)`; default workspace features and no benchmark-specific feature flags.
**Filesystem and cache policy:** the benchmark creates its directory through `std::env::temp_dir()` on
the local Windows temporary filesystem. OS/filesystem caches were not flushed between the excluded
warmup and measured repetitions.
**CPU/RAM:** not captured. The attempted `Get-CimInstance Win32_Processor` and
`Get-CimInstance Win32_ComputerSystem` calls were denied with `Access denied` (`0x80041003`).

The input is deterministic synthetic id-only data: sequential `i64` values from `0` through
`commits - 1`, one row per commit, with no `vector` column and therefore no HNSW insertion. Each
point ran one full warmup sequence excluded from results, then five measured sequences. The exact
commands used the current `Dataset` transaction/manifest path:

```text
$env:STRATA_GROWTH_COMMITS='1';   $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
$env:STRATA_GROWTH_COMMITS='10';  $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
$env:STRATA_GROWTH_COMMITS='20';  $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
$env:STRATA_GROWTH_COMMITS='40';  $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
$env:STRATA_GROWTH_COMMITS='80';  $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
$env:STRATA_GROWTH_COMMITS='160'; $env:STRATA_GROWTH_WARMUP_RUNS='1'; $env:STRATA_GROWTH_REPETITIONS='5'; cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
```

| Sequential commits | Median wall time | p95 wall time | Median newest manifest bytes | p95 manifest bytes |
|---:|---:|---:|---:|---:|
| 1 | 50.846 ms | 58.733 ms | 712 | 712 |
| 10 | 468.254 ms | 504.589 ms | 2,917 | 2,918 |
| 20 | 1,015.169 ms | 1,024.508 ms | 5,403 | 5,408 |
| 40 | 2,380.915 ms | 2,427.971 ms | 10,377 | 10,383 |
| 80 | 5,528.159 ms | 5,636.349 ms | 20,328 | 20,331 |
| 160 | 14,075.935 ms | 14,250.955 ms | 40,475 | 40,489 |

The full per-repetition raw output, timing variances, manifest checkpoint bytes, command exit codes,
and failed CPU/RAM-provenance attempts are in the Task 5 report. These are host-local observations
over this exact synthetic envelope, not a universal or asymptotic bound. They do not establish
portable performance, a retained-history limit, or real-fixture behavior.

No checked-in `bench/cloud-performance/` harness or
`.github/workflows/cloud-performance-before-after.yml` workflow exists on this branch. Consequently
PERF-01 remains open for portable, real-fixture, and cloud provenance even though this bounded
synthetic PERF-02 matrix is recorded.

## Task 6 recovery-byte accounting evidence (fresh, bounded)

**Run date:** 2026-08-02. **Baseline revision:**
`698185f7901ef935dacc63e09384b643ee28e12f`; the Task 6 implementation and
its final commit are recorded in the task report. **Runner/toolchain:** local
Windows `10.0.26200`, `x86_64-pc-windows-msvc`; rustc `1.90.0
(1159e78c4 2025-09-14)` and Cargo `1.90.0 (840b83a10 2025-07-30)`.

The recovery diagnostic reports only payload bytes loaded and validated by a
successful `Dataset::open_with_recovery_accounting`: the selected current
manifest, immutable row-ID reservation records, manifest-listed Arrow row
files, and manifest-listed immutable vector segments. It excludes process RSS,
allocator churn, directory-listing metadata, and retained manifest versions
that recovery did not open. The accounting regression covers empty, small
vector-bearing, retained-history, and 16-commit bounded-larger datasets, and
checks every category against the current manifest's actual listed files.

The lifecycle smoke used the existing workload unchanged except for choosing
the opt-in diagnostic reopen API:

```text
$env:STRATA_BENCH_SOURCE='synthetic'; $env:STRATA_BENCH_SEED='20260801';
$env:STRATA_LIFECYCLE_ROWS='16'; $env:STRATA_LIFECYCLE_BATCH_ROWS='4';
$env:STRATA_PINNED_SNAPSHOTS='2';
cargo bench -p strata-bench --bench lifecycle_bench -- --noplot
```

It committed 16 deterministic 512-dimensional rows in four commits, reopened
before the existing retained-snapshot and concurrent-commit phases, and
reported 82,134 recovery payload bytes: 3,954 manifest, 44,328 row data, 60
row-ID catalog, and 33,792 immutable segments. Reopen wall time was 12.68 ms;
the same run reported two pinned historical/current snapshots and 24 concurrent
commits. This is one warm local-filesystem synthetic observation, not a
universal recovery, retained-history, or concurrent-operation bound.

## Task 7 immutable-segment fan-out evidence (fresh, bounded)

**Run date:** 2026-08-03. **Runner/toolchain:** local Windows `10.0.26200`,
`x86_64-pc-windows-msvc`; rustc `1.90.0 (1159e78c4 2025-09-14)` and Cargo
`1.90.0 (840b83a10 2025-07-30)`. The benchmark used default workspace
features, no benchmark-specific feature flags, and the local Windows temporary
filesystem. OS/filesystem caches were not flushed.

The final measured workload was deterministic xorshift64 synthetic input with
seed `20260801`, input hash `97f1b4d1524e42f1`, 64 rows, 512 dimensions, and
8 queries. Each K creates K contiguous commits through `Dataset`, drops and
reopens the dataset, then searches the manifest-listed immutable segments via
`Snapshot`. K was exactly 1, 2, 4, 8, 16, 32, and 64; `M=16`,
`ef_construction=100`, `ef_search=32`, `max_layer=16`, and `k=10` were held
fixed. Each K used one complete unfiltered-plus-filtered warmup sweep excluded
from results, followed by two complete measured sweeps. The reported p95 uses
the nearest-rank method; the median is the central-value average for an even
number of samples.

The unfiltered path calls `Snapshot::vector_search(query, 10, None)`. The
filtered path calls the same public API with `segment_cohort = 0`; cohorts
alternate by contiguous segment, so K=1 contains only cohort 0 and K>1 selects
approximately half of the segment cohorts. The benchmark verifies that every
filtered hit belongs to cohort 0 and scores each mode against its own exact
brute-force top-10 ground truth. Thus these figures exercise the current
manifest-listed immutable-segment fan-out and filtered facade path; they do not
compare the retired direct `HnswIndex` path.

This is an evidence-only measured envelope. It adds no typed API guard and no
supported maximum for segment count, latency, recall, or memory.

| K | Unfiltered recall@10 median / p95 | Unfiltered us/query median / p95 | Unfiltered QPS median / p95 | Filtered recall@10 median / p95 | Filtered us/query median / p95 | Filtered QPS median / p95 | Build+reopen s | Peak-live delta |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1.0000 / 1.0000 | 59.2 / 61.1 | 16,894 / 17,422 | 1.0000 / 1.0000 | 59.2 / 59.8 | 16,892 / 17,050 | 0.084 | 1.0 MiB |
| 2 | 1.0000 / 1.0000 | 90.7 / 91.9 | 11,033 / 11,186 | 1.0000 / 1.0000 | 74.6 / 75.6 | 13,411 / 13,594 | 0.125 | 0.8 MiB |
| 4 | 1.0000 / 1.0000 | 83.4 / 83.4 | 11,991 / 11,998 | 1.0000 / 1.0000 | 43.6 / 43.7 | 22,943 / 23,022 | 0.322 | 0.7 MiB |
| 8 | 1.0000 / 1.0000 | 57.5 / 58.8 | 17,390 / 17,774 | 1.0000 / 1.0000 | 30.9 / 31.0 | 32,416 / 32,560 | 0.584 | 0.7 MiB |
| 16 | 1.0000 / 1.0000 | 38.7 / 39.4 | 25,852 / 26,316 | 1.0000 / 1.0000 | 28.3 / 29.3 | 35,401 / 36,731 | 1.085 | 0.6 MiB |
| 32 | 1.0000 / 1.0000 | 34.6 / 37.5 | 29,076 / 31,521 | 1.0000 / 1.0000 | 32.5 / 33.7 | 30,826 / 32,000 | 2.206 | 0.6 MiB |
| 64 | 1.0000 / 1.0000 | 47.9 / 49.1 | 20,905 / 21,453 | 1.0000 / 1.0000 | 78.6 / 83.2 | 12,766 / 13,507 | 4.903 | 1.1 MiB |

Peak-live is the benchmark process's global allocator delta above the start of
each K, spanning build, reopen, warmup, and measured query sweeps. It is not
RSS, total process memory, or a retained-snapshot bound. The table is the
K-dependent observation for this short run; its direction must be read from
the recorded rows, not inferred as a monotonic segmentation effect. The
unfiltered K=64 median was 47.9 us/query versus 59.2 at K=1. This is a
host-local observation, not evidence of a segment-count maximum. Filtered
results include the warmed predicate live-set cache and are not a cold-filter
latency claim.

An unchanged pre-improvement 256-row, 16-query command was also reproduced on
this host with the same synthetic seed and input hash `6263e3d344dba5e7`.
That older harness performed one unfiltered timed sweep per K and did not
report repeat statistics or the filtered facade path, so it is retained only
as reproduction history and is not part of this supported evidence envelope.

## Earlier related measurements

**Run date:** 2026-08-02
**Revision:** `16812af3c196f993ac37834a8a6c06eb5ac6a0b5` (benchmark implementation used for these measurements)
**Host:** Windows MSVC, `x86_64-pc-windows-msvc`
**Toolchain:** `rustc 1.90.0 (1159e78c4 2025-09-14)`, Cargo 1.90.0
**Features:** workspace defaults; no benchmark-specific feature flags
**Filesystem:** local Windows temporary filesystem (directory handles and fsyncs use the configured
local durability path); OS cache is not flushed between phases.
**CPU:** not captured by the benchmark harness.
**RAM:** not captured by the benchmark harness; results are host-local evidence, not a portable baseline.

## Reproduction matrix

The commands below use the checked-in benchmarks and a deterministic synthetic source because the
optional `bench/data/dbpedia-openai-100k.parquet` fixture is not present in this checkout. Set
`STRATA_BENCH_SOURCE=fixture` (and optionally `STRATA_BENCH_FIXTURE`) when a compatible fixture is
available. The synthetic generator is xorshift64 with seed `20260801`; the reported input hashes
are guards against accidental recipe changes.

```text
$env:STRATA_GROWTH_COMMITS='40'
cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot

$env:STRATA_BENCH_SOURCE='synthetic'
$env:STRATA_BENCH_SEED='20260801'
$env:STRATA_LIFECYCLE_ROWS='64'
$env:STRATA_LIFECYCLE_BATCH_ROWS='8'
$env:STRATA_PINNED_SNAPSHOTS='4'
cargo bench -p strata-bench --bench lifecycle_bench -- --noplot

$env:STRATA_BENCH_SOURCE='synthetic'
$env:STRATA_BENCH_SEED='20260801'
$env:STRATA_SEG_ROWS='64'
$env:STRATA_SEG_QUERIES='8'
$env:STRATA_SEG_WARMUP_RUNS='1'
$env:STRATA_SEG_REPETITIONS='2'
cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
```

The manifest-growth run uses deterministic id-only rows and measures the current full-manifest
publication path. The lifecycle run uses the `Dataset` facade, drops and reopens the dataset, scans
and filters it, performs vector searches, pins immutable snapshots, and executes concurrent commits.
The segment run commits each contiguous segment through `Dataset`, then searches the reopened
manifest-listed immutable segments through `Snapshot`; production parameters `M=16`,
`ef_construction=100`, `ef_search=32`, `max_layer=16`, and `k=10` remain unchanged. It records
unfiltered and `segment_cohort=0` filtered results separately, after one full warmup sweep, over two
measured sweeps per K.

Cache state is not forcibly flushed: recovery follows the preceding write in the same process, and
the first vector query warms each segment before timing. The results therefore describe a warm local
filesystem/query path. The lifecycle benchmark reports allocation and peak-live deltas, not RSS.

## Earlier bounded results

| Workload | Observed result | Interpretation |
|---|---|---|
| 40 sequential id-only commits | Superseded for manifest-growth evidence by the fresh Task 5 1/10/20/40/80/160 matrix above; this earlier one-run sample is retained only as historical context. | Use the fresh five-repetition values above for current bounded manifest-growth evidence. |
| 64 synthetic 512-dim rows, 8-row commits | reopen 21.66 ms; 24 concurrent commits 792.68 ms; 4 historical/current pinned handles 600 ns (423,598 live allocator bytes); end-of-run manifest 24,544 bytes after concurrent commits | Shows the current reopen, serialized-fsync, and historical-snapshot retention paths on this host; it is not an operating limit. |
| 256 synthetic 512-dim rows, 16 queries, K=1…64 manifest-listed segments | K=1 recall@10 0.9938 at 98.2 us/query; K=64 recall@10 1.0000 at 157.2 us/query (1.6x) | This sample shows a bounded fan-out result without a recall drop; it does not prove behavior for production-scale data or all distributions. |

Input hashes: lifecycle `97f1b4d1524e42f1`; earlier segment recall
`6263e3d344dba5e7`; Task 7 segment recall `97f1b4d1524e42f1`.

## Current operating envelope

These measurements establish reproducible bounded evidence points for the immutable,
manifest-listed segment path and the current lifecycle. Task 7 supports only the named 64-row,
512-dimensional, 8-query synthetic workload across K=1, 2, 4, 8, 16, 32, and 64, with the
recorded cache and repeat policy; the table records its K-dependent direction without a monotonic
degradation claim. It is an evidence-only operational envelope: no typed API guard
or supported maximum follows from it. The small sweep covers 40 retained manifest versions and the lifecycle records reopen time, end-of-run manifest bytes, and pinned historical snapshots.
They do not establish a universal latency,
memory, recovery-time, or segment-count guarantee. The manifest and segment sets grow with retained
commits in the current implementation. Compaction, vacuum, orphan cleanup, retention policy, and
indefinite sustained operation remain Phase 3 work. Portable benchmark provenance, a captured host
matrix, and real-fixture measurements remain open evidence work; PERF-01 through PERF-05 therefore
remain tracked rather than marked universally remediated.
