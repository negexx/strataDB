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

The table above is the retained canonical summary of the per-repetition measurements, timing
variances, manifest checkpoint bytes, command exit codes, and failed CPU/RAM-provenance attempts;
no separate Task 5 report artifact is checked in. These are host-local observations
over this exact synthetic envelope, not a universal or asymptotic bound. They do not establish
portable performance, a retained-history limit, or real-fixture behavior.

The checked-in `bench/cloud-performance/` harness and
`.github/workflows/cloud-performance-before-after.yml` workflow now provide a reproducible cloud
comparison path. A later portability run also passed the native foundation matrix on Ubuntu and
Windows and exercised the pinned real fixture on Ubuntu; the full-dataset-scale comparison and
universal operating bounds remain open.

## Cloud before/after comparison (fresh, bounded, synthetic)

**Run:** [GitHub Actions 30881988012](https://github.com/negexx/strataDB/actions/runs/30881988012),
Ubuntu 24.04 x86_64, four vCPUs, Rust 1.90.0, lockfile SHA-256
`252e017f63e8bfbe6f6521fb9fb5d39085b1961b2f73d75f48479f9cd20b305b`, with separate target
directories per revision and OS caches left untouched. The retained artifact is named
`cloud-performance-before-after-30881988012-attempt-1`.

The directly comparable pair is merged PR #56,
`76d12919b5234f5e089cf26e4ba469e7aaa982f0`, versus the evidence branch,
`c64f000ea96efdfbf754de3de94ef294984550e1`. Both use deterministic synthetic workloads. The
candidate-side changes in this comparison are evidence-harness and validation changes, not a
production performance optimization; observed deltas therefore must not be attributed to a product
performance fix. The corrected run validated 370 records and 185 before/after deltas.

| Workload | Before | After | Relative change |
|---|---:|---:|---:|
| Manifest growth, 160 commits: wall | 562.894 ms | 794.943 ms | +41.22% |
| Manifest growth, 160 commits: newest manifest | 40,470 B | 40,474 B | +0.01% |
| Segment K=1 unfiltered: us/query | 39.8 | 40.3 | +1.26% |
| Segment K=64 unfiltered: us/query | 62.0 | 62.2 | +0.32% |
| Lifecycle, 0 pins: ingest+commit wall | 284.99 ms | 248.57 ms | -12.78% |
| Lifecycle, 64 pins: ingest+commit wall | 243.15 ms | 293.10 ms | +20.54% |

These are small, synthetic, VM-local observations—not proof of a broad performance win or
regression. The matrix does not isolate the statistics path, full real-fixture before/after
behavior, portable native filesystems, RSS/process-memory bounds, projection/pruning, or a
supported segment/history maximum. The performance-related Phase 1 rows therefore remain
evidence-limited rather than closed by this comparison. The separate portability run
[30881986345](https://github.com/negexx/strataDB/actions/runs/30881986345) passed Ubuntu/Windows
native foundation checks and Ubuntu fixture smoke (download, size, SHA, and segmented search).

## Cloud before/after comparison (fresh, pinned real-fixture segment matrix)

**Run:** [GitHub Actions 30892210202](https://github.com/negexx/strataDB/actions/runs/30892210202),
Ubuntu 24.04 x86_64, Rust 1.90.0, the same lockfile SHA-256 and separate revision target
directories as the synthetic run. The retained artifact is named
`cloud-performance-before-after-30892210202-attempt-1`.

The directly comparable pair is merged PR #56,
`76d12919b5234f5e089cf26e4ba469e7aaa982f0`, versus the evidence branch,
`dad48a8ee0cf561bdc304e18b02576c2154fb0e8`. Both use the exact pinned Qdrant fixture identified
above; the workflow recorded and verified size `363758493` and SHA-256
`5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d` before running either
revision. The segment benchmark uses a bounded 256-row prefix of that 100K-row fixture, 16 fixed
queries, K=1…64, one excluded warmup, and five measured repetitions per K/mode. Both revisions
emitted the same fixture input hash `7a911b4e03268d44`; `summarize.py --validate` accepted 542
records and 269 before/after deltas.

| Workload | Before | After | Relative change |
|---|---:|---:|---:|
| Fixture K=1 unfiltered: median µs/query | 45.4 | 46.1 | +1.54% |
| Fixture K=1 filtered: median µs/query | 47.9 | 46.8 | -2.30% |
| Fixture K=64 unfiltered: median µs/query | 80.4 | 84.2 | +4.73% |
| Fixture K=64 filtered: median µs/query | 57.8 | 59.5 | +2.94% |
| Fixture recall, all reported K/modes | 1.0000 | 1.0000 | 0.00% |

This is the complete before/after K/mode matrix on the bounded fixture prefix, not a full 100K-row
benchmark. It provides real-input provenance and measured direction for the current segmented path;
it does not establish a production performance win, a full-dataset bound, RSS/process-memory bound,
statistics-path isolation, projection/pruning behavior, or a supported segment/history maximum.

## Cloud before/after comparison (fresh, full pinned 100K-row fixture)

**Run:** [GitHub Actions 30901929352](https://github.com/negexx/strataDB/actions/runs/30901929352),
Ubuntu 24.04 x86_64, Rust 1.90.0, lockfile SHA-256
`252e017f63e8bfbe6f6521fb9fb5d39085b1961b2f73d75f48479f9cd20b305b`, separate target directories,
and OS caches left untouched. The retained artifact is
`cloud-performance-before-after-30901929352-attempt-1`; `summarize.py --validate` accepted 640
records and 314 before/after deltas.

The directly comparable pair is merged `origin/main` at
`21811031d0fbe3ed3f55532941c056c0c9e091b0` versus closeout commit `821dad3`. Both revisions loaded
the exact pinned Qdrant fixture (363,758,493 bytes; SHA-256
`5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d`) and emitted the identical
input hash `f09d3ebad4b1a2b3`. The segmented matrix used 100,000 rows, 200 fixed queries, K=1...64,
both filtered/unfiltered modes, one excluded warmup, and five measured repetitions. The lifecycle
matrix used the same 100,000 rows, 5,000 rows per commit (20 commits), one retained snapshot, one
excluded warmup, and five measured repetitions, with recovery-byte accounting and all ten lifecycle
phases retained.

| Workload | Before | After | Relative change |
|---|---:|---:|---:|
| Fixture lifecycle ingest+commit wall | 25,200 ms | 25,730 ms | +2.10% |
| Fixture lifecycle recovery/reopen wall | 194.86 ms | 201.25 ms | +3.28% |
| Fixture lifecycle full scan wall | 58.47 ms | 59.41 ms | +1.61% |
| Fixture lifecycle unfiltered vector-search wall | 163.22 ms | 166.79 ms | +2.19% |
| Fixture K=1 unfiltered median us/query | 163.3 | 163.7 | +0.24% |
| Fixture K=16 unfiltered median us/query | 2,942.0 | 3,228.9 | +9.75% |
| Fixture K=64 unfiltered median us/query | 9,234.0 | 9,252.0 | +0.19% |
| Fixture recall, reported K/modes | unchanged | unchanged | 0.00% |

This run did not measure a performance improvement: the closeout candidate is modestly slower on
the listed wall-time and median-latency samples, while recall is unchanged. The candidate changes
are benchmark/evidence validation changes rather than a production optimization, so these results
must not be attributed to a product performance fix or generalized into a universal regression.
They are now full-fixture bounded evidence for PERF-01 and the named PERF-03/PERF-04/PERF-05
protocols. They do not establish universal latency, memory, recovery, RSS, statistics/projection,
segment-count, or lifecycle-reclamation bounds; compaction, vacuum, retention, orphan cleanup, and
cross-process durability remain deferred.

## Task 6 recovery-byte accounting evidence (fresh, bounded)

**Run date:** 2026-08-02. **Implementation revision:** `4b39f16`; the
deterministic Task 6 regression and its evidence are recorded in this canonical
performance record. **Runner/toolchain:** local
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
`1.90.0 (840b83a10 2025-07-30)`. **Benchmark source revision:** `1f879ac`.
The benchmark used default workspace
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
| 1 | 1.0000 / 1.0000 | 34.7 / 35.9 | 28,843 / 29,851 | 1.0000 / 1.0000 | 35.8 / 37.1 | 27,947 / 28,923 | 0.053 | 1.2 MiB |
| 2 | 1.0000 / 1.0000 | 47.5 / 47.9 | 21,065 / 21,243 | 1.0000 / 1.0000 | 37.5 / 37.8 | 26,636 / 26,783 | 0.099 | 1.0 MiB |
| 4 | 1.0000 / 1.0000 | 43.8 / 45.3 | 22,860 / 23,669 | 1.0000 / 1.0000 | 23.2 / 23.7 | 43,099 / 44,004 | 0.224 | 0.9 MiB |
| 8 | 1.0000 / 1.0000 | 31.9 / 33.5 | 31,399 / 32,935 | 1.0000 / 1.0000 | 17.4 / 17.6 | 57,413 / 57,887 | 0.360 | 0.9 MiB |
| 16 | 1.0000 / 1.0000 | 20.0 / 20.7 | 49,961 / 51,613 | 1.0000 / 1.0000 | 14.1 / 14.3 | 70,739 / 71,365 | 0.942 | 0.9 MiB |
| 32 | 1.0000 / 1.0000 | 16.9 / 17.7 | 59,488 / 62,598 | 1.0000 / 1.0000 | 15.3 / 15.4 | 65,388 / 65,681 | 1.473 | 0.9 MiB |
| 64 | 1.0000 / 1.0000 | 16.0 / 16.4 | 62,591 / 64,205 | 1.0000 / 1.0000 | 22.8 / 23.9 | 43,972 / 46,189 | 3.181 | 1.4 MiB |

Peak-live is the benchmark process's global allocator peak growth above the
start-of-K sample, spanning build, reopen, warmup, and measured query sweeps. It is not
RSS, total process memory, or a retained-snapshot bound. The table is the
K-dependent observation for this short run; its direction must be read from
the recorded rows, not inferred as a monotonic segmentation effect. The
unfiltered K=64 median was 16.0 us/query versus 34.7 at K=1. This is a
host-local observation, not evidence of a segment-count maximum. Filtered
results include the warmed predicate live-set cache and are not a cold-filter
latency claim.

An unchanged pre-improvement 256-row, 16-query command was also reproduced on
this host with the same synthetic seed and input hash `6263e3d344dba5e7`.
That older harness performed one unfiltered timed sweep per K and did not
report repeat statistics or the filtered facade path, so it is retained only
as reproduction history and is not part of this supported evidence envelope.

## Task 8 retained snapshot/cache footprint evidence (fresh, bounded)

**Run date:** 2026-08-03. **Measurement source:** the Task 8 working tree
based on `1511b3b4472aa428ec72e1352634ebab82a066e5`, with the lifecycle
retained-footprint diagnostic added in this task. **Lockfile SHA-256:**
`2e6dfa6a8a1c8afd17085660894361256c319f876f5440e19b2902d9d336bb39`.
**Runner/toolchain:** local Windows `10.0.26200`, `x86_64-pc-windows-msvc`;
rustc `1.90.0 (1159e78c4 2025-09-14)` and Cargo `1.90.0
(840b83a10 2025-07-30)`. The local Windows temporary filesystem was used and
OS/filesystem caches were not flushed. CPU/RAM/OS CIM queries were denied with
`0x80041003`, so no CPU or RAM inventory is claimed.

The lifecycle benchmark used deterministic xorshift64 synthetic input: 64
rows, 512 dimensions, seed `20260801`, input hash `97f1b4d1524e42f1`, and 64
one-row commits. At every requested retained-handle point (exactly 0, 1, 4,
16, and 64), it reopened through `Dataset::open_with_recovery_accounting`,
constructed the final retained `pinned` set, and then warmed one `category =
3` filtered-vector-search entry on each retained snapshot. Each pinned handle
is paired with the exact just-published manifest payload and its
manifest-listed row-data and immutable-segment entries before a later commit
can occur. After the final set is constructed, the benchmark sums those
per-handle records and also deduplicates their physical manifest/file
identities. The current operational snapshot needed for the rest of the
lifecycle is deliberately excluded from the retained-handle count. On
2026-08-03, each point ran one excluded warmup and five measured repetitions
(30 isolated lifecycle processes total); deterministic input and the requested
pin count were passed to every child process. The command was:

```text
$env:STRATA_BENCH_SOURCE='synthetic'; $env:STRATA_BENCH_SEED='20260801'
$env:STRATA_LIFECYCLE_ROWS='64'; $env:STRATA_LIFECYCLE_BATCH_ROWS='1'
$env:STRATA_PINNED_SNAPSHOTS='0,1,4,16,64'
$env:STRATA_LIFECYCLE_WARMUP_RUNS='1'; $env:STRATA_LIFECYCLE_REPETITIONS='5'
cargo bench -p strata-bench --bench lifecycle_bench -- --noplot
```

Each child labels itself as either `excluded warmup 1/1` or `measured
repetition N/5`, and prints direct accounting separately from cache/allocator
observations. The direct row-data and immutable-segment values below were
identical across the five measured repetitions for their pin count. Manifest
payload is also read and reported exactly for every retained snapshot in every
child, but varies slightly with serialized per-run metadata; it is deliberately
not collapsed into a fabricated cross-run aggregate.

| Retained handles / distinct snapshots | Measured repetitions | Direct manifest payload bytes | Direct manifest-listed row-data bytes (logical / unique) | Direct manifest-listed immutable-segment bytes (logical / unique) |
|---:|---:|---:|---:|
| 0 / 0 | 5 | 0 / 0 in every repetition | 0 / 0 | 0 / 0 |
| 1 / 1 | 5 | per-repetition exact payload | 4,362 / 4,362 | 2,240 / 2,240 |
| 4 / 4 | 5 | per-repetition exact payload | 43,620 / 17,448 | 22,400 / 8,960 |
| 16 / 16 | 5 | per-repetition exact payload | 593,232 / 69,792 | 304,640 / 35,840 |
| 64 / 64 | 5 | per-repetition exact payload | 9,072,960 / 279,168 | 4,659,200 / 143,360 |

The manifest payload byte count is the exact serialized payload read and
validated for each pinned snapshot; row-data and immutable-segment bytes are
the exact `byte_len` values listed in that snapshot's manifest. `logical`
sums every retained handle's manifest-listed reference. `unique` deduplicates
manifest versions and immutable row-data/segment filenames across the final
pinned set. Thus logical totals describe retained references, while unique
totals describe the distinct raw file payloads they retain; neither is process
RSS, allocator residency, filesystem block allocation, or a universal bound.

| Retained handles / distinct snapshots | Measured repetitions | Cache entries per repetition | Cache charged bytes (approx., per repetition) | Aggregate cache budget (per repetition) |
|---:|---:|---:|---:|---:|
| 0 / 0 | 5 | 0 | 0 | 0 |
| 1 / 1 | 5 | 1 | 272 B | 64 MiB |
| 4 / 4 | 5 | 4 | 1,088 B | 256 MiB |
| 16 / 16 | 5 | 16 | 4,352 B | 1 GiB |
| 64 / 64 | 5 | 64 | 17,408 B | 4 GiB |

`charged bytes` is the live-set cache's admission-control accounting (entry
overhead, predicate-key bytes, and `LiveSet` payload). It is intentionally
approximate: it excludes hash-map buckets, synchronization/`Arc` headers, and
allocator metadata; aggregate budget is a soft per-snapshot cap, not retained
usage. Snapshot/cache wall time and allocator deltas remain per-process
approximate observations and are intentionally not summarized as a memory
bound. No RSS sample was taken; any future RSS value is supplemental only.
The lifecycle explicitly drops the retained handles, operational snapshot, and
dataset before removing its temporary directory. The focused transaction
regression performs this creation/warming/immutability/release lifecycle for
exactly 0, 1, 4, 16, and 64 pins.

This is bounded local synthetic evidence only. It does not establish a
portable or real-fixture result, an exact RSS/process-memory bound, a cache
eviction guarantee, a retained-history limit, or universal PERF-05 closure.

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
$env:STRATA_LIFECYCLE_BATCH_ROWS='1'
$env:STRATA_PINNED_SNAPSHOTS='0,1,4,16,64'
$env:STRATA_LIFECYCLE_WARMUP_RUNS='1'
$env:STRATA_LIFECYCLE_REPETITIONS='5'
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
indefinite sustained operation remain Phase 3 work. Native foundation provenance, Ubuntu fixture
smoke, and a validated full 100K-row real-fixture before/after matrix are now captured. Universal
operating bounds remain open evidence work; PERF-01 through PERF-05 are bounded-evidence rows rather
than universal performance guarantees.
