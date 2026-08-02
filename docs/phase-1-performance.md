# Phase 1 performance evidence

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

$env:STRATA_SEG_ROWS='256'
$env:STRATA_SEG_QUERIES='16'
cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
```

The manifest-growth run uses deterministic id-only rows and measures the current full-manifest
publication path. The lifecycle run uses the `Dataset` facade, drops and reopens the dataset, scans
and filters it, performs vector searches, pins immutable snapshots, and executes concurrent commits.
The segment run commits each contiguous segment through `Dataset`, then searches the reopened
manifest-listed immutable segments through `Snapshot`; production parameters `M=16`,
`ef_construction=100`, `ef_search=32`, `max_layer=16`, and `k=10` remain unchanged.

Cache state is not forcibly flushed: recovery follows the preceding write in the same process, and
the first vector query warms each segment before timing. The results therefore describe a warm local
filesystem/query path. The lifecycle benchmark reports allocation and peak-live deltas, not RSS.

## Bounded results

| Workload | Observed result | Interpretation |
|---|---|---|
| 40 sequential id-only commits | 1.60 s total; manifest bytes 711 / 2,916 / 5,401 / 10,372 at retained versions 1 / 10 / 20 / 40; first-to-last timing bucket 1.23x | No strong latency-growth signal at this small point; manifest bytes grow across all retained versions. Repeat at larger history before drawing a scaling conclusion. |
| 64 synthetic 512-dim rows, 8-row commits | reopen 21.66 ms; 24 concurrent commits 792.68 ms; 4 historical/current pinned handles 600 ns (423,598 live allocator bytes); end-of-run manifest 24,544 bytes after concurrent commits | Shows the current reopen, serialized-fsync, and historical-snapshot retention paths on this host; it is not an operating limit. |
| 256 synthetic 512-dim rows, 16 queries, K=1…64 manifest-listed segments | K=1 recall@10 0.9938 at 98.2 us/query; K=64 recall@10 1.0000 at 157.2 us/query (1.6x) | This sample shows a bounded fan-out result without a recall drop; it does not prove behavior for production-scale data or all distributions. |

Input hashes: lifecycle `97f1b4d1524e42f1`; segment recall `6263e3d344dba5e7`.

## Current operating envelope

These measurements establish a reproducible bounded evidence point for the immutable,
manifest-listed segment path and the current lifecycle. The small sweep covers 40 retained
manifest versions and the lifecycle records reopen time, end-of-run manifest bytes, and pinned historical snapshots.
They do not establish a universal latency,
memory, recovery-time, or segment-count guarantee. The manifest and segment sets grow with retained
commits in the current implementation. Compaction, vacuum, orphan cleanup, retention policy, and
indefinite sustained operation remain Phase 3 work. Portable benchmark provenance, a captured host
matrix, and real-fixture measurements remain open evidence work; PERF-01 through PERF-05 therefore
remain tracked rather than marked universally remediated.
