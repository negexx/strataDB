# Phase 2 performance audit

**Run:** 2026-08-13. No optimization was implemented during this audit.

The query facade uses projected reads and applies filtering before result conversion. The branch
has no isolated Phase 2 counters proving bytes read, columns skipped, or pruning effectiveness on
the manifest-listed segment path. This is an evidence gap, not a confirmed runtime regression.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| PERF2-01 | Implemented evidence path | Narrow-read savings were not measured on the current path. | Added `bench/benches/projected_read_bench.rs` and `.github/workflows/phase-2-query-evidence.yml`, comparing full-file and one-column reads over a deterministic 100,000-row fixture. A successful run is still not a product objective. |
| PERF2-02 | P2 | No Phase 2 latency/RSS contract is claimed for CLI or Python conversion. | Keep the API bounded; do not infer a product SLO from existing tests. |
| PERF2-03 | P3 | Benchmark and query evidence recipes are split across phase documents. | Consolidate with the next evidence refresh. |

The Phase 1 pinned benchmark run `31647664161` is supporting repository evidence, not a Phase 2
query-operator benchmark.
