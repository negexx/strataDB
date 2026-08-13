# Phase 1 performance audit

**Run:** 2026-08-12. No optimization was implemented during this audit.

## Result

The checked-in benchmark documentation contains bounded synthetic and pinned-fixture measurements
for manifest growth, recovery accounting, immutable-segment fan-out, and retained snapshots. It
explicitly does not define supported maxima, cold-cache latency, universal RSS, or sustained lifecycle
bounds. The current implementation still serializes publication and grows manifests/segments with
commits; compaction and reclamation remain Phase 3.

The benchmark binaries could not be freshly executed on this host because the MSVC linker is absent.
Therefore there is no new before/after measurement and no optimization claim.

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| PERF-01 | P1 evidence gap | No fresh local benchmark run for this branch. | Full pinned-fixture and synthetic cloud benchmark run `31647664161` completed successfully with retained provenance. | Closed within the named cloud workload; no universal performance claim. |
| PERF-02 | P1 deferred design | Manifest and segment fan-out grow with commits. | `docs/phase-1-performance.md` records K=1..64 and 1/10/20/40/80/160 commit matrices without a supported bound. | Defer compaction/history policy to Phase 3; keep evidence-only envelope. |
| PERF-03 | P2 evidence gap | Cold filtered I/O is unmeasured. | Current benchmark documentation labels filtered searches warm-cache only. | Add a named cold-cache recipe before setting a product objective. |
| PERF-04 | P2 evidence gap | Recovery and memory measurements are not portable RSS bounds. | Current evidence uses payload/accounting and host-local allocator observations. | Keep claims scoped; add platform matrix only after an operating budget exists. |
| PERF-05 | P2 concurrency evidence gap | No fresh p95/p99 shared-handle contention run. | Existing measurements are bounded observations, not a fairness or queueing contract. | Add a named workload and fairness objective within the shared-handle boundary. |

## Existing evidence envelope

Use the current tables in `docs/phase-1-performance.md` as baseline only. They report K=1, 2, 4,
8, 16, 32, and 64 segment fan-out, 0/1/4/16/64 retained handles, and deterministic synthetic
inputs. Since no performance code changed, before/after is **N/A** for this audit.
