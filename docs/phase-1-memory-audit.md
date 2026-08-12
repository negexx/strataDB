# Phase 1 memory-footprint audit

## Result

The immutable snapshot design retains manifest references and manifest-listed data/segment files;
live-set cache admission is budgeted per snapshot. Physical live-set structures are proportional to
the maximum row ID, not row count, and recovery rejects unreasonable high-water values before index
allocation. The checked-in lifecycle evidence distinguishes logical retained bytes from unique payload
bytes and explicitly does not claim RSS or allocator bounds.

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| MEM-01 | P1 deferred design | Retained snapshots and immutable segments can grow without a product retention bound. | Architecture and performance docs state no compaction/vacuum/segment reclamation. | Phase 3 design work; not a Phase 1 code defect. |
| MEM-02 | P2 evidence gap | No fresh RSS/peak-RSS measurement was possible. | Benchmark documentation reports accounting/allocator observations only; local benchmark linking is blocked. | Add named platform measurements before claiming a budget. |
| MEM-03 | P2 confirmed complexity risk | Live-set structures scale with high row IDs, so sparse/high physical IDs can consume more memory than row count suggests. | Index tests document byte size proportional to maximum row ID; recovery has a capacity guard. | Preserve guard; document as an operating constraint and benchmark sparse IDs. |
| MEM-04 | P3 evidence gap | Cache accounting excludes buckets, Arc/synchronization headers, and allocator metadata. | `docs/phase-1-performance.md` states the accounting is approximate. | Keep the limitation explicit; no speculative data-structure change. |

No data structure change is recommended without a measured sparse-ID workload and an approved design.
