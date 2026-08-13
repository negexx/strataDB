# Phase 1 concurrency audit

## Result

The supported boundary remains one process sharing one `Dataset` handle. The commit path acquires a
preparation lease, performs I/O before `commit_lock`, reloads current state under the lock, checks
write conflicts, publishes the manifest, and only then swaps the immutable snapshot. The graph and
source inspection found no new lock-order inversion in the documented path. The separate-process
coordination decision is maintained only in the Phase 4 reservation.

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| CONC-01 | P1 evidence gap | Transaction loom models were not runnable locally. | `cargo rustc -p strata-index --lib --profile test -- --cfg loom` fails at missing `link.exe`; transaction models have the same native-link requirement. | Blocked; run exact CI model list on Linux/Windows CI and retain artifacts. |
| CONC-02 | P1 evidence gap | Full shared-handle interleaving coverage is not freshly verified on this branch. | Existing model names and tests are present, but local integration tests cannot link. | Blocked on runnable native test environment. |
| CONC-04 | P2 evidence gap | Lifecycle/commit fairness under sustained contention is not measured. | Performance evidence labels lifecycle fairness a future gate. | Add a bounded workload and policy before claiming fairness. |
| CONC-05 | P3 maintainability | Publication has a dense lock/visibility ordering that is correctness-critical. | `Transaction::commit` contains the full ordering and extensive invariants in one method. | Consider review-only refactoring after verification; no speculative rewrite now. |
