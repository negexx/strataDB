# Phase 2 whole-branch refactoring audit

The supported dependency direction is coherent: `strata-txn` owns the dataset/snapshot contract,
`strata-query` supplies query primitives, and CLI/PyO3 remain thin adapters. No dependency or
on-disk-format refactor is justified by this audit.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| REF2-01 | Reduced | `strata-query` exposed implementation submodules in addition to its supported root re-exports. | Query implementation modules are now private; storage/index visibility remains intentionally public to workspace-internal consumers and is not a published API. |
| REF2-02 | Reduced | Query execution and result conversion span several adapters, so contract drift is possible without cross-surface tests. | Existing focused tests plus the packaged-wheel smoke job cover the current surfaces. |
| REF2-03 | P3 | Historical MVP names and comments add phase vocabulary noise. | Clean up without changing behavior. |
