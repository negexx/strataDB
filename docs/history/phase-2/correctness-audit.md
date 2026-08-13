# Phase 2 correctness audit

**Run:** 2026-08-13. **Scope:** typed snapshot queries, projection/filtering, lookup, group-by,
vector search, Python bindings, and CLI surfaces.

## Result

No new P0 or P1 correctness defect was confirmed. The typed facade validates schema, projection,
filters, row IDs, aggregates, and vector dimensions before execution. Focused CLI and PyO3 tests
cover the public result/error shapes. The exact-head GitHub Actions suite passed the workspace and
Phase 2 integration gates; local native execution remains blocked by the missing MSVC linker.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| COR2-01 | P0/P1 resolved | Query requests are snapshot-bound and typed; internal `_row_id` is not exposed as a user projection. | Retain focused regressions. |
| COR2-02 | P1 resolved by scope | Independent openers do not coordinate. | Explicitly outside Phase 2; reserved for Phase 4. |
| COR2-03 | Resolved | All-null `Min`/`Max`/`Avg` groups previously returned floating-point sentinels. | Nullable result cells and focused regressions are present in `crates/query/src/group_by.rs`. |
