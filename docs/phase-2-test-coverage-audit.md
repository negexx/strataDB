# Phase 2 test-coverage audit

| Invariant/behavior | Evidence | Result | Gap |
|---|---|---|---|
| Typed schema, projection, and filters | `crates/txn/src/query.rs` tests; CLI tests | Exact-head CI passed | No fresh local runtime due missing linker |
| Point lookup and physical RowId outcomes | txn query tests and CLI tests | Exact-head CI passed | Keep tombstone/not-found regressions |
| Group-by semantics | txn/query tests and CLI/binding tests | Exact-head CI passed | All-null aggregate behavior is covered; no package-level smoke test |
| Vector search/filter/hydration | txn, CLI, and binding tests | Exact-head CI passed | No broad recall/SLO claim |
| Python IPC and typed exceptions | In-process PyO3 tests in `crates/bindings/src/lib.rs`; packaged-wheel smoke in `.github/workflows/phase-2-query-evidence.yml` | Evidence path implemented | Cloud run required for current result |
| CLI stable output and acknowledgement boundary | `crates/cli/tests/phase_2_cli.rs` | Exact-head CI passed | Compatibility commands remain intentionally separate |

## Findings

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| VER2-01 | P2 | Python coverage exercises the Rust facade in-process, not a built/imported wheel or installed extension. | Add packaging smoke coverage before claiming distribution readiness. |
| VER2-02 | P2 | Narrow-read, memory, and fairness evidence is absent as measured Phase 2 acceptance data. | Keep as evidence gaps; no runtime defect inferred. |
| VER2-03 | P3 | Local native runtime gates cannot run without `link.exe`. | Use the exact-head cloud run for current runtime evidence. |
