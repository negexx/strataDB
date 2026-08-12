# Phase 1 evidence matrix

| Finding | Source | Test/command | Severity | Status/disposition |
|---|---|---|---|---|
| COR-01..05 | `crates/txn/src/dataset.rs`, storage/index validation | targeted dataset/storage/index tests | P0 historical | Implemented and exact-head runtime-verified in CI run 31644869407 |
| COR-06 / VER-01 | workspace manifests and native runtime gate | `cargo test --workspace --no-default-features` | P1 | Exact-head GitHub Actions run 31644869407 passed; local MSVC linker remains unavailable |
| COR-07 / REF-01 | `crates/txn/src/dataset.rs:1223-1484` | graph complexity query | P2 | Deferred bounded refactor |
| PERF-01 | `bench/`, `docs/phase-1-performance.md` | Cloud before/after workflow | P1 | Closed within named bounded workload by successful run 31647664161 |
| PERF-02 | manifest/segment lifecycle design | existing phase performance matrices | P1 | Deferred Phase 3 retention/compaction decision |
| PERF-03..05 / MEM-01..04 | snapshot/cache/index and performance docs | benchmark recipes | P2/P1 | Evidence gaps or deferred design; no code fix |
| CONC-01..02 | `crates/txn` loom modules, CI workflow | exact scoped loom command | P1 | Exact-head GitHub Actions run 31644869407 passed |
| VER-02..04 | `.github/workflows/ci.yml`, docs | CI provenance, cargo deny | P1/P2 | Exact-head CI and benchmark provenance passed in runs 31644869407 and 31647664161 |
| DOC-01..05 | AGENTS/docs/toolchain/CI | `rg`, `cargo doc` | P2/P3 | Documentation tasks |
| REF-01..04 | txn/storage source | clippy/fmt/doc/graph | P2/P3 | Deferred/refactoring-only; no speculative edits |

## Scope classification

- **Confirmed defects:** none newly confirmed in executable behavior during this run.
- **Evidence gaps:** none for the named Phase 1 bounds; universal operating limits remain outside the phase contract.
- **Documentation-only:** DOC-01 through DOC-05 and rustdoc warnings.
- **Deferred design:** PERF-02, MEM-01, CONC-03/04, and lifecycle reclamation.
- **Intentionally unsupported:** compaction, remote storage,
  and universal power-loss guarantees.
