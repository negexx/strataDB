# Phase 1 evidence matrix

| Finding | Source | Test/command | Severity | Status/disposition |
|---|---|---|---|---|
| COR-01..05 | `crates/txn/src/dataset.rs`, storage/index validation | targeted dataset/storage/index tests | P0 historical | Implemented; runtime gate blocked locally |
| COR-06 / VER-01 | workspace manifests and MSVC toolchain | `cargo test --workspace --no-default-features` | P1 | Blocked: `link.exe` missing |
| COR-07 / REF-01 | `crates/txn/src/dataset.rs:1223-1484` | graph complexity query | P2 | Deferred bounded refactor |
| PERF-01 | `bench/`, `docs/phase-1-performance.md` | Criterion bench commands | P1 | Blocked: native linking; no before/after |
| PERF-02 | manifest/segment lifecycle design | existing phase performance matrices | P1 | Deferred Phase 3 retention/compaction decision |
| PERF-03..05 / MEM-01..04 | snapshot/cache/index and performance docs | benchmark recipes | P2/P1 | Evidence gaps or deferred design; no code fix |
| CONC-01..02 | `crates/txn` loom modules, CI workflow | exact scoped loom command | P1 | Blocked: linker; CI rerun required |
| CONC-03 | status/design/decision 0010 | documentation inspection | P2 | Intentionally unsupported Phase 1 scope |
| VER-02..04 | `.github/workflows/ci.yml`, docs | CI provenance, cargo deny | P1/P2 | Older remote evidence or local environment block |
| DOC-01..05 | AGENTS/docs/toolchain/CI | `rg`, `cargo doc` | P2/P3 | Documentation tasks |
| REF-01..04 | txn/storage source | clippy/fmt/doc/graph | P2/P3 | Deferred/refactoring-only; no speculative edits |

## Scope classification

- **Confirmed defects:** none newly confirmed in executable behavior during this run.
- **Evidence gaps:** COR-06, PERF-01, CONC-01/02, VER-01/02/03/04.
- **Documentation-only:** DOC-01 through DOC-05 and rustdoc warnings.
- **Deferred design:** PERF-02, MEM-01, CONC-03/04, and lifecycle reclamation.
- **Intentionally unsupported:** cross-process publication, serializability, compaction, remote storage,
  and universal power-loss guarantees.
