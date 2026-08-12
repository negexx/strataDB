# Phase 2 evidence matrix

| Area | Current evidence | Status | Remaining limitation |
|---|---|---|---|
| Rust query contract | `crates/txn/src/query.rs` and focused tests | Implemented within bounds | Local execution unavailable without MSVC linker |
| CLI | `crates/cli/tests/phase_2_cli.rs` | Implemented within bounds | Legacy compatibility commands are not generic query APIs |
| Python | In-process PyO3 tests in `crates/bindings/src/lib.rs` | Implemented within bounds | No wheel/install smoke test |
| Integration | Exact-head GitHub Actions run `31644869407` | Passed | Cloud evidence, not local reproduction |
| Narrow reads/pruning | `bench/benches/projected_read_bench.rs`; `.github/workflows/phase-2-query-evidence.yml`; Phase 3 pruning tests | Implemented evidence path | A successful run supplies evidence, not a product SLO |
| Memory/concurrency | `query_concurrency_bench`; Phase 2 workflow `/usr/bin/time -v` capture | Implemented evidence path | Measurements are bounded observations, not RSS or fairness guarantees |
| Scope boundary | `AGENTS.md`, `docs/design.md`, `docs/decisions.md` | Explicit | Cross-process and serializability remain Phase 4/non-claims |
