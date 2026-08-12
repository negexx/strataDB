# Phase 1 test-coverage audit

## Coverage map

| Invariant/behavior | Tests/evidence present | Fresh local result | Gap |
|---|---|---|---|
| Schema ownership and reserved columns | `crates/txn/src/dataset.rs` tests | Not runnable: linker | CI/runtime evidence needed |
| Row-ID monotonicity and restart non-reuse | `row_id_reservation_restart.rs`, dataset regressions | Not runnable: linker | Native runtime gate |
| Tombstone/update/delete target semantics | dataset and transaction integration tests | Not runnable: linker | Native runtime gate |
| Manifest/row/segment identity and CRC rejection | storage/index/dataset tests and fuzz targets | Index unit tests: 158 passed; storage/txn blocked | Runtime/fuzz artifact refresh |
| Shared-handle OCC and snapshot visibility | dataset tests, `concurrent_snapshot_isolation.rs`, loom models | Blocked: linker | Exact loom and integration runs |
| Chaos/checkpoint recovery | storage checkpoint test and `tests/sim` | Not run: linker | CI/Ubuntu/Windows artifacts |
| Python/CLI facade | bindings and CLI integration tests | Not run: linker | Native test and package verification |
| Performance/memory bounds | Criterion benches and phase performance tables | Not run: linker | Fresh provenance and no universal claim |

## Findings

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| VER-01 | P1 | The local environment cannot execute the majority of runtime gates. | Blocked; provision MSVC linker or use CI. |
| VER-02 | P1 | Exact-head CI evidence referenced by current docs is for older revisions, not `224ea42`. | Refresh CI artifact links/provenance on this branch. |
| VER-03 | P1 | Fuzz, chaos, and loom evidence is historical/remote rather than freshly reproduced here. | Retain as supporting evidence; do not close the gate locally. |
| VER-04 | P2 | `cargo deny` is blocked by a read-only advisory DB lock path. | Rerun with writable Cargo advisory DB in CI or approved environment. |
| VER-05 | P2 | `cargo doc` emits two warnings. | Fix documentation links in a bounded cleanup task. |
| VER-06 | P3 | Test/benchmark command recipes are spread across docs and CI. | Consolidate after the phase gate; not a correctness blocker. |
