# Phase 1 correctness audit

**Run:** 2026-08-12, branch `codex/phase-1-audit`, Rust 1.97.1 MSVC
**Scope:** storage, transaction publication/recovery, manifest-listed index segments, query facade, bindings, CLI, chaos worker, and simulation harness.

## Result

No new correctness counterexample was confirmed from static inspection. The commit path reloads the
latest snapshot under `commit_lock`, checks write-write conflicts before manifest mutation, validates
the published segment dimension, commits the manifest before swapping the in-memory snapshot, and
persists row/attempt high-water marks. Recovery validates schema, row ownership, tombstones, and
manifest-listed segments before constructing a snapshot (`crates/txn/src/dataset.rs:512-608` and
`1223-1484`).

The result is not a green correctness gate: the transaction and storage integration suites could not
run locally because the configured MSVC target has no `link.exe`. Existing repository regressions and
remote evidence remain bounded evidence, not fresh verification of this branch.

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| COR-01 | P0 historical/remediated | Future tombstones could hide later inserts. | Current base-snapshot target validation and targeted tombstone tests are present; runtime suite was not linkable locally. | Retain as a required regression gate; no new fix proposed. |
| COR-02 | P0 historical/remediated | Restart could reuse abandoned physical row IDs. | Durable high-water catalog and restart tests are present; runtime suite was not linkable locally. | Retain as a required regression gate; no new fix proposed. |
| COR-03 | P0 historical/remediated | Directory durability failures could be acknowledged. | Fail-closed sync paths and fault-injection tests are present; runtime suite was not linkable locally. | Retain named local-filesystem boundary; no universal durability claim. |
| COR-04 | P0 historical/remediated | Manifest filename/version identity was insufficiently checked. | Recovery derives and validates the versioned manifest path and envelope. | Retain as a required regression gate. |
| COR-05 | P0 historical/remediated | Update/delete target and replacement cardinality were underspecified. | Current facade validates live physical targets and creates one replacement row. | Retain as a required regression gate. |
| COR-06 | P1 evidence gap | Full correctness suite is unverified on this branch. | `cargo test --workspace --no-default-features` and transaction feature tests fail before execution because `link.exe` is missing. | Blocked on a Windows build environment with MSVC linker or CI artifact. |
| COR-07 | P2 maintainability | `Transaction::commit` is a 262-line, complexity-16 publication procedure with multiple responsibilities. | Code graph reports complexity 16/cognitive 24 at `crates/txn/src/dataset.rs:1223-1484`. | Defer to a bounded refactor after Phase 1 gate; preserve commit ordering and no on-disk change. |

## Required regression commands

```text
cargo test --workspace --no-default-features
cargo test -p strata-txn --features parallel-insert
cargo test -p strata-txn --features test-fault-injection
```

All three were attempted. They are blocked by the missing linker, not by a test assertion.
