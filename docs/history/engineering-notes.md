# Historical engineering notes

This is the compact record of implementation-plan outcomes. It replaces dated step-by-step plans and
copied code scaffolding that are no longer useful as active instructions.

## Implementation chronology

- Rust remediation moved the repository from the early C++ assumptions to the current Cargo workspace.
- Transaction and harness work added shared-handle OCC, snapshots, loom models, process chaos, and
  recovery experiments. The Phase 1 audit later found allocator, durability, and contract gaps.
- Query and group-by work added predicates, pruning, filtered ANN primitives, and in-memory grouping;
  it did not create a full planner or SQL layer.
- HNSW work moved from mutable graph/delta-log material to immutable per-commit segments. The segment
  abstraction landed in PR #31 and the write/load cutover in PR #33.
- Zone-map work in PR #36 added file/segment pruning metadata.
- Chaos-worker work in PR #47 expanded real-process crash/reopen coverage while documenting that some
  suites and checkpoints still need CI gating.
- The 2026-08-01 documentation cleanup migrated OpenCode guidance to Codex, introduced the
  Luna -> Sol -> Terra workflow, and separated current guidance from historical rationale.

## Process policy

Plans are working instruments, not permanent documentation. Once a task is complete, retain only the
decision, durable design fact, verification evidence, and unresolved follow-up that a future reader
needs. Keep detailed implementation history in Git and link a PR or commit when it materially explains
the current code.
