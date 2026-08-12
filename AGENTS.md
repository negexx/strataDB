# Strata

> Durable project guidance for Codex. It applies to the whole repository. Keep this file concise;
> put task-specific reasoning in `docs/` and use closer-scoped `AGENTS.md` files only when a subtree
> genuinely needs different rules.

## Mission

Strata is an embedded, single-node Rust database for concurrent AI-agent access to structured
columns and vector embeddings. Its supported concurrency scope is one process using a shared
`Dataset` handle. The intended target is immutable snapshot reads, write-write optimistic conflict
detection, durable manifest publication, and row/index consistency without silent buffering; the
2026-08-01 Phase 1 audit records counterexamples and keeps this boundary Partial and blocked.

Non-negotiable target invariants:

- A write must be acknowledged only after it is durable, conflict-checked, and visible. The current
  implementation does not yet prove every part of this target; see `docs/status.md` and the Phase 1
  audit before describing it as achieved behavior.
- Row data and vector-index changes share one transaction boundary and manifest publication.
- The vector index uses immutable per-commit segments listed in the manifest; do not reintroduce the
  retired pre-S1 shared-index mechanism.
- The intended isolation ceiling is snapshot isolation, not serializability. The current API is
  narrower—immutable snapshot reads plus write-write OCC—so do not claim a full read/write snapshot
  transaction interface or add another isolation level without a superseding ADR.
- Conflicts are typed errors that identify contested row IDs; never silently resolve them.
- Row IDs are intended to be dataset-global physical allocation values: monotonically allocated,
  never reused; gaps are safe. Restart non-reuse is regression-covered, while final CI and
  platform evidence remain part of the Phase 1 gate. `update` tombstones an old physical row and
  inserts a replacement with a new ID.
- Strata remains embedded and single-node. Distributed transactions, full SQL, and additional ANN
  index families are out of scope.

## Source of truth

Before changing behavior, read the narrowest current sources needed:

1. `docs/architecture.md`, `docs/status.md`, and `docs/roadmap.md` for current system shape,
   implementation boundaries, and planned work.
2. `docs/design.md` before `crates/txn` or `crates/storage` work.
3. `docs/decisions.md` and `docs/phase-1-audit.md` for governing decisions and current blockers.
4. Current source, tests, Cargo manifests, and `.github/workflows/ci.yml`.
5. Relevant historical material under `docs/history/` when the current documents need rationale.

Historical summaries preserve rationale and may describe superseded mechanisms or paths. Resolve
disagreements against the current documents, current code, and tests. The Rust toolchain supersedes
the old C++ direction; immutable index segments are current; group commit remains only proposed.

## Codebase knowledge graph

This project uses `codebase-memory-mcp` for code discovery. Prefer its graph tools over grep, glob,
or broad file reads when locating or understanding code:

1. `search_graph` — find functions, structs, methods, routes, and variables.
2. `trace_path` — trace callers, callees, dependencies, or data flow.
3. `get_code_snippet` — read the exact source for a graph symbol.
4. `query_graph` — run Cypher for multi-hop or aggregate analysis.
5. `get_architecture` — inspect project structure, boundaries, layers, and hotspots.

Use grep/glob for string literals, configuration, scripts, and non-code files, or only when the
graph does not contain the needed result. Index the repository before discovery when the index is
missing or stale; do not re-index unnecessarily.

## Workspace

- Rust edition 2024, pinned Rust toolchain 1.97.1, Cargo workspace.
- `crates/storage`: columnar files, manifest/versioning, backend abstraction.
- `crates/txn`: OCC, snapshots, commit protocol, row/index atomicity—the flagship subsystem.
- `crates/index`: from-scratch HNSW and immutable on-disk segments.
- `crates/query`: predicates, pruning, vectorized operators, group-by.
- `crates/bindings`: PyO3 extension module.
- `crates/cli`: `strata` command-line interface.
- `crates/chaos-worker` and `tests/sim`: crash/concurrency correctness harness.
- `bench`: Criterion benchmarks; `fuzz`: separate cargo-fuzz workspace.

## Commands

```text
cargo check --workspace
cargo build --workspace
cargo test --workspace --no-default-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo doc --workspace --no-deps
cargo deny check bans sources advisories
```

`strata-bindings` keeps PyO3's `extension-module` feature enabled by default for packaging. Native
Rust test binaries must use `--no-default-features` so the bindings tests link the Python
interpreter instead of relying on extension-module's unresolved-symbol behavior.

When relevant, also run:

```text
cargo test -p strata-txn --features parallel-insert
cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings
```

For loom, follow the crate-scoped
`cargo rustc -p strata-index --lib --profile test -- --cfg loom` pattern (substituting
`strata-txn` only when testing that crate) and run the produced test binary directly. Never set
workspace-wide `RUSTFLAGS=--cfg loom`; it breaks dependency crates whose loom crate is dev-only.
Use the exact current CI/module recipe.

## Engineering rules

- Safe Rust by default. Every `unsafe` block needs a `// SAFETY:` invariant; never use `unsafe` to
  evade ownership design.
- Use typed `Result` errors on engine paths. Avoid `unwrap()`/`expect()` in production and at the
  PyO3 boundary.
- Keep PyO3 thin, release the GIL around blocking I/O/locks, and preserve distinct conflict errors.
- No dependency additions without explicit justification and user approval.
- HNSW parameter changes require benchmark evidence. Performance work must state the measured
  baseline and preserve correctness gates.
- Every interleaving-sensitive `crates/txn`/`crates/index` change needs a targeted loom model plus
  normal tests. State why loom is not applicable when a nearby change is purely structural.
- Preserve on-disk compatibility deliberately. Reject corrupt/unknown state loudly; never make a
  compatibility change accidentally.
- Prefer vertical slices, focused files, and the smallest change that satisfies the approved design.

## Luna → Sol → Terra workflow

- **Luna** owns intake, read-only triage, status/baseline capture, dispatch, and final consolidation.
  Luna may answer questions and handle trivial orchestration, but routes non-trivial changes through
  Sol before Terra.
- **Sol** owns architecture, concurrency analysis, design specs, ADRs, implementation plans, and
  the final complete-branch review. In review mode Sol makes no edits.
- **Terra** executes one approved plan task, uses TDD for behavior changes, runs task verification,
  and returns evidence. A separate Terra instance independently reviews each completed task; the
  implementation worker never reviews or self-approves its own diff. Terra does not redesign
  requirements silently.

For non-trivial work:

1. Luna records objective, allowed/forbidden scope, dirty baseline, controlling docs, and success
   criteria.
2. Sol explores current code/docs/history, compares approaches, obtains design approval, and writes
   the spec/plan.
3. Luna dispatches one bounded task at a time to Terra. Parallelize read-only independent work only;
   do not allow overlapping writers.
4. Terra returns files changed, commands/results, deviations, and blockers—never an unsupported
   whole-task completion claim.
5. A separate Terra reviewer independently reviews each completed implementation task, including
   `crates/txn` or `crates/index` changes, and records concrete findings before acceptance.
6. Sol performs the final complete-branch review before merge and reviews architecture/design work
   or any superseding concurrency or durability decision. Terra resolves accepted findings and
   reruns affected checks. Luna reads the final diff and fresh verification output before reporting
   completion.

Every handoff includes the objective, exact file scope, invariants, controlling spec/plan, interfaces,
expected checks, dirty-worktree notes, authority limits, and required return evidence.

## Skills and documentation

Before acting, inspect the skills/tools actually exposed in the session. Use relevant Superpowers
workflows when available: brainstorming for design, systematic debugging for failures, writing-plans
before multi-step edits, TDD for behavior changes, and verification-before-completion before success
claims. Read and follow the selected skill; do not merely name it. Never claim an unavailable skill,
connector, subagent, or reviewer was used.

For external APIs/crates, prefer an exposed authoritative documentation tool. Otherwise inspect
official docs and installed dependency source, then verify assumptions by compiling/testing. Do not
hard-code credentials or machine-local connector configuration in the repository.

## Verification and review gate

- Guidance/config-only changes: parse config, smoke-test Codex discovery, scan for stale references
  and credentials, run `git diff --check`, and verify exact file scope.
- Rust changes: targeted red/green tests, then build, workspace tests, clippy, and format.
- Feature, loom, benchmark, chaos, fuzz, and binding checks are additional gates when the affected
  subsystem or controlling plan requires them.
- Evidence comes from fresh command output and diff inspection, not agent confidence or a prior run.
- Do not call unrelated dirty Rust work green after a documentation/config-only task.

## Worktree and Git safety

- Assume the worktree may contain user changes. Start with `git status --short`; preserve unrelated
  modifications and untracked files.
- Never reset, checkout, restore, clean, delete, or reformat outside the assigned scope.
- Do not commit, stage, push, branch, force-push, or open a PR unless the user explicitly requests it.
- If committing is authorized, stage only reviewed task files and inspect the staged diff first.
- Never commit `.env*`, credentials, auth/session data, build artifacts, benchmark datasets, or
  machine-local Codex state.
