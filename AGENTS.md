# Strata

> Project memory for OpenCode. Loaded automatically into every session.
> Keep this file under ~200 lines — it's always in context.

## What this project is

Strata is an embedded, single-node database engine that lets multiple AI agents read from and write to the same store *concurrently*, with real transactional guarantees — no lost updates, no phantom reads, no silently stale vector search results — spanning both structured columns and vector embeddings in one unified columnar format.

The flagship claim: "correct under concurrent multi-agent writes, with no silent buffering." Vector storage (HNSW search, columnar format) is a mostly-solved problem elsewhere (LanceDB, Qdrant, pgvector) — Strata's differentiator is the transaction/conflict layer sitting between every reader/writer and the manifest, treating write durability and index/row consistency as first-class, not bolted on. See `docs/architecture.md` for the full design and `docs/decisions/` for why key tradeoffs (Rust over C++, snapshot isolation over serializability, single-node only, HNSW-only) were made.

## Stack

- **Language:** Rust, edition 2024
- **Build system / package manager:** Cargo workspace (`crates/*` member crates)
- **Test framework:** built-in `cargo test` (workspace-wide); `cargo-nextest` as an optional faster runner — not installed by default, install with `cargo install cargo-nextest` if wanted
- **Linter:** `clippy` (workspace lints in root `Cargo.toml`: `clippy::all` + `clippy::pedantic` at warn, `unwrap_used`/`expect_used` at warn)
- **Formatter:** `rustfmt` (`rustfmt.toml` — stable-only options; `imports_granularity`/`group_imports` are nightly-only and deliberately not used)
- **Columnar library:** `arrow` (arrow-rs)
- **HNSW library:** none — `crates/index` is a from-scratch, fully lock-free HNSW implementation, replacing an earlier `hnsw_rs` dependency (`docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md`). The only remaining external dependency is `anndists` (with `simdeez_f` enabled), used solely for SIMD-accelerated distance kernels.
- **Python bindings:** PyO3 (modern `#[pymodule] mod { #[pymodule_export] ... }` form) + `maturin` for building wheels
- **Concurrency correctness:** `loom` (exhaustive interleaving testing of locks/atomics/CAS loops) for `crates/txn`/`crates/index`. Phase 7's correctness harness (`tests/sim`, `crates/chaos-worker`) follows Jepsen's methodology: real process spawn, real `std::process::abort()` at instrumented checkpoints, seed-reproducible scenarios.

## Commands

| Task | Command |
|------|---------|
| Install deps | `cargo build` (Cargo resolves and fetches automatically) |
| Build | `cargo build --workspace` |
| Typecheck | `cargo check --workspace` (fast, no codegen) |
| Test | `cargo test --workspace` (or `cargo nextest run` if installed) |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| Format | `cargo fmt` |
| Format check | `cargo fmt --check` |
| Python bindings | `maturin develop` (from `crates/bindings/`) or `maturin build --release` |

## Architecture at a glance

Layered, top to bottom — every read/write from the client layer passes through the transaction layer; nothing bypasses it:
-> Query Executor | Vector Index (HNSW) | Random-Access Reader
-> Transaction & Conflict Resolution Layer  (flagship — OCC, snapshot isolation, atomic row+index commits)
-> Manifest / Version Layer  (commit = atomic CAS of "current version")
-> Columnar Storage Format  (append-only files, local disk first)

Cargo workspace layout:
- `crates/storage/` — columnar file format, manifest/versioning (`strata-storage`)
- `crates/txn/` — transaction & conflict resolution (the flagship subsystem — see `.opencode/rules/concurrency-txn-layer.md`) (`strata-txn`)
- `crates/index/` — HNSW vector index, append-only delta log (see `.opencode/rules/vector-index.md`) (`strata-index`)
- `crates/bindings/` — PyO3 Python bindings, builds `strata_ext` (see `.opencode/rules/python-bindings.md`)
- `crates/cli/` — `strata` binary, CLI for inspecting datasets/manifests
- `tests/sim/` — deterministic simulation / chaos harness (Phase 7 — correctness suite)
- `bench/` — benchmarks (`criterion`)

Unit tests live inline per crate (`#[cfg(test)] mod tests`). Read `docs/design/phase-0-transaction-and-format-spec.md` before writing anything real in `crates/txn/` or `crates/storage/`.

## Conventions

- **No write is acknowledged until it's durable, conflict-checked, and visible.** No async "we'll get to it" buffering, ever.
- **The vector index shares the transaction boundary with row data.** Index mutations are an append-only delta log, never in-place graph mutation.
- **Isolation level is snapshot isolation, not serializability.**
- **Conflicts are surfaced via a typed error identifying the contested rows, never silently resolved.**
- Safe Rust by default; `unsafe` requires a `// SAFETY:` comment justifying the invariant it upholds (`unsafe_op_in_unsafe_fn = "deny"` workspace-wide).
- Prefer `Result<T, E>` with typed errors over panics; `unwrap()`/`expect()` trigger `clippy::warn`.
- Every concurrency-touching change gets a `loom` test for the interleavings that matter.
- Vertical slices over layers: every milestone should run end-to-end.

## Agents

Defined as markdown files in `.opencode/agents/`.

| Agent | Mode | Model | Role |
|-------|------|-------|------|
| `orchestrator` | primary | `opencode-go/deepseek-v4-flash` | Default session agent. Handles git/cargo, quick fixes, dispatches subagents |
| `@architect` | subagent | `opencode-go/glm-5.2` | System design, concurrency analysis, writes ADRs and `PLANS.md` |
| `@coder` | subagent | `opencode-go/glm-5.2` | Executes TDD cycles from PLANS.md. Writes code and tests |
| `@reviewer` | subagent | `opencode-go/glm-5.2` | Read-only audit of diffs before merge. Checks invariants, coverage, correctness |

### Dispatch Rules

- **Architecture & planning** → `@architect`
- **Implementation from a plan** → `@coder`
- **Pre-merge review** → `@reviewer` — mandatory for `crates/txn/` and `crates/index/` changes
- **Quick fixes, shell commands, orchestration** → `orchestrator` (do it yourself)

### Mandatory Review Gate

No non-trivial task is marked "done" without a `@reviewer` pass to ensure transaction invariants, clippy rules, and test coverage are strictly met.

## What "done" means

Before claiming work is complete:
1. `cargo build --workspace` succeeds with no warnings.
2. `cargo test --workspace` passes.
3. `cargo clippy --workspace --all-targets -- -D warnings` is clean.
4. New behavior has a test (TDD for non-trivial logic) — includes a `loom` test for `crates/txn/` or `crates/index/`.
5. Reviewed by `@reviewer` subagent — no task is marked done without this.

## Skills & MCP Tools — When to Invoke

Invoke relevant skills/tools BEFORE acting whenever there is a ≥1% chance they apply:

- **Bug / test failure:** Load `superpowers:systematic-debugging` skill.
- **New feature / creative work:** Load `superpowers:brainstorming` skill before writing code.
- **Multi-file task / refactor:** Load `superpowers:writing-plans` skill first.
- **Token efficiency / brevity:** Load `caveman` skill when user asks for concise output.
- **API / Library documentation:** ALWAYS use the `context7` MCP tool for `arrow-rs`, PyO3, `loom`, or external crate docs.
- **Hard architectural decisions:** Ask `@reviewer` for architectural review or trade-off evaluation.
- **Task Verification:** Perform verification steps before claiming complete.

## Don'ts

- Don't commit `.env*` files or `target/` build artifacts.
- Don't push to `main` directly — PRs only.
- Don't add dependencies without justifying them in the commit message.
- Don't weaken the "no silent write-buffering" invariant to chase throughput.
- Don't add serializability, multi-node/distributed transactions, IVF-PQ, or SQL parsers.
- Don't let index mutations happen outside the transaction layer's delta log.
- Don't reach for `unsafe` to work around the borrow checker instead of restructuring.