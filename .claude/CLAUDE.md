# Strata

> Project memory for Claude Code. Loaded automatically into every session.
> Keep this file under ~200 lines — it's always in context.

## What this project is

Strata is an embedded, single-node database engine that lets multiple AI agents read from and write to the same store *concurrently*, with real transactional guarantees — no lost updates, no phantom reads, no silently stale vector search results — spanning both structured columns and vector embeddings in one unified columnar format.

The flagship claim: "correct under concurrent multi-agent writes, with no silent buffering." Vector storage (HNSW search, columnar format) is a mostly-solved problem elsewhere (LanceDB, Qdrant, pgvector) — Strata's differentiator is the transaction/conflict layer sitting between every reader/writer and the manifest, treating write durability and index/row consistency as first-class, not bolted on. See `.claude/docs/architecture.md` for the full design and `.claude/docs/decisions/` for why key tradeoffs (Rust over C++, snapshot isolation over serializability, single-node only, HNSW-only) were made.

## Stack

- **Language:** Rust, edition 2024
- **Build system / package manager:** Cargo workspace (`crates/*` member crates)
- **Test framework:** built-in `cargo test` (workspace-wide); `cargo-nextest` as an optional faster runner — not installed by default, install with `cargo install cargo-nextest` if wanted
- **Linter:** `clippy` (workspace lints in root `Cargo.toml`: `clippy::all` + `clippy::pedantic` at warn, `unwrap_used`/`expect_used` at warn)
- **Formatter:** `rustfmt` (`rustfmt.toml` — stable-only options; `imports_granularity`/`group_imports` are nightly-only and deliberately not used)
- **Columnar library:** `arrow` (arrow-rs)
- **HNSW library:** `hnsw_rs` (pure Rust — chosen over `usearch`'s Rust bindings specifically to avoid re-introducing a C++ core via FFI, which would undercut the reason for switching to Rust; see `docs/decisions/0005-rust-over-cpp-reversal.md`). Like every HNSW library audited (C++ or Rust), it doesn't expose graph internals for a native delta log — Strata's transaction shim maintains that log itself.
- **Python bindings:** PyO3 (modern `#[pymodule] mod { #[pymodule_export] ... }` form, not the older function-based API) + `maturin` for building wheels
- **Concurrency correctness:** `loom` (exhaustive interleaving testing of locks/atomics/CAS loops — this is the whole reason Rust was the original recommendation) + `madsim`/`turmoil` for FoundationDB-style deterministic simulation (Phase 7). Unlike C++, both are real, maintained, reusable crates — no bespoke VOPR-style simulator has to be built from scratch.

## Commands

| Task | Command |
|------|---------|
| Install deps | `cargo build` (Cargo resolves and fetches automatically — no separate install step) |
| Build | `cargo build --workspace` |
| Typecheck | `cargo check --workspace` (fast, no codegen) |
| Test | `cargo test --workspace` (or `cargo nextest run` if installed) |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| Format | `cargo fmt` |
| Format check | `cargo fmt --check` |
| Python bindings | `maturin develop` (from `crates/bindings/`) or `maturin build --release` for a wheel |

## Architecture at a glance

Layered, top to bottom — every read/write from the client layer passes through the transaction layer; nothing bypasses it:

```
Client Layer (query API, dataloader API, CLI, Python bindings)
   -> Query Executor | Vector Index (HNSW) | Random-Access Reader
        -> Transaction & Conflict Resolution Layer  (flagship — OCC, snapshot isolation, atomic row+index commits)
             -> Manifest / Version Layer  (commit = atomic CAS of "current version")
                  -> Columnar Storage Format  (append-only files, local disk first)
```

Cargo workspace layout:
- `crates/storage/` — columnar file format, manifest/versioning (`strata-storage`)
- `crates/txn/` — transaction & conflict resolution (the flagship subsystem — see `rules/concurrency-txn-layer.md`) (`strata-txn`)
- `crates/index/` — HNSW vector index, append-only delta log (see `rules/vector-index.md`) (`strata-index`)
- `crates/query/` — expression/filter API, vectorized scan/filter/agg (`strata-query`)
- `crates/bindings/` — PyO3 Python bindings, builds `strata_ext` (see `rules/python-bindings.md`)
- `crates/cli/` — `strata` binary, CLI for inspecting datasets/manifests
- `tests/sim/` — deterministic simulation / chaos harness using `madsim` (Phase 7 — the correctness-proof suite)
- `bench/` — benchmarks (`criterion`, once real code exists to benchmark)

Unit tests live inline per crate (`#[cfg(test)] mod tests`) — idiomatic Rust, not a centralized top-level `tests/` binary like the abandoned C++ scaffold used.

Full phase-by-phase roadmap and the full architecture diagram: `.claude/docs/architecture.md`. Phase 0's deliverable — the precise definitions of "conflict" and "transaction boundary," and the file format — is done: `.claude/docs/design/phase-0-transaction-and-format-spec.md`. Read it before writing anything real in `crates/txn/` or `crates/storage/`; don't improvise a definition that contradicts it.

## Conventions

Non-obvious patterns only — see `.claude/docs/conventions.md` for the full Rust style guide.

- **No write is acknowledged until it's durable, conflict-checked, and visible.** No async "we'll get to it" buffering, ever — this is a correctness invariant, not a style preference.
- **The vector index shares the transaction boundary with row data.** Index mutations are an append-only delta log, never in-place graph mutation, so they can commit atomically alongside row writes.
- **Isolation level is snapshot isolation, not serializability** — don't add serializability machinery; it's an explicit, documented cut (see `docs/decisions/`).
- **Conflicts are surfaced via a typed error identifying the contested rows, never silently resolved.** A last-writer-wins mode may exist but must be opt-in.
- Safe Rust by default; `unsafe` requires a `// SAFETY:` comment justifying the invariant it upholds, and is denied implicitly (`unsafe_op_in_unsafe_fn = "deny"` workspace-wide).
- Prefer `Result<T, E>` with typed errors over panics; `unwrap()`/`expect()` are `clippy::warn` workspace-wide, not silently allowed.
- Every concurrency-touching change gets a `loom` test for the interleavings that matter — the borrow checker prevents data races in safe code, it doesn't prove your OCC logic is correct.
- Vertical slices over layers: every milestone should run end-to-end, however small — see the Design Principles in `.claude/docs/architecture.md`.

## Model dispatch

Pick the tier that matches the task's complexity, not the biggest model available.

| Task | Model |
|------|-------|
| Trivial — search, read, copy, simple lookups | Haiku 4.5 |
| Implementation — basic to medium complexity (DEFAULT) | Sonnet 5 |
| In-depth planning, architecture, highly complex implementation | Fable 5 — fall back to Opus 4.8 if Fable 5 isn't available |
| Review — every completed task, before it's marked done | Opus 4.8 (mandatory, not an escalation) |

**Escalate** Sonnet 5 → Fable 5 (→ Opus 4.8 if Fable 5 is unavailable) when: the task is architectural, security-critical, the approach is genuinely unclear, or a wrong call here is expensive to undo. Given this project's flagship subsystem (Phase 6 concurrency engine) is exactly that kind of work by design, escalate liberally when touching `crates/txn/`.

**Downgrade** back to Sonnet 5 once the approach is settled and the remaining work is mechanical.

**Review is not optional.** Every task — regardless of which model implemented it — goes through an Opus 4.8 review (the `reviewer` subagent) before it's marked done.

Never drive a main session on Haiku 4.5. Never skip the Opus 4.8 review step to save time.

## What "done" means

Before claiming work is complete:

1. `cargo build --workspace` succeeds with no warnings (clippy is the real gate, see below)
2. `cargo test --workspace` passes
3. `cargo clippy --workspace --all-targets -- -D warnings` is clean
4. New behavior has a test (TDD for non-trivial logic) — for anything touching `crates/txn/` or `crates/index/`, that includes a `loom` interleaving test, not just a happy-path unit test
5. Reviewed by the `reviewer` subagent (Opus 4.8) — no task is marked done without this, regardless of which model implemented it

## Skills — when to invoke

Invoke the skill BEFORE acting. A ≥1% chance it applies means you MUST invoke it.

**Process skills — change HOW you work:**

| Situation | Skill |
|---|---|
| Bug / test failure / unexpected behavior | `superpowers:systematic-debugging` |
| New feature or creative work — before any code | `superpowers:brainstorming` |
| Multi-file task (feature, refactor, migration) | `superpowers:writing-plans` |
| Non-trivial logic (transactions, conflict detection, index math) | `superpowers:test-driven-development` |
| Plan with multiple independent tasks | `superpowers:subagent-driven-development` |
| 2+ tasks with no shared state | `superpowers:dispatching-parallel-agents` |
| Before claiming done / before commit or PR | `superpowers:verification-before-completion` |
| Feature branch implementation complete | `superpowers:finishing-a-development-branch` |

**Domain skills:**

| Situation | Skill |
|---|---|
| Writing code against arrow-rs, hnsw_rs, PyO3, loom, or madsim APIs | `context7-mcp` |
| Hard architectural or tradeoff decision (isolation level, conflict granularity, index format) | `llm-council` |
| Prior-art research before a design decision — this project's Design Principle #7 | `deep-research` |
| Confirm a change works end-to-end, not just under `cargo test` | `run` |

## Don't

- Don't commit `.env*` files or `target/` build artifacts
- Don't push to `main` directly — PRs only
- Don't add dependencies without justifying them in the commit message
- Don't weaken the "no silent write-buffering" invariant to chase throughput — it's a non-negotiable design principle, not a tunable
- Don't add serializability, multi-node/distributed transactions, IVF-PQ, a SQL parser, or temporal/graph memory features — see the Non-Goals table in `.claude/docs/architecture.md` before reopening any of these
- Don't let index mutations happen outside the transaction layer's delta log, even for "just a quick fix"
- Don't reach for `unsafe` to work around the borrow checker instead of restructuring — if `unsafe` is genuinely needed, it needs a `// SAFETY:` comment and extra review scrutiny, not a shortcut

## External references

Reading list from the project spec — study before Phase 0 (design) and Phase 7 (correctness harness) especially:

- Jepsen (Kyle Kingsbury) — methodology for testing distributed/concurrent correctness claims
- FoundationDB engineering blog — deterministic simulation testing approach (`madsim`/`turmoil` are the reusable Rust equivalent)
- CockroachDB / TiDB public design docs — MVCC and transaction internals
- *Designing Data-Intensive Applications* (Kleppmann) — transactions/consistency chapters
- Lance format — storage/versioning reference
- DuckDB — vectorized execution reference
- CMU 15-445 / 15-721 — curriculum spine for storage/execution phases

---

*This file is the project's persistent memory. Keep it accurate. Stale instructions are worse than missing ones.*
