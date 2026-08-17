# Consumer Audit Issues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the documented CLI and Python consumer workflows accurately discoverable and verifiable.

**Architecture:** Keep the existing typed CLI contracts, add stable help/JSON behavior only where the current public surface already implies it, and align README examples with executable commands. Add regression tests at the CLI boundary before changing implementation. Do not expand transaction semantics or add dependencies.

**Tech Stack:** Rust 2024, Cargo integration tests, `strata-cli`, Markdown documentation, PyO3/maturin packaging checks.

## Global Constraints

- Keep all implementation work on `codex/fix-consumer-audit-issues`; do not edit `main`.
- Preserve the documented single-process/shared-`Dataset` boundary.
- Do not add dependencies.
- Do not make `lookup` JSON behavior inconsistent with the existing stable JSON admin surface.
- Keep the Python facade thin and verify packaging without changing its public scope.

---

### Task 1: Lock the consumer-facing regressions with tests

**Files:**
- Modify: `crates/cli/tests/admin_cli.rs`
- Modify: `crates/cli/tests/phase_2_cli.rs` only if the shared fixture is required

- [x] Add a help-contract test covering the documented `explain`, `lookup --json`, and typed `query-scan` forms.
- [x] Add an explain invocation test proving the README command shape succeeds with a real predicate and JSON output.
- [x] Add a lookup JSON test covering live, tombstoned, and not-found outcomes as structured JSON.
- [x] Run `cargo test -p strata-cli --test admin_cli --test phase_2_cli`; expected new assertions fail before implementation changes.

### Task 2: Implement the minimal CLI contract fixes

**Files:**
- Modify: `crates/cli/src/main.rs`

- [x] Extend CLI help with executable examples for `explain`, `lookup --json`, and `query-scan`.
- [x] Add structured JSON output for lookup outcomes while preserving existing human-readable output.
- [x] Keep parsing strict and return the existing stable usage/error categories.
- [x] Re-run the focused CLI tests and confirm green.

### Task 3: Align README and package verification guidance

**Files:**
- Modify: `README.md`
- Modify: `docs/phase-3-verification-report.md` only if the packaging limitation needs current evidence

- [x] Replace the invalid bare `explain` example with a valid predicate-bearing command.
- [x] Document the typed `query-scan`, `search`, `lookup --json`, and `group-by` examples.
- [x] State that Python wheel smoke requires maturin and provide the exact supported command.
- [x] Add a documentation test or command transcript check where the repository already has a suitable pattern.

### Task 4: Verify as an external consumer

**Files:**
- No additional source files unless verification exposes a tested defect.

- [x] Run the Rust quickstart.
- [x] Run CLI create/insert/reopen/inspect/schema/query-scan/search/lookup/group-by/explain workflows.
- [x] Run `cargo test --workspace --no-default-features`, clippy, fmt, and docs.
- [x] Run the Python wheel smoke if maturin is available; otherwise record the exact environment blocker without claiming it passed.

Verification note: on Windows, the workspace test, clippy, and documentation gates were run
through the installed Visual Studio Developer Command Prompt with the MSVC and Windows SDK
include paths initialized. A plain PowerShell invocation is not valid evidence because it cannot
compile the native `alloca`/zstd dependencies without those paths. The corrected runs passed;
`maturin` was not installed, so the wheel smoke remains explicitly unavailable.

### Task 5: Finish and publish

- [ ] Inspect the exact diff and exclude `mutants.out/` and build artifacts.
- [ ] Commit the reviewed changes on the feature branch.
- [ ] Push and open a PR against `main`.
- [ ] Wait for all CI checks, address failures, and merge only after CI is green.
