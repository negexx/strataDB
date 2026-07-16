# Conventions — Strata

> The rules an agent should match before introducing new patterns. If something here is wrong or outdated, fix the doc — don't quietly diverge in code.

## Code style

- Match `rustfmt`/`clippy`. They're the source of truth for style and lint-level correctness.
- Files end with a newline. No trailing whitespace.
- One concept per module. If a file does two unrelated things, split it.

## Naming

- Files/modules: `snake_case.rs`
- Types/traits/enums: `PascalCase`
- Functions, variables, modules: `snake_case`
- Constants and statics: `SCREAMING_SNAKE_CASE`
- Crates: `strata-<module>` (kebab-case in `Cargo.toml`, becomes `strata_<module>` when used as a Rust identifier)

## Rust

- `clippy::all` + `clippy::pedantic` at warn workspace-wide (see root `Cargo.toml`'s `[workspace.lints]`) — new warnings block a clean `/verify`, they aren't follow-up TODOs.
- Safe Rust by default. `unsafe` requires a `// SAFETY:` comment stating the invariant it upholds; `unsafe_op_in_unsafe_fn = "deny"` is set workspace-wide so an `unsafe fn` can't silently skip re-stating its own preconditions.
- `unwrap()`/`expect()` are `clippy::warn`, not banned outright — fine in tests and genuinely-infallible paths (with a comment saying why it's infallible), not fine on a path that can receive untrusted input or fail at runtime.
- Prefer borrowing (`&T`/`&mut T`) over cloning; when you do clone, that's a signal to double-check whether ownership could be restructured instead.
- `#[must_use]` on any function whose return value must be checked (especially anything in `crates/txn/` returning a commit/conflict result) — Rust's own unused-`Result`-must-use lint already covers `Result`-returning functions, so this is mainly for non-`Result` return types where a caller could plausibly ignore something they shouldn't.
- Don't wrap `hnsw_rs`'s (or whatever HNSW crate is in use) serialization APIs behind an abstraction that hides them from the delta-log/commit code — see `docs/decisions/0005-rust-over-cpp-reversal.md`.

## Imports

- Order: `std` → external crates → workspace-internal crates (`strata-*`) — `rustfmt` on stable can't auto-group this (that config is nightly-only, see `rustfmt.toml`), so group manually and let `cargo fmt` handle in-group ordering.
- No unnecessary `pub use` re-exports at a crate root unless it's genuinely part of that crate's public API surface.
- No unnecessary dependencies — if `cargo clippy` or `cargo-udeps`-style tooling flags an unused import/dependency, remove it, don't suppress the lint.

## Errors

- Use a typed `enum` error (via `thiserror` or hand-rolled `impl std::error::Error`) on the hot commit/conflict path in `crates/txn/` — no `Box<dyn Error>` or stringly-typed errors on that path, callers need to match on the specific conflict variant.
- `anyhow`-style dynamic errors are fine at the CLI boundary (`crates/cli/`) where there's no caller left to match on a specific variant.
- Never swallow an error silently — propagate with `?` or log with enough context to debug later (which transaction, which keys, for anything in `crates/txn/`).
- A panic across the PyO3 FFI boundary is undefined behavior, not just an ugly traceback — see `.claude/rules/python-bindings.md`.

## Tests

- One assertion idea per test (multiple `assert!`/`assert_eq!` calls are fine if they test the same idea).
- Test names describe behavior, not implementation: `conflicting_writes_return_typed_error`, not `test_conflict_check`.
- For `crates/txn/` and `crates/index/`: every change needs a `loom`-based interleaving test alongside the normal `#[test]` happy path — see `.claude/rules/concurrency-txn-layer.md`.

## Commits

Conventional Commits:
- `feat:` new user-visible behavior
- `fix:` bug fix
- `chore:` housekeeping (deps, config)
- `refactor:` no behavior change
- `docs:` documentation only
- `test:` test-only changes

Subject ≤72 chars, imperative ("add" not "added"), no trailing period.

## Comments

Default: no comments. Names and types should carry the meaning.

Write a comment when:
- There's a non-obvious *why* (constraint, workaround, surprising invariant)
- The code does something that would look like a bug to a careful reader
- It's a `// SAFETY:` comment justifying an `unsafe` block — this one is required, not optional

Don't write a comment for:
- What the code does (the code does it)
- Who added it or why (`git blame` does that)
- Future plans ("TODO: refactor this later")
