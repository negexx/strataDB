# ADR 0005 — Reversal: Rust, not C++

**Status:** Accepted — supersedes ADR 0002 (C++ as the implementation language) and ADR 0004 (Toolchain audit 2026-07) in full. Both are left unedited as the historical record of the C++ path; this ADR is the current source of truth for the language and toolchain.

**Date:** 2026-07-16

## Context

ADR 0002 explicitly named Rust as the original research recommendation and picked C++ instead "by explicit project-owner choice, not a technical deficiency," while flagging the cost plainly: *"No reusable deterministic-simulation-testing library exists for C++... this is the single largest scope cost of choosing C++ over Rust for this specific project."*

The C++ path was then built out for real: a full toolchain (CMake+Ninja, vcpkg, GoogleTest, clang-tidy+Cppcheck, Arrow C++, usearch, nanobind), a `.claude/` workspace fully written for it, and a `vcpkg install` in progress pulling in ~99 packages (Boost, OpenSSL, Thrift, etc.) to build Arrow C++ from source. ADR 0004's entire "layer in Relacy/FuzzTest/rr before the full harness" plan existed specifically to soften the exact gap ADR 0002 had already flagged — cheaper workarounds for the missing `loom`/`madsim` equivalent, not a real substitute for it.

At this point — zero real engine logic written, only toolchain scaffolding — the project owner asked whether it was too late to switch back to Rust. It wasn't: nothing under `src/` was more than a placeholder proving the toolchain linked, and the Rust research from ADR 0002's original pass was already done. The decision was made to reverse.

## Decision

Switch the implementation language from C++23 back to Rust (edition 2024). Adopt:

- **Toolchain:** Cargo workspace (`crates/*`), `rustfmt`, `clippy` (`clippy::all` + `clippy::pedantic` at warn, workspace-wide)
- **Columnar:** `arrow` (arrow-rs)
- **HNSW:** `hnsw_rs` — a pure-Rust crate, chosen over `usearch`'s Rust bindings specifically to avoid re-introducing a C++ core via FFI, which would have undercut the entire point of switching (see Alternatives below)
- **Python bindings:** `pyo3` + `maturin`, using the modern `#[pymodule] mod { #[pymodule_export] ... }` API form
- **Concurrency correctness:** `loom` (exhaustive interleaving testing) + `madsim`/`turmoil` (deterministic simulation testing) for Phase 7 — real, maintained, off-the-shelf crates

Every crate/API choice was verified against the actual installed source or upstream docs before being committed to code — this surfaced two wrong guesses immediately (see Consequences).

## Alternatives considered

- **Stay on C++, accept the DST gap:** rejected. The whole reason C++ was ever a live option was project-owner preference, not a technical advantage — and the toolchain build-out made the cost of that preference concrete (a 99-package vcpkg install still building Boost/OpenSSL/Thrift minutes in, versus a Rust workspace that built, tested, linted, and formatted cleanly in under 20 seconds once the API guesses were fixed).
- **`usearch`'s Rust bindings instead of `hnsw_rs`:** rejected. `usearch` wraps a C++ core — using it from Rust would silently reintroduce the exact toolchain (a C++ compiler, C++ build tooling) the language switch was meant to eliminate, for a marginal maturity gain over `hnsw_rs`. Neither crate exposes graph internals for a native delta log regardless, so the atomic-commit requirement costs the same custom transaction-shim work either way — that wasn't a deciding factor.
- **`cargo-nextest` as the default test runner:** not adopted yet — it isn't installed on this machine and `cargo test` already works correctly; nextest remains a documented optional upgrade, not a blocker.

## Consequences

- Positive: `loom`/`madsim`/`turmoil` are real, reusable, off-the-shelf DST tooling — Phase 7 no longer requires building a bespoke FoundationDB/TigerBeetle-VOPR-style simulator from zero, which was flagged as the single largest scope risk under the C++ plan.
- Positive: the toolchain itself is simpler to bootstrap — no vcpkg, no CMake presets, no separate ASan/UBSan-vs-TSan build matrix. `cargo fmt`'s hook doesn't need the git-repo/empty-file-list guarding that C++'s `clang-format` hook needed, because `cargo fmt` operates on the Cargo manifest, not a file list built from `git ls-files`.
- Negative: real, non-trivial rework was required — the entire `.claude/` workspace (`CLAUDE.md`, `settings.json`, `commands/`, `agents/`, `rules/`, `docs/`) had to be rewritten for the new stack, and the in-progress C++ vcpkg build was discarded (harmless — it consumed background compute and disk, nothing else).
- Negative/learning: two API guesses were wrong on the first real build — `hnsw_rs::{Hnsw, DistL2}` are not re-exported at the crate root (the correct path is `hnsw_rs::prelude::{Hnsw, DistL2}`, found only by reading the installed source, not the crate's own README example verbatim) — reinforcing that even a verified-to-exist crate's *usage examples* can be stale or imprecise; always confirm against the actual installed source when a build is about to depend on the answer.
- Neutral: `usearch`, `hnswlib`, and Apache Arrow C++ remain referenced in ADR 0002/0004 as historical context for why C++ was briefly chosen — that record is intentionally preserved, not deleted.

## How to revisit

This is the second reversal on this axis (Rust → C++ → Rust). If a third reversal is ever seriously considered, that itself is a signal to stop and have an explicit conversation about decision-making process before writing any more code under either stack — not just to author ADR 0006.
