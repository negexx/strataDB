# ADR 0004 — Toolchain audit (2026-07)

**Status:** Accepted — amends/supersedes toolchain specifics in ADR 0002 where noted below
**Date:** 2026-07-15

## Context

The original toolchain (ADR 0002 and the initial `.claude/` scaffold) was resolved from a single research pass. The project owner requested a second, source-driven audit of every Stack row, providing specific external sources for build systems, package managers, and testing frameworks, and asked whether C++26 should replace C++23. This ADR records the outcome: what changed, what was confirmed, and why.

## Decision

**Changed:**

1. **Language: stay on C++23, do not move to C++26.** C++26 was ratified March 2026, but MSVC has zero public C++26 support with no ETA (historically 12-24 months from first experimental support to stable, per the concepts/modules/coroutines precedent), and none of Arrow C++, vcpkg, GoogleTest, usearch, or nanobind have published C++26 verification. Industry consensus (Citadel, Spotify, multiple 2026 sources) is 1-2 years from production-readiness. For a solo 2.2-year roadmap, that risk (debugging compiler/library incompatibilities alone, no vendor precedent to lean on) isn't worth it. If the driver is concurrency primitives, `std::execution` (senders/receivers) is already available experimentally under C++23 in GCC 14+/Clang 18+.
2. **HNSW library: usearch instead of hnswlib.** Neither library provides native delta-log atomicity — that shim is always Strata's own code — but usearch's stream-based serialization API (`save_to_stream`/`load_from_stream` with callbacks) and finer-grained per-node locking are a meaningfully better foundation for it than hnswlib's more rigid binary format and coarser locking. This directly serves the project's flagship requirement (atomic row+index commits), so it's worth the tradeoff of usearch being less battle-tested at scale than hnswlib/Faiss — watch for this specifically in Phase 4's benchmarks.
3. **Linter: add Cppcheck alongside clang-tidy.** Cppcheck catches undefined-behavior and memory-error classes clang-tidy doesn't consistently flag, is free, and this project has two high-correctness domains (the transaction layer, the vector index) where that matters. Both tools must be clean, not just one.
4. **Concurrency-correctness tooling: layer in before the full DST harness, not instead of it.** Add Relacy (exhaustive interleaving testing of locks/atomics/CAS loops in isolation), libFuzzer/FuzzTest (property-based fuzzing of the transaction API), and `rr` (record-replay debugging once a race reproduces) as incremental steps ahead of Phase 7's full bespoke deterministic-simulation harness. These are lower-cost and catch a real subset of what the full harness would eventually catch, reducing solo-developer risk earlier in the roadmap. OpenThesis (an open-source Antithesis-style deterministic hypervisor) was noted as a possible foundation for the eventual full harness rather than building the hypervisor layer from bare metal — worth evaluating in Phase 0/7, not decided here.

**Confirmed unchanged, with sources:**

- **Build system — CMake + Ninja.** Cross-checked against a build-systems comparison (Bazel/Meson/CMake breakdown): "For modern C++ libraries: CMake (with Ninja) + Conan/vcpkg. For large polyglot codebases: Bazel or Buck2." Strata is a single-language, solo-dev embedded library, not a polyglot monorepo — CMake+Ninja is the correct bracket, not Bazel's. Separately checked Meson specifically (a generic Meson-vs-CMake comparison prompted a second look): Meson is genuinely faster to configure and simpler syntax, but it doesn't fit *this* dependency stack — nanobind's own docs (Feb 2026) state CMake is the only officially supported build system (Meson works only via an unofficial third-party WrapDB package), and Arrow C++ documents CMake `find_package` as the primary consumption path, pkg-config as the fallback for non-CMake systems. Trading official nanobind support for an unofficial wrapper isn't worth Meson's speed advantage here. Not changed.
- **Package manager — vcpkg.** Cross-checked against a C++ package-manager roundup; no clear winner was declared over Conan ("the best way to choose is to try a few and see which you like"), but vcpkg and Conan were the two top picks, and vcpkg's specific fit here (ready ports for every dependency this project needs, no Python runtime dependency, native CMake toolchain-file integration) still holds. Not changed.
- **Test framework — GoogleTest.** Re-confirmed: GoogleTest is positioned for "complex testing scenarios" on "large and complex projects" (matches this project), while Catch2/doctest are lighter-weight tools better suited to simpler projects. death-tests, parameterized tests, and gmock remain relevant for a correctness-critical transaction engine.
- **Formatter — clang-format.** No real competitor exists at feature parity in 2026 sources checked; not changed.
- **Columnar library — Apache Arrow C++.** Confirmed still the right 2026 default; its `MemoryPool`/`Buffer` abstractions and the Arrow C Data Interface give enough control for the project's atomicity needs without forking or going fully custom.
- **Python bindings — nanobind.** Re-confirmed strongly: still faster to compile, smaller binaries, lower call overhead than pybind11, with clean GIL-release semantics for the transaction API's blocking-I/O paths. No 2026 development (including free-threaded Python) changes this.

## Alternatives considered

See the "Changed" and "Confirmed" sections above for the specific alternative compared against each row (Bazel/Meson for build system; Conan for package manager; Catch2/doctest for test framework; hnswlib/Faiss/custom for the index; Arrow-from-scratch/DuckDB-as-library for columnar; pybind11/Cython/cppyy for bindings; C++26 for language).

## Consequences

- Positive: the index library now has a real, sourced reason to expect the delta-log shim to be buildable, rather than an assumption; linting and early concurrency testing catch more before the expensive Phase 7 harness is even built.
- Negative: usearch is a newer, less battle-tested dependency than hnswlib — if it turns out to have correctness or performance gaps at scale, revisiting this (back to hnswlib, or a custom index) costs real rework in `src/index/`. This is the main risk this ADR accepts.
- Neutral: this ADR doesn't reduce Phase 7's ultimate scope (the bespoke DST harness is still needed) — it only adds cheaper intermediate rungs on the way there.

## How to revisit

If usearch proves unstable or under-documented in practice during Phase 4, write a new ADR reopening the HNSW library choice — don't silently swap back. If MSVC ships stable C++26 support and the dependency chain (Arrow, vcpkg, GoogleTest, usearch, nanobind) is verified compatible, C++26 becomes worth reopening — likely not before 2028 per the researched timeline.
