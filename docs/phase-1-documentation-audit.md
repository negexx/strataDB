# Phase 1 documentation audit

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| DOC-01 | P2 | `AGENTS.md` and several historical implementation plans say Rust 1.90 while `rust-toolchain.toml`, CI, and the active host use 1.97.1. | `AGENTS.md:66`; `docs/phase-1-implementation-plan.md`; `rust-toolchain.toml`; `.github/workflows/ci.yml`. | Update current guidance/plans to 1.97.1, preserving historical notes where intentional. |
| DOC-02 | P2 | Current status/audit language points to older branch/revision baselines. | `docs/status.md` names `codex/phase-0-audit`; `docs/phase-1-audit.md` names commit `21811031`; current head is `224ea42`. | Add exact-head provenance after CI; avoid rewriting historical evidence. |
| DOC-03 | P2 | The requested lane reports and evidence matrix/final verification report were absent before this audit refresh. | Only consolidated `phase-1-audit.md` and `phase-1-performance.md` existed among the requested lane outputs. | Added the lane reports and companion plan/report on this branch. |
| DOC-04 | P3 | `cargo doc` reports a redundant explicit intra-doc link and a link to a private item. | Fresh `cargo doc --workspace --no-deps` output. | Fix in a documentation-only task. |
| DOC-05 | P3 | Status uses “Phase 2 implemented within named bounds” while architecture calls Python/CLI surfaces “partial”. | `docs/status.md` vs `docs/architecture.md:55`. | Clarify that both refer to different contract dimensions. |

## Positive findings

The governing docs consistently preserve the one-process/shared-handle boundary, reject universal
durability/performance claims, distinguish intended guarantees from current evidence, and keep
compaction out of Phase 1.
