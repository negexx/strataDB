# Audit 5 Architecture and Documentation Remediation

Status: approved implementation slice

## Contract

The active documentation must describe the implementation that exists at the
current merged head: one process with shared `Dataset` handles, immutable
snapshot reads, write-write OCC, durable local manifest publication, and
typed indeterminate-publication errors. It must not imply serializability,
cross-process coordination, universal power-loss guarantees, or a stable
surface for internal crates.

## Scope

- reconcile active architecture, status, roadmap, decision, and readiness
  records with the current implementation and exact-head CI evidence;
- document retry/reopen behavior and lifecycle stop-the-world/resource bounds;
- repair active source links and comments that refer to retired paths or
  superseded phases;
- add the accepted follow-on decision for the stable Python/CLI/query/schema
  and lifecycle surfaces, including compatibility limits;
- add method-level public facade/CLI guidance where the supported surface is
  otherwise ambiguous;
- preserve historical records under `docs/history/` unchanged.

## Verification

Run `git diff --check`, `cargo fmt --check`, `cargo doc --workspace --no-deps`,
and the relevant documentation tests. Validate that no active file points to
the retired `docs/phase-1-audit.md` path or claims that implemented lifecycle,
compaction, or conflict behavior is future work.
