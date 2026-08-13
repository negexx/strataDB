# Strata documentation

This is the short reading path for humans and AI agents. Current source and tests are authoritative;
these documents explain the implementation boundary and the work still required.

1. [Architecture](architecture.md) — what Strata is, how commits/readers/indexes work, and what is not supported.
2. [Status](status.md) — evidence-backed capability ledger and known gaps.
3. [Roadmap](roadmap.md) — current phases, future work, deferred scope, and exit criteria.
4. [Decisions](decisions.md) — the active architectural decisions in one place.
5. [Current design](design.md) — storage, transaction, segment, query, and verification contracts.
6. [Phase 0 audit](audit/phase-0/audit.md) — foundation scope and boundaries.
7. [Phase 1 audit](audit/phase-1/audit.md) — consolidated correctness, durability, and evidence review.
8. [Phase 2 audit](audit/phase-2/audit.md) — query and client-surface contract.
9. [Phase 3 verification](phase-3-verification-report.md) — current lifecycle implementation evidence.
10. [Agent guidance](../AGENTS.md) — repository workflow, invariants, and verification expectations.

## History

[Documentation history](history/README.md) contains compact summaries of superseded decisions, designs,
audits, and engineering notes. It preserves rationale but is non-authoritative. Do not begin there
unless you need historical context.
