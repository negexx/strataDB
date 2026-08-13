# Phase 2 audit implementation plan

This is a follow-up plan for the findings recorded by the 2026-08-13 audit. It does not reopen
the completed Phase 2 implementation or Phase 4-reserved decisions.

1. Run `cargo bench -p strata-bench --bench projected_read_bench` in cloud CI and record the
   deterministic narrow-read comparison; do not convert it into a product SLO without approval.
2. Review the remaining storage/index workspace visibility before any future crate publication;
   the supported `strata-query` implementation modules are now private.
3. Run the packaged-wheel smoke job in cloud CI and record the artifact/provenance.
4. Consolidate Phase 2 evidence recipes.

Do not implement cross-process coordination, serializability, compaction, schema migration, or
additional ANN families as part of this plan.
