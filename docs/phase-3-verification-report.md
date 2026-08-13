# Phase 3 verification report

**Run date:** 2026-08-13
**Branch:** `codex/phase-3-vacuum`
**Remediation head:** `12478ebace5dae1bd527b3450e6a841a6ce5a772`

## Fresh cloud provenance

Canonical final exact-head GitHub Actions run
[31705066942](https://github.com/negexx/strataDB/actions/runs/31705066942) completed successfully
for final remediation head `12478ebace5dae1bd527b3450e6a841a6ce5a772`. The controller reported all
workflow jobs successful:

- `ci`;
- `phase-0-foundation-evidence`;
- `phase-3-lifecycle-evidence`;
- `fuzz-and-provenance`;
- `thorough-chaos`; and
- `windows-directory-durability`.

This is the canonical final exact-head evidence for the Task 1–4 remediation aggregate, including manifest-publication
timestamps, recovery-recognized numeric manifest authority, constrained vacuum cleanup, lifecycle
inventory compatibility, and compaction crash/reopen coverage.

## Prior cloud provenance

The earlier exact-head GitHub Actions run
[31701969422](https://github.com/negexx/strataDB/actions/runs/31701969422) completed successfully
for remediation head `105c24f74dc68d8c4c552bfa9d4b63b1a4c79a2c`, with the same controller job set
listed above. It remains retained provenance for the earlier Task 1–4 remediation aggregate.

The still earlier exact-head GitHub Actions run
[31698710652](https://github.com/negexx/strataDB/actions/runs/31698710652) completed successfully
for remediation head `0141355d1261ee20d8128cbc11c38b23e83045c9`, with the same controller job set
listed above. It remains retained provenance for the earlier Task 1–4 remediation aggregate.

## Local limitation

The local Windows host has no MSVC `link.exe`; the Task 1–4 native `cargo test` attempts stopped at
linking before test assertions ran. Their targeted compile-only checks passed where recorded in the
task ledgers. This is an environment limitation, not a local test-pass claim; the exact-head cloud
run above supplies the completed functional evidence.

## Claim boundary

Phase 3 remains Partial. Its implemented lifecycle operations apply only to one process using one
shared `Dataset` handle. Compaction preserves active snapshots, age retention protects legacy zero
timestamps, vacuum deletes only recognized unprotected objects, and `storage_bound_met` is one
maintenance run's final inventory observation. This evidence does not establish serializability,
cross-process coordination, universal power-loss durability, or atomic/continuing storage-bound
enforcement.
