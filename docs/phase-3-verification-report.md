# Phase 3 verification report

**Run date:** 2026-08-13
**Execution branch:** historical `codex/phase-3-vacuum`
**Verification head:** `7be77d5` (final pre-merge PR head)
**Merged branch:** `main` at `65449a9` (PR #68)

## Fresh cloud provenance

Canonical final exact-head GitHub Actions run
[31714285971](https://github.com/negexx/strataDB/actions/runs/31714285971) completed successfully
for final verification head `7be77d5`. The controller reported all
workflow jobs successful:

- `ci`;
- `phase-0-foundation-evidence`;
- `phase-3-lifecycle-evidence`;
- `fuzz-and-provenance`;
- `thorough-chaos`; and
- `windows-directory-durability`.

This is the canonical final exact-head evidence for the Task 1–6 remediation aggregate, including
manifest-publication timestamps; Task 6 exact-key protection for recovery-recognized numeric manifest
keys, with duplicate padded/unpadded aliases rejected fail-closed; constrained vacuum cleanup; lifecycle
inventory compatibility; and compaction crash/reopen coverage. It also covers the unpadded current-manifest
vacuum regression: both the protected row file and vector segment survive, and after reopening the
dataset the vector search returns physical row ID `0`. The active-snapshot compaction regression now
correctly asserts that two unprotected superseded objects are reclaimed while the historical snapshot's
row file and vector segment remain readable.

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

The local Windows host now has the x64 MSVC compiler and linker binaries, but the installed Build
Tools do not include the x64 MSVC library directory or `msvcrt.lib`. The Task 1–6 native `cargo test`
attempts, including the strengthened unpadded vacuum/vector regression, therefore still stop at
linking before test assertions run. Their targeted compile-only checks passed where recorded in the
task ledgers. This remains an environment limitation, not a local test-pass claim; the exact-head
cloud run above supplies the completed functional evidence.

## Claim boundary

Phase 3 remains Partial. Its implemented lifecycle operations apply only to one process using one
shared `Dataset` handle. Compaction preserves active snapshots, age retention protects legacy zero
timestamps, vacuum deletes only recognized unprotected objects, and `storage_bound_met` is one
maintenance run's final inventory observation. This evidence does not establish serializability,
cross-process coordination, universal power-loss durability, or atomic/continuing storage-bound
enforcement.
