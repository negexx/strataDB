# Phase 0 Foundation Audit

**Date:** 2026-08-03
**Baseline:** `d0b0a8e` (merged PR #52)
**Scope:** embedded, single-node, local-disk foundation used by one process and one shared
`Dataset` handle.
**Status:** Read-only audit; implementation follow-up is tracked by this branch.
**Verdict:** Phase 0 is **Partial**, with no new P0 implementation defect found in this review.

The local evidence below was collected from this branch's working tree, based on the merged PR #52
baseline and including the regression test and documentation/CI changes described here. It is not
evidence that existed at commit `d0b0a8e` itself.

This audit applies the same evidence-first review to the foundation that the Phase 1 audit applied
to correctness and durability. It separates the mechanisms Phase 0 establishes from guarantees and
operational work owned by Phase 1 or later phases.

## Phase 0 contract

Phase 0 establishes:

- Arrow IPC row-file encoding and decoding;
- JSON version manifests and version discovery;
- local filesystem object operations through `LocalFs`;
- physical row-ID allocation and durable high-water recovery;
- basic transaction preparation, commit ordering, and immutable snapshots; and
- immutable HNSW segment construction, serialization, loading, and fan-out search.

Phase 0 does not establish a complete query/client API, compaction, bounded history or segment
growth, cross-process coordination, serializability, object storage, or universal power-loss
durability. Those boundaries belong to Phase 1 or later roadmap phases and must not be pulled into
this audit as closure requirements.

## Fresh local evidence

The targeted foundation suites on the merged PR #52 baseline are:

| Area | Command | Result |
|---|---|---|
| Storage format/backend/manifest | `cargo test -p strata-storage --lib` | 94 passed, 0 failed |
| Transactions/allocator/snapshots | `cargo test -p strata-txn --lib` | 151 passed, 0 failed |
| HNSW/immutable segments | `cargo test -p strata-index --lib` | 147 passed, 0 failed, 1 intentionally ignored |

The tests cover round-trip encoding, malformed input rejection, manifest envelope checksums and
filename/version identity, local-key containment, directory-sync failure handling, row-ID high-water
records, restart non-reuse, snapshot visibility, transaction conflict behavior, segment CRCs,
topology validation, global row-ID mapping, and fan-out search.

These are local Windows results. They are not a substitute for retained CI provenance or a portable
filesystem matrix.

## Finding register

| ID | Severity | Area | Current disposition | Required closure/evidence |
|---|---|---|---|---|
| F0-01 | P0 | Restart-safe physical row-ID allocation | Implemented and locally regression-covered. The durable high-water record is created with the dataset and prevents abandoned pre-publication claims from being reused after reopen. | Retain the restart/non-reuse regression in CI and preserve the named local-filesystem/platform boundary. This is the primary Phase 0 exit assertion. |
| F0-02 | P1 | On-disk format compatibility | Manifest and segment formats have explicit version checks; row files use Arrow IPC directly and have no Strata-owned version discriminator. Dataset recovery adds schema, physical-column, length, and CRC validation. Legacy or unsupported manifest/segment state is rejected rather than silently relabeled; manifest envelope, manifest, and entry structures reject unknown fields. There is no general migration framework. | Document the supported format versions and rejection behavior for each artifact, including the delegated Arrow IPC boundary. Migration/evolution remains later work unless a new compatibility requirement is approved. |
| F0-03 | P1 | Manifest and file publication | The manifest is the intended visibility boundary; preparation writes immutable row files/segments before publication. Directory synchronization and acknowledgement semantics are already tracked by the Phase 1 audit. | Do not broaden Phase 0 into universal durability. Close the remaining Phase 1 CI and platform evidence gates before claiming the publication contract is proven. |
| F0-04 | P1 | Transaction primitive scope | Shared-handle commit locking, write-write OCC, immutable snapshots, and typed conflict errors exist and are regression-covered. The API intentionally has no read-your-own-writes or serializable read/write transaction interface. | Keep the supported boundary explicit. Full snapshot transactions and stronger isolation require a new design decision and are not Phase 0 work. |
| F0-05 | P1 | Preparation failures and unreachable artifacts | A failed commit can leave an unreferenced row file or segment because publication is manifest-based and cleanup is not implemented. This does not make the artifact visible to a later snapshot. | Track reclamation, orphan cleanup, compaction, and bounded growth in Phase 3. Add no implicit cleanup to Phase 0. |
| F0-06 | P1 | Immutable vector-segment foundation | The segmented design is coherent: each vector-bearing commit produces an immutable segment, manifests list segments, readers validate them, and search maps local ordinals back to global physical row IDs. | Preserve segment format tests and current segmented benchmarks. Recall/latency bounds, segment-count limits, and compaction are separate Phase 1 evidence and Phase 3 lifecycle work. |
| F0-07 | P1 | Foundation verification visibility | Targeted storage, transaction, and index suites pass locally. CI execution and retained artifacts are still pending; portable/native evidence is also incomplete. | Require CI-retained targeted regressions and keep platform/fixture limitations explicit. Do not mark the phase complete from local output alone. |
| F0-08 | P2 | Backend abstraction completeness | `Backend`/`LocalFs` provide the current local implementation, but object-store semantics and independent opener coordination are not implemented. | Keep object storage in Phase 6 and cross-process coordination in Phase 4. The Phase 0 contract should say local backend, not generic remote durability. |

## Audit conclusions

1. The Phase 0 mechanisms exist and have strong direct local test coverage.
2. The main Phase 0 exit assertion—restart-safe non-reuse of physical row IDs—is implemented and
   locally verified, but its evidence must remain visible in CI before the phase is formally closed.
3. No monolithic index path is required or recommended. The foundation is the current immutable,
   manifest-listed segmented design.
4. The largest remaining limitations are not missing Phase 0 mechanisms: they are Phase 1 evidence
   provenance/portability and later lifecycle, query, cross-process, and deployment work.
5. Phase 0 should be marked **Foundation implemented within the named local bounds** only after the
   pending CI checks pass and the canonical roadmap/status documents record this audit. Until then,
   the conservative status remains **Partial**.

## Recommended next action

Run the retained Phase 0 foundation evidence checks for this branch/PR, then update the
roadmap/status wording after the artifact is retained to distinguish:

- Phase 0 foundation: locally implemented and regression-covered;
- Phase 1 correctness/durability: still blocked by retained CI, portability, and performance
  evidence; and
- later phases: lifecycle, query/client, cross-process, branching, and object storage.
