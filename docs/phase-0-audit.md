# Phase 0 Foundation Audit

**Date:** 2026-08-03
**Baseline:** `eb48519` (merged PR #53)
**Scope:** embedded, single-node, local-disk foundation used by one process and one shared
`Dataset` handle.
**Status:** Evidence complete within the named local-filesystem bounds; later-phase limitations
remain tracked separately.
**Verdict:** Phase 0 is **Foundation implemented within named local bounds**.

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

The targeted foundation suites on the merged PR #53 baseline are:

| Area | Command | Result |
|---|---|---|
| Storage format/backend/manifest | `cargo test -p strata-storage --lib` | 94 passed, 0 failed |
| Transactions/allocator/snapshots | `cargo test -p strata-txn --lib` | 151 passed, 0 failed |
| HNSW/immutable segments | `cargo test -p strata-index --lib` | 147 passed, 0 failed, 1 intentionally ignored |

The tests cover round-trip encoding, malformed input rejection, manifest envelope checksums and
filename/version identity, local-key containment, directory-sync failure handling, row-ID ownership
and uniqueness through high-water records and restart non-reuse, snapshot visibility, transaction
conflict behavior, segment CRCs, topology validation, global row-ID mapping, and fan-out search.
They establish manifest-listed metadata/checksum consistency, not byte-for-byte identity of decoded
Arrow vector values: tampering that changes vector values while recomputing the corresponding
metadata/checksums is outside the supported integrity boundary.

These are local Windows results. The merged PR #53 CI run also passed the retained foundation
evidence job on Ubuntu: [run 30841989478](https://github.com/negexx/strataDB/actions/runs/30841989478),
with retained artifact `phase-0-foundation-evidence-30841989478-attempt-1`. This is not a portable
filesystem matrix or a universal power-loss guarantee.

## Finding register

| ID | Severity | Area | Current disposition | Required closure/evidence |
|---|---|---|---|---|
| F0-01 | P0 | Restart-safe physical row-ID allocation | Implemented, locally regression-covered, and retained in the merged PR #53 CI artifact. The durable high-water record is created with the dataset and prevents abandoned pre-publication claims from being reused after reopen. | Preserve the named local-filesystem/platform boundary. This Phase 0 exit assertion is satisfied. |
| F0-02 | P1 | On-disk format compatibility | Manifest and segment formats have explicit version checks; row files use Arrow IPC directly and have no Strata-owned version discriminator. Dataset recovery adds schema, physical-column, length, and CRC validation. Legacy or unsupported manifest/segment state is rejected rather than silently relabeled; manifest envelope, manifest, and entry structures reject unknown fields. There is no general migration framework. | Document the supported format versions and rejection behavior for each artifact, including the delegated Arrow IPC boundary. Migration/evolution remains later work unless a new compatibility requirement is approved. |
| F0-03 | P1 | Manifest and file publication | The manifest is the intended visibility boundary; preparation writes immutable row files/segments before publication. Directory synchronization and acknowledgement semantics are already tracked by the Phase 1 audit. | Do not broaden Phase 0 into universal durability. Close the remaining Phase 1 CI and platform evidence gates before claiming the publication contract is proven. |
| F0-04 | P1 | Transaction primitive scope | Shared-handle commit locking, write-write OCC, immutable snapshots, and typed conflict errors exist and are regression-covered. The API intentionally has no read-your-own-writes or serializable read/write transaction interface. | Keep the supported boundary explicit. Full snapshot transactions and stronger isolation require a new design decision and are not Phase 0 work. |
| F0-05 | P1 | Preparation failures and unreachable artifacts | A failed commit can leave an unreferenced row file or segment because publication is manifest-based and cleanup is not implemented. This does not make the artifact visible to a later snapshot. | Track reclamation, orphan cleanup, compaction, and bounded growth in Phase 3. Add no implicit cleanup to Phase 0. |
| F0-06 | P1 | Immutable vector-segment foundation | The segmented design is coherent: each vector-bearing commit produces an immutable segment, manifests list segments, readers validate them, and search maps local ordinals back to global physical row IDs. | Preserve segment format tests and current segmented benchmarks. Recall/latency bounds, segment-count limits, and compaction are separate Phase 1 evidence and Phase 3 lifecycle work. |
| F0-07 | P1 | Foundation verification visibility | Targeted storage, transaction, and index suites pass locally, and the merged PR #53 CI run retained the targeted evidence artifact. Portable/native evidence remains incomplete. | Keep platform/fixture limitations explicit. Phase 0 is closed only within the named local bounds, not as a universal portability claim. |
| F0-08 | P2 | Backend abstraction completeness | `Backend`/`LocalFs` provide the current local implementation, but object-store semantics and independent opener coordination are not implemented. | Keep object storage in Phase 6 and cross-process coordination in Phase 4. The Phase 0 contract should say local backend, not generic remote durability. |

## Audit conclusions

1. The Phase 0 mechanisms exist and have strong direct local test coverage.
2. The main Phase 0 exit assertion—restart-safe non-reuse of physical row IDs—is implemented,
   locally verified, and retained in CI.
3. No monolithic index path is required or recommended. The foundation is the current immutable,
   manifest-listed segmented design.
4. The largest remaining limitations are not missing Phase 0 mechanisms: they are Phase 1 evidence
   provenance/portability and later lifecycle, query, cross-process, and deployment work.
5. Phase 0 is **Foundation implemented within the named local bounds**. Its local filesystem and
   single-process/shared-handle limits remain part of the contract.

## Recommended next action

Proceed to the remaining Phase 1 correctness, durability, portability, and performance evidence
blockers while preserving the distinction between:

- Phase 0 foundation: locally implemented and regression-covered;
- Phase 1 correctness/durability: still blocked by retained CI, portability, and performance
  evidence; and
- later phases: lifecycle, query/client, cross-process, branching, and object storage.
