# Phase 1 architecture and API audit

**Date:** 2026-08-01

**Lane:** Sol — crate layering, Rust API semantics, schema ownership, client surfaces, documentation alignment, and extensibility

**Scope:** Current working tree; supported concurrency is one process using clones of one shared `Dataset` handle. The tree was already heavily dirty across the audited source and documentation. This lane changes no Rust source, tests, dependencies, or configuration and writes only this report.

**Method:** Read-only trace from workspace manifests and active architecture/status/roadmap/ADRs through `Dataset`, `Transaction`, `Snapshot`, storage/query/index boundaries, the CLI, and the PyO3 module. No long suite was run; the findings are API- and control-flow proofs from the cited working-tree lines.

## Verdict

**BLOCKED — Phase 1 should not exit on the current architecture/API contract.**

The implementation has a sound high-level publication shape and the active status/architecture documents are substantially more honest than the historical material. However, three public-contract defects are Phase 1 blockers:

1. there is no dataset-owned schema, commits accept mutually incompatible batches, and scans match visible columns by position rather than identity;
2. `delete`/`update` accept arbitrary physical row IDs and `update` accepts any replacement cardinality, so the public operations do not enforce the identity semantics their names imply; and
3. an internal “insufficient history” outcome is exposed as a genuine `TxnError::Conflict` that falsely says every write-set row was modified.

The governing decision set is also internally inconsistent: ADR 0003 still says every transaction receives a row/index point-in-time view, while the active index and architecture correctly say the implemented API is write-only transactions plus separate immutable snapshots. That wording must be amended or superseded before Phase 1 claims a coherent evidence boundary.

Crate dependencies themselves form a sensible acyclic direction, but the visibility/package surface does not enforce that architecture. Low-level manifest mutation, file I/O, conflict-log machinery, benchmark-only graph modules, fixture helpers, and representation types are publicly reachable, while `strata-txn`'s signatures expose types from `strata-query`, `strata-storage`, and `strata-index`. This is reparable before Phase 2 stabilizes the Rust/client API; it should not be allowed to fossilize accidentally.

## Layering map

| Layer | Current dependency direction | Audit assessment |
|---|---|---|
| Storage | Arrow/serde only | Appropriate bottom layer, but its public surface includes raw durable writes and manifest publication. |
| Index | Arrow plus index implementation dependencies | Independent of transaction/storage types; opaque `Any` pruning metadata avoids a cycle but weakens type safety. |
| Query | Arrow + storage | Acyclic, but logical predicates depend on storage's serialized `Value` vocabulary. |
| Transaction | storage + query + index | Correct orchestration location for row/index atomicity; its public signatures leak all three subordinate crates. |
| CLI | transaction + storage + query + index | Correct top-layer placement, but tightly coupled to the MVP fixture and low-level types. |
| Bindings | PyO3 only | Intentionally isolated placeholder; no database API exists yet. |

Evidence: workspace membership is defined at [`Cargo.toml:1-11`](../../../Cargo.toml#L1-L11); transaction edges are explicit at [`crates/txn/Cargo.toml:26-33`](../../../crates/txn/Cargo.toml#L26-L33), query's storage edge at [`crates/query/Cargo.toml:10-18`](../../../crates/query/Cargo.toml#L10-L18), and CLI's four direct workspace edges at [`crates/cli/Cargo.toml:14-19`](../../../crates/cli/Cargo.toml#L14-L19). The active component responsibilities agree with that intended direction ([`docs/architecture.md:18-27`](../../architecture.md#L18-L27)).

## Findings

### ARCH-01 — Schema-less commits can acknowledge data that no public scan schema can read, and positional casting can silently relabel columns

- **Severity:** Critical
- **Confidence:** High
- **Affected phase:** Phase 1 schema/error behavior; blocks Phase 2 stable schema/query APIs
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - `Dataset::create` accepts only a path, not a logical schema ([`crates/txn/src/dataset.rs:306-342`](../../../crates/txn/src/dataset.rs#L306-L342)).
  - `Manifest` stores files, IDs, tombstones, timestamps, and segments but no dataset schema or schema version ([`crates/storage/src/manifest.rs:100-166`](../../../crates/storage/src/manifest.rs#L100-L166)).
  - `Transaction::insert` unconditionally buffers any Arrow `RecordBatch` ([`crates/txn/src/dataset.rs:741-746`](../../../crates/txn/src/dataset.rs#L741-L746)). Commit-time logical-schema validation checks only reserved hidden names before independently encoding and writing every pending batch ([`crates/txn/src/dataset.rs:1191-1211`](../../../crates/txn/src/dataset.rs#L1191-L1211), [`1453-1512`](../../../crates/txn/src/dataset.rs#L1453-L1512)); vector shape, dimensions, and finite values have separate checks.
  - Existing tests explicitly rely on batches in one transaction having different column sets and different types for the same name ([`crates/txn/src/dataset.rs:3980-4015`](../../../crates/txn/src/dataset.rs#L3980-L4015), [`4033-4085`](../../../crates/txn/src/dataset.rs#L4033-L4085)).
  - `Snapshot::scan` requires the caller to supply a schema for all manifest-listed files ([`crates/txn/src/snapshot.rs:233-249`](../../../crates/txn/src/snapshot.rs#L233-L249)). `cast_batch_to_schema` checks only the number of visible fields, consumes physical visible columns positionally, casts them to caller-requested types, then installs the caller's field names ([`crates/txn/src/dataset.rs:1962-1969`](../../../crates/txn/src/dataset.rs#L1962-L1969), [`1988-2046`](../../../crates/txn/src/dataset.rs#L1988-L2046)).
  - The status ledger says schema handling is Partial and limits the current behavior to caller-provided batch shape plus reserved-column/vector checks, while also acknowledging that no dataset-owned catalog or migration workflow exists ([`status ledger`](../../status.md#capability-ledger)). Phase 1 explicitly includes schema enforcement and Phase 2 depends on stable schema/error behavior ([`Phase 1 — Correctness and durability baseline`](../../roadmap.md#phase-1--correctness-and-durability-baseline)).

**Counterexamples:**

1. Commit one file with one visible field and another with two visible fields. The commit returns `Ok(())`, but every `scan(schema)` fails: a one-field schema fails against the two-field file and a two-field schema fails against the one-field file. The acknowledged rows have no successful whole-snapshot scan through the public API.
2. Commit a physical field `account_id: Int64`, then scan with a one-field schema `balance: Int64`. Counts and types match, so the data is returned under the false name `balance` without an error. Predicate evaluation after casting can then operate on the silently relabeled data.

Phase 1 needs an explicit schema ownership contract. The smallest sound direction is to persist a dataset logical schema (and a format/schema version), validate every inserted batch against it before any durable write, and match projected fields by identity rather than visible position. If schema evolution is deferred, reject it loudly; do not encode “arbitrary per-file schemas” as an accidental compatibility promise.

### ARCH-02 — `delete` and `update` do not validate their target, and `update` does not enforce singular replacement semantics

- **Severity:** Critical
- **Confidence:** High
- **Affected phase:** Phase 0 row-ID invariant; Phase 1 update/delete identity semantics
- **Disposition:** Phase 1 blocker; shared with correctness/index-atomicity lanes
- **Evidence:**
  - `delete(row_id)` merely appends the supplied value to pending tombstones and the write set; it returns no result and performs no existence/visibility check ([`crates/txn/src/dataset.rs:748-755`](../../../crates/txn/src/dataset.rs#L748-L755)).
  - Commit accepts each pending tombstone and inserts it into the manifest set without validating that the row existed in the transaction's base snapshot or latest snapshot ([`crates/txn/src/dataset.rs:1069-1082`](../../../crates/txn/src/dataset.rs#L1069-L1082)).
  - Insert-only transactions have an empty write set and are always considered conflict-clean, even when retained history is insufficient ([`crates/txn/src/commit_log.rs:79-100`](../../../crates/txn/src/commit_log.rs#L79-L100)). Row IDs are claimed before commit serialization ([`crates/txn/src/dataset.rs:1237-1250`](../../../crates/txn/src/dataset.rs#L1237-L1250)). Together these permit a delete of a future/in-flight ID to publish first and a later insert of that ID to return success while remaining hidden by the tombstone.
  - `update` is documented as inserting “the replacement” but accepts an arbitrary `RecordBatch` and simply calls `delete` plus `insert` ([`crates/txn/src/dataset.rs:797-807`](../../../crates/txn/src/dataset.rs#L797-L807)). Commit allocates and writes every row in all pending batches; there is no `num_rows() == 1` check ([`crates/txn/src/dataset.rs:1237-1250`](../../../crates/txn/src/dataset.rs#L1237-L1250)). A zero-row update is only a delete; an N-row update is one deletion plus N inserts.
  - Active architecture calls stable logical identity an open Phase 1 question ([`docs/architecture.md:41-43`](../../architecture.md#L41-L43)); the roadmap makes update/delete semantics part of Phase 1 ([`docs/roadmap.md:25-33`](../../roadmap.md#L25-L33)).

Define the target contract before implementation: whether the target must be visible in the begin-time snapshot; distinct typed outcomes for missing, already-deleted, and numerically future IDs; and whether `update` is exactly one replacement row. The API should return a typed error at or before commit and must close the stale-delete/future-insert conflict hole. If bulk replacement is wanted later, give it a name and semantics distinct from singular `update`.

### ARCH-03 — `InsufficientHistory` is erased into a false row-level conflict report

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 typed conflict/error behavior
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - Internally, `ConflictCheck` correctly distinguishes a real overlap from `InsufficientHistory`, where cleanliness simply cannot be proved ([`crates/txn/src/commit_log.rs:11-26`](../../../crates/txn/src/commit_log.rs#L11-L26)).
  - `Transaction::commit` maps a real overlap and insufficient history to the same public `TxnError::Conflict`; the latter copies the transaction's entire write set into `contested_row_ids` ([`crates/txn/src/dataset.rs:948-962`](../../../crates/txn/src/dataset.rs#L948-L962)).
  - The public error text asserts that those IDs “were modified by another transaction” ([`crates/txn/src/error.rs:46-49`](../../../crates/txn/src/error.rs#L46-L49)). The regression test itself notes that the reported ID was not touched by any intervening commit and is named only because history aged out ([`crates/txn/src/dataset.rs:6631-6647`](../../../crates/txn/src/dataset.rs#L6631-L6647)).
  - Repository policy requires conflict errors to identify contested row IDs ([`AGENTS.md:23`](../../../AGENTS.md#L23)); the active architecture also describes conflicts as typed errors identifying contested IDs ([`docs/architecture.md:31-35`](../../architecture.md#L31-L35)).

Expose a distinct public `InsufficientHistory`/`RetryRequired` error (with the transaction base/current versions and no fabricated contested IDs), or change the public conflict payload so it can represent “unknown overlap” without asserting a false fact. The per-handle telemetry counter is useful observability but cannot repair the semantic lie at the call boundary.

### ARCH-04 — Accepted ADR 0003 states a broader transaction guarantee than the implemented and indexed contract

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 guarantee boundary and ADR alignment
- **Disposition:** Phase 1 documentation blocker; amend or supersede the ADR, do not reinterpret its decision only in an index
- **Evidence:**
  - ADR 0003's accepted decision says “every transaction sees a consistent point-in-time view across both the row store and vector index” ([`docs/decisions/0003-snapshot-isolation-not-serializability.md:10-13`](../../decisions/0003-snapshot-isolation-not-serializability.md#L10-L13)).
  - The decision index instead says ADR 0003 sets only an intended ceiling and does not establish a full read/write transaction API ([`docs/decisions/README.md:3-11`](../../decisions/README.md#L3-L11)).
  - Active architecture accurately describes `Transaction` as write-only and snapshots as separate, and explicitly says the implemented surface is narrower than full read/write snapshot isolation ([`docs/architecture.md:29-37`](../../architecture.md#L29-L37), [`49-53`](../../architecture.md#L49-L53)). The public `Transaction` surface contains insert/delete/update/commit but no scan or search methods ([`crates/txn/src/dataset.rs:712-915`](../../../crates/txn/src/dataset.rs#L712-L915)).
  - The Phase 0 design has been marked historical where it described transactional reads ([`docs/design/phase-0-transaction-and-format-spec.md:3-9`](../../design/phase-0-transaction-and-format-spec.md#L3-L9), [`33-45`](../../design/phase-0-transaction-and-format-spec.md#L33-L45)); the accepted ADR has not received equivalent corrective wording.

An index cannot silently narrow an accepted ADR's normative decision. Record whether the policy is (a) an eventual isolation ceiling only, with the current immutable-snapshot/write-write-OCC API accepted as a narrower v1 contract, or (b) still a v1 requirement whose implementation remains incomplete. Use a superseding ADR or an explicit accepted amendment so client-facing docs have one unambiguous source.

### ARCH-05 — Public visibility and package metadata do not enforce the intended internal crate boundaries

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 invariant/API boundary; Phase 2 Rust API stabilization
- **Disposition:** Phase 1 contract decision required; close or explicitly disclaim invariant-bypassing surfaces before Phase 2
- **Evidence:**
  - `strata-storage` publicly exports its modules plus raw data/byte writers, directory sync, manifest read, and manifest commit primitives ([`crates/storage/src/lib.rs:4-18`](../../../crates/storage/src/lib.rs#L4-L18)). `commit_manifest` can publish a caller-built `Manifest` directly and instantiates `LocalFs` without transaction validation ([`crates/storage/src/manifest.rs:193-216`](../../../crates/storage/src/manifest.rs#L193-L216)).
  - `strata-txn` publicly exposes `commit_log` and MVP fixture modules in addition to its intended facade types ([`crates/txn/src/lib.rs:6-17`](../../../crates/txn/src/lib.rs#L6-L17)); `CommitLog` and `ConflictCheck` are fully public implementation machinery ([`crates/txn/src/commit_log.rs:11-27`](../../../crates/txn/src/commit_log.rs#L11-L27), [`55-88`](../../../crates/txn/src/commit_log.rs#L55-L88)).
  - `Dataset::data_files` and `Snapshot::data_files` are public while their documentation says they exist for tests ([`crates/txn/src/dataset.rs:507-513`](../../../crates/txn/src/dataset.rs#L507-L513), [`crates/txn/src/snapshot.rs:148-154`](../../../crates/txn/src/snapshot.rs#L148-L154)).
  - `strata-index` makes graph/distance modules technically public for benchmarks and calls the crate “internal, unpublished” ([`crates/index/src/lib.rs:8-35`](../../../crates/index/src/lib.rs#L8-L35)). None of the workspace crate manifests shown here sets `publish = false` ([`crates/index/Cargo.toml:1-6`](../../../crates/index/Cargo.toml#L1-L6), [`crates/storage/Cargo.toml:1-6`](../../../crates/storage/Cargo.toml#L1-L6), [`crates/txn/Cargo.toml:1-6`](../../../crates/txn/Cargo.toml#L1-L6)); the “internal-only” intent is therefore not enforced by Cargo metadata.
  - Project conventions say crate-root re-exports should exist only when genuinely part of the public API ([`docs/conventions.md:28-32`](../../conventions.md#L28-L32)).

The supported invariant should be scoped to an explicit facade, not merely inferred from which path normal examples use. Either make implementation crates/modules non-publishable and reduce visibility to workspace-private patterns, or document that direct storage/index APIs are expert/internal surfaces outside `Dataset` guarantees. Benchmark access should use a deliberate bench-support surface rather than define production visibility by accident.

### ARCH-06 — `strata-txn` is not a cohesive facade: public signatures leak subordinate crate types and couple semver across layers

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 2 Rust/Python API design
- **Disposition:** Later-phase implementation, but decide the facade before declaring any Rust API stable
- **Evidence:**
  - `Snapshot::scan_with_predicate` takes `strata_query::Predicate`; `Predicate` in turn embeds `strata_storage::Value` ([`crates/txn/src/snapshot.rs:285-304`](../../../crates/txn/src/snapshot.rs#L285-L304), [`crates/query/src/predicate.rs:14-23`](../../../crates/query/src/predicate.rs#L14-L23)).
  - `Snapshot::vector_search` returns `Vec<strata_index::VectorMatch>` ([`crates/txn/src/snapshot.rs:353-364`](../../../crates/txn/src/snapshot.rs#L353-L364)). `Dataset`/`Snapshot` metadata accessors expose `strata_storage::DataFileEntry` ([`crates/txn/src/dataset.rs:507-513`](../../../crates/txn/src/dataset.rs#L507-L513), [`crates/txn/src/snapshot.rs:148-154`](../../../crates/txn/src/snapshot.rs#L148-L154)).
  - `TxnError` publicly wraps storage and index errors ([`crates/txn/src/error.rs:16-31`](../../../crates/txn/src/error.rs#L16-L31)). The crate root re-exports Arrow but not the query/value/match types required to use its own methods ([`crates/txn/src/lib.rs:14-17`](../../../crates/txn/src/lib.rs#L14-L17)).
  - The predicate scalar vocabulary is defined in storage and currently limited to `Int64`, `Float64`, and `Utf8` ([`crates/storage/src/stats.rs:14-28`](../../../crates/storage/src/stats.rs#L14-L28)), coupling logical expression growth to serialized storage metadata.

Choose a single supported Rust facade and ownership for logical API types. Re-exporting stable facade types, introducing an API/types crate, or intentionally documenting a multi-crate SDK can all work; the current accidental mixture should not become the compatibility contract. Keep physical stats encoding behind conversion boundaries so adding query scalar types does not automatically become an on-disk compatibility change.

### ARCH-07 — The backend abstraction is not threaded through `Dataset` or data/index I/O

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 6 object storage; Phase 2 constructor/facade design
- **Disposition:** Intentionally later-phase implementation; reserve the API seam before stabilizing constructors
- **Evidence:**
  - `Backend` is a synchronous object-safe abstraction intended to separate storage logic from local disk ([`crates/storage/src/backend/mod.rs:1-5`](../../../crates/storage/src/backend/mod.rs#L1-L5), [`21-92`](../../../crates/storage/src/backend/mod.rs#L21-L92)).
  - `Dataset` owns a concrete `PathBuf`, `create` directly creates local directories, and `open` reconstructs from a path ([`crates/txn/src/dataset.rs:173-210`](../../../crates/txn/src/dataset.rs#L173-L210), [`341-390`](../../../crates/txn/src/dataset.rs#L341-L390), [`426-485`](../../../crates/txn/src/dataset.rs#L426-L485)).
  - Manifest operations construct `LocalFs` internally rather than accepting a backend ([`crates/storage/src/manifest.rs:202-215`](../../../crates/storage/src/manifest.rs#L202-L215), [`229-255`](../../../crates/storage/src/manifest.rs#L229-L255)). Data and segment paths are still written/read through raw filesystem paths ([`crates/storage/src/datafile.rs:21-59`](../../../crates/storage/src/datafile.rs#L21-L59), [`122-143`](../../../crates/storage/src/datafile.rs#L122-L143)).
  - The status ledger accurately marks object storage Proposed and local-only ([`status ledger`](../../status.md#capability-ledger)); the roadmap places it in Phase 6 after coordination/lifecycle prerequisites ([`Phase 6 — Object storage and deployment`](../../roadmap.md#phase-6--object-storage-and-deployment)).

This is not a Phase 1 implementation requirement. It is an extensibility warning: `Dataset::create/open(path)` will be a breaking constraint if declared stable before a backend/location abstraction is designed, and swapping only manifest I/O is insufficient because row and segment I/O remain path-bound.

### ARCH-08 — CLI snapshot labels can describe a different version than the rows displayed

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 2 coherent CLI; Phase 1 client-facing guarantee wording
- **Disposition:** Fix with the Phase 2 client surface; add a public `Snapshot::version()` accessor or return versioned read results
- **Evidence:**
  - A `Snapshot` stores its captured version privately (`pub(crate)`) and exposes no version accessor in its public read surface ([`crates/txn/src/snapshot.rs:51-63`](../../../crates/txn/src/snapshot.rs#L51-L63), [`124-396`](../../../crates/txn/src/snapshot.rs#L124-L396)).
  - `Dataset::current_version()` obtains a fresh snapshot at call time ([`crates/txn/src/dataset.rs:488-500`](../../../crates/txn/src/dataset.rs#L488-L500)).
  - CLI `scan` and `inspect` scan one temporary snapshot and then independently call `ds.current_version()` for the label ([`crates/cli/src/main.rs:70-80`](../../../crates/cli/src/main.rs#L70-L80), [`93-103`](../../../crates/cli/src/main.rs#L93-L103)). A shared-handle writer can commit between those calls, yielding rows from version N labeled N+1.
  - The CLI is accurately documented as a fixed-assumption MVP tool rather than a stable administration surface ([`status ledger`](../../status.md#capability-ledger), [`how Strata works`](../../how-strata-works.md#scope-boundary)).

The vector-search path gets this right by capturing one snapshot for both search and row translation ([`crates/cli/src/main.rs:229-252`](../../../crates/cli/src/main.rs#L229-L252)). Apply the same ownership rule to scan/inspect output once a snapshot version accessor exists.

## Documentation and client-surface alignment

### Aligned

- Active architecture/status/how-it-works consistently bound concurrency to clones of one shared in-process `Dataset` handle and explicitly reject a cross-process CAS claim ([`architecture`](../../architecture.md#commit-and-snapshot-lifecycle), [`status ledger`](../../status.md#concurrency-scope), [`how Strata works`](../../how-strata-works.md#scope-boundary)).
- Active docs correctly distinguish immutable snapshot reads from write-only transactions and do not claim read-your-own-writes or a full read/write snapshot transaction ([`docs/architecture.md:29-35`](../../architecture.md#L29-L35), [`docs/how-strata-works.md:15-21`](../../how-strata-works.md#L15-L21)).
- CLI and Python status is truthful. The CLI is a fixed MVP/demo surface, while the binding exports only `placeholder_version` and has no transaction dependency ([`crates/bindings/src/lib.rs:1-15`](../../../crates/bindings/src/lib.rs#L1-L15), [`crates/bindings/Cargo.toml:7-15`](../../../crates/bindings/Cargo.toml#L7-L15), [`status ledger`](../../status.md#capability-ledger)).
- ADR 0008's central layout decision matches the implementation: immutable per-commit segments are manifest-listed, while branching and compaction remain absent ([`ADR 0008`](../../decisions/0008-adopt-segmented-index-layout.md#decision), [`status ledger`](../../status.md#capability-ledger)).

### Corrections still needed

- ARCH-04 is recorded in ADR 0003's current-API limitation; the ADR is a design ceiling, not a full transaction-surface claim.
- The active status ledger narrows schema behavior to caller-provided batch shape plus reserved hidden names and vector shape/dimension checks; dataset-wide logical field identity remains absent until ARCH-01 is fixed ([`status ledger`](../../status.md#capability-ledger)).
- ADR 0008 now uses bounded manifest-publication terminology and explicitly states that compaction and cross-process CAS are not implemented.

## Strengths

- The dependency DAG has no cycle and puts transaction orchestration above storage, query, and index concerns.
- `Dataset::snapshot()` returns an `Arc` to a snapshot whose manifest, segment set, and tombstones are private and replaced as a unit; callers cannot mutate a captured snapshot through the safe public API ([`crates/txn/src/dataset.rs:488-495`](../../../crates/txn/src/dataset.rs#L488-L495), [`crates/txn/src/snapshot.rs:51-63`](../../../crates/txn/src/snapshot.rs#L51-L63)).
- Row and vector-index publication share one manifest transition and one replacement snapshot. New file/segment preparation occurs before the lock; manifest assembly is based on the latest snapshot; in-memory visibility follows successful namespace publication, while durability remains subject to the Phase 1 audit ([`crates/txn/src/dataset.rs:915-1035`](../../../crates/txn/src/dataset.rs#L915-L1035), [`1113-1156`](../../../crates/txn/src/dataset.rs#L1113-L1156)).
- Hidden physical columns are reserved and rejected on insert, preventing direct user collision with `_row_id`/`_timestamp` ([`crates/txn/src/dataset.rs:42-65`](../../../crates/txn/src/dataset.rs#L42-L65), [`1205-1211`](../../../crates/txn/src/dataset.rs#L1205-L1211)).
- Snapshot scan, predicate scan, and filtered-vector support share centralized tombstone filtering, reducing the chance that one public read path forgets visibility filtering ([`crates/txn/src/snapshot.rs:156-230`](../../../crates/txn/src/snapshot.rs#L156-L230)).
- Vector dimension and non-finite-component checks are typed and performed before publication, with an authoritative in-lock dimension recheck against the latest segment set ([`crates/txn/src/dataset.rs:870-885`](../../../crates/txn/src/dataset.rs#L870-L885), [`994-1035`](../../../crates/txn/src/dataset.rs#L994-L1035), [`1822-1897`](../../../crates/txn/src/dataset.rs#L1822-L1897)).
- The Python placeholder is deliberately honest rather than presenting incomplete transaction semantics as a supported client API.

## Phase disposition summary

| Finding | Severity | Confidence | Affected phase | Disposition |
|---|---|---|---|---|
| ARCH-01 schema ownership/positional relabeling | Critical | High | 1, blocks 2 | Block Phase 1; persist and enforce one explicit logical schema contract. |
| ARCH-02 row-target/update semantics | Critical | High | 0 invariant, 1 | Block Phase 1; validate targets and replacement cardinality with typed outcomes. |
| ARCH-03 false conflict payload | High | High | 1 | Block Phase 1; expose insufficient history distinctly. |
| ARCH-04 ADR 0003 mismatch | High | High | 1 | Block Phase 1 documentation closure; amend or supersede the ADR. |
| ARCH-05 public escape hatches | High | High | 1 boundary, 2 | Decide and enforce the supported facade before Phase 1/2 claims. |
| ARCH-06 cross-crate facade leakage | Medium | High | 2 | Design before stable Rust/Python API; implementation can remain Phase 2. |
| ARCH-07 backend seam not plumbed | Medium | High | 6, constructor implications in 2 | Intentionally later; reserve a non-breaking location/backend seam. |
| ARCH-08 CLI version label race | Medium | High | 2 | Fix with coherent CLI/read-result design; not an engine-atomicity blocker. |

## Blockers and required decisions

1. Who owns the dataset logical schema, where is it persisted/versioned, and what exact equality/evolution rules apply to every inserted batch?
2. Must `delete`/`update` target a row visible in the begin-time snapshot? What typed outcomes distinguish missing, already deleted, future, and concurrently changed rows? Is `update` exactly one replacement row?
3. What public error represents “history was insufficient to prove no conflict,” without falsely identifying contested rows?
4. Is ADR 0003 an eventual isolation ceiling or a still-unmet v1 full read/write transaction requirement? Record the answer in a governing ADR.
5. Which crate/module is the supported Rust facade whose invariants Strata promises, and which low-level crates are explicitly internal or outside those guarantees?

ARCH-01 through ARCH-05 should be dispositioned before Phase 1 exit. ARCH-06 through ARCH-08 are not reasons to implement Phase 2/6 early, but their seams should be decided before stable client or constructor commitments make later work needlessly breaking.
