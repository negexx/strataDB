# Phase 0 Spec — File Format & Transaction Model

**Status:** Draft — this is the Phase 0 deliverable itself (see `.claude/docs/architecture.md`'s roadmap). Exit criterion: reviewed against FoundationDB's and CockroachDB's public consistency docs, plus Lance's format spec. This document does that review explicitly — every design choice below says which reference it follows and which it deliberately departs from, and why.

**Scope:** this defines *what* Strata's transaction and storage model are, precisely enough to implement against. It does not contain code. Real implementation happens in `crates/storage/` and `crates/txn/` starting in Phase 1, and must not diverge from this spec without a new revision here first (see rules/concurrency-txn-layer.md).

---

## 1. What a "conflict" is (precise definition)

A **conflict** exists between transaction A and transaction B if and only if:

- A committed with commit-version `v_A`, and
- B's read-version `v_B < v_A` (B started before A committed), and
- B is still attempting to commit (B's commit-version has not yet been assigned), and
- The **row/key-range read set or write set of B intersects the write set of A**.

This is FoundationDB's read-write-overlap definition, adopted directly: *"A transaction conflicts if it reads a key that was written by another committed transaction between the transaction's read version and commit version."* Strata narrows "key" to **row-id ranges and, separately, index key ranges** (see §4) — matching Strata's own existing design language ("row/key-range granularity conflict detection, not whole-dataset locking").

**What does NOT count as a conflict:**
- Two transactions writing disjoint rows — never conflicts, regardless of timing.
- A transaction reading a row that is later written by another transaction that started *after* the reader's commit — not a conflict; the reader's snapshot was valid at its own read-version.
- Two transactions both reading the same row without either writing it — never conflicts (this is why reads never block reads or writes: see §3).

**Explicit non-goal:** Strata does not detect *write skew* (the classic snapshot-isolation anomaly where two transactions each read a value the other writes, and both commit safely under SI but would violate serializability). This is not a bug — it's ADR 0003's accepted tradeoff. Do not add write-skew detection without a new ADR superseding 0003.

## 2. What a "transaction boundary" is (precise definition)

- **Begin:** a transaction's read-version is set to the current manifest version *at the moment of its first operation* (read or write) — not at some earlier "session start." This matches FoundationDB's implicit-begin model (no separate `BEGIN` call fixes the version; the first read/write does). Everything the transaction reads from that point on is read *as of* that manifest version, even as later commits land concurrently.
- **During:** all writes are buffered in-memory (or in a transaction-local staging area) and are **not visible to any other transaction, including via disk reads** — this is a deliberate simplification versus CockroachDB's write-intents (see §5, "What we don't need"). Strata is single-node; there is no cross-node intent-resolution problem to solve, so there is no reason to make in-flight writes durable or visible before commit.
- **Commit:** see §3 for the exact protocol. A transaction is committed, atomically and permanently, at the instant its manifest-version CAS succeeds — not before, not gradually.
- **Abort:** happens if (a) the caller explicitly aborts, or (b) commit-time conflict detection finds a real conflict. An aborted transaction's buffered writes are discarded; nothing it did is ever visible to any reader, including itself after abort.
- **Visibility:** a transaction's own writes are visible to itself immediately (read-your-own-writes within the transaction), but never to any other transaction until commit succeeds.

## 3. Commit protocol (exact steps)

Adapted from FoundationDB's pipeline, collapsed for a single-node system (no Proxy/Resolver split needed — one process can do all of this in-order):

1. **Read-set / write-set finalization.** The transaction has been accumulating a read-set (row-ids and index keys read) and a write-set (row-ids and index keys written) throughout its lifetime. At commit time, both are finalized.
2. **Conflict check.** Walk every transaction that committed with a commit-version between this transaction's read-version and "now." For each, check whether *this* transaction's read-set intersects *that* transaction's write-set (§1's definition). If any intersection exists: abort, return a typed conflict error naming the contested row-ids/keys (see `rules/concurrency-txn-layer.md`).
3. **Durable write.** If clean: write the new row data and the vector-index delta-log entries to disk, `fsync`. This happens *before* the manifest pointer moves — nothing is visible yet.
4. **Atomic manifest commit.** Compare-and-swap the "current manifest version" pointer from the transaction's read-version-plus-any-intervening-commits to the new version. On a local filesystem this is implemented as an atomic rename (`rename()` is atomic on the same filesystem on both POSIX and NTFS) of a new manifest file into the well-known "current" path or an equivalent atomic pointer update — this is exactly Lance's mechanism (*"A new version is committed via an atomic operation on the underlying object store (rename-if-not-exists or put-if-not-exists)"*), adopted directly since Strata is local-disk-first (Phase 1-8) before any object-storage backend (Phase 9).
5. **Acknowledge.** Only after step 4 succeeds is the caller told the transaction committed. If the CAS in step 4 fails (another transaction's manifest update landed first), re-run step 2's conflict check against the newly-committed transaction before retrying the CAS — do not blindly retry the CAS without re-checking conflicts.

**Where this differs from FoundationDB:** FDB assigns the commit-version from a centralized version oracle *before* conflict detection, because it's distributed and needs to fix an ordering across many machines before doing parallel conflict checks. Strata is single-node — there's no need to pre-assign a version before checking conflicts, since there's no distributed coordination to hide latency behind. The manifest-version CAS *is* both the version assignment and the durability point, combined into one step.

**Where this differs from CockroachDB:** CockroachDB's Parallel Commits protocol exists to avoid a second network round-trip when resolving intents across a distributed cluster. Strata has no network round-trips to avoid — everything is local. There is no `STAGING` state, no intent resolution, no asynchronous cleanup. Commit is synchronous and atomic in one step (§3.4).

## 4. Conflict detection granularity

Two independent conflict domains, checked separately:

- **Row-level:** keyed by row-id (or a stable primary-key-derived id). A write-set entry is a row-id; overlap = set intersection.
- **Vector-index-level:** keyed by the same row-id, since every indexed vector corresponds to exactly one row (Strata does not support indexing a value that isn't also a row-level column). A transaction that writes row R and updates the index entry for R produces conflict-relevant entries at *both* levels for the same id — but they're still checked as one unit, because §6 requires them to commit atomically anyway. There is no independent "index-only" conflict domain that could conflict without a corresponding row conflict.

This directly implements FDB's "conflict ranges" concept, narrowed from arbitrary byte-ranges to row-id sets, matching Strata's existing "row/key-range granularity, not whole-dataset locking" language.

## 5. What we deliberately don't need (vs. FoundationDB / CockroachDB)

Both reference systems solve problems Strata, as a single-node embedded engine, doesn't have:

- **No distributed consensus / Raft.** FDB's Resolvers and CockroachDB's Raft-replicated transaction records exist because their data spans multiple physical machines. Strata's manifest lives on one machine's disk (or, later, one object-storage bucket) — the CAS in §3.4 is the entire "consensus" mechanism needed.
- **No write intents visible before commit.** CockroachDB makes writes visible-but-provisional (as intents) *specifically* so other distributed nodes can make progress without waiting on a slow commit. Strata has no other nodes to unblock — buffered writes staying fully invisible until commit (§2) is strictly simpler and loses nothing.
- **No timestamp-pushing / dynamic priority.** CockroachDB pushes a transaction's timestamp forward or aborts based on priority to close the serializability gap. Strata doesn't try to guarantee serializability (ADR 0003) — plain "detect overlap, abort the later committer" (§1) is sufficient for snapshot isolation and is what FDB itself does for the equivalent case.
- **No Hybrid Logical Clocks.** HLCs exist to give a globally-comparable timestamp across machines with clock skew. Strata's manifest version is a single monotonically-increasing counter on one machine — no clock synchronization problem exists.

## 6. File format (storage half)

Adapted from Lance's on-disk layout, since Lance is explicitly named as the storage-format reference in the project spec and its design already solves the "columnar + vector columns in one file, versioned manifest" problem Strata also has:

- **Per-file footer** (fixed size, at end of file): magic bytes (`STRA`, distinct from Lance's `LANC` — these are different formats and must not be mistaken for each other), format major/minor version, column count, offset to column metadata table, offset to global buffer table, global buffer count. Mirrors Lance's 40-byte footer shape directly.
- **Column metadata:** one descriptor per column (offsets, encoding, null bitmap location). Vector columns use a fixed-size-list representation, same as Lance's `FixedSizeList` approach for embeddings — this is a well-proven shape, no reason to invent a different one.
- **Manifest:** one file per version (`_versions/{version:020}.manifest`, zero-padded for lexicographic = numeric ordering, following Lance's own convention exactly), containing: schema, the list of data files belonging to this version, the vector-index delta-log entries belonging to this version, writer metadata. Manifests are immutable once written — a new version is always a new file, never an edit of an existing one.
- **Commit = atomic manifest-pointer update** (§3.4) — this is where Strata's design **intentionally stops following Lance**: Lance treats conflicting concurrent writers as an application-level problem with three outcome classes (rebasable/retryable/incompatible) and *does not guarantee serializability or snapshot isolation for conflicting writes*. That gap — leaving correctness under concurrent writes to the calling application — is precisely the problem Strata's whole flagship differentiator exists to close (see `docs/architecture.md`'s one-paragraph overview). Every commit in Strata goes through §1-§4's conflict detection before the manifest pointer ever moves; there is no "rebasable, auto-merge and hope" path.

## 7. Open questions for Phase 1 (not blocking, but flag before implementing)

- Exact on-disk encoding of the row-id → conflict-set mapping used during §3.2's walk (in-memory ring buffer of recent commits, à la FDB's 5-second resolver window, is the likely shape — needs a retention-window decision: how far back must a live transaction be able to look?).
- ~~Exact wire format of the vector-index delta-log entries referenced in §6's manifest~~ — resolved by §8, added ahead of Phase 4.
- ~~Whether the row-id conflict-set and the manifest's data-file list need to be stored together or can be derived from each other~~ — resolved by §8: row-id is now a defined, first-class concept (a `next_row_id` counter alongside `version` in the manifest), not an implementation detail deferred to commit-check time.

## 8. Row-ID definition and lifecycle (added ahead of Phase 4)

**Status:** added via `llm-council` review before Phase 4 (Vector Index) implementation — see `.superpowers/council/council-transcript-20260716-174711.md` for the full deliberation. §4 already presumed a "row-id (or a stable primary-key-derived id)" existed for conflict detection; this section supplies the definition §4 deferred, driven by Phase 4's more immediate need to key HNSW insertions by something stable.

**Definition.** A row-id is a **logical identity**, not a physical location or user data. Every row, from the moment it is inserted until it is deleted (not merely superseded by an update), has exactly one row-id for its entire lifetime:

- A `u64`, assigned monotonically at commit time from a `next_row_id: u64` counter stored in the manifest alongside `version`.
- Global across the dataset's entire history — never reset, never reused, even after the row it names is later deleted.
- Independent of any user-supplied column data (rejected: reusing a user `id` column — see the council transcript's Contrarian/First-Principles analysis on why this is a category error, not a shortcut).

**Assignment timing and atomicity.** The counter is claimed as part of §3 step 4 (the atomic manifest CAS), not as a separate operation before or after it. A commit writing N rows atomically: (a) claims the contiguous range `[next_row_id, next_row_id + N)` for its rows, (b) advances `next_row_id` by N, and (c) swaps the manifest pointer — all as one CAS, the same choke point every commit already serializes through per §3. This adds no new contention.

If the CAS fails and the transaction retries per §3 step 5, the row-ids provisionally claimed by the failed attempt are discarded, never reused — a fresh range is claimed on the successful retry. Row-ids are therefore monotonically increasing in successful-commit order but may have gaps; **gaps are safe, reuse is forbidden** (a reused row-id could collide with a still-live row's HNSW graph entry and silently corrupt search results — exactly the failure mode this project's flagship correctness claim exists to rule out). This mirrors how `Manifest.version` already behaves on abort. Per `.claude/rules/concurrency-txn-layer.md`, the counter-bump-plus-CAS step needs a `loom` interleaving test proving it is genuinely atomic under concurrent commit attempts before Phase 6 lands — not deferred, since Phase 4 is where this code is first written.

**Provisional ids within one transaction.** A transaction buffers rows locally before `commit()` and does not know their final row-ids until the commit's CAS succeeds (the base could shift if another commit lands first). While buffered, each row has a transaction-local, zero-based provisional index (0, 1, 2, ... in insertion order within that transaction). At commit, once the real base is claimed, provisional index `i` resolves to real row-id `claimed_base + i`. Any future in-transaction self-reference to "the row I just inserted" resolves through this offset mapping, never through a value read back from disk mid-transaction.

**UPDATE semantics (not yet implemented, but must not be precluded).** Strata's API today (Phases 1-4) is insert/scan/filter/search only — there is no UPDATE. When one is added (Phase 5/6 territory), it is defined as: the row-id is preserved; physically it is a tombstone of the row-id's previous vector-index entry plus a fresh row insert carrying the *same* row-id and new values. The row-id is never reassigned by an UPDATE — this is the concrete meaning of "row-id is logical identity." `Dataset::scan`'s existing visibility rule (§2) already determines which of a row-id's physical versions across commits is current for a given manifest version; no new mechanism is needed.

**DELETE semantics (not yet implemented, but must not be precluded).** A DELETE is a tombstone entry for the row's row-id with no successor insert. The row-id is retired permanently — never reassigned, even after Phase 8 compaction reclaims the physical bytes.

**Vector-index delta-log entry shape** (resolves the wire-format question from §7, and the "vector-index delta-log entries belonging to this version" reference in §6): a delta-log entry is one of:

```
Insert    { row_id: u64, vector: Vec<f32> }   // row_id's embedding enters the graph for the first time
Tombstone { row_id: u64 }                     // row_id's current graph entry is logically removed
          // (used for DELETE, and as the first half of an UPDATE's tombstone-then-insert pair)
```

Each commit's delta-log entries are written to their own append-only file, mirroring `crates/storage`'s per-commit data files, and referenced from the manifest for that version. The exact byte-level encoding (bincode vs. a small custom binary layout) is Phase 4 implementation detail, not a spec-level decision — the tagged-enum-keyed-by-row-id shape above is.

**Recovery / graph reconstruction.** `Dataset::open` (or the first vector-search call) replays every committed delta-log entry, across all committed versions up to the current manifest version, in commit order, into a fresh in-memory `hnsw_rs::Hnsw`: `Insert` entries call `hnsw.insert((&vector, row_id_as_usize))`; `Tombstone` entries are recorded in an in-memory tombstone set. Per `hnsw_rs`'s no-in-place-mutation constraint (confirmed against the installed `hnsw_rs-0.3.4` source — there is no node-removal API), a tombstoned row's graph node physically remains until Phase 8 compaction rebuilds the graph from only-live rows; until then, tombstoned row-ids are filtered out of search results at query time via the same `FilterT` mechanism used for predicate-filtered ANN (a tombstone-membership check composed with any caller-supplied predicate filter).

**HNSW label space (`u64` row-id vs. `usize` graph label).** `hnsw_rs::Hnsw::insert` takes a plain `usize` id. Strata's row-id is `u64`. On the 64-bit platforms this project builds and tests for (no 32-bit target exists in CI), `usize` is 64 bits and the row-id passes through directly via a checked `u64::try_from`/`usize::try_from` conversion at the two call sites (`insert`, and reading `Neighbour::get_origin_id()` back) — no separate mapping table is needed. If a 32-bit target is ever added, this conversion becomes fallible there and must return a typed error, never silently truncate; the checked conversion already makes this the default behavior, so no change is needed when that day comes, only a decision about what the error should do.

**Conflict-detection key (Phase 5/6 forward reference).** Row-id as defined here — global, monotonic, logical, permanent — is exactly the identifier §4 already presumed existed for row-level conflict detection. No change to §4 is needed; this section supplies the definition §4 left open.

**Tombstone GC — explicitly deferred, not designed here.** Reclaiming a tombstoned row's physical storage (columnar bytes) and HNSW graph node is Phase 8 compaction's responsibility, not Phase 4-6's. Phase 4-6 code must not assume a tombstoned row is ever physically removed before compaction runs: `scan`, `search`, and (later) conflict detection must all treat a tombstoned row-id as dead regardless of whether its bytes are still on disk.

---

*This spec should be updated in place as understanding improves during Phase 1 implementation — unlike ADRs, it is not immutable, because it is a living technical reference, not a decision record. If a change here contradicts an existing ADR (e.g. reopens serializability), stop and write a new ADR first; don't let the spec and the ADRs drift apart.*
