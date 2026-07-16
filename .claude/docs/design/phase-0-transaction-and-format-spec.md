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
- Exact wire format of the vector-index delta-log entries referenced in §6's manifest (depends on `crates/index/`'s real implementation, which doesn't exist yet — see `.claude/rules/vector-index.md`).
- Whether the row-id conflict-set and the manifest's data-file list need to be stored together or can be derived from each other at commit-check time — an implementation detail that doesn't change this spec's guarantees either way.

---

*This spec should be updated in place as understanding improves during Phase 1 implementation — unlike ADRs, it is not immutable, because it is a living technical reference, not a decision record. If a change here contradicts an existing ADR (e.g. reopens serializability), stop and write a new ADR first; don't let the spec and the ADRs drift apart.*
