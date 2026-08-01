# Phase 1 concurrency audit

**Date:** 2026-08-01

**Lane:** Sol — lock scope, interleavings, shared-handle/process boundaries, and loom evidence

**Baseline:** current working tree, with [`docs/status.md`](../../status.md),
[`docs/roadmap.md`](../../roadmap.md), [`docs/architecture.md`](../../architecture.md), and
[`docs/audits/phase-1/README.md`](README.md) controlling. The tree was already substantially dirty,
including `crates/txn/src/dataset.rs`, `crates/txn/src/snapshot.rs`, transaction tests, and CI. This
lane changed no Rust, tests, dependencies, or configuration.

## Verdict

Phase 1 should **not** exit on the current concurrency evidence.

Within one process using clones of one `Dataset`, the normal commit path has a coherent shape:
preparation uses unique in-handle allocation, one mutex serializes validation through manifest
publication and snapshot installation, and readers load one immutable old-or-new snapshot. Focused
normal tests and three representative transaction loom models passed during this audit.

Two Phase 1 blockers remain:

1. A delete/update can tombstone an unallocated or in-flight row ID. A later insert can then return
   `Ok(())` while its newly published row is immediately invisible, violating the acknowledged-write
   invariant inside the supported shared-handle scope (CONC-01).
2. Transaction and live-set-cache loom models are not run by CI; only `strata-index`'s loom binary is
   built and executed (CONC-02).

The implementation also does not durably preserve abandoned row-ID reservations across an immediate
reopen, contrary to the current “never reused; permanent gaps” wording. Phase 1 must either preserve
that contract or explicitly narrow it through an approved design change (CONC-03).

Independent `Dataset::open` handles and separate writer processes are not coordinated. Their failure
modes are severe, but the active baseline correctly places implementation in Phase 4; this is an
intentionally bounded non-goal rather than a Phase 1 implementation blocker (CONC-04).

## Actual supported-scope guarantees

| Area | Actual guarantee and boundary | Evidence |
|---|---|---|
| Handle sharing | `Dataset::clone` shares the snapshot cell, row allocator, attempt counter, commit mutex/history, and timestamp floor. A separate `Dataset::open` shares none of them. | [`crates/txn/src/dataset.rs:173-210`](../../../crates/txn/src/dataset.rs#L173-L210), [`426-485`](../../../crates/txn/src/dataset.rs#L426-L485), [`516-529`](../../../crates/txn/src/dataset.rs#L516-L529) |
| Commit lock scope | Data files, row IDs, and an immutable vector segment are prepared/fsynced before the lock. The lock covers latest-snapshot reload, conflict check, manifest construction/publication, commit-log append, and snapshot store. No HNSW mutation occurs in-lock. | [`crates/txn/src/dataset.rs:915-945`](../../../crates/txn/src/dataset.rs#L915-L945), [`951-1035`](../../../crates/txn/src/dataset.rs#L951-L1035), [`1113-1155`](../../../crates/txn/src/dataset.rs#L1113-L1155), [`1191-1275`](../../../crates/txn/src/dataset.rs#L1191-L1275) |
| Allocators | One `RowIdAllocator` mutex gives non-overlapping, contiguous per-transaction claims in allocation order. `write_attempt_counter` gives unique paths only among transactions sharing that handle. Allocation occurs before commit serialization; allocation order can differ from publication order. | [`crates/txn/src/row_id.rs:101-163`](../../../crates/txn/src/row_id.rs#L101-L163), [`crates/txn/src/dataset.rs:1213-1250`](../../../crates/txn/src/dataset.rs#L1213-L1250) |
| Conflict detection | OCC is write-write only on row IDs added by `delete`/`update`. Inserts have an empty write set and never conflict. History is a 2,048-entry in-memory ring; missing history conservatively returns a typed conflict containing the transaction's entire write set. | [`crates/txn/src/dataset.rs:67-83`](../../../crates/txn/src/dataset.rs#L67-L83), [`752-806`](../../../crates/txn/src/dataset.rs#L752-L806), [`951-962`](../../../crates/txn/src/dataset.rs#L951-L962), [`crates/txn/src/commit_log.rs:55-112`](../../../crates/txn/src/commit_log.rs#L55-L112) |
| Snapshot publication | Production uses `ArcSwap`; `snapshot()` returns a full `Arc<Snapshot>`. A normal shared-handle reader observes one whole pre- or post-commit snapshot. Existing snapshots retain immutable manifest, segment-set, and tombstone state. This is not a transactional read/write API. | [`crates/txn/src/dataset.rs:85-120`](../../../crates/txn/src/dataset.rs#L85-L120), [`488-500`](../../../crates/txn/src/dataset.rs#L488-L500), [`1123-1155`](../../../crates/txn/src/dataset.rs#L1123-L1155), [`crates/txn/src/snapshot.rs:51-63`](../../../crates/txn/src/snapshot.rs#L51-L63) |
| Row/index boundary | A manifest and the installed `Snapshot` pair data-file entries with the corresponding immutable segment entries/readers. Failed pre-publication work is orphaned and unreachable. | [`crates/txn/src/dataset.rs:974-1035`](../../../crates/txn/src/dataset.rs#L974-L1035), [`1113-1155`](../../../crates/txn/src/dataset.rs#L1113-L1155), [`crates/txn/src/snapshot.rs:124-146`](../../../crates/txn/src/snapshot.rs#L124-L146) |

These guarantees do **not** amount to serializability or even a full read/write snapshot transaction:
`Transaction` has no read set or transactional read API, and the active architecture already states
that narrower boundary ([`docs/architecture.md:29-35`](../../architecture.md#L29-L35),
[`49-53`](../../architecture.md#L49-L53)).

## Findings

### CONC-01 — A future/in-flight tombstone can make a successful insert invisible

- **Severity:** Critical
- **Confidence:** High (direct control/data-flow proof; no existence/membership validation is present)
- **Affected phase:** Phase 1
- **Disposition:** Phase 1 blocker

`Transaction::delete` accepts any `u64` and immediately adds it to both `pending_tombstones` and the
write set; it does not require that the row is present in the transaction's base snapshot or any
committed manifest ([`crates/txn/src/dataset.rs:748-755`](../../../crates/txn/src/dataset.rs#L748-L755)).
Commit applies every such ID to the latest tombstone set and manifest after only write-write history
validation ([`951-963`](../../../crates/txn/src/dataset.rs#L951-L963),
[`1069-1082`](../../../crates/txn/src/dataset.rs#L1069-L1082)). Inserts do not enter the write set, and
the commit log explicitly treats an empty write set as unconditionally clean
([`crates/txn/src/commit_log.rs:89-98`](../../../crates/txn/src/commit_log.rs#L89-L98)). Visibility is
then exactly “not in this snapshot's tombstone set”
([`crates/txn/src/snapshot.rs:124-146`](../../../crates/txn/src/snapshot.rs#L124-L146)).

Minimal supported-scope interleaving:

1. Insert transaction A claims row ID `r` and writes its files before `commit_lock`
   ([`crates/txn/src/dataset.rs:915-928`](../../../crates/txn/src/dataset.rs#L915-L928),
   [`1237-1258`](../../../crates/txn/src/dataset.rs#L1237-L1258)).
2. Transaction B calls `delete(r)` and commits first. Because A has not published, B sees no
   conflicting committed write and publishes a tombstone for `r`; the audit does not treat this as a
   universal power-loss durability proof.
3. A acquires `commit_lock`. Its insert-only write set is empty, so it cannot conflict. It layers its
   file/segment onto B's latest manifest, which already contains tombstone `r`, stores the snapshot,
   and returns `Ok(())`.
4. Both scan and vector search filter out A's row. The insert returned success after the current
   publication path, but is not visible; directory durability remains a separate blocked finding.

Concurrency is not required to trigger the underlying flaw: deleting ID `0` on an empty dataset and
then inserting the first row also poisons that future allocation. The concurrent form matters because
even a high-water validation would be insufficient: another commit can publish a `next_row_id` that
numerically covers A's still-in-flight claim, a state the current design deliberately permits
([`docs/design/phase-0-transaction-and-format-spec.md:110-116`](../../design/phase-0-transaction-and-format-spec.md#L110-L116)).
Validation therefore needs committed-row membership semantics, not just `row_id < next_row_id`.

Current evidence does not cover this schedule. Delete/update unit and loom tests target seeded rows;
the chaos worker only chooses IDs recorded after successful commits
([`crates/chaos-worker/src/commit_ops.rs:39-42`](../../../crates/chaos-worker/src/commit_ops.rs#L39-L42),
[`61-96`](../../../crates/chaos-worker/src/commit_ops.rs#L61-L96)). Model 3 races insert-only writers
and proves no in-flight row leaks into an earlier snapshot, but it does not race a tombstone against an
in-flight allocation ([`crates/txn/src/dataset.rs:8003-8039`](../../../crates/txn/src/dataset.rs#L8003-L8039)).

**Required disposition:** define delete/update behavior for absent IDs, prevent tombstones for IDs not
present in an appropriate committed snapshot, and add a deterministic regression plus a targeted loom
model for the in-flight allocation schedule. The implementation choice requires Sol design review;
silently accepting the current outcome is incompatible with “acknowledged only after ... visible.”

### CONC-02 — Transaction loom models are not a CI gate

- **Severity:** Major
- **Confidence:** High
- **Affected phase:** Phase 1 verification
- **Disposition:** Phase 1 blocker

The transaction crate contains `cfg(loom)` replacements for its mutexes/snapshot cell and dataset-level
models for conflict arbitration, failed publication, dimension races, atomic row/index visibility, and
in-flight allocation. `live_set_cache.rs` has another loom model. The documented build requires a
crate-scoped `--cfg loom` invocation because workspace-wide `RUSTFLAGS` breaks dependency builds
([`crates/txn/src/dataset.rs:6970-6988`](../../../crates/txn/src/dataset.rs#L6970-L6988),
[`crates/txn/src/live_set_cache.rs:437-442`](../../../crates/txn/src/live_set_cache.rs#L437-L442)).

CI runs ordinary workspace tests and a `parallel-insert` transaction test, but its sole loom step builds
and executes `strata-index`; there is no `strata-txn` loom invocation
([`.github/workflows/ci.yml:23-56`](../../../.github/workflows/ci.yml#L23-L56)). Consequently, ordinary
green CI neither compiles nor executes the transaction-specific loom configuration.

This audit freshly built the transaction loom binary and ran representative exact models:

```text
cargo rustc -p strata-txn --lib --profile test -- --cfg loom
dataset::loom_tests::a_commits_row_and_its_segment_become_visible_as_one_atomic_step  PASS
dataset::loom_tests::a_failed_commits_segment_is_never_visible_to_a_concurrent_reader PASS
dataset::loom_tests::two_threads_deleting_the_same_row_exactly_one_conflicts           PASS
```

The preemption-bounded Model 3
`a_reader_never_sees_one_in_flight_commits_row_while_observing_an_unrelated_commits_row_id_counter`
did not fail but exceeded a 300-second audit timeout, so its fresh result is **inconclusive**, not a
pass. The source already documents that this model is the one bounded exception and that whole-module
runs can exhaust Windows resources
([`crates/txn/src/dataset.rs:7033-7047`](../../../crates/txn/src/dataset.rs#L7033-L7047),
[`7118-7125`](../../../crates/txn/src/dataset.rs#L7118-L7125),
[`8044-8081`](../../../crates/txn/src/dataset.rs#L8044-L8081)).

**Required disposition:** add a CI-visible, crate-scoped transaction loom gate, sharding exact models
if necessary. Give the expensive bounded model an explicit budget/runner strategy rather than relying
on historical local results. Add CONC-01's missing model before Phase 1 exit.

### CONC-03 — Abandoned row-ID reservations can be reused after immediate reopen

- **Severity:** Major contract mismatch; currently no demonstrated collision with a committed row
- **Confidence:** High (direct persistence-path proof)
- **Affected phase:** Phase 0 row-identity invariant and Phase 1 recovery evidence
- **Disposition:** Phase 1 blocker pending design decision; implementation fix or explicit contract correction

Row IDs are claimed before `commit_lock` and before conflict/durability outcomes
([`crates/txn/src/dataset.rs:1237-1250`](../../../crates/txn/src/dataset.rs#L1237-L1250)). A conflict,
dimension error, injected manifest error, or panic before manifest publication returns without persisting
the advanced allocator. `manifest.next_row_id` is copied from the in-memory allocator only on a later
clean commit ([`951-963`](../../../crates/txn/src/dataset.rs#L951-L963),
[`1023-1041`](../../../crates/txn/src/dataset.rs#L1023-L1041),
[`1093-1114`](../../../crates/txn/src/dataset.rs#L1093-L1114)). An immediate `Dataset::open` seeds a new
allocator solely from the older manifest value
([`426-445`](../../../crates/txn/src/dataset.rs#L426-L445)). Its next insert can therefore claim the
same ID abandoned by the failed attempt.

The failed-commit tests do reopen immediately, but only check version/segment/search state; they do not
commit through the reopened allocator or assert the next row ID
([`crates/txn/src/dataset.rs:2240-2310`](../../../crates/txn/src/dataset.rs#L2240-L2310),
[`6708-6763`](../../../crates/txn/src/dataset.rs#L6708-L6763)). A later successful commit on the original
handle does persist past the abandoned gap, which is a different sequence
([`6289-6369`](../../../crates/txn/src/dataset.rs#L6289-L6369)).

This contradicts the current source/spec wording that a reopened dataset never reuses committed **or
abandoned** IDs and that failed attempts become permanent gaps
([`crates/txn/src/dataset.rs:1036-1041`](../../../crates/txn/src/dataset.rs#L1036-L1041),
[`crates/storage/src/manifest.rs:112-119`](../../../crates/storage/src/manifest.rs#L112-L119),
[`docs/design/phase-0-transaction-and-format-spec.md:118`](../../design/phase-0-transaction-and-format-spec.md#L118)).
That spec sentence also incorrectly says IDs are monotonic in successful-commit order; pre-lock claims
make them monotonic in allocation order, while publication order may invert.

**Required disposition:** either make allocation reservations durable enough to preserve abandoned gaps
across reopen, or approve and document the narrower guarantee that only published row IDs are never
reused. Until that choice is made consistently in AGENTS/spec/source/tests, the Phase 0/1 identity claim
is not evidenced.

### CONC-04 — Independent openers can collide and lose acknowledged writes

- **Severity:** Critical impact when the unsupported mode is used
- **Confidence:** High
- **Affected phase:** Phase 4 — cross-process coordination
- **Disposition:** Later-phase implementation item; intentionally bounded/non-goal for Phase 1

Every `Dataset::open` constructs new allocators, attempt counter, commit mutex/history, and snapshot cell
from the same current manifest ([`crates/txn/src/dataset.rs:426-485`](../../../crates/txn/src/dataset.rs#L426-L485)).
Two same-version openers can therefore claim the same row IDs and attempt IDs. Their pre-lock write
phases target the same paths, and both data and segment writers use truncating `File::create`
([`crates/storage/src/datafile.rs:21-58`](../../../crates/storage/src/datafile.rs#L21-L58)). Each local
mutex then independently chooses the same `latest_version + 1`; manifest publication is an unconditional
rename/replace, not compare-and-swap
([`crates/txn/src/dataset.rs:945-987`](../../../crates/txn/src/dataset.rs#L945-L987),
[`crates/storage/src/manifest.rs:193-216`](../../../crates/storage/src/manifest.rs#L193-L216)). Both
commits can return success while one manifest replaces the other, or while one opener has overwritten
files referenced by the other. A handle opened before another handle's commit also remains stale because
its `ArcSwap` is never refreshed from disk.

The chaos harness is a real-process crash/restart test, not a multi-process writer test. Each run starts
one worker process ([`tests/sim/tests/chaos.rs:191-203`](../../../tests/sim/tests/chaos.rs#L191-L203));
that worker creates one shared `Arc<Dataset>` and gives clones to its OS threads
([`crates/chaos-worker/src/main.rs:136-156`](../../../crates/chaos-worker/src/main.rs#L136-L156),
[`183-200`](../../../crates/chaos-worker/src/main.rs#L183-L200)). It supplies useful shared-handle and
recovery evidence but no independent-opener safety evidence.

The active documentation is accurate: shared-handle scope is explicit
([`status ledger`](../../status.md#concurrency-scope)), and durable conditional publication, shared
allocation, and process-boundary tests are Phase 4
([`Phase 4 — Cross-process coordination`](../../roadmap.md#phase-4--cross-process-coordination)). Do not reinterpret the current lock, chaos harness,
or `Dataset::open` recovery tests as cross-process coordination. Phase 1 needs no hidden implementation
of Phase 4, but public/API documentation should keep the warning prominent until coordination or an
enforced exclusive-opener guard exists.

## Loom and test evidence boundary

- `cargo test -p strata-txn --test concurrent_snapshot_isolation` passed all 4 tests during this audit.
  The suite exercises old-snapshot stability and row/vector visibility against one shared handle
  ([`crates/txn/tests/concurrent_snapshot_isolation.rs:22-113`](../../../crates/txn/tests/concurrent_snapshot_isolation.rs#L22-L113)).
- Transaction loom substitutes a `Mutex<Arc<Snapshot>>` for production `ArcSwap`. This correctly makes
  protocol-level load/store order visible to loom, but it does not model ArcSwap's internal atomics or
  lock-freedom. The source states that boundary explicitly
  ([`crates/txn/src/dataset.rs:85-130`](../../../crates/txn/src/dataset.rs#L85-L130)). Treat the models as
  proof over an atomic snapshot-cell abstraction plus loom mutexes, not as verification of the external
  crate's implementation.
- Most dataset loom models are exhaustive, but Model 3 is explicitly preemption-bounded at 3. Its
  `OBSERVED` bitmask checks four coarse publication states, which is useful coverage evidence but does
  not make the run exhaustive ([`crates/txn/src/dataset.rs:8044-8092`](../../../crates/txn/src/dataset.rs#L8044-L8092),
  [`8179-8202`](../../../crates/txn/src/dataset.rs#L8179-L8202)).
- No current test targets future-ID tombstones, simultaneous independent writers, or post-failure
  allocation through an immediately reopened handle. Those omissions correspond directly to CONC-01,
  CONC-03, and the deferred Phase 4 boundary.

## Disposition summary

| ID | Severity | Confidence | Phase | Disposition |
|---|---|---|---|---|
| CONC-01 | Critical | High | 1 | Block Phase 1; specify absent-row delete/update semantics and add deterministic + loom regression coverage. |
| CONC-02 | Major | High | 1 verification | Block Phase 1; make transaction/cache loom models a CI-visible gate. |
| CONC-03 | Major contract mismatch | High | 0 invariant / 1 recovery | Block Phase 1 pending durable reservation or approved narrowing of “never reused.” |
| CONC-04 | Critical impact outside supported scope | High | 4 | Intentionally bounded Phase 1 non-goal; implement conditional publication/shared coordination and process tests in Phase 4. |
