# Phase S1 W2 — First-Class Timestamp Column — Design

**Date:** 2026-07-25
**Trigger:** `.claude/docs/design/phase-s1-segmented-index-spec.md` §5.2 specs W2 at a high level and
flags two open design decisions (logical vs. physical clock; real column vs. manifest metadata) plus
a landmine recorded during W1's final review (missing-column handling once older files predate this
column). This doc resolves both, plus a third tension the spec's own wording exposes once the commit
path is read closely: "non-decreasing across versions" is in real tension with `write_phase` running
outside `commit_lock`.

**Process:** Brainstormed live against the actual `commit()`/`write_phase`/`Manifest` code (not a
paraphrase — re-read fresh this session, since it moved since W1 landed), one product-level tradeoff
decided via dialogue (§1), then an Opus-tier pass pressure-tested the resulting draft against the real
codebase and found two would-be bugs (§3, §5) and one framing error (§2) before any code was written.

---

## 1. The core tradeoff, and the decision

The spec requires timestamps to be (a) assigned via "a single monotonic clock read per commit" and (b)
"non-decreasing across versions." `write_phase` — where row-ids are claimed and the data file is
written and fsynced — runs **outside** `commit_lock`, deliberately, so that expensive I/O is never
serialized (a property this project protects; see the concurrent, independent effort in another
session right now further optimizing exactly this path). Two transactions can read the wall clock in
one order but reach `commit_lock` — which assigns `Manifest.version`, the actual publish order — in
the *opposite* order. There is no way to guarantee a strict per-row non-decreasing ordering across
versions without either serializing the data-file write inside the lock (a real throughput regression)
or decoupling "the row's own honest timestamp" from "the dataset's published monotonic guarantee."

**Decided:** decouple. Each row's `_timestamp` is an honest wall-clock capture from its own
transaction, taken outside the lock, same architecture as row-id. A separate manifest field provides
the actual non-decreasing-across-versions guarantee the spec asks for (§4). This preserves the
lock-scope property `write_phase` exists for, matches the spec's own preferred column-based approach
(§2), and — as walked through in §6 — does not weaken file-pruning correctness, because pruning
operates per-file, never by comparing manifest version order to time order.

---

## 2. Column: type, precision, placement

**`_timestamp: Int64`, microseconds since the Unix epoch** — not `UInt64`, and not milliseconds.

- **Not `UInt64`:** `strata_storage::Value` (the shared vocabulary between `Predicate` and
  `ColumnStats`) has exactly three variants — `Int64`, `Float64`, `Utf8` — no unsigned integer. A
  `UInt64` column would compile, commit, and then fail every predicate against it at runtime
  (`arrow-ord`'s comparison kernel errors on a physical-type mismatch when a leaf builds an `Int64`
  scalar against a `UInt64` array) — directly breaking W2's own stated exit criterion ("a temporal
  predicate filters"). `Int64` is what `Value`/`Predicate`/`compute_stats` already support end to end.
- **Not milliseconds:** at millisecond resolution, two commits landing in the same millisecond under
  concurrent load share an identical value, which would make W2's own required monotonicity test
  provable only as `>=`, never able to distinguish "genuinely equal" from "resolution too coarse to
  tell." Microseconds pushes that collision window narrow enough to be a non-issue for this project's
  workloads without adding real complexity.

**Placement:** appended as a hidden column exactly like `_row_id` — `append_timestamp_column`,
analogous to and called alongside `append_row_id_column` in `write_pending_batches`, after
`compute_stats` computes the *user* columns' stats on the pre-append batch (§4 covers `_timestamp`'s
own stats entry, added separately, not via that same `compute_stats` call — see below).

---

## 3. Capture point and the two-layer monotonicity guard

**Capture once, at the top of `Transaction::commit()`, before `write_phase` is called — not inside
`write_phase`.** Two reasons, both found by pressure-testing the first draft: `write_phase` returns
early for a delete-only transaction (no pending batches), which would silently skip timestamp capture
for a commit that should still be able to advance the monotonic high-water mark (§4); and capturing
before `self.row_ids.claim()` means a clock-read failure aborts before any row-id is claimed or any
file written — no partial state to unwind.

**Layer 1 — issuance-order monotonicity, lock-free, guards against wall-clock regression.** A wall
clock can jump backward on its own (an NTP step, a manual clock change) with no concurrency involved
at all — a single-threaded gap the first draft understated by framing this as purely a concurrency
race. A `Dataset`-level `AtomicI64` (call it `last_issued_timestamp`) issues each captured value via
`fetch_max(candidate, Ordering::SeqCst)`, so the value actually used by a commit is
`max(now(), every_prior_issuance)` — non-decreasing in issuance order regardless of what the OS clock
does. No `Mutex` needed (unlike `RowIdAllocator`, which must make a counter-advance and a registry
push atomic together — timestamp capture has no such paired state; a single atomic RMW is sufficient).
Initialized from `Manifest.commit_time_high_water` (§4) on `Dataset::open`, so this floor survives a
restart — the spec's "crash-consistent" requirement.

**Layer 2 — commit-order (version-order) monotonicity, inside `commit_lock`.** Issuance order can
still diverge from commit order under genuine concurrency (transaction A issues before B, but B's
`write_phase` finishes and reaches the lock first) — Layer 1 alone doesn't close that. See §4.

---

## 4. `Manifest.commit_time_high_water: i64`

```rust
pub struct Manifest {
    // ...existing fields...
    #[serde(default)]
    pub commit_time_high_water: i64,
}
```

Updated inside `commit_lock`, once per commit: `manifest.commit_time_high_water =
manifest.commit_time_high_water.max(this_transaction's_captured_timestamp)`. `#[serde(default)]`,
matching `tombstones`/`next_attempt_id`'s existing pattern so older manifests still deserialize.

**Deliberately not named `max_timestamp`.** After a genuine commit-order inversion, this value can
exceed the highest `_timestamp` any individual row actually carries in that version — it is the
commit-order-monotone *envelope* of captured commit times, not a claim about any specific row. A name
implying "the max row timestamp" would invite exactly the false invariant this design is careful not
to promise.

**Why keep it at all, rather than only documenting the per-row laxity (YAGNI check):** W2's own
required test is "monotonicity across commits." Without this field, there is no artifact for which
that property is actually true by construction and therefore actually testable — per-row values
can't satisfy it under a race, by this design's own admission. This field is also the only per-version
time record that will exist once this lands: data-file timestamps live inside file bodies, but this is
manifest metadata, giving "which version was current at time T" as a lookup over `_versions/*.manifest`
filenames with no data-file reads at all — the natural building block for the time-travel-by-timestamp
read interface `how-strata-works.md` §12.4 already anticipates. Eight bytes on a manifest that already
carries the full data-file list is not a real cost.

---

## 5. Fix required: `cast_batch_to_schema`'s hidden-column handling

`crates/txn/src/dataset.rs`'s `cast_batch_to_schema` currently hardcodes an assumption of *at most one*
hidden column (`_row_id`), matched by **position**: `logical = physical - 1` iff `_row_id` is present
in the physical batch but not requested in the caller's logical schema. With `_timestamp` added as a
second hidden column, this breaks in two ways: the ordinary case (neither hidden column requested)
computes `logical = physical - 2 != physical - 1`, misfires the existing single-hidden-column
correction, and returns a spurious `SchemaMismatch`; and the mixed case (one hidden column requested,
the other not) can **silently miscast** — a caller requesting `_timestamp` back but not `_row_id` would
have `_timestamp`'s field zipped positionally against `_row_id`'s array, and `UInt64→Int64`... wait,
both are now `Int64`-typed hidden columns after §2's fix, so a mismatched pairing would not even trip a
type-cast error — it would silently swap which physical column backs which logical field. This is a
real, silent-corruption-shaped bug, not a hypothetical.

**Fix:** stop matching hidden columns by position. Identify `_row_id` and `_timestamp` by **name** in
the physical schema, exclude them from the positional zip entirely, and reattach only the ones the
caller's logical schema actually requested, by name. This generalizes cleanly to a third hidden column
later (S2/Phase B may add more manifest-adjacent metadata) without repeating this bug.

---

## 6. Why per-row cross-version laxity doesn't weaken pruning correctness

Concrete check: File A (manifest version N, `_timestamp` stats `{min:200, max:200}`) and File B
(version N+1, `{min:150, max:150}`) — the "inverted" case §1 accepts as possible. Predicate
`_timestamp >= 180`. `should_scan_file` (`crates/query/src/predicate.rs`) is a pure per-file min/max
overlap test — it takes a file's own stats and a predicate, nothing else; manifest version is not an
input and cannot influence the decision. File A: `180 <= 200` → scan. File B: `180 <= 150` is false →
skip. Correct, regardless of which file is the newer version. The same argument holds unchanged for a
future W4 per-segment zone map — same overlap test, different stats container. Pruning correctness
depends on each file's own stats being an honest record of its own rows, which this design guarantees
(one clock read per transaction, applied to every row in it) — it does not depend on files being in
strict timestamp order relative to their version number.

Checked for any other place version order is assumed to track time order: `Snapshot`, `CommitLog`, and
conflict detection are entirely row-id/version-space, with zero references to wall-clock time anywhere
in the engine today. `how-strata-works.md` §12.4's future time-travel interface describes "the store as
it was at an older *version*" — a version axis, distinct from a timestamp axis; nothing requires them
to agree row-for-row under a race, only that each axis is independently consistent, which both are.

---

## 7. `_timestamp` in `compute_stats` (reversed from the first draft)

**Included**, not excluded. The first draft mirrored `_row_id`'s exclusion from
`DataFileEntry.stats` ("not a user column subject to file-pruning stats") — wrong by analogy here,
because `_row_id` is excluded *because nothing predicates on it*, while `_timestamp` exists
specifically to be predicated on (spec's own words: "exposed for predicates and for zone maps").
Excluding it would leave `should_scan_file` failing open on every file (no stats to prune with) and,
less obviously, degrade `Snapshot::widen_ef`'s search-widening heuristic: `widen_ef` derives its
selectivity estimate from `explain`'s scanned/total file ratio, and with zero pruning signal a highly
selective temporal predicate would estimate selectivity `1.0` and under-widen `ef`, risking fewer than
`k` filtered vector-search results. Since every row in one file shares the same `_timestamp` value
(§3), computing this entry is trivial — `{min: ts, max: ts}` — not a new code path, just one more
insertion alongside the existing per-user-column stats computation.

---

## 8. Missing-column gap — resolved, moot

The gap recorded in `.claude/docs/design/phase-s1-segmented-index-spec.md` §5.2 during W1's review
(older files lacking a predicate-referenced column causing a hard error instead of graceful pruning)
does not apply here. Checked every dataset-creating code path in the repo (chaos harness, benchmarks,
the chaos worker binary, CLI tests) — every one builds a fresh directory; no persisted dataset fixtures
are checked into the repository. `_timestamp` is assigned at commit time for every row from a dataset's
very first commit once this lands — there is no "old file predates the column" scenario reachable
within one dataset's lifetime under one consistently-versioned build. The only residual is a developer
manually reusing a scratch directory across a rebuild without recreating it, which this project's
already-decided no-backward-compatibility stance (settled during W1's brainstorming) already excludes
by policy. **No special missing-column handling is built in W2.** This spec's §5.2 note should be
updated to record this as resolved, not left as an open item for a future workstream.

---

## 9. Other findings folded in

- **Dictionary encoding.** `encode_batch` runs after the hidden columns are appended and dictionary-
  encodes any column under a 0.4 distinct-value ratio. `_row_id` is unique per row (ratio 1.0, never
  encoded); `_timestamp` is constant per file (ratio `1/N`, always encoded to
  `Dictionary(Int32, Int64)`). Predicates still work unchanged — the comparison kernel unwraps
  dictionary-encoded arrays transparently — but this is a real divergence from "mirrors `_row_id`"
  worth its own round-trip test (write, close, reopen, predicate-filter) rather than only an in-memory
  batch test, since dictionary encoding only happens on the actual file-write path.
- **Name collision.** A user column literally named `_timestamp` collides the same way a user column
  named `_row_id` already could (Arrow permits duplicate field names; `index_of` returns the first
  match). Pre-existing risk class, not newly introduced — documented as a known non-goal, matching
  `_row_id`'s existing precedent, not a new check.

---

## 10. Tests

- Every row across every pending batch in one transaction shares an identical `_timestamp` (the
  single-clock-read-per-commit guarantee).
- A delete-only commit (no pending batches) still advances `Manifest.commit_time_high_water`.
- `Manifest.commit_time_high_water` is non-decreasing across a sequence of commits, including under
  concurrent commits (an integration test, not just a unit test on the field's arithmetic).
- Timestamps survive `Dataset::open` — both the per-row values (round-tripped through the actual file
  format, dictionary-encoded) and `last_issued_timestamp`'s restart floor (`commit_time_high_water`
  correctly seeds the atomic on reopen).
- A `_timestamp >= X AND category = Y` compound predicate filters correctly through
  `Snapshot::vector_search`'s `row_ids_matching` path — the exact path W1's own review found a gap in
  for multi-column predicates; this is the literal exit-criterion example both the S1 spec and W1's
  plan named but couldn't yet instantiate for lack of a timestamp column.
- `should_scan_file` prunes a file using `_timestamp` stats (file-level pruning, now real per §7 — not
  deferred to W4 as the first draft assumed).
- `cast_batch_to_schema` correctly reattaches `_row_id`, `_timestamp`, both, or neither, by name — the
  regression test for §5's fix, including the specific mixed-request case that risked silent miscasting.

No `loom` test is required for the timestamp-capture mechanism itself (no paired shared state the way
`RowIdAllocator` has — a single atomic RMW). `commit_time_high_water`'s update happens inside the
already-loom-tested `commit_lock` critical section; no new interleaving is introduced there.

---

## 11. Explicitly not decided here

- **Row-file pruning via `_timestamp`** (§7) is real and immediate; **index-segment zone-map pruning**
  is still W4's job, operating on a different data structure (index segments, not row data files) that
  doesn't exist until W3. This design only ensures W2 doesn't foreclose or complicate that later work.
- **A time-travel-by-timestamp read API** is not built here — `Manifest.commit_time_high_water` is
  laid down as the primitive a future API would need, per §4, not the API itself.
