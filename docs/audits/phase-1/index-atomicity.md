# Phase 1 index atomicity audit

**Date:** 2026-08-01

**Lane:** Sol — index atomicity, segment eligibility, snapshot visibility, and search correctness

**Scope:** Current working tree; single-process concurrency through one shared `Dataset` handle. This
lane inspected `SegmentEntry`/`SegmentSet`, segment serialization and recovery validation, manifest
eligibility, snapshot tombstones, vector-search fan-out/merge/filtering, row/vector commit ordering,
approximate-search caveats, and stale-row behavior. The tree was already heavily dirty across the
reviewed source and documentation. This lane changed no Rust, tests, dependencies, or configuration.

## Verdict

**BLOCKED — Phase 1 should not exit on the current index-atomicity contract.**

The normal publication path has the right structural shape. A vector-carrying commit writes row files
and one immutable segment before taking `commit_lock`; under the lock it rebuilds from the latest
snapshot, appends both row-file and segment metadata to one manifest, publishes that manifest, and
only then swaps one immutable snapshot containing the manifest, segment set, and tombstone set. Failed
or conflicting commits leave only unreferenced files, and old snapshots retain their old segment and
tombstone views.

That structure does not close an unrestricted-tombstone counterexample: a public `delete` can target
an unallocated row ID, after which an insert using that physical ID can commit successfully while both
scan and vector search hide it. Recovery also accepts multiple self-consistent segments that map the
same row ID to different vectors; merge silently chooses the query-nearest occurrence rather than
rejecting ambiguous/stale identity. Finally, `Snapshot::vector_search` fixes unfiltered `ef_search` at
32 without constraining or widening `k`, so a one-segment query asking for more than 32 results is
deterministically unable to return `k` results, while even smaller requests may underfill because the
search is approximate. The public contract does not state these cardinality boundaries.

## Findings

### IDX-01 — An unallocated tombstone can hide a later acknowledged row and its vector

- **Severity:** Critical
- **Confidence:** High
- **Affected phase:** Phase 1; violates the Phase 0 row-ID foundation
- **Disposition:** Phase 1 blocker
- **Evidence:**
  - `Transaction` retains only the begin-time version, not the begin-time snapshot needed to validate
    a delete target (`crates/txn/src/dataset.rs:556-573`).
  - `delete` accepts any caller-supplied `u64` and unconditionally appends it to both
    `pending_tombstones` and `write_set`; `insert` adds no allocated ID to the write set
    (`crates/txn/src/dataset.rs:741-755`).
  - A delete-only transaction skips row-ID allocation entirely, while the next insert claims from the
    unchanged allocator high-water mark (`crates/txn/src/dataset.rs:1196-1204`,
    `crates/txn/src/dataset.rs:1237-1249`).
  - Commit persists every pending tombstone in the same manifest that publishes new data files and a
    new segment (`crates/txn/src/dataset.rs:986-1035`, `crates/txn/src/dataset.rs:1069-1082`).
  - All row and vector read paths then treat the tombstone as authoritative: scan filters matching
    `_row_id` values, and vector search passes `is_visible == !tombstones.contains(row_id)` into graph
    traversal (`crates/txn/src/snapshot.rs:124-145`, `crates/txn/src/snapshot.rs:176-230`,
    `crates/txn/src/snapshot.rs:359-395`).
  - Current delete/update tests first commit a real target row; they do not cover missing, future, or
    same-transaction newly allocated IDs (`crates/txn/src/dataset.rs:5591-5680`).

On an empty dataset, either `delete(0)` followed by a later one-row insert, or one transaction that
calls `delete(0)` and inserts one row, publishes row 0 and its vector under a tombstone for row 0.
`commit()` returns success, yet a fresh snapshot cannot scan or search that row. In a concurrent form,
a stale delete of row 0 can begin before row 0 exists, race an insert whose write set is empty, and
commit without the typed write/write conflict expected inside the supported shared-handle boundary.

Require delete/update targets to satisfy an explicit base-snapshot contract before publication. At a
minimum, reject future/nonexistent targets with a typed error and close the stale-delete/insert conflict
hole; decide separately whether re-deleting a row already tombstoned in the base snapshot is an
idempotent success or a typed missing-row result. Add direct scan and vector-search tests for the
same-transaction and concurrent counterexamples.

### IDX-02 — Manifest recovery accepts ambiguous row-to-vector identity and merge can select stale data

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 recovery and segment eligibility; Phase 3 compaction prerequisite
- **Disposition:** Phase 1 recovery blocker; define overlap semantics before Phase 3 compaction
- **Evidence:**
  - `SegmentReader` validates section geometry, CRCs, strictly ascending row IDs, adjacency, and entry
    bounds, but it does not compare the row-ID array's first/last values with the header's
    `row_id_min`/`row_id_max` before storing those header values (`crates/index/src/segment_reader.rs:113-236`,
    `crates/index/src/segment_reader.rs:238-247`, `crates/index/src/segment_reader.rs:333-372`).
  - `load_segments` checks each `SegmentEntry` against its own segment header and enforces one common
    dimension, but does not reject duplicate segment names, duplicate row IDs across segments, row IDs
    at or above `Manifest.next_row_id`, or vector row IDs with no row-store counterpart
    (`crates/txn/src/dataset.rs:1721-1807`).
  - Fan-out maps local ordinals to global row IDs, sorts all candidates by distance, and deduplicates
    by retaining the nearest occurrence of a row ID (`crates/index/src/segment_set.rs:163-217`).
  - The unit suite deliberately constructs two segments containing the same row IDs with different
    vectors and codifies “keep the nearer occurrence” (`crates/index/src/segment_set.rs:663-718`).

Normal Phase 1 commits allocate disjoint physical row IDs, so they do not produce this overlap. A
hand-edited or logically corrupt manifest can nevertheless list two individually valid, same-dimension
segments that assign different vectors to the same row ID, and `Dataset::open` accepts them. Which
vector represents that row then changes with the query: the occurrence with the smaller distance wins.
That is silent, query-dependent stale-row resolution rather than loud corruption rejection. It is also
not a safe version-selection rule for future compaction unless overlapping copies are guaranteed
byte-identical.

At open, validate the segment identity relationships Phase 1 actually requires: header range versus
the resident row-ID array, safe/unique segment entries, each vector row ID below `next_row_id`, and no
cross-segment duplicate row IDs while compaction is absent. Decide how cheaply to prove that every
vector row ID has a row-store owner. Before compaction permits overlap, specify whether copies must be
identical or carry generation/precedence metadata; distance must not choose freshness.

### IDX-03 — `vector_search(k)` has an undocumented fixed-`ef` result-count ceiling

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 guarantee/evidence boundary and Phase 2 query API
- **Disposition:** Phase 1 contract decision required; implementation or explicit API bound before Phase 2
- **Evidence:**
  - Unfiltered `Snapshot::vector_search` always calls `SegmentSet::search` with
    `EF_SEARCH_DEFAULT == 32`, regardless of `k`; filtered search widens from the same base using a
    heuristic unrelated to `k` (`crates/txn/src/snapshot.rs:101-121`,
    `crates/txn/src/snapshot.rs:359-395`).
  - The HNSW layer-0 result heap is capped at `ef`, and the completed result is only afterward
    truncated to `k` (`crates/index/src/graph.rs:971-1045`, `crates/index/src/graph.rs:1128-1155`).
  - `SegmentSet` passes the caller's `k` and `ef_search` unchanged to every part, then merges and
    truncates globally (`crates/index/src/segment_set.rs:163-217`,
    `crates/index/src/segment_set.rs:230-244`).
  - The equivalence test intentionally exercises `k = 40, ef_search = 5` but only compares the sealed
    reader with the source graph; it does not require 40 results (`crates/index/src/segment_set.rs:436-503`).
  - The end-to-end snapshot-isolation test records observed 9/10 and 10/10-with-one-foreign-point
    underfill at `ef_search = 32` as an approximate-search recall caveat, then reduces `k` to avoid
    asserting full cluster recovery (`crates/txn/tests/concurrent_snapshot_isolation.rs:258-273`).

For a single segment containing more than 32 live rows, an unfiltered request with `k > 32` cannot
return more than 32 results even if every node is reached; with multiple segments the accidental ceiling
becomes segment-count-dependent. For `k <= 32`, approximation or filtering can still return fewer than
`k`, which may be acceptable, but it must be an explicit API guarantee rather than an inference from
the word “approximate.”

Choose and test the contract. If `k` means “request up to `k` best-effort ANN results,” document when
underfill is allowed and reject or bound unsupported values. If enough reachable/live rows should fill
the request, use an effective search width at least `k` on every searched segment, retaining an honest
recall caveat. Any HNSW parameter-policy change needs benchmark evidence under the repository rules.

### IDX-04 — The accepted ADR overgeneralizes one recall experiment

- **Severity:** Low
- **Confidence:** High
- **Affected phase:** Phase 1 documentation and Phase 3 lifecycle expectations
- **Disposition:** Documentation correction; does not reopen ADR 0008's segmented-layout decision
- **Evidence:**
  - The pre-reconciliation ADR 0008 generalized one fixed dataset/parameter experiment into “recall
    is segment-count-safe” and said a lagging compactor makes queries “slower, never wrong.” The active
    ADR now labels the result as historical workload evidence and states that compaction is not
    implemented.
  - The replacement ADR retains the caveat that the observed recall rise is an over-fetch artifact and
    that a latency-matched policy could differ.
  - The living architecture is more accurate: segmented fan-out is a mechanism for a consistent
    segment set, not proof of ANN recall, and filtered-search recall/cost remain evidence work
    (`docs/architecture.md:45-47`).
  - The end-to-end fan-out integration test requires recall of at least 0.9 on one 60-point fixture,
    not exact parity or a general segment-count theorem (`crates/txn/src/dataset.rs:4611-4747`).

Keep the segmented-layout decision, but narrow the empirical conclusion to the tested dataset,
parameters, `k`, and segment construction. “No recall cliff in this experiment” is supported; “never
wrong” is not a meaningful general guarantee for approximate search. Phase 3 compaction policy should
continue to measure recall as well as latency across representative distributions, filters, tombstone
rates, and segment sizes.

## Strengths

- Row data and index changes have one eligibility boundary. `write_phase` writes and fsyncs unique row
  and segment files before manifest publication; the locked section appends against the latest state,
  commits the manifest, then creates and stores the replacement snapshot
  (`crates/txn/src/dataset.rs:915-1156`, `crates/txn/src/dataset.rs:1191-1275`).
- A failed or conflicting commit cannot leak a searchable graph mutation because no shared mutable
  graph exists. Regression tests cover manifest-step failure, typed conflict, panic before manifest
  publication, and later successful commits (`crates/txn/src/dataset.rs:6288-6315`,
  `crates/txn/src/dataset.rs:6708-6900`).
- Segment serialization is explicit and defensive: the writer checks ascending row IDs, node/vector
  completeness, dimensions, neighbor ordinals, checked section arithmetic, alignment, and CRCs; the
  reader revalidates format, geometry, CRCs, sections, CSR offsets, neighbor bounds, and entry bounds
  (`crates/index/src/segment_writer.rs:58-296`, `crates/index/src/segment_reader.rs:113-373`).
- Recovery cross-checks manifest byte length, format, count, dimension, row-ID range, and
  cross-segment dimension before constructing a `SegmentSet`
  (`crates/txn/src/dataset.rs:1721-1807`).
- Fan-out searches every eligible part, maps local ordinals to global IDs, globally orders with
  `total_cmp`, deduplicates, and truncates. Predicate pruning skips rejected parts before traversal;
  uninterpretable zone-map payloads fail open (`crates/index/src/segment_set.rs:150-217`,
  `crates/txn/src/snapshot.rs:27-48`).
- Tombstoned nodes remain available as graph waypoints but cannot enter results. Old snapshots retain
  their own segment/tombstone state while new snapshots hide newly tombstoned rows
  (`crates/index/src/graph.rs:963-1047`,
  `crates/txn/tests/concurrent_snapshot_isolation.rs:224-271`,
  `crates/txn/tests/concurrent_snapshot_isolation.rs:392-478`).
- The loom row/segment publication model derives manifest entry count, in-memory part count, and
  search result from one captured snapshot and admits only complete pre- or post-commit states
  (`crates/txn/src/dataset.rs:7863-7984`).

## Verification evidence

Fresh focused checks against the audited working tree:

- `cargo test -p strata-index segment_set::tests --quiet` — 15 passed, 0 failed.
- `cargo test -p strata-index segment_reader::tests --quiet` — 11 passed, 0 failed.
- `cargo test -p strata-index segment_writer::tests --quiet` — 10 passed, 0 failed.
- `cargo test -p strata-txn vector_search_fan_out_matches_brute_force_ground_truth_across_overlapping_segments --quiet` — 1 passed, 0 failed.
- `cargo test -p strata-txn --test concurrent_snapshot_isolation --quiet` — 4 passed, 0 failed.

These checks validate the supported happy paths and existing regressions. They do not cover IDX-01's
future-ID tombstone, IDX-02's cross-segment identity ambiguity, or IDX-03's one-segment `k > 32`
boundary. This lane did not rerun loom; it inspected the existing model relevant to publication and
made no interleaving-sensitive source change.

## Open questions

1. Must delete/update target a row visible in the transaction's begin-time snapshot? What are the
   distinct outcomes for a missing row, an already-tombstoned row, and a numerically future row?
2. Does public `vector_search(query, k, predicate)` promise exactly `min(k, live matching rows)` when
   enough rows exist, or only an arbitrary best-effort count up to `k`? Is `k > 32` supported?
3. Which segment eligibility relationships must `Dataset::open` prove eagerly: unique names, unique
   row IDs, row IDs below `next_row_id`, header/body range agreement, and a row-store owner for every
   vector row ID?
4. When compaction introduces overlapping source/output segments, are duplicate row IDs required to
   carry byte-identical vectors, or will the format carry generation/precedence metadata? How are
   mismatches rejected rather than distance-resolved?
5. What recall evidence is required before making segment-count or filtered-ANN claims: datasets,
   dimensions, segment-size distributions, tombstone rates, predicate selectivities, `k`, and
   latency-matched `ef_search` policies?
6. Should rows with no vector or a null vector be formally documented as scan-visible but absent from
   vector search, and should recovery validate that asymmetry explicitly?
