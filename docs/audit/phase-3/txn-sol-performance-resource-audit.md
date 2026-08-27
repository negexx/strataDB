# Strata-Txn Sol Performance, Resource Footprint, and Allocation Audit

Date: 2026-08-27
Scope: `crates/txn`, relevant `crates/storage` paths, and existing benchmark evidence  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 1 head `bcb6d4b`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** No P0 performance defect was confirmed.
The filtered-vector cache remediation is implemented and measured. The
row-ID reservation catalog and whole-dataset compaction retain documented
scalability limits; neither is claimed as a universal RAF/SAF or latency
guarantee, and durable-format redesign remains a separate product decision.

The attached performance criteria were applied: write amplification factor
(WAF), read amplification factor (RAF), space amplification factor (SAF),
allocation frequency/site, heap growth, cache behavior, CPU hot paths,
microbenchmark evidence, and cache-locality evidence.

## Findings

### [Named limit] Row-ID allocation has quadratic recovery metadata reads

Locations:

- [`crates/txn/src/row_id.rs:189`](../../../crates/txn/src/row_id.rs#L189)
- [`crates/storage/src/row_id_high_water.rs:111`](../../../crates/storage/src/row_id_high_water.rs#L111)
- [`crates/storage/src/row_id_high_water.rs:289`](../../../crates/storage/src/row_id_high_water.rs#L289)
- [`crates/txn/src/vacuum.rs:91`](../../../crates/txn/src/vacuum.rs#L91)
- [`crates/txn/src/lifecycle.rs:20`](../../../crates/txn/src/lifecycle.rs#L20)

Each inserting transaction persists a durable high-water reservation. The
operation lists and reads every immutable reservation record before creating
another record. This produces O(reservation records) metadata reads per inserting attempt and
O(attempts²) cumulative metadata reads, plus one permanently retained metadata
object per inserting transaction. Vacuum scans only `data/`, and lifecycle
accounting excludes row-ID records.

This is a confirmed scalability limitation of the current immutable
reservation format, not a correctness defect. Recovery and allocation remain
bounded by the documented local/shared-handle contract. Replacing the
immutable reservation catalog with a compacted or indexed format requires a
separate durability/on-disk-compatibility design and is not silently changed
by this audit.

### [Resolved P1] Filtered vector query path uses the live-set cache

Locations:

- [`crates/txn/src/snapshot.rs:1082`](../../../crates/txn/src/snapshot.rs#L1082)
- [`crates/txn/src/snapshot.rs:1146`](../../../crates/txn/src/snapshot.rs#L1146)
- [`crates/txn/src/snapshot.rs:1251`](../../../crates/txn/src/snapshot.rs#L1251)
- [`crates/storage/src/datafile.rs:559`](../../../crates/storage/src/datafile.rs#L559)
- [`crates/storage/src/backend/local.rs:287`](../../../crates/storage/src/backend/local.rs#L287)

`vector_search_query` rebuilds filtered row IDs on every invocation, while
the older predicate-based `vector_search` uses `LiveSetCache`. The projected
reader loads the complete object into a `Vec<u8>` and local reads use
`fs::read`; projection reduces decoding but not physical bytes read.

The filtered query path now keys and reuses the bounded per-snapshot live-set
cache. Existing projected reads still decode from complete backend payloads;
the cache removes repeated live-set computation, not physical backend I/O that
the storage abstraction does not expose as a range read.

### [Named limit] Compaction is stop-the-world and whole-dataset materializing

Locations:

- [`crates/txn/src/dataset.rs:479`](../../../crates/txn/src/dataset.rs#L479)
- [`crates/txn/src/dataset.rs:475`](../../../crates/txn/src/dataset.rs#L475)
- [`crates/txn/src/dataset.rs:598`](../../../crates/txn/src/dataset.rs#L598)
- [`crates/txn/src/dataset.rs:632`](../../../crates/txn/src/dataset.rs#L632)

Compaction holds lifecycle exclusivity and the commit lock while it reads all
surviving batches, concatenates and re-encodes the complete live dataset,
publishes replacements, validates protected history, and deletes old objects.

Retained Phase 3 cloud evidence reports a 79.49-second compaction median with
1,289.2 MB peak live memory, and a 74.16-second maintenance median with
1,090.4 MB peak live memory. These passed a comparison gate but are not
product SLOs. Snapshot protection and lifecycle exclusivity preserve
correctness; incremental compaction is outside this bounded closeout.

## Amplification measurement status

No defensible numeric WAF, RAF, or SAF is currently available:

- Manifest evidence measures newest-manifest size and wall time, not cumulative
  bytes written.
- Recovery accounting measures payload bytes read during `open`, but lacks a
  logical-request denominator.
- Lifecycle inventory measures manifest/data object bytes but excludes logical
  uncompressed size, filesystem allocation, and row-ID metadata.
- Projected-read evidence measures decode latency, not physical bytes read.

The existing evidence therefore supports risks and trends, not exact
amplification factors.

## Allocation, CPU, and cache evidence gaps

The counting allocator reports allocated bytes and peak-live deltas, but not
allocation count/site, fragmentation, RSS attribution, or retained allocator
arenas. No checked-in heap profile, flamegraph, hardware-counter run, L1/L2
miss measurement, or cache-line audit was found.

Graph hotspots include `Dataset::compact`, `Transaction::commit`,
`load_segments_with_owner`, and `lifecycle::collect`.

## P3 allocation/locality concerns

These are estimable risks, not measured defects:

- One `Vec<f32>` allocation/copy per vector row
  ([`dataset.rs:3392`](../../../crates/txn/src/dataset.rs#L3392)).
- Separate row-ID and timestamp vectors per batch
  ([`dataset.rs:3583`](../../../crates/txn/src/dataset.rs#L3583)).
- Batch collection followed by concatenation during scans
  ([`snapshot.rs:405`](../../../crates/txn/src/snapshot.rs#L405)).
- Boolean-mask allocation for tombstone filtering
  ([`snapshot.rs:371`](../../../crates/txn/src/snapshot.rs#L371)).
- Live-set cache accounting excludes buckets, synchronization/`Arc` headers,
  allocator metadata, and global retained memory
  ([`live_set_cache.rs:18`](../../../crates/txn/src/live_set_cache.rs#L18)).
- Hash-based tombstone checks during HNSW traversal lack cache-locality
  measurements.

## Interaction with the correctness P1

The prior manifest-sync failure can inflate WAF through duplicate retry writes,
make acknowledged logical bytes indeterminate, and leave lifecycle accounting
observing a stale shared handle. See
[`txn-sol-correctness-static-hygiene.md`](txn-sol-correctness-static-hygiene.md).

## Verification blockers

- `link.exe` is absent from `PATH`; no fresh native tests or Criterion runs
  were executed.
- No fresh heap profiles, flamegraphs, or hardware-counter runs exist.
- The pinned 100K-row fixture is unavailable locally.

The closeout records the cache implementation and retains the row-ID and
compaction trade-offs as explicit limits. Any durable reservation-format
migration or incremental compaction design requires a new Sol design and
focused benchmark evidence.

## Verification evidence

- Filtered-cache implementation and regression tests are present in the
  merged Audit 2 history.
- Current transaction tests and clippy pass at the merged Audit 1 head.
- Retained benchmark evidence reports latency, memory, and lifecycle
  measurements with their fixture and scope limitations; no universal WAF,
  RAF, SAF, allocation-site, or cache-miss claim is made.

