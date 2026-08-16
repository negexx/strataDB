# Strata-Txn Sol Performance, Resource Footprint, and Allocation Audit

Date: 2026-08-15  
Scope: `crates/txn`, relevant `crates/storage` paths, and existing benchmark evidence  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 performance defect was confirmed, but three P1
performance/resource risks remain. The prior P1 indeterminate manifest
publication finding also blocks approval.

The attached performance criteria were applied: write amplification factor
(WAF), read amplification factor (RAF), space amplification factor (SAF),
allocation frequency/site, heap growth, cache behavior, CPU hot paths,
microbenchmark evidence, and cache-locality evidence.

## Findings

### [P1] Row-ID allocation has quadratic metadata-read growth

Locations:

- [`crates/txn/src/row_id.rs:182`](../../../../crates/txn/src/row_id.rs:182)
- [`crates/storage/src/row_id_high_water.rs:111`](../../../../crates/storage/src/row_id_high_water.rs:111)
- [`crates/storage/src/row_id_high_water.rs:289`](../../../../crates/storage/src/row_id_high_water.rs:289)
- [`crates/txn/src/vacuum.rs:91`](../../../../crates/txn/src/vacuum.rs:91)
- [`crates/txn/src/lifecycle.rs:20`](../../../../crates/txn/src/lifecycle.rs:20)

Each inserting transaction persists a durable high-water reservation. The
operation lists and reads every immutable reservation record before creating
another record. This produces O(commits) metadata reads per insert and
O(commits²) cumulative metadata reads, plus one permanently retained metadata
object per inserting transaction. Vacuum scans only `data/`, and lifecycle
accounting excludes row-ID records.

This is a confirmed RAF/SAF scalability issue. It requires a durability and
on-disk-format design; Terra must not change it independently.

### [P1] Filtered vector query path bypasses the live-set cache

Locations:

- [`crates/txn/src/snapshot.rs:1082`](../../../../crates/txn/src/snapshot.rs:1082)
- [`crates/txn/src/snapshot.rs:1146`](../../../../crates/txn/src/snapshot.rs:1146)
- [`crates/txn/src/snapshot.rs:1251`](../../../../crates/txn/src/snapshot.rs:1251)
- [`crates/storage/src/datafile.rs:559`](../../../../crates/storage/src/datafile.rs:559)
- [`crates/storage/src/backend/local.rs:287`](../../../../crates/storage/src/backend/local.rs:287)

`vector_search_query` rebuilds filtered row IDs on every invocation, while
the older predicate-based `vector_search` uses `LiveSetCache`. The projected
reader loads the complete object into a `Vec<u8>` and local reads use
`fs::read`; projection reduces decoding but not physical bytes read.

Repeated filtered queries therefore incur full-file I/O and temporary
allocations despite an existing per-snapshot cache.

### [P1] Compaction is stop-the-world and whole-dataset materializing

Locations:

- [`crates/txn/src/dataset.rs:446`](../../../../crates/txn/src/dataset.rs:446)
- [`crates/txn/src/dataset.rs:475`](../../../../crates/txn/src/dataset.rs:475)
- [`crates/txn/src/dataset.rs:598`](../../../../crates/txn/src/dataset.rs:598)
- [`crates/txn/src/dataset.rs:632`](../../../../crates/txn/src/dataset.rs:632)

Compaction holds lifecycle exclusivity and the commit lock while it reads all
surviving batches, concatenates and re-encodes the complete live dataset,
publishes replacements, validates protected history, and deletes old objects.

Retained Phase 3 cloud evidence reports a 79.49-second compaction median with
1,289.2 MB peak live memory, and a 74.16-second maintenance median with
1,090.4 MB peak live memory. These passed a local comparison gate but are not
product SLOs.

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
  ([`dataset.rs:3392`](../../../../crates/txn/src/dataset.rs:3392)).
- Separate row-ID and timestamp vectors per batch
  ([`dataset.rs:3583`](../../../../crates/txn/src/dataset.rs:3583)).
- Batch collection followed by concatenation during scans
  ([`snapshot.rs:405`](../../../../crates/txn/src/snapshot.rs:405)).
- Boolean-mask allocation for tombstone filtering
  ([`snapshot.rs:371`](../../../../crates/txn/src/snapshot.rs:371)).
- Live-set cache accounting excludes buckets, synchronization/`Arc` headers,
  allocator metadata, and global retained memory
  ([`live_set_cache.rs:18`](../../../../crates/txn/src/live_set_cache.rs:18)).
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

No files were edited by the Sol reviewer. The report is an audit record only;
no optimization should be implemented until Sol produces a focused design and
plan for the P1 durability/resource issues.

