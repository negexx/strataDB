# Strata-Txn Sol Binary Serialization, Schema Evolution, and On-Disk Integrity Audit

Date: 2026-08-15  
Scope: `crates/txn` and its direct storage/index serialization boundaries  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**IMPLEMENTED within named bounds.** The two P1 recovery blockers are fixed;
namespace amplification and independent historical Arrow-writer evidence remain
explicit limits of this audit.

## Findings

### [Resolved P1] Historical format-v1 manifests can fail after upgrade

Locations:

- [`crates/storage/src/manifest.rs:181`](../../../crates/storage/src/manifest.rs:181)
- [`crates/storage/src/manifest.rs:272`](../../../crates/storage/src/manifest.rs:272)
- [`crates/storage/src/manifest.rs:322`](../../../crates/storage/src/manifest.rs:322)
- [`crates/storage/src/manifest.rs:457`](../../../crates/storage/src/manifest.rs:457)

A format-v1 envelope written before `committed_at_us` existed omits that field.
Current deserialization inserts zero with `#[serde(default)]`, then checksum
validation reserializes the expanded struct. The added key changes canonical
bytes, so the historical checksum may not match. A legitimate older dataset
could therefore become unavailable to a newer binary despite the documented
backward-compatible default.

Resolution: manifest recovery now computes the checksum from the parsed raw
JSON representation with only the raw `checksum` value zeroed, before typed
deserialization inserts defaults. A checksum-valid pre-`committed_at_us`
format-v1 regression is retained in `crates/storage/src/manifest.rs`.

### [Resolved P1] Recovery has allocation and stack-exhaustion paths before bounded rejection

Locations:

- [`crates/storage/src/backend/local.rs:288`](../../../crates/storage/src/backend/local.rs:288)
- [`crates/storage/src/manifest.rs:453`](../../../crates/storage/src/manifest.rs:453)
- [`crates/storage/src/manifest.rs:322`](../../../crates/storage/src/manifest.rs:322)
- [`crates/storage/src/datafile.rs:430`](../../../crates/storage/src/datafile.rs:430)
- [`crates/storage/src/datafile.rs:477`](../../../crates/storage/src/datafile.rs:477)

Manifest loading reads the complete object, parses unrestricted JSON,
deserializes and clones it, and builds another canonical JSON tree before
checksum validation. There is no maximum manifest byte size, field count,
schema length, file count, segment count, tombstone count, or string length.
Resolution: recovery rejects encoded manifests over 64 MiB, applies the
serde_json depth guard (128 levels), limits object fields and strings, and
limits manifest arrays, data files, segments, tombstones, and schema IPC bytes
before typed envelope deserialization. Arrow row decoding retains its existing
typed panic-to-corruption boundary; its allocator and nested Arrow-schema
limits remain separate datafile concerns.

### [P2] Recovery namespace and object counts are unbounded

Locations:

- [`crates/storage/src/backend/local.rs:223`](../../../crates/storage/src/backend/local.rs:223)
- [`crates/storage/src/backend/local.rs:409`](../../../crates/storage/src/backend/local.rs:409)
- [`crates/storage/src/row_id_high_water.rs:160`](../../../crates/storage/src/row_id_high_water.rs:160)

Object listing recursively collects and sorts every matching object in memory,
and row-ID recovery scans every named reservation record. Very large manifest
or high-water namespaces can impose unbounded open-time memory and work. This
reinforces the existing row-ID metadata amplification finding.

### [P2] Historical compatibility evidence remains intentionally bounded

- The pre-`committed_at_us` manifest shape is now retained as a checksum-valid
  regression in the storage test suite; it is generated from the historical
  raw envelope shape rather than checked in as a binary fixture.
- No valid Arrow row fixture from an older Arrow/Strata writer is retained.
- Segments are generated and read by the current writer/reader; no independent
  v1 golden segment exists.
- Row-ID high-water has current round trips but no historical fixture or
  version discriminator.

Current round trips alone would not detect coordinated writer/reader format
drift for Arrow objects; the manifest compatibility path now has an independent
historical-shape regression. Arrow compatibility remains delegated to the
pinned Arrow IPC implementation.

### [P3] Explicit format-evolution limits

- Manifests read envelope version 1; legacy unenveloped manifests require
  migration and future versions fail closed.
- Schema catalogs support versions 1 and 2.
- Segments read format 1.
- Row-ID records are an unversioned 8-byte value plus CRC32C.
- Arrow row compatibility is delegated to the pinned Arrow IPC implementation;
  row files have no Strata version discriminator.

These are documented format limits, not independently confirmed corruption
defects.

## Integrity conclusions

Reachable persistent objects have strong accidental-corruption checks:

- Manifest: canonical-envelope CRC32C, envelope version, filename/payload
  version identity, and unknown-field rejection.
- Arrow row files: byte length and CRC32C before decode, then physical schema,
  row count/range, uniqueness, and ownership validation
  ([`dataset.rs:3060`](../../../crates/txn/src/dataset.rs:3060)).
- Segments: manifest byte-length/metadata cross-checks, complete header/body
  CRC32C, and checked geometry/topology
  ([`dataset.rs:3200`](../../../crates/txn/src/dataset.rs:3200),
  [`segment_reader.rs:113`](../../../crates/index/src/segment_reader.rs:113)).
- Row-ID records: exact 12-byte length, payload CRC32C, and filename/payload
  identity.

CRC32C is not authentication. A writer able to rewrite payloads and recompute
dependent CRCs can produce accepted state; current documentation correctly
limits checksums to accidental/torn corruption.

## Representative mutation assessment

| Mutation | Static assessment |
|---|---|
| Skip CRC validation | Existing manifest, row-file, segment, and high-water tests should kill it |
| Accept unknown version | Version tests should kill it |
| Inflate segment length/count | Segment and catalog extent checks reject ordinary inflation |
| Bypass schema migration | Catalog, physical-schema, stale-transaction, and migration tests reject simple bypasses |
| Truncate segment | Covered by `SegmentReader` and `Dataset::open` tests |
| Inflate manifest/Arrow allocation metadata | Resource-exhaustion paths remain exposed |
| Remove pre-`committed_at_us` compatibility handling | Likely survives because no historical fixture exists |

## Verification status

Fresh focused storage verification after the implementation included 117/117
unit tests passing, including the historical checksum and recovery-bound
regressions. Full workspace gates are recorded on the implementation PR.

The implementation follows the approved Sol format/bounds design; no new
cross-process or authentication guarantee is claimed.

