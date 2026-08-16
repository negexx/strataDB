# Strata-Txn Sol Binary Serialization, Schema Evolution, and On-Disk Integrity Audit

Date: 2026-08-15  
Scope: `crates/txn` and its direct storage/index serialization boundaries  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT.** No P0 was confirmed, but two P1 findings block approval.

## Findings

### [P1] Historical format-v1 manifests can fail after upgrade

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

Required follow-up: Sol format design to validate checksums against the original
parsed representation, preserve field presence, or define a versioned
compatibility decoder. Terra must not choose this independently.

### [P1] Recovery has allocation and stack-exhaustion paths before bounded rejection

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
An invalid-checksum manifest can exhaust memory during parsing. Checksum-valid
Arrow input can also reach documented allocation and nested-schema stack limits.

### [P2] Recovery namespace and object counts are unbounded

Locations:

- [`crates/storage/src/backend/local.rs:223`](../../../crates/storage/src/backend/local.rs:223)
- [`crates/storage/src/backend/local.rs:409`](../../../crates/storage/src/backend/local.rs:409)
- [`crates/storage/src/row_id_high_water.rs:160`](../../../crates/storage/src/row_id_high_water.rs:160)

Object listing recursively collects and sorts every matching object in memory,
and row-ID recovery scans every named reservation record. Very large manifest
or high-water namespaces can impose unbounded open-time memory and work. This
reinforces the existing row-ID metadata amplification finding.

### [P2] Historical compatibility evidence is incomplete

- No pre-`committed_at_us` manifest fixture exists.
- No valid Arrow row fixture from an older Arrow/Strata writer is retained.
- Segments are generated and read by the current writer/reader; no independent
  v1 golden segment exists.
- Row-ID high-water has current round trips but no historical fixture or
  version discriminator.

Current round trips therefore cannot detect coordinated writer/reader format
drift.

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

The Sol review completed graph/source/history inspection and `git diff --check`.
It did not complete a fresh Cargo test run before the requested early return, so
no fresh test-pass claim is made in this audit.

No files were edited by the Sol reviewer. Terra must not implement either P1
fix without a Sol format/bounds design.

