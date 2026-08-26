# Audit 9 Serialization, Schema, and Integrity Design

**Status:** Approved implementation slice

## Goal

Make recovery fail closed on oversized manifest input and preserve checksum
compatibility for historical format-v1 manifests whose defaulted fields were
absent when they were written.

## Scope

The change is limited to `crates/storage` manifest decoding and its focused
tests/evidence. It does not change the durable manifest format, add a
dependency, introduce authentication, or implement cross-process
coordination.

## Design

1. Read the manifest as a JSON value and validate the envelope's required
   scalar fields and the raw checksum representation before converting it to
   `ManifestEnvelope`. The checksum input is the canonicalized parsed JSON
   object with only the `checksum` value replaced by zero. This preserves the
   presence/absence of legacy defaulted fields while remaining independent of
   JSON object insertion order.
2. Apply bounded-input validation in a streaming JSON preflight before parsing
   into `serde_json::Value`. The preflight enforces the 128-level depth,
   4,096-object-field, 1 MiB-string, field-specific array, and 1,000,000-node
   limits while tokens are visited, so compact high-cardinality arrays cannot
   expand into an unbounded raw JSON tree. The subsequent 64 MiB encoded-input
   and typed collection limits remain resource guards, not format-version
   changes; exceeding one returns `CorruptManifest`.
3. After the raw checks pass, deserialize with the existing
   `deny_unknown_fields` envelope and run the existing format, filename,
   schema-catalog, and Arrow-schema validation. New writers continue to use the
   existing canonical typed envelope bytes.
4. Keep the known recovery namespace amplification finding explicit: object
   listing and row-ID reservation scans remain bounded only by the documented
   local operational workload, because imposing an arbitrary global object
   count would change the supported lifecycle/storage semantics in this slice.

## Invariants

- A checksum-valid historical manifest remains readable without being
  rewritten or default-expanded for checksum calculation.
- Unknown envelope or manifest fields remain rejected.
- No typed manifest collection or schema byte vector is materialized until
  the encoded input and raw JSON shape pass the resource guards.
- Current manifests retain byte-for-byte writer compatibility.
- Corrupt or over-limit input produces a typed error, never a successful
  recovery state.

## Evidence

Tests will retain a checksum-valid pre-`committed_at_us` JSON envelope, test
the raw checksum against reordered keys, exercise each resource-bound class,
and prove current round-trip, unknown-field, unsupported-version, and checksum
failure behavior remains intact. The Audit 9 record will distinguish these
implemented bounds from the intentionally retained namespace limitation.
