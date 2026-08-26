# Audit 9 Serialization Integrity Implementation Plan

**Goal:** Make manifest recovery preserve historical checksums and reject resource-exhausting serialized input within explicit bounds.

**Architecture:** Keep the current versioned JSON envelope and typed validation. Add a raw JSON validation/checksum layer before typed deserialization, then reuse the existing `ManifestEnvelope::validate` and schema checks.

**Tech Stack:** Rust 2024, serde_json, CRC32C, Arrow IPC, Cargo tests.

**Design:** [`docs/designs/phase-3-audit9-serialization-integrity.md`](../designs/phase-3-audit9-serialization-integrity.md)

## Global Constraints

- Preserve the one-process/shared-`Dataset` boundary and current on-disk format.
- Do not add dependencies.
- Keep unknown-field rejection and typed corruption errors.
- Use TDD: each behavior test must fail before the implementation change.
- Run focused storage tests, workspace tests, clippy, format, and diff checks.

## Task 1: Add raw manifest bounds and checksum compatibility

**Files:**
- Modify: `crates/storage/src/manifest.rs`
- Modify: `crates/storage/src/error.rs` only if a more precise typed error is required; otherwise use `CorruptManifest`
- Test: `crates/storage/src/manifest.rs` unit tests

- [ ] Write a test that constructs a format-v1 envelope JSON without `committed_at_us`, computes its checksum from the exact raw representation with checksum zeroed, writes it under a numeric manifest key, and asserts `read_current` succeeds with `committed_at_us == 0`.
- [ ] Run the focused manifest test and confirm it fails because typed default insertion changes the checksum.
- [ ] Add explicit constants and a recursive JSON-shape validator for encoded byte length, depth, object member count, string length, and array length; add field-specific checks for data files, segments, tombstones, and schema IPC bytes.
- [ ] Add a raw-value canonical checksum helper that replaces only the raw `checksum` field with zero and canonicalizes object keys recursively.
- [ ] Update manifest decode to reject oversized input/shape, validate raw format/checksum, then deserialize and run existing typed validation.
- [ ] Run the compatibility and bounds tests and confirm they pass.

## Task 2: Expand regression evidence and audit record

**Files:**
- Modify: `crates/storage/src/manifest.rs` tests
- Modify: `docs/audit/phase-3/txn-sol-serialization-schema-integrity-audit.md`
- Modify: `docs/status.md` only for the capability/evidence wording required by the audit verdict

- [ ] Add tests for reordered raw object keys, representative array/field bounds, unknown fields, unsupported versions, mutated checksums, and current writer round trips.
- [ ] Run the focused storage tests and inspect failure counts.
- [ ] Update the Audit 9 verdict to `IMPLEMENTED within named bounds`, list the implemented bounds and historical fixture evidence, and retain the namespace/object-count limitation explicitly.
- [ ] Run `cargo fmt --check`, `cargo test -p strata-storage`, `cargo test --workspace --no-default-features`, `cargo clippy --workspace --all-targets -- -D warnings`, and `git diff --check`.
- [ ] Inspect the final diff and commit only the scoped files.
