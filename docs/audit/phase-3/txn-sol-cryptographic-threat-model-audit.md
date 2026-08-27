# Strata-Txn Sol Cryptographic, Attack Surface, and Threat Model Audit

Date: 2026-08-27
Scope: `crates/txn` and direct storage/bindings boundaries  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: merged Audit 10 head `c1e2d38`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The supported embedded threat model is now
explicit and enforced by existing path/format/bounds controls, crate-level
`forbid(unsafe_code)` gates, and redacted operational events. Enterprise
authenticated storage, encryption, hostile-filesystem race containment, and
tenant/network security remain explicitly outside the product boundary.

## Findings

### [Named limit] Hostile-filesystem TOCTOU is outside the supported boundary

Locations:

- [`local.rs:107`](../../../crates/storage/src/backend/local.rs#L107)
- [`local.rs:166`](../../../crates/storage/src/backend/local.rs#L166)
- [`local.rs:330`](../../../crates/storage/src/backend/local.rs#L330)
- [`local.rs:517`](../../../crates/storage/src/backend/local.rs#L517)

The trusted-filesystem assumption is now explicit in
[`docs/security/threat-model.md`](../../security/threat-model.md). Existing
lexical and static symlink containment controls remain useful, but the local
backend does not claim to defend against a malicious concurrent actor racing
path-based operations. Race-safe handle-relative/no-follow primitives require
a separate portability and product decision.

### [Resolved P1] Recovery input has bounded preflight within the current scope

Locations:

- [`local.rs:288`](../../../crates/storage/src/backend/local.rs#L288)
- [`manifest.rs:447`](../../../crates/storage/src/manifest.rs#L447)
- [`dataset.rs:3060`](../../../crates/txn/src/dataset.rs#L3060)
- [`dataset.rs:3200`](../../../crates/txn/src/dataset.rs#L3200)
- [`datafile.rs:410`](../../../crates/storage/src/datafile.rs#L410)
- [`bindings/lib.rs:476`](../../../crates/bindings/src/lib.rs#L476)

Manifest recovery rejects oversized/deep/high-cardinality input before raw JSON
materialization. Row/segment readers retain typed length, checksum, schema,
range, and topology checks. Python and Arrow allocator/stack limits remain
delegated to the pinned Arrow implementation and are not claimed as a general
hostile-input sandbox.

### [Named limit] Authenticated encryption and tamper authentication are not a product claim

Data, manifests, and segments remain plaintext, and CRC32C remains an
accidental-corruption check rather than authentication. No secret-bearing API
exists, so key management, rotation, password handling, zeroization, and TDE
are deliberately not introduced. Authenticated confidentiality requires a
superseding product decision; this audit makes no such claim.

### [Resolved P2] Trusted-filesystem and privilege assumptions are documented

The threat-model document explicitly requires application ownership of the
dataset tree and states that malicious concurrent filesystem actors are outside
the supported boundary. Custom backends retain their caller-owned durability
and namespace contracts.

### [Resolved P2] Unsafe-code regression gate is enforced

No direct unsafe/native FFI block is present in the audited engine crates, and
`strata-txn` plus `strata-storage` now use crate-level `forbid(unsafe_code)`.
CI clippy/build gates and the retained unsafe inventory cover regressions in the
supported transaction/storage boundary.

### [Named limit] Diagnostics remain caller-visible, while operational events are redacted

CLI and Python errors may expose filesystem paths, schema descriptions, row IDs,
versions, and corruption details to their immediate caller. No passwords or
keys exist to leak. Operational events intentionally store only allow-listed
kind/outcome/sequence fields, avoiding a second diagnostic leakage channel.

## Scope and positive controls

There is no IPC, RPC, socket, HTTP, authentication, authorization, tenant, or
server privilege boundary. `--ack-single-writer` is an operational
acknowledgement, not authentication.

Positive controls include lexical key validation, traversal rejection, static
symlink rejection, manifest containment, format/version/filename identity,
canonical CRCs, unknown-field rejection, row-file and segment consistency
checks, unreasonable row-ID caps, PyO3 GIL release around storage/query work,
and typed corruption/conflict categories.

No application secret, password, token, cryptographic key, nonce, or
security-sensitive randomness was found.

## Mutation assessment

- Bypassing lexical path validation is likely killed by existing tests, but
  race/temp-symlink bypasses are not covered.
- Disabling checksum comparisons is likely killed by corruption regressions.
- Recomputing CRC after tampering succeeds by design; CRC is not authentication.
- Leaking sensitive errors and exposing a new unsafe API would likely survive.
- Removing row-ID/range bounds or manifest/object-size/depth bounds is likely
  detected by the existing corruption and recovery regressions. Arrow's own
  allocator and nesting limits remain an external dependency boundary.

The implementation records the trusted-filesystem assumption, enforces the
bounded recovery checks already in scope, and adds crate-level unsafe-code
regression gates. Race-safe file primitives and authenticated
integrity/encryption remain explicit future product decisions.

## Verification evidence

The implementation branch must retain fresh evidence for:

- `cargo fmt --check`
- targeted `strata-txn` and `strata-storage` test suites
- targeted clippy with `-D warnings`
- `git diff --check`

These checks validate the new unsafe-code gates and preserve the existing
path, corruption, bounds, and redaction regressions; they do not expand the
supported threat model beyond one local process and an application-owned
dataset tree.

