# Strata-Txn Sol Cryptographic, Attack Surface, and Threat Model Audit

Date: 2026-08-15  
Scope: `crates/txn` and direct storage/bindings boundaries  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**REJECT** for enterprise/adversarial-storage security claims. No P0 was
found, and there is no network-facing service, but local filesystem race
containment, tampered-input DoS, and authenticated confidentiality/integrity
are unresolved.

## Findings

### [P1] Conditional filesystem TOCTOU can clobber an outside file

Locations:

- [`local.rs:107`](../../../crates/storage/src/backend/local.rs#L107)
- [`local.rs:166`](../../../crates/storage/src/backend/local.rs#L166)
- [`local.rs:330`](../../../crates/storage/src/backend/local.rs#L330)
- [`local.rs:517`](../../../crates/storage/src/backend/local.rs#L517)

`LocalFs` checks symlinks before later path-based operations. Temporary names
are predictable from PID/counter and opened with `File::create`, which follows
a pre-positioned symlink. A user able to modify the dataset directory could
race or pre-create a symlink and cause the process to overwrite a file outside
the dataset under its own privileges. Static symlink tests do not cover
race-safe handle-relative/no-follow operations.

### [P1] Tampered input can cause process denial of service

Locations:

- [`local.rs:288`](../../../crates/storage/src/backend/local.rs#L288)
- [`manifest.rs:447`](../../../crates/storage/src/manifest.rs#L447)
- [`dataset.rs:3060`](../../../crates/txn/src/dataset.rs#L3060)
- [`dataset.rs:3200`](../../../crates/txn/src/dataset.rs#L3200)
- [`datafile.rs:410`](../../../crates/storage/src/datafile.rs#L410)
- [`bindings/lib.rs:476`](../../../crates/bindings/src/lib.rs#L476)

Recovery loads complete objects before all bounds are validated. Manifest JSON
is materialized multiple times; Arrow input retains allocation-failure and
nested-schema stack-overflow paths. The Python IPC parser lacks the storage
reader's input-size/depth limit and panic conversion. Exploitability requires
storage modification or a hostile in-process caller; there is no remote
endpoint.

### [P1] Authenticated encryption and tamper authentication are absent

Data, manifests, and segments are plaintext. There is no TDE, AEAD/MAC,
signature, key management, rotation, password handling, secret storage, or
zeroization. CRC32C detects accidental corruption but is forgeable by a writer
able to rewrite payloads and checksums. This is outside current prototype
scope, but blocks confidentiality and authenticated-tamper claims.

### [P2] Trusted-filesystem and privilege model is incomplete

The documentation limits Strata to embedded local use but does not explicitly
require exclusive OS ownership of the dataset tree or define behavior under a
malicious concurrent filesystem actor. Custom backends rely on callers honoring
durability and namespace contracts.

### [P2] Unsafe-code regression gate is absent

No direct unsafe/native FFI block was found in `txn`, `storage`, `bindings`, or
CLI, but the workspace does not forbid unsafe code for these crates. A future
unsafe API could compile without a dedicated security gate.

### [P3] Diagnostic leakage and panic-hook noise

CLI and Python errors can expose filesystem paths, schema descriptions, row
IDs, versions, and corruption details. Caught Arrow panics still invoke the
process-global panic hook. No passwords or keys exist to leak, so impact is
primarily deployment metadata exposure.

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
- Removing row-ID/range bounds is likely detected; manifest/object-size and
  Arrow nesting-depth bounds are absent.

No files were edited by the Sol reviewer. Remediation requires design decisions
for trusted-filesystem assumptions, race-safe file primitives, bounded
decoding, and whether authenticated integrity/encryption belongs in product
scope.

