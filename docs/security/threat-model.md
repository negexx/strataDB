# Strata threat model

Status: implemented within the embedded local/shared-handle boundary

Strata is an embedded, single-node Rust engine. The supported trust boundary
assumes the application owns the dataset directory and controls its local
process. Strata does not provide a network listener, authentication,
authorization, tenant isolation, IPC coordination, or a hostile-filesystem
defense boundary.

## Enforced controls

- Storage keys reject traversal, ambiguous separators, unsafe components, and
  symlinked roots/nested components before object access.
- Manifest, row-file, segment, and row-ID objects validate lengths, versions,
  checksums, ownership, ranges, and schema/topology before becoming reachable.
- Manifest recovery performs bounded encoded-size, depth, field, string,
  array, and node preflight before constructing the raw JSON tree.
- Transaction and lifecycle failures are typed; the Audit 10 event journal is
  redacted and never stores paths, schemas, row IDs, credentials, or caller
  strings.
- `strata-txn` and `strata-storage` forbid new unsafe Rust code at their crate
  boundaries. The workspace audit retains the unsafe inventory gate.

## Explicit non-goals

CRC32C detects accidental corruption and torn writes; it is not authentication.
Strata does not encrypt data at rest, manage keys, rotate credentials, or
zeroize secrets because no secret-bearing API or product requirement exists.
An actor able to rewrite the dataset and recompute checksums is trusted outside
the supported boundary. Race-safe handle-relative filesystem primitives,
authenticated encryption, and remote/tenant security require a new product
decision and design rather than a silent guarantee in the embedded core.
