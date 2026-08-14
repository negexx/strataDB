# ARCH-08 CLI Snapshot Labels Design

**Status:** Approved for implementation

## Goal

Ensure read-only CLI output labels describe the exact immutable snapshot whose rows or counts are displayed, rather than a later `Dataset` version.

## Current problem

The `scan` and `inspect` commands open a `Dataset`, create a snapshot, and read from it, but print `Dataset::current_version()`. A later commit can advance the dataset after the snapshot is captured, producing a version label that does not describe the displayed data. The `filter` command also reads from a snapshot but has no version label.

## Design

Expose the immutable snapshot version through a read-only `Snapshot::version()` accessor. For `scan` and `inspect`, bind one `Snapshot` value before reading and use that same value for both the query and the version label:

```rust
let snapshot = ds.snapshot();
let version = snapshot.version();
let batch = snapshot.scan(&schema)?;
```

The existing output formats remain unchanged except that the version value comes from the captured snapshot. Mutation output, including `committed version ...`, continues to report the version returned by the committing dataset.

`filter` remains a compatibility command and keeps its existing output format because its current contract does not expose a version label. Its read path will still use one captured snapshot for the scan and filtering operation.

## Testing

Add a transaction-level regression for the read-only accessor. Add CLI read-path regressions that create a dataset, capture a known snapshot, advance the same dataset independently, and verify the scan/inspect summaries use the captured snapshot version and row count together. Preserve existing output assertions for the compatibility commands and run targeted plus workspace checks.

## Boundaries

- This is a local CLI presentation correction only.
- It does not add cross-process coordination, serializability, or a read/write transaction API.
- It does not change snapshot isolation, row visibility, or dataset version allocation.
- The pre-existing single-process/shared-`Dataset` boundary remains in force.
