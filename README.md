# Strata

Strata is an embedded, single-node Rust database for structured Arrow data and
vector embeddings. It is designed for concurrent AI-agent workloads that share
one `Dataset` handle inside one process.

Strata is a focused engine, not a general SQL database or a distributed
transaction system. Its guarantees are intentionally bounded by the local
filesystem and shared-handle concurrency model documented below.

## What is implemented

- Arrow IPC row storage with versioned, checksummed manifests.
- Immutable snapshots and snapshot-preserving reads.
- Write-write optimistic concurrency control with typed row-level conflicts.
- Atomic manifest publication for row data, tombstones, and vector segments.
- From-scratch HNSW indexing with immutable on-disk segments and fan-out search.
- Predicate filtering, zone-map pruning, grouped aggregation, and a bounded
  logical/physical query planner with `explain` output.
- Snapshot-preserving compaction, manifest retention, recognized-object vacuum,
  and lifecycle diagnostics.
- A bounded schema catalog with the supported nullable-column migration.
- Stable Rust, PyO3, and administration CLI surfaces.
- Crash/reopen, loom, fuzz, chaos, and benchmark coverage for the supported
  boundaries.

## Concurrency and durability boundary

The supported concurrency scope is one process sharing one `Dataset` handle.
Transactions capture immutable base snapshots, stage private writes, and check
write-write conflicts before publication. A clean commit publishes row and
vector-index metadata through one manifest boundary.

The isolation ceiling is snapshot isolation; the current API does not provide
full serializability or general read/write conflict validation. Opening the
same dataset independently does not create a cross-process transaction
protocol. Distributed transactions, cross-process coordination, SQL, arbitrary
schema evolution, and object-storage backends are out of scope for the current
engine.

See [`docs/status.md`](docs/status.md) for the detailed capability ledger and
named limitations.

## Architecture

| Crate | Responsibility |
|---|---|
| [`strata-storage`](crates/storage) | Arrow files, manifests, statistics, schema metadata, and local filesystem persistence |
| [`strata-txn`](crates/txn) | Datasets, snapshots, transactions, row IDs, OCC, tombstones, commit ordering, and publication |
| [`strata-index`](crates/index) | HNSW construction/search, immutable segment encoding, validation, and loading |
| [`strata-query`](crates/query) | Predicates, pruning, vectorized operations, grouping, and planner contracts |
| [`strata-cli`](crates/cli) | Local inspection, query, migration, recovery, retention, and evidence commands |
| [`strata-bindings`](crates/bindings) | Thin PyO3 `strata_ext` facade |

## Rust quick start

The end-to-end example creates a dataset, inserts Arrow rows with a fixed-size
vector column, commits them, scans a snapshot, and performs vector search:

```text
cargo run --example basic_usage -p strata-txn
```

The example source is [`crates/txn/examples/basic_usage.rs`](crates/txn/examples/basic_usage.rs).
For library use, add the workspace crate from source and work through
`strata_txn::Dataset`, `Snapshot`, and `Transaction`.

## CLI

Build and invoke the local administration tool with Cargo:

```text
cargo run -p strata-cli -- help
cargo run -p strata-cli -- inspect ./my-dataset --json
cargo run -p strata-cli -- schema ./my-dataset --json
cargo run -p strata-cli -- explain ./my-dataset --json
cargo run -p strata-cli -- manifest-status ./my-dataset --json
cargo run -p strata-cli -- recovery-status ./my-dataset --json
cargo run -p strata-cli -- evidence --json
```

The CLI has stable human and JSON output for inspection, schema, planned
explain, migration, manifest, recovery, query, and evidence operations. Local
mutation commands retain explicit single-writer acknowledgement requirements.

## Python

The PyO3 extension is packaged as `strata_ext` through maturin:

```text
python -m venv .venv
.venv\Scripts\activate
python -m pip install maturin
maturin develop --release
```

The Python facade supports dataset creation/open, immutable snapshots, bounded
transaction overlays, Arrow IPC batch insertion, commits, vector search, and
the supported nullable-column migration. A minimal setup looks like:

```python
from pathlib import Path
import strata_ext

dataset = strata_ext.Dataset.create(
    Path("my-dataset"),
    [
        ("id", "int64", False),
        ("embedding", "vector[3]", False),
    ],
)
print(dataset.api_version())
```

Python transaction reads are deliberately bounded. After staged writes,
vector search returns a typed unsupported-operation error rather than exposing
stale index results. The facade does not add cross-process coordination or
serializability.

## Verification

Common local checks are:

```text
cargo check --workspace --no-default-features
cargo test --workspace --no-default-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo doc --workspace --no-deps
```

Additional loom, fuzz, chaos, packaging, and benchmark commands are recorded
in [`docs/phase-3-verification-report.md`](docs/phase-3-verification-report.md).

## Project guidance

- [`docs/architecture.md`](docs/architecture.md) - component boundaries and current behavior
- [`docs/design.md`](docs/design.md) - active implementation design
- [`docs/decisions.md`](docs/decisions.md) - governing decisions and deferred coordination seam
- [`docs/status.md`](docs/status.md) - capability ledger and limitations
- [`docs/roadmap.md`](docs/roadmap.md) - phase ordering and future work
- [`AGENTS.md`](AGENTS.md) - repository workflow and engineering rules

Strata is currently version `0.1.0` and should be evaluated within the named
local, single-process boundaries rather than as a production distributed
database.
