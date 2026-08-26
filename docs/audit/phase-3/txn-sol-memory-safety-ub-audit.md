# Strata Memory Safety and Undefined Behavior Audit

Date: 2026-08-15  
Scope: whole repository unsafe code, with focus on `crates/txn`,
`crates/storage`, and `crates/index`  
Reviewer: Sol (`gpt-5.6-sol`), independent read-only review  
Baseline: `codex/readme-current-state` at `e7a4bee`

## Verdict

**APPROVE**, with evidence limitations. No P0/P1 issue or confirmed undefined
behavior was found at the audited head.

## Findings

### [P2] Ownership uniqueness is not type-enforced

Locations:

- [`crates/index/src/node.rs:16`](../../../crates/index/src/node.rs#L16)
- [`crates/index/src/node_table.rs:253`](../../../crates/index/src/node_table.rs#L253)
- [`crates/index/src/graph.rs:566`](../../../crates/index/src/graph.rs#L566)

`Node` is `Copy`, while inserting the same handle into multiple slots would
cause double reclamation when the table is dropped. The code documents this
precondition, and the inspected production path constructs one fresh node per
call and inserts it once, so the hazard is not currently reachable. Future
refactoring should make ownership linear or make the precondition unsafe and
type-enforced.

### [P2] Dynamic UB evidence is unavailable

No dynamic UB tool completed in this audit:

- Miri is unavailable on pinned Rust 1.97.1 and not installed for the checked
  nightly toolchain.
- Rudra, `cargo-rudra`, and `cargo-geiger` are unavailable.
- Nightly exposes `-Z sanitizer`, but no sanitizer run completed.
- No exhaustive dependency unsafe count is claimed without cargo-geiger.

These are evidence gaps, not demonstrated memory defects.

### [P3] Dependency unsafe inventory is incomplete

The locked graph includes `pyo3-ffi`; `mmap-rs` is reachable through the
benchmark dependency `hnsw_rs`; Arrow supplies the core buffer/IPC stack. No
dependency UB was demonstrated, but dependency-level unsafe counts remain
unverified without cargo-geiger.

## Unsafe inventory

Production project unsafe code is concentrated in the HNSW node allocator and
node table:

- Raw allocation, layout, initialization, and deallocation:
  [`node_layout.rs:177`](../../../crates/index/src/node_layout.rs#L177) and
  [`node_layout.rs:261`](../../../crates/index/src/node_layout.rs#L261).
- Manual reclamation, `Send`/`Sync`, and pointer-derived views:
  [`node.rs:34`](../../../crates/index/src/node.rs#L34),
  [`node.rs:49`](../../../crates/index/src/node.rs#L49),
  [`node.rs:77`](../../../crates/index/src/node.rs#L77),
  [`node.rs:91`](../../../crates/index/src/node.rs#L91),
  [`node.rs:111`](../../../crates/index/src/node.rs#L111), and
  [`node.rs:138`](../../../crates/index/src/node.rs#L138).
- Atomic pointer publication, `Box::from_raw`, reclamation, and insertion:
  [`node_table.rs:62`](../../../crates/index/src/node_table.rs#L62),
  [`node_table.rs:101`](../../../crates/index/src/node_table.rs#L101),
  [`node_table.rs:136`](../../../crates/index/src/node_table.rs#L136),
  [`node_table.rs:207`](../../../crates/index/src/node_table.rs#L207),
  [`node_table.rs:288`](../../../crates/index/src/node_table.rs#L288),
  [`node_table.rs:352`](../../../crates/index/src/node_table.rs#L352), and
  [`node_table.rs:370`](../../../crates/index/src/node_table.rs#L370).
- Aligned byte-slice construction:
  [`segment_format.rs:162`](../../../crates/index/src/segment_format.rs#L162).
- Benchmark-only global allocators:
  [`lifecycle_bench.rs:64`](../../../bench/benches/lifecycle_bench.rs#L64) and
  [`segment_recall_bench.rs:65`](../../../bench/benches/segment_recall_bench.rs#L65).

No project unsafe blocks, raw-pointer access, `transmute`, `MaybeUninit`,
custom core allocator, direct mmap view, or manual memory reclamation were
found in `crates/txn`, `crates/storage`, `crates/bindings`, or
`crates/chaos-worker`. Bindings use PyO3's generated FFI boundary without
project-written `extern "C"` or explicit unsafe blocks.

## Positive safety evidence

- Layout alignment and bounds derive consistently from `Layout::extend`.
- Accessor arithmetic shares the same layout helpers.
- Deallocation reconstructs the original layout from immutable initialized
  header fields.
- Published node memory is not reclaimed until exclusive table drop.
- Mutable shared fields use atomics, with targeted loom models.
- Every project unsafe region has a stated `SAFETY` invariant.
- No confirmed aliasing, out-of-bounds, use-after-free, double-free, or
  misalignment defect was found in the inspected reachable paths.

No files were edited by the Sol reviewer. Ownership hardening would require a
small approved plan, but no memory-safety implementation is required by this
audit.

