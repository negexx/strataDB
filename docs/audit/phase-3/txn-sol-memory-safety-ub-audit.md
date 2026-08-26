# Strata Memory Safety and Undefined Behavior Audit

Date: 2026-08-20
Scope: project-owned Rust unsafe code, raw-pointer/layout handling, typed segment views, and their immediate callers; emphasis on `crates/index`, with a repository-wide source scan
Reviewer: Terra (execution worker)
Baseline: `codex/audit-memory-safety-ub` at `f88862ff8f0f3e005e10de9970b43a9b452c2e72`

## Verdict

**IMPLEMENTED WITH NAMED LIMITS.** The two P2 hardening findings are resolved
for the audited project-owned paths, and the explicit project-owned unsafe
inventory guardrail is implemented. No confirmed undefined behavior was found
in the audited reachable paths. This is a bounded source-and-test audit, not a
proof that every future caller, dependency configuration, feature, or target
is UB-free.

## Findings

### [Resolved P2] Header validation completes before raw allocation

Locations:

- [`crates/index/src/node_layout.rs:211`](../../../crates/index/src/node_layout.rs#L211)
- [`crates/index/src/node_layout.rs:212`](../../../crates/index/src/node_layout.rs#L212)
- [`crates/index/src/node_layout.rs:213`](../../../crates/index/src/node_layout.rs#L213)
- [`crates/index/src/node_layout.rs:214`](../../../crates/index/src/node_layout.rs#L214)
- [`crates/index/src/node_layout.rs:219`](../../../crates/index/src/node_layout.rs#L219)

`alloc_node` now completes and retains the fallible `u32`/`u8`/`u16`
conversions for `NodeHeader` before `alloc_node_block(layout)`, the raw
allocator at [`node_layout.rs:19`](../../../crates/index/src/node_layout.rs#L19). A conversion failure
therefore occurs before any raw block is owned, so the former panic-path
allocation leak is resolved. Normal `Graph::insert` continues to clamp
`level` before `Node::new` ([`graph.rs:564`](../../../crates/index/src/graph.rs#L564)).

This resolves the identified resource-leak path; it is not dynamic UB-tool
evidence or a proof about allocator failures outside the audited path.

### [Resolved P2] `Node` has unique allocation ownership

Locations:

- [`crates/index/src/node.rs:30`](../../../crates/index/src/node.rs#L30)
- [`crates/index/src/node.rs:34`](../../../crates/index/src/node.rs#L34)
- [`crates/index/src/node_table.rs:253`](../../../crates/index/src/node_table.rs#L253)
- [`crates/index/src/node_table.rs:259`](../../../crates/index/src/node_table.rs#L259)
- [`crates/index/src/node_table.rs:307`](../../../crates/index/src/node_table.rs#L307)

`Node` is no longer `Clone + Copy`; it uniquely owns its independently
allocated node block. `NodeTable::insert` consumes that owner and reclaims it
on insertion failure, so safe callers cannot place copies of one allocation in
multiple reclaiming slots. The production construction path continues to
create a fresh node and immediately move it into the table
([`graph.rs:565`](../../../crates/index/src/graph.rs#L565),
[`graph.rs:578`](../../../crates/index/src/graph.rs#L578)).

This resolves the identified double-reclamation ownership hazard. It does not
prove the absence of UB in every unsafe path or make a future copyable read
handle safe without a separate ownership design.

### [P3] Dynamic UB-tool evidence is unavailable

On 2026-08-23, Windows 10.0.26200.0 with Rust 1.97.1 on
`x86_64-pc-windows-msvc` had `nightly-x86_64-pc-windows-msvc` and
`nightly-2026-07-25-x86_64-pc-windows-msvc` installed, but neither had the
Miri component: both `rustup run nightly-x86_64-pc-windows-msvc cargo miri
--version` and `cargo +nightly-2026-07-25 miri --version` exited 1 with
`cargo-miri.exe` absent. `cargo rudra --version` and `cargo geiger --version`
each exited 101 because the command is not installed. These probes are
**UNAVAILABLE**, not green results.

`rustc +nightly-x86_64-pc-windows-msvc -Z help` exited 0 and lists
`-Z sanitizer`; it is compiler-capability evidence only. No AddressSanitizer,
MemorySanitizer, ThreadSanitizer, Miri, Rudra, or Geiger analysis ran in this
task. Linux sanitizer targets and compatible instrumented standard library and
dependency graph are **UNRUN** on this Windows host; MemorySanitizer is
explicitly deferred for that reason. This remains an **evidence gap**, not a
demonstrated memory defect or proof that no UB exists.

When the named tools and targets are already provisioned, the authorized
follow-up is:

```text
# Miri, only when already-installed nightly-2026-07-25 includes cargo-miri
cargo +nightly-2026-07-25 miri test -p strata-index --lib node_layout::tests::alloc_node_initializes_header_vector_and_every_slot_to_empty -- --exact
cargo +nightly-2026-07-25 miri test -p strata-index --lib node_layout::tests::dealloc_node_frees_a_block_with_multiple_layers_without_use_after_free -- --exact
cargo +nightly-2026-07-25 miri test -p strata-index --lib node_table::tests::a_row_id_past_capacity_reclaims_a_real_node_instead_of_leaking_it -- --exact
cargo +nightly-2026-07-25 miri test -p strata-index --lib node_table::tests::dropping_a_table_of_real_nodes_frees_every_node -- --exact
cargo +nightly-2026-07-25 miri test -p strata-index --lib segment_format::tests::aligned_bytes_lets_bytemuck_cast_every_typed_view_the_format_needs -- --exact

# Linux x86_64 sanitizer characterization, only with an already-installed supporting nightly/target
RUSTFLAGS="-Zsanitizer=address" cargo +nightly-2026-07-25 test -p strata-index --lib --target x86_64-unknown-linux-gnu node_layout::tests::dealloc_node_frees_a_block_with_multiple_layers_without_use_after_free -- --exact
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly-2026-07-25 test -p strata-index --lib --target x86_64-unknown-linux-gnu node::tests::full_node_publish_is_completely_visible_to_a_reader -- --exact
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly-2026-07-25 test -p strata-index --lib --target x86_64-unknown-linux-gnu node_table::tests::concurrent_chunk_allocation_publishes_exactly_one_chunk -- --exact

# Rudra, only when cargo-rudra is already installed; run from crates/index
cargo rudra

# Geiger is inventory corroboration only, never UB proof or the admission gate
cargo geiger --all-features --all-targets
```

Sanitizer support is target/toolchain-specific. Rudra and Geiger findings need
human triage, and a passing dynamic tool is not proof of UB absence; an
unavailable or unrun tool cannot close this P3.

### [Implemented P3 guardrail] Mechanical admission for project-owned explicit unsafe constructs

**Resolved within the project-owned source boundary.**
`scripts/check_unsafe_inventory.py` lexically excludes comments and Rust
normal/raw/byte strings and character literals, then requires a unique
`SAFETY[ID]` source marker and complete schema-version-1 inventory record for
each executable `unsafe` block, function, and impl. The checked inventory
contains exactly 58 sites: 43 blocks and 15 declarations across the six named
index/benchmark files. The checker fails closed for unmarked, stale, duplicate,
malformed, path/kind-mismatched, or unknown unsafe forms; it has no update or
accept mode.

The required `unsafe_inventory` CI step runs its thirteen independently scoped
fixture and repository-inventory tests before Build, and final CI provenance
records its outcome.
This is an admission guardrail only for project-owned explicit unsafe blocks,
functions, and impls. It is not a claim about dependencies, generated code,
unsafe reachable only under another feature/target, or absence of undefined
behavior.

## Reviewed safety evidence

- **Pointer/layout construction.** `NodeHeader` is `#[repr(C)]`
  ([`node_layout.rs:38`](../../../crates/index/src/node_layout.rs#L38)); one
  `Layout::extend` sequence reserves the header, vector, and atomic slots
  ([`node_layout.rs:87`](../../../crates/index/src/node_layout.rs#L87)), and
  accessor arithmetic is regression-checked against that layout
  ([`node_layout.rs:330`](../../../crates/index/src/node_layout.rs#L330)).
  `dealloc_node` rebuilds the same layout from initialized header fields
  ([`node_layout.rs:285`](../../../crates/index/src/node_layout.rs#L285)).
- **Atomic pointer lifetime/publication.** Chunks and values are published with
  `SeqCst` atomics ([`node_table.rs:223`](../../../crates/index/src/node_table.rs#L223),
  [`node_table.rs:312`](../../../crates/index/src/node_table.rs#L312)); they are
  never removed while readers can obtain references, and raw boxes are rebuilt
  only during exclusive drop ([`node_table.rs:101`](../../../crates/index/src/node_table.rs#L101),
  [`node_table.rs:136`](../../../crates/index/src/node_table.rs#L136)). The
  node-publication loom model exercises complete initialization visibility
  ([`node.rs:353`](../../../crates/index/src/node.rs#L353)).
- **Aligned bytes and typed segment views.** `AlignedBytes` uses a 64-byte
  aligned backing type and bounds its raw byte views by the recorded length
  ([`segment_format.rs:128`](../../../crates/index/src/segment_format.rs#L128),
  [`segment_format.rs:173`](../../../crates/index/src/segment_format.rs#L173)).
  The reader validates section extent/alignment before using checked
  `bytemuck::try_cast_slice` views
  ([`segment_reader.rs:179`](../../../crates/index/src/segment_reader.rs#L179),
  [`segment_reader.rs:243`](../../../crates/index/src/segment_reader.rs#L243));
  malformed/truncated input is regression-covered
  ([`segment_reader.rs:734`](../../../crates/index/src/segment_reader.rs#L734)).
- **Repository concentration.** The executable project unsafe inventory is
  limited to `crates/index`'s node allocator/table and aligned-byte helper,
  plus benchmark-only `GlobalAlloc` wrappers. No project-written production
  unsafe block was found in `crates/txn`, `crates/storage`, `crates/bindings`,
  or `crates/chaos-worker`; `dataset.rs` only mentions rejected `transmute` in
  commentary ([`dataset.rs:4188`](../../../crates/txn/src/dataset.rs#L4188)).

## Fresh verification

Luna accepts the [Task 7 provenance disposition](task-7-provenance-disposition.md):
exact pre-Task-7 bytes and hashes for the five shared files cannot be
reconstructed safely. Its forward-looking checkpoint is not labelled
pre-Task-7 and does not establish byte-for-byte preservation.

| Command | Result |
|---|---|
| `cargo test -p strata-index` | Exit 0; 160 unit tests passed, 0 failed, 1 ignored; 5 doctests passed, 0 failed. |
| `python -m unittest scripts/test_check_unsafe_inventory.py -v` | Exit 0; 13 independently scoped fixture and repository-inventory tests passed. |
| `python scripts/check_unsafe_inventory.py --root . --inventory docs/audit/phase-3/unsafe-inventory.json` | Exit 0; 58 approved constructs (43 blocks, 15 functions/impls) across 6 files. |
| `rustup run nightly-x86_64-pc-windows-msvc cargo miri --version` | **UNAVAILABLE**; exit 1, `cargo-miri.exe` is not installed. |
| `cargo +nightly-2026-07-25 miri --version` | **UNAVAILABLE**; exit 1, `cargo-miri.exe` is not installed. |
| `cargo rudra --version` / `cargo geiger --version` | **UNAVAILABLE**; each exited 101 because the command is not installed. |
| `rustc +nightly-x86_64-pc-windows-msvc -Z help` | Exit 0 and lists `-Z sanitizer`; Linux ASan/TSan and MemorySanitizer are **UNRUN**, not evidence of a sanitizer run. |

## Scope limits

This report does not audit third-party dependency internals, prove absence of
UB under all targets/features/allocator failures, or replace the separate
concurrency, durability, crash-recovery, fuzzing, and performance audits. It
does not authorize a raw-memory redesign, dependency addition, Miri/Rudra/
sanitizer installation, or changes beyond the implemented hardening recorded
above.
