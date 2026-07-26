# Single-Allocation HNSW Node Layout — Design

**Date:** 2026-07-23
**Trigger:** An external optimization report on `crates/index/` (Section 3 of
this project's pipeline documentation) was fact-checked by a Opus 5 audit
against the real code. Two of its findings survived verification and are
independently corroborated by this project's own prior design doc
(`docs/superpowers/specs/2026-07-19-hnsw-search-performance-improvements-design.md`
§7, which already recorded a single-allocation node layout as the identified
prerequisite for any future graph-reordering work): (1) a published node at
level `L` costs **L + 4** separate heap allocations today (one `Box<Node>`,
one `Vec<f32>` for the vector, one `Vec<SlotArray>` for the layers container,
and `L + 1` separate `Box<[AtomicU64]>`s — one per `SlotArray`, since a node
participates in layers `0..=L`), fragmenting exactly the memory
`search_layer`'s hot neighbor-walk loop chases on every distance evaluation;
(2) per-node `Box::into_raw(Box::new(value))` calls in `NodeTable::insert`
touch the global allocator on every insert, a plausible (though — see below
— unmeasured for this codebase) source of lock/contention overhead under
concurrent writes.

## 1. Goal and scope

Two sequential stages, each independently shippable:

- **Stage A — single-block `Node`.** Collapse each node's own internal
  allocations (vector, layers container, per-layer slot arrays) into one
  raw allocation per node, laid out `[Header][Vector][Layer 0
  edges][Layer 1 edges]...[Layer L edges]`. Takes per-node allocation count
  from L+4 to 1. Directly fixes the fragmentation/cache-locality problem,
  which has a real, structural argument behind it independent of any
  benchmark (the same reasoning this project's own prior design doc already
  recorded).
- **Stage B — chunk-owned bump arena.** Replace even that one `alloc()`
  call with a lock-free bump-pointer claim into arena memory the node's
  chunk already owns, removing per-insert global-allocator interaction
  entirely on the hot path.

**Stage A and Stage B are calibrated differently, deliberately.** Stage A's
justification doesn't need a benchmark to be a reasonable bet — the
allocation-count reduction and the resulting cache-locality improvement are
structurally true, and this project's own design doc already reached the
same conclusion independently before this report existed. Stage B's
motivating problem — allocator lock contention under concurrent insert — is
plausible in general but was explicitly found **unmeasured for this
codebase** by the Opus 5 audit; no benchmark anywhere in this repo touches
allocator behavior on the insert path. Stage B is being built because it was
the scope explicitly chosen for this plan, not because its necessity is
proven — its implementation plan must therefore include a real concurrent-
insert-throughput benchmark (with/without the arena) as a landing
requirement, not an afterthought. If that benchmark shows no measurable win,
Stage A alone (already complete and valuable on its own by that point) is
still a fully acceptable stopping point.

**Explicitly out of scope:** physical graph reordering (Gorder/Corder/
Porder-style relocation of already-inserted nodes). This design is a
prerequisite for that work, not that work itself — reordering remains
architecturally blocked by the never-moved invariant regardless of this
change, per `2026-07-19-...-design.md` §7's own finding.

## 2. The de-risking insight: `SlotArray` becomes a borrowed view

`SlotArray`'s methods (`claim`, `clear_matching`, `occupied`) all take
`&self` — every mutation goes through interior atomics (`AtomicU64::
compare_exchange`/`load`), so `SlotArray` never actually needs *ownership*
of its slots, only shared access to them. Today it owns `Box<[AtomicU64]>`;
under this design it instead borrows `&[AtomicU64]`, a view into whichever
node's single-block allocation the slots physically live in.

This is the single fact that keeps both stages low-risk: `SlotArray`'s
public API, every one of its existing behaviors, and every atomic operation
it performs are unchanged — only who owns the backing memory changes.
`graph.rs` calls `node.layer(lc).claim(...)`, `.occupied()`,
`.clear_matching(...)` and never inspects how the returned `SlotArray` is
backed, so **`graph.rs`'s algorithm logic requires no changes** — not
`search_layer`, not `insert`, not `select_neighbors_heuristic`. Every
existing test for those functions, and every existing `SlotArray`-level
loom guarantee (the `concurrent_claim_and_shrink_never_corrupts_a_slot`
test in `slot_array.rs`), continues to hold unmodified once `SlotArray`'s
constructor changes from "allocate a boxed slice" to "borrow a slice out of
a larger block," because the slice's element type and every operation on it
are identical.

## 3. Stage A: `Node`'s raw single-block layout

### 3.1 Layout

```
[Header: row_id: u64, level: u8, mmax0: u16, mmax: u16, deleted: AtomicU8] [padding]
[Vector: dim × f32]
[Layer 0 edges: (Mmax0 + 1) × AtomicU64]
[Layer 1 edges: (Mmax + 1) × AtomicU64]
...
[Layer L edges: (Mmax + 1) × AtomicU64]
```

`row_id` is retained in the header even though `Node::row_id()` is
currently dead code (`#[allow(dead_code)]`, "not read by any production
code path yet") — 8 bytes, zero behavioral risk, and it keeps the existing
`vector_and_row_id_are_preserved` test meaningful unchanged. `level` is a
`u8` (this design reuses the same 255-max-level assumption `graph.rs`'s
`LEVEL_BITS = 8`/`LEVEL_MASK` already encodes for the entry point, so no
new precision ceiling is introduced). `deleted` becomes an `AtomicU8` (0/1)
in place of today's `AtomicBool` — behaviorally identical, `AtomicBool`
just isn't guaranteed a stable layout for manual offset composition the way
a plain integer atomic is.

**`mmax0`/`mmax` are stored in the header too, even though they're the same
value for every node in a given graph.** `layer(lc)`'s pointer arithmetic
needs each layer's slot count to find the right byte offset (layer 0 is
`Mmax0 + 1` slots, every other layer is `Mmax + 1`), and today's `Node`
gets that "for free" because each layer already has its own independently-
sized `SlotArray`. Without storing these two `u16`s, `layer()` would need
`mmax0`/`mmax` passed in as new parameters at every call site — which
`search_layer` and `k_nn_search` don't currently take at all, so that would
ripple signature changes through most of `graph.rs` and break this design's
own "zero changes to `graph.rs`'s algorithm" claim in §2. Four redundant
bytes per node is a trivial cost against that.

Byte offsets and padding are computed via `std::alloc::Layout` composition
(`Layout::new::<Header>().extend(Layout::array::<f32>(dim)?)?.0.extend(...)`
chained once per layer) — this API computes correct alignment automatically
(`AtomicU64` needs 8-byte alignment, `f32` needs 4-byte), so the
implementation does not hand-compute byte arithmetic anywhere.

### 3.2 Construction

```rust
// Sketch -- exact signatures finalized during implementation.
unsafe fn alloc_node(row_id: u64, vector: &[f32], level: usize, mmax0: usize, mmax: usize) -> *mut u8 {
    let layout = compute_node_layout(vector.len(), level, mmax0, mmax); // Layout composition
    let ptr = std::alloc::alloc(layout); // NOT alloc_zeroed -- see below
    // SAFETY: `ptr` is non-null (checked) and `layout` exactly matches the
    // field writes below; every field is written before any other thread
    // can observe `ptr` (it is not published to NodeTable/NodeArena until
    // after this function returns).
    unsafe {
        ptr::write(header_ptr(ptr), Header { row_id, level: level as u8, deleted: AtomicU8::new(0) });
        ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr(ptr), vector.len());
        for slot_ptr in all_slot_ptrs(ptr, level, mmax0, mmax) {
            ptr::write(slot_ptr, AtomicU64::new(EMPTY)); // EMPTY = u64::MAX, NOT zero
        }
    }
    ptr
}
```

**Not `alloc_zeroed`**: `SlotArray::EMPTY` is `u64::MAX`, not `0` — a
zero-filled slot would be silently misread as an occupied edge to row-id
`0`, not an empty slot. Every slot must be explicitly initialized via
`ptr::write(slot_addr, AtomicU64::new(EMPTY))`.

**No `Drop` implementation anywhere in this design.** Nodes are never freed
today — `NodeTable::insert`'s existing `Box::into_raw` is never paired with
a matching `from_raw`, so a `Node`'s owned `Vec`s are already deliberately
leaked forever, matching the whole crate's "never freed or moved"
invariant. The raw-allocation version preserves this exactly: there is
nothing to deallocate, so there is no cleanup path to get wrong, and no new
risk is introduced by *not* writing a `Drop` impl — it would be actively
wrong to add one.

### 3.3 Unsafe surface and review requirement

This introduces meaningfully more `unsafe` code than this crate has today.
Existing `unsafe` here is limited to dereferencing already-published,
never-freed pointers (`node_table.rs`'s `&*existing`/`&*new_chunk`
patterns); this design adds manual layout computation and raw pointer
writes to initialize a block before it's ever safely typed. Per this
project's own convention (every `unsafe` block needs a `// SAFETY:`
comment; this project treats `unsafe` as a signal for *extra* review
scrutiny, not a shortcut), the implementation plan must include a dedicated
soundness review of the layout/allocation/initialization code specifically
— beyond the standard reviewer pass — before this is considered done.

## 4. Stage B: the chunk-owned bump arena

`NodeTable<T>` — the existing generic chunk-directory type — is **left
completely untouched**. It's a genuinely generic utility (its own tests
exercise it with plain `u32`/`u64`, not just `Node`), and retrofitting it
for variable-size records would mean either constraining its `T` to
something like a runtime-sized-type bound (deep, likely-unstable Rust
territory) or splitting its behavior by type — both worse than just not
touching it.

Instead, a new, purpose-built `NodeArena` type replaces `Graph`'s
`nodes: NodeTable<Node>` with `nodes: NodeArena`. It reuses the same shape
`NodeTable` already established — same `CHUNK_SIZE`, same demand-allocated
chunk directory via CAS-publish-or-discard — but each chunk additionally
owns a **chain of growable bump-allocated blocks**:

```rust
struct ArenaBlock {
    data: Box<[u8]>,        // fixed-size backing storage, allocated once
    bump_offset: AtomicUsize, // next free byte offset within `data`
    next: AtomicPtr<ArenaBlock>, // chained when this block fills
}
```

A node claims space via `block.bump_offset.fetch_add(node_size, ...)`; if
the claim would exceed the block's capacity, the claiming thread allocates
and CAS-publishes a fresh `ArenaBlock`, linked via `next`, and retries —
the exact same "loser's allocation was never visible to anyone, drop it
synchronously, no reclamation needed" pattern `NodeTable::get_or_create_
chunk` already uses for chunk publication, applied one layer deeper. Blocks
are never freed or moved, consistent with every other structure in this
crate.

Each chunk's per-row-id slot (still an `AtomicPtr<u8>`-equivalent, same
count and structure as today's `AtomicPtr<Node>` array) is written once,
pointing at wherever within whichever block that row's node ended up — the
slot array itself doesn't need to know which block a node lives in, only
that the pointer, once published, is valid forever. `NodeArena::get(row_id)`
is unchanged in shape from `NodeTable::get`: two atomic loads (chunk
pointer, then slot pointer), no locking.

## 5. Testing strategy

Three new `loom` tests, each modeling one bounded primitive — matching this
crate's existing pattern (`slot_array.rs`'s `concurrent_claim_and_shrink_
never_corrupts_a_slot`, `node_table.rs`'s `concurrent_chunk_allocation_
publishes_exactly_one_chunk`) rather than attempting whole-`Graph::insert`
coverage:

1. **Bump-pointer claim races.** Multiple threads claiming space in the
   same `ArenaBlock` concurrently — proves claimed byte ranges never
   overlap.
2. **Block-publish race.** Multiple threads racing to CAS-publish a new
   `ArenaBlock` when the current one overflows — proves exactly one
   winning block is ever visible, the losers' allocations are safely
   discarded, and every subsequent claim lands in the winning block.
3. **Full-node publish visibility.** A thread that only observes a node
   through its published `AtomicPtr` must see every field fully
   initialized — header, vector, and every slot preset to `EMPTY` — never
   a partially-constructed node. This is the test most likely to actually
   catch a release/acquire ordering mistake, mirroring this project's
   existing `one_writer_store_races_safely_with_many_readers_load`-style
   coverage elsewhere.

Beyond `loom`: a real-thread stress test analogous to `graph.rs`'s existing
`concurrent_inserts_are_all_findable_afterward`, re-run against the new
storage to confirm no behavioral regression under real concurrent load; and
(Stage B specifically) a concurrent-insert-throughput benchmark, with the
arena enabled vs. disabled, as the empirical validation this design's own
§1 calibration requires before treating Stage B's motivating problem as
solved rather than merely plausible.

**Explicitly unchanged, requiring no new tests because no behavior
changes:** `HnswIndex`'s public method signatures; `graph.rs`'s algorithm
(`search_layer`, `insert`, `k_nn_search`, `select_neighbors_heuristic`) and
every existing test for it; `NodeTable<T>`'s own tests and every other
consumer of the generic type.

## 6. References

- `docs/superpowers/specs/2026-07-19-hnsw-search-performance-improvements-design.md`
  §7 — the prior, independent recording of single-allocation layout as
  graph-reordering's prerequisite, and why reordering itself stays out of
  scope here.
- `docs/superpowers/specs/2026-07-18-lockfree-hnsw-rewrite-design.md` §2 —
  the never-moved/never-freed invariant this design must preserve exactly.
- This session's Opus 5 audit of the external Section 3 optimization
  report — the source of both motivating findings (L+4 allocation count,
  the unmeasured-but-plausible allocator-contention claim) and of the
  finding that graph reordering itself remains a genuine use-after-free
  risk regardless of this change.
