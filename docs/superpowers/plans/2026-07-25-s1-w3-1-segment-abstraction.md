# S1 W3.1 — Segment Abstraction (Segment Set of One) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the `NodeSource` trait abstraction and the `SegmentSet`/`IndexPart` types as a pure, behavior-preserving refactor — `search_layer`/`k_nn_search` become generic over `NodeSource` instead of inherent `Graph<D>` methods, `Snapshot` holds a `SegmentSet` of exactly one `Live` part instead of a bare `Arc<HnswIndex>`, and the manifest gains an always-empty `segments` field — with zero observable behavior change. This is W3.1 of Phase S1's W3 migration (segment format + delta-segment writes + fan-out search); it does not write or read any `.seg` file, does not touch the commit lock, and introduces no new concurrency.

**Architecture:** `crates/index` gains a `NodeSource` trait (implemented by `Graph<D>`) that abstracts element access (neighbors, vectors, deleted flag) away from traversal logic, so `search_layer`/`k_nn_search`'s algorithm bodies move to free functions generic over `S: NodeSource` while `Graph<D>`'s existing methods become one-line delegations. A new `SegmentSet` type (holding today exactly one `IndexPart::Live`) becomes the thing `Snapshot` searches through instead of `Arc<HnswIndex>` directly. `crates/storage::Manifest` gains an empty `segments: Vec<SegmentEntry>` field so W3.2 is a write/open-path change only, not a format change.

**Tech Stack:** Rust (edition 2024), existing `strata-index`/`strata-txn`/`strata-storage` crates, `cargo test`/`clippy`/`fmt` as the verification gate. No new dependencies in this plan.

## Global Constraints

- Every task must leave `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check` green before its commit.
- **Zero behavior change**: no task in this plan may alter what any existing test asserts. Every existing test in `crates/index`, `crates/txn`, `crates/storage` must pass unmodified (not edited) at the end of every task.
- No new loom model is required for this plan (no new concurrency is introduced — see the base design doc §4/W3.1: "No new loom model needed"). Existing loom tests (`crates/txn`'s `#[cfg(loom)]` module) must still compile and pass per `.claude/rules/concurrency-txn-layer.md`'s scoped-cfg pattern; this plan does not touch code they exercise, so no action is expected, but Task 6's step explicitly checks this since it touches `Snapshot` construction, which the residue loom model depends on.
- This plan implements **only** W3.1. `IndexPart::Sealed`/`SegmentReader`/the on-disk `.seg` format (design doc §1/§2) are **not** built here — see "Scope decision" below.
- Follow `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md` **as corrected by** `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md` — the amendment takes precedence wherever the two disagree (this plan already incorporates both).
- `unsafe` is not needed anywhere in this plan; none should be added.

### Scope decision: `IndexPart` ships Live-only in W3.1

The base design doc's §4 code sketch shows `IndexPart { Live(Arc<HnswIndex>), Sealed(Arc<SegmentReader>) }` as the target shape, but `SegmentReader` and the on-disk format (§1/§2) don't exist until W3.2 builds a segment writer. Building a full binary-format reader in W3.1 that nothing ever constructs would be dead code and contradicts "pure refactor, zero behavior change." **This plan's `IndexPart` has only the `Live` variant.** W3.2's plan (written separately, once this lands) adds `Sealed(Arc<SegmentReader>)` as a new variant to the same enum — an additive change, not a rework of what this plan builds.

---

### Task 1: `NodeSource` trait + `impl NodeSource for Graph<D>`

**Files:**
- Create: `crates/index/src/node_source.rs`
- Modify: `crates/index/src/graph.rs` (add `impl<D: Distance> NodeSource for Graph<D>` near the end of the file, after the closing `}` of `impl<D: Distance> Graph<D>` at line 728; add `mod node_source;` and `pub use node_source::NodeSource;` — check `crates/index/src/lib.rs` for where other `pub use`s live and match that pattern)
- Test: inline `#[cfg(test)] mod tests` in `crates/index/src/node_source.rs`

**Interfaces:**
- Produces: `pub trait NodeSource` with methods `entry_point`, `level`, `neighbors_into`, `vector`, `row_id`, `dimension`, `is_deleted` (default `false`) — this is the exact interface Task 2/3's generic functions and Task 4's `SegmentSet` consume.
- Consumes: `Graph<D>`'s existing private fields (`nodes: NodeTable<Node>`, `entry_point: EntryPoint`, `dimension: AtomicUsize`) and existing methods (`self.nodes.get`, `node.level()`, `node.layer(lc)`, `node.vector()`, `node.is_deleted()`, `self.established_dimension()`) — all already `pub(crate)` or accessible within `graph.rs`.

- [ ] **Step 1: Write `crates/index/src/node_source.rs` with the trait definition**

```rust
//! Abstracts element access during HNSW traversal (neighbors, vectors, the
//! deleted flag) away from the live, mutable `Graph<D>` — so `search_layer`/
//! `k_nn_search`'s algorithm bodies can run unchanged over either today's
//! live graph or (starting in W3.2) an immutable on-disk segment. See
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §2, as corrected by `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md`
//! §2 (the `is_deleted` method below is the amendment's addition — the base
//! doc's sketch omitted it).
//!
//! Addressed in `u64` "local ids" universally: for a live graph the local id
//! IS the row-id (`row_id` is the identity function); a future segment's
//! local id is a `u32` ordinal, zero-extended.

/// Traversal-time element access, implemented once per graph representation
/// (today: `Graph<D>`; from W3.2: a segment reader).
pub trait NodeSource {
    /// `(local id, level)` of the current entry point, or `None` if empty.
    fn entry_point(&self) -> Option<(u64, usize)>;
    /// The level of the node at `local`, or `None` if it doesn't exist.
    fn level(&self, local: u64) -> Option<usize>;
    /// Appends `local`'s neighbor ids at `level` into `out`, clearing `out`
    /// first. A no-op (after clearing) if `local` doesn't exist or has no
    /// slot array at `level`.
    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>);
    /// The vector stored at `local`, or `None` if it doesn't exist.
    fn vector(&self, local: u64) -> Option<&[f32]>;
    /// The row-id `local` corresponds to.
    fn row_id(&self, local: u64) -> u64;
    /// The established vector dimension of this source, or `0` if none yet.
    fn dimension(&self) -> usize;
    /// Whether `local` is tombstoned. Defaults to `false` — a segment (from
    /// W3.2) has no per-node deleted flag; deletion there is a manifest-level
    /// tombstone applied by the caller's `filter`, not this method. The live
    /// graph overrides this to check `Node::is_deleted()`, since
    /// `GraphResidueGuard`'s soft-delete mechanism is still active through
    /// W3.2a (see the amendment's §2 for why this default can't be omitted).
    fn is_deleted(&self, local: u64) -> bool {
        false
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::NodeSource;

    // A minimal, independent NodeSource impl (not Graph<D>) — proves the
    // trait's default `is_deleted` compiles and behaves as documented
    // without depending on Graph<D>'s own test suite covering it.
    struct Stub;
    impl NodeSource for Stub {
        fn entry_point(&self) -> Option<(u64, usize)> {
            None
        }
        fn level(&self, _local: u64) -> Option<usize> {
            None
        }
        fn neighbors_into(&self, _local: u64, _level: usize, out: &mut Vec<u64>) {
            out.clear();
        }
        fn vector(&self, _local: u64) -> Option<&[f32]> {
            None
        }
        fn row_id(&self, local: u64) -> u64 {
            local
        }
        fn dimension(&self) -> usize {
            0
        }
    }

    #[test]
    fn is_deleted_defaults_to_false_when_not_overridden() {
        assert!(!Stub.is_deleted(0));
    }

    #[test]
    fn row_id_is_the_identity_function_for_a_stub_source() {
        assert_eq!(Stub.row_id(42), 42);
    }
}
```

- [ ] **Step 2: Wire the new module into `crates/index/src/lib.rs`**

Read `crates/index/src/lib.rs` first to find the existing `mod`/`pub use` block (it already has entries for `graph`, `distance`, etc. per the re-verification report). Add, alongside the existing module declarations:

```rust
mod node_source;
```

and alongside the existing `pub use`s:

```rust
pub use node_source::NodeSource;
```

(`NodeSource` must be crate-public at minimum for Task 4's `segment_set.rs` to use it; make it `pub` at the crate root since W3.2's `SegmentReader` — in a future plan — will need to implement it from outside `graph.rs` too.)

- [ ] **Step 3: Run the new test**

Run: `cargo test -p strata-index node_source::tests -- --nocapture`
Expected: `test node_source::tests::is_deleted_defaults_to_false_when_not_overridden ... ok`, `test node_source::tests::row_id_is_the_identity_function_for_a_stub_source ... ok`

- [ ] **Step 4: Implement `NodeSource for Graph<D>` in `crates/index/src/graph.rs`**

Add this `impl` block immediately after the closing `}` of the existing `impl<D: Distance> Graph<D> { ... }` block (which ends at line 728, right before the `select_neighbors_simple` free function):

```rust
impl<D: Distance> crate::node_source::NodeSource for Graph<D> {
    fn entry_point(&self) -> Option<(u64, usize)> {
        self.entry_point.get()
    }

    fn level(&self, local: u64) -> Option<usize> {
        self.nodes.get(local).map(Node::level)
    }

    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>) {
        out.clear();
        if let Some(node) = self.nodes.get(local)
            && level <= node.level()
        {
            node.layer(level).occupied_into(out);
        }
    }

    fn vector(&self, local: u64) -> Option<&[f32]> {
        self.nodes.get(local).map(Node::vector)
    }

    fn row_id(&self, local: u64) -> u64 {
        local
    }

    fn dimension(&self) -> usize {
        self.established_dimension()
    }

    fn is_deleted(&self, local: u64) -> bool {
        self.nodes.get(local).is_some_and(Node::is_deleted)
    }
}
```

- [ ] **Step 5: Verify it compiles and existing tests are untouched**

Run: `cargo build -p strata-index && cargo test -p strata-index`
Expected: builds clean; every existing test in `graph.rs`'s `mod tests` still passes (they don't exercise this new `impl` yet — Task 2 wires it in).

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/node_source.rs crates/index/src/graph.rs crates/index/src/lib.rs
git commit -m "feat(index): add NodeSource trait and impl for Graph<D>

Pure addition, no call sites yet -- Task 2 of the S1 W3.1 plan wires
search_layer/k_nn_search to use it generically."
```

---

### Task 2: `search_layer` becomes a generic free function

**Files:**
- Modify: `crates/index/src/graph.rs:229-371` (the existing `fn search_layer` inherent method) and `crates/index/src/graph.rs:373-377` (`distance_to`)

**Interfaces:**
- Consumes: `NodeSource` (Task 1), `Distance` (existing trait in `crate::distance`), `SearchScratch`/`Candidate`/`SEARCH_SCRATCH` (existing, unchanged).
- Produces: `fn search_layer_generic<S, D>(source: &S, distance: &D, query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool, saturate: bool) -> Vec<(u64, f32)> where S: NodeSource, D: Distance` — Task 3's `k_nn_search_generic` calls this by name.

- [ ] **Step 1: Replace the `search_layer` method with a thin wrapper around a new generic free function**

**Do not touch `fn distance_to(&self, query: &[f32], row_id: u64) -> f32` (lines 373-377) — leave it exactly as-is.** It has a second caller besides `search_layer`: `Graph::insert`'s shrink step at line 541 (`self.distance_to(neighbor_node.vector(), id)`), which is out of scope for this plan and must keep working unchanged. `search_layer_generic` below gets its own nested distance helper instead of reusing this method, specifically so `distance_to` doesn't need to become generic or move.

Replace only the existing `fn search_layer(&self, ...) -> Vec<(u64, f32)> { ... }` method body (lines 228-371, the `#[allow(clippy::too_many_lines)]`-annotated method) with:

```rust
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        lc: usize,
        filter: &impl Fn(u64) -> bool,
        saturate: bool,
    ) -> Vec<(u64, f32)> {
        search_layer_generic(self, &self.distance, query, entry, ef, lc, filter, saturate)
    }
```

(Keep this as a private inherent method on `Graph<D>` — its doc comment from the original method should move to the new free function below, since that's where the algorithm now lives.)

Then, as a new free function in this module (place it right after the `impl<D: Distance> Graph<D> { ... }` block closes, before `select_neighbors_simple`, i.e. immediately before the `NodeSource for Graph<D>` impl added in Task 1 — order between the two doesn't matter, keep them adjacent):

```rust
/// Algorithm 2, `SEARCH-LAYER`. Returns up to `ef` `(local id, distance)`
/// pairs, nearest-first, found by greedy traversal from `entry` at layer
/// `lc`. `filter` and the deleted-flag check both gate entry into the
/// returned result set `W`, never `neighbourhood(c)` traversal — a node
/// excluded by `filter` (or tombstoned) still serves as a live waypoint for
/// reaching other nodes. Generic over `NodeSource` so the identical
/// algorithm runs over `Graph<D>` today and a segment reader from W3.2 —
/// see `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
/// §2. `filter`/`row_id` operate in row-id space; everything else (`entry`,
/// the returned ids, traversal) is in `source`'s local-id space — for
/// `Graph<D>` these coincide (`row_id` is the identity), so this is not yet
/// externally visible, but callers over a future segment must remember the
/// two domains can differ.
#[allow(clippy::too_many_lines)]
fn search_layer_generic<S: NodeSource, D: Distance>(
    source: &S,
    distance: &D,
    query: &[f32],
    entry: u64,
    ef: usize,
    lc: usize,
    filter: &impl Fn(u64) -> bool,
    saturate: bool,
) -> Vec<(u64, f32)> {
    fn distance_to<S: NodeSource, D: Distance>(source: &S, distance: &D, query: &[f32], local: u64) -> f32 {
        source
            .vector(local)
            .map_or(f32::INFINITY, |v| distance.eval(query, v))
    }

    SEARCH_SCRATCH.with_borrow_mut(|scratch| {
        scratch.visited.clear();
        scratch.candidates.clear();
        scratch.result.clear();
        scratch.previous_result_ids.clear();
        scratch.current_result_ids.clear();

        scratch.visited.insert(entry);

        let entry_dist = distance_to(source, distance, query, entry);
        scratch.candidates.push(std::cmp::Reverse(Candidate {
            row_id: entry,
            dist: entry_dist,
        }));
        if !source.is_deleted(entry) && filter(source.row_id(entry)) {
            scratch.result.push(Candidate {
                row_id: entry,
                dist: entry_dist,
            });
        }

        #[allow(clippy::items_after_statements)]
        const SATURATION_THRESHOLD_PERCENT: u32 = 95;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let patience: u32 = ((ef as f64) * 0.3).ceil().max(7.0) as u32;
        let mut saturated_streak: u32 = 0;

        while let Some(std::cmp::Reverse(c)) = scratch.candidates.pop() {
            if let Some(furthest) = scratch.result.peek()
                && c.dist > furthest.dist
                && scratch.result.len() >= ef
            {
                break;
            }
            let Some(node_level) = source.level(c.row_id) else {
                continue;
            };
            if lc > node_level {
                continue;
            }
            source.neighbors_into(c.row_id, lc, &mut scratch.occupied_buf);
            for &neighbor_id in &scratch.occupied_buf {
                if scratch.visited.contains(&neighbor_id) {
                    continue;
                }
                scratch.visited.insert(neighbor_id);
                let neighbor_dist = distance_to(source, distance, query, neighbor_id);
                let should_add = match scratch.result.peek() {
                    Some(furthest) => neighbor_dist < furthest.dist || scratch.result.len() < ef,
                    None => true,
                };
                if should_add {
                    scratch.candidates.push(std::cmp::Reverse(Candidate {
                        row_id: neighbor_id,
                        dist: neighbor_dist,
                    }));
                    if !source.is_deleted(neighbor_id) && filter(source.row_id(neighbor_id)) {
                        scratch.result.push(Candidate {
                            row_id: neighbor_id,
                            dist: neighbor_dist,
                        });
                        if scratch.result.len() > ef {
                            scratch.result.pop();
                        }
                    }
                }
            }

            if saturate {
                scratch.current_result_ids.clear();
                scratch
                    .current_result_ids
                    .extend(scratch.result.iter().map(|c| c.row_id));
                if !scratch.previous_result_ids.is_empty() && ef > 0 {
                    let overlap = scratch
                        .previous_result_ids
                        .intersection(&scratch.current_result_ids)
                        .count();
                    #[allow(
                        clippy::cast_precision_loss,
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss
                    )]
                    let overlap_percent = ((overlap as f64 / ef as f64) * 100.0) as u32;
                    if overlap_percent >= SATURATION_THRESHOLD_PERCENT {
                        saturated_streak += 1;
                        if saturated_streak >= patience {
                            break;
                        }
                    } else {
                        saturated_streak = 0;
                    }
                }
                std::mem::swap(
                    &mut scratch.previous_result_ids,
                    &mut scratch.current_result_ids,
                );
            }
        }

        let mut out: Vec<(u64, f32)> = scratch.result.iter().map(|c| (c.row_id, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
        out
    })
}
```

Note the two behavior-preserving substitutions from the original body (documented here so a reviewer can check them against the amendment):
- `self.nodes.get(c.row_id)` + `node.level()` → `source.level(c.row_id)` (one call instead of two, same information).
- `node.layer(lc).occupied_into(&mut scratch.occupied_buf)` → `source.neighbors_into(c.row_id, lc, &mut scratch.occupied_buf)` — per the amendment §1, this still routes through `scratch.occupied_buf`, just via the trait now.
- `!node.is_deleted() && filter(id)` → `!source.is_deleted(id) && filter(source.row_id(id))` — for `Graph<D>`, `source.row_id(id) == id`, so this is identical today; the `row_id` indirection only matters once a segment's local id diverges from row-id (W3.2+).

`Graph::insert` (lines 404-567) calls `self.search_layer(...)` at two sites (lines 482, 493) — **do not change these call sites**; `search_layer` is still a valid private method on `Graph<D>`, now a one-line wrapper. `distance_to`'s other caller, `pairwise_distance` (line 635), is unaffected — it's a separate method that doesn't go through `search_layer` at all; leave it exactly as-is.

- [ ] **Step 2: Build and run the full existing `crates/index` test suite — this is the zero-behavior-change proof**

Run: `cargo build -p strata-index && cargo test -p strata-index`
Expected: builds clean, and every test that existed before this task still passes with **no test file edits** — specifically `search_layer_finds_the_true_nearest_neighbor_in_a_small_graph`, `search_layer_excludes_a_deleted_node_from_results`, `search_layer_filter_excludes_a_live_node_from_results_but_not_from_traversal`, `search_layer_scratch_buffers_do_not_leak_state_across_calls`, `search_layer_traverses_through_an_excluded_node_to_reach_a_node_beyond_it`, `insert_creates_bidirectional_edges_between_new_and_existing_nodes`, `insert_advances_the_entry_point_when_a_new_node_has_a_higher_level`, `insert_shrinks_a_full_neighbor_list_to_keep_the_closer_candidate`, `k_nn_search_finds_the_true_nearest_neighbor_across_layers`, `k_nn_search_descends_through_upper_layers_to_reach_a_far_entry_points_target` (this last one is Task 3's territory too, but it already exercises `search_layer` transitively — confirm it still passes here).

If any of these fail, the substitution introduced a behavior change — stop and diff against the exact original body above rather than adjusting the test.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p strata-index --all-targets -- -D warnings`
Expected: clean. If `search_layer_generic` trips a lint the original method didn't (e.g. `too_many_arguments` on a free function vs. a method with implicit `&self`), add the matching `#[allow(...)]` with the same justification comment style as the surrounding code (see the module's existing `#[allow(clippy::too_many_arguments)]` on `Graph::insert` for the pattern).

- [ ] **Step 4: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "refactor(index): move search_layer's algorithm to a NodeSource-generic free function

Graph::search_layer becomes a one-line delegation to
search_layer_generic. Zero behavior change: every existing
crates/index test passes unmodified. Per the S1 W3.1 design (see
docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md
§2 and its 2026-07-25 amendment)."
```

---

### Task 3: `k_nn_search` becomes a generic free function

**Files:**
- Modify: `crates/index/src/graph.rs:661-688` (the existing `pub fn k_nn_search` method)

**Interfaces:**
- Consumes: `search_layer_generic` (Task 2), `NodeSource::entry_point`/`dimension` (Task 1).
- Produces: `fn k_nn_search_generic<S, D>(source: &S, distance: &D, query: &[f32], k: usize, ef: usize, filter: impl Fn(u64) -> bool) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError> where S: NodeSource, D: Distance` — Task 4's `SegmentSet::search`/`search_filtered` call this directly.

- [ ] **Step 1: Replace the `k_nn_search` method with a thin wrapper, and add the generic free function**

Replace the body of `pub fn k_nn_search(&self, query: &[f32], k: usize, ef: usize, filter: impl Fn(u64) -> bool) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError>` (lines 661-688) with:

```rust
    pub fn k_nn_search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: impl Fn(u64) -> bool,
    ) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError> {
        k_nn_search_generic(self, &self.distance, query, k, ef, filter)
    }
```

Keep the doc comment (`/// Algorithm 5, K-NN-SEARCH...` and the `# Errors` section) on this method as-is — it's still the public entry point. Add the new free function adjacent to `search_layer_generic` (added in Task 2):

```rust
/// Algorithm 5, `K-NN-SEARCH`. Descends layers `L..1` with `ef=1` greedy
/// search, then one real `SEARCH-LAYER` at layer 0 with the caller's actual
/// `ef`. Returns `(row_id, distance)` pairs, nearest-first, capped at `k`.
/// Generic over `NodeSource` — see `search_layer_generic`'s doc comment for
/// the local-id-vs-row-id note, which applies identically here.
///
/// # Errors
///
/// Returns `IndexError::DimensionMismatch` if `query`'s length doesn't
/// match `source`'s established dimension.
fn k_nn_search_generic<S: NodeSource, D: Distance>(
    source: &S,
    distance: &D,
    query: &[f32],
    k: usize,
    ef: usize,
    filter: impl Fn(u64) -> bool,
) -> Result<Vec<(u64, f32)>, crate::hnsw::IndexError> {
    let established = source.dimension();
    if established != 0 && query.len() != established {
        return Err(crate::hnsw::IndexError::DimensionMismatch {
            query_len: query.len(),
            expected: established,
        });
    }
    let Some((mut entry, mut level)) = source.entry_point() else {
        return Ok(Vec::new());
    };
    while level >= 1 {
        let found = search_layer_generic(source, distance, query, entry, 1, level, &filter, true);
        if let Some((nearest, _)) = found.first() {
            entry = *nearest;
        }
        level -= 1;
    }
    let mut results = search_layer_generic(source, distance, query, entry, ef, 0, &filter, true);
    results.truncate(k);
    Ok(results)
}
```

`Graph::insert` does **not** call `k_nn_search` (it calls `self.search_layer` directly, per Task 2's note) — no other call site in `graph.rs` needs updating.

- [ ] **Step 2: Build and run the full existing `crates/index` test suite again**

Run: `cargo build -p strata-index && cargo test -p strata-index`
Expected: clean build; `k_nn_search_finds_the_true_nearest_neighbor_across_layers` and `k_nn_search_descends_through_upper_layers_to_reach_a_far_entry_points_target` (and everything from Task 2's list) still pass unmodified.

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo clippy -p strata-index --all-targets -- -D warnings && cargo fmt --check -p strata-index`
Expected: both clean.

- [ ] **Step 4: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "refactor(index): move k_nn_search's algorithm to a NodeSource-generic free function

Completes the search_layer/k_nn_search generic-over-NodeSource
refactor started in the previous commit. Zero behavior change."
```

---

### Task 4: `SegmentSet`/`IndexPart` (Live-only) + equivalence test

**Files:**
- Create: `crates/index/src/segment_set.rs`
- Modify: `crates/index/src/hnsw.rs:72` (change `graph: Graph<L2>` to `pub(crate) graph: Graph<L2>`)
- Modify: `crates/index/src/lib.rs` (add `mod segment_set; pub use segment_set::{IndexPart, SegmentSet};`)

**Interfaces:**
- Consumes: `k_nn_search_generic` (Task 3, currently private to `graph.rs` — make it `pub(crate)` in this task since `segment_set.rs` is a sibling module needing to call it), `HnswIndex.graph` (now `pub(crate)`), `strata_index::VectorMatch`/`IndexError` (existing, in `hnsw.rs`).
- Produces: `pub struct SegmentSet { parts: Arc<[IndexPart]> }` with `pub fn from_live(index: Arc<HnswIndex>) -> Self`, `pub fn search(&self, query: &[f32], k: usize, ef_search: usize, is_visible: impl Fn(u64) -> bool) -> Result<Vec<VectorMatch>, IndexError>`, `pub fn search_filtered(&self, query: &[f32], k: usize, ef_search: usize, live_ids: &[usize], is_visible: impl Fn(u64) -> bool) -> Result<Vec<VectorMatch>, IndexError>`, `pub fn established_dimension(&self) -> usize` — Task 6's `crates/txn` changes call all four.

- [ ] **Step 1: Make `k_nn_search_generic` crate-visible**

In `crates/index/src/graph.rs`, change the function signature added in Task 3 from `fn k_nn_search_generic` to `pub(crate) fn k_nn_search_generic`.

- [ ] **Step 2: Make `HnswIndex.graph` crate-visible**

In `crates/index/src/hnsw.rs`, change line 72 from:

```rust
    graph: Graph<L2>,
```

to:

```rust
    pub(crate) graph: Graph<L2>,
```

- [ ] **Step 3: Write `crates/index/src/segment_set.rs`**

```rust
//! The set of index parts a snapshot searches over. Today (S1 W3.1) this is
//! always exactly one `Live` part — the existing shared, mutable `HnswIndex`
//! — wrapped so `crates/txn` searches through a `SegmentSet` instead of a
//! bare `Arc<HnswIndex>`. This is a pure abstraction step: W3.2 (a later
//! plan) adds an `IndexPart::Sealed(Arc<SegmentReader>)` variant and a
//! per-commit build/publish path; W3.3 adds real multi-part fan-out to
//! `search`/`search_filtered` below. See
//! `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
//! §4 (W3.1) and its 2026-07-25 amendment for why `Sealed` is deliberately
//! absent from this file.

use std::sync::Arc;

use crate::graph::k_nn_search_generic;
use crate::hnsw::{HnswIndex, IndexError, VectorMatch};

/// One part of a segment set. Only `Live` exists as of S1 W3.1 — see this
/// module's doc comment.
pub enum IndexPart {
    Live(Arc<HnswIndex>),
}

/// The set of index parts a snapshot searches. Cheap to clone (`Arc<[_]>`).
#[derive(Clone)]
pub struct SegmentSet {
    parts: Arc<[IndexPart]>,
}

impl SegmentSet {
    /// Builds a segment set of exactly one live part, wrapping today's
    /// shared mutable index. The only constructor until W3.2 adds a second.
    #[must_use]
    pub fn from_live(index: Arc<HnswIndex>) -> Self {
        Self {
            parts: Arc::from(vec![IndexPart::Live(index)]),
        }
    }

    /// Mirrors [`HnswIndex::search`] — delegates to the single `Live` part
    /// via the `NodeSource`-generic traversal (Task 2/3 of this plan), not
    /// `HnswIndex::search` itself, so this genuinely proves the refactor's
    /// equivalence rather than routing around it.
    ///
    /// # Errors
    ///
    /// Same as [`HnswIndex::search`].
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
        let raw = k_nn_search_generic(&index.graph, &crate::distance::L2, query, k, ef_search, is_visible)?;
        Ok(raw
            .into_iter()
            .map(|(row_id, dist)| VectorMatch {
                row_id,
                squared_distance: dist * dist,
            })
            .collect())
    }

    /// Mirrors [`HnswIndex::search_filtered`] — see [`Self::search`]'s doc
    /// comment for why this goes through the generic path directly.
    ///
    /// # Errors
    ///
    /// Same as [`HnswIndex::search_filtered`].
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        live_ids: &[usize],
        is_visible: impl Fn(u64) -> bool,
    ) -> Result<Vec<VectorMatch>, IndexError> {
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
        index.search_filtered(query, k, ef_search, live_ids, is_visible)
    }

    /// The established vector dimension of this set's parts. Used by
    /// `crates/txn` in place of `HnswIndex::established_dimension` now that
    /// `Snapshot` no longer holds a bare `Arc<HnswIndex>`.
    #[must_use]
    pub fn established_dimension(&self) -> usize {
        let [IndexPart::Live(index)] = self.parts.as_ref() else {
            unreachable!("SegmentSet has exactly one Live part until W3.2")
        };
        index.established_dimension()
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
mod tests {
    use super::*;
    use crate::{EfConstruction, MaxConnections, MaxElements, MaxLayers};

    fn build_index(n: usize) -> Arc<HnswIndex> {
        let index = HnswIndex::new(
            MaxConnections(16),
            MaxElements(100),
            MaxLayers(16),
            EfConstruction(100),
        )
        .unwrap();
        for i in 0..n as u64 {
            index.insert_owned(i, vec![i as f32, 0.0, 0.0]).unwrap();
        }
        Arc::new(index)
    }

    #[test]
    fn search_over_one_live_part_matches_hnsw_index_search_directly() {
        let index = build_index(20);
        let set = SegmentSet::from_live(Arc::clone(&index));

        let direct = index.search(&[0.0, 0.0, 0.0], 5, 32, |_| true).unwrap();
        let via_set = set.search(&[0.0, 0.0, 0.0], 5, 32, |_| true).unwrap();

        assert_eq!(
            via_set.len(),
            direct.len(),
            "SegmentSet::search must return the same number of matches as HnswIndex::search"
        );
        for (a, b) in via_set.iter().zip(direct.iter()) {
            assert_eq!(a.row_id, b.row_id, "row-id order must match exactly");
            assert!(
                (a.squared_distance - b.squared_distance).abs() < f32::EPSILON,
                "distances must match exactly: {} vs {}",
                a.squared_distance,
                b.squared_distance
            );
        }
    }

    #[test]
    fn search_filtered_over_one_live_part_matches_hnsw_index_search_filtered_directly() {
        let index = build_index(20);
        let set = SegmentSet::from_live(Arc::clone(&index));
        let live_ids: Vec<usize> = (0..20).step_by(2).collect(); // only even row-ids

        let direct = index
            .search_filtered(&[0.0, 0.0, 0.0], 5, 32, &live_ids, |_| true)
            .unwrap();
        let via_set = set
            .search_filtered(&[0.0, 0.0, 0.0], 5, 32, &live_ids, |_| true)
            .unwrap();

        assert_eq!(via_set.len(), direct.len());
        for (a, b) in via_set.iter().zip(direct.iter()) {
            assert_eq!(a.row_id, b.row_id);
        }
        assert!(
            via_set.iter().all(|m| m.row_id % 2 == 0),
            "only even row-ids were in live_ids: {via_set:?}"
        );
    }

    #[test]
    fn established_dimension_matches_the_underlying_index() {
        let index = build_index(3);
        let set = SegmentSet::from_live(Arc::clone(&index));
        assert_eq!(set.established_dimension(), index.established_dimension());
        assert_eq!(set.established_dimension(), 3);
    }
}
```

- [ ] **Step 4: Wire the new module into `crates/index/src/lib.rs`**

Add `mod segment_set;` alongside the other `mod` declarations and `pub use segment_set::{IndexPart, SegmentSet};` alongside the other `pub use`s (same pattern as Task 1 Step 2). Confirm `VectorMatch` and `IndexError` are already `pub` from `hnsw.rs` (they must be, since `crates/txn` already uses `strata_index::VectorMatch` per `snapshot.rs`'s imports) — no change needed there.

- [ ] **Step 5: Build and run the new tests plus the full `crates/index` suite**

Run: `cargo build -p strata-index && cargo test -p strata-index`
Expected: builds clean; the three new `segment_set::tests` pass, and every test from Tasks 2/3's lists still passes unmodified.

- [ ] **Step 6: Run clippy and fmt**

Run: `cargo clippy -p strata-index --all-targets -- -D warnings && cargo fmt --check -p strata-index`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/index/src/segment_set.rs crates/index/src/hnsw.rs crates/index/src/lib.rs crates/index/src/graph.rs
git commit -m "feat(index): add SegmentSet/IndexPart (Live-only) with an equivalence test

SegmentSet::search/search_filtered route through the NodeSource-generic
traversal (not HnswIndex::search directly), proving Tasks 2-3's refactor
is behavior-preserving. IndexPart::Sealed is deliberately not added yet
-- see this plan's Scope decision section and the design doc amendment."
```

---

### Task 5: `Manifest` gains an empty `segments: Vec<SegmentEntry>` field

**Files:**
- Modify: `crates/storage/src/manifest.rs` (add `SegmentEntry` struct, add `segments` field to `Manifest`, update `Manifest::empty()`, update 5 test literals)
- Modify: `crates/txn/src/dataset.rs` (update 4 test literals that hand-construct a `Manifest`)

**Interfaces:**
- Consumes: `crate::stats::ColumnStats` (existing, already imported in `manifest.rs`).
- Produces: `pub struct SegmentEntry { name: String, format_version: u32, vector_count: u64, dimension: u32, row_id_min: u64, row_id_max: u64, byte_len: u64, zone_map: HashMap<String, ColumnStats> }` and `Manifest.segments: Vec<SegmentEntry>` — Task 6 does not use these yet (Snapshot doesn't read them until W3.2), but they must exist and always be empty per the design doc §3/§4's "W3.2 is purely a write-and-open-path change, not a format change."

- [ ] **Step 1: Add `SegmentEntry` and the `segments` field in `crates/storage/src/manifest.rs`**

Add this struct right after `DataFileEntry` (after its closing `}` at line 37):

```rust
/// One immutable index segment listed in the manifest — see
/// `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
/// §3. Always an empty `Vec` on `Manifest` until S1 W3.2 starts writing
/// segments; `#[serde(default)]` on the field below and on `zone_map` here
/// both make "field absent" (a manifest written before this existed) and
/// "field present but empty" indistinguishable, which is required: an
/// absent/empty `zone_map` must always mean "must scan," never "may prune"
/// (binding invariant, see the design doc §3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// Relative to the dataset's `data/` directory, e.g. `"{attempt_id:020}.seg"`.
    pub name: String,
    /// Per-segment, not per-dataset — segments are immutable and never
    /// rewritten, so a future writer must still be able to read an older
    /// segment's format.
    pub format_version: u32,
    pub vector_count: u64,
    pub dimension: u32,
    /// Inclusive.
    pub row_id_min: u64,
    /// Inclusive.
    pub row_id_max: u64,
    pub byte_len: u64,
    /// Empty until S1 W4 populates it. An absent or empty map must always
    /// fail safe to "must scan" in whatever pruning evaluator W4 writes.
    #[serde(default)]
    pub zone_map: HashMap<String, ColumnStats>,
}
```

Then add the field to `Manifest` (after `commit_time_high_water` at line 85):

```rust
    /// Immutable index segments as of this version — see
    /// `docs/superpowers/specs/2026-07-24-s1-segment-format-w3-migration-design.md`
    /// §3. Always empty until S1 W3.2 starts writing segments.
    /// `#[serde(default)]` so manifests written before this field existed
    /// still deserialize, same reasoning as `tombstones`/`next_attempt_id`.
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
```

And update `Manifest::empty()` (lines 88-99) to add `segments: Vec::new(),` after `commit_time_high_water: 0,`.

- [ ] **Step 2: Update the 5 existing hand-constructed `Manifest { ... }` literals in `crates/storage/src/manifest.rs`'s own tests**

Add `segments: Vec::new(),` immediately after each literal's existing `commit_time_high_water: 0,` line, in all five: `commit_then_read_current_round_trips`'s `m0` (line ~214-225) and `m1` (line ~227-245), `leftover_tmp_file_is_never_picked_up_as_current`'s `m0` (line ~259-270), `commit_then_read_current_with_populated_stats`'s `m0` (line ~337-348), `commit_manifest_writes_compact_json_not_pretty_printed`'s `m0` (line ~436-447).

- [ ] **Step 3: Add a round-trip test proving legacy manifests still deserialize with `segments` defaulting to empty**

Add to `crates/storage/src/manifest.rs`'s test module, alongside the existing `manifest_without_next_attempt_id_field_deserializes_with_default_zero` test:

```rust
    #[test]
    fn manifest_without_segments_field_deserializes_with_default_empty() {
        let old_json = serde_json::json!({
            "version": 0,
            "data_files": [],
            "next_row_id": 0,
        });
        let deserialized: Manifest = serde_json::from_value(old_json).unwrap();
        assert!(deserialized.segments.is_empty());
    }

    #[test]
    fn empty_manifest_has_no_segments() {
        assert!(Manifest::empty().segments.is_empty());
    }
```

- [ ] **Step 4: Build and run `crates/storage`'s test suite — expect compile errors, then fix them**

Run: `cargo build -p strata-storage`
Expected: compile errors at every `Manifest { ... }` literal missing the new `segments` field — this includes `crates/txn/src/dataset.rs`'s 4 literals, which will show up as errors in `crates/txn` once you build the workspace next. For now, in `crates/storage` itself, confirm all 5 test-module literals from Step 2 were updated and it builds clean:

Run: `cargo test -p strata-storage`
Expected: all pass, including the 2 new tests from Step 3.

- [ ] **Step 5: Fix the 4 `Manifest { ... }` literals in `crates/txn/src/dataset.rs`**

Add `segments: Vec::new(),` immediately after each literal's `commit_time_high_water: 0,` line:
- `legacy_manifest` in `opening_a_legacy_pre_attempt_id_manifest_does_not_destroy_its_data_files` (around line 2199-2210)
- `hostile` in `open_errors_instead_of_attempting_a_huge_allocation_on_an_unreasonable_next_row_id` (around line 2328-2335)
- `hostile` in `commit_errors_instead_of_overflowing_when_version_would_wrap` (around line 2355-2362)
- `hostile` in the path-traversal-guard test around line 3390-3401

- [ ] **Step 6: Build and test the whole workspace**

Run: `cargo build --workspace && cargo test --workspace`
Expected: clean build, every test passes (this also re-confirms Tasks 1-4 didn't regress anything now that `crates/txn` rebuilds against the changed `strata-storage`/`strata-index`).

- [ ] **Step 7: Run clippy and fmt on the whole workspace**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add crates/storage/src/manifest.rs crates/txn/src/dataset.rs
git commit -m "feat(storage): add SegmentEntry and an always-empty Manifest.segments field

Field exists now so S1 W3.2 is purely a write/open-path change, not a
format change (design doc §3/§4). zone_map defaults empty and must
fail safe to 'must scan' per the design doc's binding invariant."
```

---

### Task 6: `Snapshot.graph: Arc<HnswIndex>` → `Snapshot.index: SegmentSet`

**Files:**
- Modify: `crates/txn/src/snapshot.rs` (struct field, `vector_search`, test helper)
- Modify: `crates/txn/src/dataset.rs` (3 production `Snapshot { ... }` construction sites, 3 test call sites reading `.graph.established_dimension()`)

**Interfaces:**
- Consumes: `SegmentSet::from_live`/`search`/`search_filtered`/`established_dimension` (Task 4).
- Produces: `Snapshot.index: SegmentSet` (renamed from `graph: Arc<HnswIndex>`) — this is the last interface change in this plan; nothing downstream of this plan consumes anything new from `Snapshot`.

- [ ] **Step 1: Change the `Snapshot` struct field in `crates/txn/src/snapshot.rs`**

Change line 24 from:

```rust
    pub(crate) graph: Arc<HnswIndex>,
```

to:

```rust
    pub(crate) index: strata_index::SegmentSet,
```

Remove the now-unused `use strata_index::HnswIndex;` import at line 12 if nothing else in this file uses `HnswIndex` directly (check after Step 2 below — `vector_search` will no longer reference it).

- [ ] **Step 2: Update `vector_search` to use `self.index` instead of `self.graph`**

In `vector_search` (lines 250-275), change:

```rust
            return Ok(self
                .graph
                .search(query, k, EF_SEARCH_DEFAULT, |id| self.is_visible(id))?);
```

to:

```rust
            return Ok(self
                .index
                .search(query, k, EF_SEARCH_DEFAULT, |id| self.is_visible(id))?);
```

and:

```rust
        Ok(self
            .graph
            .search_filtered(query, k, ef, &live_ids, |id| self.is_visible(id))?)
```

to:

```rust
        Ok(self
            .index
            .search_filtered(query, k, ef, &live_ids, |id| self.is_visible(id))?)
```

- [ ] **Step 3: Update `snapshot.rs`'s own test helper**

In `test_snapshot_with_in_flight` (lines 343-365), change:

```rust
            graph: Arc::new(
                HnswIndex::new(
                    MaxConnections(16),
                    MaxElements(100),
                    MaxLayers(16),
                    EfConstruction(200),
                )
                .unwrap(),
            ),
```

to:

```rust
            index: strata_index::SegmentSet::from_live(Arc::new(
                strata_index::HnswIndex::new(
                    MaxConnections(16),
                    MaxElements(100),
                    MaxLayers(16),
                    EfConstruction(200),
                )
                .unwrap(),
            )),
```

(The test module already imports `EfConstruction, MaxConnections, MaxElements, MaxLayers` from `strata_index` at line 335 — leave that import as-is; only `HnswIndex` needs to become a qualified `strata_index::HnswIndex` here since the top-of-file `use strata_index::HnswIndex;` may have been removed in Step 1.)

- [ ] **Step 4: Update `crates/txn/src/dataset.rs`'s 3 production `Snapshot { ... }` construction sites**

Site 1, `create_with_commit_log_capacity` (around line 285-294): change

```rust
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(imbl::HashSet::new()),
```

to:

```rust
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(Arc::new(graph)),
            tombstones: Arc::new(imbl::HashSet::new()),
```

Site 2, `Dataset::open` (around line 362-377): change

```rust
            manifest: Arc::new(manifest),
            graph: Arc::new(graph),
            tombstones: Arc::new(tombstones),
```

to:

```rust
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(Arc::new(graph)),
            tombstones: Arc::new(tombstones),
```

Site 3, the commit path (around line 1077-1085): this one already has `self.graph: Arc<HnswIndex>` (the `Transaction`'s own field, distinct from `Snapshot`'s — this field name is **not** changing, only `Snapshot`'s field is). Change:

```rust
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            graph: self.graph,
            watermark,
            in_flight: visibility.in_flight,
            tombstones: Arc::new(tombstones),
        };
```

to:

```rust
        let snapshot = Snapshot {
            dir: self.dir,
            version: new_version,
            manifest: Arc::new(manifest),
            index: strata_index::SegmentSet::from_live(self.graph),
            watermark,
            in_flight: visibility.in_flight,
            tombstones: Arc::new(tombstones),
        };
```

(`self.graph` here is `Transaction.graph: Arc<HnswIndex>` — unchanged field, just now wrapped when handed to `Snapshot`.)

- [ ] **Step 5: Fix the 3 test call sites reading `.graph.established_dimension()` on a `Snapshot`**

These are read-only accessor calls, not construction sites — grep for them to get current line numbers (they may have shifted from the reference lines below after Step 4's edits):

Around line 4254: `snapshot_before.graph.established_dimension()` → `snapshot_before.index.established_dimension()`
Around line 4320: `snapshot_after.graph.established_dimension()` → `snapshot_after.index.established_dimension()`
Around line 4505: `snapshot.graph.established_dimension()` → `snapshot.index.established_dimension()`

- [ ] **Step 6: Build the workspace and fix any remaining compile errors mechanically**

Run: `cargo build --workspace`
Expected: any remaining error will be a `Snapshot { graph: ... }`-shaped construction or a `.graph.` accessor on a `Snapshot` value that Steps 4-5 missed (there should be none left per the greps run while writing this plan, but `cargo build`'s error output is the authoritative check). Fix each following the exact substitution pattern above: a construction site gets `index: strata_index::SegmentSet::from_live(<the Arc<HnswIndex> expression>)`; a read-only accessor gets `.index.` in place of `.graph.`. Do **not** touch any `Transaction.graph` or `Dataset`-level `graph` field access (line 465, 642, 674, 878, 955, 959 per the grep run while writing this plan) — those are a different field on a different struct and are unaffected by this task.

- [ ] **Step 7: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: every test passes, including the loom-gated concurrency tests. Per this plan's Global Constraints, also run the loom-scoped check per `.claude/rules/concurrency-txn-layer.md`:

Run: `cargo rustc -p strata-txn --lib --profile test -- --cfg loom` then run the resulting test binary (path printed by that command, under `target/debug/deps/`) directly, or `cargo test -p strata-txn --lib --cfg loom` after confirming the `rustc` step built successfully.
Expected: the existing `#[cfg(loom)]` residue/atomicity models still pass — this task changes what type `Snapshot` holds but not `GraphResidueGuard`, `RowIdAllocator`, or the commit lock, so no new interleaving is introduced.

- [ ] **Step 8: Run clippy and fmt on the whole workspace**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: both clean.

- [ ] **Step 9: Commit**

```bash
git add crates/txn/src/snapshot.rs crates/txn/src/dataset.rs
git commit -m "refactor(txn): Snapshot.graph: Arc<HnswIndex> -> Snapshot.index: SegmentSet

Completes S1 W3.1 (docs/superpowers/plans/2026-07-25-s1-w3-1-segment-abstraction.md).
Zero behavior change: vector_search's two call sites now route through
SegmentSet, which itself routes through the NodeSource-generic
traversal added in this plan's earlier tasks. Transaction/Dataset's own
graph: Arc<HnswIndex> field is untouched -- only Snapshot's field
changed. All workspace tests, including loom, pass unmodified."
```

---

## Plan-level exit criteria (re-run after Task 6, before calling W3.1 done)

- `cargo build --workspace` — clean, no warnings.
- `cargo test --workspace` — every test passes, none edited from what existed before this plan (except the additions explicitly listed in Tasks 4/5's new tests).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo fmt --check` — clean.
- Loom (scoped per `.claude/rules/concurrency-txn-layer.md`) — green.
- Opus-tier `reviewer` subagent review of the full diff across all 6 tasks (mandatory per `.claude/CLAUDE.md` — "no task is marked done without this, regardless of which model implemented it").
- Confirm the equivalence property the design doc's §4/W3.1 calls out explicitly: `SegmentSet::search`/`search_filtered` over one `Live` part return results identical to `HnswIndex::search`/`search_filtered` for a fixed dataset/query set — Task 4's two equivalence tests are that proof; re-read them at review time to confirm they actually assert equality (row-id order and distance), not just "doesn't panic."

## Explicitly out of scope for this plan (deferred to W3.2's own plan, written fresh once this lands)

- The on-disk `.seg` segment format (design doc §1), `SegmentReader`, `IndexPart::Sealed`.
- Any change to `write_phase`, `GraphResidueGuard`, `replay_index`, `delta_log.rs`, or the commit lock's in-lock graph-mutation loop.
- Relocating the Arrow vector-extraction / non-finite-guard / dimension-pre-validation logic (amendment §3) — still needed by `build_delta_entries` exactly as today until W3.2 changes the write path.
- Populating `SegmentEntry.zone_map` (W4) or fan-out search across more than one part (W3.3).
