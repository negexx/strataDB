# HNSW Search Performance Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the empirically-measured performance gap in `crates/index/`'s HNSW search path (real distance-call profiling found ~71%+ of search latency is distance computation, much of it plausibly cache-miss-inflated) via five independently-shippable improvements, without touching the lock-free rewrite's zero-reclamation invariant.

**Architecture:** Enable `anndists`'s already-present SIMD feature (a build config change, zero code changes), add saturation-based early termination and thread-local scratch buffers to `search_layer`, generalize `select_neighbors_heuristic`'s diversity check with a tunable α parameter (internal-only, `HnswIndex`'s frozen public API is untouched), and conditionally add software prefetching only if a post-SIMD benchmark still shows it's warranted.

**Tech Stack:** Rust, `anndists` (SIMD distance backend), `criterion` (benchmarking), existing `crates/index` lock-free HNSW (`Graph`, `NodeTable`, `SlotArray`).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-19-hnsw-search-performance-improvements-design.md` — read it for the full reasoning behind every design choice below; this plan implements it, not re-derives it.
- `HnswIndex`'s public API (`new`/`insert`/`established_dimension`/`search`/`search_filtered`, `VectorMatch`, `IndexError`, the four newtype params) must stay byte-for-byte unchanged — this is a hard constraint carried over from the lock-free HNSW rewrite (`crates/txn` depends on it never changing). None of the five tasks below touch it.
- Workspace toolchain is pinned to stable Rust 1.90 (`rust-toolchain.toml`) — no nightly-only features anywhere, including in Task 1's SIMD choice.
- `cargo build --workspace` clean with no warnings, `cargo test --workspace` passing, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean — required before any task is marked done (this project's standing "What done means" gate).
- No loom tests are required for Tasks 2-4 — none of them touch atomics, CAS, or any concurrency-sensitive code path (confirmed in the spec §8); Task 3's `thread_local!` buffers introduce no new shared-state race by construction. Task 1 and Task 5 don't touch Rust code that needs loom coverage either.
- Every task gets reviewed before being marked done, per this project's standing "Review is not optional" rule — use the most capable available review model for tasks touching `search_layer`'s core loop (Tasks 2 and 3) given their correctness subtlety; a lighter review is fine for Tasks 1 and 5 (mechanical/conditional).
- `search_layer`, `select_neighbors_heuristic`, and `Graph::insert`/`insert_batch` all live in `crates/index/src/graph.rs` — every task below modifies this same file; work through the tasks in order (each assumes the previous tasks' changes are already in place).

---

### Task 1: Enable `anndists` SIMD feature

**Files:**
- Modify: `crates/index/Cargo.toml`

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing new (no code, no new types) — this task only changes which code path `anndists::dist::{DistL2, DistCosine}` execute internally. `crate::distance::L2::eval`'s signature and behavior (same numeric results, just faster) are unchanged, so every later task consumes it exactly as it exists today.

- [ ] **Step 1: Change the dependency line**

In `crates/index/Cargo.toml`, change:

```toml
anndists = "0.1"
```

to:

```toml
anndists = { version = "0.1", features = ["simdeez_f"] }
```

`simdeez_f` (not `stdsimd`) because `stdsimd` maps to Rust's nightly-only `std::simd`, and this workspace is pinned to stable 1.90 — see Global Constraints and the spec §2.

- [ ] **Step 2: Confirm the workspace still builds and existing tests still pass**

Run: `cargo build --workspace`
Expected: builds cleanly, no warnings.

Run: `cargo test -p strata-index --lib distance::tests`
Expected: PASS — all existing `distance.rs` unit tests (unchanged, since `L2`/`Cosine`/`Dot`'s numeric outputs are unaffected by which internal code path computes them).

- [ ] **Step 3: Measure the actual improvement — do not trust the estimate**

Run: `cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench`

Record, in the commit message: the `l2_distance_eval_only` benchmark's reported time (compare against the pre-change baseline of 330.64ns/call recorded in the design doc §1 — confirm it actually dropped), the recall@10 numbers (must still both clear 0.8 and `Graph` should still be >= `hnsw_rs`'s, unchanged from before since this doesn't change any distance *values*, only computation speed), and the `graph_top_10`/`hnsw_rs_top_10` timings.

If `l2_distance_eval_only`'s time did NOT measurably drop, stop and investigate before proceeding to Task 2 — it means the feature isn't actually taking effect (e.g., the CPU running this doesn't support the SIMD instruction set `simdeez` selected, or the feature didn't propagate) rather than assume the rest of this plan's motivation still holds.

- [ ] **Step 4: Commit**

```bash
git add crates/index/Cargo.toml Cargo.lock
git commit -m "perf(index): enable anndists' simdeez_f SIMD feature for distance computation"
```

---

### Task 2: Saturation-based early termination in `search_layer`

**Files:**
- Modify: `crates/index/src/graph.rs:172-250` (the `search_layer` method)

**Interfaces:**
- Consumes: `search_layer`'s existing signature (`query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool`) — unchanged. `Candidate`'s existing `Ord`/`PartialOrd` (nearest-first via `Reverse`, farthest-first directly) — unchanged.
- Produces: `search_layer`'s external behavior — same signature, same return type (`Vec<(u64, f32)>`), but may return after visiting fewer candidates than before when the result set has saturated. Every caller (`Graph::insert`'s two call sites, `Graph::k_nn_search`'s two call sites) needs no changes — this task doesn't touch call sites, only `search_layer`'s internal loop.

- [ ] **Step 1: Write the failing test**

Add to `crates/index/src/graph.rs`'s `mod tests` block:

```rust
    #[test]
    fn saturation_based_early_termination_preserves_recall_and_reduces_distance_evals() {
        // Regression/discrimination test for saturation-based early
        // termination ("Patience in Proximity", see design doc): proves
        // (a) recall is unaffected -- the returned top-ef set still
        // exactly matches the true nearest neighbors -- and (b) early
        // termination actually fires -- fewer distance evaluations than
        // visiting every node reachable at ef=5 would require.
        //
        // Fixture: query at the origin, ef=5. Five points at distance
        // ~1.0-1.004 (the true nearest 5, extremely tightly clustered so
        // they're nearly indistinguishable from each other -- this
        // mirrors the "flat convergence zone" the saturation mechanism
        // targets, where many near-equal-distance candidates cause the
        // top-ef membership to stabilize well before the classic
        // Algorithm 2 stopping condition would fire on its own).
        // Twenty-five additional points on a ring at distance ~1.5,
        // spaced closely enough to be graph-connected to each other and
        // to the near cluster, so they get discovered and considered
        // (each is closer than *some* points, gating them into the
        // search) without ever displacing the true top 5.
        //
        // NOTE for whoever maintains this test: this fixture's exact
        // discriminating power (does the assertion at the bottom
        // actually catch a broken/disabled saturation check?) was
        // verified empirically during implementation by temporarily
        // reverting the saturation-check code in Step 3 below and
        // confirming `evals_with_patience` increased. If you change
        // `search_layer`'s traversal logic and this test's second
        // assertion becomes flaky or stops discriminating, re-run that
        // same red/green check rather than assuming the fixture still
        // works -- do not just loosen the bound to make it pass.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingL2 {
            calls: Arc<AtomicUsize>,
        }
        impl crate::distance::Distance for CountingL2 {
            fn eval(&self, a: &[f32], b: &[f32]) -> f32 {
                self.calls.fetch_add(1, Ordering::Relaxed);
                crate::distance::L2.eval(a, b)
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let graph = Graph::new(
            CountingL2 {
                calls: Arc::clone(&calls),
            },
            40,
        );
        let m_l = 1.0 / (16f64).ln();

        // True nearest 5: distance ~1.0, tightly clustered.
        for i in 0..5u64 {
            #[allow(clippy::cast_precision_loss)]
            let offset = i as f32 * 0.001;
            graph
                .insert(
                    i,
                    vec![1.0 + offset, 0.0, 0.0],
                    16,
                    32,
                    16,
                    100,
                    m_l,
                    0.5,
                )
                .unwrap();
        }

        // Boundary ring: 25 points at distance ~1.5, spread around the
        // query so they form a densely-interconnected shell.
        for i in 5..30u64 {
            #[allow(clippy::cast_precision_loss)]
            let angle = i as f64 * 0.25;
            #[allow(clippy::cast_possible_truncation)]
            let vector = vec![(1.5 * angle.cos()) as f32, (1.5 * angle.sin()) as f32, 0.0];
            graph
                .insert(i, vector, 16, 32, 16, 100, m_l, 0.5)
                .unwrap();
        }

        calls.store(0, Ordering::Relaxed);
        let results = graph.k_nn_search(&[0.0, 0.0, 0.0], 5, 5, |_| true).unwrap();
        let evals_with_patience = calls.load(Ordering::Relaxed);

        let mut result_ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        result_ids.sort_unstable();
        assert_eq!(
            result_ids,
            vec![0, 1, 2, 3, 4],
            "must still find the true 5 nearest neighbors despite early \
             termination: {result_ids:?}"
        );

        // Upper bound: this graph has 30 nodes total. A search that
        // never terminated early would need to distance-eval close to
        // all of them (every node is reachable and graph-connected).
        // Saturation firing means meaningfully fewer evals than that.
        assert!(
            evals_with_patience < 25,
            "saturation-based termination should stop well before \
             exhausting the 30-node graph: {evals_with_patience} evals"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p strata-index --lib graph::tests::saturation_based_early_termination_preserves_recall_and_reduces_distance_evals`
Expected: FAIL — either the recall assertion fails (unlikely, since this is unimplemented — the search should already find the true 5 via the existing Algorithm 2 logic), or (more likely) the `evals_with_patience < 25` assertion fails because no early termination exists yet.

If the recall assertion fails at this stage (before any implementation), the fixture itself is wrong — the true top-5 isn't being found even by the existing, correct search — fix the fixture's coordinates before proceeding.

- [ ] **Step 3: Implement saturation-based early termination**

Replace `crates/index/src/graph.rs:172-250` (`search_layer`'s body) with:

```rust
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        lc: usize,
        filter: &impl Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(entry);

        let entry_dist = self.distance_to(query, entry);
        // Min-heap of candidates still to explore (nearest first via `Reverse`).
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        candidates.push(std::cmp::Reverse(Candidate {
            row_id: entry,
            dist: entry_dist,
        }));
        // Max-heap of the best `ef` results found so far (farthest first, for cheap eviction).
        let mut result: BinaryHeap<Candidate> = BinaryHeap::new();
        if let Some(node) = self.nodes.get(entry)
            && !node.is_deleted()
            && filter(entry)
        {
            result.push(Candidate {
                row_id: entry,
                dist: entry_dist,
            });
        }

        // Saturation-based early termination ("Patience in Proximity",
        // Teofili & Lin, ECIR 2025 -- see design doc
        // docs/superpowers/specs/2026-07-19-hnsw-search-performance-improvements-design.md
        // §3). `ef` stands in for the paper's `k`: this function has no
        // separate top-k concept, only the ef-capped `result` set. If the
        // result set's membership stops changing across consecutive
        // candidate visits, further traversal is unlikely to improve it.
        const SATURATION_THRESHOLD_PERCENT: u32 = 95;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let patience: u32 = ((ef as f64) * 0.3).ceil().max(7.0) as u32;
        let mut previous_result_ids: std::collections::HashSet<u64> =
            std::collections::HashSet::new();
        let mut saturated_streak: u32 = 0;

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            if let Some(furthest) = result.peek()
                && c.dist > furthest.dist
                && result.len() >= ef
            {
                break; // Algorithm 2 line 7-8: all of W is settled.
            }
            let Some(node) = self.nodes.get(c.row_id) else {
                continue;
            };
            // A node's layer-lc slot array only exists for lc <= node.level().
            if lc > node.level() {
                continue;
            }
            for neighbor_id in node.layer(lc).occupied() {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                let neighbor_dist = self.distance_to(query, neighbor_id);
                let should_add = match result.peek() {
                    Some(furthest) => neighbor_dist < furthest.dist || result.len() < ef,
                    None => true,
                };
                if should_add {
                    candidates.push(std::cmp::Reverse(Candidate {
                        row_id: neighbor_id,
                        dist: neighbor_dist,
                    }));
                    if let Some(neighbor_node) = self.nodes.get(neighbor_id)
                        && !neighbor_node.is_deleted()
                        && filter(neighbor_id)
                    {
                        result.push(Candidate {
                            row_id: neighbor_id,
                            dist: neighbor_dist,
                        });
                        if result.len() > ef {
                            result.pop(); // evict the current furthest
                        }
                    }
                }
            }

            let current_result_ids: std::collections::HashSet<u64> =
                result.iter().map(|c| c.row_id).collect();
            if !previous_result_ids.is_empty() && ef > 0 {
                let overlap = previous_result_ids
                    .intersection(&current_result_ids)
                    .count();
                #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
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
            previous_result_ids = current_result_ids;
        }

        let mut out: Vec<(u64, f32)> = result.into_iter().map(|c| (c.row_id, c.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
        out
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::saturation_based_early_termination_preserves_recall_and_reduces_distance_evals`
Expected: PASS.

If the `evals_with_patience < 25` assertion still fails (saturation never fires on this fixture), the coordinates need adjusting — try tightening the true-5 cluster further (e.g., offsets of `0.0001` instead of `0.001`) or widening the boundary ring's point count. Do not weaken the assertion instead of fixing the fixture.

- [ ] **Step 5: Empirically verify the test actually discriminates (red/green)**

Temporarily comment out the saturation-check block added in Step 3 (the `if !previous_result_ids.is_empty() ...` block and its surrounding bookkeeping), leaving only the original Algorithm 2 stopping condition. Re-run the test from Step 4.

Expected: the recall assertion (`result_ids == [0,1,2,3,4]`) still passes (saturation never affects correctness, only when to stop), but `evals_with_patience` should now be measurably higher (record the actual number in a code comment above the assertion, e.g. `// verified: N evals without saturation, M evals with it`). If `evals_with_patience` is NOT measurably higher without the saturation check, the fixture isn't discriminating — strengthen it (per Step 4's guidance) before restoring the implementation.

Restore the Step 3 implementation once verified.

- [ ] **Step 6: Run the full existing test suite**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — every existing `search_layer`/`insert`/`k_nn_search` test in the file (traversal-through-excluded-node, deleted-node exclusion, filter tests, multi-layer descent, deletion correctness, the concurrent-insert stress test) continues to pass unchanged, since saturation-based termination never changes *which* nodes are eligible for `result`, only when the loop stops searching for more.

- [ ] **Step 7: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "perf(index): add saturation-based early termination to search_layer"
```

---

### Task 3: Pre-allocated per-query search buffers

**Files:**
- Modify: `crates/index/src/graph.rs` (top-level, near `Candidate`'s definition, and `search_layer`'s body from Task 2)

**Interfaces:**
- Consumes: `search_layer`'s Task-2 body — this task changes *where* `visited`/`candidates`/`result`/`previous_result_ids` live (thread-local scratch instead of fresh per-call allocations), not the algorithm itself.
- Produces: no signature changes to `search_layer` or anything that calls it.

- [ ] **Step 1: Write the failing test**

Add to `crates/index/src/graph.rs`'s `mod tests` block:

```rust
    #[test]
    fn search_layer_scratch_buffers_do_not_leak_state_across_calls() {
        let graph = Graph::new(crate::distance::L2, 10);
        let m_l = 1.0 / (16f64).ln();
        graph
            .insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();
        graph
            .insert(1, vec![0.1, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5)
            .unwrap();

        // First call, entry = row 0: row 0 gets marked visited in
        // whatever scratch buffer backs this call.
        let first = graph.search_layer(&[0.0, 0.0, 0.0], 0, 5, 0, &|_| true);
        assert!(first.iter().any(|(id, _)| *id == 0));

        // Second, independent call from a DIFFERENT entry point (row 1)
        // must still be able to reach and return row 0 via traversal --
        // if a reused scratch buffer's `visited` set wasn't cleared
        // between calls, row 0 would still show up as "already visited"
        // from the first call and get wrongly skipped here.
        let second = graph.search_layer(&[0.0, 0.0, 0.0], 1, 5, 0, &|_| true);
        assert!(
            second.iter().any(|(id, _)| *id == 0),
            "reused scratch buffers must be cleared between calls -- row 0 \
             was wrongly excluded, implying stale `visited` state leaked \
             across calls: {second:?}"
        );
    }
```

- [ ] **Step 2: Run test to verify it passes even before this task's change**

Run: `cargo test -p strata-index --lib graph::tests::search_layer_scratch_buffers_do_not_leak_state_across_calls`
Expected: PASS — today's implementation allocates fresh buffers every call, so there's nothing to leak yet. This test exists to *stay* passing after Step 3's change, proving the reused buffers are correctly cleared — it's a regression guard, not a red/green discriminator for this task (there's no "before" bug to reproduce; the risk is introducing one).

- [ ] **Step 3: Implement thread-local scratch buffers**

Add this near `Candidate`'s definition in `crates/index/src/graph.rs` (after the `impl Ord for Candidate` block, before `impl<D: Distance> Graph<D>`):

```rust
/// Per-thread reusable scratch space for `search_layer`, avoiding a
/// fresh `HashSet`/two `BinaryHeap`s/two more `HashSet`s on every call.
/// Safe as plain `RefCell` (not a `Mutex`/atomic): `search_layer` is
/// never called reentrantly on the same thread -- every caller
/// (`Graph::insert`'s two call sites, `Graph::k_nn_search`'s two call
/// sites) calls it sequentially and lets each call fully return before
/// starting the next, so a nested `borrow_mut()` can never happen.
#[derive(Default)]
struct SearchScratch {
    visited: std::collections::HashSet<u64>,
    candidates: BinaryHeap<std::cmp::Reverse<Candidate>>,
    result: BinaryHeap<Candidate>,
    previous_result_ids: std::collections::HashSet<u64>,
    current_result_ids: std::collections::HashSet<u64>,
}

thread_local! {
    static SEARCH_SCRATCH: std::cell::RefCell<SearchScratch> =
        std::cell::RefCell::new(SearchScratch::default());
}
```

Then replace `search_layer`'s body (the version from Task 2) with:

```rust
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        lc: usize,
        filter: &impl Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        SEARCH_SCRATCH.with_borrow_mut(|scratch| {
            scratch.visited.clear();
            scratch.candidates.clear();
            scratch.result.clear();
            scratch.previous_result_ids.clear();
            scratch.current_result_ids.clear();

            scratch.visited.insert(entry);

            let entry_dist = self.distance_to(query, entry);
            scratch.candidates.push(std::cmp::Reverse(Candidate {
                row_id: entry,
                dist: entry_dist,
            }));
            if let Some(node) = self.nodes.get(entry)
                && !node.is_deleted()
                && filter(entry)
            {
                scratch.result.push(Candidate {
                    row_id: entry,
                    dist: entry_dist,
                });
            }

            const SATURATION_THRESHOLD_PERCENT: u32 = 95;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let patience: u32 = ((ef as f64) * 0.3).ceil().max(7.0) as u32;
            let mut saturated_streak: u32 = 0;

            while let Some(std::cmp::Reverse(c)) = scratch.candidates.pop() {
                if let Some(furthest) = scratch.result.peek()
                    && c.dist > furthest.dist
                    && scratch.result.len() >= ef
                {
                    break;
                }
                let Some(node) = self.nodes.get(c.row_id) else {
                    continue;
                };
                if lc > node.level() {
                    continue;
                }
                for neighbor_id in node.layer(lc).occupied() {
                    if scratch.visited.contains(&neighbor_id) {
                        continue;
                    }
                    scratch.visited.insert(neighbor_id);
                    let neighbor_dist = self.distance_to(query, neighbor_id);
                    let should_add = match scratch.result.peek() {
                        Some(furthest) => neighbor_dist < furthest.dist || scratch.result.len() < ef,
                        None => true,
                    };
                    if should_add {
                        scratch.candidates.push(std::cmp::Reverse(Candidate {
                            row_id: neighbor_id,
                            dist: neighbor_dist,
                        }));
                        if let Some(neighbor_node) = self.nodes.get(neighbor_id)
                            && !neighbor_node.is_deleted()
                            && filter(neighbor_id)
                        {
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

                scratch.current_result_ids.clear();
                scratch
                    .current_result_ids
                    .extend(scratch.result.iter().map(|c| c.row_id));
                if !scratch.previous_result_ids.is_empty() && ef > 0 {
                    let overlap = scratch
                        .previous_result_ids
                        .intersection(&scratch.current_result_ids)
                        .count();
                    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
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
                std::mem::swap(&mut scratch.previous_result_ids, &mut scratch.current_result_ids);
            }

            let mut out: Vec<(u64, f32)> = scratch
                .result
                .iter()
                .map(|c| (c.row_id, c.dist))
                .collect();
            out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));
            out
        })
    }
```

Note the `std::mem::swap` at the end of the saturation-check block: this reuses `current_result_ids`'s already-allocated capacity as next iteration's `previous_result_ids` buffer (and vice versa) instead of cloning — the entire point of this task is eliminating exactly this kind of per-iteration allocation, not just the three original buffers.

`with_borrow_mut` requires Rust 1.90 (stabilized in a recent edition) — confirm this is available on the pinned toolchain; if not, use `SEARCH_SCRATCH.with(|cell| { let mut scratch = cell.borrow_mut(); ... })` instead, identical behavior.

- [ ] **Step 4: Run tests to verify everything still passes**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — all tests including both new ones from Tasks 2 and 3.

- [ ] **Step 5: Run the concurrent stress test specifically, with extra attention**

Run: `cargo test -p strata-index --lib graph::tests::concurrent_inserts_are_all_findable_afterward`
Expected: PASS. This test exercises many real threads calling `insert`/`search_layer` concurrently — it's the test most likely to reveal a `thread_local!` misuse (e.g., if `search_layer` were ever accidentally called reentrantly on the same thread, `with_borrow_mut` would panic on the nested borrow, and this is the test with enough real concurrency to have a chance of hitting that if it existed). A clean pass here is real evidence for the "never reentrant" claim in Step 3's doc comment, not just the comment's say-so.

- [ ] **Step 6: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "perf(index): reuse thread-local scratch buffers in search_layer instead of per-call allocation"
```

---

### Task 4: α-tunable pruning in `select_neighbors_heuristic`

**Files:**
- Modify: `crates/index/src/graph.rs` (`select_neighbors_heuristic`, `Graph::insert`, `Graph::insert_batch`, and every test call site to any of these three)
- Modify: `crates/index/src/hnsw.rs` (`HnswIndex::insert`'s single call to `self.graph.insert(...)`)
- Modify: `bench/benches/lockfree_vs_hnsw_rs_bench.rs` (both `graph.insert(...)` call sites)

**Interfaces:**
- Consumes: nothing new from earlier tasks.
- Produces: `select_neighbors_heuristic(candidates: &[(u64, f32)], m: usize, alpha: f64, pairwise_dist: impl Fn(u64, u64) -> f32) -> Vec<u64>` (new `alpha: f64` parameter, inserted as the third argument), `Graph::insert(..., m_l: f64, alpha: f64, unif: f64)` and `Graph::insert_batch(..., m_l: f64, alpha: f64, unifs: &[f64])` (new `alpha: f64` parameter, inserted right before the existing last parameter). `HnswIndex::insert` and the bench file's call sites pass `1.0` for `alpha`, exactly reproducing today's behavior.

- [ ] **Step 1: Write the failing tests**

In `crates/index/src/graph.rs`'s `mod tests` block, update the existing test and add a new one:

```rust
    #[test]
    fn select_neighbors_heuristic_prunes_a_candidate_dominated_by_an_already_picked_neighbor() {
        // Candidate 2: dist-to-query 1.0. Candidate 3: dist-to-query 3.0,
        // and dist(3, 2) = 2.0 -- candidate 3 is dominated by already-picked
        // candidate 2, so the heuristic should skip it in favor of a more
        // diverse pick (candidate 4) if one exists.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let pairwise = |a: u64, b: u64| -> f32 {
            match (a, b) {
                (3, 2) | (2, 3) => 2.0,
                (4, 2) | (2, 4) => 5.0,
                _ => 0.0,
            }
        };
        let selected = select_neighbors_heuristic(&candidates, 2, 1.0, pairwise);
        assert_eq!(
            selected,
            vec![2, 4],
            "alpha=1.0 must reproduce the original heuristic exactly: {selected:?}"
        );
    }

    #[test]
    fn select_neighbors_heuristic_alpha_greater_than_one_retains_a_previously_dominated_candidate() {
        // Same fixture as the alpha=1.0 test above. At alpha=2.0,
        // candidate 3's relaxed threshold (query_dist 3.0 / alpha 2.0 =
        // 1.5) is no longer exceeded by pairwise_dist(3, 2) = 2.0, so 3
        // is no longer dominated and gets kept ahead of the more-diverse
        // candidate 4 -- proving alpha genuinely changes behavior, not
        // just an inert parameter that's accepted and ignored.
        let candidates = vec![(2, 1.0), (3, 3.0), (4, 3.1)];
        let pairwise = |a: u64, b: u64| -> f32 {
            match (a, b) {
                (3, 2) | (2, 3) => 2.0,
                (4, 2) | (2, 4) => 5.0,
                _ => 0.0,
            }
        };
        let selected = select_neighbors_heuristic(&candidates, 2, 2.0, pairwise);
        assert_eq!(
            selected,
            vec![2, 3],
            "alpha=2.0 must retain candidate 3 (no longer dominated at the \
             relaxed threshold), unlike alpha=1.0's [2, 4]: {selected:?}"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail to compile**

Run: `cargo test -p strata-index --lib graph::tests::select_neighbors_heuristic`
Expected: FAIL to compile — `select_neighbors_heuristic` doesn't take an `alpha` parameter yet.

- [ ] **Step 3: Implement `alpha` in `select_neighbors_heuristic`**

Replace `select_neighbors_heuristic`'s definition in `crates/index/src/graph.rs`:

```rust
fn select_neighbors_heuristic(
    candidates: &[(u64, f32)],
    m: usize,
    alpha: f64,
    pairwise_dist: impl Fn(u64, u64) -> f32,
) -> Vec<u64> {
    let mut working: Vec<(u64, f32)> = candidates.to_vec();
    working.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(CmpOrdering::Equal));

    let mut result: Vec<u64> = Vec::new();
    for (candidate_id, query_dist) in working {
        if result.len() >= m {
            break;
        }
        // Vamana's RobustPrune reachability parameter (alpha >= 1,
        // Subramanya et al. / DiskANN): a candidate is dominated only if
        // some already-picked neighbor is closer to it than
        // query_dist/alpha. alpha=1.0 reproduces Algorithm 4 line 11's
        // original check exactly (the previous, hardcoded behavior);
        // alpha>1.0 relaxes the check, retaining more longer-range
        // edges.
        #[allow(clippy::cast_possible_truncation)]
        let relaxed_threshold = (f64::from(query_dist) / alpha) as f32;
        let dominated = result
            .iter()
            .any(|&picked| pairwise_dist(candidate_id, picked) < relaxed_threshold);
        if !dominated {
            result.push(candidate_id);
        }
    }
    result
}
```

- [ ] **Step 4: Thread `alpha` through `Graph::insert` and `Graph::insert_batch`**

In `crates/index/src/graph.rs`, change `insert`'s signature (currently ending `ef_construction: usize, m_l: f64, unif: f64`) to insert `alpha: f64` before `unif`:

```rust
    pub fn insert(
        &self,
        row_id: u64,
        vector: Vec<f32>,
        m: usize,
        mmax0: usize,
        mmax: usize,
        ef_construction: usize,
        m_l: f64,
        alpha: f64,
        unif: f64,
    ) -> Result<(), crate::hnsw::IndexError> {
```

Update both call sites to `select_neighbors_heuristic` inside `insert`'s body (the initial connection-building call and the shrink-step call) to pass `alpha`:

```rust
            let chosen =
                select_neighbors_heuristic(&candidates, m, alpha, |a, b| self.pairwise_distance(a, b));
```

```rust
                        let keep = select_neighbors_heuristic(&with_dists, capacity, alpha, |a, b| {
                            self.pairwise_distance(a, b)
                        });
```

Do the same for `insert_batch`'s signature (insert `alpha: f64` before `unifs: &[f64]`) and its single forwarding call to `insert` (add `alpha` before `unif`).

Both `insert` and `insert_batch` carry a doc comment saying their parameter
list is "inherently 8 conceptual parameters wide" (justifying the
`#[allow(clippy::too_many_arguments)]` above each). Update both comments
to say "9 conceptual parameters wide" — it's now stale otherwise, and a
stale justification comment is worse than no comment.

- [ ] **Step 5: Update every remaining call site to `Graph::insert`/`insert_batch`**

This is mechanical but exhaustive — the compiler will catch every missed site as a type error, so work through them systematically rather than trying to remember them all:

Run: `grep -n '\.insert(' crates/index/src/graph.rs crates/index/src/hnsw.rs`

For every `.insert(row_id_or_i, vector_expr, m_or_16, mmax0_or_32, mmax_or_16, ef_construction_or_100_or_200, m_l, ..., unif_or_0.5_expr)` call found in `crates/index/src/graph.rs`'s test module (both direct `Graph::insert` calls and the one `insert_batch` test), insert `1.0` as the new second-to-last argument (immediately before the existing `unif`/`unifs` argument), e.g.:

```rust
// before:
graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 0.5).unwrap();
// after:
graph.insert(0, vec![0.0, 0.0, 0.0], 16, 32, 16, 100, m_l, 1.0, 0.5).unwrap();
```

In `crates/index/src/hnsw.rs`, `HnswIndex::insert`'s single call to `self.graph.insert(...)` gets the same treatment — insert `1.0` before the `unif` argument it already computes. This preserves `HnswIndex`'s behavior exactly (alpha is not part of its public API in this task — see the design doc §5 for why).

In `bench/benches/lockfree_vs_hnsw_rs_bench.rs`, both `graph.insert(...)` and `counting_graph.insert(...)` calls (inside `bench_lockfree_vs_hnsw_rs` and `print_distance_calls_per_search`) get the same treatment.

- [ ] **Step 6: Run tests to verify everything compiles and passes**

Run: `cargo build --workspace`
Expected: builds cleanly — this will fail loudly with a clear "expected N arguments, found N-1" error at every call site not yet updated in Step 5; fix each one.

Run: `cargo test -p strata-index --lib`
Expected: PASS — every test, including the two from Step 1 and every pre-existing test now passing `1.0` for `alpha` (which must reproduce identical behavior to before this task, since `alpha=1.0` is mathematically a no-op on the dominated check).

Run: `cargo check -p strata-bench --all-targets`
Expected: builds cleanly (confirms the bench file's two call sites were updated correctly; the bench itself is not re-run here, that's Task 1's job and doesn't need repeating for this task since alpha=1.0 doesn't change distance computation).

- [ ] **Step 7: Commit**

```bash
git add crates/index/src/graph.rs crates/index/src/hnsw.rs bench/benches/lockfree_vs_hnsw_rs_bench.rs
git commit -m "perf(index): add alpha-tunable RobustPrune-style pruning to select_neighbors_heuristic (internal only, HnswIndex hardcodes alpha=1.0)"
```

---

### Task 5: Conditional software prefetch — decision gate

**Files:**
- Modify (only if the decision criteria in Step 1 are met): `crates/index/src/graph.rs` (`search_layer`'s neighbor-expansion loop)

**Interfaces:**
- Consumes: `search_layer`'s Task-3 body.
- Produces: no signature changes regardless of outcome — this is a pure internal latency optimization, and its non-implementation (if the criteria aren't met) produces nothing for later tasks to consume, since there are no tasks after this one in this plan.

- [ ] **Step 1: Re-run the benchmark and evaluate the decision criteria**

Run: `cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench`

Per the design doc §6, this item is conditional: implement it **only if** the `avg distance eval() calls per k_nn_search` count (printed by `print_distance_calls_per_search`) multiplied by the now-current `l2_distance_eval_only` benchmark time still accounts for meaningfully less than the measured `graph_top_10` time than it did in the original profiling (i.e., the gap between "expected distance-only time" and "measured full-search time" — the ~29% originally attributed to "everything else" — is still large in absolute terms, not just relative, after Tasks 1-4's changes).

Record the actual numbers from this run in the task's commit message or a short note, regardless of which branch is taken.

**If the gap is now small** (Tasks 1-4 already closed most of it, e.g. SIMD's speedup made distance computation dominate proportionally even more, or saturation/buffer-reuse already closed most of the "everything else" share): skip Steps 2-5, and commit only a one-line note explaining the decision:

```bash
git add docs/superpowers/plans/2026-07-19-hnsw-search-performance-improvements-plan.md
git commit -m "docs(plan): defer Task 5 (prefetch) -- post-Task-1-4 benchmark shows the memory-stall gap already closed"
```

(Amend this task's checkbox with a one-line note in the plan file itself before committing, e.g. `- [x] Task 5: DEFERRED -- <numbers> -- see commit <hash>`.)

**If the gap is still large:** proceed to Step 2.

- [ ] **Step 2: Write the implementation** (only if Step 1's criteria were met)

`std::arch::x86_64::_mm_prefetch`'s exact signature (const-generic strategy
parameter vs. a plain `i32` argument) has changed across Rust versions in
the past — verify the actual signature for the pinned 1.90 toolchain
(`rustup doc std::arch::x86_64::_mm_prefetch` or docs.rs pinned to 1.90)
before trusting the shape below; treat it as a starting sketch, not a
guaranteed-correct signature.

In `crates/index/src/graph.rs`'s `search_layer` (the Task-3 version), inside the `for neighbor_id in node.layer(lc).occupied()` loop, before computing `neighbor_dist`, resolve and prefetch the neighbor's vector:

```rust
                for neighbor_id in node.layer(lc).occupied() {
                    if scratch.visited.contains(&neighbor_id) {
                        continue;
                    }
                    scratch.visited.insert(neighbor_id);
                    #[cfg(target_arch = "x86_64")]
                    if let Some(neighbor_node) = self.nodes.get(neighbor_id) {
                        // SAFETY: `_mm_prefetch` is a pure latency hint --
                        // it never dereferences memory, cannot fault, and
                        // has no effect on program correctness regardless
                        // of whether `ptr` is valid, aligned, or even
                        // still allocated by the time the prefetch
                        // actually executes.
                        unsafe {
                            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                                neighbor_node.vector().as_ptr().cast::<i8>(),
                            );
                        }
                    }
                    let neighbor_dist = self.distance_to(query, neighbor_id);
```

- [ ] **Step 3: Run tests to verify correctness is unaffected**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — prefetching cannot change any result (it's a hint, not a data dependency), so every existing test (including Tasks 2 and 3's new ones) passes unchanged.

- [ ] **Step 4: Re-run the benchmark to confirm the win**

Run: `cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench`
Record the `graph_top_10` timing before/after this specific change in the commit message — this is a hint-based optimization with no correctness signal, so the benchmark number is the only evidence it did anything.

- [ ] **Step 5: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "perf(index): prefetch neighbor vectors during search_layer traversal (x86_64 only)"
```

---

## Self-Review Notes

- **Spec coverage:** design doc §2 (SIMD) → Task 1; §3 (patience) → Task 2; §4 (buffers) → Task 3; §5 (alpha) → Task 4; §6 (conditional prefetch) → Task 5; §7 (deferred reordering) → intentionally has no task, recorded in the spec only; §8 (testing strategy) → each task's own test steps match the spec's per-item requirements exactly (non-vacuous patience test, buffer-leak test, alpha=1.0-regression + alpha>1.0-discrimination tests).
- **Placeholder scan:** no TBD/TODO; Task 5's conditional branching is a real, concrete decision procedure with exact criteria and exact commands for both branches, not a deferred "figure it out" — this is a deliberate plan-level accommodation for a genuinely conditional design item (per the spec's own explicit framing), not an unresolved gap.
- **Type consistency:** `select_neighbors_heuristic(candidates, m, alpha, pairwise_dist)`'s parameter order is consistent between Task 4's Step 3 (definition) and every call site update in Steps 1, 4, and 5. `Graph::insert`'s new `alpha: f64` parameter position (immediately before `unif`) is consistent between Task 4's Step 4 (definition) and Step 5 (every call site). `SearchScratch`'s field names (`visited`, `candidates`, `result`, `previous_result_ids`, `current_result_ids`) are used identically between Task 3's Step 3 definition and its use inside `search_layer`'s body, and Task 5's Step 2 modification targets the same `scratch.visited`/`scratch.result` fields Task 3 introduced.
