# Saturation-During-Insert Recall Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop saturation-based early termination from firing during `Graph::insert`'s own internal `search_layer` calls, recovering the construction-side recall loss (0.9890 → ~0.9940 per the audited A/B) while keeping it at query time — and measure the actual insert-time cost this trades away, not assume it's acceptable.

**Architecture:** Add a `saturate: bool` parameter to `search_layer`, threaded explicitly through its 4 call sites (2 in `Graph::insert`, both `false`; 2 in `Graph::k_nn_search`, both `true`). Capture a real wall-clock insert-time baseline *before* the fix lands, then re-measure *after*, using the same instrumentation both times.

**Tech Stack:** Rust, existing `crates/index` lock-free HNSW (`Graph::search_layer`), `bench/benches/lockfree_vs_hnsw_rs_bench.rs`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-19-saturation-during-insert-design.md` — read it for the full reasoning; this plan implements it, not re-derives it.
- `HnswIndex`'s public API is untouched — this fix is entirely internal to `Graph`.
- `cargo build --workspace` clean with no warnings, `cargo test --workspace` passing, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean — required before any task is marked done.
- No loom test is required — this fix adds a plain `bool` parameter and an `if` gate around existing logic; it introduces no new atomics, CAS, or shared-state concurrency surface.
- Every task gets reviewed before being marked done. Task 2 (the actual `search_layer` change) touches the same hot-path loop three prior tasks (the search-performance plan's Tasks 2 and 3) needed Opus-level review for — use the most capable available review model for it.
- The `Instant`-based wall-clock timing added in Task 1 must survive unchanged into Task 3 — do not change how it's measured between capturing the "before" and "after" numbers, or the comparison isn't valid.

---

### Task 1: Capture the insert-time baseline (before the fix)

**Files:**
- Modify: `bench/benches/lockfree_vs_hnsw_rs_bench.rs`

**Interfaces:**
- Consumes: nothing new — this task only adds `println!`-based timing around the existing `graph.insert(...)` loop inside `bench_lockfree_vs_hnsw_rs`.
- Produces: a printed wall-clock number this task's own run captures as the "before" baseline (recorded in the commit message, not consumed programmatically by any later task — Task 3 re-reads the same printed line after the fix lands).

- [ ] **Step 1: Add wall-clock timing around the `Graph` insert loop**

In `bench/benches/lockfree_vs_hnsw_rs_bench.rs`, find the `// --- new lock-free Graph ---` section inside `bench_lockfree_vs_hnsw_rs` (currently around line 221-238):

```rust
    // --- new lock-free Graph ---
    let graph = Graph::new(L2, N);
    let m_l = 1.0 / 16f64.ln();
    for (i, v) in vectors.iter().enumerate() {
        graph
            .insert(
                i as u64,
                v.clone(),
                16,
                32,
                16,
                200,
                m_l,
                1.0,
                bench_unif(i as u64),
            )
            .unwrap();
    }
```

Replace it with:

```rust
    // --- new lock-free Graph ---
    let graph = Graph::new(L2, N);
    let m_l = 1.0 / 16f64.ln();
    let graph_insert_start = std::time::Instant::now();
    for (i, v) in vectors.iter().enumerate() {
        graph
            .insert(
                i as u64,
                v.clone(),
                16,
                32,
                16,
                200,
                m_l,
                1.0,
                bench_unif(i as u64),
            )
            .unwrap();
    }
    let graph_insert_elapsed = graph_insert_start.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let inserts_per_sec = N as f64 / graph_insert_elapsed.as_secs_f64();
    println!(
        "Graph::insert wall-clock time for {N} vectors: {graph_insert_elapsed:?} \
         ({inserts_per_sec:.1} inserts/sec)"
    );
```

This is plain `std::time::Instant`, not a `criterion::bench_function` — criterion's repeated-sampling model doesn't fit a single 10k-vector bulk build (which only happens once per benchmark run, during setup, same reasoning as why `graph_top_10`/`hnsw_rs_top_10` measure single-query search separately from this one-time build).

- [ ] **Step 2: Confirm it compiles**

Run: `cargo check -p strata-bench --all-targets`
Expected: builds cleanly.

- [ ] **Step 3: Run the benchmark and record the baseline**

Run: `cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench`
This takes 5-15 minutes — run in the foreground with a long timeout, or via whatever long-running-command tooling your environment provides (a prior session in this repo found plain backgrounded `Bash` processes can silently die mid-run).

Record the printed `Graph::insert wall-clock time for 10000 vectors: ... (... inserts/sec)` line — this is the **before** baseline, measured with saturation still firing during insert (this task doesn't change `search_layer` itself yet). Put the exact number in the commit message.

- [ ] **Step 4: Commit**

```bash
git add bench/benches/lockfree_vs_hnsw_rs_bench.rs
git commit -m "bench(index): add insert wall-clock timing to lockfree_vs_hnsw_rs_bench

Baseline (saturation still firing during insert): <PASTE THE ACTUAL
PRINTED NUMBER HERE>"
```

---

### Task 2: Add `saturate: bool` to `search_layer`, thread through all 4 call sites

**Files:**
- Modify: `crates/index/src/graph.rs`

**Interfaces:**
- Consumes: `search_layer`'s existing signature and body (`query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool`) — from the search-performance plan's Tasks 2/3, unchanged since.
- Produces: `search_layer(&self, query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool, saturate: bool) -> Vec<(u64, f32)>` (new `saturate: bool` parameter, added as the 6th/last parameter). `Graph::insert` and `Graph::k_nn_search` need no signature changes themselves — only their internal calls to `search_layer` change.

- [ ] **Step 1: Write the failing test**

Add to `crates/index/src/graph.rs`'s `mod tests` block (this reuses the exact fixture from
`saturation_based_early_termination_preserves_recall_and_reduces_distance_evals`,
already proven to trigger saturation — see that test, a few hundred lines above in the same file, for the fixture's full reasoning):

```rust
    #[test]
    fn search_layer_saturate_false_visits_more_candidates_than_saturate_true() {
        // Direct discrimination test for the `saturate` parameter itself
        // (not routed through insert/k_nn_search): on a fixture already
        // proven to trigger saturation, calling search_layer with
        // saturate=false must visit strictly more candidates than
        // saturate=true, on the identical graph and query. Reuses the
        // same 10-point-cluster + ef=10 + 10-satellite fixture as
        // saturation_based_early_termination_preserves_recall_and_reduces_distance_evals
        // above, which is already validated to discriminate.
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
            30,
        );
        let m_l = 1.0 / (16f64).ln();

        // True nearest 10: distance ~1.0, tightly clustered.
        for i in 0..10u64 {
            #[allow(clippy::cast_precision_loss)]
            let offset = i as f32 * 0.0001;
            graph
                .insert(i, vec![1.0 + offset, 0.0, 0.0], 16, 32, 16, 100, m_l, 1.0, 0.5)
                .unwrap();
        }
        // 10 satellite points, one per cluster member, giving the
        // saturation streak real otherwise-unvisited neighbors to skip.
        for i in 10..20u64 {
            #[allow(clippy::cast_precision_loss)]
            let angle = i as f64 * 0.3;
            #[allow(clippy::cast_possible_truncation)]
            let vector = vec![(1.5 * angle.cos()) as f32, (1.5 * angle.sin()) as f32, 0.0];
            graph
                .insert(i, vector, 16, 32, 16, 100, m_l, 1.0, 0.5)
                .unwrap();
        }

        let (entry, entry_level) = graph.entry_point.get().unwrap();
        let query = [1.0, 0.0, 0.0];

        calls.store(0, Ordering::Relaxed);
        let _ = graph.search_layer(&query, entry, 10, entry_level, &|_| true, true);
        let evals_with_saturation = calls.load(Ordering::Relaxed);

        calls.store(0, Ordering::Relaxed);
        let _ = graph.search_layer(&query, entry, 10, entry_level, &|_| true, false);
        let evals_without_saturation = calls.load(Ordering::Relaxed);

        assert!(
            evals_without_saturation > evals_with_saturation,
            "saturate=false must visit strictly more candidates than \
             saturate=true on this fixture: {evals_without_saturation} vs \
             {evals_with_saturation}"
        );
    }
```

Note: `graph.entry_point`/`Graph`'s internal fields are accessible here because this test lives in the same module (`mod tests` inside `graph.rs`) — matches the existing pattern already used by `insert_advances_the_entry_point_when_a_new_node_has_a_higher_level` and similar tests in this file. If `entry_point`/`EntryPoint::get` isn't directly what you find in the current file, check the actual current field/method names in `crates/index/src/graph.rs` before assuming — this snippet is a starting point, not guaranteed byte-exact against the current file state.

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `cargo test -p strata-index --lib graph::tests::search_layer_saturate_false_visits_more_candidates_than_saturate_true`
Expected: FAIL to compile — `search_layer` doesn't take a `saturate` parameter yet.

- [ ] **Step 3: Add the `saturate` parameter and gate the saturation-check block**

In `crates/index/src/graph.rs`, change `search_layer`'s signature (currently `fn search_layer(&self, query: &[f32], entry: u64, ef: usize, lc: usize, filter: &impl Fn(u64) -> bool) -> Vec<(u64, f32)>`) to:

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
```

Inside the function body, wrap the saturation-check block (the `const SATURATION_THRESHOLD_PERCENT`/`patience`/`saturated_streak` setup before the loop, and the membership-comparison block inside the loop) in `if saturate { ... }`. The setup block becomes:

```rust
            // Saturation-based early termination ("Patience in Proximity",
            // Teofili & Lin, ECIR 2025) -- gated by `saturate`: firing
            // during Graph::insert's own construction-time search_layer
            // calls permanently bakes truncated-candidate-set edges into
            // the graph for a one-time build-speed win, a worse trade
            // than the intended recurring per-query one -- see design doc
            // docs/superpowers/specs/2026-07-19-saturation-during-insert-design.md.
            #[allow(clippy::items_after_statements)]
            const SATURATION_THRESHOLD_PERCENT: u32 = 95;
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let patience: u32 = ((ef as f64) * 0.3).ceil().max(7.0) as u32;
            let mut saturated_streak: u32 = 0;
```

(unchanged — `patience`/`saturated_streak` are cheap to compute even when unused; only the check-and-break block inside the loop needs the `if saturate` gate). Change the block at the end of each loop iteration from:

```rust
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
```

to:

```rust
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
```

When `saturate` is `false`, `scratch.previous_result_ids`/`scratch.current_result_ids` are never touched during the loop — they're still cleared at the top of every call (Task 3's existing scratch-clearing code, unchanged), so no stale state can leak into a later `saturate: true` call on the same thread.

- [ ] **Step 4: Update all 4 call sites**

In `Graph::insert` (around line 423, the ef=1 descent — exact line number may have shifted, search for the surrounding comment `// Phase 1 (Algorithm 1 lines 5-7)`):

```rust
            let found = self.search_layer(query, entry, 1, entry_level, &|_| true, false);
```

(around line 434, the `ef_construction` connection-building — search for `// Phase 2 (Algorithm 1 lines 8-17)`):

```rust
            let candidates = self.search_layer(query, entry, ef_construction, lc, &|_| true, false);
```

In `Graph::k_nn_search` (around line 587, the ef=1 descent):

```rust
            let found = self.search_layer(query, entry, 1, level, &filter, true);
```

(around line 593, the real-`ef` layer-0 call):

```rust
        let mut results = self.search_layer(query, entry, ef, 0, &filter, true);
```

- [ ] **Step 5: Update every other existing call site to `search_layer`**

This is mechanical but exhaustive — the compiler catches every missed site as a type error. Run:

```bash
grep -n '\.search_layer(' crates/index/src/graph.rs
```

For every remaining call in the test module (the existing `search_layer`-direct tests, e.g. `search_layer_finds_the_true_nearest_neighbor_in_a_small_graph`, `search_layer_excludes_a_deleted_node_from_results`, `search_layer_filter_excludes_a_live_node_from_results_but_not_from_traversal`, `search_layer_scratch_buffers_do_not_leak_state_across_calls`, `search_layer_traverses_through_an_excluded_node_to_reach_a_node_beyond_it`), append `true` as the new last argument (matching `k_nn_search`'s query-time behavior — these are all correctness tests about traversal/filtering/deletion, not about saturation itself, so they should keep the mechanism that was already active when they were written and reviewed, unless a specific test is actually testing saturation itself, e.g. `saturation_based_early_termination_preserves_recall_and_reduces_distance_evals`, which already calls `k_nn_search`, not `search_layer` directly, so it needs no change here at all).

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p strata-index --lib graph::tests::search_layer_saturate_false_visits_more_candidates_than_saturate_true`
Expected: PASS.

- [ ] **Step 7: Run the full test suite**

Run: `cargo test -p strata-index --lib graph::tests`
Expected: PASS — every existing test, including `saturation_based_early_termination_preserves_recall_and_reduces_distance_evals` (which goes through `k_nn_search`, now `saturate: true`, unchanged behavior) and `k_nn_search_descends_through_upper_layers_to_reach_a_far_entry_points_target` (the multi-layer descent test — also goes through `k_nn_search`, unchanged).

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/index/src/graph.rs
git commit -m "fix(index): disable saturation-based early termination during Graph::insert's own search_layer calls"
```

---

### Task 3: Re-measure recall and insert-time cost (after the fix)

**Files:**
- None modified — this task only runs the already-instrumented benchmark and records the result.

**Interfaces:**
- Consumes: Task 1's `Instant`-based timing (unchanged since Task 1, per Global Constraints), Task 2's fix.
- Produces: nothing consumed by any later task — this is the plan's final verification step.

- [ ] **Step 1: Run the full benchmark**

Run: `cargo bench -p strata-bench --bench lockfree_vs_hnsw_rs_bench`
Same 5-15 minute caveat as Task 1 Step 3 — run in the foreground or via long-running-command tooling, not a bare backgrounded process.

- [ ] **Step 2: Record and compare recall**

From the printed `recall@10 ... Graph: <NUMBER>` line, confirm it has moved from 0.9890 toward the audited ~0.9940 (not merely "some improvement"). If it lands meaningfully short of 0.9940 (e.g. still close to 0.9890), investigate before treating this task as done — it would mean the fix isn't actually taking effect for some reason (e.g. a missed call site from Task 2 Step 4/5, or `bench_unif`'s deterministic sequence producing a different topology than the original audit's manual toggle used).

- [ ] **Step 3: Record and compare insert-time cost**

From the printed `Graph::insert wall-clock time for 10000 vectors: ...` line, compare against Task 1's recorded baseline. Compute the percentage change (slower, by how much).

- [ ] **Step 4: Commit a summary note**

Add a short section to the bottom of this plan file recording the final before/after numbers (recall and insert time), then commit:

```bash
git add docs/superpowers/plans/2026-07-19-saturation-during-insert-recall-fix-plan.md
git commit -m "docs(plan): record before/after recall and insert-time numbers for the saturation-during-insert fix"
```

If the insert-time cost turns out large enough to be a real concern, note that explicitly in the same commit and flag it for the human rather than silently deciding it's fine — per the design doc §3, there's no pre-agreed threshold; that's a judgment call for whoever reviews the actual numbers.

---

## Self-Review Notes

- **Spec coverage:** design doc §2 (mechanism, call site assignments) → Task 2; §3 item 1 (discriminating test) → Task 2 Step 1; §3 item 2 (insert-throughput measurement) → Tasks 1 and 3; §3 item 3 (end-to-end recall re-verification) → Task 3; §4 (adaptive saturation explicitly not pursued) → no task, correctly absent.
- **Placeholder scan:** no TBD/TODO. Task 2 Step 4/5's line numbers are flagged as approximate ("exact line number may have shifted") rather than asserted as exact — this is an honest hedge given the file has been edited by several prior tasks since these numbers were last confirmed, not a placeholder; the actual code to find/replace is given in full either way.

## Task 3 Results (recorded after the fix)

- **Recall@10:** 0.9890 (before, saturation firing during insert) → 0.9940 (after, saturation disabled during insert) — target was ~0.9940. Matched exactly, and reproduced identically (0.9940) across two independent full benchmark runs, since `bench_unif`'s seeded sequence is deterministic.
- **Insert wall-clock (10000 vectors):** 10.1057801s / 989.5 inserts/sec (before) → 8.0171388s / 1247.3 inserts/sec (run 1, after) — 20.7% *faster*, not slower. A second independent run measured 6.3003606s / 1587.2 inserts/sec — 37.7% faster than baseline, and itself 21% faster than run 1.
- **Concern — insert-time direction contradicts the design doc's prediction.** The design doc and this plan expected insert to get *slower* after disabling saturation during construction (more distance evaluations per `search_layer` call with no early exit). Both measured runs show insert getting *faster* instead, and the two after-fix runs disagree with each other by ~21%, which is larger than the "slowdown" the design anticipated. This makes the `Instant`-based single-sample wall-clock insert timing too noisy on this shared machine to reliably attribute a direction to the code change alone — it does not, on its own, indicate the fix is behaving incorrectly (the deterministic, exactly-reproduced recall number is strong evidence the saturation-gating logic itself is correct), but the insert-time claim in the design doc should not be treated as confirmed by these numbers. Flagging for reviewer judgment per the design doc §3 rather than resolving unilaterally.
- **Type consistency:** `search_layer`'s new `saturate: bool` parameter position (last, after `filter`) is consistent between Task 2 Step 3 (definition) and every call site update in Steps 1, 4, and 5.
