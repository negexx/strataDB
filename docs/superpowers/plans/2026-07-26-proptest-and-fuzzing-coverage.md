# Property-Based Testing and Fuzzing Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add property-based tests for Strata's core row/conflict-arithmetic invariants (row-id ranges, OCC conflict detection, predicate filtering) and two new `cargo-fuzz` targets for untrusted-input parsing paths (delta-log entries, Arrow IPC data files) that `cargo test`'s example-based unit tests structurally can't cover.

**Architecture:** Property tests live inline in each target module's existing `#[cfg(test)] mod tests`, matching this project's established convention (no centralized top-level test binary) — each asserts the real function's output against an independent, deliberately-naive reference implementation across randomly generated inputs, using `proptest`. Fuzz targets are added to the *existing* `fuzz/` cargo-fuzz workspace (already has one target, `manifest_parse.rs`, fuzzing manifest JSON deserialization) — this plan adds two more targets to that same harness, not a new fuzzing setup.

**Tech Stack:** `proptest` (new dev-dependency), `cargo-fuzz` / `libfuzzer-sys` (already present in `fuzz/Cargo.toml`).

## Global Constraints

- `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check` must all stay clean after every task (per `.claude/CLAUDE.md`'s "what done means").
- Every new test is TDD'd: write it, watch it fail for the right reason (either the function doesn't exist yet, or — for these property tests, which check *existing* functions — a deliberately-wrong reference implementation first, per Task 2's note), then make it pass.
- `crates/txn`'s row-id/conflict-detection code is flagship-subsystem code per `.claude/rules/concurrency-txn-layer.md` — nothing in this plan changes any of that code's actual logic, only adds tests, so no new `loom` test is needed (loom is for concurrency interleavings; these are pure, single-threaded, deterministic functions).
- `fuzz/` is its own separate Cargo workspace (`[workspace]` with no members, per `fuzz/Cargo.toml`) — its dependencies are deliberately outside `cargo deny check`'s scope (confirmed during this project's cargo-deny hardening pass) and outside the main workspace's `cargo build`/`cargo test`. Building/running fuzz targets uses `cargo fuzz run <target>` from inside `fuzz/`, not the main workspace commands.
- `proptest`'s default 256 cases per test, run in `cargo test`'s normal debug profile, must complete in well under a second each — if a property test is slow, reduce `proptest::test_runner::Config::cases` explicitly in that test rather than leaving a slow default in the normal `cargo test --workspace` path.

---

### Task 1: Add `proptest` as a workspace dev-dependency

**Files:**
- Modify: `Cargo.toml` (root, `[workspace.dependencies]`)
- Modify: `crates/txn/Cargo.toml` (`[dev-dependencies]`)
- Modify: `crates/query/Cargo.toml` (`[dev-dependencies]`)

**Interfaces:**
- Produces: `proptest` available via `proptest.workspace = true` in any crate's `[dev-dependencies]`, and the `proptest::prelude::*` / `proptest!` macro usable in that crate's `#[cfg(test)]` code.

- [ ] **Step 1: Add `proptest` to the root workspace dependencies**

In `Cargo.toml`, in the `[workspace.dependencies]` table (alongside the existing `arrow`, `criterion`, `thiserror`, etc.), add:

```toml
proptest = "1"
```

- [ ] **Step 2: Add it as a dev-dependency to `crates/txn` and `crates/query`**

In `crates/txn/Cargo.toml`, under the existing `[dev-dependencies]` section (which currently has `loom` and `tempfile`), add:

```toml
proptest = { workspace = true }
```

In `crates/query/Cargo.toml`, add a `[dev-dependencies]` section (it currently has none) with:

```toml
[dev-dependencies]
proptest = { workspace = true }
```

- [ ] **Step 3: Verify it resolves and builds**

Run: `cargo check -p strata-txn -p strata-query --all-targets`
Expected: clean build, no errors (nothing uses `proptest` yet, so this only proves the dependency resolves).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/txn/Cargo.toml crates/query/Cargo.toml
git commit -m "build: add proptest as a workspace dev-dependency"
```

---

### Task 2: Property test — `RowIdRange::contains` matches a naive reference

**Files:**
- Modify: `crates/txn/src/row_id.rs` (add to its existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `RowIdRange { base: u64, len: u64 }` (already defined, `pub(crate)`, fields are `pub(crate)`) and its existing `fn contains(self, row_id: u64) -> bool` method (`crates/txn/src/row_id.rs:81-92`).
- Produces: nothing new for later tasks — this is a leaf test.

This property test exists because `contains`'s real implementation (`row_id >= self.base && row_id - self.base < self.len`) was deliberately written as a subtraction specifically to avoid an overflow bug a more obvious `row_id < self.base + self.len` formulation would have (per its own doc comment) — that is exactly the kind of arithmetic edge case example-based tests are easy to under-cover and property-based random inputs are good at finding.

- [ ] **Step 1: Write the failing test**

Find the existing `#[cfg(test)] mod tests` block in `crates/txn/src/row_id.rs` (search for `mod tests`) and add, using `use proptest::prelude::*;` at the top of that module if not already present:

**Correction, found during execution:** a first version of this test used bare `base: u64, len: u64, row_id: u64` (fully independent random generation for all three). Verified directly (not assumed): injecting the `row_id <= base + len` off-by-one below and re-running with independent-random generation still passed clean — hitting `row_id == base + len` exactly by pure chance across `u64`'s range is astronomically unlikely, so that version had essentially zero power to catch the exact boundary bug it existed to check. Use this corrected strategy instead, which mixes fully-random `row_id` values with several boundary-adjacent candidates relative to `(base, len)`:

```rust
    proptest! {
        // `row_id` is NOT independently random -- see this task's own
        // "Correction, found during execution" note for why a bare
        // `any::<u64>()` has ~zero power to catch a boundary off-by-one.
        // `row_id` is instead drawn from a mix of a fully random value AND
        // several boundary-adjacent candidates relative to `(base, len)` --
        // `base - 1`, `base`, `base + len - 1`, `base + len`, and
        // `base + len + 1` (wrapping, so this never panics at u64's own
        // edges) -- so the exact boundary this range type's contract turns
        // on is actually exercised, not just plausible-by-volume.
        #[test]
        fn contains_matches_naive_range_check(
            (base, len, row_id) in (any::<u64>(), any::<u64>()).prop_flat_map(|(base, len)| {
                let boundary_candidates = vec![
                    base.wrapping_sub(1),
                    base,
                    base.wrapping_add(len).wrapping_sub(1),
                    base.wrapping_add(len),
                    base.wrapping_add(len).wrapping_add(1),
                ];
                (
                    Just(base),
                    Just(len),
                    prop_oneof![any::<u64>(), prop::sample::select(boundary_candidates)],
                )
            })
        ) {
            let range = RowIdRange { base, len };
            let actual = range.contains(row_id);
            // Deliberately naive reference: computed with u128 so the
            // reference itself can never overflow, independent of whatever
            // technique the real implementation uses to avoid overflow.
            let naive = {
                let base = u128::from(base);
                let len = u128::from(len);
                let row_id = u128::from(row_id);
                row_id >= base && row_id < base + len
            };
            prop_assert_eq!(actual, naive);
        }
    }
```

- [ ] **Step 2: Run it to make sure it fails for the right reason first (sanity check the harness, not the function)**

This specific test should actually pass immediately, since `contains` is already correct — there's no RED step here in the usual "function doesn't exist yet" sense. Instead, verify the *test itself* can fail by temporarily changing `naive`'s condition to `row_id <= base + len` (off-by-one) and confirming `cargo test` reports a `prop_assert_eq` failure with a shrunk counterexample, then revert that temporary change back to `<`. This was directly re-verified with the corrected strategy above: the injected off-by-one produced a real, proptest-shrunk minimal counterexample where `row_id == base + len` exactly, confirming the boundary-adjacent candidates actually get exercised (not just theoretically included).

Run: `cargo test -p strata-txn --lib -- contains_matches_naive_range_check`
Expected (with the temporary off-by-one): FAIL, with proptest printing a minimal `(base, len, row_id)` counterexample.
Then revert the temporary change, and delete `crates/txn/proptest-regressions/row_id.txt` if the failing run created it — that regression file pins the just-injected bug's seed, not a real regression in the correct code, so it shouldn't be committed.

- [ ] **Step 3: Run it for real**

Run: `cargo test -p strata-txn --lib -- contains_matches_naive_range_check`
Expected: PASS (256 cases, per proptest's default).

- [ ] **Step 4: Commit**

```bash
git add crates/txn/src/row_id.rs
git commit -m "test(txn): property-test RowIdRange::contains against a naive reference"
```

---

### Task 3: Property test — `CommitLog::conflicts_with` matches a naive reference conflict check

**Files:**
- Modify: `crates/txn/src/commit_log.rs` (add to its existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `CommitLog::new(capacity: usize)`, `CommitLog::push(&mut self, version: u64, write_set: Vec<u64>)`, `CommitLog::conflicts_with(&self, since_version: u64, up_to_version: u64, write_set: &[u64]) -> ConflictCheck` (all `pub`, `crates/txn/src/commit_log.rs:55-88`), and `ConflictCheck` (`pub enum { Clean, Conflict(Vec<u64>), InsufficientHistory }`, derives `PartialEq`).
- Produces: nothing new for later tasks.

This is the highest-value property test in this plan: `.claude/rules/concurrency-txn-layer.md` states "two transactions touching disjoint rows must never spuriously conflict" as a hard correctness invariant, and `conflicts_with` has a real optimization (linear scan vs. hashing, see `HASH_WRITE_SET_ABOVE`) whose two code paths must agree — a property test comparing against a naive reference exercises both without needing to special-case which one runs.

- [ ] **Step 1: Write the failing test**

Add to `crates/txn/src/commit_log.rs`'s `#[cfg(test)] mod tests` (add `use proptest::prelude::*;` at the top of the module if not already present):

**Corrections, found during execution** (two, both verified directly, not assumed):

1. `committed` as independently generated is unordered and possibly has duplicate versions, but `CommitLog::push` has an implicit precondition (its own doc comment, and the `front()`-is-oldest logic `conflicts_with` relies on) that pushes happen in strictly ascending version order, matching every real caller (always `latest_version + 1` under `commit_lock`). Sort and deduplicate `committed` by version before both the push loop and the naive reference — this doesn't change what either side computes (`.min()` and the `contested` filter/collect are already order-independent over the same set of pairs), it only removes inputs `CommitLog` itself could never actually be given.
2. `since_version`/`up_to_version` independently random over `0u64..1000`, same lesson as Task 2's `RowIdRange::contains` test: verified directly that an injected off-by-one on the version-range filter passed clean across multiple fresh 64-case runs with this generation, because exact equality against one of `committed`'s actual versions — what the `(since_version, up_to_version]` boundary turns on — is unlikely to be hit by independent random draws even over a comparatively small 1000-wide range once you also need a write-set overlap on the SAME entry. Fixed by generating `since_version`/`up_to_version` as a mix of independent random values and version-adjacent candidates (each committed version, and one below/above it), and by raising the case count from 64 to 2000 (still well under a second — verified at ~0.2-0.4s per run) since even the corrected strategy needs enough cases to hit the *joint* condition (version-boundary hit AND write-set overlap), not just enough to hit the version boundary alone.

```rust
    proptest! {
        // 64 cases (picked for per-case cost: up to 20 pushes plus a full
        // scan) turned out too few to reliably hit the joint condition an
        // injected boundary bug needs to surface -- verified directly: it
        // passed clean across multiple fresh 64-case runs, but reliably
        // failed (with a real shrunk counterexample) once raised. 2000
        // stays well under a second with comfortable margin.
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn conflicts_with_matches_a_naive_reference(
            (committed, since_version, up_to_version, write_set) in
                prop::collection::vec(
                    (1u64..1000, prop::collection::vec(0u64..50, 0..8)),
                    0..20,
                )
                .prop_flat_map(|committed| {
                    // since_version/up_to_version are NOT independently
                    // random -- see this task's own "Corrections, found
                    // during execution" note #2.
                    let mut version_candidates: Vec<u64> = committed
                        .iter()
                        .flat_map(|(v, _)| [v.saturating_sub(1), *v, v.saturating_add(1)])
                        .collect();
                    version_candidates.push(0); // always non-empty, even if committed is empty
                    (
                        Just(committed),
                        prop_oneof![0u64..1000, prop::sample::select(version_candidates.clone())],
                        prop_oneof![0u64..1000, prop::sample::select(version_candidates)],
                        prop::collection::vec(0u64..50, 0..8),
                    )
                })
        ) {
            // committed is unordered/possibly-duplicated as generated --
            // see this task's own "Corrections, found during execution"
            // note #1.
            let mut committed = committed;
            committed.sort_by_key(|(v, _)| *v);
            committed.dedup_by_key(|(v, _)| *v);

            let mut log = CommitLog::new(64); // capacity well above `committed`'s max length (20), so nothing evicts
            for (version, ws) in &committed {
                log.push(*version, ws.clone());
            }

            let actual = log.conflicts_with(since_version, up_to_version, &write_set);

            // Naive reference: since capacity (64) never evicts here, "the
            // log's oldest entry is newer than since_version" can only be
            // true if `committed` itself never covers back that far -- an
            // empty log with a non-empty requested range is the same case.
            let history_gap = match committed.iter().map(|(v, _)| *v).min() {
                Some(oldest) => oldest > since_version + 1,
                None => up_to_version > since_version,
            };
            let naive = if write_set.is_empty() || up_to_version <= since_version {
                ConflictCheck::Clean
            } else if history_gap {
                ConflictCheck::InsufficientHistory
            } else {
                let mut contested: Vec<u64> = committed
                    .iter()
                    .filter(|(v, _)| *v > since_version && *v <= up_to_version)
                    .flat_map(|(_, ws)| ws.iter().copied())
                    .filter(|row| write_set.contains(row))
                    .collect();
                contested.sort_unstable();
                contested.dedup();
                if contested.is_empty() {
                    ConflictCheck::Clean
                } else {
                    ConflictCheck::Conflict(contested)
                }
            };

            // Conflict(_)'s row-id ORDER isn't part of its contract (only
            // which rows are contested), so compare as sorted+deduped sets
            // rather than requiring the real implementation's iteration
            // order to match the naive reference's.
            let normalize = |c: ConflictCheck| match c {
                ConflictCheck::Conflict(mut rows) => {
                    rows.sort_unstable();
                    rows.dedup();
                    ConflictCheck::Conflict(rows)
                }
                other => other,
            };
            prop_assert_eq!(normalize(actual), normalize(naive));
        }
    }
```

- [ ] **Step 2: Run it to verify the harness can fail (same sanity-check pattern as Task 2)**

Temporarily change the naive reference's version-range filter from `*v > since_version && *v <= up_to_version` to `*v >= since_version && *v <= up_to_version` (off-by-one on the exclusive lower bound), run, confirm a shrunk counterexample fails, then revert. This was directly re-verified with the corrected strategy and case count above: the injected off-by-one reliably fails (3/3 fresh runs) with a real proptest-shrunk counterexample.

Run: `cargo test -p strata-txn --lib -- conflicts_with_matches_a_naive_reference`
Expected (with the temporary off-by-one): FAIL.
Then revert the temporary change, and delete `crates/txn/proptest-regressions/commit_log.txt` if the failing run created it.

- [ ] **Step 3: Run it for real**

Run: `cargo test -p strata-txn --lib -- conflicts_with_matches_a_naive_reference`
Expected: PASS (2000 cases — raised from the plan's original 64, see this task's "Corrections" note; still well under a second, ~0.2-0.4s measured).

- [ ] **Step 4: Commit**

```bash
git add crates/txn/src/commit_log.rs
git commit -m "test(txn): property-test CommitLog::conflicts_with against a naive reference"
```

---

### Task 4: Property test — `mask`/`filter` matches a naive per-row reference

**Files:**
- Modify: `crates/query/src/predicate.rs` (add to its existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `Predicate::{Eq, Lt, LtEq, Gt, GtEq}(String, Value)` (`crates/query/src/predicate.rs:14-20`), `pub fn mask(batch: &RecordBatch, predicate: &Predicate) -> Result<BooleanArray, ArrowError>` (`:71-75`), and `strata_storage::Value::{Int64, Float64, Utf8}` (derives `PartialEq, PartialOrd`).
- Produces: nothing new for later tasks.

Scoped to a single `Int64` column (not every `Value` variant × every column type) — this is a deliberate, valuable-but-bounded slice, not exhaustive predicate/type-combination coverage. `Int64` is chosen because it has no floating-point comparison edge cases (`NaN`, `-0.0`) to reason about, keeping this task's naive reference unambiguous; broadening to `Float64`/`Utf8` columns is a natural follow-up, not included here.

- [ ] **Step 1: Write the failing test**

Add to `crates/query/src/predicate.rs`'s `#[cfg(test)] mod tests` (add `use proptest::prelude::*;` at the top of that module if not already present; the module already imports `RecordBatch`, `Int64Array`, etc. for its existing tests — reuse those imports):

**Design note, applied proactively before this task's own execution** (based on the same lesson Tasks 2 and 3 each independently rediscovered and had to fix mid-execution): `Predicate::Eq`'s naive arm is `v == threshold` — with `threshold` drawn fully independently from `values`, this has the same near-zero-power problem as `RowIdRange::contains`'s `row_id == base + len` and `conflicts_with`'s `since_version == committed_version`. Rather than let this task rediscover the same issue a third time, `threshold` is generated as a mix of fully random `i64` and one of `values`' actual entries up front. Still verify this empirically in Step 2 below, exactly as the prior two tasks did — a reasoned prediction is not a substitute for actually running it.

```rust
    proptest! {
        #[test]
        fn mask_matches_a_naive_per_row_reference(
            (values, threshold) in prop::collection::vec(any::<i64>(), 0..30)
                .prop_flat_map(|values| {
                    // threshold is NOT independently random -- see this
                    // task's own "Design note" above. Predicate::Eq's
                    // naive arm needs threshold to actually equal one of
                    // values' entries some of the time, or that variant's
                    // true branch is never meaningfully exercised.
                    let mut candidates = values.clone();
                    candidates.push(0); // always non-empty, even if values is empty
                    (Just(values), prop_oneof![any::<i64>(), prop::sample::select(candidates)])
                }),
            variant in 0..5u8,
        ) {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(values.clone()))],
            ).unwrap();

            let predicate = match variant {
                0 => Predicate::Eq("id".to_string(), Value::Int64(threshold)),
                1 => Predicate::Lt("id".to_string(), Value::Int64(threshold)),
                2 => Predicate::LtEq("id".to_string(), Value::Int64(threshold)),
                3 => Predicate::Gt("id".to_string(), Value::Int64(threshold)),
                _ => Predicate::GtEq("id".to_string(), Value::Int64(threshold)),
            };

            let actual = mask(&batch, &predicate).unwrap();

            let naive: Vec<bool> = values
                .iter()
                .map(|&v| match variant {
                    0 => v == threshold,
                    1 => v < threshold,
                    2 => v <= threshold,
                    3 => v > threshold,
                    _ => v >= threshold,
                })
                .collect();

            prop_assert_eq!(actual.len(), naive.len());
            for (i, expected) in naive.iter().enumerate() {
                prop_assert_eq!(actual.value(i), *expected, "row {}", i);
            }
        }
    }
```

- [ ] **Step 2: Run it to verify the harness can fail**

**Correction, found during execution:** flipping `variant => 0`'s naive arm from `v == threshold` to `v != threshold` is a full boolean negation, not an equality-boundary bug — it fails on almost any non-empty input regardless of whether `threshold` ever actually equals an entry, so "it failed" doesn't prove the `prop_flat_map` correlation strategy matters, only that *some* bug is detectable (verified directly: this flip fails readily even with the correlation stripped back to bare `any::<i64>()`). Use this check instead: temporarily inject a realistic off-by-one into `compare()`'s actual `Eq` match arm in `crates/query/src/predicate.rs` (production code, not the test's naive reference) — one that's wrong only on the branch where a real match occurs. Run, confirm failure with a shrunk counterexample where `threshold` actually equals one of `values`' entries; then temporarily strip the correlation (`threshold`'s strategy back to bare `any::<i64>()`) with the same bug still injected, and confirm it now goes UNDETECTED across a few fresh runs — this is what actually proves the correlation is load-bearing, not just that some bug is catchable. Revert both temporary changes (the production off-by-one and the correlation strip) back to the shipped state.

Separately, do the simpler sanity check for `variant => 1`'s naive arm (`v < threshold` flipped to `v <= threshold`), which doesn't depend on the correlation (any random threshold exercises `Lt` meaningfully) and should fail readily on its own.

Run: `cargo test -p strata-query --lib -- mask_matches_a_naive_per_row_reference`
Expected (with each temporary change): FAIL, with a real proptest-shrunk counterexample; the correlation-dependent check should also demonstrate the reverse (undetected without correlation).
Then revert every temporary change, and delete `crates/query/proptest-regressions/predicate.txt` if a failing run created it.

- [ ] **Step 3: Run it for real**

Run: `cargo test -p strata-query --lib -- mask_matches_a_naive_per_row_reference`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/query/src/predicate.rs
git commit -m "test(query): property-test predicate mask against a naive per-row reference"
```

---

### Task 5: Fuzz target — delta-log entry deserialization

**Files:**
- Create: `fuzz/fuzz_targets/delta_log_parse.rs`
- Modify: `fuzz/Cargo.toml`

**Interfaces:**
- Consumes: `strata_index::DeltaEntry` (`pub enum { Insert { row_id: u64, vector: Vec<f32> }, Tombstone { row_id: u64 } }`, derives `Deserialize`, `crates/index/src/delta_log.rs:18-21`).
- Produces: nothing later tasks depend on.

Mirrors this plan's already-existing sibling target, `fuzz/fuzz_targets/manifest_parse.rs` (fuzzes `serde_json::from_slice::<strata_storage::Manifest>` directly against arbitrary bytes) — same pattern, applied to the delta log's per-line entry format instead. Directly motivated by a real bug found and fixed in this project's history: a `DeltaEntry::Insert` with a zero-length `vector` was, before that fix, silently accepted during replay and durably poisoned the dataset. This fuzz target's job is catching the *next* malformed-shape issue in this exact deserialization path before it reaches production data, not re-proving that specific already-fixed bug (which has its own regression tests in `crates/txn/src/dataset.rs`).

- [ ] **Step 1: Add the new fuzz binary to `fuzz/Cargo.toml`**

Add a second `[dependencies]` entry and a second `[[bin]]` section (after the existing `manifest_parse` one):

```toml
[dependencies.strata-index]
path = "../crates/index"
```

```toml
[[bin]]
name = "delta_log_parse"
path = "fuzz_targets/delta_log_parse.rs"
test = false
doc = false
bench = false
```

- [ ] **Step 2: Write the fuzz target**

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzzes the actual delta-log entry deserialization step
// (`strata_index::delta_log::read_delta_log`'s internal
// `serde_json::from_str::<DeltaEntry>(line)` call, exercised here via
// `from_slice` since fuzz input is raw bytes, not necessarily valid UTF-8)
// directly against arbitrary bytes -- this is the real untrusted-input
// surface: a corrupted disk, a downgraded binary writing an older delta-log
// shape, or a hand-edited/pre-fix log entry could all hand a reader exactly
// this. Must never panic; returning an error for garbage input is correct
// and expected.
fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<strata_index::DeltaEntry>(data);
});
```

- [ ] **Step 3: Verify it builds**

Run (from the `fuzz/` directory): `cargo +nightly fuzz build delta_log_parse`
Expected: clean build (no `cargo fuzz run` yet — that runs indefinitely and isn't a pass/fail step to script here; building alone catches signature/dependency mistakes).

- [ ] **Step 4: Run it briefly to confirm it actually executes**

Run (from the `fuzz/` directory): `cargo +nightly fuzz run delta_log_parse -- -max_total_time=30`
Expected: runs for ~30 seconds, reports an iteration/exec count, no crash.

- [ ] **Step 5: Commit**

```bash
git add fuzz/Cargo.toml fuzz/fuzz_targets/delta_log_parse.rs
git commit -m "test(fuzz): add a fuzz target for delta-log entry deserialization"
```

---

### Task 6: Fuzz target — Arrow IPC data-file parsing

**Files:**
- Create: `fuzz/fuzz_targets/datafile_parse.rs`
- Modify: `fuzz/Cargo.toml`

**Interfaces:**
- Consumes: `arrow::ipc::reader::FileReader` directly (not `strata_storage::read_batch`, which only accepts a `&Path`/`File` — fuzzing needs an in-memory reader; `FileReader::try_new` is generic over `R: Read + Seek`, and a `std::io::Cursor<&[u8]>` satisfies that, so this exercises the identical parsing logic `read_batch` (`crates/storage/src/datafile.rs:69-76`) wraps, just fed from memory instead of a real file).
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Add the new fuzz binary to `fuzz/Cargo.toml`**

Add an `arrow` dependency and a third `[[bin]]` section:

```toml
[dependencies]
arrow = "58"
```

```toml
[[bin]]
name = "datafile_parse"
path = "fuzz_targets/datafile_parse.rs"
test = false
doc = false
bench = false
```

- [ ] **Step 2: Write the fuzz target**

```rust
#![no_main]

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use libfuzzer_sys::fuzz_target;

// Fuzzes the same Arrow IPC parsing path `strata_storage::read_batch`
// wraps (`crates/storage/src/datafile.rs`), fed from an in-memory buffer
// instead of a real file so libFuzzer can drive it directly against
// arbitrary bytes. This is the real untrusted-input surface for data files:
// a corrupted disk or a partially-written file after a crash mid-write
// (exactly what this project's Phase 7 chaos harness injects) could hand a
// reader exactly this. Must never panic; returning an error for garbage
// input is correct and expected.
fuzz_target!(|data: &[u8]| {
    if let Ok(mut reader) = FileReader::try_new(Cursor::new(data), None) {
        for batch in reader.by_ref() {
            let _ = batch;
        }
    }
});
```

- [ ] **Step 3: Verify it builds**

Run (from the `fuzz/` directory): `cargo +nightly fuzz build datafile_parse`
Expected: clean build.

- [ ] **Step 4: Run it briefly to confirm it actually executes**

Run (from the `fuzz/` directory): `cargo +nightly fuzz run datafile_parse -- -max_total_time=30`
Expected: runs for ~30 seconds, reports an iteration/exec count, no crash.

- [ ] **Step 5: Commit**

```bash
git add fuzz/Cargo.toml fuzz/fuzz_targets/datafile_parse.rs
git commit -m "test(fuzz): add a fuzz target for Arrow IPC data-file parsing"
```

---

### Task 7: Full workspace verification

**Files:** none (verification only)

- [ ] **Step 1: Full build**

Run: `cargo build --workspace`
Expected: clean, no warnings.

- [ ] **Step 2: Full test suite**

Run: `cargo test --workspace`
Expected: all pass, including the three new property tests from Tasks 2-4.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. (`fuzz/` is its own workspace and isn't covered by this command — that's expected, per this plan's Global Constraints.)

- [ ] **Step 4: Format check**

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 5: Send the whole plan's diff through the `reviewer` subagent**

Per `.claude/CLAUDE.md`'s "what done means" — no task is marked done without this, regardless of which model implemented it.
