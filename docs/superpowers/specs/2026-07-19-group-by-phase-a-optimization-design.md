# GROUP BY Phase A Optimization — Design

**Date:** 2026-07-19
**Status:** Approved for implementation planning

## 1. Goal and scope

`crates/query/src/group_by.rs`'s hash-based `GROUP BY` kernel allocates far
more than it needs to under high-cardinality grouping. This design covers
**Phase A only**: replacing the lookup/state-storage internals with a
unified row-keyed index and columnar accumulator state, **as a pure
internal implementation swap** — `group_by`'s public signature, error
conditions, output schema, and all numeric semantics (Float64 coercion,
null handling, identity values) stay byte-for-byte identical. The existing
`crates/query/src/group_by.rs` test suite is the correctness oracle; no
new behavior is being added, so no existing test's expected output
changes.

**Explicitly out of scope**, per prior discussion in this design session:

- **Phase B** (native-width accumulation instead of upfront `Float64`
  casting, dictionary-key-only grouping bypassing `RowConverter`) —
  deferred; only worth doing once Phase A is benchmarked and the `Float64`
  cast is confirmed to actually matter.
- **Sorted-streaming aggregation** — dropped, not deferred. `group_by` is
  single-`RecordBatch`-in/single-`RecordBatch`-out with no multi-batch
  pipeline concept, and Strata's storage layer tracks no sort-order
  metadata anywhere a "this input happens to be pre-sorted" signal could
  come from. Out of scope for this project's stated non-goal of avoiding
  DataFusion-scale query engine machinery.
- A `hashbrown::raw::RawTable`-based index — superseded by a simpler
  mechanism found while designing this (§3).

## 2. Baseline: what's actually expensive today

The kernel this replaces (`crates/query/src/group_by.rs:98`,
`crates/query/src/group_by.rs:168-193`):

```rust
let mut groups: HashMap<OwnedRow, Vec<Accumulator>> = HashMap::new();
for i in 0..batch.num_rows() {
    let key = rows.row(i).owned();
    let accs = groups.entry(key).or_insert_with(|| { /* one Accumulator per (col, func) */ });
    // ... update each accumulator ...
}
```

Two corrections to the motivating proposal this design responds to, both
confirmed by reading the actual code and the actual `arrow-row 58.3.0`
source (not assumed):

1. **`Accumulator` is not a boxed trait object.** It's
   `#[derive(Debug, Clone, Copy)] enum Accumulator { Count(u64), Sum(f64),
   Min(f64), Max(f64), Avg { sum: f64, count: u64 } }` — a small `Copy`
   value. `Vec<Accumulator>` (one per group) is one contiguous heap
   allocation of small values, not a scattered array of boxed pointers.
2. **The real dominant cost is worse than "one allocation per group."**
   `rows.row(i).owned()` runs on **every row**, not once per distinct
   group — `HashMap::entry` requires an owned key up front even when the
   row belongs to an already-seen group, so today's code heap-allocates a
   fresh `OwnedRow` (a `Vec<u8>` byte buffer) **N times** for N input
   rows, discarding almost all of them immediately after the lookup. This
   is the actual bottleneck this design fixes.

`build_result_batch`'s `owned_keys: Vec<OwnedRow> = groups.keys().cloned().collect()`
adds a second full clone of every group's key on the way out — also
eliminated by this design (§3), as a byproduct rather than a separate
goal.

## 3. Mechanism: `HashMap<Row<'_>, usize>` over the existing `Rows` buffer

`crates/query/src/group_by.rs:148` already builds
`rows: Rows = converter.convert_columns(&group_arrays)?` — one contiguous
buffer holding every input row's group-key bytes, in one pass, before the
per-row loop starts. That buffer already exists today; it's just discarded
per-row via `.owned()` instead of being read from directly.

`arrow_row::Row<'a>` (confirmed against the vendored `arrow-row-58.3.0`
source, `src/lib.rs:1442-1492`) is `{ data: &'a [u8], config: &'a
RowConfig }`, and already implements `Hash`, `Eq`, `Ord`, all defined
purely over `self.data` (the byte slice) — no allocation, no custom logic
needed. That means a plain `std::collections::HashMap<Row<'a>, usize>`
(borrowed row → group index) does everything a hand-rolled
`hashbrown::raw::RawTable` index would have been built to do — hash/compare
rows without materializing an `OwnedRow` per probe — using only std,
`arrow_row`'s existing public API (`Rows::row`, confirmed at
`arrow-row-58.3.0/src/lib.rs:1257`), and zero new dependencies. This
supersedes the originally-proposed `hashbrown::raw::RawTable<(u64,
usize)>` design: same architectural goal (unified contiguous row storage +
a hash index over it, no per-row key allocation), less code, no raw-table
rehash-closure footgun, no `unsafe`.

## 4. Components

```rust
enum ColumnarAccumulator {
    Count(Vec<u64>),
    Sum(Vec<f64>),
    Min(Vec<f64>),
    Max(Vec<f64>),
    Avg { sum: Vec<f64>, count: Vec<u64> },
}
```

Replaces the scalar `Accumulator` enum entirely. Same per-variant identity
values and update/finish math as today's `Accumulator::new` /
`Accumulator::update` / `Accumulator::finish` — reshaped so each variant
holds one `Vec<T>` indexed by `group_idx`, instead of one `Accumulator`
instance existing per group:

- `push_identity()` — appends this variant's identity element (`Count`→0,
  `Sum`→0.0, `Min`→`f64::INFINITY`, `Max`→`f64::NEG_INFINITY`,
  `Avg`→`{0.0, 0}`), called for every requested aggregate the moment a
  group is first seen (mirrors today's `or_insert_with` fully
  initializing a group's entire `Vec<Accumulator>` up front, regardless of
  which columns happen to be null on the row that discovered the group).
- `update(group_idx, value)` — same per-variant math as
  `Accumulator::update`, indexed instead of `self`-mutating.
- `finish_all(self) -> Vec<AggValue>` — consumes the whole vector at once
  (`Count`→cast to `i64`, `Sum`/`Min`/`Max`→pass through as `f64`,
  `Avg`→`sum[i] / count[i] as f64`), replacing the current per-group
  `Accumulator::finish` dispatch.

`AggValue` (already exists, unchanged) remains the bridge type into
`finish_agg_column`.

## 5. Algorithm

Unchanged through row-key construction (validate `group_cols`, extract
`group_arrays`/`agg_arrays` with the existing Float64-cast-except-`Count`
rule, build `RowConverter`, call `convert_columns`). The per-row loop and
output construction change to:

```rust
let mut group_index_of: HashMap<Row<'_>, usize> = HashMap::new();
let mut group_key_rows: Vec<Row<'_>> = Vec::new();
let mut state: Vec<ColumnarAccumulator> =
    aggs.iter().map(|(_, f)| ColumnarAccumulator::new(*f)).collect();

for i in 0..batch.num_rows() {
    let row = rows.row(i);
    let group_idx = *group_index_of.entry(row).or_insert_with(|| {
        let idx = group_key_rows.len();
        group_key_rows.push(row);
        for acc in &mut state {
            acc.push_identity();
        }
        idx
    });
    for (agg_idx, (arr, func)) in agg_arrays.iter().enumerate() {
        if arr.is_null(i) {
            continue;
        }
        if matches!(func, AggFunc::Count) {
            state[agg_idx].update(group_idx, 0.0); // value unused by Count's update, matches today's Accumulator::update
            continue;
        }
        // Same defensive `if let Some` as today's code, not a new expect()/panic
        // path -- agg_float_arrays[agg_idx] is None only for Count, handled above.
        if let Some(col) = agg_float_arrays[agg_idx] {
            state[agg_idx].update(group_idx, col.value(i));
        }
    }
}

let group_columns = converter.convert_rows(group_key_rows)?; // same call as today, different key source
// ... one finish_all() per requested (column, AggFunc), assembled into the output RecordBatch exactly as build_result_batch does today.
```

Every `state[agg_idx]`'s internal vector length always equals
`group_key_rows.len()` by construction — `push_identity()` runs for every
accumulator whenever *any* new group is discovered, before any per-column
null check. `state[agg_idx].update(group_idx, ...)` is therefore always
safe, plain-indexed (no `unsafe`, no `get_unchecked`) — this project
defaults to safe Rust, and there's no benchmark evidence yet that the
bounds check costs anything worth reaching for `unsafe` over.

**Output row order remains unspecified**, same as today — the old
`HashMap<OwnedRow, _>`'s iteration order was never guaranteed either,
which is why every existing test sorts the result before asserting. Not a
behavior change.

## 6. Error handling

No new fallible paths. Every error condition (`schema.index_of` failures,
the non-numeric-column check for `Sum`/`Min`/`Max`/`Avg`,
`arrow::compute::cast` failures, `RowConverter::new`/`convert_columns`
failures) is validated before the row loop, exactly where it happens
today — none of that code moves. `HashMap::entry` is infallible; `state`
indexing is safe by the lockstep invariant in §5.

## 7. Testing

The existing `group_by.rs` test suite must pass unchanged — it's the
correctness oracle for the "byte-for-byte identical" constraint, not a set
of tests to update.

One new test: a differential check at real scale — a few thousand rows,
high cardinality (enough to force genuine hash collisions, unlike the
existing hand-written tests' 2-5 groups) — comparing the new
implementation's output against an independent naive-`HashMap` reference,
asserted as a *set* of `(group_key, agg_values)` tuples so the assertion
itself is row-order-independent rather than relying on the caller to sort.
This is the test that would catch a lockstep bug (accumulator state
vectors falling out of sync across columns) or a hash/eq inconsistency
silently merging two distinct groups — scale the existing small tests
don't reach.

No `loom` test: `group_by` is a synchronous, single-threaded pure function
over one already-in-memory `RecordBatch`. Nothing here touches shared
mutable state across threads.

## 8. Benchmark: cardinality sweep

Added to the existing `bench/benches/group_by_bench.rs` as a second
benchmark function (`bench_group_by_cardinality_sweep`), registered
alongside today's `bench_group_by` in the same file's `criterion_group!` —
no new `[[bench]]` entry in `bench/Cargo.toml`.

Fixed at 1,000,000 rows across all cases:

| cardinality | aggs | purpose |
|---|---|---|
| 1,000 groups | `Sum` | low end, comparable in shape to the existing 10M-row/1,000-category bench |
| 100,000 groups | `Sum` | mid point — where the baseline's cost curve starts bending |
| 1,000,000 groups | `Sum` | near-1:1 worst case — the scenario this design targets |
| 1,000,000 groups | `Count`+`Sum`+`Min`+`Max` | multiple aggregates × worst-case cardinality |

Each case gets its own correctness gate before timing (generalizing the
existing `check_correctness`/`naive_reference_sum` to also cover
`Count`/`Min`/`Max`), matching this project's "correctness before
throughput" convention. `sample_size` tuned down from the criterion
default given per-iteration cost at 1M rows, mirroring the existing
bench's `sample_size(10)`.

**Sequencing (this is the success gate):** the implementation plan runs
this benchmark against **today's** `HashMap<OwnedRow, Vec<Accumulator>>`
implementation first to record a real baseline per case, *then* implements
§3-§5, *then* re-runs the same benchmark. Phase A is only done once the
1,000,000-group case shows a measured improvement — an architecturally
"correct" rewrite with no measured win does not satisfy this design's
success criterion.

## 9. Success criteria

1. `group_by.rs`'s existing test suite passes unchanged.
2. The new differential/collision test (§7) passes.
3. `cargo build --workspace`, `cargo test --workspace`,
   `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`
   all clean.
4. The cardinality-sweep benchmark (§8) shows a measured improvement over
   the pre-change baseline at the 1,000,000-group case, with before/after
   numbers recorded (matching this project's existing precedent of
   recording real benchmark numbers in commit messages, not estimates).
5. Reviewed by the `reviewer` subagent before being marked done, per this
   project's standing rule.
