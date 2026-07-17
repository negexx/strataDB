# Phase 2 Spec — Real Encodings & Vectorized GROUP BY

**Status:** Implemented — see `crates/storage/src/encoding.rs`, `crates/query/src/group_by.rs`, `bench/benches/group_by_bench.rs`. This was the design deliverable for the "Columnar Core & Vectorized Execution" phase in `.claude/docs/architecture.md`'s roadmap (exit criterion: "`GROUP BY` over 10M+ rows, correct, benchmarked").

**Scope:** two independent additions, neither touching `crates/txn` or the manifest/commit protocol — Phase 2 is entirely about what's *inside* a data file and what operations run over scanned data. Does not implement Strata's own custom on-disk binary format (Phase 0 spec §6's STRA-magic-bytes footer design) — that remains a later, possibly unnecessary, decision. See "Alternatives considered" below for why.

## 1. Automatic dictionary encoding (`crates/storage::encoding`)

```rust
pub fn encode_batch(batch: &RecordBatch) -> Result<RecordBatch, StorageError>
```

For each column: compute distinct-value ratio (distinct count / row count) within the batch. Below a threshold (0.4 — in the range real columnar engines like Parquet default to), cast the column to `DictionaryArray<Int32Type>` via `arrow::compute::kernels::cast::cast`. At or above the threshold, leave the column as-is.

Called once inside `Transaction::commit`, before `write_batch`, for every pending batch. Transparent to `filter_eq`, which operates through `Array`/`Datum` trait interfaces rather than assuming a concrete primitive type — `brute_force_search` is *not* similarly transparent (it takes a concrete `&FixedSizeListArray` and requires a `Float32` child, erroring otherwise); its safety instead comes entirely from the read path below, same as every other caller. **`Dataset::scan` needed a real change, not a free ride** — this was wrong in the original version of this spec, caught by Phase 2's whole-branch review, not by any task-scoped review (each only saw one task's diff). Because `encode_batch` decides per-commit, independently, based on that commit's own data, two files backing the same logical column can legitimately end up with different physical types (one plain `Utf8`, another `Dictionary(Int32, Utf8)`) — and `concat_batches` requires every input batch to match a single schema exactly. `scan` now casts each file's columns to the caller's logical schema before concatenating (`crates/txn/src/dataset.rs`'s `cast_batch_to_schema`), which is what actually makes dictionary-encoded columns transparent to callers, not an automatic property of the encoding itself.

**Explicitly deferred:** run-end encoding (`RunArray`/REE) for genuinely run-heavy columns. Dictionary encoding alone is the meaningful win for the current MVP schema's shape (low-cardinality `name` column); REE is a reasonable follow-up, not part of this phase.

## 2. Hash-based GROUP BY (`crates/query::group_by`)

```rust
pub enum AggFunc { Count, Sum, Min, Max, Avg }

pub fn group_by(
    batch: &RecordBatch,
    group_cols: &[&str],
    aggs: &[(&str, AggFunc)],
) -> Result<RecordBatch, ArrowError>
```

- Group-by columns are converted to a comparable/hashable byte-row key via `arrow_row::RowConverter` — arrow-rs's own tool for multi-column keys, so row comparison/ordering isn't hand-rolled, only the hashing/accumulation loop around it.
- `HashMap<OwnedRow, Vec<Accumulator>>`, one accumulator per requested `(column, AggFunc)` pair. `Accumulator` is a small internal enum: `Count(u64)`, `Sum(f64)`, `Min(f64)`, `Max(f64)`, `Avg { sum: f64, count: u64 }`. Updated per row as the batch is scanned once. Every `AggFunc` except `Count` requires a numeric column and is coerced to `f64` via `arrow::compute::cast` before accumulation — Phase 2 does not preserve source integer precision in aggregate results (e.g. a `Sum` over very large `Int64` values may lose precision past `f64`'s 53-bit mantissa). This is an accepted simplification, not an oversight; revisit only if a real workload needs exact large-integer aggregation. `Count` is exempt from both the numeric-column requirement and the cast: it only null-checks its column (any type, including non-numeric), and its result column is `Int64`, not `Float64` like every other `AggFunc` — `Count` is semantically an integer, and casting a non-numeric column to `Float64` just to support it would be wrong (arrow's cast kernel would try to *parse* strings as numbers). A null value in an agg column is skipped, not treated as zero — see `group_by`'s own doc comment for the exact sentinel values (`Min`/`Max`/`Avg`) an all-null group produces.
- Final step: drain the `HashMap` into a `RecordBatch` — group-by columns first (in the order given), then one result column per `(column, AggFunc)` pair (in the order given).
- True O(n) in row count, chosen over a sort+partition alternative specifically for that guarantee (see "Alternatives considered").

### Error handling

`group_by` returns `Result<_, ArrowError>`, matching Phase 1's `filter_eq` precedent. `encode_batch` returns `Result<_, StorageError>` (`crates/storage`'s existing error type, which wraps `ArrowError` via `#[from]`) since it lives in `crates/storage` alongside that crate's other fallible I/O/serialization operations.

`group_by` errors (via `ArrowError::InvalidArgumentError`) on:
- an unknown column name in `group_cols` or `aggs`
- a non-numeric column passed to `Sum`/`Min`/`Max`/`Avg`
- an empty `group_cols` — "aggregate everything, no grouping" is a real, distinct operation this phase does not support; adding it later is additive, not a breaking change to this API shape

## 3. Testing

- **Encoding:** round-trip test (`encode_batch` → `write_batch` → `read_batch` → assert data-equivalence, ignoring the dictionary-vs-plain physical representation); a test confirming a batch with a genuinely low-cardinality column actually gets dictionary-encoded (assert the resulting column's `DataType` is `Dictionary(...)`), and a companion test confirming a high-cardinality column does *not*.
- **`group_by`:** hand-checked small cases — single-column grouping, multi-column grouping, each `AggFunc` individually, and at least one case combining multiple `(column, AggFunc)` pairs in one call. Plus the error paths listed above.
- **Exit criterion — the actual benchmark:** a `criterion` benchmark in `bench/`, generating 10M+ synthetic rows using a two-column `category`/`amount` schema sized for group-by-heavy workloads (not the MVP schema's id/name/vector shape — this benchmark exercises `group_by`'s kernel in isolation, not the full commit→scan→group_by pipeline), running `group_by`, and asserting correctness against a naively-computed reference (a simple, obviously-correct-but-slow grouping done with plain Rust `HashMap` + linear iteration over un-encoded data) before reporting throughput numbers. Correctness-against-a-reference matters more here than the throughput number itself — a fast wrong answer is worse than a slow right one.

## Alternatives considered

- **Sort + partition (lexsort_to_indices → take → partition → arrow's own aggregate kernels) instead of hash-based grouping:** rejected in favor of hash-based, specifically for the O(n) vs O(n log n) guarantee at the 10M+ row scale this phase's exit criterion targets. Costs more hand-written code (the hashing/accumulator loop) since arrow-rs doesn't ship a full GroupBy operator itself (that's DataFusion's job, already decided against — see `.claude/docs/architecture.md`'s "Small expression/filter API, no full SQL parser" principle) — but `RowConverter` still means row comparison/ordering itself isn't hand-rolled.
- **Strata's own custom on-disk binary format (Phase 0 spec §6) instead of Arrow's built-in encoded array types inside Arrow IPC files:** rejected for this phase. Arrow already provides `DictionaryArray`/`RunArray` as first-class types with a working reader/writer story; building a fully custom footer/column-chunk format now would be a much larger undertaking for the same "real encodings" outcome. The custom format remains a later, possibly-never-needed decision — revisit only if a concrete requirement Arrow's own types can't satisfy actually shows up (e.g. a specific need for byte-level control this project doesn't have yet).
- **Explicit, caller/schema-declared dictionary encoding instead of automatic heuristic-based encoding:** rejected — pushes a storage-layer optimization decision onto every caller (including the MVP schema and future Python-binding users), when real columnar engines (Parquet, Lance) make this call automatically by default.

## Non-goals for this phase

- `RunArray`/REE encoding (noted above — deferred, not rejected)
- Predicate pushdown / file-chunk pruning using the new encodings (that's Phase 3's stated scope)
- Strata's own custom on-disk format (see Alternatives above)
- `GROUP BY` with no grouping columns ("aggregate everything")
