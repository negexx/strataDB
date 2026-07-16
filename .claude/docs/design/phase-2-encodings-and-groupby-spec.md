# Phase 2 Spec — Real Encodings & Vectorized GROUP BY

**Status:** Draft — approved design, not yet implemented. This is the design deliverable for the "Columnar Core & Vectorized Execution" phase in `.claude/docs/architecture.md`'s roadmap (exit criterion: "`GROUP BY` over 10M+ rows, correct, benchmarked").

**Scope:** two independent additions, neither touching `crates/txn` or the manifest/commit protocol — Phase 2 is entirely about what's *inside* a data file and what operations run over scanned data. Does not implement Strata's own custom on-disk binary format (Phase 0 spec §6's STRA-magic-bytes footer design) — that remains a later, possibly unnecessary, decision. See "Alternatives considered" below for why.

## 1. Automatic dictionary encoding (`crates/storage::encoding`)

```rust
pub fn encode_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError>
```

For each column: compute distinct-value ratio (distinct count / row count) within the batch. Below a threshold (0.4 — in the range real columnar engines like Parquet default to), cast the column to `DictionaryArray<Int32Type>` via `arrow::compute::kernels::cast::cast`. At or above the threshold, leave the column as-is.

Called once inside `Transaction::commit`, before `write_batch`, for every pending batch — transparent to every existing caller (CLI, tests). No changes needed on the read side: Arrow's IPC reader deserializes `DictionaryArray` natively, and every existing consumer (`filter_eq`, `brute_force_search`, `Dataset::scan`) already operates through `Array`/`Datum` trait interfaces rather than assuming a concrete primitive type — dictionary-encoded columns flow through unchanged.

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
- `HashMap<OwnedRow, Vec<Accumulator>>`, one accumulator per requested `(column, AggFunc)` pair. `Accumulator` is a small internal enum: `Count(u64)`, `Sum(f64)`, `Min(f64)`, `Max(f64)`, `Avg { sum: f64, count: u64 }`. Updated per row as the batch is scanned once. Any numeric column type (`Int64`, `Float32`, `Float64`, etc.) is coerced to `f64` via `arrow::compute::cast` before accumulation — Phase 2 does not preserve source integer precision in aggregate results (e.g. a `Sum` over very large `Int64` values may lose precision past `f64`'s 53-bit mantissa). This is an accepted simplification, not an oversight; revisit only if a real workload needs exact large-integer aggregation.
- Final step: drain the `HashMap` into a `RecordBatch` — group-by columns first (in the order given), then one result column per `(column, AggFunc)` pair (in the order given).
- True O(n) in row count, chosen over a sort+partition alternative specifically for that guarantee (see "Alternatives considered").

### Error handling

Both functions return `Result<_, ArrowError>` — no new error type, matching Phase 1's `filter_eq` precedent.

`group_by` errors (via `ArrowError::InvalidArgumentError`) on:
- an unknown column name in `group_cols` or `aggs`
- a non-numeric column passed to `Sum`/`Min`/`Max`/`Avg`
- an empty `group_cols` — "aggregate everything, no grouping" is a real, distinct operation this phase does not support; adding it later is additive, not a breaking change to this API shape

## 3. Testing

- **Encoding:** round-trip test (`encode_batch` → `write_batch` → `read_batch` → assert data-equivalence, ignoring the dictionary-vs-plain physical representation); a test confirming a batch with a genuinely low-cardinality column actually gets dictionary-encoded (assert the resulting column's `DataType` is `Dictionary(...)`), and a companion test confirming a high-cardinality column does *not*.
- **`group_by`:** hand-checked small cases — single-column grouping, multi-column grouping, each `AggFunc` individually, and at least one case combining multiple `(column, AggFunc)` pairs in one call. Plus the error paths listed above.
- **Exit criterion — the actual benchmark:** a `criterion` benchmark in `bench/`, generating 10M+ synthetic rows (reusing the MVP schema's shape — numeric id, string name, vector — extended with a couple of extra group-by-able columns if needed), running `group_by`, and asserting correctness against a naively-computed reference (a simple, obviously-correct-but-slow grouping done with plain Rust `HashMap` + linear iteration over un-encoded data) before reporting throughput numbers. Correctness-against-a-reference matters more here than the throughput number itself — a fast wrong answer is worse than a slow right one.

## Alternatives considered

- **Sort + partition (lexsort_to_indices → take → partition → arrow's own aggregate kernels) instead of hash-based grouping:** rejected in favor of hash-based, specifically for the O(n) vs O(n log n) guarantee at the 10M+ row scale this phase's exit criterion targets. Costs more hand-written code (the hashing/accumulator loop) since arrow-rs doesn't ship a full GroupBy operator itself (that's DataFusion's job, already decided against — see `.claude/docs/architecture.md`'s "Small expression/filter API, no full SQL parser" principle) — but `RowConverter` still means row comparison/ordering itself isn't hand-rolled.
- **Strata's own custom on-disk binary format (Phase 0 spec §6) instead of Arrow's built-in encoded array types inside Arrow IPC files:** rejected for this phase. Arrow already provides `DictionaryArray`/`RunArray` as first-class types with a working reader/writer story; building a fully custom footer/column-chunk format now would be a much larger undertaking for the same "real encodings" outcome. The custom format remains a later, possibly-never-needed decision — revisit only if a concrete requirement Arrow's own types can't satisfy actually shows up (e.g. a specific need for byte-level control this project doesn't have yet).
- **Explicit, caller/schema-declared dictionary encoding instead of automatic heuristic-based encoding:** rejected — pushes a storage-layer optimization decision onto every caller (including the MVP schema and future Python-binding users), when real columnar engines (Parquet, Lance) make this call automatically by default.

## Non-goals for this phase

- `RunArray`/REE encoding (noted above — deferred, not rejected)
- Predicate pushdown / file-chunk pruning using the new encodings (that's Phase 3's stated scope)
- Strata's own custom on-disk format (see Alternatives above)
- `GROUP BY` with no grouping columns ("aggregate everything")
