# Phase 3 Spec — Predicate Pushdown & File/Chunk Pruning

**Status:** Draft — approved design, not yet implemented. Design deliverable for the "Query Layer Refinement" phase in `.claude/docs/architecture.md`'s roadmap (exit criterion: "`EXPLAIN` proves a filtered query skips untouched files").

**Scope:** four additions across `crates/storage`, `crates/query`, `crates/txn`, `crates/cli`. Does not touch the manifest's commit protocol beyond adding a field, does not touch `crates/index`, does not add a query planner or SQL surface — this project's architecture explicitly stays at "small expression/filter API, no full SQL parser."

## 1. File statistics in the manifest (`crates/storage`)

At commit time, alongside `encode_batch` (existing, Phase 2), compute per-column min/max on the **original, pre-encoding** batch — not the dictionary-encoded one, so stats reflect logical values directly with no decode step needed at read time. Scoped to orderable types only: numeric columns and UTF-8 string columns (lexicographic min/max). Vector columns get no stats — they aren't orderable and aren't pruning candidates. Min/max are computed over non-null values only; a column with zero non-null values in a given batch gets no `ColumnStats` entry for that file (see §2's `should_scan_file` — this is a defined, not accidental, case: no entry means "unknown, must scan").

```rust
pub struct ColumnStats {
    pub min: Value,
    pub max: Value,
}

pub struct DataFileEntry {
    pub name: String,
    pub stats: HashMap<String, ColumnStats>,  // column name -> stats
}
```

`Manifest.data_files` changes from `Vec<String>` to `Vec<DataFileEntry>`. This is a breaking manifest format change with no version negotiation or migration path — an explicit, accepted tradeoff (see "Alternatives considered"), not an oversight.

`Value` (the scalar type for both `ColumnStats` and `Predicate`, see below) lives in `crates/storage` alongside `ColumnStats`/`DataFileEntry`, since it's part of the manifest's serialized shape:

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Utf8(String),
}
```

## 2. `Predicate` and general row-level `filter` (`crates/query`)

```rust
pub enum Predicate {
    Eq(String, Value),
    Lt(String, Value),
    LtEq(String, Value),
    Gt(String, Value),
    GtEq(String, Value),
}

pub fn filter(batch: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ArrowError>
```

Generalizes, rather than duplicates, Phase 1's `filter_eq(batch, column, value)`: `filter_eq` becomes a thin wrapper delegating to `filter(batch, &Predicate::Eq(column.to_string(), Value::Utf8(value.to_string())))`, so its existing callers (the CLI's `filter` subcommand, the Phase 1 MVP checklist test) need no changes.

The same `Predicate` also drives pruning, via a pure decision function with zero I/O:

```rust
pub fn should_scan_file(stats: &HashMap<String, strata_storage::ColumnStats>, predicate: &Predicate) -> bool
```

Returns `true` if the file *could* contain a matching row, `false` only when the predicate's value range provably can't overlap the file's `[min, max]` for that column. Fails open to "always scan" in every ambiguous case, never a false skip:
- The predicate's column isn't a key in `stats` at all — either because the column is genuinely absent from the schema (in which case the later row-level `filter()` call will error, matching `filter_eq`'s existing unknown-column behavior — pruning fails open, filtering fails closed) or because that column had no orderable values to compute stats from (see below).
- A column with no non-null values in a given file gets no `ColumnStats` entry for that file at all (stats are computed over non-null values only) — same fail-open default applies, since there's no meaningful `[min, max]` to compare against.
- The predicate's `Value` variant doesn't match the column's stats' `Value` variant (e.g. a `Utf8` predicate against a column whose stats are `Int64`) — fails open rather than trusting derived `PartialOrd`'s cross-variant, declaration-order comparison, which compares by enum discriminant order, not value semantics.

Fully unit-testable against synthetic `HashMap<String, ColumnStats>` values — no `Dataset`, no files, no I/O.

## 3. `Dataset::explain` and `Dataset::scan_with_predicate` (`crates/txn`)

Both additive; the existing predicate-less `scan()` is untouched.

```rust
pub struct ExplainResult {
    pub total_files: usize,
    pub scanned: Vec<String>,
    pub skipped: Vec<String>,
}

impl Dataset {
    pub fn explain(&self, predicate: &Predicate) -> ExplainResult
    pub fn scan_with_predicate(&self, schema: &SchemaRef, predicate: &Predicate) -> Result<RecordBatch>
}
```

`explain` never opens a file body — it only consults `self.manifest.data_files`' already-loaded stats (no I/O beyond what `Dataset::open`/`create` already did), calling `strata_query::should_scan_file` per file. This is why it's cheap enough to be a pure introspection tool.

`scan_with_predicate` does the identical pruning decision, then reads only the surviving files (via `read_batch`, same as today's `scan`), casts to the logical schema (existing `cast_batch_to_schema` from the Phase 2 fix), concatenates, and applies `strata_query::filter` row-level on the result. This is the real performance path; `explain` is its introspection twin, sharing the exact same pruning logic so the two can never disagree about what would be skipped.

## 4. `strata explain` CLI subcommand

Prints `ExplainResult` in a human-readable form: total file count, which were scanned vs. skipped, and why (the predicate). Thin wrapper — no new logic, matches this project's existing CLI-is-a-thin-wrapper convention.

## Data flow

**Write path (extends Phase 2's, unchanged in spirit):** batch arrives at `Transaction::commit` → stats computed on the original batch → `encode_batch` runs (unaffected by stats computation, same batch, different pass) → `write_batch` → `sync_dir` → `commit_manifest` with the new `DataFileEntry` (name + stats) instead of a bare filename.

**Read path (new):** `Dataset::explain(predicate)` or `scan_with_predicate(schema, predicate)` → for each `DataFileEntry` in the manifest, `should_scan_file(&entry.stats, predicate)` decides scan-or-skip → `explain` just records the decision; `scan_with_predicate` additionally reads, casts, concatenates, and row-filters the surviving files.

## Error handling

`should_scan_file` never errors — it's a pure decision function; an unknown/absent column defaults to "scan" (never silently drops data). `filter`/`scan_with_predicate` return `Result` matching the existing `strata_query`/`strata_txn` error type conventions (no new error enum).

## Testing

- **`should_scan_file`** unit tests: each `Predicate` variant's genuine prune case, genuine no-prune case, and the "column absent from stats → always scan" safety default (both sub-cases: column genuinely not in the schema, and column present but all-null in that file).
- **`filter`**: each `Predicate` variant against a small hand-built batch, plus confirming `filter_eq` still produces identical results to before (regression guard for the Phase 1 call sites it now wraps).
- **Integration (the actual exit criterion):** commit 3+ batches with disjoint value ranges for some column (so each lands in its own file with non-overlapping `[min, max]`), call `explain` with a predicate matching only one range, assert the `skipped` list correctly names the others by filename. A second test does the same via `scan_with_predicate` and asserts the returned rows are exactly the ones from the non-skipped file(s) that also pass the row-level filter.

## Alternatives considered

- **Version-negotiated / backward-compatible manifest schema** (e.g. `#[serde(default)]` on a new optional `stats` field, keeping `data_files: Vec<String>` shape) instead of the breaking `Vec<String> → Vec<DataFileEntry>` change: rejected for now. Strata has no deployed data yet, and the manifest format has already evolved once (Phase 2's dictionary encoding was transparent to the format itself, but this is the first change to `Manifest`'s own shape). Building real migration/version-negotiation machinery for a pre-1.0 format that may still change again is premature — revisit if/when Strata has real data anyone needs to preserve across this change.
- **Lazy stats via file footers on scan** instead of manifest-stored stats computed at commit time: rejected — defeats the point of pruning, since reading every file's footer to decide whether to skip its body is most of the I/O cost for small-to-medium files.
- **Equality-only predicates** (matching `filter_eq`'s original scope) instead of `Eq`/`Lt`/`LtEq`/`Gt`/`GtEq`: rejected — range predicates come essentially free once min/max stats exist; scoping to equality-only now just means redoing this later for zero savings today.
- **CLI-only `explain`** (no library function) instead of `crates/query`/`crates/txn` functions + a thin CLI wrapper: rejected — untestable without spawning a subprocess, and out of step with this project's established CLI-is-a-thin-wrapper pattern.

## Non-goals for this phase

- Query planner / cost-based optimization (this stays a direct pruning check, not a plan search)
- SQL parser or expression trees beyond the flat `Predicate` enum
- Manifest format migration/versioning tooling
- Pruning on `crates/index`'s vector column (not orderable, not in scope)
- Compound predicates (AND/OR) — `Predicate` stays single-condition for this phase; combining predicates is additive future work, not a redesign
