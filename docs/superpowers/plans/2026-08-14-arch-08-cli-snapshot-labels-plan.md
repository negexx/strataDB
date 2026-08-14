# ARCH-08 CLI Snapshot Labels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Make CLI read labels use the immutable snapshot version that produced the displayed rows.

**Architecture:** Keep the CLI’s existing command routing and output formats. Add a read-only `Snapshot::version()` accessor, capture one `Snapshot` in `scan` and `inspect`, pass its version to small formatting helpers, and use the same snapshot for the read. Add transaction and CLI coverage for the real snapshot-read path without changing transaction semantics.

**Tech Stack:** Rust 2024, `strata-cli`, `strata-txn`, existing unit tests, Cargo.

## Global Constraints

- Preserve the embedded, single-process/shared-`Dataset` boundary.
- Do not add cross-process coordination, serializability, or a read/write transaction API.
- Do not change `Dataset` version allocation, snapshot isolation, or mutation acknowledgement output.
- `Snapshot::version()` is read-only and exposes only the already-published immutable version number.
- Do not add dependencies.
- Preserve the existing `scan` output shape `"{} rows at version {}"` and `inspect` output shape `"version={} row_count={}"`.
- Preserve the unrelated untracked file `docs/audit/phase-3/terra-audit.md`.

---

### Task 1: Add failing snapshot-label regression tests

**Files:**
- Modify: `crates/cli/src/main.rs` in the existing `#[cfg(test)] mod tests`.
- Test: `crates/cli/src/main.rs` unit tests, using the existing temporary-dataset and `run` patterns.

**Interfaces:**
- Consumes: the current `run` command dispatcher and existing CLI output formats.
- Produces: failing tests for snapshot-bound scan/inspect summary helpers that later implementation will satisfy.

- [ ] **Step 1: Write the failing tests**

Add unit tests that exercise a helper accepting a captured snapshot and returning the version/row-count summary used by both commands. The test must create a dataset at version 1, capture its snapshot, advance the same dataset to version 2, and assert the captured summary remains version 1 with the captured row count. Cover both scan and inspect summary output with exact expectations:

```rust
let (batch, header) = scan_summary(&snapshot, &schema).unwrap();
assert_eq!(batch.num_rows(), 1);
assert_eq!(header, "1 rows at version 1");
assert_eq!(inspect_summary(&snapshot, &schema).unwrap(), "version=1 row_count=1");
```

The test names and expected strings must make clear that the version is supplied by the captured snapshot, not queried from a mutable dataset after the read.

- [ ] **Step 2: Run the focused tests and verify the intended failure**

Run:

```text
cargo test -p strata-cli --no-default-features snapshot_label
```

Expected: compilation failure because the two formatting helpers do not yet exist. Do not write production code before observing this failure.

---

### Task 2: Implement snapshot-bound labels and update the deferred ledger

**Files:**
- Modify: `crates/txn/src/snapshot.rs`.
- Modify: `crates/cli/src/main.rs`.
- Modify: `docs/phase-1-closeout-ledger.md`.
- Modify: `docs/audit/phase-1/audit.md`.

**Interfaces:**
- Consumes: the failing tests from Task 1.
- Produces: a read-only snapshot-version accessor, snapshot-bound CLI summaries, and reconciled ARCH-08 status records.

- [ ] **Step 1: Add minimal formatting helpers**

Add the public read-only accessor near the existing `Snapshot` methods:

```rust
pub fn version(&self) -> u64 {
    self.version
}
```

Add a transaction regression asserting an accessor returns the immutable captured version after a later commit.

Add private CLI helpers near the command handlers. They must accept one captured snapshot, scan it once, and return the summary data consumed by `run`:

```rust
fn scan_summary(
    snapshot: &strata_txn::Snapshot,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<(RecordBatch, String), Box<dyn Error>> {
    let batch = snapshot.scan(schema)?;
    let header = format_scan_header(batch.num_rows(), snapshot.version());
    Ok((batch, header))
}

fn inspect_summary(
    snapshot: &strata_txn::Snapshot,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<String, Box<dyn Error>> {
    let batch = snapshot.scan(schema)?;
    Ok(format_inspect_line(snapshot.version(), batch.num_rows()))
}

fn format_scan_header(row_count: usize, snapshot_version: u64) -> String {
    format!("{row_count} rows at version {snapshot_version}")
}

fn format_inspect_line(snapshot_version: u64, row_count: usize) -> String {
    format!("version={snapshot_version} row_count={row_count}")
}
```

- [ ] **Step 2: Bind `scan` to one snapshot and use its version**

Replace the current chained `ds.snapshot().scan(...)` call with one binding and use `scan_summary`:

```rust
let snapshot = ds.snapshot();
let (batch, header) = scan_summary(&snapshot, &strata_txn::mvp_fixtures::mvp_schema())?;
println!("{header}");
print_batch(&batch)?;
```

Keep `print_batch(&batch)` and the existing output text unchanged.

- [ ] **Step 3: Bind `inspect` to one snapshot and use its version**

Use the same pattern in `inspect` and print:

```rust
let snapshot = ds.snapshot();
println!("{}", inspect_summary(&snapshot, &strata_txn::mvp_fixtures::mvp_schema())?);
```

Do not change `crash-loop`, `insert`, or any other mutation output.

- [ ] **Step 4: Run the focused tests and verify they pass**

Run:

```text
cargo test -p strata-cli --no-default-features snapshot_label
cargo test -p strata-txn --no-default-features --lib snapshot_version_accessor
```

Expected: both tests pass.

- [ ] **Step 5: Update the Phase 1 deferred ledger and audit**

Replace the `ARCH-08` deferred wording in both `docs/phase-1-closeout-ledger.md` and `docs/audit/phase-1/audit.md` with an implemented-with-named-bounds entry that points to the Phase 2 CLI audit and states that read-only labels are sourced from the captured snapshot; preserve the separate deferred status of broader client/API stabilization.

- [ ] **Step 6: Run CLI verification**

Run:

```text
cargo test -p strata-cli --no-default-features
cargo test -p strata-txn --no-default-features
cargo test --workspace --no-default-features
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

Expected: all commands exit 0; no files outside the approved implementation/docs files and the plan/spec may be changed, except the already-existing untracked Terra audit file.

---

### Task 3: Final review evidence

**Files:**
- Review only: `crates/cli/src/main.rs`, `docs/phase-1-closeout-ledger.md`, and the Task 1/2 diff.

- [ ] **Step 1: Inspect the diff**

Confirm no command reads `Dataset::current_version()` to label rows obtained from a separate snapshot in `scan` or `inspect`, and confirm `Snapshot::version()` is read-only.

- [ ] **Step 2: Run the affected tests again**

```text
cargo test --workspace --no-default-features
```

Expected: exit 0 with zero failures.

- [ ] **Step 3: Record the bounded result**

Document that `ARCH-08` is implemented for the CLI’s snapshot-backed read labels, while cross-process and stronger transaction semantics remain out of scope.
