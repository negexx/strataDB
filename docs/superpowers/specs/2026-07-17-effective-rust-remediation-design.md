# Effective Rust Remediation (Items 6, 7, 22, 24, 30, 32) — Design

**Date:** 2026-07-17
**Branch:** `chore/effective-rust-remediation` (off `audit/phase-1-2-3`)
**Context:** A conversational audit against two external Rust guideline sources (Microsoft's Rust Guidelines, the `rust-lang` API Guidelines checklist, and David Drysdale's *Effective Rust*) surfaced six concrete, verified gaps mapped to *Effective Rust* item numbers. This is a separate lens from the existing `audit/phase-1-2-3` remediation batches (which track findings from `.claude/docs/audits/2026-07-17-phase-1-2-3-audit-report.md`) — no overlap in scope, safe to build independently. Every gap below was confirmed against the actual source (grep + running the relevant `cargo` command), not assumed from the book's description alone.

## Scope decisions (asked and answered before this doc was written)

- **Item 30** (more than unit tests): full scope — doctests, an `examples/` program, and a `cargo-fuzz` target. Not just doctests.
- **Item 24** (re-export deps): simple `pub use arrow;`, not a wrapper type hiding `arrow` from the public API — `arrow` is the project's intentional interchange format per `.claude/CLAUDE.md`, not an implementation detail to hide.
- **Item 32** (CI): core gate (build/test/clippy/fmt/doc) + `cargo-deny` (bans/sources/advisories only — licenses excluded, see below), single `ubuntu-latest` runner. No Windows matrix.
- **Item 6/7** (newtype/builder): newtype the params, keep a plain `new()` — no builder. All four params are required with no sensible defaults, which is exactly the case *Effective Rust* itself says builders don't earn their keep.

## Scope addition found during design

`strata-storage`'s public API has the identical Item 24 gap to `strata-txn`: `write_batch`, `read_batch` (`crates/storage/src/datafile.rs`), `encode_batch` (`crates/storage/src/encoding.rs`), and `compute_stats` (`crates/storage/src/stats.rs`) all take or return `arrow::RecordBatch` with no `pub use arrow;` in `crates/storage/src/lib.rs`. Same fix, same rationale — included alongside `strata-txn` rather than left half-done.

---

## 1. Item 6 — newtype `HnswIndex::new`'s four `usize` params

**File:** `crates/index/src/hnsw.rs`

**Current:**

```rust
pub fn new(
    max_nb_connection: usize,
    max_elements: usize,
    max_layer: usize,
    ef_construction: usize,
) -> Result<Self, IndexError>
```

Four same-typed positional `usize` args — nothing stops a caller from transposing `max_layer` and `ef_construction`; the compiler can't catch it because they're structurally identical.

**Fix:** four newtypes, each `#[derive(Debug, Clone, Copy, PartialEq, Eq)]`:

```rust
pub struct MaxConnections(pub usize);
pub struct MaxElements(pub usize);
pub struct MaxLayers(pub usize);
pub struct EfConstruction(pub usize);
```

`new()`'s signature becomes `new(max_nb_connection: MaxConnections, max_elements: MaxElements, max_layer: MaxLayers, ef_construction: EfConstruction) -> Result<Self, IndexError>`. Internal body unwraps `.0` where the underlying `hnsw_rs::Hnsw::new` call and the `> 256` bounds check need a raw `usize`.

**Call sites to update** (11 total — pre-1.0 internal crate, clean break, no deprecation path needed):
- `crates/index/src/hnsw.rs` — 9 sites, all in `#[cfg(test)]`/`#[cfg(loom)]` modules.
- `crates/txn/src/dataset.rs:347` — real production call site.
- `crates/txn/src/snapshot.rs:234` — real production call site.

**Test:** existing unit/loom tests updated in place to construct the newtypes (mechanical) — no new test needed, this is a signature change, not a behavior change. `cargo test -p strata-index` and `cargo test -p strata-txn` staying green is the evidence.

## 2. Item 7 — builder for `HnswIndex::new`

**Decision: no builder.** Recorded explicitly (per the scope-decision answer above) so this item isn't silently dropped — *Effective Rust*'s own criteria for when builders help (many fields, many *optional* fields, or required fields with no sensible default) don't match a 4-field, all-required constructor. The newtype fix in §1 already closes the actual risk (parameter transposition); a builder here would just add a `.build()` call and four setter methods around fields that have no meaningful default to omit.

## 3. Item 22 — minimize visibility

**Decision: no code change.** Verified during the earlier audit: `grep` for `pub` struct fields across `Dataset`, `Transaction`, `Snapshot`, and the error types in `crates/txn/src/*.rs` and `crates/storage/src/*.rs` returned zero hits — access already goes through methods. Recorded here so it's traceable as "checked, clean" rather than silently absent from the plan.

## 4. Item 24 — re-export `arrow`

**Files:** `crates/txn/src/lib.rs`, `crates/storage/src/lib.rs`

Add `pub use arrow;` to both. One line each. No in-repo call-site changes — the workspace already pins a single `arrow` version via `[workspace.dependencies]`, so this only matters for a hypothetical external consumer of `strata-txn`/`strata-storage` as a standalone crate, which is exactly the scenario the guideline is protecting against (see `crates/index`, which already gets this right — `hnsw_rs` types never cross its public API, so it needs no re-export).

**Test:** `cargo build --workspace` succeeding is sufficient evidence (this is an additive, non-breaking API surface change — nothing to regress).

## 5. Item 30 — doctests, `examples/`, `cargo-fuzz`

### 5a. Doctests

Add `# Examples` sections (with compiling, runnable code, not `no_run` unless filesystem I/O genuinely can't be made deterministic) to the primary public surface:

- `strata-txn`: `Dataset::create`, `Dataset::open`, `Transaction::insert`/`commit`, `Dataset::vector_search`, `Snapshot`.
- `strata-index`: `HnswIndex::new`, `insert`, `search`.
- Error types (`TxnError`, `StorageError`, `IndexError`): one doctest each showing `.to_string()` output for a representative variant, reusing the pattern already established in `crates/txn/src/error.rs`'s existing unit test.

Temp-directory-needing examples reuse the existing hand-rolled pattern already in `crates/txn/src/dataset.rs`'s test module (`std::env::temp_dir().join(format!("strata-doctest-{label}-{}", std::process::id()))`) rather than adding a `tempfile` dependency — no new dependency, matches `.claude/CLAUDE.md`'s "don't add dependencies without justifying them" rule.

**Test:** `cargo test --workspace --doc` currently reports 0 passed/0 failed across every crate (confirmed by running it) — after this change it must report N passed, 0 failed.

### 5b. `examples/basic_usage.rs`

New file: `crates/txn/examples/basic_usage.rs`. Demonstrates `Dataset::create` → `Transaction::insert` → `commit` → `vector_search`, end to end, against a temp directory. Written as:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ...
    Ok(())
}
```

— no `unwrap()`, per *Effective Rust*'s own guidance that examples should model the practice you want users to copy.

**Test:** `cargo run --example basic_usage -p strata-txn` exits 0.

### 5c. `cargo-fuzz` target

New top-level `fuzz/` directory (sibling to `crates/`, `bench/` — cargo-fuzz's standard `cargo fuzz init` layout). Its `Cargo.toml` carries its own empty `[workspace]` table (cargo-fuzz convention) so it's excluded from the main workspace — no nightly-toolchain or lockfile leakage into the rest of the project.

One fuzz target, `manifest_parse`: feeds arbitrary bytes into `strata_storage::manifest::read_current`'s on-disk parsing path (the actual untrusted-input surface — on-disk manifest state, which is exactly what a corrupted disk, a downgraded binary, or a hostile actor with filesystem access could hand the reader). This also happens to be the same code path already flagged by the separate `audit/phase-1-2-3` dependency-lane findings (hostile-manifest capacity, path traversal) — the fuzz target is new infrastructure, not a duplicate fix.

**Not wired into CI** — *Effective Rust* itself says fuzzing shouldn't gate every PR (expensive, open-ended, no natural "done" state). Documented in the fuzz target's own README/comment as a manually-run tool (`cargo fuzz run manifest_parse`).

**Test/verification:** `cargo fuzz build` succeeding proves the target compiles against the current `strata-storage` API. Actually running the fuzzer for a stretch of wall-clock time is optional/manual, not part of this branch's "done" gate.

## 6. Item 32 — CI

**New file:** `.github/workflows/ci.yml`. Single `ubuntu-latest` job:

1. Checkout
2. Install the pinned toolchain via `dtolnay/rust-toolchain@master` with explicit `toolchain: "1.90"` and `components: clippy, rustfmt` inputs — the action does not read `rust-toolchain.toml` (that file only pins local `cargo`/`rustup` invocations; the GitHub Action requires the toolchain and components to be passed explicitly via `with:`, matching `rust-toolchain.toml`'s values)
3. `cargo build --workspace`
4. `cargo test --workspace`
5. `cargo clippy --workspace --all-targets -- -D warnings`
6. `cargo fmt --check`
7. `cargo doc --workspace --no-deps`
8. `cargo deny check bans sources advisories` (licenses check excluded — `deny.toml`'s empty allow-list is a separate, already-tracked finding in `audit/phase-1-2-3`'s dependency lane; silently working around it here would hide that finding instead of fixing it)

**New file:** `rust-toolchain.toml` pinning `channel = "1.90"` — matches the existing `rust-version = "1.90"` MSRV floor already declared in root `Cargo.toml`'s `[workspace.package]`, so this is stating an existing intent explicitly, not changing it.

**Verification:** YAML reviewed carefully by hand (and via `actionlint` if available locally) before commit; actual trigger behavior can only be proven by a real PR run against GitHub Actions, which is out of this branch's local-verification reach — noted as a residual manual-check item in the plan.

---

## Testing strategy summary

| Item | Verification |
|---|---|
| 6 | `cargo test -p strata-index`, `cargo test -p strata-txn` green |
| 7 | N/A — no code change |
| 22 | N/A — no code change, already verified clean |
| 24 | `cargo build --workspace` green |
| 30a (doctests) | `cargo test --workspace --doc`: 0 → N passed |
| 30b (example) | `cargo run --example basic_usage -p strata-txn` exits 0 |
| 30c (fuzz) | `cargo fuzz build` succeeds; not run in CI |
| 32 (CI) | YAML reviewed locally; real trigger only provable via an actual PR |

Full workspace gate stays green throughout: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check` — per `.claude/CLAUDE.md`'s "what done means" checklist. Opus 4.8 review per task, per the same file's mandatory model-dispatch rule. No `loom` test is needed for any of this — nothing here touches `crates/txn/`'s conflict-detection or commit logic, only constructor signatures, doc comments, and tooling.

## Out of scope

- No changes to conflict detection, snapshot isolation, or any correctness-critical logic.
- No `deny.toml` license-allowlist fix (tracked separately in `audit/phase-1-2-3`'s dependency lane).
- No Windows CI runner.
- No builder for `HnswIndex` (see §2).
