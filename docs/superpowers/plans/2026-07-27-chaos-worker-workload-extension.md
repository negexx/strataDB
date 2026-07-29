# Chaos-Worker Workload Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `crates/chaos-worker` and `tests/sim/tests/chaos.rs` so the Phase 7 chaos harness exercises deletes, updates, genuine write-write conflicts, predicate-filtered reads under zone-map pruning, and multi-batch zone-map merges — all under real crash injection — closing the gap the S1 spec's closure note names explicitly.

**Architecture:** `crates/chaos-worker` grows from one `main.rs` into `main.rs` + three new modules (`ops.rs` for op-verb generation and target resolution, `commit_ops.rs` for commit execution/retry/registry, `reader.rs` for a live concurrent predicate-pruning check) plus a tiny `schema.rs` helper. `tests/sim/tests/chaos.rs`'s `run_worker`/`check_invariants` are rewritten around a richer, explicit stdout protocol and one new invariant.

**Tech Stack:** Rust, `rand`/`rand_chacha` (already a dependency), `strata-txn`, `strata-storage`, `strata-query` (new dependency for `crates/chaos-worker`), `arrow`.

**Spec:** `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md` — read it first; this plan implements it exactly, with one load-bearing mechanism the spec didn't specify (see Global Constraint 6 below).

## Global Constraints

1. **Op-verb weights** (spec §3.1): Insert 40%, Delete 20%, Update 20%, MultiBatchInsert 20% — implemented as exact `f64` boundaries `0.40`/`0.60`/`0.80`, not tunable via CLI/env for this plan.
2. **Contested pool size** = 6 rows (`POOL_SIZE`), committed as 6 individual single-row inserts before the interleaved scheduler loop starts.
3. **Target selection** (spec §3.1): 50% draw from the contested pool, 50% from the acting agent's own live rows; fall back to whichever source is non-empty; downgrade to Insert if both are empty.
4. **Conflict handling** (spec §3.2): on `TxnError::Conflict` only, retry the identical logical op exactly once against a fresh `dataset.begin()`; drop (no acknowledgment) if the retry also conflicts. Any other error remains a fatal `panic!`, unchanged from today.
5. **MultiBatchInsert** (spec §3.1) consumes 2 of the agent's op slots, building 2 separate `Transaction::insert()` calls (each its own single-row batch, via `strata_txn::mvp_fixtures::mvp_row`) inside one `commit()`. Downgrades to a plain single-row Insert if only 1 slot remains.
6. **Row-id lookup mechanism (new — not specified in the design doc, resolved here):** `Transaction::commit()` returns only `Result<()>`, never the row-id(s) it assigned, but `Transaction::delete`/`update` need the *internal* system row-id (not the business `id` column value chaos-worker already tracks). This plan resolves that by reading it back via `Snapshot::scan_with_predicate` on an extended schema that includes `strata_txn::ROW_ID_COLUMN`, immediately after every successful insert-type commit — a real observation, not a prediction from the row-id allocator's own claim-order semantics (which would work today but would silently couple this test harness to an internal implementation detail it has no business depending on). This costs one extra `scan_with_predicate` call per insert-type op; accepted, since this is a correctness harness, not a throughput-sensitive path.
7. **Reader predicate** (spec §3.3): fixed `Predicate::Eq("name", Value::Utf8("agent0"))`, poll interval 1ms, runs for the whole worker process lifetime.
8. **Failure signal** (spec §3.4): a global panic hook prints `GENUINE_FAILURE: <message>` and calls `std::process::exit(2)` on any panic, main thread or reader thread. `tests/sim/tests/chaos.rs` checks exit code 2 first, before anything else.
9. Every task's code must pass `cargo build --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --check` clean, in addition to its own tests — per `.claude/CLAUDE.md`'s "What done means."
10. `crates/chaos-worker` and `tests/sim` are not `crates/txn`/`crates/index`, so no `loom` test is required by `.claude/rules/concurrency-txn-layer.md` — this plan adds no new lock/atomic/CAS logic of its own (the reader thread only ever calls already-thread-safe `Dataset`/`Snapshot` methods).
11. **`crates/chaos-worker` stays a bin-only crate — no `src/lib.rs`.** It has one `[[bin]]` target (`chaos-worker`) and no `[lib]` target (confirmed against its `Cargo.toml`); this matches `crates/cli`'s existing precedent (`strata-cli` is bin-only with inline `#[cfg(test)] mod tests` in `main.rs`, tested via `cargo test -p strata-cli --bin strata`, NOT `--lib` — `--lib` errors with "no library targets found" against a bin-only package, confirmed empirically). Every task below tests its new module the same way: `cargo test -p strata-chaos-worker --bin chaos-worker`. Do not add a `src/lib.rs` to route around a test-command issue — if a test command in this plan doesn't work as written, the command is wrong, not the crate's structure; flag it rather than restructuring the crate.
12. **Dead-code warnings between tasks are expected and handled uniformly.** Tasks 1-4 each add a module whose functions aren't called from `main()` yet — that wiring only happens in Task 6 — so `cargo clippy --workspace --all-targets -- -D warnings` would otherwise fail on a real `dead_code` lint in the non-test build profile (the test profile doesn't trip it, since each module's own `#[cfg(test)] mod tests` calls everything). Each of Tasks 1-4 places a single `#[allow(dead_code)]` directly on that task's `mod` declaration in `main.rs` (attributes on a `mod` item apply recursively to everything inside it), with a comment explaining it's temporary. Task 6 removes every one of these `#[allow(dead_code)]` lines as part of its full-file rewrite of `main.rs`, since by then every module is genuinely called — leaving one in past Task 6 would silently suppress real future dead-code detection in that module.

---

### Task 1: Op-verb model and just-in-time target resolution

**Files:**
- Create: `crates/chaos-worker/src/ops.rs`
- Modify: `crates/chaos-worker/src/main.rs` (add `mod ops;`)

**Interfaces:**
- Produces: `pub(crate) enum OpVerb { Insert, Delete, Update, MultiBatchInsert }` (derives `Debug, Clone, Copy, PartialEq, Eq`); `pub(crate) fn generate_verb_sequence(seed: u64, agent: u64, ops_per_agent: u64) -> Vec<OpVerb>`; `pub(crate) fn resolve_target(target_rng: &mut ChaCha8Rng, pool_rows: &[u64], own_rows: &[u64]) -> Option<u64>`; `pub(crate) fn resolve_slot_consumption(verb: OpVerb, slots_remaining: u64) -> (OpVerb, u64)`.

- [ ] **Step 1: Write the failing tests**

Create `crates/chaos-worker/src/ops.rs`:

```rust
//! Per-agent operation-verb generation and just-in-time target resolution
//! for the chaos workload. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! §3.1.

use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;

/// One agent's chosen action for a single op slot. Drawn up front per
/// agent (see [`generate_verb_sequence`]) from a fixed weighted
/// distribution — see the design doc §3.1 for why these specific
/// percentages are starting defaults, not load-bearing constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpVerb {
    Insert,
    Delete,
    Update,
    MultiBatchInsert,
}

fn verb_for_fraction(u: f64) -> OpVerb {
    if u < 0.40 {
        OpVerb::Insert
    } else if u < 0.60 {
        OpVerb::Delete
    } else if u < 0.80 {
        OpVerb::Update
    } else {
        OpVerb::MultiBatchInsert
    }
}

/// Draws one [`OpVerb`]: 40% Insert, 20% Delete, 20% Update, 20%
/// MultiBatchInsert.
fn draw_verb(rng: &mut ChaCha8Rng) -> OpVerb {
    verb_for_fraction(rng.random())
}

/// Distinct RNG stream from the existing per-op vector generation (itself
/// seeded `seed ^ agent`), so consuming it for verbs doesn't perturb the
/// vector sequence.
const VERB_STREAM: u64 = 0xC0DE_A62B_005E_1234;

/// Generates one agent's full verb sequence up front, independent of
/// scheduling — same seeding discipline as the existing per-op vector
/// generation.
pub(crate) fn generate_verb_sequence(seed: u64, agent: u64, ops_per_agent: u64) -> Vec<OpVerb> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ agent ^ VERB_STREAM);
    (0..ops_per_agent).map(|_| draw_verb(&mut rng)).collect()
}

/// Resolves a Delete/Update op's target row-id at scheduling time: 50% a
/// random row from `pool_rows`, 50% a random row from `own_rows` (this
/// agent's own live prior inserts), falling back to whichever of the two
/// is non-empty, or `None` (the caller must downgrade this op slot to
/// Insert) if both are empty.
pub(crate) fn resolve_target(
    target_rng: &mut ChaCha8Rng,
    pool_rows: &[u64],
    own_rows: &[u64],
) -> Option<u64> {
    let prefer_pool = target_rng.random_bool(0.5);
    let (primary, secondary) = if prefer_pool {
        (pool_rows, own_rows)
    } else {
        (own_rows, pool_rows)
    };
    let source = if !primary.is_empty() {
        primary
    } else if !secondary.is_empty() {
        secondary
    } else {
        return None;
    };
    Some(source[target_rng.random_range(0..source.len())])
}

/// Given the verb drawn for this agent's current op slot and how many
/// slots remain (including the current one — so `slots_remaining >= 1` is
/// always expected; the caller only ever invokes this for an agent it has
/// already filtered to have at least one op left), decides how many slots
/// this op actually consumes and what verb to execute. `MultiBatchInsert`
/// needs 2 slots; if only 1 remains, it downgrades to a plain `Insert`
/// consuming just that 1 slot.
pub(crate) fn resolve_slot_consumption(verb: OpVerb, slots_remaining: u64) -> (OpVerb, u64) {
    debug_assert!(slots_remaining >= 1, "caller must never invoke this for an exhausted agent");
    match verb {
        OpVerb::MultiBatchInsert if slots_remaining < 2 => (OpVerb::Insert, 1),
        OpVerb::MultiBatchInsert => (OpVerb::MultiBatchInsert, 2),
        other => (other, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_for_fraction_respects_the_documented_boundaries() {
        assert_eq!(verb_for_fraction(0.0), OpVerb::Insert);
        assert_eq!(verb_for_fraction(0.399), OpVerb::Insert);
        assert_eq!(verb_for_fraction(0.4), OpVerb::Delete);
        assert_eq!(verb_for_fraction(0.599), OpVerb::Delete);
        assert_eq!(verb_for_fraction(0.6), OpVerb::Update);
        assert_eq!(verb_for_fraction(0.799), OpVerb::Update);
        assert_eq!(verb_for_fraction(0.8), OpVerb::MultiBatchInsert);
        assert_eq!(verb_for_fraction(0.999), OpVerb::MultiBatchInsert);
    }

    #[test]
    fn generate_verb_sequence_matches_the_documented_distribution_over_many_draws() {
        // Exercises draw_verb/generate_verb_sequence end-to-end through the
        // real RNG, not just the pure verb_for_fraction boundary function
        // above -- if draw_verb's rng.random() call ever changed range (a
        // silent drift verb_for_fraction's own test cannot catch), this is
        // what would notice. Design doc §6 names precisely this failure
        // mode: "a distribution that's too insert-heavy would silently
        // under-exercise the very thing this design exists to add."
        let sequence = generate_verb_sequence(42, 1, 10_000);
        assert_eq!(sequence.len(), 10_000);
        let count = |verb: OpVerb| sequence.iter().filter(|&&v| v == verb).count();
        let insert = count(OpVerb::Insert);
        let delete = count(OpVerb::Delete);
        let update = count(OpVerb::Update);
        let multi = count(OpVerb::MultiBatchInsert);
        assert_eq!(insert + delete + update + multi, 10_000);
        // Generous tolerance (+/- 300 of the expected count, ~7.5% relative)
        // so this isn't flaky, while still catching a distribution that
        // silently drifted to e.g. 50/50.
        assert!((3700..=4300).contains(&insert), "insert count {insert} outside expected range");
        assert!((1700..=2300).contains(&delete), "delete count {delete} outside expected range");
        assert!((1700..=2300).contains(&update), "update count {update} outside expected range");
        assert!((1700..=2300).contains(&multi), "multibatch count {multi} outside expected range");
    }

    #[test]
    fn the_same_seed_and_agent_always_produce_the_same_sequence() {
        let a = generate_verb_sequence(42, 1, 20);
        let b = generate_verb_sequence(42, 1, 20);
        assert_eq!(a, b);
        assert_eq!(a.len(), 20, "must produce exactly ops_per_agent verbs");
    }

    #[test]
    fn generate_verb_sequence_has_a_pinned_golden_output_for_seed_42_agent_1() {
        // Pins the actual seeding discipline (seed ^ agent ^ VERB_STREAM,
        // ChaCha8Rng, draw order), not just "calling it twice gives the
        // same answer" (true of any pure function, including a badly
        // seeded one) -- the chaos harness depends on cross-run,
        // cross-version reproducibility (CLAUDE.md's Phase 7 bullet:
        // "seed-reproducible scenarios"), which only a literal golden
        // vector actually tests. Captured by running this exact call once
        // and pasting its real output -- do not hand-derive or guess this
        // value.
        let sequence = generate_verb_sequence(42, 1, 8);
        assert_eq!(
            sequence,
            vec![
                OpVerb::Insert,
                OpVerb::Insert,
                OpVerb::Delete,
                OpVerb::Delete,
                OpVerb::Insert,
                OpVerb::Update,
                OpVerb::Insert,
                OpVerb::Insert,
            ]
        );
    }

    #[test]
    fn different_agents_produce_different_sequences() {
        let a = generate_verb_sequence(42, 1, 20);
        let b = generate_verb_sequence(42, 2, 20);
        assert_ne!(
            a, b,
            "two different agents drawing identical 20-op sequences by chance is \
             astronomically unlikely and would indicate the per-agent XOR isn't \
             actually varying the stream"
        );
    }

    #[test]
    fn resolve_target_returns_none_when_both_sources_are_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        assert_eq!(resolve_target(&mut rng, &[], &[]), None);
    }

    #[test]
    fn resolve_target_falls_back_to_pool_when_own_rows_is_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        for _ in 0..20 {
            let target = resolve_target(&mut rng, &[7, 8, 9], &[]);
            assert!(matches!(target, Some(7 | 8 | 9)));
        }
    }

    #[test]
    fn resolve_target_falls_back_to_own_rows_when_pool_is_empty() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        for _ in 0..20 {
            let target = resolve_target(&mut rng, &[], &[1, 2, 3]);
            assert!(matches!(target, Some(1 | 2 | 3)));
        }
    }

    #[test]
    fn resolve_target_draws_from_both_sources_in_roughly_equal_proportion() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let mut pool_count = 0;
        let mut own_count = 0;
        for _ in 0..200 {
            match resolve_target(&mut rng, &[100], &[200]) {
                Some(100) => pool_count += 1,
                Some(200) => own_count += 1,
                other => panic!("unexpected target: {other:?}"),
            }
        }
        assert_eq!(pool_count + own_count, 200);
        // A real balance check on the stated 50/50 policy, not just "both
        // were reachable" -- a 199/1 split would satisfy reachability
        // alone but clearly isn't 50/50. Generous bound (60..140 of 200,
        // i.e. 30%-70%) so this isn't flaky.
        assert!(
            (60..=140).contains(&pool_count),
            "pool_count {pool_count}/200 outside the expected ~50/50 balance"
        );
        assert!(
            (60..=140).contains(&own_count),
            "own_count {own_count}/200 outside the expected ~50/50 balance"
        );
    }

    #[test]
    fn resolve_target_is_deterministic_for_identically_seeded_rngs() {
        let mut rng_a = ChaCha8Rng::seed_from_u64(99);
        let mut rng_b = ChaCha8Rng::seed_from_u64(99);
        for _ in 0..20 {
            let a = resolve_target(&mut rng_a, &[1, 2, 3], &[4, 5, 6]);
            let b = resolve_target(&mut rng_b, &[1, 2, 3], &[4, 5, 6]);
            assert_eq!(a, b, "identically-seeded RNGs must draw the identical target sequence");
        }
    }

    #[test]
    fn multi_batch_insert_consumes_two_slots_when_available() {
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 2),
            (OpVerb::MultiBatchInsert, 2)
        );
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 5),
            (OpVerb::MultiBatchInsert, 2)
        );
    }

    #[test]
    fn multi_batch_insert_downgrades_to_a_single_insert_on_the_last_slot() {
        assert_eq!(
            resolve_slot_consumption(OpVerb::MultiBatchInsert, 1),
            (OpVerb::Insert, 1)
        );
    }

    #[test]
    fn other_verbs_always_consume_exactly_one_slot() {
        for verb in [OpVerb::Insert, OpVerb::Delete, OpVerb::Update] {
            assert_eq!(resolve_slot_consumption(verb, 5), (verb, 1));
            assert_eq!(resolve_slot_consumption(verb, 1), (verb, 1));
        }
    }
}
```

In `crates/chaos-worker/src/main.rs`, add near the top (after the existing `use` block):

```rust
// Not yet called from main() -- wired in by Task 6 of the workload-extension
// plan, which removes this attribute once it is.
#[allow(dead_code)]
mod ops;
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: all `ops::tests::*` tests pass (this module has no prior implementation to be "failing against" — it's new, so write-then-run is the TDD cycle here rather than red-then-green against pre-existing code).

- [ ] **Step 3: Commit**

```bash
git add crates/chaos-worker/src/ops.rs crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): add op-verb model and just-in-time target resolution"
```

---

### Task 2: Row-id lookup helper and the contested-row registry

**Files:**
- Create: `crates/chaos-worker/src/schema.rs`
- Create: `crates/chaos-worker/src/commit_ops.rs` (registry section only this task)
- Modify: `crates/chaos-worker/src/main.rs` (add `mod schema;` and `mod commit_ops;`)
- Modify: `crates/chaos-worker/Cargo.toml` (add `strata-query` dependency)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `pub(crate) fn schema_with_row_id() -> Arc<Schema>` (in `schema.rs`); `pub(crate) fn lookup_row_id(dataset: &Dataset, business_id: i64) -> u64` and `pub(crate) struct Registry` with `new(num_agents: usize)`, `record_pool_row(&mut self, row_id: u64)`, `record_own_row(&mut self, agent: usize, row_id: u64)`, `remove(&mut self, agent: usize, row_id: u64)`, `pool_rows(&self) -> &[u64]`, `own_rows(&self, agent: usize) -> &[u64]` (in `commit_ops.rs`) — later tasks build op execution on top of these.

- [ ] **Step 1: Add the new dependency**

In `crates/chaos-worker/Cargo.toml`, add under `[dependencies]`:

```toml
strata-query = { path = "../query" }
```

- [ ] **Step 2: Write the schema helper and its test**

Create `crates/chaos-worker/src/schema.rs`:

```rust
//! `mvp_schema()`'s fields plus the hidden row-id column — needed to read
//! back the internal system row-id `Transaction::commit()` itself never
//! returns. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! Global Constraint 6 in the implementation plan for why this is a
//! read-back rather than a prediction from the row-id allocator's own
//! claim-order semantics.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

pub(crate) fn schema_with_row_id() -> Arc<Schema> {
    let mvp = strata_txn::mvp_fixtures::mvp_schema();
    let mut fields: Vec<Field> = mvp.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(
        strata_txn::ROW_ID_COLUMN,
        DataType::UInt64,
        false,
    ));
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn includes_every_mvp_field_plus_the_row_id_column() {
        let schema = schema_with_row_id();
        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("vector").is_ok());
        let row_id_field = schema.field_with_name(strata_txn::ROW_ID_COLUMN).unwrap();
        assert_eq!(
            *row_id_field.data_type(),
            DataType::UInt64,
            "must be UInt64 -- cast_batch_to_schema silently casts on a type \
             mismatch instead of erroring, which would turn a wrong DataType \
             here into a runtime downcast panic in commit_ops.rs instead of \
             a caught-at-the-source bug"
        );
    }
}
```

- [ ] **Step 3: Write the row-id lookup helper and the registry, with tests**

Create `crates/chaos-worker/src/commit_ops.rs` (this task only adds the pieces below; op execution is added in Task 3):

```rust
//! Commit execution, conflict retry, the contested-row-id registry, and
//! acknowledgment-line printing for the chaos workload. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`
//! §3.1, §3.2, and §3.5.

use arrow::array::UInt64Array;
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::{Dataset, ROW_ID_COLUMN};

use crate::schema::schema_with_row_id;

/// Looks up the internal system row-id assigned to the row whose business
/// `id` column equals `business_id`. Called immediately after a
/// successful insert-type commit — `Transaction::commit` returns only
/// `Result<()>`, never the row-id(s) it assigned, and `Transaction::delete`/
/// `update` need the internal row-id, not the business `id` column value.
pub(crate) fn lookup_row_id(dataset: &Dataset, business_id: i64) -> u64 {
    let predicate = Predicate::Eq("id".to_string(), Value::Int64(business_id));
    let batch = dataset
        .snapshot()
        .scan_with_predicate(&schema_with_row_id(), &predicate)
        .expect("scan_with_predicate must succeed for a row this worker just committed");
    assert_eq!(
        batch.num_rows(),
        1,
        "business id {business_id} must resolve to exactly one row right after its own insert"
    );
    let row_id_col = batch
        .column(batch.schema_ref().index_of(ROW_ID_COLUMN).unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    row_id_col.value(0)
}

/// Tracks which row-ids are eligible Delete/Update targets: the shared
/// contested pool, and each agent's own live (not-yet-tombstoned) rows.
/// Updated only after a commit actually durably succeeds — never
/// speculatively.
///
/// Every method taking an `agent` index requires `agent < num_agents` (the
/// value passed to [`Registry::new`]) — callers are all in-crate and
/// already have a valid agent index in hand from the scheduler loop, so
/// this is an internal invariant, not a checked precondition.
pub(crate) struct Registry {
    pool_live: Vec<u64>,
    own_live: Vec<Vec<u64>>,
}

impl Registry {
    pub(crate) fn new(num_agents: usize) -> Self {
        Self {
            pool_live: Vec::new(),
            own_live: vec![Vec::new(); num_agents],
        }
    }

    pub(crate) fn record_pool_row(&mut self, row_id: u64) {
        self.pool_live.push(row_id);
    }

    pub(crate) fn record_own_row(&mut self, agent: usize, row_id: u64) {
        self.own_live[agent].push(row_id);
    }

    /// Removes `row_id` from wherever it currently lives (the pool, or
    /// some agent's own-row list) after it's been tombstoned. A target is
    /// always either a pool row or the ACTING agent's own row (never
    /// another agent's), so checking the pool then this agent's own list
    /// is exhaustive.
    pub(crate) fn remove(&mut self, agent: usize, row_id: u64) {
        if let Some(pos) = self.pool_live.iter().position(|&r| r == row_id) {
            self.pool_live.swap_remove(pos);
        } else if let Some(pos) = self.own_live[agent].iter().position(|&r| r == row_id) {
            self.own_live[agent].swap_remove(pos);
        }
    }

    pub(crate) fn pool_rows(&self) -> &[u64] {
        &self.pool_live
    }

    pub(crate) fn own_rows(&self, agent: usize) -> &[u64] {
        &self.own_live[agent]
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-worker-test-{label}-{}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn lookup_row_id_finds_the_row_just_inserted() {
        // Two rows with distinct business ids, not one -- with only one
        // row present, this test can't tell "matched business id 42" from
        // "returned whatever row happened to be there," since both would
        // return the dataset's only row-id. Two rows makes the predicate
        // itself load-bearing: a lookup_row_id that filtered on the wrong
        // column, the wrong Value variant, or ignored the predicate
        // entirely would still pass a single-row version of this test.
        let dir = temp_dir("lookup-row-id");
        let dataset = Dataset::create(&dir).unwrap();
        let mut first = dataset.begin();
        first.insert(strata_txn::mvp_fixtures::mvp_row(42, "agent0", [1.0, 2.0, 3.0]).unwrap());
        first.commit().unwrap();
        let mut second = dataset.begin();
        second.insert(strata_txn::mvp_fixtures::mvp_row(7, "agent0", [4.0, 5.0, 6.0]).unwrap());
        second.commit().unwrap();

        // First-ever commit in a fresh dataset always claims row-id 0; the
        // second commit claims row-id 1.
        assert_eq!(lookup_row_id(&dataset, 42), 0);
        assert_eq!(lookup_row_id(&dataset, 7), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn registry_record_and_remove_round_trip() {
        let mut registry = Registry::new(2);
        registry.record_pool_row(100);
        registry.record_own_row(0, 200);
        registry.record_own_row(1, 300);

        assert_eq!(registry.pool_rows(), &[100]);
        assert_eq!(registry.own_rows(0), &[200]);
        assert_eq!(registry.own_rows(1), &[300]);

        registry.remove(0, 100); // a pool row, removed via agent 0's delete
        assert_eq!(registry.pool_rows(), &[] as &[u64]);

        registry.remove(0, 200); // agent 0's own row
        assert_eq!(registry.own_rows(0), &[] as &[u64]);
        // Untouched: agent 1's own row.
        assert_eq!(registry.own_rows(1), &[300]);
    }

    #[test]
    fn registry_remove_of_an_unknown_row_id_is_a_harmless_no_op() {
        let mut registry = Registry::new(1);
        registry.record_own_row(0, 1);
        registry.remove(0, 999);
        assert_eq!(registry.own_rows(0), &[1]);
    }
}
```

In `crates/chaos-worker/src/main.rs`, add:

```rust
// Not yet called from main() -- wired in by Task 6, which removes these
// attributes once they are.
#[allow(dead_code)]
mod commit_ops;
#[allow(dead_code)]
mod schema;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: all `schema::tests::*` and `commit_ops::tests::*` tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/chaos-worker/Cargo.toml crates/chaos-worker/src/schema.rs crates/chaos-worker/src/commit_ops.rs crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): add row-id lookup and the contested-row registry"
```

---

### Task 3: Commit execution, conflict retry, and acknowledgment printing

**Files:**
- Modify: `crates/chaos-worker/src/commit_ops.rs` (append; do not remove Task 2's content)

**Interfaces:**
- Consumes: `lookup_row_id` and `Registry` from Task 2; `OpVerb` from Task 1 (used only in doc comments/tests here — the scheduler in Task 6 is what actually calls these with a verb already resolved).
- Produces: `pub(crate) enum ExecOutcome { CommittedInsert { business_id: i64, row_id: u64 }, CommittedDelete { target_row_id: u64 }, CommittedUpdate { target_row_id: u64, row_id: u64 }, CommittedMultiBatch { row_ids: [u64; 2] }, Dropped }`; `pub(crate) fn execute_insert(...) -> ExecOutcome`; `pub(crate) fn execute_delete(...) -> ExecOutcome`; `pub(crate) fn execute_update(...) -> ExecOutcome`; `pub(crate) fn execute_multi_batch_insert(...) -> ExecOutcome`; `pub(crate) fn print_outcome(out: &mut impl std::io::Write, agent: u64, op: u64, outcome: &ExecOutcome)` — Task 6's scheduler calls these directly.

- [ ] **Step 1: Write the failing tests**

Append to `crates/chaos-worker/src/commit_ops.rs` (before the closing `}` of the existing `mod tests` block — add these as new `#[test]` functions inside that same module, and add the new non-test code above the `#[cfg(test)]` line):

Add above the `#[cfg(test)]` line (`use std::io::Write;`, not `use std::io::Write as _;` — this file, unlike `main.rs`, names `Write` as a type in `print_outcome`'s `impl Write` parameter below, and an anonymous `as _` import only brings trait methods into scope, not the name itself):

```rust
use std::io::Write;

use strata_txn::TxnError;

/// The result of attempting one op. `Dropped` means a conflict occurred
/// twice in a row (the original attempt and one retry) — see the module
/// doc comment and design doc §3.2.
#[derive(Debug)]
pub(crate) enum ExecOutcome {
    CommittedInsert { business_id: i64, row_id: u64 },
    CommittedDelete { target_row_id: u64 },
    CommittedUpdate { target_row_id: u64, row_id: u64 },
    CommittedMultiBatch { row_ids: [u64; 2] },
    Dropped,
}

/// Whether [`commit_with_retry_once`]'s two-attempt policy ended in a
/// commit or a drop. Deliberately not `Result` -- neither outcome is an
/// error at this layer, and folding "dropped" into an `Err` variant would
/// invite a stray `?` to silently propagate it as one.
enum RetryOutcome {
    Committed,
    Dropped,
}

/// The retry-once-then-drop policy shared by [`execute_delete`] and
/// [`execute_update`] — see design doc §3.2. `attempt` must build and
/// commit a FRESH transaction on every call (never reuse transaction
/// state across calls), since a retry needs a `dataset.begin()` at the
/// post-winner version to have any chance of succeeding. `context` names
/// the operation for the panic message on an unexpected (non-`Conflict`)
/// error.
///
/// Extracted as its own function, rather than duplicated inline in both
/// callers, specifically so this policy is unit-testable in isolation
/// with a mock closure — real concurrent conflicts need `crates/txn`'s
/// `#[cfg(test)]`-private rendezvous hooks, which aren't reachable from
/// this crate, so a mock is the only way to exercise the `Dropped` path
/// at all outside a real multi-agent chaos run (see Task 8).
fn commit_with_retry_once(
    mut attempt: impl FnMut() -> Result<(), TxnError>,
    context: &str,
) -> RetryOutcome {
    match attempt() {
        Ok(()) => RetryOutcome::Committed,
        Err(TxnError::Conflict { .. }) => match attempt() {
            Ok(()) => RetryOutcome::Committed,
            Err(TxnError::Conflict { .. }) => RetryOutcome::Dropped,
            Err(e) => panic!("unexpected commit error on {context} retry: {e}"),
        },
        Err(e) => panic!("unexpected commit error on {context}: {e}"),
    }
}

/// A pure insert has an empty write-set (`Transaction::insert` never
/// touches it), so it structurally cannot conflict — any error here is a
/// genuine, unexpected bug, exactly like today's insert-only worker.
pub(crate) fn execute_insert(dataset: &Dataset, business_id: i64, name: &str, vector: [f32; 3]) -> ExecOutcome {
    let batch = strata_txn::mvp_fixtures::mvp_row(business_id, name, vector)
        .expect("mvp_row must succeed for a well-formed insert");
    let mut txn = dataset.begin();
    txn.insert(batch);
    match txn.commit() {
        Ok(()) => ExecOutcome::CommittedInsert {
            business_id,
            row_id: lookup_row_id(dataset, business_id),
        },
        Err(e) => panic!("unexpected commit error on a pure insert (inserts cannot conflict): {e}"),
    }
}

/// See design doc §3.2 and [`commit_with_retry_once`] for the retry
/// policy.
pub(crate) fn execute_delete(dataset: &Dataset, target_row_id: u64) -> ExecOutcome {
    let attempt = || {
        let mut txn = dataset.begin();
        txn.delete(target_row_id);
        txn.commit()
    };
    match commit_with_retry_once(attempt, "delete") {
        RetryOutcome::Committed => ExecOutcome::CommittedDelete { target_row_id },
        RetryOutcome::Dropped => ExecOutcome::Dropped,
    }
}

/// See design doc §3.2 and [`commit_with_retry_once`] for the retry
/// policy.
pub(crate) fn execute_update(
    dataset: &Dataset,
    target_row_id: u64,
    business_id: i64,
    name: &str,
    vector: [f32; 3],
) -> ExecOutcome {
    let attempt = || {
        let batch = strata_txn::mvp_fixtures::mvp_row(business_id, name, vector)
            .expect("mvp_row must succeed for a well-formed update");
        let mut txn = dataset.begin();
        txn.update(target_row_id, batch);
        txn.commit()
    };
    match commit_with_retry_once(attempt, "update") {
        RetryOutcome::Committed => ExecOutcome::CommittedUpdate {
            target_row_id,
            row_id: lookup_row_id(dataset, business_id),
        },
        RetryOutcome::Dropped => ExecOutcome::Dropped,
    }
}

/// Bundles 2 separate `Transaction::insert()` calls into one `commit()` —
/// the multi-batch shape that exercises `merge_zone_map_stats` across
/// batches. Like [`execute_insert`], a pure insert cannot conflict.
pub(crate) fn execute_multi_batch_insert(
    dataset: &Dataset,
    business_ids: [i64; 2],
    name: &str,
    vectors: [[f32; 3]; 2],
) -> ExecOutcome {
    let batch0 = strata_txn::mvp_fixtures::mvp_row(business_ids[0], name, vectors[0])
        .expect("mvp_row must succeed for a well-formed multi-batch insert");
    let batch1 = strata_txn::mvp_fixtures::mvp_row(business_ids[1], name, vectors[1])
        .expect("mvp_row must succeed for a well-formed multi-batch insert");
    let mut txn = dataset.begin();
    txn.insert(batch0);
    txn.insert(batch1);
    match txn.commit() {
        Ok(()) => ExecOutcome::CommittedMultiBatch {
            row_ids: [
                lookup_row_id(dataset, business_ids[0]),
                lookup_row_id(dataset, business_ids[1]),
            ],
        },
        Err(e) => panic!("unexpected commit error on multi-batch insert (inserts cannot conflict): {e}"),
    }
}

/// Prints one acknowledgment line matching the design doc §3.5 protocol,
/// and flushes immediately (`tests/sim`'s orchestrator reads this over a
/// pipe and needs each line as soon as it's written, same as the
/// pre-existing insert-only worker's own behavior).
pub(crate) fn print_outcome(out: &mut impl Write, agent: u64, op: u64, outcome: &ExecOutcome) {
    match outcome {
        ExecOutcome::CommittedInsert { row_id, .. } => {
            writeln!(out, "agent {agent} committed insert op {op} row_id {row_id}").unwrap();
        }
        ExecOutcome::CommittedDelete { target_row_id } => {
            writeln!(
                out,
                "agent {agent} committed delete op {op} target_row_id {target_row_id}"
            )
            .unwrap();
        }
        ExecOutcome::CommittedUpdate { target_row_id, row_id } => {
            writeln!(
                out,
                "agent {agent} committed update op {op} target_row_id {target_row_id} row_id {row_id}"
            )
            .unwrap();
        }
        ExecOutcome::CommittedMultiBatch { row_ids } => {
            writeln!(
                out,
                "agent {agent} committed multibatch op {op} row_ids {},{}",
                row_ids[0], row_ids[1]
            )
            .unwrap();
        }
        ExecOutcome::Dropped => {
            writeln!(out, "agent {agent} dropped op {op} (conflict)").unwrap();
        }
    }
    out.flush().unwrap();
}
```

Add these new tests inside the existing `mod tests` block (alongside `lookup_row_id_finds_the_row_just_inserted` and the registry tests from Task 2):

```rust
    #[test]
    fn commit_with_retry_once_commits_on_first_success() {
        let outcome = commit_with_retry_once(|| Ok(()), "test");
        assert!(matches!(outcome, RetryOutcome::Committed));
    }

    #[test]
    fn commit_with_retry_once_retries_and_commits_after_one_conflict() {
        let mut calls = 0;
        let outcome = commit_with_retry_once(
            || {
                calls += 1;
                if calls == 1 {
                    Err(TxnError::Conflict {
                        contested_row_ids: vec![1],
                    })
                } else {
                    Ok(())
                }
            },
            "test",
        );
        assert!(matches!(outcome, RetryOutcome::Committed));
        assert_eq!(calls, 2, "must retry exactly once, not more");
    }

    #[test]
    fn commit_with_retry_once_drops_after_a_second_conflict() {
        let mut calls = 0;
        let outcome = commit_with_retry_once(
            || {
                calls += 1;
                Err(TxnError::Conflict {
                    contested_row_ids: vec![1],
                })
            },
            "test",
        );
        assert!(matches!(outcome, RetryOutcome::Dropped));
        assert_eq!(calls, 2, "must attempt exactly twice, never a third time");
    }

    #[test]
    #[should_panic(expected = "unexpected commit error on test:")]
    fn commit_with_retry_once_panics_on_a_non_conflict_error_on_the_first_attempt() {
        commit_with_retry_once(|| Err(TxnError::NotFound(std::path::PathBuf::from("x"))), "test");
    }

    #[test]
    #[should_panic(expected = "unexpected commit error on test retry:")]
    fn commit_with_retry_once_panics_on_a_non_conflict_error_on_the_retry() {
        // Distinct from the first-attempt panic test above: the message
        // prefix "unexpected commit error on test" is a substring of both
        // panic sites, so without asserting the ":" vs " retry:" suffix
        // (and without a first attempt that actually conflicts) this
        // would pass even if the retry arm's panic message were wrong.
        let mut calls = 0;
        commit_with_retry_once(
            || {
                calls += 1;
                if calls == 1 {
                    Err(TxnError::Conflict {
                        contested_row_ids: vec![1],
                    })
                } else {
                    Err(TxnError::NotFound(std::path::PathBuf::from("x")))
                }
            },
            "test",
        );
    }

    #[test]
    fn print_outcome_matches_the_documented_ack_line_format_for_every_variant() {
        // Task 7's orchestrator parses these lines by exact format -- a
        // typo here (a missing token, "multi_batch" instead of
        // "multibatch", a space instead of a comma) would compile and
        // pass every execute_* test while silently breaking Task 7's
        // parser. Round-trips through a Vec<u8> writer rather than
        // spawning the real worker binary, so this stays a fast unit test.
        let mut out: Vec<u8> = Vec::new();
        print_outcome(
            &mut out,
            1,
            2,
            &ExecOutcome::CommittedInsert {
                business_id: 99,
                row_id: 5,
            },
        );
        print_outcome(&mut out, 1, 3, &ExecOutcome::CommittedDelete { target_row_id: 5 });
        print_outcome(
            &mut out,
            1,
            4,
            &ExecOutcome::CommittedUpdate {
                target_row_id: 5,
                row_id: 6,
            },
        );
        print_outcome(
            &mut out,
            1,
            5,
            &ExecOutcome::CommittedMultiBatch { row_ids: [7, 8] },
        );
        print_outcome(&mut out, 1, 6, &ExecOutcome::Dropped);

        let printed = String::from_utf8(out).unwrap();
        let expected = "\
agent 1 committed insert op 2 row_id 5
agent 1 committed delete op 3 target_row_id 5
agent 1 committed update op 4 target_row_id 5 row_id 6
agent 1 committed multibatch op 5 row_ids 7,8
agent 1 dropped op 6 (conflict)
";
        assert_eq!(printed, expected);
    }

    #[test]
    fn execute_insert_returns_the_looked_up_row_id() {
        let dir = temp_dir("execute-insert");
        let dataset = Dataset::create(&dir).unwrap();
        let outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        assert!(matches!(
            outcome,
            ExecOutcome::CommittedInsert { business_id: 1, row_id: 0 }
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_delete_tombstones_the_target_row() {
        let dir = temp_dir("execute-delete");
        let dataset = Dataset::create(&dir).unwrap();
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert { row_id, .. } = insert_outcome else {
            panic!("expected CommittedInsert");
        };

        let delete_outcome = execute_delete(&dataset, row_id);
        assert!(matches!(
            delete_outcome,
            ExecOutcome::CommittedDelete { target_row_id } if target_row_id == row_id
        ));

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(visible.num_rows(), 0, "the deleted row must no longer be visible");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_update_tombstones_the_old_row_and_makes_the_new_one_visible() {
        let dir = temp_dir("execute-update");
        let dataset = Dataset::create(&dir).unwrap();
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert { row_id: old_row_id, .. } = insert_outcome else {
            panic!("expected CommittedInsert");
        };

        let update_outcome = execute_update(&dataset, old_row_id, 2, "agent0", [9.0, 9.0, 9.0]);
        let ExecOutcome::CommittedUpdate { target_row_id, row_id: new_row_id } = update_outcome else {
            panic!("expected CommittedUpdate, got {update_outcome:?}");
        };
        assert_eq!(target_row_id, old_row_id);
        assert_ne!(new_row_id, old_row_id, "the replacement insert must get a fresh row-id");

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(visible.num_rows(), 1, "exactly the new row must be visible");
        let id_col = visible
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 2, "the new row's business id must be the update's, not the old one's");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn execute_multi_batch_insert_commits_both_rows_in_one_transaction() {
        let dir = temp_dir("execute-multibatch");
        let dataset = Dataset::create(&dir).unwrap();
        let outcome = execute_multi_batch_insert(
            &dataset,
            [1, 2],
            "agent0",
            [[1.0, 1.0, 1.0], [2.0, 2.0, 2.0]],
        );
        let ExecOutcome::CommittedMultiBatch { row_ids } = outcome else {
            panic!("expected CommittedMultiBatch, got {outcome:?}");
        };
        assert_ne!(row_ids[0], row_ids[1]);

        let schema = strata_txn::mvp_fixtures::mvp_schema();
        let visible = dataset.snapshot().scan(&schema).unwrap();
        assert_eq!(visible.num_rows(), 2, "both rows from the one multi-batch commit must be visible");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deleting_an_already_tombstoned_row_is_a_harmless_idempotent_commit() {
        // NOT a test of the retry-then-drop path: this single-threaded
        // scenario has no second concurrent transaction, so
        // execute_delete's second call here cannot produce an actual
        // TxnError::Conflict -- it just re-tombstones an already-dead
        // row-id, which the design doc's own note says is harmless. This
        // pins that specific claim down. Genuine conflict-drop coverage
        // (a real TxnError::Conflict, retried once, then dropped) can
        // only come from real concurrent interleaving -- that's exercised
        // by Task 8's chaos-tier runs (many agents, real scheduling, a
        // shared contested pool), not by a unit test here.
        let dir = temp_dir("execute-delete-idempotent-retombstone");
        let dataset = Dataset::create(&dir).unwrap();
        let insert_outcome = execute_insert(&dataset, 1, "agent0", [1.0, 2.0, 3.0]);
        let ExecOutcome::CommittedInsert { row_id, .. } = insert_outcome else {
            panic!("expected CommittedInsert");
        };

        let first = execute_delete(&dataset, row_id);
        assert!(matches!(first, ExecOutcome::CommittedDelete { .. }));

        let second = execute_delete(&dataset, row_id);
        assert!(
            matches!(second, ExecOutcome::CommittedDelete { .. }),
            "re-deleting an already-tombstoned row-id must commit cleanly, not error or drop: {second:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: all `commit_ops::tests::*` tests pass.

- [ ] **Step 3: Run the full verification gate**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 4: Commit**

```bash
git add crates/chaos-worker/src/commit_ops.rs
git commit -m "feat(chaos-worker): add commit execution, conflict retry, and ack-line printing"
```

---

### Task 4: Live predicate-pruning reader thread

**Files:**
- Create: `crates/chaos-worker/src/reader.rs`
- Modify: `crates/chaos-worker/src/main.rs` (add `mod reader;`)

**Interfaces:**
- Consumes: `schema_with_row_id` from `crate::schema` (Task 2).
- Produces: `pub(crate) fn spawn(dataset: Arc<Dataset>) -> (JoinHandle<()>, Arc<AtomicBool>)` — Task 6's `main()` calls this once at startup and sets the returned flag before joining.

- [ ] **Step 1: Write the failing tests**

Create `crates/chaos-worker/src/reader.rs`:

```rust
//! The live predicate-pruning correctness check — see design doc §3.3.
//! Runs on its own thread for the whole worker process lifetime,
//! concurrently with the main commit loop, comparing a zone-map-pruned
//! predicate query against an unpruned reference on the SAME snapshot.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, StringArray, UInt64Array};
use strata_query::Predicate;
use strata_storage::Value;
use strata_txn::Dataset;

use crate::schema::schema_with_row_id;

const READER_PREDICATE_NAME: &str = "agent0";
const READER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);
/// Generous upper bound on rows any single chaos run will ever produce.
/// This bounds only the post-merge truncation across parts
/// (`SegmentSet::search_filtered_pruned_live`'s final `k`-truncation) --
/// per-part recall is governed by `ef` (`EF_SEARCH_DEFAULT`, widened by
/// `widen_ef`), not by this constant. Fine for this one-directional
/// pruned-subset-of-reference check; a future reverse-direction check
/// (reference implies prunable) would need to reason about `ef` instead.
const READER_SEARCH_K: usize = 100_000;

/// One iteration's check, as a pure function over already-fetched
/// row-id sets so it's unit-testable without a real `Dataset`. Returns
/// the pruned-but-not-in-reference row-ids, if any (a disagreement).
fn disagreement(pruned_row_ids: &[u64], reference_row_ids: &HashSet<u64>) -> Vec<u64> {
    pruned_row_ids
        .iter()
        .copied()
        .filter(|id| !reference_row_ids.contains(id))
        .collect()
}

/// Spawns the reader thread. The caller must set the returned
/// `Arc<AtomicBool>` (via `Ordering::SeqCst`) once every agent has
/// finished, then join the handle — the thread has no other stop signal
/// (a genuine chaos-induced crash kills it along with the whole process,
/// which needs no explicit signaling at all).
pub(crate) fn spawn(dataset: Arc<Dataset>) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let done = Arc::new(AtomicBool::new(false));
    let done_for_thread = Arc::clone(&done);
    let handle = std::thread::spawn(move || {
        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        while !done_for_thread.load(Ordering::SeqCst) {
            check_once(&dataset, &predicate);
            std::thread::sleep(READER_POLL_INTERVAL);
        }
        // One final check so the last batch of commits (landed between
        // the reader's last loop iteration and the writer setting
        // `done`) is still checked at least once.
        check_once(&dataset, &predicate);
    });
    (handle, done)
}

fn check_once(dataset: &Dataset, predicate: &Predicate) {
    let snapshot = dataset.snapshot();
    let schema = schema_with_row_id();

    // Neither call legitimately errors here: an empty snapshot (zero
    // committed files) resolves to Ok(vec![]) on both paths, and this
    // codebase has no compaction/GC that could make a manifest-listed
    // file vanish out from under a live snapshot. A real Err is therefore
    // always a genuine bug (a dimension mismatch, a predicate/column type
    // mismatch, a cast failure) — exactly the class of failure this
    // reader thread exists to surface via the global panic hook (design
    // doc §3.4), so it must panic here, not silently skip the check.
    let pruned = snapshot
        .vector_search(&[0.0, 0.0, 0.0], READER_SEARCH_K, Some(predicate))
        .expect("vector_search must succeed against a live snapshot");
    let pruned_row_ids: Vec<u64> = pruned.into_iter().map(|m| m.row_id).collect();

    let all_rows = snapshot
        .scan(&schema)
        .expect("scan must succeed against a live snapshot");
    let name_col = all_rows
        .column(all_rows.schema().index_of("name").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column must be Utf8");
    let row_id_col = all_rows
        .column(all_rows.schema().index_of(strata_txn::ROW_ID_COLUMN).unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("_row_id column must be UInt64");

    let reference_row_ids: HashSet<u64> = (0..all_rows.num_rows())
        .filter(|&i| name_col.value(i) == READER_PREDICATE_NAME)
        .map(|i| row_id_col.value(i))
        .collect();

    let bad = disagreement(&pruned_row_ids, &reference_row_ids);
    assert!(
        bad.is_empty(),
        "predicate-pruning disagreement: vector_search with Eq(name, {READER_PREDICATE_NAME:?}) \
         returned row-ids {bad:?}, which the unpruned reference scan does not have tagged \
         name={READER_PREDICATE_NAME:?} — zone-map pruning (or its merge across multi-batch \
         commits) returned a wrong result"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "strata-chaos-worker-reader-test-{label}-{}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn disagreement_is_empty_when_every_pruned_id_is_in_the_reference() {
        let reference: HashSet<u64> = [1, 2, 3].into_iter().collect();
        assert_eq!(disagreement(&[1, 2], &reference), Vec::<u64>::new());
    }

    #[test]
    fn disagreement_reports_a_pruned_id_missing_from_the_reference() {
        let reference: HashSet<u64> = [1, 2].into_iter().collect();
        assert_eq!(disagreement(&[1, 2, 99], &reference), vec![99]);
    }

    #[test]
    fn spawn_and_stop_against_a_real_but_empty_dataset_does_not_panic() {
        let dir = temp_dir("spawn-empty");
        let dataset = Arc::new(Dataset::create(&dir).unwrap());

        let (handle, done) = spawn(Arc::clone(&dataset));
        std::thread::sleep(std::time::Duration::from_millis(20));
        done.store(true, Ordering::SeqCst);
        handle.join().unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_once_passes_against_real_committed_rows() {
        // The empty-dataset spawn test above never exercises check_once's
        // actual comparison logic: num_rows() == 0 means the reference-set
        // construction, both `expect` downcasts, and the assertion never
        // run against real data anywhere else in this module's tests. Two
        // agents' worth of rows makes the "name" predicate load-bearing --
        // a check_once that ignored the predicate and returned every
        // row's id, or that matched the wrong column, would still pass a
        // single-agent version of this test.
        let dir = temp_dir("check-once-real-data");
        let dataset = Dataset::create(&dir).unwrap();
        let mut txn = dataset.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "agent0", [1.0, 0.0, 0.0]).unwrap());
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "agent1", [0.0, 1.0, 0.0]).unwrap());
        txn.commit().unwrap();

        let predicate = Predicate::Eq(
            "name".to_string(),
            Value::Utf8(READER_PREDICATE_NAME.to_string()),
        );
        // Must not panic: the pruned agent0-only search result must be a
        // subset of the unpruned reference scan's agent0 rows.
        check_once(&dataset, &predicate);

        std::fs::remove_dir_all(&dir).ok();
    }
}
```

In `crates/chaos-worker/src/main.rs`, add:

```rust
// Not yet called from main() -- wired in by Task 6, which removes this
// attribute once it is.
#[allow(dead_code)]
mod reader;
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: all `reader::tests::*` tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/chaos-worker/src/reader.rs crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): add the live predicate-pruning reader thread"
```

---

### Task 5: Global panic hook and reserved failure exit code

**Files:**
- Modify: `crates/chaos-worker/src/main.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `fn install_failure_hook()`, called once at the very start of `main()` in Task 6 — no other task depends on this beyond that call site. This task does not modify `fn main()` itself at all (see Step 2's note on why the original plan for that turned out to be broken).

- [ ] **Step 1: Write the hook**

In `crates/chaos-worker/src/main.rs`, add this function (near the top, after the `mod` declarations and before `fn main()`):

```rust
/// Installs a global panic hook: any panic, on the main thread or the
/// reader thread, prints a `GENUINE_FAILURE: <message>` line and exits
/// with a reserved code — distinct from `chaos_checkpoint`'s
/// `std::process::abort()` (an entirely different termination mechanism,
/// not a panic) — so `tests/sim`'s orchestrator can tell a genuine bug
/// apart from an expected chaos-induced crash without decoding
/// OS-specific exit signals. See design doc §3.4.
const GENUINE_FAILURE_EXIT_CODE: i32 = 2;

fn install_failure_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        // Best-effort: if even this write fails, still exit with the
        // reserved code below so the orchestrator's exit-code check still
        // fires (it does not require the message line to be present).
        let _ = writeln!(out, "GENUINE_FAILURE: {message}");
        let _ = out.flush();
        std::process::exit(GENUINE_FAILURE_EXIT_CODE);
    }));
}
```

Note: `writeln!`/`Write` are already imported at the top of `main.rs` (`use std::io::Write as _;`) — no new import needed for this step.

- [ ] **Step 2: Add a direct test for the hook, via a subprocess**

**Correction (caught before implementation, not a review finding on committed code):** an earlier version of this step spawned `std::env::current_exe()` with a bare `--test-trigger-genuine-failure` CLI flag and had `fn main()` check for it as its first action. That does not work: under `cargo test`, `current_exe()` returns the *test-harness* binary (the one whose entry point is libtest's generated `main`, not this crate's `fn main()` — visible in every test run's own header line, `Running unittests src\main.rs (target\debug\deps\chaos_worker-<hash>.exe)`). Passing an arbitrary flag to that binary reaches libtest's own CLI parser, not `fn main()` — verified empirically with a throwaway scratch crate reproducing this exact shape: libtest rejects the unrecognized flag (`error: Unrecognized option: 'trigger'`, exit code 101), so `fn main()`'s check is never reached and the panic never fires. Two consequences: this task needs no `fn main()` modification at all (Task 6 is unaffected — its `main()` just calls `install_failure_hook()` as its first line, nothing else), and the test must trigger the panic from *inside* the test-harness process itself, not by handing it a flag it won't recognize.

The corrected pattern: a second, normally-inert `#[test]` function that only does anything when an env var is set, invoked by the first test via `current_exe()` with libtest's own `--exact <test-path>` filter (so only that one test runs in the subprocess) plus the env var that makes it act. This was also verified empirically (same scratch crate): the subprocess exits with code 2, prints the `GENUINE_FAILURE:` line, and — critically — every *other* test in the suite is unaffected when the env var is unset, because the inner test is then a no-op. This is what makes it safe despite `install_failure_hook`'s global `std::panic::set_hook` call: without the env var, the inner test never calls it, so the shared test-runner process's default hook is never touched.

Add a `#[cfg(test)]` block at the bottom of `main.rs`:

```rust
#[cfg(test)]
mod failure_hook_tests {
    use super::install_failure_hook;

    /// Env var gating this test's actual body -- unset (the normal case,
    /// when this test runs as part of the full suite) it's a no-op, so it
    /// never installs the global panic hook and never disturbs any other
    /// test sharing this process. Only the subprocess spawned by
    /// `a_panic_after_the_hook_is_installed_exits_with_the_reserved_code`,
    /// which sets this var and filters to run ONLY this one test, ever
    /// exercises the real body.
    const TRIGGER_ENV_VAR: &str = "CHAOS_WORKER_TEST_TRIGGER_GENUINE_FAILURE";

    #[test]
    fn inner_trigger_genuine_failure() {
        if std::env::var(TRIGGER_ENV_VAR).is_err() {
            return;
        }
        install_failure_hook();
        panic!("deliberate test panic");
    }

    // Spawns THIS SAME test binary (via current_exe()), filtered with
    // libtest's own --exact to run ONLY inner_trigger_genuine_failure,
    // with the trigger env var set -- the only way to observe a global
    // panic hook's effect without corrupting other tests sharing this
    // process. A bare CLI flag checked in fn main() does NOT work here:
    // current_exe() under `cargo test` is the libtest-harness binary,
    // not this crate's real fn main() entry point, so an arbitrary flag
    // reaches libtest's own parser (which rejects it), not fn main().
    #[test]
    fn a_panic_after_the_hook_is_installed_exits_with_the_reserved_code() {
        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "failure_hook_tests::inner_trigger_genuine_failure",
                "--exact",
                "--nocapture",
            ])
            .env(TRIGGER_ENV_VAR, "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("GENUINE_FAILURE: deliberate test panic"),
            "expected the marker line in stdout, got: {stdout}"
        );
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: `failure_hook_tests::a_panic_after_the_hook_is_installed_exits_with_the_reserved_code` and `failure_hook_tests::inner_trigger_genuine_failure` both pass (the inner one passes trivially as a no-op in the normal run — it only does real work inside the subprocess the outer test spawns).

- [ ] **Step 4: Run the full verification gate**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): add global panic hook with a reserved genuine-failure exit code"
```

---

### Task 6: Pool setup and the rewritten scheduler loop (wiring it all together)

**Files:**
- Modify: `crates/chaos-worker/src/main.rs` (full rewrite of `fn main()`'s body below the Task 5 hook/trigger check)

**Interfaces:**
- Consumes: everything from Tasks 1-5 (`ops::{generate_verb_sequence, resolve_target, resolve_slot_consumption, OpVerb}`, `commit_ops::{Registry, execute_insert, execute_delete, execute_update, execute_multi_batch_insert, print_outcome, ExecOutcome}`, `reader::spawn`, `install_failure_hook`).
- Produces: the complete worker binary — Task 7 (the orchestrator) is the only consumer, and it only observes this task's output via stdout/exit-code, not via any Rust API.

- [ ] **Step 1: Replace the entire contents of `main.rs`**

By this task, `crates/chaos-worker/src/main.rs` has accumulated, from Tasks 1/2/4/5: four `mod` declarations (each with a temporary `#[allow(dead_code)]` per Global Constraint 12), `install_failure_hook`, and its `failure_hook_tests` module (which does NOT touch `fn main()` — see Task 5's Step 2 correction). This step **replaces the ENTIRE FILE, from the first line to the last** — consolidating all of that into one coherent final version, superseding (not adding to) the smaller incremental edits those earlier tasks made. Do not end up with the `mod` declarations, `install_failure_hook`, or `failure_hook_tests` defined twice, and drop every `#[allow(dead_code)]` — by this task every module is genuinely called from `main()`, so none of them are needed anymore (the code below has none).

Replace the entire contents of `crates/chaos-worker/src/main.rs` with:

```rust
//! Chaos-testing worker: deterministically commits a seed-derived sequence
//! of operations against a real `Dataset`, printing and flushing an
//! acknowledgment after every successful commit. Meant to be spawned as a
//! child process by `tests/sim`'s orchestrator with `STRATA_CHAOS_ABORT_AT`
//! set, so it may be aborted mid-run by `strata_storage::chaos`. See
//! `docs/superpowers/specs/2026-07-27-chaos-worker-workload-extension-design.md`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod commit_ops;
mod ops;
mod reader;
mod schema;

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;

use commit_ops::{ExecOutcome, Registry, execute_delete, execute_insert, execute_multi_batch_insert, execute_update};
use ops::{OpVerb, generate_verb_sequence, resolve_slot_consumption, resolve_target};

const GENUINE_FAILURE_EXIT_CODE: i32 = 2;
const POOL_SIZE: u64 = 6;
const POOL_STREAM: u64 = 0x9001_5EED_0000_0001;
const TARGET_STREAM: u64 = 0x7A46_E7D0_0000_0002;

fn install_failure_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let _ = writeln!(out, "GENUINE_FAILURE: {message}");
        let _ = out.flush();
        std::process::exit(GENUINE_FAILURE_EXIT_CODE);
    }));
}

/// Commits `POOL_SIZE` individual single-row inserts before the
/// interleaved phase starts, establishing the shared contested row-id
/// pool — see design doc §3.1. Business ids are negative (`-1..-POOL_SIZE`)
/// so they can never collide with an agent's own `global_id` business ids
/// (always >= 0).
fn setup_contested_pool(
    dataset: &strata_txn::Dataset,
    seed: u64,
    registry: &mut Registry,
    out: &mut impl std::io::Write,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ POOL_STREAM);
    for i in 0..POOL_SIZE {
        let business_id = -1 - i64::try_from(i).unwrap();
        let vector = [rng.random::<f32>(), rng.random::<f32>(), rng.random::<f32>()];
        let outcome = execute_insert(dataset, business_id, "pool", vector);
        let ExecOutcome::CommittedInsert { row_id, .. } = outcome else {
            panic!("pool setup insert must always commit cleanly: {outcome:?}");
        };
        registry.record_pool_row(row_id);
        writeln!(out, "pool committed insert row_id {row_id}").unwrap();
        out.flush().unwrap();
    }
}

fn main() {
    install_failure_hook();

    let args: Vec<String> = std::env::args().collect();
    let dir = args
        .get(1)
        .expect("usage: chaos-worker <dir> <seed> <num_agents> <ops_per_agent>");
    let seed: u64 = args.get(2).expect("missing <seed>").parse().expect("seed must be a u64");
    let num_agents: u64 = args
        .get(3)
        .expect("missing <num_agents>")
        .parse()
        .expect("num_agents must be a u64");
    let ops_per_agent: u64 = args
        .get(4)
        .expect("missing <ops_per_agent>")
        .parse()
        .expect("ops_per_agent must be a u64");

    let dataset = Arc::new(
        strata_txn::Dataset::open(dir)
            .or_else(|_| strata_txn::Dataset::create(dir))
            .expect("failed to open or create dataset"),
    );

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let num_agents_usize = usize::try_from(num_agents).unwrap();
    let mut registry = Registry::new(num_agents_usize);
    setup_contested_pool(&dataset, seed, &mut registry, &mut out);

    let (reader_handle, reader_done) = reader::spawn(Arc::clone(&dataset));

    // Per-agent vector generation (unchanged from the original insert-only
    // worker) and verb generation (new — see ops.rs), both pre-generated
    // up front. Target resolution for Delete/Update stays just-in-time
    // (see resolve_target's own doc comment for why).
    let agent_vectors: Vec<Vec<[f32; 3]>> = (0..num_agents)
        .map(|agent| {
            let mut agent_rng = ChaCha8Rng::seed_from_u64(seed ^ agent);
            (0..ops_per_agent)
                .map(|op| {
                    let global_id = agent * ops_per_agent + op;
                    #[allow(clippy::cast_precision_loss)]
                    let v = global_id as f32;
                    [
                        v + agent_rng.random::<f32>(),
                        v + agent_rng.random::<f32>(),
                        v + agent_rng.random::<f32>(),
                    ]
                })
                .collect()
        })
        .collect();
    let agent_verbs: Vec<Vec<OpVerb>> = (0..num_agents)
        .map(|agent| generate_verb_sequence(seed, agent, ops_per_agent))
        .collect();
    let mut agent_target_rngs: Vec<ChaCha8Rng> = (0..num_agents)
        .map(|agent| ChaCha8Rng::seed_from_u64(seed ^ agent ^ TARGET_STREAM))
        .collect();

    let mut next_op: Vec<u64> = vec![0; num_agents_usize];
    let mut remaining: Vec<u64> = vec![ops_per_agent; num_agents_usize];
    let mut scheduler_rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA9E1_C0DE_u64);

    loop {
        let live_agents: Vec<usize> = (0..num_agents_usize).filter(|&a| remaining[a] > 0).collect();
        if live_agents.is_empty() {
            break;
        }
        let pick = live_agents[scheduler_rng.random_range(0..live_agents.len())];
        let agent = pick as u64;
        let op = next_op[pick];
        let drawn_verb = agent_verbs[pick][usize::try_from(op).unwrap()];
        let (verb, slots_consumed) = resolve_slot_consumption(drawn_verb, remaining[pick]);

        let outcome = match verb {
            OpVerb::Insert => {
                let global_id = agent * ops_per_agent + op;
                let vector = agent_vectors[pick][usize::try_from(op).unwrap()];
                execute_insert(&dataset, i64::try_from(global_id).unwrap(), &format!("agent{agent}"), vector)
            }
            OpVerb::MultiBatchInsert => {
                let global_id_0 = agent * ops_per_agent + op;
                let global_id_1 = agent * ops_per_agent + op + 1;
                let vector_0 = agent_vectors[pick][usize::try_from(op).unwrap()];
                let vector_1 = agent_vectors[pick][usize::try_from(op + 1).unwrap()];
                execute_multi_batch_insert(
                    &dataset,
                    [i64::try_from(global_id_0).unwrap(), i64::try_from(global_id_1).unwrap()],
                    &format!("agent{agent}"),
                    [vector_0, vector_1],
                )
            }
            OpVerb::Delete => {
                match resolve_target(&mut agent_target_rngs[pick], registry.pool_rows(), registry.own_rows(pick)) {
                    Some(target_row_id) => execute_delete(&dataset, target_row_id),
                    None => {
                        // No eligible target yet -- downgrade to Insert per design doc §3.1.
                        let global_id = agent * ops_per_agent + op;
                        let vector = agent_vectors[pick][usize::try_from(op).unwrap()];
                        execute_insert(&dataset, i64::try_from(global_id).unwrap(), &format!("agent{agent}"), vector)
                    }
                }
            }
            OpVerb::Update => {
                match resolve_target(&mut agent_target_rngs[pick], registry.pool_rows(), registry.own_rows(pick)) {
                    Some(target_row_id) => {
                        let global_id = agent * ops_per_agent + op;
                        let vector = agent_vectors[pick][usize::try_from(op).unwrap()];
                        execute_update(&dataset, target_row_id, i64::try_from(global_id).unwrap(), &format!("agent{agent}"), vector)
                    }
                    None => {
                        let global_id = agent * ops_per_agent + op;
                        let vector = agent_vectors[pick][usize::try_from(op).unwrap()];
                        execute_insert(&dataset, i64::try_from(global_id).unwrap(), &format!("agent{agent}"), vector)
                    }
                }
            }
        };

        match &outcome {
            ExecOutcome::CommittedInsert { row_id, .. } => registry.record_own_row(pick, *row_id),
            ExecOutcome::CommittedUpdate { target_row_id, row_id } => {
                registry.remove(pick, *target_row_id);
                registry.record_own_row(pick, *row_id);
            }
            ExecOutcome::CommittedDelete { target_row_id } => registry.remove(pick, *target_row_id),
            ExecOutcome::CommittedMultiBatch { row_ids } => {
                registry.record_own_row(pick, row_ids[0]);
                registry.record_own_row(pick, row_ids[1]);
            }
            ExecOutcome::Dropped => {}
        }
        commit_ops::print_outcome(&mut out, agent, op, &outcome);

        next_op[pick] += slots_consumed;
        remaining[pick] -= slots_consumed;
    }

    reader_done.store(true, Ordering::SeqCst);
    reader_handle.join().expect("reader thread panicked without going through the failure hook -- this should be unreachable");
}

// Carried over verbatim from Task 5 -- this full-file replacement does not
// touch failure_hook_tests, it only removes the #[allow(dead_code)]
// attributes that guarded the mod declarations above while they were
// unused. See Task 5's Step 2 for why this needs current_exe() + an
// env-var-gated inner test rather than a fn main() CLI-flag check.
#[cfg(test)]
mod failure_hook_tests {
    use super::install_failure_hook;

    const TRIGGER_ENV_VAR: &str = "CHAOS_WORKER_TEST_TRIGGER_GENUINE_FAILURE";

    #[test]
    fn inner_trigger_genuine_failure() {
        if std::env::var(TRIGGER_ENV_VAR).is_err() {
            return;
        }
        install_failure_hook();
        panic!("deliberate test panic");
    }

    #[test]
    fn a_panic_after_the_hook_is_installed_exits_with_the_reserved_code() {
        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "failure_hook_tests::inner_trigger_genuine_failure",
                "--exact",
                "--nocapture",
            ])
            .env(TRIGGER_ENV_VAR, "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("GENUINE_FAILURE: deliberate test panic"),
            "expected the marker line in stdout, got: {stdout}"
        );
    }
}
```

- [ ] **Step 2: Run the deterministic (non-chaos) tests to verify nothing regressed**

Run: `cargo test -p strata-chaos-worker --bin chaos-worker`
Expected: all `ops::tests::*`, `commit_ops::tests::*`, `schema::tests::*`, `reader::tests::*` tests still pass (this task only touches `main()`, which none of them call).

Run: `cargo test -p strata-chaos-worker --test '*' 2>&1 || true` — actually there is no separate integration test file for chaos-worker; skip this and instead do a manual smoke run:

Run: `cargo run -p strata-chaos-worker --release -- /tmp/strata-chaos-smoke 1 3 10` (on Windows, use a real temp path instead of `/tmp/...`, e.g. `%TEMP%\strata-chaos-smoke`)
Expected: the process runs to completion, printing a mix of `committed insert`/`committed delete`/`committed update`/`committed multibatch`/`dropped` lines, then exits 0. Delete the temp directory afterward.

- [ ] **Step 3: Run the full verification gate**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 4: Commit**

```bash
git add crates/chaos-worker/src/main.rs
git commit -m "feat(chaos-worker): wire pool setup, scheduler, reader thread, and failure hook together"
```

---

### Task 7: Rewrite the orchestrator (`tests/sim/tests/chaos.rs`)

**Files:**
- Modify: `tests/sim/tests/chaos.rs`

**Interfaces:**
- Consumes: the stdout protocol from Task 3 (`print_outcome`'s exact line formats) and the exit-code contract from Task 5 (`GENUINE_FAILURE_EXIT_CODE = 2`). Does not depend on any Rust-level API from `crates/chaos-worker` — this crate only talks to it over stdout/exit-code, as today.
- Produces: nothing further downstream — this is the last task before final integration validation (Task 8).

- [ ] **Step 1: Replace `RunResult`, `run_worker`, and `check_invariants`**

In `tests/sim/tests/chaos.rs`, replace the `RunResult` struct, `run_worker`, and `check_invariants` (everything between `worker_bin_path` and the `#[test] fn fast_tier_random_seeds_survive_random_crash_points`) with:

```rust
struct RunResult {
    acknowledged_inserts: HashSet<u64>,
    acknowledged_tombstones: HashSet<u64>,
    crashed: bool,
}

fn run_worker(dir: &std::path::Path, seed: u64, abort_at: Option<u64>) -> RunResult {
    let mut cmd = Command::new(worker_bin_path());
    cmd.args([
        dir.to_str().unwrap(),
        &seed.to_string(),
        &NUM_AGENTS.to_string(),
        &OPS_PER_AGENT.to_string(),
    ]);
    if let Some(n) = abort_at {
        cmd.env("STRATA_CHAOS_ABORT_AT", n.to_string());
    }
    let output = cmd.output().unwrap();

    // Exit code 2 is the reserved genuine-failure signal (design doc
    // §3.4) -- a real bug, never an expected chaos-abort. Fail the test
    // immediately with the worker's own printed message, before even
    // attempting to reopen the dataset.
    if output.status.code() == Some(2) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let message = stdout
            .lines()
            .find(|l| l.starts_with("GENUINE_FAILURE:"))
            .unwrap_or("GENUINE_FAILURE: <no message line found in stdout>");
        panic!("chaos-worker reported a genuine failure at seed={seed} abort_at={abort_at:?}: {message}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut acknowledged_inserts = HashSet::new();
    let mut acknowledged_tombstones = HashSet::new();
    for line in stdout.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        match words.as_slice() {
            ["pool", "committed", "insert", "row_id", row_id] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
            }
            [.., "committed", "insert", "op", _, "row_id", row_id] => {
                acknowledged_inserts.insert(row_id.parse().unwrap());
            }
            [.., "committed", "delete", "op", _, "target_row_id", target_row_id] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
            }
            [.., "committed", "update", "op", _, "target_row_id", target_row_id, "row_id", row_id] => {
                acknowledged_tombstones.insert(target_row_id.parse().unwrap());
                acknowledged_inserts.insert(row_id.parse().unwrap());
            }
            [.., "committed", "multibatch", "op", _, "row_ids", row_ids] => {
                for id in row_ids.split(',') {
                    acknowledged_inserts.insert(id.parse().unwrap());
                }
            }
            [.., "dropped", "op", _, "(conflict)"] => {
                // Informational only -- not an acknowledgment either way.
            }
            _ => panic!("unrecognized chaos-worker stdout line: {line:?}"),
        }
    }

    RunResult {
        acknowledged_inserts,
        acknowledged_tombstones,
        crashed: !output.status.success(),
    }
}

fn check_invariants(dir: &std::path::Path, result: &RunResult) {
    let acknowledged = &result.acknowledged_inserts;
    let crashed = result.crashed;

    // Invariant 1: no corruption (see the original comment this
    // preserves verbatim in spirit -- only the acknowledged-set field
    // name changed).
    let dataset = match strata_txn::Dataset::open(dir) {
        Ok(ds) => ds,
        Err(strata_txn::TxnError::NotFound(_)) if acknowledged.is_empty() => return,
        Err(e) => panic!("dataset failed to reopen after crash — corruption: {e}"),
    };

    let schema = strata_txn::mvp_fixtures::mvp_schema();
    let batch = dataset.snapshot().scan(&schema).expect("scan failed after reopen");
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let visible_row_ids: HashSet<u64> = (0..batch.num_rows())
        .map(|i| u64::try_from(id_col.value(i)).unwrap())
        .collect();

    // Invariant 2: no lost commits.
    let lost: Vec<&u64> = acknowledged.difference(&visible_row_ids).collect();
    assert!(lost.is_empty(), "lost commits: acknowledged but not visible after reopen: {lost:?}");

    // Invariant 3: no phantom commits (at most one tolerated for a
    // crashed run — the single-in-flight-op ambiguous-outcome case).
    let phantom: Vec<&u64> = visible_row_ids.difference(acknowledged).collect();
    let max_tolerated_phantoms = usize::from(crashed);
    assert!(
        phantom.len() <= max_tolerated_phantoms,
        "phantom commits: visible after reopen but never acknowledged: {phantom:?} \
         (tolerated at most {max_tolerated_phantoms} for this {} run)",
        if crashed { "crashed" } else { "clean" }
    );

    // New invariant: no resurrected tombstones. Unlike an ambiguous
    // insert outcome, there is no legitimate scenario where a durably
    // tombstoned row should still be visible -- Snapshot::is_visible is a
    // pure `!tombstones.contains` check with no timing window, so this
    // gets NO crash-tolerance carve-out.
    let resurrected: Vec<&u64> = result
        .acknowledged_tombstones
        .intersection(&visible_row_ids)
        .collect();
    assert!(
        resurrected.is_empty(),
        "resurrected tombstones: acknowledged as deleted but visible after reopen: {resurrected:?}"
    );

    // Invariant 4: row + index consistency (unchanged from the
    // insert-only version; still iterates every currently-visible row).
    for &row_id in &visible_row_ids {
        let row_idx = (0..batch.num_rows())
            .find(|&i| u64::try_from(id_col.value(i)).unwrap() == row_id)
            .expect("visible row must be in the scanned batch (it was just derived from it)");
        let vector_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
            .unwrap();
        let values = vector_col.value(row_idx);
        let values: &arrow::array::Float32Array = values.as_any().downcast_ref().unwrap();
        let query: Vec<f32> = (0..values.len()).map(|i| values.value(i)).collect();

        let results = dataset.snapshot().vector_search(&query, 1, None).expect("vector_search failed");
        assert!(
            !results.is_empty() && results[0].squared_distance < 0.001,
            "row {row_id} is visible in the row store but not findable in the HNSW graph \
             (row+index consistency violated) — got {results:?}"
        );
    }
}
```

Note: the reference row-ids in the ack-line parser (e.g. `row_id.parse()`) parse as `u64`, matching the `id_col.value(i)` cast used in `check_invariants` — both namespaces are still the "business id" values chaos-worker prints, consistent with the pre-existing design (see the untouched invariant-4 comment in the original file about `VectorMatch::row_id` being a different, internal identifier — that distinction is unaffected by this rewrite).

- [ ] **Step 2: Update both call sites of `check_invariants`**

In both `fast_tier_random_seeds_survive_random_crash_points` and `thorough_tier_satisfies_the_phase_7_exit_criterion`, find:

```rust
check_invariants(&dir, &result.acknowledged_row_ids, result.crashed);
```

Replace with:

```rust
check_invariants(&dir, &result);
```

And in `fast_tier_random_seeds_survive_random_crash_points`, find:

```rust
        if !result.crashed {
            // The randomly-picked threshold happened to exceed the total
            // checkpoint count for this seed — the run completed cleanly.
            // Still a valid, still-checked iteration; not a bug.
            assert_eq!(
                result.acknowledged_row_ids.len(),
                usize::try_from(NUM_AGENTS * OPS_PER_AGENT).unwrap(),
                "worker exited successfully but didn't acknowledge every op"
            );
        }
```

Replace with:

```rust
        if !result.crashed {
            // The randomly-picked threshold happened to exceed the total
            // checkpoint count for this seed — the run completed cleanly.
            // Still a valid, still-checked iteration; not a bug. Unlike
            // the insert-only workload, a clean run's total acknowledged
            // COUNT is no longer a fixed function of NUM_AGENTS *
            // OPS_PER_AGENT alone (MultiBatchInsert consumes 2 slots per
            // commit, Delete/Update don't grow acknowledged_inserts by
            // one-per-slot the way Insert does, and a dropped conflict
            // acknowledges nothing at all) -- so this checks only that
            // SOMETHING was acknowledged, not an exact count.
            assert!(
                !result.acknowledged_inserts.is_empty() || !result.acknowledged_tombstones.is_empty(),
                "worker exited successfully but acknowledged nothing at all"
            );
        }
```

- [ ] **Step 3: Run the fast tier to verify the rewrite works end-to-end**

Run: `cargo test -p strata-sim --test chaos fast_tier_random_seeds_survive_random_crash_points --release -- --nocapture`
Expected: `test result: ok. 1 passed`. If it fails, the failure message will name which invariant broke and at which seed/abort_at — use `STRATA_CHAOS_ONLY_SEED`/re-running with that exact seed to reproduce and debug before proceeding (do not weaken an invariant to make it pass without understanding why it failed first — per `.claude/rules/concurrency-txn-layer.md`'s spirit, a failing invariant here is exactly the kind of real signal this harness exists to catch).

- [ ] **Step 4: Run the full verification gate**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/sim/tests/chaos.rs
git commit -m "test(sim): rewrite chaos orchestrator for the richer op protocol and no-resurrected-tombstones invariant"
```

---

### Task 8: Full integration validation

**Files:** none modified — this task only runs and observes.

**Interfaces:** N/A (validation only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass, including `strata-chaos-worker`'s new unit tests and `strata-sim`'s fast-tier chaos test.

- [ ] **Step 2: Run the thorough tier**

Run: `STRATA_CHAOS_THOROUGH=1 cargo test -p strata-sim --test chaos thorough_tier_satisfies_the_phase_7_exit_criterion --release -- --nocapture`
Expected: `2000/2000 seeds checked, zero violations so far` progress lines, ending `test result: ok. 1 passed`. This run now exercises the full op mix (insert/delete/update/multibatch/conflict-drop) and the live reader thread, not just inserts — expect it to take noticeably longer than the pre-existing ~6.4-minute baseline (the extra `scan_with_predicate` row-id lookups and the reader thread's own queries both add real I/O per commit); record the new wall-clock time in the commit message below for future reference.

- [ ] **Step 3: Run the final full verification gate**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
Expected: all clean.

- [ ] **Step 4: Commit (if Steps 1-3 required any fixes; otherwise skip — nothing to commit)**

```bash
git add -A
git commit -m "fix(chaos): address issues found by the full thorough-tier run"
```

---

## Self-Review

- **Spec coverage:** §3.1 (op verbs, target resolution, pool, multi-batch) → Tasks 1, 6. §3.2 (conflict retry) → Task 3. §3.3 (reader thread) → Task 4. §3.4 (failure signaling) → Task 5. §3.5 (ack protocol) → Task 3 (printing) + Task 7 (parsing). §3.6 (invariants) → Task 7. §5 (testing: fallback-to-Insert, multi-batch downgrade, failure-signaling path) → Task 1's `resolve_target`/`resolve_slot_consumption` tests and Task 5's subprocess test. Global Constraint 6 (row-id lookup) → Task 2.
- **Placeholder scan:** no TBD/TODO; every step has complete code. Task 8's Step 4 is conditionally a no-op by design (validation-only task), not a placeholder.
- **Type consistency:** `ExecOutcome`, `OpVerb`, `Registry`'s method names, and `RunResult`'s field names are used identically across every task that references them (verified by construction — each later task's Interfaces block was copied from the exact signatures the producing task defines).
