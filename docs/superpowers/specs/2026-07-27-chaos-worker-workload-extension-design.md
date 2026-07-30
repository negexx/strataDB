# Chaos-Worker Workload Extension — Design

**Date:** 2026-07-27
**Status:** Approved for implementation planning

## 1. Goal

Close the gap the S1 spec's own closure note names explicitly (`.claude/docs/design/phase-s1-segmented-index-spec.md` §9): the Phase 7 chaos harness is insert-only, single-row-per-commit, and constructed so every commit succeeds cleanly. It never exercises a delete, an update, a genuine write-write conflict, a predicate-filtered `scan_with_predicate`/`vector_search` under zone-map pruning, or a multi-batch commit's zone-map merge — all under a real crash. This design extends `crates/chaos-worker` and `tests/sim/tests/chaos.rs` to cover all five, in one pass.

**Post-implementation correction (final whole-branch review, before merge):** as implemented, this closes 3 of the 5 gaps (delete, update, predicate-filtered search under pruning) rather than all 5. The scheduler that landed simulates `NUM_AGENTS` agents sequentially on one thread, not concurrently on separate threads — every commit's `base_version` equals `latest_version` at commit time, so `TxnError::Conflict` is structurally unreachable, and the "genuine write-write conflict" gap remains open (§3.2's retry/drop logic is real code but dead at runtime in this harness). The multi-batch zone-map-merge gap also remains open, for reasons detailed in §3.3's own post-implementation correction. Both are recorded here rather than silently treated as closed; see `.superpowers/sdd/progress.md`'s Task-8/final-review ledger entries for the full reasoning and what closing them for real would require (real concurrent agent threads, for the conflict gap).

## 2. Current state (baseline)

`crates/chaos-worker/src/main.rs`: a single binary, given `(dir, seed, num_agents, ops_per_agent)`. Each agent's full op sequence (a vector per op) is generated entirely up front from `(seed, agent_index)`, independent of scheduling. A single seeded scheduler RNG picks which not-yet-finished agent commits next, one op = one single-row insert per commit. The worker panics on any commit error, since fresh monotonic row-ids structurally cannot conflict under an insert-only workload. Each successful commit prints `agent {A} committed op {O} row_id {G}` to stdout and flushes.

`tests/sim/tests/chaos.rs`: builds the worker binary via `escargot`, spawns it with an optional `STRATA_CHAOS_ABORT_AT` (a checkpoint-count threshold at which `strata_storage::chaos::chaos_checkpoint()` calls `std::process::abort()`), parses stdout for acknowledged row-ids (assumes the last whitespace-separated token on every line is a row-id), and after reopening the dataset checks four invariants: no corruption, no lost commits, at most one tolerated phantom commit (crashed runs only — the single-in-flight-op ambiguous-outcome case), and row+index consistency for every visible row.

Both the fast tier (30 random seeds, part of default `cargo test --workspace`) and the thorough tier (2000 seeds, opt-in via `STRATA_CHAOS_THOROUGH=1`) currently pass clean against this insert-only workload.

## 3. Target design

### 3.1 Op model and scheduling

Add an op-verb enum agents draw from per op slot, pre-generated up front per agent (same seeding discipline as today) from a fixed weighted distribution:

```
Insert            40%
Delete            20%
Update            20%
MultiBatchInsert  20%
```

**Target resolution is just-in-time, not pre-generated.** System row-ids are assigned in commit order across the whole run, not knowable per-agent ahead of time — this was already implicitly true today (it just didn't matter for insert-only). The scheduler maintains a live registry, updated as each commit lands:

- `known_rows: HashMap<u64, RowState>` — every acknowledged row-id this run has ever committed, whichever agent, tagged live/tombstoned.
- `pool_rows: Vec<u64>` — the contested pool's row-ids (populated once, in the setup phase — see below).

Immediately before executing a Delete or Update op, the scheduler draws (from the *acting agent's own* pre-seeded RNG stream, consumed in a fixed order matching that agent's verb sequence, so replay of a given seed is still fully deterministic — the only source of "unpredictability" is which rows exist yet, which is itself schedule-derived and seed-deterministic, not truly random) a target:

- 50%: a uniformly random row-id from `pool_rows` (if non-empty)
- 50%: a uniformly random row-id from this agent's own prior successfully-committed inserts (if any)
- **Fallback, in order:** if the preferred source is empty, try the other; if both are empty, downgrade this op slot to Insert instead. This is a normal, expected, deterministic path — not an error.

**Contested pool setup.** Before the interleaved scheduler loop starts, the worker commits a fixed pool of 6 rows (`POOL_SIZE`, chosen at design time to be 2× the then-current `NUM_AGENTS = 3` so multiple agents were likely to collide on the same handful of ids — `NUM_AGENTS` was later raised to 8 by the real-concurrency work in `2026-07-30-chaos-worker-real-concurrency-and-zonemap-verification-design.md` without `POOL_SIZE` following it, so this ratio is stale; see that doc for why `POOL_SIZE` turned out not to be the lever real conflict frequency needed) via `POOL_SIZE` individual single-row inserts, using the same real commit path (and therefore subject to `chaos_checkpoint`/abort injection, same as everything else — a crash during setup is handled by the orchestrator's existing "nothing acknowledged yet, `NotFound` on reopen is fine" early-return, unchanged). Pool row-ids are recorded into `pool_rows` as each setup commit's acknowledgment prints.

**MultiBatchInsert** consumes 2 of the acting agent's op slots at once (folding two adjacent slots into a single transaction: two `Transaction::insert()` calls, one `commit()`), which is what actually exercises `merge_zone_map_stats` across batches within one commit. If only 1 op slot remains for that agent when this verb is drawn, downgrade to a regular single-row Insert for that last slot.

### 3.2 Conflict handling

On `TxnError::Conflict` (and *only* that variant — any other error remains a fatal, unexpected-bug panic, same as today), the agent re-reads latest state (a fresh `dataset.begin()`) and retries the *identical* logical op exactly once. If the retry also conflicts, the op is dropped: no third attempt, no further retry, bounded termination guaranteed regardless of contention level. A dropped op prints an informational (non-acknowledgment) line and moves on to the agent's next op slot.

The retry needs no special-casing for "the conflict winner already tombstoned my target": `Transaction::delete`'s write-set entry is keyed by row-id, and the retry's fresh `begin()` reads a version that already includes whatever the winner committed — so unless a *third* transaction also touches the same row-id in the brief retry window, the retry will not conflict again. Re-tombstoning an already-tombstoned row-id is a harmless idempotent set-insert at the storage layer (`Manifest.tombstones` is a `HashSet`), not a distinct error case to handle.

### 3.3 Live predicate-pruning reader

A reader thread spawns once at worker startup, running for the whole process lifetime concurrently with the main commit loop. Each iteration:

1. Take one `Snapshot` via `dataset.snapshot()`.
2. Run `vector_search(query, k, Some(&predicate))` where `predicate = Eq("name", "agent0")`, against *that same snapshot object*.
3. Also run an unfiltered `scan(&schema)` against the *same* snapshot object, and manually filter rows where `name == "agent0"` in the test's own code (bypassing zone-map pruning entirely — the ground-truth reference).
4. Assert the two agree: every row the pruned path finds must be in the reference set, and (for the row+index check already established in invariant 4) every reference row with a genuinely nearest vector must be findable through the pruned path too.
5. Sleep 1ms, repeat, until a shared `AtomicBool` (set by the main thread once every agent has finished, including the setup phase) signals stop; then join.

`Eq("name", "agent0")` is deliberately not a business-logic-arbitrary choice: because every commit is its own segment (S1's segmented-index design), and only some segments will ever contain `agent0`'s rows, this predicate gives the zone map a real opportunity to skip whole segments — proving pruning is *exercised*, not just present. A corrupted or partially-merged zone map (the MultiBatchInsert gap) would surface here as a silently wrong result, not as a separate invariant category.

This closes two of the five original gaps (predicate-pruning correctness, and multi-batch zone-map-merge correctness) through one mechanism, rather than needing two bespoke checks.

**Post-implementation correction (final whole-branch review, before merge):** step 4's second direction ("every reference row with a genuinely nearest vector must be findable through the pruned path too") was never implemented — only the pruned-subset-of-reference direction shipped. This matters: `vector_search`'s pruned path applies `live_set` as an *exact* per-row membership filter after pruning (`build_live_filter_from_live_set`), so `pruned ⊆ reference` holds **by construction**, independent of whether the zone map itself is correct. The realistic zone-map-merge bug is a merged range that's too *narrow*, wrongly pruning a segment **out** — producing *missing* rows, which only the unimplemented reverse direction could detect. Separately, `execute_multi_batch_insert` gives both rows in one commit the *same* `name` value, so even the reverse direction would exercise a degenerate merge (identical on the one column this predicate covers) — the two batches differ only in `id` and `vector`, neither of which this reader's predicate touches. Net effect: this mechanism verifies gaps 2 and 4 (delete/update and predicate-filtered search are genuinely exercised under pruning) but does **not** verify gap 5 (multi-batch zone-map-merge correctness) as originally claimed. Left as a known, documented gap rather than fixed in this pass — see the design's own §1 for the five-gap list this affects.

### 3.4 Failure signaling

Install one global panic hook (`std::panic::set_hook`) at the very start of `main()`, before spawning the reader thread or starting the scheduler loop. On any panic — from the main commit-loop thread or the reader thread — the hook prints `GENUINE_FAILURE: <panic message>` to stdout, flushes, and calls `std::process::exit(2)` immediately, before unwinding proceeds any further. This makes a genuine bug (an unexpected commit error, a reader-detected disagreement, anything else that would `panic!`) produce a distinct, reserved exit code — deliberately *not* relying on OS-specific signal decoding (`std::process::abort()`'s exact termination signature differs between the Linux CI environment and local Windows dev machines; exit code 2 does not).

`tests/sim/tests/chaos.rs`'s `run_worker` checks the child's exit code in this priority order:
1. **Exit code 2** → a genuine, unexpected failure. Fail the test immediately with the printed `GENUINE_FAILURE` message — do **not** proceed to `check_invariants` at all, and do **not** count this as a tolerated crash.
2. **Success (0)** → clean run, existing behavior.
3. **Any other non-zero exit** → the existing "expected chaos-abort" path, unchanged: proceed to `check_invariants` with `crashed = true`.

This is a real, if narrow, gap-closer on its own: today a genuine worker-panic (e.g. a real, non-conflict commit error) is silently swallowed into the same bucket as an expected crash, and invariant-checking might well still pass even though something real broke. This design fixes that as a side effect of adding the reader thread's own failure-reporting need.

### 3.5 Acknowledgment protocol

Replace the current "last token is a row-id" parsing with an explicit, per-op-type line format:

```
agent {A} committed insert op {O} row_id {G}
agent {A} committed delete op {O} target_row_id {T}
agent {A} committed update op {O} target_row_id {T} row_id {G}
agent {A} committed multibatch op {O} row_ids {G1},{G2}
agent {A} dropped op {O} (conflict)
pool committed insert row_id {G}
```

The pool-setup line has no `agent`/`op` fields (it's printed once per pool row before the interleaved phase, not from within the scheduler loop) but is otherwise an ordinary insert acknowledgment and is added to `acknowledged_inserts` the same way.

The orchestrator's stdout parser is rewritten to a small line-oriented match on the third word (`insert`/`delete`/`update`/`multibatch`) rather than a blind last-token split, producing two sets instead of one:

- `acknowledged_inserts: HashSet<u64>` — every row-id that ever became durably visible via an insert or an update's insert half.
- `acknowledged_tombstones: HashSet<u64>` — every row-id durably tombstoned via a delete or an update's delete half.

`dropped` lines are informational only (useful for debugging a failing seed), never added to either set.

### 3.6 New and revised invariants

- **Invariant 2 (no lost commits)** — unchanged in spirit, now checked against `acknowledged_inserts`.
- **Invariant 3 (no phantom commits)** — unchanged in spirit, now checked against `acknowledged_inserts`.
- **New: no resurrected tombstones.** Every row-id in `acknowledged_tombstones` must **not** appear in the reopened dataset's visible row-ids, unconditionally (no crash-tolerance carve-out needed here — unlike an ambiguous *insert* outcome, there is no legitimate scenario where a durably-tombstoned row should still be visible; `Snapshot::is_visible` is a pure `!tombstones.contains` check with no timing window).
- **Invariant 4 (row+index consistency)** — unchanged, still iterates every currently-visible row-id.
- **Reader-thread predicate agreement** — not a post-hoc `check_invariants` category at all; it's a live, in-band assertion inside the worker process itself, surfaced via §3.4's failure-signaling path.

## 4. Non-goals for this pass

- **No cross-process concurrent reader.** The reader is a thread inside the same worker process, sharing one `Dataset` handle. A genuinely separate reader *process* racing the writer process is a strictly harder variant (needs its own lifecycle/IPC-result protocol) and is not what the S1 closure note's gap list asks for — it's a plausible future follow-up, not in scope here.
- **No new loom model.** The concurrent-segment-publication loom model is a separate queued Phase 6/7 hardening item, deliberately not folded into this one.
- **No CI wiring.** Making the thorough tier (and loom) part of a scheduled CI job is the third queued Phase 6/7 item, also deliberately separate — this design only makes the workload itself richer.
- **`NUM_AGENTS`/`OPS_PER_AGENT` tuning** is left to implementation-time empirical validation (enough ops per agent that delete/update/multibatch verbs actually get meaningfully exercised across the fast tier's 30 seeds) rather than fixed here.

## 5. Testing

- Existing fast-tier (30 seeds) and thorough-tier (2000 seeds) structure is unchanged; both now exercise the full op mix by construction, no new test functions needed at the `tests/sim` level.
- A handful of new, non-chaos, non-crash unit/integration tests are still warranted directly against the worker's op-generation logic (e.g., "a Delete op with an empty pool and no own rows deterministically downgrades to Insert," "MultiBatchInsert on an agent's last remaining op slot downgrades to single Insert") — deterministic, fast, no process spawn needed, and they pin exactly the fallback behavior §3.1 specifies rather than leaving it implicitly exercised only by luck of which seeds happen to hit it.
- The failure-signaling path (§3.4) needs its own direct test: inject a synthetic panic (e.g. a test-only env var forcing the reader thread to assert something false) and confirm the orchestrator's exit-code-2 branch fires and reports the message, without running the full chaos tiers.

## 6. Risks / open questions carried into implementation

- Exact weighted-distribution percentages and pool size are starting defaults (§3.1), not load-bearing constants — expect them to be tuned once real seed runs show whether delete/update/conflict actually occur often enough to be useful (a distribution that's too insert-heavy would silently under-exercise the very thing this design exists to add).
- The reader thread's 1ms poll interval is a starting guess balancing "catch a bad state promptly" against "don't dominate wall-clock cost of the thorough tier's 2000-seed run" — worth measuring against the existing ~6.4-minute thorough-tier baseline once implemented.
