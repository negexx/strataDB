# Phase 7 — Correctness Harness / Chaos Testing — Design

**Date:** 2026-07-22
**Status:** Approved for implementation planning

## 1. Goal and scope

Phase 7 is the proof phase for everything Phases 0-6 built. Per
`.claude/docs/architecture.md`'s roadmap: "deterministic simulation testing,
randomized fault injection," exit criterion "thousands of randomized
concurrent-agent runs, zero invariant violations." Concretely, this closes
the one gap nothing built so far covers: **concurrent multi-agent writes
combined with a crash at an arbitrary point** — Phase 1 proved single-writer
crash-recovery (its own MVP checklist's `kill -9` step), Phase 6 proved
concurrent-write correctness under real interleavings (`loom`), but nothing
has ever tested both at once. That combination is exactly what the flagship
claim ("correct under concurrent multi-agent writes, with no silent
buffering") stands or falls on.

**Scope note resolved during brainstorming:** architecture.md's "v0.3
concurrent multi-agent write slice" milestone description (end of Phase 6's
roadmap row) mentions the scenario "re-runs under randomized process kills"
— this reads as describing the *complete* proof of that milestone, delivered
here in Phase 7, not as an unmet Phase 6 requirement. Phase 6 built the
mechanism (OCC, atomic commits, conflict detection); Phase 7 builds the
proof that it holds under adversarial scheduling and crashes. Phase 6's own
design doc already deferred crash-consistency testing on exactly this
reasoning.

## 2. A load-bearing finding: madsim/turmoil don't fit this codebase

CLAUDE.md's stack section names `madsim`/`turmoil` for this phase. Actually
consulting both (not just citing them, per this project's Design Principle
#7) found a real mismatch: both are fundamentally async/tokio-shaped —
madsim requires replacing tokio, tonic, etc. with its own simulated runtime;
turmoil-fs is explicitly "a drop-in replacement for `tokio::fs`." Both
target genuinely distributed, networked systems (TiKV, RisingWave, Tokio's
own ecosystem). strataDB's production code — everything built through
Phase 6 — is entirely synchronous (`std::thread`, `std::sync::Mutex`,
blocking `std::fs`), with no async runtime anywhere. Neither tool can
transparently intercept that as-is, and rewriting the already-reviewed
storage/txn layers onto an async runtime purely to satisfy a testing
dependency would be a wildly disproportionate change unrelated to this
phase's actual goal.

This design instead follows Jepsen's own methodology more directly than
FoundationDB's DST: real processes, real faults (`std::process::abort()`,
not a simulated one), reproducibility from a seed rather than from replaying
identical async-runtime scheduling. This is a deliberate, informed departure
from CLAUDE.md's stack line, not an oversight — CLAUDE.md should be updated
to reflect it once this design is implemented.

## 3. Components

- **`crates/storage/src/chaos.rs`** (new) — a global `AtomicU64` checkpoint
  counter and a `chaos_checkpoint()` call inserted at each durability
  boundary already defined by the commit protocol
  (`.claude/docs/design/phase-0-transaction-and-format-spec.md` §3): inside
  `write_batch` (data-file content fsync), after `sync_dir` (new data-file
  directory-entry fsync), inside `commit_manifest` before and after the
  tmp-file `sync_all`, and immediately after `commit_manifest`'s final
  `rename`. Each call increments the counter and, only when a
  `STRATA_CHAOS_ABORT_AT` env var is set, compares against it and calls
  `std::process::abort()` on match. Gated behind a new, off-by-default
  Cargo feature (`chaos-injection`) — zero cost and zero surface for the
  real `strata` binary and every other consumer.
- **`crates/chaos-worker/`** (new workspace member, `publish = false`) — a
  small binary crate depending on `strata-txn`/`strata-storage` with
  `chaos-injection` enabled. Takes a seed, agent count, and op count;
  deterministically interleaves simulated agents (§4) against a real
  `Dataset` in a real process, printing and flushing an acknowledgment after
  every successful commit, until either every agent finishes or
  `chaos_checkpoint()` aborts the process.
- **The orchestrator** (`tests/sim/`, matching architecture.md's already-
  reserved location) — a normal Rust integration test, linking `strata-txn`
  directly with `chaos-injection` *not* enabled (verification is a plain
  read path — the feature is only needed by the worker being crashed). For
  each seed: picks a random abort threshold, spawns `chaos-worker` as a real
  child process with that seed/threshold, captures its stdout and waits for
  it to either finish or die, then calls `Dataset::open` itself (no third
  process needed — a fresh `open` in the orchestrator's own process
  exercises the identical crash-recovery path a fresh process would) and
  runs the invariant checks (§6).

## 4. Deterministic interleaving mechanism

Each simulated agent is a small state machine: a sequence of operations
(insert/update/delete) generated deterministically from the seed *before*
any execution starts, so what each agent wants to do is fixed independent of
scheduling. The worker then drives them step by step: at each step, a seeded
`Rng` picks which not-yet-finished agent goes next, and that agent executes
exactly one operation — buffering a write on its own in-flight
`Transaction`, or committing it. This produces real interleaving of
concurrent transactions and real conflict-detection exercise, entirely
deterministically: same seed → same turn order → same interleaving, every
run, forever. Concurrency width (how many agents are in flight at once) and
conflict density (how much their target rows overlap) are also seed-derived,
so one seed fully reproduces an entire run's shape.

This deliberately does not re-test what `loom` already proved (real-hardware
memory-ordering safety under genuine multi-core races) — that remains Phase
6's job. This harness's job is logical correctness under interleaving
combined with crash-consistency, which single-threaded deterministic
interleaving exercises cleanly. Real-multi-thread-plus-crash is a legitimate
future extension, not required for this first slice.

## 5. Orchestration, scale, and tiering

Each iteration costs at least one real process spawn (the worker) plus real
fsyncs — genuinely slow relative to in-memory tests, the same lesson this
project already hit once this session (a `CommitLog` capacity bump
ballooned one test from ~6s to ~294s before being fixed). Applying that
lesson proactively: two tiers.

- **Fast tier** (part of default `cargo test --workspace`): a small, fixed
  set of seeds (~20-50), small agent/op counts — a regression net, low tens
  of seconds total.
- **Thorough tier** (opt-in — env-var-gated or `#[ignore]`, run via CI on a
  schedule or on demand): the actual "thousands of randomized runs" the
  exit criterion asks for, larger agent/op counts, run separately so it
  never taxes the normal dev loop.

Only the thorough tier's clean run — thousands of seeds, zero invariant
violations — satisfies the Phase 7 exit criterion. The fast tier exists so
regressions get caught immediately, not to prove the milestone by itself.

## 6. Invariant checking

After every reopen, four checks run:

1. **No corruption.** `Dataset::open(dir)` must succeed without error.
   `commit_manifest`'s rename-based protocol already guarantees
   `read_current` only ever observes a fully-written manifest — a crash
   mid-tmp-write can't corrupt the "current" pointer, since rename only
   happens after the tmp file is fully synced — so a clean open is itself
   the proof; there is no corruption path a crash could produce here.
2. **No lost commits.** Every row acknowledged (printed and flushed by the
   worker immediately after a successful `commit()`, mirroring the existing
   `crash-loop` pattern) must be present and visible after reopen.
3. **No phantom commits.** The mirror check: every row visible after reopen
   must trace back to something in the acknowledgment log. Anything visible
   that was never acknowledged is a violation.
4. **Row + index consistency.** After `replay_index` rebuilds the graph from
   delta logs on reopen, the set of row-ids present in the `HnswIndex` graph
   must exactly match the set of visible (non-tombstoned) rows in the
   manifest. Likely needs one small new test-only introspection method on
   `HnswIndex` (list currently-indexed row-ids) if one doesn't already
   exist — a small, expected implementation detail, not a design blocker.

All four are checked on every single iteration in both tiers — they're
complementary facets of one correctness claim, not alternatives to choose
among.

## 7. Non-goals (cut list for this slice)

| Cut | Why |
|---|---|
| Real multi-thread agents (true OS-thread concurrency) combined with crash injection | Would reintroduce scheduling nondeterminism, defeating seed reproducibility; `loom` already covers real-hardware race safety separately. Legitimate future extension once the deterministic version proves itself. |
| Cross-process concurrent writers | Phase 6's design doc already scoped this out as a documented non-goal (in-process concurrency only); building it here would silently reopen that decision. |
| Rewriting production code onto an async runtime to use `madsim`/`turmoil` as originally named in CLAUDE.md | Wildly disproportionate to this phase's actual goal; see §2. |
| Network partition / multi-node fault injection | Strata is single-node by design (architecture.md Non-Goals); nothing to partition. |

## 8. References

- `.claude/docs/architecture.md` — Phase 7 roadmap entry and the "v0.3"
  milestone's process-kill requirement this design fulfills.
- `.claude/docs/design/phase-0-transaction-and-format-spec.md` §3 — the
  exact commit-protocol steps `chaos_checkpoint()`'s placement is derived
  from.
- `crates/cli/src/main.rs`'s existing `crash-loop` command and
  `crates/cli/tests/mvp_checklist_6_crash_recovery.rs` — the proven,
  already-tested single-writer pattern this design extends to multi-agent.
- Prior art consulted during design: madsim
  (<https://github.com/madsim-rs/madsim>) and turmoil
  (<https://github.com/tokio-rs/turmoil>) — both confirmed async/tokio-shaped
  on inspection, not a fit for this codebase's synchronous production code
  (see §2). Jepsen's methodology (real processes, real faults,
  seed-reproducible scenarios rather than simulated-runtime replay) is the
  actual model this design follows, per this project's own reading list.
