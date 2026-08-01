# Phase 1 verification and documentation audit

**Date:** 2026-08-01

**Lane:** Sol — unit/property/loom/chaos/fuzz/benchmark evidence, CI gates,
opt-in suites, reproducibility, and active-document consistency

**Scope:** Current working tree and its existing dirty changes. The baseline was already heavily
dirty, including `.github/workflows/ci.yml`, benchmark sources, transaction/index/storage/query Rust,
tests, `AGENTS.md`, and active documentation; the branch was `main...origin/main [ahead 2]`. This lane
changed no source, tests, dependencies, configuration, or pre-existing documentation. Its only write
is this report.

**Method:** Static review of test sources, Cargo metadata/manifests, the sole CI workflow, benchmark
and fuzz harnesses, active navigation documents, accepted decisions, and neighboring Phase 1 lane
evidence. Lightweight read-only commands included `git status --short --branch`, `rg` inventories,
an active-document relative-link scan, and `cargo metadata --no-deps --format-version 1`. No unit,
property, loom, chaos, fuzz, or benchmark suite was run; in particular, this lane did not rerun the
long transaction loom module or 2,000-seed chaos tier.

## Verdict

**BLOCKED — Phase 1 does not have a CI-enforced, reproducible verification story, and its active
narratives overstate guarantees that the audit has disproved.**

The ordinary suite is broad, the two property tests are thoughtfully constructed, index loom is a
real CI gate, and the default chaos test exercises 30 real-process crash scenarios. The current test
sources also contain strong targeted coverage for immutable snapshots, row/index publication,
segment parsing, conflict-log behavior, and recovery from several corrupt states.

Four lane-level blockers remain:

1. Known Phase 1 counterexamples have no direct regression tests and coexist with green ordinary
   package suites (VER-01).
2. Seven transaction/cache loom models are absent from CI; only the eight index models are gated
   (VER-02).
3. The 2,000-seed chaos exit tier and the dedicated checkpoint test are passing no-ops in ordinary
   CI, with no scheduled/on-demand workflow that actually enables them (VER-03).
4. The active architecture/how-it-works text states durability, visibility, and row-ID non-reuse as
   achieved behavior while the status ledger says Partial and the current Phase 1 audits contain
   direct counterexamples (VER-07).

Fuzz reachability, CI-tool pinning, chaos replayability, and benchmark data/provenance are additional
evidence gaps. These do not demand compaction, cross-process transactions, serializability, or another
ANN family; they are inside the current shared-handle Phase 1 verification/documentation boundary.

## Evidence inventory

### Source-level test surface

Static `#[test]`-attribute counts in the current tree are shown below. These are an inventory, not a
claim that default CI executes every body: the counts include `cfg(loom)` tests and environment-gated
tests that return early.

| Area | `#[test]` attributes | Important qualification |
|---|---:|---|
| `crates/storage` | 61 | Includes the environment-gated checkpoint integration test. |
| `crates/txn` | 147 | Includes six dataset loom tests and one live-set-cache loom test. |
| `crates/index` | 158 | Includes eight loom tests and one ignored real-thread stress test. |
| `crates/query` | 60 | Includes one property test. |
| `crates/cli` | 7 | Includes the crash-recovery integration test. |
| `crates/chaos-worker` | 44 | Mostly worker/parser/operation unit tests; the real-process tier lives in `strata-sim`. |
| `tests/sim` | 4 | Includes the 30-seed default tier and self-skipping 2,000-seed tier. |
| `crates/bindings` | 0 | Consistent with the current placeholder-only status, but not Phase 2 API evidence. |

Normal integration coverage includes four concurrent snapshot tests
(`crates/txn/tests/concurrent_snapshot_isolation.rs:22-482`), MVP transaction/CLI checks
(`crates/txn/tests/mvp_checklist_1_to_5.rs`,
`crates/cli/tests/mvp_checklist_6_crash_recovery.rs`), and pruning integration tests
(`crates/txn/tests/phase_3_pruning.rs`). Neighboring audit lanes freshly ran the relevant normal
packages and found them green while still deriving correctness/durability counterexamples
(`docs/audits/phase-1/correctness.md:112-121`). Green ordinary tests therefore demonstrate covered
cases, not Phase 1 closure.

### Property tests

- `CommitLog::conflicts_with` is compared with an independent naive reference using 2,000 cases. Its
  generator deliberately samples exact version boundaries and both sides of the linear/hash lookup
  threshold (`crates/txn/src/commit_log.rs:269-394`). This is strong, mutation-informed property
  coverage.
- Predicate masks are compared row-by-row with a naive scalar reference; the threshold generator is
  correlated with actual values so equality's true branch is exercised
  (`crates/query/src/predicate.rs:767-814`). It uses proptest's default case count.
- Property coverage is otherwise narrow: row allocation/persistence, manifest relationships, segment
  decoding, update/delete contracts, and row/index transaction state are covered by examples or fuzz
  targets, not properties.

### Loom

The current source contains 15 loom tests:

- Eight index models: five in `graph.rs`, plus publication/allocation models in `node.rs`,
  `node_table.rs`, and `slot_array.rs` (`crates/index/src/graph.rs:2682-3190`,
  `crates/index/src/node.rs:343-384`, `crates/index/src/node_table.rs:518-545`,
  `crates/index/src/slot_array.rs:212-255`). The CI step compiles the crate with scoped `--cfg loom`,
  extracts the produced test binary, and executes it single-threaded
  (`.github/workflows/ci.yml:37-56`).
- Six dataset models cover snapshot-cell publication, failed/successful row-index visibility,
  first-vector dimension races, and in-flight allocation windows
  (`crates/txn/src/dataset.rs:7058-8170`). One live-set-cache model covers concurrent same-key misses
  (`crates/txn/src/live_set_cache.rs:434-481`). These require the same crate-scoped build pattern
  (`crates/txn/src/dataset.rs:6969-6988`) but are not built or executed by CI.

The transaction source explicitly says whole-module runs can exceed ten minutes and exhaust Windows
resources, recommending exact-model invocations (`crates/txn/src/dataset.rs:7033-7047`). That is a
reason to shard/budget the gate, not to omit it.

### Chaos and ignored/opt-in suites

| Surface | Default behavior | Opt-in/reproduction interface | CI disposition |
|---|---|---|---|
| 30-seed process-crash tier | Runs as an ordinary `strata-sim` test; fixed master RNG seed chooses checkpoint thresholds (`tests/sim/tests/chaos.rs:562-615`). | Individual worker content is seed-derived. | Reached by `cargo test --workspace`. |
| 2,000-seed thorough tier | Test returns `Ok` immediately when `STRATA_CHAOS_THOROUGH` is absent (`tests/sim/tests/chaos.rs:617-630`). | `STRATA_CHAOS_NUM_SEEDS`, `STRATA_CHAOS_ONLY_SEED`, and `STRATA_CHAOS_CONCURRENCY` tune runs (`:632-669`). | Not enabled; no second workflow exists. |
| Storage checkpoint abort test | Test returns `Ok` when `STRATA_CHAOS_TEST_HELPER_BUILT` is absent (`crates/storage/tests/chaos_checkpoint_actually_aborts.rs:11-24`). | Also needs `--features chaos-injection`; the source's printed command omits the required environment variable (`:18-22`). | Appears as passed/no-op in ordinary CI. |
| Empty-graph real-thread stress | `#[ignore]`, 8,000 trials, documented at about 40 seconds (`crates/index/src/graph.rs:2185-2196`). | Run with libtest `--ignored`. | Intentionally not run; the corresponding loom model is gated. |
| `parallel-insert` | Compile-time feature is off by default (`crates/txn/Cargo.toml:40-54`). | `cargo test/clippy -p strata-txn --features parallel-insert`. | Both test and clippy are explicit CI steps (`.github/workflows/ci.yml:29-35`, `:61-62`). |

The chaos worker is honest about its reproducibility limit: operation content is seed-derived, but OS
thread interleaving is not reproducible from the seed alone
(`crates/chaos-worker/src/main.rs:1-14`). The thorough harness pre-draws `(seed, abort_at)` pairs so
chunking/concurrency does not change those inputs (`tests/sim/tests/chaos.rs:661-669`), but a race
failure still needs retained stdout, platform/filesystem metadata, and repeated execution; rerunning
one seed at concurrency 1 changes the schedule.

### Fuzzing

- `fuzz/` is a separate Cargo workspace (`fuzz/Cargo.toml:1-46`), so root build/test/clippy/deny steps
  do not cover it. CI contains no `cargo fuzz build` or bounded `cargo fuzz run` step.
- `manifest_parse` feeds bytes directly to `serde_json::from_slice::<Manifest>`
  (`fuzz/fuzz_targets/manifest_parse.rs:5-13`). It checks deserialization panic-safety, not manifest
  filename selection, filename/payload version agreement, safe path relationships, or full
  `read_current`/`Dataset::open` recovery.
- `datafile_parse` exercises the real `strata_storage::read_batch` path through temporary files
  (`fuzz/fuzz_targets/datafile_parse.rs:7-37`) and has two checked-in valid Arrow IPC seeds. There is no
  checked-in manifest corpus and no segment-format/`SegmentReader::from_bytes` fuzz target.
- Reproducibility strengths: `fuzz/Cargo.lock` is retained separately, and Arrow/Arrow IPC are pinned
  exactly to 58.3.0 to match the main workspace (`fuzz/Cargo.toml:13-27`).

### Benchmarks

`bench/Cargo.toml:23-57` registers nine benchmark binaries. Five use the ignored local file
`bench/data/dbpedia-openai-100k.parquet`: `vector_search_bench`, `lockfree_vs_hnsw_rs_bench`,
`lifecycle_bench`, `segment_recall_bench`, and `ef_construction_sweep_bench` (for example,
`bench/benches/vector_search_bench.rs:23-55`, `bench/benches/lifecycle_bench.rs:103-125`). The file is
excluded by `.gitignore:39`; active navigation docs provide no download URL, immutable dataset
revision, size/hash, or preparation command. The only inspected retrieval recipe is in intentional
history (`docs/history/design/phase-4-implementation-plan.md:1497-1509`), which `docs/README.md:12-14`
correctly says is non-authoritative.

Several harnesses have useful correctness gates: vector search checks recall against brute force
before reporting QPS (`bench/benches/vector_search_bench.rs:118-198`), group-by checks independent
references, and segment recall uses exact ground truth. `manifest_growth_bench` is a deterministic
sequential growth probe with an environment-controlled commit count
(`bench/benches/manifest_growth_bench.rs:43-46`, `:74-160`). However, no benchmark runs in CI, current
results are not retained with revision/environment provenance, and the production segment-count and
recovery matrix required by Phase 1 is absent; see the dedicated performance lane for the full
disposition (`docs/audits/phase-1/performance.md:34-64`).

### CI gates and reproducibility

The sole workflow is `.github/workflows/ci.yml`. It runs, in order: workspace build/test, transaction
tests with `parallel-insert`, index loom, workspace and feature clippy with warnings denied, format,
docs, and `cargo deny` bans/sources/advisories (`.github/workflows/ci.yml:11-74`). That is a solid
ordinary baseline.

The Rust compiler and components are pinned to 1.90 both locally and in CI (`rust-toolchain.toml:1-3`,
`.github/workflows/ci.yml:17-21`), and both Cargo workspaces retain lockfiles. The runner and tooling
are not fully reproducible: `ubuntu-latest`, `actions/checkout@v4`,
`dtolnay/rust-toolchain@master`, and an unversioned `taiki-e/install-action@cargo-deny` can change
without a repository diff (`.github/workflows/ci.yml:13-20`, `:70-74`). Advisory freshness is
intentionally time-varying, but the runner/action/cargo-deny versions need not be.

## Findings

### VER-01 — Known Phase 1 counterexamples have no direct regression gates

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 0 row-identity invariant and Phase 1 correctness/durability exit
- **Disposition:** **Phase 1 blocker.** Land fixes with direct normal tests; add loom only where the
  counterexample depends on an interleaving.
- **Evidence:**
  - Current delete/update tests seed a real row and use one-row replacements
    (`crates/txn/src/dataset.rs:5591-5605`, `:5663-5680`); there is no direct test for deleting a
    future/nonexistent row, stale-delete versus insert, or zero/many-row update replacement.
  - Failed-manifest tests assert visibility/orphan state across reopen but do not commit after reopen
    and prove the abandoned row ID remains consumed (`crates/txn/src/dataset.rs:2240-2309`,
    `:6708-6765`).
  - Manifest recovery tests cover malformed JSON, temp files, and non-numeric names, not a numeric
    filename whose payload has a different version (`crates/storage/src/manifest.rs:397-416`,
    `:472-508`).
  - `sync_dir` cannot currently be fault-tested: it discards directory-open/sync errors and always
    returns `Ok(())` (`crates/storage/src/datafile.rs:75-83`).
  - The correctness lane derives all four counterexamples while freshly run normal packages remain
    green (`docs/audits/phase-1/correctness.md:22-101`, `:112-121`).

Required regression set: future/nonexistent tombstone followed by insert (scan and ANN), stale delete
versus first insert with typed conflict behavior, failed publication → drop/reopen → insert with strict
row-ID non-reuse, filename/payload manifest-version mismatch, update cardinality contract, and
directory-sync failure acknowledgement/recovery. Tests must encode the accepted contracts rather than
silently choosing missing-row/update semantics.

### VER-02 — Transaction and live-set-cache loom models are not CI gates

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 shared-handle concurrency verification
- **Disposition:** **Phase 1 blocker.** Add a crate-scoped transaction loom job, sharded by exact test
  with explicit time/resource budgets; add the missing future-tombstone interleaving model with the
  corresponding fix.
- **Evidence:**
  - Transaction/cache loom has seven tests under `cfg(loom)`
    (`crates/txn/src/dataset.rs:7058-8170`, `crates/txn/src/live_set_cache.rs:434-481`). Ordinary
    `cargo test --workspace` does not compile that configuration.
  - CI's only loom build names `strata-index` and runs only the resulting index test binary
    (`.github/workflows/ci.yml:37-56`).
  - The source documents why whole-module execution is too expensive and how to invoke exact models
    (`crates/txn/src/dataset.rs:7033-7047`). The concurrency lane's fresh representative models passed,
    while the expensive bounded model exceeded 300 seconds and remained inconclusive
    (`docs/audits/phase-1/concurrency.md:125-145`).

### VER-03 — Chaos exit suites can report success without exercising their assertions

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 1 crash/recovery verification
- **Disposition:** **Phase 1 blocker for explicit execution evidence.** Keep the 30-seed PR tier;
  create a scheduled/on-demand 2,000-seed job with artifact retention and make skips machine-visible.
  Gate the checkpoint integration test structurally on the Cargo feature or wire the required
  environment explicitly; do not return success from a test named as though it ran.
- **Evidence:**
  - The roadmap makes loom/chaos/test evidence part of Phase 1 and requires direct verification for
    asserted guarantees (`docs/roadmap.md`, `## Phase 1 — Correctness and durability baseline`).
  - The thorough test returns before any seed when `STRATA_CHAOS_THOROUGH` is absent
    (`tests/sim/tests/chaos.rs:617-630`); CI never sets it and there is no other workflow.
  - The dedicated checkpoint test similarly returns when `STRATA_CHAOS_TEST_HELPER_BUILT` is absent
    (`crates/storage/tests/chaos_checkpoint_actually_aborts.rs:11-24`). Its own skip message recommends
    a command that still omits that variable (`:18-22`).
  - Neither is `#[ignore]`, so libtest reports an ordinary passing test rather than an explicit ignored
    or externally classified skipped suite.
  - Chaos covers process abort on a live filesystem, not power loss or failed sync/rename operations
    (`docs/audits/phase-1/durability.md:176-215`). The new job must not be described as proving those
    distinct durability cases.

For reproducibility, retain at least the revision/dirty state, OS/filesystem, seed, abort threshold,
concurrency, complete worker stdout/stderr, and failure artifacts. The seed reproduces workload
content, not an OS schedule (`crates/chaos-worker/src/main.rs:7-14`).

### VER-04 — Fuzz targets are neither build-gated nor aligned to all Phase 1 parsers

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 corrupt-state/recovery evidence
- **Disposition:** Phase 1 evidence gap. Add a bounded separate fuzz job that at minimum builds both
  targets from `fuzz/Cargo.lock`; add seeded manifest and immutable-segment targets before claiming
  parser hardening. Long open-ended fuzz campaigns may remain scheduled/manual.
- **Evidence:**
  - `fuzz/` is excluded from the root workspace by its own `[workspace]` and CI never enters it
    (`fuzz/Cargo.toml:1-46`, `.github/workflows/ci.yml:23-74`). Main-workspace source/API changes can
    therefore break fuzz compilation without failing a PR.
  - Only manifest JSON deserialization and Arrow data-file parsing are targeted
    (`fuzz/Cargo.toml:32-44`); the load-bearing immutable segment decoder has no target.
  - The manifest target bypasses filename/version selection and full recovery relationships
    (`fuzz/fuzz_targets/manifest_parse.rs:5-13`).
  - The only checked-in corpus is two valid Arrow files under `fuzz/corpus/datafile_parse/`; there is
    no manifest or segment seed corpus.

### VER-05 — CI gate provenance is mutable despite a pinned Rust/Cargo baseline

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 verification reproducibility; ongoing project-wide CI
- **Disposition:** CI hardening before treating a historical green run as reproducible. Pin the runner
  image/version policy, action revisions, and cargo-deny version; record the resolved versions in job
  output. Keep advisory data fresh and timestamped rather than pretending it is immutable.
- **Evidence:**
  - Rust is pinned to 1.90 and both root/fuzz lockfiles are retained
    (`rust-toolchain.toml:1-3`, `Cargo.lock`, `fuzz/Cargo.lock`).
  - The workflow uses mutable runner/action/tool references: `ubuntu-latest`,
    `actions/checkout@v4`, `dtolnay/rust-toolchain@master`, and unversioned cargo-deny installation
    (`.github/workflows/ci.yml:13-20`, `:70-74`).
  - Thus an identical commit can be compiled or linted by different surrounding tooling without any
    repository change. This is especially relevant for the exact loom-binary extraction recipe and
    policy/lint gates.

### VER-06 — Benchmark inputs/results are not portable or attributable Phase 1 evidence

- **Severity:** Medium
- **Confidence:** High
- **Affected phase:** Phase 1 growth/recall/performance evidence
- **Disposition:** **Phase 1 evidence blocker where roadmap exit depends on measurement;** otherwise
  benchmark-harness documentation correction. Add an active data-preparation document with immutable
  source revision, byte size, and checksum. Retain a bounded result matrix with revision/dirty state,
  command/environment overrides, hardware, OS/filesystem, and warm/cold-cache conditions.
- **Evidence:**
  - Five benchmark binaries require an ignored 347 MB local Parquet file (`.gitignore:39` and the
    `DATASET_PATH` constants cited in the inventory above). The active docs contain no authoritative
    acquisition/checksum recipe.
  - No benchmark runs in CI (`.github/workflows/ci.yml:23-74`). Existing `target/criterion` output is
    generated, untracked, and not attributable to this dirty tree.
  - The performance lane found no current production-path commit/segment/recovery matrix and classified
    the missing retained evidence as a Phase 1 blocker (`docs/audits/phase-1/performance.md:34-64`).
  - Some harnesses are deliberately one-shot custom binaries rather than Criterion
    (`bench/benches/lifecycle_bench.rs:1-20`, `bench/benches/manifest_growth_bench.rs:1-20`), making
    explicit environment/provenance capture more important.

### VER-07 — Active docs state unverified or currently false guarantees as achieved behavior

- **Severity:** High
- **Confidence:** High
- **Affected phase:** Phase 0 foundation claims and Phase 1 documentation exit
- **Disposition:** **Phase 1 documentation blocker.** Preserve `AGENTS.md`'s invariants as normative.
  Until fixes and regression gates land, qualify active behavior narratives as intended contracts with
  known blockers and keep the capability statuses Partial. Reconsider the roadmap's Phase 0
  `Implemented` label or explicitly separate a designed foundation from verified row-identity behavior.
- **Evidence:**
  - The audit baseline contained architecture/how-it-works wording that presented acknowledged writes,
    joint durability/visibility, and row-ID non-reuse as current behavior. Those active narratives are
    now explicitly qualified as intended/Partial and blocked; `AGENTS.md` still states the non-negotiable
    target invariants, now explicitly labelled as targets rather than achieved behavior.
  - The status ledger and architecture table correctly mark shared-handle publication, transactions,
    update/delete semantics, chaos, and durability Partial (`docs/status.md`, `## Capability ledger`,
    `docs/architecture.md`, `## Implemented, partial, and absent`). The active lead/narrative sentences
    have since been reconciled to that status.
  - Current audit evidence shows acknowledged-write invisibility/conflict bypass, restart reuse of an
    abandoned row ID, and swallowed directory-sync failures
    (`docs/audits/phase-1/correctness.md:22-73`). These are inside the supported shared-handle/local
    boundary, not unsupported cross-process requests.
  - The roadmap marks Phase 0 Partial and says its row-identity/transaction invariants are
    represented in source/tests while durable restart non-reuse remains blocked (`docs/roadmap.md`,
    `## Phase 0 — Foundation`), while its controlling specification
    calls itself a partially superseded historical baseline and remains `Status: Draft`
    (`docs/design/phase-0-transaction-and-format-spec.md:3-9`). The demonstrated restart non-reuse
    counterexample is itself a Phase 0 invariant failure.
  - The status ledger inventories only transaction dataset loom, omitting the live-set-cache and all
    index models and not stating that transaction loom is absent from CI. Its durability entry points to
    `manifest.rs` for manifest fsync but the current write delegates to `LocalFs::put`, whose directory
    step uses best-effort `datafile::sync_dir` (`crates/storage/src/manifest.rs:202-212`,
    `crates/storage/src/backend/local.rs:185-216`, `crates/storage/src/datafile.rs:75-83`). These are
    smaller evidence-map corrections once the guarantee wording is fixed.

Intentional history was excluded from this finding. Dated S1 closure notes, pre-S1 comparisons,
historical phase labels mapped by the `## Legacy phase map` section of `docs/status.md`, and documents under `docs/history/` were not
treated as current claims merely because they describe retired mechanisms.

## Strengths

- The ordinary test surface is large and subsystem-focused. Immutable snapshot behavior has direct
  multithreaded integration coverage, while row and vector visibility are checked from the same
  snapshot (`crates/txn/tests/concurrent_snapshot_isolation.rs:22-482`).
- The commit-log property test is unusually disciplined: independent reference, boundary-correlated
  generation, explicit coverage of both algorithm branches, shrinking, and a justified 2,000-case
  budget (`crates/txn/src/commit_log.rs:269-394`).
- Index loom is a genuine CI gate rather than a compile-only gesture. The scoped build avoids the
  known workspace-wide `RUSTFLAGS=--cfg loom` dependency failure and fails if no test binary is found
  (`.github/workflows/ci.yml:37-56`).
- The default chaos tier performs real child-process abort/reopen checks with row/index/tombstone
  invariants, and the current worker actually uses concurrent OS threads. Fixed master seeds and
  pre-drawn abort thresholds stabilize workload inputs (`tests/sim/tests/chaos.rs:562-615`,
  `:661-669`).
- The separate fuzz lock and exact Arrow pins explicitly prevent the fuzz harness from drifting away
  from the shipped parser dependency (`fuzz/Cargo.toml:13-27`).
- Benchmarks often gate timing on correctness, expose scale overrides, and distinguish synthetic
  microbenchmarks from real-data lifecycle measurements. The harness building blocks are sufficient;
  Phase 1 needs retained/provenanced runs, not a new benchmark framework.
- Active navigation now clearly separates current status from intentional history
  (`docs/README.md:3-14`, `docs/decisions/README.md:1-26`), and the active-doc relative-link scan found
  no broken target; the consolidated audit report referenced by
  `docs/audits/phase-1/README.md:24-26` now exists at
  `docs/audits/phase-1-sol-audit-report.md`.

## Phase disposition summary

1. **Before Phase 1 exit:** fix and regression-test VER-01; gate transaction/cache loom (VER-02);
   make thorough/checkpoint chaos execution explicit and retain artifacts (VER-03); reconcile active
   guarantee/Phase 0 wording (VER-07); and retain the measurement matrix already required by the
   performance lane (VER-06).
2. **Phase 1 hardening, not necessarily every PR:** build-gate fuzz targets and add manifest/segment
   seeded coverage (VER-04); pin/record CI tool provenance (VER-05).
3. **Phase 2:** add real Python/API integration tests when the placeholder becomes a database API and
   complete public query-surface integration coverage. Zero binding tests are truthful today, not a
   Phase 1 blocker.
4. **Phase 3 and later:** compaction/GC, cross-process coordination, branching, object storage, and
   their additional chaos/benchmark matrices remain roadmap work. They must not be used to defer the
   shared-handle verification and documentation blockers above.
