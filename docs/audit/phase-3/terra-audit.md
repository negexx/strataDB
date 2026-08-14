# Phase 3 Terra Read-Only Audit

**Audit date:** 2026-08-14
**Audited implementation revision:** `32c79f4a582c8802f7aaf32c5e466200f633cfcc` (`main`)
**Scope:** Phase 3 lifecycle mechanisms in one process using one shared `Dataset` handle on the
local filesystem. This is an independent read-only audit, not a phase-exit approval.

## Disposition

**Mechanisms assessed: implemented within the documented shared-handle boundary.** No P0 runtime
defect was confirmed by this audit, and the focused lifecycle suite, the complete workspace test
suite (including doctests), the applicable local transaction loom models, exact-head CI, and the
pinned-fixture before/after performance protocol passed in the documented bounds. The unrelated
dirty files listed in the handoff remain outside this audit and were not included in the remediation
commits.

The Phase 3 design remains intentionally narrower than a storage-bound service: it provides explicit
operations and one final observation, not continuous/atomic enforcement, time travel, cross-process
coordination, serializability, arbitrary cleanup, or universal power-loss durability.

## Maturity matrix

Scores are 1 (absent) through 5 (direct implementation plus current, reproducible evidence).

| Area | Score | Assessment |
|---|---:|---|
| Correctness and lifecycle invariants | 4 | Focused regression suite passed; lifecycle methods use typed failures and fail-closed manifest authority. |
| Durability and recovery | 4 | Ordered publish/reopen paths and fault-injection code/tests have fresh workspace evidence in the explicit native MSVC environment; claims remain within the named local-filesystem boundary. |
| Row/index atomicity | 4 | Row and immutable segment publication remain together in `Transaction::commit`; compaction publishes replacement rows/segment through one manifest. |
| OCC, snapshots, and lifecycle concurrency | 5 | Shared-handle lock ordering and snapshot leases are explicit; the applicable local lifecycle-coordination and retention loom models passed. |
| Inventory, retention, compaction, vacuum, maintenance | 4 | All operations are present, bounded, and directly exercised by the fresh focused suite. |
| Performance and complexity evidence | 4 | The pinned 100,000-row fixture protocol completed five measured repetitions for both revisions, and the validator enforced the 20% paired wall/peak-live budget. The artifact is retained and records bounded comparison evidence, not a product SLO. |
| Test, loom, chaos, fuzz coverage | 5 | Fresh workspace tests/doctests and the applicable local transaction loom models passed; exact-head CI was independently retrieved for the audited commit only. |
| Documentation, CI, and reproducibility | 4 | Current HEAD contains a checked-in Phase 3 closeout audit and verification report, and the exact-head CI run/job outcomes were independently retrieved. |
| Robustness, path safety, unsafe/error handling | 4 | Lifecycle paths use typed errors and recognized-name restrictions; unsafe code is isolated to the index arena and carries `SAFETY` invariants. |

**Overall: 38/45 (4.2/5) — guarded implementation with current exact-head CI and bounded pinned-fixture performance evidence.**

## Positive findings

- `Transaction::commit` obtains a preparation lease before writing, conflict-checks under the shared
  commit lock, calls `commit_manifest` before publishing the replacement in-memory snapshot, and
  appends the immutable segment in that same manifest (`crates/txn/src/dataset.rs:1557-1819`). This
  supports the scoped row/index atomicity claim.
- `LifecycleCoordinator::acquire_exclusive` blocks until existing preparations finish and prevents
  later preparation admission while an executor waits (`crates/txn/src/lifecycle_coordination.rs:48-61`).
  The CI recipe invokes the exact `preparation_and_exclusive_execution_never_overlap` loom model
  (`.github/workflows/ci.yml:278-311`).
- Compaction rejects a policy that would drop active snapshots, publishes replacement objects before
  reclamation, creates an empty OCC history entry for its manifest-version advance, and validates
  protected snapshot manifests before deletion (`crates/txn/src/dataset.rs:404-658`). It preserves
  run-aware physical row-ID layout rather than incorrectly coalescing tombstone gaps.
- Vacuum takes the same lifecycle-then-commit lock order, validates current/leased manifests, walks
  every listed manifest before deciding reachability, and deletes only temporary or `.arrow`/`.seg`
  names not in the protected set (`crates/txn/src/vacuum.rs:27-108`).
- Maintenance deliberately reports, rather than promises, a bound after compact → age-retain →
  vacuum → inventory (`crates/txn/src/maintenance.rs:47-73`). This matches the documented
  non-claim.
- Local backend key validation and lifecycle retention helpers reject unsafe manifest/object keys
  (`crates/storage/src/backend/local.rs:62-125`; `crates/txn/src/retention.rs:460-482`). No unsafe
  block occurs in the lifecycle modules audited. Index arena unsafe blocks are documented with
  `SAFETY:` invariants (`crates/index/src/node.rs:35-156`,
  `crates/index/src/node_table.rs:113-387`, and `crates/index/src/node_layout.rs:185-273`).

## Findings

| ID | Severity | Classification | Evidence and impact | Required remediation |
|---|---|---|---|---|
| T3-01 | P1 | Resolved locally and at exact head | The explicit native environment was MSVC Build Tools 18.9, MSVC `14.52.36615`, and Windows SDK `10.0.26100`. `cargo test --workspace --no-default-features` completed with exit 0, including all workspace tests and doctests. The crate-scoped txn loom build also completed; model discovery succeeded, and the local lifecycle-coordination and retention models passed. | Retain the environment and command transcript; exact-head CI for the implementation commit is recorded below. |
| T3-02 | P2 | Closed by applicability analysis | The compaction design now explains why a dedicated filesystem-backed `Dataset::compact()` loom model would not faithfully schedule its storage effects, identifies the lifecycle-coordination and retention models that cover its controllable shared-handle interleavings, and names the normal compaction/fault-injection regressions that cover filesystem-backed effects ([compaction loom applicability analysis](../../designs/phase-3/compaction.md#loom-applicability-t3-02)). | Retain the applicability analysis and existing model/regression coverage if compaction admission or snapshot-lease mechanics change. |
| T3-03 | P2 | Closed with bounded evidence | The validator/protocol requires recovery, compaction, maintenance, five measured samples, pinned fixture identity, and paired wall/peak-live checks. Successful [cloud run 31802746538](https://github.com/negexx/strataDB/actions/runs/31802746538) retained artifact `9221704428` with 1,218 summary records. For the fixture lifecycle medians: recovery wall was 222.96→230.95 ms (+3.58%), compaction 76,670→79,490 ms (+3.68%), maintenance 72,310→74,160 ms (+2.56%); peak-live memory was unchanged at 219.6 MB, 1,289.2 MB, and 1,090.4 MB respectively. All paired changes are within the configured 20% budget. | Retain the artifact and keep the claim bounded to this fixture, runner, cache policy, and before/after pair. Do not treat the budget as a product SLO or universal performance guarantee. |
| T3-04 | P2 | Resolved for audited implementation commit | Exact-head [CI run 31802724390](https://github.com/negexx/strataDB/actions/runs/31802724390) and [portability run 31802724422](https://github.com/negexx/strataDB/actions/runs/31802724422) both completed successfully for `32c79f4a582c8802f7aaf32c5e466200f633cfcc`. The exact-head [cloud performance run 31802746538](https://github.com/negexx/strataDB/actions/runs/31802746538) also completed successfully. | Keep future evidence references pinned to the exact tested revision. |
| T3-05 | P3 | Closed / documented boundary | `maintain` can report `storage_bound_met = false` when active snapshots, protected history, unknown objects, or noncontiguous live row IDs keep counts above policy (`crates/txn/src/maintenance.rs:47-73`; `crates/txn/src/dataset.rs:479-523`). This is documented behavior, not a defect, and the API does not claim continuous quota enforcement. | No remediation is required within Phase 3. A quota/SLO requires a separately authorized design. |

## Testing and verification evidence

| Command | Fresh outcome |
|---|---|
| `git diff --check` | Exit 0 before report creation; only CRLF warnings for then-existing user documentation. |
| `cargo fmt --check` | Not green for the current dirty workspace: it reports the pre-existing unrelated `crates/cli/src/main.rs` formatting change; scoped rustfmt for the remediation file passes. |
| `cargo check --workspace` | Not used as a current-worktree acceptance claim; the fresh targeted `strata-txn` check and complete workspace test run passed. |
| `cargo clippy --workspace --all-targets --no-default-features -- -D warnings` | Not rerun against the current dirty workspace; the audited committed head's CI clippy steps passed. |
| `cargo test -p strata-txn --no-default-features --test lifecycle_inventory --test retention_plan --test retention_age --test manifest_retention_executor --test compaction --test vacuum --test maintenance` | Exit 0: **47 passed, 0 failed** (7 compaction, 10 inventory, 2 maintenance, 5 executor, 1 age, 16 plan, 6 vacuum). |
| `cargo check --workspace --tests --no-default-features` | Exit 0. |
| `cargo check -p strata-txn --all-targets --features test-fault-injection` | Exit 0. |
| `cargo check -p strata-txn --all-targets --features parallel-insert` | Exit 0. |
| Explicit native toolchain environment | MSVC Build Tools 18.9; MSVC `14.52.36615`; Windows SDK `10.0.26100`. |
| `cargo test --workspace --no-default-features` | Exit 0; all workspace tests and doctests passed. The command output did not provide one aggregate count, so none is asserted here. |
| `cargo rustc -p strata-txn --lib --profile test -- --cfg loom` | Exit 0; crate-scoped loom build completed and model discovery succeeded. The local lifecycle-coordination and retention models passed. |
| Synthetic lifecycle benchmark, 100,000 rows / 5,000-row batches / one pin / one warmup / five requested repetitions | The process completed warmup and one measured repetition before the 15-minute command timeout. The observed repetition reported compaction at 174.47 s, bounded maintenance at 176.81 s, and compaction peak-live at 1,289.2 MB. This is scalability evidence only; it is not pinned-fixture or before/after regression evidence. |
| Exact-head cloud lifecycle comparison | Run `31802746538` succeeded on `32c79f4a582c8802f7aaf32c5e466200f633cfcc`; artifact `9221704428` retained 1,218 summary records and passed validation with a 20% paired budget. |
| Exact-head CI and portability | CI run `31802724390` and portability run `31802724422` both succeeded for the implementation revision. |

CI inspection found pinned action SHAs, Rust 1.97.1, direct crate-scoped loom-binary invocation,
fast and scheduled thorough chaos, deterministic parser-fuzz smoke inputs, log artifacts, and a
separate cloud before/after benchmark validator. The current lifecycle protocol requires its named
phases and five measured samples; it may enforce a configured paired regression budget, but that
budget is optional and is not a product threshold. Synthetic lifecycle measurements are available
locally, and the pinned five-sample 100,000-row comparison is retained in cloud artifact
`9221704428`
(`bench/cloud-performance/README.md`; `.github/workflows/ci.yml:149-182, 278-392,
480-584`; `.github/workflows/cloud-performance-before-after.yml:94-113`). Independent GitHub
Actions retrieval confirms that CI run `31802724390` and portability run `31802724422` completed
successfully for implementation revision `32c79f4a582c8802f7aaf32c5e466200f633cfcc`. The cloud
comparison was independently retrieved as run `31802746538`, and artifact `9221704428` was
downloaded and replayed locally through the validator. The earlier run `31797829718` failed on a
retained-handle validation bug, was diagnosed from its retained artifact, and is superseded by the
corrected successful run.

The targeted static scans found lifecycle TODO/FIXME/unimplemented markers and credential patterns
absent. The unsafe scan found only documented index implementation blocks: the node-arena modules
and the segment-format byte conversions (`crates/index/src/segment_format.rs:173,195`); no unsafe
block was found in the audited lifecycle modules.

## Non-claims retained by this audit

- No serializability or read/write snapshot transaction interface.
- No coordination, conditional publication, snapshot protection, or reclamation safety across
  independent `Dataset::open` handles or processes.
- No universal filesystem/power-loss durability, storage-growth, memory, recovery-latency, ANN
  recall, or throughput guarantee.
- No background compaction, arbitrary orphan cleanup, time-travel retention, object-store support,
  migration framework, or continuous/atomic storage quota.

## Prioritized remediation plan

1. **P3:** if operators need an enforced quota, create a new cross-process/continuous-maintenance
   design rather than widening `storage_bound_met` by implication.

## Audit limitations and baseline drift

The initial handoff described modified Phase 3 documentation plus an untracked closeout audit.
Those changes were committed in the scoped remediation history (`73b761c`, `26f503a`, `32c79f4`);
unrelated user changes were preserved. Fresh local remediation evidence applies to the current
workspace, while the exact-head cloud and CI evidence applies to implementation revision
`32c79f4a582c8802f7aaf32c5e466200f633cfcc`.
