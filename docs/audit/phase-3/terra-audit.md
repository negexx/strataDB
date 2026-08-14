# Phase 3 Terra Read-Only Audit

**Audit date:** 2026-08-14
**Audited revision:** `1e82240d218f29fe57b7d08194cd6a51784a4b17` (`main`)
**Scope:** Phase 3 lifecycle mechanisms in one process using one shared `Dataset` handle on the
local filesystem. This is an independent read-only audit, not a phase-exit approval.

## Disposition

**Mechanisms assessed: implemented within the documented shared-handle boundary.** No P0 runtime
defect was confirmed by this audit, and the focused lifecycle suite, the complete workspace test
suite (including doctests), and the applicable local transaction loom models passed freshly in an
explicit native MSVC environment. **The audit remains bounded and is not an exact-head approval for
the current workspace:** GitHub Actions run `31748164676` covers committed
`1e82240d218f29fe57b7d08194cd6a51784a4b17` only; it does not cover the current uncommitted
remediation changes or this report.

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
| Performance and complexity evidence | 2 | The validator now enforces lifecycle phases and five samples and can apply an optional paired regression budget. A local synthetic 100,000-row run completed warmup plus one measured repetition before the 15-minute command bound; it observed roughly 175–205 seconds for compaction, 176–183 seconds for maintenance, and up to about 1.3 GB peak-live memory. No pinned-fixture measurements or local fixture are present. |
| Test, loom, chaos, fuzz coverage | 5 | Fresh workspace tests/doctests and the applicable local transaction loom models passed; exact-head CI was independently retrieved for the audited commit only. |
| Documentation, CI, and reproducibility | 4 | Current HEAD contains a checked-in Phase 3 closeout audit and verification report, and the exact-head CI run/job outcomes were independently retrieved. |
| Robustness, path safety, unsafe/error handling | 4 | Lifecycle paths use typed errors and recognized-name restrictions; unsafe code is isolated to the index arena and carries `SAFETY` invariants. |

**Overall: 36/45 (4.0/5) — guarded implementation, with pinned-fixture performance evidence and final uncommitted provenance still open.**

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
| T3-01 | P1 | Resolved locally; provenance remains bounded | The explicit native environment was MSVC Build Tools 18.9, MSVC `14.52.36615`, and Windows SDK `10.0.26100`. `cargo test --workspace --no-default-features` completed with exit 0, including all workspace tests and doctests. The crate-scoped txn loom build also completed; model discovery succeeded, and the local lifecycle-coordination and retention models passed. | Retain the environment and command transcript with the remediation evidence. A later exact-head CI run is still required for any commit that includes current uncommitted changes. |
| T3-02 | P2 | Closed by applicability analysis | The compaction design now explains why a dedicated filesystem-backed `Dataset::compact()` loom model would not faithfully schedule its storage effects, identifies the lifecycle-coordination and retention models that cover its controllable shared-handle interleavings, and names the normal compaction/fault-injection regressions that cover filesystem-backed effects ([compaction loom applicability analysis](../../designs/phase-3/compaction.md#loom-applicability-t3-02)). | Retain the applicability analysis and existing model/regression coverage if compaction admission or snapshot-lease mechanics change. |
| T3-03 | P2 | Partially remediated performance evidence gap | Compaction still materializes surviving physical batches and copied row-ID/timestamp vectors before writing replacement runs (`crates/txn/src/dataset.rs:416-523`). The validator/protocol now requires the lifecycle phases and five measured samples, and supports an optional paired regression budget, but no pinned fixture or resulting measurements exist locally. | Keep lifecycle performance and boundedness non-claimed until pinned-fixture compaction, recovery, and maintenance artifacts are produced and validated. Do not treat an optional paired budget as an operating threshold or product SLO. |
| T3-04 | P2 | Resolved for audited commit | Independent GitHub Actions retrieval confirmed [run 31748164676](https://github.com/negexx/strataDB/actions/runs/31748164676): `success`, `push`, and head `1e82240d218f29fe57b7d08194cd6a51784a4b17`. Its five successful jobs were [CI 94607544308](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607544308), [Phase 3 lifecycle 94607544376](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607544376), [Windows durability 94607544325](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607544325), [fuzz/provenance 94607544252](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607544252), and [Phase 0 foundation 94607544345](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607544345); the thorough chaos job [94607545151](https://github.com/negexx/strataDB/actions/runs/31748164676/job/94607545151) was skipped. | This run covers committed `HEAD` `1e82240d218f29fe57b7d08194cd6a51784a4b17`, not the current uncommitted remediation file. After this audit update is committed, obtain and retain a later exact-head run for that committing revision. |
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

CI inspection found pinned action SHAs, Rust 1.97.1, direct crate-scoped loom-binary invocation,
fast and scheduled thorough chaos, deterministic parser-fuzz smoke inputs, log artifacts, and a
separate cloud before/after benchmark validator. The current lifecycle protocol requires its named
phases and five measured samples; it may enforce a configured paired regression budget, but that
budget is optional and is not a product threshold. Synthetic lifecycle measurements are available
locally, but no pinned fixture or completed five-sample 100,000-row run is available
(`bench/cloud-performance/README.md`; `.github/workflows/ci.yml:149-182, 278-392,
480-584`; `.github/workflows/cloud-performance-before-after.yml:94-113`). Independent GitHub
Actions retrieval confirms that run `31748164676` completed successfully for audited `HEAD`
`1e82240d218f29fe57b7d08194cd6a51784a4b17` on a `push` event. Its CI job (`94607544308`) completed
the workspace test, parallel-insert feature test, transaction loom, live-set-cache loom, index loom,
clippy, format, doc, and `cargo-deny` steps successfully. The Phase 3 lifecycle job
(`94607544376`) completed its manifest-retention crash-recovery evidence step; the Windows
durability job (`94607544325`) completed its durability/restart tests; the fuzz/provenance job
(`94607544252`) completed its declared-target build and parser-smoke step; and the Phase 0
foundation job (`94607544345`) completed successfully. The thorough chaos job (`94607545151`) was
skipped. This is independently retrieved exact-head provenance for the audited commit, not
evidence for current uncommitted remediation files.

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

1. **P2:** run and publish pinned-fixture lifecycle measurements, including peak memory and
   tombstone-gap/object-count scenarios, before promoting any performance/boundedness wording.
2. **P2:** commit an approved closeout/evidence record only after its references point to the exact
   tested head.
3. **P3:** if operators need an enforced quota, create a new cross-process/continuous-maintenance
   design rather than widening `storage_bound_met` by implication.

## Audit limitations and baseline drift

The initial handoff described modified Phase 3 documentation plus an untracked closeout audit.
Those changes were subsequently incorporated into current `HEAD` before this report was finalized.
The only file created by this audit is this untracked Terra report; no pre-existing files were
restored, overwritten, or recreated. Fresh local remediation evidence applies to the current
workspace, but GitHub Actions run `31748164676` applies only to committed
`1e82240d218f29fe57b7d08194cd6a51784a4b17`. It must not be represented as exact-head evidence for
the uncommitted remediation changes or this report; a later run for the committing revision is
required.
