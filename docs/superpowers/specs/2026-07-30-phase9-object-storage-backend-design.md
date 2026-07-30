# Phase 9 — Object Storage Backend — Design

**Date:** 2026-07-30
**Status:** Approved for implementation planning

## 1. Goal and scope

Per `.claude/docs/architecture.md`'s roadmap, Phase 9 makes the same
format/manifest logic work against S3-compatible object storage, without
changing what it means to commit, read a snapshot, or search. Scope is
confirmed **S3-compatible only** (AWS S3 plus anything speaking the S3 API —
MinIO, R2, Ceph RGW, Wasabi, B2 — via one client), not a multi-cloud
abstraction; GCS/Azure are not targeted, though the chosen client crate makes
them cheap later.

The roadmap's literal exit criterion — "Full Phase 1-7 suite passes
unmodified against the object-storage backend" — is revised by this design
(see §4). A suite built to assert POSIX crash-recovery semantics (Phase 7's
`std::process::abort()` checkpoints, which assert recovery from *torn*
on-disk state) cannot pass meaningfully unmodified against a backend whose
PUTs are atomic by construction — there is no torn object to recover from,
so unmodified-and-green would prove nothing. The real exit criterion is the
triage + conformance-suite scheme in §4.

**Non-goals (unchanged from the project's existing Non-Goals table):**
multi-cloud abstraction beyond what the client crate provides for free,
multi-process-sharing-one-bucket writer coordination (never supported for
multi-process-sharing-one-directory on local disk either — see §3.3), and
any change to isolation level, conflict granularity, or the in-process commit
protocol in `crates/txn`.

## 2. How this was decided

Per `.claude/CLAUDE.md`'s model-dispatch table, backend/crate choice is a
hard architectural tradeoff and went through `llm-council` (5 independent
advisors, cross-review, chairman synthesis) after prior-art research into
Lance (this project's own storage-format reference), delta-rs, iceberg-rust,
and DuckDB. Two factual corrections surfaced during peer review and were
independently verified against this repository before being accepted into
the design, not just asserted by an advisor:

- **`object_store` is not currently a dependency.** `Cargo.lock` has no
  `object_store`, `tokio`, `hyper`, or `reqwest` entry; `arrow`'s optional
  `parquet`/`object_store` integration is not enabled. Every option in this
  phase is a genuine net-new dependency tree — "already transitive via
  arrow" (an early framing this design started from) is false and must not
  recur as a justification in the dependency ADR.
- **Sync ranged reads over `.seg` files are not the latency cliff several
  advisors assumed.** `crates/index/src/segment_reader.rs` has no `pread`,
  no `read_at`, no mmap anywhere — segments are already whole-object-load
  (`SegmentReader::from_bytes(&[u8])`) then fully in-memory traversal. One
  GET per segment is object storage's best case; `crates/index` needs
  essentially zero change for this phase.

The full council transcript (all 5 advisor responses, all 5 peer reviews,
chairman synthesis) is preserved in this session's history; this doc
captures the settled design, not the deliberation.

## 3. Architecture

### 3.1 The `Backend` trait

Today, `crates/storage` has no I/O abstraction at all — every site is a free
function over `&Path`: `datafile::{write_batch, write_bytes, read_batch,
read_batch_columns, sync_dir}`, `manifest::{commit_manifest, read_current}`.
This phase introduces one new trait, object-safe, fully synchronous:

```rust
trait Backend: Send + Sync {
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn get_range(&self, key: &str, range: Range<u64>) -> Result<Vec<u8>>;
    fn get_many(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>>;
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;
    fn delete(&self, key: &str) -> Result<()>;
}
```

No streams, no `AsyncRead`, no `impl Future`, no `Path` in the signature —
an async trait sync-wrapped at this boundary would leak async upward through
every layer above `crates/storage` forever, which is exactly what this
design avoids. `put`/`put_if_absent` return only once the write is durable
per the redefinition in §4. `get_many` exists from day one, not added later
— it's where prefetch and multi-segment hydration live, and retrofitting it
after callers exist means touching every call site twice.

**Deviation from the original trait sketch above:** `get_many` was deferred
out of M0's six-method implementation (`get`/`put`/`get_range`/
`put_if_absent`/`list`/`delete`) rather than included from day one as
originally planned here. Nothing in M0 has a multi-object caller to
exercise it against, and `LocalFs` has no meaningful concurrency story for
it yet. It remains planned for M2, where `S3Backend`'s async bridge (§3.2)
gives it a real implementation (`block_on(join_all(...))`) and a real
caller (segment/manifest prefetch). Flagged here so this deviation is a
recorded decision, not a silent drop.

Two implementations: `LocalFs` (wraps today's `std::fs`-based code
verbatim — no behavior change) and `S3Backend` (via `object_store`, S3
feature only). Backend selection is runtime, by URI scheme (`file://` vs
`s3://`) resolved where `Dataset::open`/`Dataset::create` parse their
location argument today — matching the pattern independently converged on
by Lance, delta-rs, and iceberg-rust.

### 3.2 Async/sync bridge

`object_store` (like every mainstream Rust object-storage client) is async
end-to-end. The bridge lives entirely inside `crates/storage`:

- One `OnceLock<tokio::runtime::Runtime>`, **`current_thread` flavor, one
  dedicated OS thread**, constructed lazily on first `s3://` open.
- Every `S3Backend` method body is `RT.handle().block_on(fut)`.
  `get_many` becomes one `block_on(join_all(...))` — concurrency without
  adding threads.
- No tokio type appears in any signature crossing into `crates/txn` or any
  layer above `crates/storage`.
- **`current_thread`, not multi-thread-with-N-workers**: `object_store`'s
  underlying `hyper` client is fully async, so N concurrent in-flight
  requests on one reactor thread already gets all the request concurrency
  that determines S3 throughput. This sidesteps the codebase's documented
  Windows thread-budget constraint (`.claude/rules/concurrency-txn-layer.md`
  — loom caps 5 created threads per execution) entirely rather than tuning
  around it, since the runtime's thread count is exactly one, fixed,
  independent of load.
- **Hard rule, enforced not just documented:** never call `block_on` while
  holding `crates/txn`'s commit lock.
- **Hard rule for `crates/bindings`:** every PyO3 entry point that can reach
  a `Backend` wraps the call in `py.allow_threads`. `crates/bindings` has
  zero `allow_threads` usage today — harmless while all I/O is local
  `std::fs`, but blocking a Python-called thread inside the runtime would
  stall every other Python thread the moment a call does a real network
  round-trip. This is new work for this phase, not a pre-existing gap to
  defer.
- **loom never instantiates a real backend.** Every loom model runs against
  `LocalFs`/`InMemory` through the same trait; this is asserted in loom test
  setup, not left as a convention.

### 3.3 Commit and durability semantics

**Durable is redefined, explicitly, not hand-waved:** a `put`/`put_if_absent`
returns durable once the PUT returns 2xx with checksum verified. This is a
documented semantic change, not a weakening — S3's replication story is
arguably stronger than a single local fsync, and per `.claude/rules/
concurrency-txn-layer.md` this must never become a silent buffering path.

**Writer arbitration does not move.** "Who commits version N" is not, and
has never been, a storage-level primitive on local disk: `crates/storage/
src/manifest.rs`'s `commit_manifest` writes one immutable, versioned
filename (`{version:020}.manifest`) via tmp-write + fsync + atomic
`rename()`, and `read_current` picks the highest-numbered `*.manifest` file
by directory listing — there is no single mutable "current version" pointer
being CAS'd at the storage layer. Real writer arbitration happens one layer
up, in-process, via `crates/txn`'s `ArcSwap`-based `SnapshotCell` on
`Dataset.current` guarded by the in-process commit lock. That doesn't
change for this phase. Consequence: **multi-process-sharing-one-bucket
stays unsupported**, exactly as multi-process-sharing-one-dataset-directory
was already unsupported on local disk (`manifest.rs`'s own doc comment:
"Never call this twice concurrently for the same `dataset_dir` from
separate writers"). This is a scope note to state explicitly, not a new
limitation this phase introduces.

**Conditional PUT is still required — for a different reason than writer
arbitration.** S3-compatible object storage generally lacks `rename()`, but
the object-store layer doesn't need it for arbitration. It needs it to
close a hazard with no local-disk analogue: a chaos-injected
`std::process::abort()` kills the local writer, but a PUT already dispatched
to a remote server can complete *after* the process is dead. A naive
unconditional PUT from a crashed-then-recovered attempt could silently
clobber a version another reader has already observed. `put_if_absent`
(implemented via `object_store`'s `PutMode::Create`) turns a collision into
a hard `AlreadyExists` the commit path retries against at N+1, instead of
silent clobbering.

**Fail closed on capability mismatch.** Conditional-PUT support is probed
once at `open()` for an `s3://` location; if the target endpoint doesn't
support it, the backend refuses to open rather than degrading silently.
"S3-compatible" spans real behavioral variance (AWS S3, MinIO, R2, Ceph RGW,
Wasabi, B2 don't all implement `If-None-Match` identically), and this phase
does not attempt a compatibility matrix — it detects and refuses instead.

**Orphaned objects are a new failure mode.** A crash between a segment PUT
and its manifest PUT (both still land in the same order as today — segment
first, inside `Transaction::commit`'s write phase, then the manifest swap)
leaves an unreferenced object with no `unlink` path the way a stray local
file would eventually get cleaned up. New deliverable: a `strata vacuum`
CLI command — list every object under the dataset prefix, diff against every
still-reachable manifest, delete anything unreferenced.

## 4. Testing strategy

"Full Phase 1-7 suite passes unmodified" is replaced with:

1. **A triage of every existing Phase 1-7 test** into: backend-agnostic
   (must pass against both `LocalFs` and `S3Backend` — this is the real
   continuity guarantee), POSIX-semantics-only (asserts fsync/rename
   behavior specifically — stays `LocalFs`-only, tagged with a comment
   explaining why), or needs a new object-store-shaped analogue (e.g. a
   crash test built around a torn PUT-that-lands-late instead of a torn
   file). This triage list, not a literal unmodified pass, is the actual
   exit criterion for this phase.
2. **A backend conformance suite** — one test module run against every
   `Backend` impl, asserting the trait contract directly, including
   `AlreadyExists` on a `put_if_absent` collision. This is the suite that
   must never be satisfied by `InMemory` alone for anything touching
   conditional-PUT semantics, since a fake's CAS behavior is exactly the
   thing correctness is being bet on.

Four CI tiers:

- **Tier 0** (default `cargo test --workspace`, seconds): conformance suite
  against `LocalFs` + `object_store`'s `InMemory`; the backend-agnostic
  subset of the Phase 1-7 suite against `LocalFs`, unchanged.
- **Tier 1** (`s3-integration` Cargo feature, runs every PR): the same
  backend-agnostic suite plus the conformance suite against MinIO-in-Docker
  — one container, fast, and the tier that actually exercises real S3-API
  wire behavior (auth, multipart, real conditional-PUT responses).
- **Tier 2** (nightly, not gating): real AWS S3 and one non-AWS endpoint
  (R2), conformance suite only — catches vendor-specific drift Tier 1's
  MinIO alone can't.
- **Tier 3**: a fault-injecting `Backend` decorator (503s, slow bodies,
  truncated bodies, and specifically a PUT that completes *after* the
  caller was aborted) wired into the Phase 7 chaos harness (`tests/sim`) as
  a new fault class alongside process-abort, not replacing it — the only
  thing that actually tests the zombie-write hazard from §3.3.

## 5. Milestones

1. **M0 — Extract the `Backend` trait against local disk only.** Refactor
   `datafile.rs`/`manifest.rs` call sites onto `Backend`/`LocalFs`. Zero new
   dependencies. The existing Phase 1-7 suite passing here is the first
   real, non-vacuous gate for this phase — nothing about semantics has
   changed yet, so a regression here is a genuine bug, not an expected
   POSIX-vs-object-store divergence.
2. **M1 — Prove conditional-PUT behavior before committing further.** A
   throwaway probe binary: `PutMode::Create` twice to the same key against
   MinIO, R2, and real AWS S3; confirm the second call surfaces
   `AlreadyExists` on each, and note exactly how. Gates M2 onward — if
   conditional PUT doesn't behave uniformly, the fail-closed capability
   probe in §3.3 becomes load-bearing before any call site is touched.
3. **M2 — `S3Backend` + async bridge.** `object_store`-backed impl, the
   `OnceLock<Runtime>` bridge from §3.2, conformance suite green against
   MinIO (Tier 1 wired up).
4. **M3 — Commit-path integration.** `put_if_absent` manifest commit, the
   `open()`-time capability probe and fail-closed behavior, `crates/index`'s
   `.seg` write path onto `Backend` (expected near-zero change per §2's
   `segment_reader.rs` finding).
5. **M4 — Vacuum + chaos fault injection.** The `strata vacuum` orphan-object
   GC command; Tier 3's fault-injecting decorator wired into `tests/sim`.
   The `strata vacuum` command's scope explicitly includes sweeping
   `_versions/` for leftover `.tmp-*` files from crashed commits (a
   permanent leak since M0's `LocalFs::tmp_path_for` naming, per pid+counter
   uniqueness `put_if_absent` requires, doesn't self-recycle the way the
   pre-Backend `.tmp-{version}` naming did).
6. **M5 — Triage + close the exit criterion.** Classify every Phase 1-7 test
   per §4; land object-store analogues for anything POSIX-specific; write
   the dependency ADR for `object_store`/tokio (stating plainly that it is
   a genuine net-new ~200-crate tree, not "already transitive"); document
   the redefined durability invariant, the zombie-PUT hazard, and the
   single-process-only scope note in `.claude/docs/architecture.md` and
   `.claude/CLAUDE.md`.

## 6. Open risks carried into implementation

- Real behavioral variance across "S3-compatible" endpoints on conditional
  writes is the single biggest unknown; M1 exists specifically to retire it
  early and cheaply.
- `read_batch_columns`'s use of arrow IPC's `FileReader` wants `Read + Seek`;
  against an object-store key this needs either a whole-object fetch into a
  `Cursor<Vec<u8>>` or a `Read+Seek` shim over ranged GETs. Whole-object
  fetch is the simpler starting point given segments are already
  whole-object-load (§2); revisit only if a real workload needs otherwise.
- `crates/bindings`' `allow_threads` gap is being closed as part of this
  phase (§3.2), not deferred — a missed call site here is a silent
  throughput bug, not a correctness one, so it needs its own explicit test
  (a blocked S3 call must not stall an unrelated Python thread) rather than
  relying on the conformance suite to catch it incidentally.
