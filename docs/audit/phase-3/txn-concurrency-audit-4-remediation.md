# Audit 4 Remediation: Concurrency and Thread Safety

**Revision:** implementation branch `codex/audit4-concurrency-lock-scope`
**Scope:** `strata-txn`, one process with one shared `Dataset` handle

## Implemented

- Compaction and schema migration now reconcile a manifest that storage verified as visible but
  could not durably acknowledge. They install the candidate commit-log entry and immutable snapshot
  before returning `IndeterminateManifestPublication`.
- The post-publication compaction fault seam now runs after in-process installation and before
  reclamation, so it models the same safe visible-state boundary.
- Regression tests exercise the compaction and migration indeterminate-publication boundaries and
  verify the shared handle and reopen select the candidate version/schema.
- An ignored, configurable stress test exercises the production `ArcSwap` path with one writer and
  four readers. Readers are required to observe an initial snapshot before the writer begins its
  commit loop and a post-publication snapshot before the writer continues after its first commit;
  the local 128-commit run completed; `writer_interval_readers=4` is the durable sentinel, and
  aggregate `reader_checks` is scheduling-dependent diagnostic output rather than a fixed
  acceptance value.
- Manual/scheduled ARM64 and ThreadSanitizer workflow lanes were added with retained provenance
  artifacts. Workflow configuration is not evidence that either hosted lane has passed.

## Deliberate non-change

Lifecycle exclusivity remains a writer barrier. Narrowing `commit_lock` around lifecycle filesystem
I/O alone would not permit normal publication because commit preparation is blocked by that lifecycle
barrier; a two-phase lifecycle protocol is outside this audit. Loom remains an abstract observable
snapshot model rather than an implementation model of `ArcSwap` internals.

## Fresh verification

- `cargo test -p strata-txn --features test-fault-injection --test compaction --test schema_migrations`: passed, including the new regression tests.
- `cargo test -p strata-txn --features test-fault-injection`: passed (281 unit tests plus focused integration/doc tests).
- `cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings`: passed.
- `cargo test --workspace --no-default-features`: passed after running from an explicitly initialized
  MSVC environment with the MSVC include/lib directories present.
- `cargo fmt --check` and `git diff --check`: passed.
- The ignored stress run's local 128-commit run completed;
  `writer_interval_readers=4` is the durable sentinel, and
  aggregate `reader_checks` is scheduling-dependent diagnostic output rather than a fixed
  acceptance value.

ARM64 and TSan hosted runs were not executed in this local Windows session; they remain pending
evidence, not green claims. Cross-process coordination, FIFO scheduling, serializability, and
universal weak-memory guarantees remain out of scope.
