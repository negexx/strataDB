# Phase 1 Closeout Design

**Status:** Approved design; implementation and bounded evidence work are in progress, with the
current branch still Partial and blocked pending the remaining verification/provenance gates.

**Goal:** Close every remaining Phase 1 implementation, verification, reproducibility, and PERF-01 through PERF-05 evidence blocker without expanding into explicitly deferred Phase 2/3 work.

## Baseline and synchronization

The implementation branch was synchronized to the merged Phase 1 result before production changes began:

- merged baseline: `8cd7696fdcf34f6253fb11f9e110f6632bc872de`;
- closeout branch: `codex/phase-1-close-all-gaps`;
- the closeout branch is based on the merged commit; subsequent task commits and evidence are listed
  in the closeout ledger and canonical performance record.

The root checkout contains unrelated user changes. The isolated worktree is the only write scope for this closeout.

## Exit boundary

The closeout target includes:

- remaining correctness, durability, recovery, schema, facade, and row/index identity blockers;
- VER-01 through VER-06 reproducibility and verification blockers;
- PERF-01 through PERF-05 current operating-envelope and evidence blockers;
- the incomplete transaction loom model and thorough chaos evidence;
- fresh canonical documentation that distinguishes implementation, evidence, and deferred work.

The closeout does not absorb explicitly later work:

- PERF-06 and PERF-07 projection/pruning query work;
- IDX-03 underfilled ANN requests and broader Phase 2 API work;
- compaction, vacuum, orphan cleanup, retention, incremental manifests, and indefinite-growth guarantees;
- cross-process publication, distributed operation, full SQL, and additional ANN families.

## Closure lanes

### Lane A: Baseline and finding ledger

Luna records the synchronized merged baseline, dirty-state boundary, exact audit findings, allowed files, dependencies, and exit criteria. A machine-readable or mechanically scannable ledger must map every remaining Phase 1 finding to implementation evidence, regression coverage, or a named evidence artifact.

### Lane B: Verification and reproducibility

Add or complete gates for:

- every named transaction, cache, and index loom model using crate-scoped builds and no workspace-wide `RUSTFLAGS`;
- the checkpoint-abort test and fast chaos tier;
- the full 2,000-seed thorough chaos tier with explicit `2000/2000` output;
- fuzz target build/discovery and focused recovery-parser smoke coverage;
- pinned CI action/toolchain provenance and retained logs/artifacts;
- stale-link, stale-claim, credential, and exact-scope scans.

The seventh loom model and thorough chaos gate are not complete until fresh output demonstrates the exact required assertions. A timeout is evidence of incompletion, not success.

### Lane C: PERF-01 through PERF-05 evidence

Use the current manifest-listed immutable segment path and deterministic inputs. Capture host, toolchain, filesystem, CPU/RAM, cache policy, fixture/source, seed, hashes, commands, repetitions, warmups, raw logs, and machine-readable summaries.

- PERF-01: establish reproducible provenance across the supported CI/platform matrix and identify real-fixture versus synthetic evidence.
- PERF-02: measure manifest bytes and timing at multiple retained-history points; document the bounded operating envelope without claiming an asymptotic guarantee.
- PERF-03: add or expose recovery-byte accounting and measure ingest, commit, reopen/recovery, retained-history, and concurrent-commit behavior at multiple bounded scales.
- PERF-04: measure K=1 through the supported segment envelope for recall, latency, throughput, and filtered search; define the supported bound or an explicit operational rejection/guard if the design requires one. Do not call the retired direct graph path a comparable baseline.
- PERF-05: measure pinned snapshot/cache residency with retained manifest/data/segment footprint accounting. Treat process-wide RSS as supplemental, not as the sole memory bound.

No HNSW parameter or production limit changes are allowed without an approved design, regression coverage, and benchmark evidence. If a finding can be closed by evidence and documentation alone, do not add production behavior.

### Lane D: Canonical status and closeout

Update `docs/phase-1-audit.md`, `docs/status.md`, `docs/roadmap.md`, and the performance evidence document only after fresh evidence exists. Each finding must say one of:

- implemented and regression-covered;
- evidence-complete within a named bounded scope;
- explicitly deferred outside Phase 1.

The Phase 1 verdict changes from `Partial/blocked` only when every in-scope blocker has fresh support and no P0/P1 item is left without an implementation, regression, or required evidence artifact.

## Review workflow

For each lane/task:

1. Terra implementation worker receives exact files, invariants, dependencies, and checks.
2. Terra writes failing regression or evidence gate first where behavior changes.
3. A fresh independent Terra subagent reviews the task diff and evidence; it makes no edits.
4. Terra resolves accepted findings and reruns the affected checks.
5. Luna consolidates only after the task review is Ready.

This closeout uses Terra for every task review as explicitly requested. Sol is reserved for one final independent review of the complete branch after all task reviews and final verification.

## Final verification and integration

Before publication, run fresh output for formatting, build/check/test, clippy, docs, deny, metadata, all relevant loom models, fuzz discovery/smoke, checkpoint, fast and thorough chaos, benchmarks, stale-claim/link scans, and `git diff --check`. Stage only intentional files, inspect the staged diff, commit focused changes, push, open a ready-for-review PR, wait for required checks, obtain the final Sol review, merge only after all gates pass, confirm remote `main`, then remove the worktree and branch.
