# Phase 1 Closeout Design

**Status:** Approved design; implementation and bounded evidence work are in progress, with the
current branch still Partial and blocked pending the remaining verification/provenance gates.

**Goal:** Close every remaining Phase 1 implementation, verification, reproducibility, and PERF-01 through PERF-05 evidence blocker without expanding into explicitly deferred Phase 2/3 work.

## Baseline and synchronization

The implementation branch was synchronized to the latest merged Phase 1 result before production changes began:

- merged baseline: PR #56, `76d12919b5234f5e089cf26e4ba469e7aaa982f0`;
- closeout branch: `codex/phase-1-gap-closure`;
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
- distributed operation, full SQL, and additional ANN families.

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

## 2026-08-04 all-at-once closeout amendment

The remaining work is executed as one coordinated closeout branch, but each deliverable remains
independently reviewable and has a disjoint write scope. The user delegated the operating-bound
decision, so the selected policy is:

- measure the full pinned 100K-row fixture before adding a product limit;
- report supported operating envelopes with exact workload, platform, toolchain, cache, repetition,
  and input identity; and
- add a typed operational guard only if a measured limit is required by an existing invariant and
  the guard can fail closed without inventing an arbitrary cap. No benchmark result may be described
  as a universal latency, memory, recovery, durability, or recall guarantee.

The closeout workstreams are:

1. **Full-scale segmented evidence.** Extend the cloud comparison workflow to dispatch the existing
   manifest/lifecycle/segment matrix with `STRATA_SEG_ROWS=100000` and a fixed query count, verify
   that both revisions loaded the complete pinned fixture and emitted the same input hash, and retain
   raw logs plus machine-readable validation. The retired monolithic `HnswIndex` path remains excluded.
2. **Remaining bounded performance evidence.** Re-run manifest growth, recovery accounting, segment
   fan-out, and snapshot residency at the largest safe existing scales. If a workload cannot produce
   a defensible bound, record that limitation and keep it open rather than adding a speculative guard.
3. **Native verification provenance.** Dispatch the current CI workflow manually so the transaction,
   cache, index loom, checkpoint, fast-chaos, thorough-chaos, fuzz, and provenance jobs execute on the
   exact branch head. Retain the `2000/2000` assertion and distinguish skipped PR-only jobs from a
   completed manual run.
4. **Final branch gate.** Run the complete required local/cloud checks on the integrated branch,
   perform fresh independent Terra reviews for every changed task, then obtain one final read-only Sol
   review. Update the canonical audit, ledger, status, roadmap, and performance records only from
   those fresh results.

Phase 1 may be marked complete only if every remaining in-scope row has fresh implementation,
regression, or required evidence and no P0/P1 blocker is left open. Full 100K-row evidence does not
close lifecycle reclamation, universal power-loss durability, or deferred
Phase 2/3 findings.
