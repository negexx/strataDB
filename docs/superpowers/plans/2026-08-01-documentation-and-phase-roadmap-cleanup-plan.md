# Strata Documentation and Phase Roadmap Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Strata’s documentation truthful, navigable, historically complete, and phase-oriented, then produce a multi-lane Phase 1 audit report without changing runtime behavior.

**Architecture:** Keep a small active documentation surface (`docs/README.md`, `docs/status.md`, `docs/roadmap.md`, current architecture/decisions), and move superseded ADRs, mechanism specifications, and analyses into `docs/history/` with an index explaining each transition. Update agent guidance and CI/documentation references to point only at current material. Run independent read-only Sol audits against the cleaned baseline and reconcile them into a single Phase 1 report.

**Tech Stack:** Markdown, Git, PowerShell/`rg`, Rust/Cargo verification where configuration or CI references change, Codex Sol subagents for independent audits.

## Global Constraints

- Preserve ADR and design history in `docs/history/`; do not erase historical reasoning.
- Delete only redundant generated files, retired tool configuration, or artifacts with no historical or operational value after checking references.
- Do not change Rust runtime behavior, public APIs, dependencies, or data formats in this plan.
- The current concurrency claim is limited to one process using a shared `Dataset` handle unless source evidence proves otherwise.
- Status labels must distinguish Implemented, Partial, Proposed, Historical, Superseded, and Deferred.
- Every current claim that materially affects adoption must link to source, test, CI, or an explicit known limitation.
- Preserve unrelated dirty-worktree changes; do not use reset, checkout, clean, or broad recursive deletion.
- Phase 1 audits are read-only and must report evidence paths, severity, confidence, affected phase, and disposition.

---

## File map and ownership

- `docs/README.md`: reading order, active-vs-history boundary, and links to the authoritative documents.
- `docs/status.md`: implementation truth ledger, evidence pointers, old-to-new phase mapping, and known limitations.
- `docs/roadmap.md`: capability phases, milestones, exit criteria, and explicit dependencies.
- `docs/history/README.md`: archive policy and index of moved ADRs/designs/analyses.
- `docs/history/decisions/`: superseded or historical ADRs, preserving original content.
- `docs/history/design/`: superseded phase/mechanism specifications and implementation plans.
- `docs/history/analysis/`: historical investigations that no longer describe the active baseline.
- `docs/decisions/README.md`: index of active decisions and proposals.
- `docs/architecture.md`: current implementation architecture and bounded product claims.
- `docs/how-strata-works.md`: current end-to-end explanation, aligned with immutable segments.
- `docs/conventions.md`: current contributor and agent conventions, with stale links removed.
- `docs/FUTURE.md`: future work index that points to the capability roadmap instead of obsolete claims.
- `AGENTS.md`: current Codex/Luna/Sol/Terra operating guidance and links to authoritative docs.
- `.github/workflows/ci.yml`: only stale documentation/rule references, if still present after archival.
- `docs/superpowers/specs/2026-08-01-documentation-and-phase-roadmap-cleanup-design.md`: approved design record.
- `docs/superpowers/plans/2026-08-01-documentation-and-phase-roadmap-cleanup-plan.md`: this execution plan.
- `docs/superpowers/plans/2026-08-01-documentation-and-phase-roadmap-cleanup-ledger.md`: task progress and audit disposition ledger.

### Task 1: Establish the active documentation map and status ledger

**Files:**
- Create: `docs/README.md`
- Create: `docs/status.md`
- Create: `docs/roadmap.md`
- Create: `docs/decisions/README.md`
- Create: `docs/history/README.md`
- Create: `docs/superpowers/plans/2026-08-01-documentation-and-phase-roadmap-cleanup-ledger.md`

**Interfaces:**
- Consumes: current source layout, `Cargo.toml`, CI workflow, existing ADR/design filenames, and the approved cleanup design.
- Produces: stable links and status vocabulary that later cleanup tasks use as their source of truth.

- [ ] **Step 1: Create the status vocabulary and evidence table**

  Record each major capability as Implemented, Partial, Proposed, Historical, Superseded, or Deferred. Include evidence paths for storage, transactions, immutable snapshots, query features, vector segments, CLI, bindings, chaos testing, durability, compaction, cross-process coordination, branching, object storage, and schema/migration behavior.

- [ ] **Step 2: Create the capability roadmap**

  Write Phases 0–6 from the approved design, including scope, dependencies, current status, explicit non-goals, and measurable exit criteria. Map legacy phase documents to the capability phases.

- [ ] **Step 3: Create navigation and archive indexes**

  Make `docs/README.md` explain the recommended reading order. Make `docs/decisions/README.md` list active decisions/proposals. Make `docs/history/README.md` explain that history is preserved but non-authoritative, and reserve subfolders for decisions, design, and analysis.

- [ ] **Step 4: Initialize the execution ledger**

  Add the plan identity as the first line and track each task, review result, parked minor finding, and final audit disposition.

- [ ] **Step 5: Verify navigation targets**

  Run `rg --files docs` and a link-target check over the new files. Confirm every link target exists before proceeding.

### Task 2: Archive superseded ADRs and legacy design material

**Files:**
- Move: superseded/reversed files from `docs/decisions/` to `docs/history/decisions/`, preserving their filenames/content.
- Move: obsolete mechanism specifications and implementation plans from `docs/design/` to `docs/history/design/`.
- Move: historical investigations from `docs/analysis/` to `docs/history/analysis/` when their conclusions describe retired architecture.
- Modify: `docs/history/README.md` with one entry per moved file and its replacement/current status.
- Modify: `docs/decisions/README.md` with active ADR statuses and supersession links.

**Interfaces:**
- Consumes: Task 1 indexes and the existing ADR/design corpus.
- Produces: a clean active documentation boundary without losing historical context.

- [ ] **Step 1: Classify every ADR**

  Keep active/current or active-proposal decisions in `docs/decisions/`; archive the template and decisions superseded by later ADRs under `docs/history/decisions/`. Preserve original decision text and add only an archive note/index entry when needed.

- [ ] **Step 2: Classify every phase/design document**

  Keep specifications that still govern current implementation in `docs/design/`. Move designs for the retired mutable HNSW graph, delta-log mechanism, or otherwise superseded implementation plans to `docs/history/design/`.

- [ ] **Step 3: Classify analysis documents**

  Move analyses that are historical snapshots or describe retired mechanisms to `docs/history/analysis/`. Keep current verification/status material active and link it from `docs/status.md`.

- [ ] **Step 4: Update archive headers and indexes**

  Add a short `Status`, `Superseded by`, or `Historical context` header only where the original document has no reliable status metadata. Do not rewrite the body of an ADR to change its historical record.

- [ ] **Step 5: Verify no active link points at an archived path without explanation**

  Search all tracked Markdown, `AGENTS.md`, and CI files for moved paths and update active references to point to the current replacement.

### Task 3: Rewrite the active architecture and user-facing explanations

**Files:**
- Modify: `docs/architecture.md`
- Modify: `docs/how-strata-works.md`
- Modify: `docs/conventions.md`
- Modify: `docs/FUTURE.md`
- Modify: `AGENTS.md` only where links or guarantees remain stale after the earlier Codex migration.

**Interfaces:**
- Consumes: `docs/status.md`, current source in `crates/storage`, `crates/txn`, `crates/index`, `crates/query`, `crates/bindings`, `crates/cli`, and the current ADR index.
- Produces: the authoritative narrative for humans and agents.

- [ ] **Step 1: Align architecture with the current commit path**

  Describe buffered write batches, durable data/segment writes, conflict validation, manifest publication, and immutable snapshot installation. State that the current local publication is lock-serialized and not a cross-process storage CAS unless source evidence changes.

- [ ] **Step 2: Align vector-index documentation**

  Describe immutable per-commit HNSW segments, segment metadata, search fan-out, tombstone/live-set filtering, and current growth/compaction limitations. Remove active delta-log/shared-mutable-graph claims.

- [ ] **Step 3: Bound transaction semantics accurately**

  State the actual write-write conflict model, shared-handle/process scope, read API limitations, update/delete identity behavior, and schema limitations. Keep aspirational snapshot-isolation language in history or roadmap unless the current API supports it.

- [ ] **Step 4: Align query, CLI, and bindings claims**

  Distinguish implemented query primitives and MVP CLI behavior from missing planner integration, random access, stable schema catalog, and the placeholder Python binding surface.

- [ ] **Step 5: Replace stale future-work claims**

  Move implemented zone-map/index work out of “future” and link all remaining work to `docs/roadmap.md` and `docs/status.md`.

- [ ] **Step 6: Verify terminology and stale-claim searches**

  Search active docs for `.opencode`, `delta log`, `mutable graph`, `hnsw_rs`, “full snapshot isolation,” and claims contradicted by the current API. Review each remaining hit and either correct it or link it explicitly as historical context.

### Task 4: Repair configuration and reference hygiene

**Files:**
- Modify: `.github/workflows/ci.yml` only for stale documentation/rule references.
- Delete: obsolete machine-specific project files only after reference and operational checks prove they have no current role.
- Modify: `.gitignore` only if the cleanup creates or removes a tracked generated-artifact convention.

**Interfaces:**
- Consumes: active docs/indexes and repository configuration.
- Produces: no broken operational/documentation references.

- [ ] **Step 1: Search configuration and source comments for retired paths**

  Run `rg -n "\.opencode|opencode\.json|skills-lock|concurrency-txn-layer|vector-index|python-bindings" --glob '!target/**' .` and classify each hit before editing.

- [ ] **Step 2: Update CI comments and links**

  Replace only references that point to removed OpenCode paths or retired active rules. Preserve CI behavior unless a reference itself makes the workflow invalid.

- [ ] **Step 3: Remove only approved obsolete artifacts**

  Delete files that are machine-specific, retired configuration with no active consumer, or exact duplicates, after checking `git grep` and repository tooling. Do not delete history files or user-owned unrelated changes.

- [ ] **Step 4: Verify configuration hygiene**

  Re-run the retired-path search and inspect `git diff --stat` and `git status --short` for accidental source/config changes.

### Task 5: Run the Phase 1 Sol audit pack

**Files:**
- Create: `docs/audits/phase-1-sol-audit-report.md`
- Create: `docs/audits/phase-1/` lane reports if the audit orchestrator needs separate artifacts.
- Modify: `docs/status.md` with verified findings and blockers.
- Modify: `docs/roadmap.md` with Phase 1 exit-criterion disposition.
- Modify: `docs/superpowers/plans/2026-08-01-documentation-and-phase-roadmap-cleanup-ledger.md` with lane results and reconciliation.

**Interfaces:**
- Consumes: cleaned active docs, current source/tests/CI, and the exact repository revision after Tasks 1–4.
- Produces: independent correctness, concurrency, durability, index-atomicity, performance, architecture/API, and verification/documentation audit findings.

- [ ] **Step 1: Capture the audit baseline**

  Record the commit/worktree state, `cargo metadata --no-deps --format-version 1`, relevant test commands, and the active status/roadmap documents.

- [ ] **Step 2: Dispatch seven independent read-only Sol lanes**

  Each lane must inspect the current code and docs, not implement fixes, and report findings with severity, evidence path/line, confidence, affected phase, and disposition recommendation.

- [ ] **Step 3: Reconcile all lanes**

  Deduplicate findings, distinguish documentation corrections from real Phase 1 blockers and later-phase work, and record disagreements with evidence.

- [ ] **Step 4: Write the consolidated report**

  Include an executive verdict, guarantee matrix, finding table, test/CI evidence, Phase 1 exit-criteria matrix, and prioritized next actions.

- [ ] **Step 5: Update status and roadmap from evidence**

  Change statuses only where the audit establishes evidence; keep unresolved questions explicitly marked Partial or Deferred.

### Task 6: Final verification and whole-branch review

**Files:**
- Modify: none unless the review identifies a required documentation correction.
- Review: all files changed by Tasks 1–5.

**Interfaces:**
- Consumes: the complete documentation cleanup and consolidated audit report.
- Produces: a verified, internally linked documentation baseline and final review disposition.

- [ ] **Step 1: Run repository-wide stale-reference and link checks**

  Confirm no active document points at a missing file, no retired OpenCode path is presented as active, and every archive entry resolves.

- [ ] **Step 2: Run proportionate repository verification**

  If only Markdown changed, run the documentation/link checks and `cargo test --workspace` only if the environment can complete it without modifying source state. If CI/configuration changed, also run the relevant workflow validation or `cargo fmt --check`/`cargo metadata` checks.

- [ ] **Step 3: Dispatch the broad Sol review**

  Review the complete documentation diff, phase model, archive boundary, and audit report for factual consistency, missing historical links, overclaims, and accidental deletion.

- [ ] **Step 4: Resolve review findings**

  Apply only documentation-scope fixes, rerun the affected checks, and update the ledger with the final verdict.

- [ ] **Step 5: Report completion with evidence**

  Summarize changed files, archived/deleted artifacts, Phase 1 audit results, unresolved blockers, and verification commands without claiming runtime features were implemented.
