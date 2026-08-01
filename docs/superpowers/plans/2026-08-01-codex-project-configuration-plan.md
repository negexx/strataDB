# Codex Project Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Strata's stale OpenCode-specific root guidance and add a portable, trusted-project
Codex configuration implementing the Luna → Sol → Terra workflow without changing Rust behavior or
disturbing existing user work.

**Architecture:** Root `AGENTS.md` remains the shared engineering/source-of-truth contract;
`.codex/config.toml` supplies safe project runtime defaults and enables multi-agent support;
`.codex/agents/*.toml` defines the three bounded roles; `.codex/README.md` documents trust,
activation, validation, and the machine-local boundary. The implementation is configuration-only and
preserves the current `.opencode/` deletions and every unrelated dirty file.

**Tech Stack:** Markdown, TOML, Codex project configuration/custom agents, PowerShell, Python 3.14
`tomllib`, Git read-only checks.

## Global Constraints

- Controlling design:
  `docs/superpowers/specs/2026-08-01-codex-project-configuration-design.md`.
- The worktree is already dirty on `main` and is ahead of `origin/main`; preserve every existing
  change. Never run reset, checkout, clean, restore, or broad replacement commands.
- Do not edit Rust source, tests, benchmarks, CI, existing technical docs, untracked user docs,
  `.opencode/`, `opencode.json`, or `skills-lock.json`.
- Do not restore any deleted OpenCode artifact.
- The only implementation writes are root `AGENTS.md`, `.gitignore`, and the five files in the exact
  `.codex/` tree defined below.
- Preserve the user's current `.gitignore` addition for `skills-lock.json` byte-for-byte; add one
  Codex log pattern after it.
- Use `apply_patch` for every edit. If any planned new `.codex/` file exists at execution time, stop
  and inspect instead of overwriting it.
- Do not read or modify `%USERPROFILE%\.codex`, auth/session state, trust settings, global profiles,
  plugins, connectors, or credentials.
- Do not set `CODEX_HOME` to this repository's `.codex/` directory.
- Do not add MCP servers, hooks, prompts, skills, plugins, dependencies, or machine-specific paths.
- Use only skills/tools actually exposed in the execution session. Caveman is not available in the
  planning session and must not be claimed or required.
- Do not commit, stage, push, branch, or open a pull request unless the user gives separate explicit
  authority after implementation review.
- A fresh Sol review and the entire Task 4 validation gate are required before Luna may report the
  configuration ready.
- Documentation/config-only verification must not claim the unrelated dirty Rust worktree is green.

## File map and contracts

| File | Action | Single responsibility |
|---|---|---|
| `AGENTS.md` | Replace | Durable repository engineering rules, source precedence, role routing, safety, and verification. |
| `.codex/config.toml` | Create | Trusted-project Luna defaults, approval/sandbox posture, and bounded multi-agent settings. |
| `.codex/README.md` | Create | Human operational guide for trust, roles, committed/local boundaries, smoke tests, and troubleshooting. |
| `.codex/agents/luna.toml` | Create | Read-only triage/dispatcher custom agent. |
| `.codex/agents/sol.toml` | Create | Architecture/specification/planning and independent-review custom agent. |
| `.codex/agents/terra.toml` | Create | Plan-bound implementation worker custom agent. |
| `.gitignore` | Modify minimally | Ignore only optional repository-local Codex diagnostic logs while preserving user changes. |

The five `.codex/` files are one configuration interface: names are exactly `luna`, `sol`, and
`terra`; root config selects Luna defaults; root `AGENTS.md` defines the routing contract; the README
explains the relationship without adding another instruction source.

---

### Task 1: Replace root guidance with the Codex engineering contract

**Files:**

- Modify: `AGENTS.md` (replace the whole file)
- Do not modify: every other file in this task

**Interfaces:**

- Consumes: current architecture, phase-0 spec, conventions, ADR status, Cargo/CI commands, dirty
  migration state, and the role/config contracts in the controlling design.
- Produces: one repository-wide instruction layer under 200 lines and 32 KiB, with no active
  OpenCode-only path/tool/model references. Tasks 2 and 3 rely on its Luna/Sol/Terra names and
  handoff rules.

- [ ] **Step 1: Capture the pre-edit baseline and confirm the target is tracked**

Run:

```powershell
git status --short --branch
git diff -- AGENTS.md .gitignore
git ls-files --error-unmatch AGENTS.md
Test-Path -LiteralPath '.codex'
```

Expected:

- the branch and all pre-existing dirty files are visible;
- `AGENTS.md` is tracked;
- the current `.gitignore` diff shows the user's `skills-lock.json` hunk;
- `.codex` is absent. If `.codex` is present, stop and inspect it before continuing.

- [ ] **Step 2: Replace `AGENTS.md` with this exact content**

```markdown
# Strata

> Durable project guidance for Codex. It applies to the whole repository. Keep this file concise;
> put task-specific reasoning in `docs/` and use closer-scoped `AGENTS.md` files only when a subtree
> genuinely needs different rules.

## Mission

Strata is an embedded, single-node Rust database for concurrent AI-agent access to structured
columns and vector embeddings. Its differentiator is correctness under concurrent writes: snapshot
isolation, optimistic conflict detection, durable publication, and row/index consistency without
silent buffering.

Non-negotiable invariants:

- A write is acknowledged only after it is durable, conflict-checked, and visible.
- Row data and vector-index changes share one transaction boundary and manifest publication.
- The vector index uses immutable per-commit segments listed in the manifest; do not reintroduce a
  shared graph mutation or the superseded delta-log mechanism.
- Isolation is snapshot isolation, not serializability. Do not add write-skew prevention or another
  isolation level without a superseding ADR.
- Conflicts are typed errors that identify contested row IDs; never silently resolve them.
- Row IDs are dataset-global logical identities: monotonically allocated, never reused; gaps are
  safe.
- Strata remains embedded and single-node. Distributed transactions, full SQL, and additional ANN
  index families are out of scope.

## Source of truth

Before changing behavior, read the narrowest current sources needed:

1. `docs/architecture.md` for system shape, roadmap, and non-goals.
2. `docs/design/phase-0-transaction-and-format-spec.md` before `crates/txn` or `crates/storage` work.
3. `docs/conventions.md` and accepted ADRs under `docs/decisions/`.
4. Current source, tests, Cargo manifests, and `.github/workflows/ci.yml`.
5. Relevant current design/amendment under `docs/superpowers/specs/`, then its plan.

Historical specs/plans preserve rationale and may describe superseded mechanisms or paths. Resolve
disagreements against accepted ADRs, living specs, current code, and tests. ADR 0005 supersedes the
C++ toolchain ADRs; ADR 0008 adopts immutable index segments; ADR 0006 group commit is only proposed.

## Workspace

- Rust edition 2024, toolchain 1.90, Cargo workspace.
- `crates/storage`: columnar files, manifest/versioning, backend abstraction.
- `crates/txn`: OCC, snapshots, commit protocol, row/index atomicity—the flagship subsystem.
- `crates/index`: from-scratch HNSW and immutable on-disk segments.
- `crates/query`: predicates, pruning, vectorized operators, group-by.
- `crates/bindings`: PyO3 extension module.
- `crates/cli`: `strata` command-line interface.
- `crates/chaos-worker` and `tests/sim`: crash/concurrency correctness harness.
- `bench`: Criterion benchmarks; `fuzz`: separate cargo-fuzz workspace.

## Commands

```text
cargo check --workspace
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo doc --workspace --no-deps
cargo deny check bans sources advisories
```

When relevant, also run:

```text
cargo test -p strata-txn --features parallel-insert
cargo clippy -p strata-txn --all-targets --features parallel-insert -- -D warnings
```

For loom, follow the crate-scoped
`cargo rustc -p strata-index --lib --profile test -- --cfg loom` pattern (substituting
`strata-txn` only when testing that crate) and run the produced test binary directly. Never set
workspace-wide `RUSTFLAGS=--cfg loom`; it breaks dependency crates whose loom crate is dev-only.
Use the exact current CI/module recipe.

## Engineering rules

- Safe Rust by default. Every `unsafe` block needs a `// SAFETY:` invariant; never use `unsafe` to
  evade ownership design.
- Use typed `Result` errors on engine paths. Avoid `unwrap()`/`expect()` in production and at the
  PyO3 boundary.
- Keep PyO3 thin, release the GIL around blocking I/O/locks, and preserve distinct conflict errors.
- No dependency additions without explicit justification and user approval.
- HNSW parameter changes require benchmark evidence. Performance work must state the measured
  baseline and preserve correctness gates.
- Every interleaving-sensitive `crates/txn`/`crates/index` change needs a targeted loom model plus
  normal tests. State why loom is not applicable when a nearby change is purely structural.
- Preserve on-disk compatibility deliberately. Reject corrupt/unknown state loudly; never make a
  compatibility change accidentally.
- Prefer vertical slices, focused files, and the smallest change that satisfies the approved design.

## Luna → Sol → Terra workflow

- **Luna** owns intake, read-only triage, status/baseline capture, dispatch, and final consolidation.
  Luna may answer questions and handle trivial orchestration, but routes non-trivial changes through
  Sol before Terra.
- **Sol** owns architecture, concurrency analysis, design specs, ADRs, implementation plans, and
  fresh independent review. In review mode Sol makes no edits.
- **Terra** executes one approved plan task, uses TDD for behavior changes, runs task verification,
  and returns evidence. Terra does not redesign requirements silently.

For non-trivial work:

1. Luna records objective, allowed/forbidden scope, dirty baseline, controlling docs, and success
   criteria.
2. Sol explores current code/docs/history, compares approaches, obtains design approval, and writes
   the spec/plan.
3. Luna dispatches one bounded task at a time to Terra. Parallelize read-only independent work only;
   do not allow overlapping writers.
4. Terra returns files changed, commands/results, deviations, and blockers—never an unsupported
   whole-task completion claim.
5. A fresh Sol reviews every non-trivial diff. `crates/txn` or `crates/index` changes always require
   this review.
6. Terra resolves accepted findings and reruns affected checks. Luna reads the final diff and fresh
   verification output before reporting completion.

Every handoff includes the objective, exact file scope, invariants, controlling spec/plan, interfaces,
expected checks, dirty-worktree notes, authority limits, and required return evidence.

## Skills and documentation

Before acting, inspect the skills/tools actually exposed in the session. Use relevant Superpowers
workflows when available: brainstorming for design, systematic debugging for failures, writing-plans
before multi-step edits, TDD for behavior changes, and verification-before-completion before success
claims. Read and follow the selected skill; do not merely name it. Never claim an unavailable skill,
connector, subagent, or reviewer was used.

For external APIs/crates, prefer an exposed authoritative documentation tool. Otherwise inspect
official docs and installed dependency source, then verify assumptions by compiling/testing. Do not
hard-code credentials or machine-local connector configuration in the repository.

## Verification and review gate

- Guidance/config-only changes: parse config, smoke-test Codex discovery, scan for stale references
  and credentials, run `git diff --check`, and verify exact file scope.
- Rust changes: targeted red/green tests, then build, workspace tests, clippy, and format.
- Feature, loom, benchmark, chaos, fuzz, and binding checks are additional gates when the affected
  subsystem or controlling plan requires them.
- Evidence comes from fresh command output and diff inspection, not agent confidence or a prior run.
- Do not call unrelated dirty Rust work green after a documentation/config-only task.

## Worktree and Git safety

- Assume the worktree may contain user changes. Start with `git status --short`; preserve unrelated
  modifications and untracked files.
- Never reset, checkout, restore, clean, delete, or reformat outside the assigned scope.
- Do not commit, stage, push, branch, force-push, or open a PR unless the user explicitly requests it.
- If committing is authorized, stage only reviewed task files and inspect the staged diff first.
- Never commit `.env*`, credentials, auth/session data, build artifacts, benchmark datasets, or
  machine-local Codex state.
```

- [ ] **Step 3: Verify size, required contracts, and absence of stale active guidance**

Run:

```powershell
$lines = (Get-Content -LiteralPath 'AGENTS.md' | Measure-Object -Line).Lines
$bytes = (Get-Item -LiteralPath 'AGENTS.md').Length
"lines=$lines bytes=$bytes"
if ($lines -ge 200) { throw 'AGENTS.md must stay under 200 lines' }
if ($bytes -ge 32768) { throw 'AGENTS.md must stay under 32 KiB' }

rg -n 'No write is acknowledged|snapshot isolation|immutable per-commit segments|Luna|Sol|Terra|verification-before-completion' AGENTS.md

rg -n -i '\.opencode|opencode-go|@architect|@coder|@reviewer|context7|caveman|append-only delta log' AGENTS.md
if ($LASTEXITCODE -eq 0) { throw 'stale active guidance remains in AGENTS.md' }
if ($LASTEXITCODE -ne 1) { throw 'rg failed while scanning AGENTS.md' }
```

Expected:

- line/byte limits pass;
- the positive scan finds the named invariants, roles, and verification workflow;
- the stale-reference scan exits 1 with no matches.

- [ ] **Step 4: Review only the Task 1 diff**

Run:

```powershell
git diff -- AGENTS.md
git diff --check -- AGENTS.md
git status --short
```

Expected: only `AGENTS.md` changed during this task; all pre-existing dirty entries remain present.
Do not stage or commit.

---

### Task 2: Add project runtime configuration and operational README

**Files:**

- Create: `.codex/config.toml`
- Create: `.codex/README.md`
- Do not modify: `AGENTS.md`, `.gitignore`, or any other file in this task

**Interfaces:**

- Consumes: Task 1's role names and workflow contract.
- Produces: trusted-project Luna defaults and explicit multi-agent settings consumed by Task 3's
  custom agents; human setup/diagnostic guidance that distinguishes committed project files from
  machine-local state.

- [ ] **Step 1: Re-check scope and create the configuration directory only if still absent**

Run:

```powershell
git status --short
if (Test-Path -LiteralPath '.codex') { throw '.codex appeared after baseline; inspect before writing' }
```

Then create `.codex/config.toml` and `.codex/README.md` with one `apply_patch` call. The patch may
create the directories implicitly; do not use shell file-write commands.

- [ ] **Step 2: Create `.codex/config.toml` with this exact content**

```toml
# Shared defaults for trusted Strata checkouts. Personal auth, provider,
# profile, telemetry, notification, connector, and plugin state stays in the
# user's Codex home and must never be added here.
model = "gpt-5.6-luna"
model_reasoning_effort = "medium"
sandbox_mode = "workspace-write"
approval_policy = "on-request"

[features]
multi_agent = true

[agents]
enabled = true
max_concurrent_threads_per_session = 4
interrupt_message = true
```

- [ ] **Step 3: Create `.codex/README.md` with this exact content**

````markdown
# Strata Codex setup

This directory contains the repository's shared Codex runtime defaults and custom agents. Root
`AGENTS.md` remains the durable engineering and workflow contract.

## Activation

Codex loads project `.codex/` layers only for a trusted repository. Open Strata as the primary
project folder and trust it when prompted. Start a fresh task/session after changing these files so
the configuration and instruction chain are rebuilt.

Do not set `CODEX_HOME` to this directory. Codex discovers project configuration automatically;
using the committed `.codex/` directory as Codex home would mix machine-local auth, sessions, cache,
and plugin state into the repository.

## Roles

- `luna`: fast read-only triage and dispatch.
- `sol`: architecture, specifications, implementation plans, and independent review mode.
- `terra`: plan-bound implementation and fresh verification.

The normal path is Luna → Sol → Terra → fresh Sol review → Luna final gate. Root `AGENTS.md` defines
the complete handoff contract and the cases where read-only questions or trivial orchestration may
stay with Luna.

The project config keeps the parent session at `workspace-write` so an approved Terra child can
write inside the workspace. Luna is restricted from direct non-trivial implementation by root
guidance; a spawned `luna` custom agent is additionally `read-only` by default. Live permission
choices in the Codex client can further restrict every spawned agent.

## Committed versus local

Committed:

- `.codex/config.toml`
- `.codex/README.md`
- `.codex/agents/luna.toml`
- `.codex/agents/sol.toml`
- `.codex/agents/terra.toml`
- root `AGENTS.md`

Machine-local and never committed:

- trust and approval decisions;
- auth, sessions, memories, caches, personal profiles, and global configuration;
- connector authorization and plugin installation state;
- provider endpoints, credentials, telemetry, notification commands, and machine-specific paths;
- optional diagnostic output under `/.codex-log/`.

## Validate

Parse all committed TOML files:

```powershell
@'
from pathlib import Path
import tomllib

paths = [Path('.codex/config.toml'), *sorted(Path('.codex/agents').glob('*.toml'))]
for path in paths:
    with path.open('rb') as handle:
        tomllib.load(handle)
    print(f'parsed {path}')
'@ | python -
```

Confirm Git tracks the project files and ignores only local diagnostics:

```powershell
git check-ignore -v .codex/config.toml
git check-ignore -v .codex-log/probe.log
git status --short
```

The first command should print nothing and exit 1 (not ignored). The second should show the
`/.codex-log/` rule and exit 0.

Run a fresh read-only Codex smoke test from the repository root:

```powershell
codex exec --sandbox read-only --ask-for-approval never "Report the active project guidance file and summarize the Luna to Sol to Terra workflow in three bullets. Do not modify files."
```

Then verify custom-agent discovery:

```powershell
codex exec --sandbox read-only --ask-for-approval never "Spawn the project-scoped sol agent in review mode. Ask it to report its name and responsibility in one sentence, make no edits, wait for it, then stop."
```

If Codex reports that a configured model is unavailable or a key is unsupported, preserve the exact
error and ask the user whether to remove that role's explicit model so it inherits the parent. Do not
silently substitute a model or weaken sandbox/approval settings.

## Troubleshooting

- Project config missing: verify Strata is trusted and is the primary project folder, then start a
  fresh session.
- Stale instructions: start a new Codex run; project guidance is rebuilt at run/session start.
- Agent missing: confirm the TOML file parses and its `name` is exactly `luna`, `sol`, or `terra`.
- Write denied: inspect the parent task's live permission mode. Parent runtime restrictions can
  constrain child agents even when the child file defaults to `workspace-write`.
- Model unavailable: stop and request a model-inheritance decision; do not edit the configuration
  opportunistically.
````

- [ ] **Step 4: Parse the project config and assert exact values**

Run:

```powershell
@'
from pathlib import Path
import tomllib

path = Path('.codex/config.toml')
with path.open('rb') as handle:
    config = tomllib.load(handle)

assert config['model'] == 'gpt-5.6-luna'
assert config['model_reasoning_effort'] == 'medium'
assert config['sandbox_mode'] == 'workspace-write'
assert config['approval_policy'] == 'on-request'
assert config['features'] == {'multi_agent': True}
assert config['agents'] == {
    'enabled': True,
    'max_concurrent_threads_per_session': 4,
    'interrupt_message': True,
}
print('project config contract verified')
'@ | python -
```

Expected: `project config contract verified`, exit 0.

- [ ] **Step 5: Verify Task 2 scope and content hygiene**

Run:

```powershell
Get-ChildItem -Recurse -File -LiteralPath '.codex' | Select-Object -ExpandProperty FullName
rg -n -i '(api[_-]?key|access[_-]?token|secret|password)\s*=' .codex
if ($LASTEXITCODE -eq 0) { throw 'credential-like assignment found under .codex' }
if ($LASTEXITCODE -ne 1) { throw 'rg failed during credential scan' }
git status --short
```

Expected: only `config.toml` and `README.md` exist under `.codex` at this stage; no credential-like
assignment is found. Do not stage or commit.

---

### Task 3: Define Luna, Sol, and Terra custom agents

**Files:**

- Create: `.codex/agents/luna.toml`
- Create: `.codex/agents/sol.toml`
- Create: `.codex/agents/terra.toml`
- Do not modify: any existing file in this task

**Interfaces:**

- Consumes: Task 1's routing/handoff/review contract and Task 2's multi-agent project settings.
- Produces: three Codex custom-agent layers whose required schema, model/reasoning choices,
  sandbox defaults, authority boundaries, and return formats are exact and independently parseable.

- [ ] **Step 1: Confirm the agent directory is absent before creation**

Run:

```powershell
git status --short
if (Test-Path -LiteralPath '.codex\agents') { throw '.codex/agents already exists; inspect before writing' }
```

Create the three files in one `apply_patch` call. Do not use shell file-write commands.

- [ ] **Step 2: Create `.codex/agents/luna.toml` with this exact content**

```toml
name = "luna"
description = "Fast read-only Strata triage and dispatcher for classifying work, capturing scope and dirty-state evidence, and routing bounded assignments to Sol or Terra."
model = "gpt-5.6-luna"
model_reasoning_effort = "medium"
sandbox_mode = "read-only"
approval_policy = "on-request"
developer_instructions = """
You are Luna, Strata's triage and dispatch agent.

Read root AGENTS.md first and treat it as the repository contract. Start with read-only evidence:
the user's exact objective, git status, active dirty work, controlling docs, and the smallest relevant
source/test surface. Preserve every unrelated change.

Classify the request before routing it:
- Answer and status work may remain with Luna.
- Bugs and failing tests require the exposed systematic-debugging workflow before a fix is proposed.
- Features, behavior changes, multi-file refactors, concurrency/index/storage-format changes, and new
  architecture go to Sol for design/specification and an implementation plan before Terra edits.
- An already-approved, exact plan may go to Terra one bounded task at a time.

Inspect the skills and tools actually exposed in the session. Use relevant Superpowers workflows
when available and read their instructions before acting. Never claim that an unavailable skill,
connector, subagent, or reviewer was used.

Every handoff must include: objective, task type/mode, exact allowed and forbidden files/actions,
dirty-worktree baseline, controlling specs/ADRs/tests, invariants, decisions already made, open
questions, required checks, authority limits, and expected return evidence.

Do not dispatch overlapping writers. Parallelize only independent read-only work. Do not implement a
non-trivial change yourself, broaden scope, commit, stage, push, branch, or open a pull request without
explicit user authority.

After Terra returns, require a fresh Sol review for every non-trivial diff and for every crates/txn or
crates/index change. Have Terra resolve accepted findings and rerun affected checks. Before reporting
completion, inspect the final diff and fresh verification output yourself. Return a concise,
self-contained status with files changed, evidence, deviations, and unresolved concerns.
"""
```

- [ ] **Step 3: Create `.codex/agents/sol.toml` with this exact content**

```toml
name = "sol"
description = "Strata architecture, concurrency, specification, implementation-planning, and independent-review specialist."
model = "gpt-5.6-sol"
model_reasoning_effort = "high"
sandbox_mode = "workspace-write"
approval_policy = "on-request"
developer_instructions = """
You are Sol, Strata's architecture and heavy-planning agent.

Read root AGENTS.md and the Luna handoff first. Confirm your assigned mode: design, planning, or
review. Capture git status and preserve all unrelated user work. Use targeted search and file reads;
never claim to have read more than you inspected.

In design mode:
- Read current architecture, the phase-0 transaction/format spec when storage or transactions are in
  scope, accepted ADRs, current code/tests, relevant Superpowers specs/plans, and pertinent history.
- Use the exposed brainstorming workflow for creative/behavioral design, compare 2-3 approaches, make
  trade-offs explicit, minimize scope, and obtain user approval before implementation planning.
- Write only the explicitly authorized spec/ADR artifacts. Do not implement Rust behavior.

In planning mode:
- Use the exposed writing-plans workflow and its required header/checklist format.
- Name exact files, interfaces, content or code, red/green checks where behavior changes, expected
  outcomes, task dependencies, review gates, rollback, and dirty-worktree constraints.
- Make each task independently reviewable and executable by Terra without rediscovering architecture.

In review mode:
- Make no edits even though the default sandbox permits workspace writes.
- Compare the actual diff and fresh evidence against the approved spec, plan, root AGENTS.md, accepted
  ADRs, current tests, and Strata's durability/snapshot/row-index invariants.
- Lead with concrete findings ordered by severity and cite exact files/lines. Distinguish blocking
  correctness/test gaps from optional suggestions. Return APPROVE only when no blocking finding
  remains; otherwise return REJECT with required fixes and verification.

Inspect the skills/tools actually exposed and follow relevant Superpowers instructions. Do not claim
unavailable capabilities. Do not restore deleted migration artifacts, add dependencies, change
architecture, commit, stage, push, branch, or open a pull request without explicit authority.

Return to Luna with artifact paths or review verdict, chosen decisions, coverage/verification map,
deviations, unresolved risks, and whether Terra can safely execute without new design work.
"""
```

- [ ] **Step 4: Create `.codex/agents/terra.toml` with this exact content**

```toml
name = "terra"
description = "Plan-bound Strata implementation worker for one approved task at a time, with TDD, scope preservation, and fresh verification evidence."
model = "gpt-5.6-terra"
model_reasoning_effort = "high"
sandbox_mode = "workspace-write"
approval_policy = "on-request"
developer_instructions = """
You are Terra, Strata's execution worker.

Execute exactly one approved Sol plan task from Luna's handoff. Before editing, read root AGENTS.md,
the controlling spec, the complete assigned task, every file you may modify, and the interfaces it
consumes/produces. Record git status and the relevant pre-edit diff. Preserve all unrelated changes
and stop if the planned target unexpectedly contains user work that the task did not account for.

Use the implementation workflow and skills actually exposed in the session. For a feature or bug fix,
follow test-driven development: create a discriminating failing test, run it and confirm the expected
failure, implement the smallest fix, rerun to green, then refactor without changing behavior. For
configuration/documentation tasks, run the exact syntax, discovery, stale-reference, credential,
scope, and diff checks in the plan.

Do not redesign requirements or silently choose between contradictory sources. Report the conflict to
Luna when the plan cannot be followed exactly or when new architecture/user authority is required.
Do not touch forbidden files, restore deleted migration artifacts, add dependencies, weaken tests or
invariants, use destructive Git commands, commit, stage, push, branch, or open a pull request without
explicit user authority.

Run every task-specific check fresh and read its full output. Never infer success from an earlier run
or another agent's report. If a check fails because of pre-existing dirty work outside scope, report
the exact evidence and do not edit that work.

Return to Luna with: files changed, concise diff summary, commands run with exit codes/failure counts,
red-green evidence when applicable, plan deviations, and unresolved blockers. Do not declare the
whole feature complete. Remain available for Sol review findings; apply accepted fixes within the
original scope and rerun all affected checks before returning again.
"""
```

- [ ] **Step 5: Parse and validate all three custom-agent schemas**

Run:

```powershell
@'
from pathlib import Path
import tomllib

expected = {
    'luna.toml': ('luna', 'gpt-5.6-luna', 'medium', 'read-only'),
    'sol.toml': ('sol', 'gpt-5.6-sol', 'high', 'workspace-write'),
    'terra.toml': ('terra', 'gpt-5.6-terra', 'high', 'workspace-write'),
}

root = Path('.codex/agents')
actual_files = {path.name for path in root.glob('*.toml')}
assert actual_files == set(expected), (actual_files, set(expected))

for filename, (name, model, effort, sandbox) in expected.items():
    path = root / filename
    with path.open('rb') as handle:
        data = tomllib.load(handle)
    assert data['name'] == name
    assert data['model'] == model
    assert data['model_reasoning_effort'] == effort
    assert data['sandbox_mode'] == sandbox
    assert data['approval_policy'] == 'on-request'
    assert isinstance(data['description'], str) and data['description'].strip()
    assert isinstance(data['developer_instructions'], str) and data['developer_instructions'].strip()
    print(f'verified {filename}')
'@ | python -
```

Expected: exactly three `verified ...` lines and exit 0.

- [ ] **Step 6: Verify role-contract coverage and Task 3 scope**

Run:

```powershell
rg -n 'git status|Superpowers|unavailable|handoff|Do not|verification|review' .codex/agents
rg -n 'design mode|planning mode|review mode' .codex/agents/sol.toml
rg -n 'test-driven development|one approved Sol plan task|Do not declare' .codex/agents/terra.toml
Get-ChildItem -File -LiteralPath '.codex\agents' | Sort-Object Name | Select-Object Name,Length
git status --short
```

Expected: all role-contract searches match; exactly `luna.toml`, `sol.toml`, and `terra.toml` exist.
Do not stage or commit.

---

### Task 4: Add the local-log ignore, validate the complete setup, and pass review

**Files:**

- Modify: `.gitignore` (two added lines plus one comment, preserving all existing content)
- Verify only: `AGENTS.md`, `.codex/config.toml`, `.codex/README.md`,
  `.codex/agents/luna.toml`, `.codex/agents/sol.toml`, `.codex/agents/terra.toml`
- Do not modify: any other file

**Interfaces:**

- Consumes: every output from Tasks 1-3 and the original dirty baseline.
- Produces: one complete, syntax-checked, scope-audited, Codex-smoke-tested, independently reviewed
  project configuration ready for user review. This task does not stage or commit it.

- [ ] **Step 1: Re-read the existing user `.gitignore` diff before editing**

Run:

```powershell
git diff -- .gitignore
Get-Content -Raw -LiteralPath '.gitignore'
```

Expected: the user's `skills-lock.json` comment/pattern is still present exactly as captured in Task
1. If it changed during execution, stop and reconcile with the user rather than overwriting it.

- [ ] **Step 2: Add only the Codex diagnostic-log ignore block**

Use `apply_patch` to insert this exact block immediately after the existing
`skills-lock.json` line and before the Cargo section:

```gitignore

# Codex machine-local diagnostic logs
/.codex-log/
```

Do not reorder, normalize, or rewrite any other `.gitignore` line.

- [ ] **Step 3: Parse all TOML and assert the complete cross-file contract**

Run:

```powershell
@'
from pathlib import Path
import tomllib

config_path = Path('.codex/config.toml')
agent_paths = sorted(Path('.codex/agents').glob('*.toml'))
assert [path.name for path in agent_paths] == ['luna.toml', 'sol.toml', 'terra.toml']

with config_path.open('rb') as handle:
    config = tomllib.load(handle)

agents = {}
for path in agent_paths:
    with path.open('rb') as handle:
        data = tomllib.load(handle)
    for required in ('name', 'description', 'developer_instructions'):
        assert isinstance(data.get(required), str) and data[required].strip(), (path, required)
    assert data['name'] not in agents, data['name']
    agents[data['name']] = data

assert set(agents) == {'luna', 'sol', 'terra'}
assert config['model'] == agents['luna']['model'] == 'gpt-5.6-luna'
assert config['model_reasoning_effort'] == agents['luna']['model_reasoning_effort'] == 'medium'
assert config['sandbox_mode'] == 'workspace-write'
assert config['approval_policy'] == 'on-request'
assert config['features']['multi_agent'] is True
assert config['agents']['enabled'] is True
assert config['agents']['max_concurrent_threads_per_session'] == 4
assert config['agents']['interrupt_message'] is True
assert agents['luna']['sandbox_mode'] == 'read-only'
assert agents['sol']['model'] == 'gpt-5.6-sol'
assert agents['terra']['model'] == 'gpt-5.6-terra'
assert agents['sol']['model_reasoning_effort'] == 'high'
assert agents['terra']['model_reasoning_effort'] == 'high'
print('all TOML and cross-file contracts verified')
'@ | python -
```

Expected: `all TOML and cross-file contracts verified`, exit 0.

- [ ] **Step 4: Check instruction size, stale active references, and credentials**

Run:

```powershell
$lines = (Get-Content -LiteralPath 'AGENTS.md' | Measure-Object -Line).Lines
$bytes = (Get-Item -LiteralPath 'AGENTS.md').Length
"AGENTS.md lines=$lines bytes=$bytes"
if ($lines -ge 200) { throw 'AGENTS.md must stay under 200 lines' }
if ($bytes -ge 32768) { throw 'AGENTS.md must stay under 32 KiB' }

rg -n -i '\.opencode|opencode-go|@architect|@coder|@reviewer|context7|caveman|append-only delta log' AGENTS.md .codex
if ($LASTEXITCODE -eq 0) { throw 'stale OpenCode-only guidance remains in active configuration' }
if ($LASTEXITCODE -ne 1) { throw 'rg failed during stale-reference scan' }

rg -n -i '(api[_-]?key|access[_-]?token|secret|password)\s*=' .codex
if ($LASTEXITCODE -eq 0) { throw 'credential-like assignment found under .codex' }
if ($LASTEXITCODE -ne 1) { throw 'rg failed during credential scan' }
```

Expected: size checks pass and both negative scans return no matches.

- [ ] **Step 5: Check line hygiene for tracked and untracked implementation files**

Run:

```powershell
git diff --check -- AGENTS.md .gitignore

@'
from pathlib import Path

paths = [
    Path('AGENTS.md'),
    Path('.gitignore'),
    Path('.codex/config.toml'),
    Path('.codex/README.md'),
    Path('.codex/agents/luna.toml'),
    Path('.codex/agents/sol.toml'),
    Path('.codex/agents/terra.toml'),
]
for path in paths:
    text = path.read_text(encoding='utf-8')
    assert text.endswith('\n'), f'{path} lacks final newline'
    bad = [index for index, line in enumerate(text.splitlines(), 1) if line.rstrip() != line]
    assert not bad, f'{path} has trailing whitespace on lines {bad}'
    print(f'clean {path}')
'@ | python -
```

Expected: tracked diff check exits 0; all seven files print `clean ...`.

- [ ] **Step 6: Verify Git ignore behavior**

Run each command and inspect its exit code:

```powershell
git check-ignore -q .codex/config.toml
"config_ignore_exit=$LASTEXITCODE"
if ($LASTEXITCODE -ne 1) { throw '.codex/config.toml must not be ignored' }

git check-ignore -q .codex/agents/sol.toml
"agent_ignore_exit=$LASTEXITCODE"
if ($LASTEXITCODE -ne 1) { throw '.codex agent files must not be ignored' }

git check-ignore -v .codex-log/probe.log
"log_ignore_exit=$LASTEXITCODE"
if ($LASTEXITCODE -ne 0) { throw '.codex-log must be ignored' }
```

Expected: config and agent checks exit 1; the log check exits 0 and cites the new `.gitignore` rule.

- [ ] **Step 7: Run the Codex semantic smoke tests in a fresh process**

Run from the repository root:

```powershell
codex exec --sandbox read-only --ask-for-approval never "Report the active project guidance file and summarize the Luna to Sol to Terra workflow in three bullets. Do not modify files."
```

Expected: exit 0; output identifies root `AGENTS.md` and accurately summarizes the role chain.

Then run:

```powershell
codex exec --sandbox read-only --ask-for-approval never "Spawn the project-scoped sol agent in review mode. Ask it to report its name and responsibility in one sentence, make no edits, wait for it, then stop."
```

Expected: exit 0; the spawned agent identifies itself as `sol`, describes architecture/planning or
review responsibility, and makes no file changes.

If either command cannot execute because this managed sandbox denies `codex.exe`, record that exact
failure and run the commands from a fresh normal Codex CLI/app terminal. If the project is untrusted,
trust it through the normal Codex UI and rerun. If a model/key error remains, stop for user direction;
do not substitute config values.

- [ ] **Step 8: Dispatch a fresh Sol review with this exact brief**

Luna dispatches `sol` in review mode with:

```text
Mode: review. Do not edit any file.

Review the Codex project-configuration implementation against:
- docs/superpowers/specs/2026-08-01-codex-project-configuration-design.md
- docs/superpowers/plans/2026-08-01-codex-project-configuration-plan.md
- root AGENTS.md

Review only these implementation files:
- AGENTS.md
- .gitignore
- .codex/config.toml
- .codex/README.md
- .codex/agents/luna.toml
- .codex/agents/sol.toml
- .codex/agents/terra.toml

Check: Codex TOML schema/required fields; model/reasoning/sandbox choices; Luna→Sol→Terra routing and
handoff contract; reviewer/verification gate; current Strata invariants (immutable segments, snapshot
isolation, typed conflicts, no silent buffering); committed-vs-local boundary; absence of credentials
or unavailable-tool claims; stale OpenCode-only instructions; dirty-worktree preservation; and scope.

Return APPROVE only if no blocking issue remains. Otherwise return REJECT with severity-ordered,
file-and-line-specific required fixes and exact re-verification. Do not make fixes yourself.
```

Expected: Sol returns `APPROVE`, or concrete findings. If findings are returned, Terra applies only
accepted in-scope fixes and repeats Steps 3-8 from the beginning. Do not mark the task ready on a
`REJECT` verdict.

- [ ] **Step 9: Perform the final scope and dirty-baseline audit**

Run:

```powershell
git status --short --branch
git diff --stat
git diff -- AGENTS.md .gitignore
Get-ChildItem -Recurse -File -LiteralPath '.codex' | Sort-Object FullName | Select-Object FullName,Length
```

Compare against Task 1 Step 1's baseline. The only implementation deltas added by Terra must be:

```text
M  AGENTS.md
M  .gitignore
?? .codex/README.md
?? .codex/config.toml
?? .codex/agents/luna.toml
?? .codex/agents/sol.toml
?? .codex/agents/terra.toml
```

The two planning documents may already be untracked from the planning session and are not Terra
implementation drift. Every pre-existing Rust/doc deletion/modification/untracked entry must still be
present and untouched. If any unrelated file changed during execution, stop and investigate; do not
hide it with a Git cleanup command.

- [ ] **Step 10: Return the implementation handoff without committing**

Terra returns to Luna:

```text
Files changed:
- AGENTS.md
- .gitignore
- .codex/README.md
- .codex/config.toml
- .codex/agents/luna.toml
- .codex/agents/sol.toml
- .codex/agents/terra.toml

Verification:
- TOML/cross-file assertions: report the command's exit code and exact success/failure summary.
- size/stale-reference/credential scans: report the command's exit code and exact success/failure summary.
- whitespace and ignore checks: report the command's exit code and exact success/failure summary.
- Codex instruction smoke: report the command's exit code and exact success/failure summary.
- Codex Sol discovery smoke: report the command's exit code and exact success/failure summary.
- fresh Sol review: report `APPROVE` or list every unresolved finding.

Scope:
- unrelated pre-existing dirty changes preserved: cite the baseline/final-status comparison.
- deviations/blockers: state `none` or describe the exact issue.
- staged/committed/pushed: no
```

Populate every report line with actual evidence; do not leave any line generic in the delivered
handoff. Luna independently reads the diff/output before reporting readiness to the user.

## Plan completion conditions

Terra may hand the work back as ready for Luna only when:

- all seven implementation files exactly satisfy this plan or an explicitly approved amendment;
- all Task 4 syntax, size, hygiene, ignore, stale-reference, credential, and scope checks pass;
- both Codex semantic smoke tests pass in an environment that can execute Codex;
- a fresh Sol review returns `APPROVE`;
- no unrelated file was modified or removed; and
- nothing was staged, committed, pushed, branched, or published.

If the only remaining issue is account/workspace model availability or inability to execute the
Codex smoke test outside this managed sandbox, the implementation is not declared ready: report the
exact external blocker and request user direction.
