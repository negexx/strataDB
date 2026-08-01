# Codex Project Configuration — Design

**Date:** 2026-08-01
**Status:** Approved for implementation planning by the user-provided objective
**Scope:** Project guidance and Codex configuration only. This design does not authorize edits to
Rust source, existing technical documents, `.opencode/`, or any machine-global Codex state.

## 1. Goal

Establish a portable, project-local Codex setup for Strata that:

- replaces the stale OpenCode-specific operating guidance in root `AGENTS.md` without weakening
  Strata's database invariants;
- uses a Luna (triage/dispatch) → Sol (architecture/specification/review) → Terra (plan execution)
  workflow;
- uses current, supported Codex configuration layers under `.codex/`;
- makes relevant exposed Superpowers workflows the default process without claiming tools or skills
  that are not available; and
- remains safe to implement in the repository's already-dirty worktree.

The implementation deliverable is configuration and guidance, not a change to Strata's runtime
behavior.

## 2. Evidence inspected

This design is grounded in targeted repository and history inspection rather than an assumed project
shape.

### 2.1 Root and build configuration

- `AGENTS.md`: current durable guidance, still describes OpenCode and names deleted `.opencode/`
  agents, rules, Caveman, and Context7.
- `Cargo.toml`: Rust 2024 workspace with nine members (`storage`, `txn`, `index`, `query`,
  `bindings`, `cli`, `chaos-worker`, `bench`, and `tests/sim`), Rust 1.90, workspace clippy and Rust
  lints, and release LTO settings.
- `rust-toolchain.toml`: pinned Rust 1.90 with `clippy` and `rustfmt`.
- `rustfmt.toml`, `deny.toml`, crate manifests, and `cargo metadata --no-deps`: confirmed crate
  boundaries, features, test targets, benchmark targets, and the separate fuzz workspace.
- `.github/workflows/ci.yml`: build, workspace tests, `parallel-insert` tests, the crate-scoped
  `strata-index` loom binary, clippy, formatting, docs, and cargo-deny are current CI gates.

### 2.2 Current architecture and invariants

- `docs/architecture.md`: current layered architecture, roadmap, non-goals, and the accepted
  segmented immutable index direction.
- `docs/design/phase-0-transaction-and-format-spec.md`: conflict definition, transaction boundary,
  durability protocol, row-id lifecycle, and its in-place amendments for the segmented index.
- `docs/conventions.md`, `docs/FUTURE.md`, scope addenda, and `docs/how-strata-works.md`: engineering
  conventions and deliberately deferred/refused scope.
- `docs/decisions/0001` through `0008`: including the Rust-over-C++ reversal, snapshot-isolation
  decision, proposed-but-unaccepted group commit, and accepted segmented-index layout. Files
  `0001` through `0006` are currently untracked user work and must remain untouched.
- Current and historical `docs/superpowers/specs/` and `docs/superpowers/plans/`: inventory,
  headers, task maps, amendments, and representative current plans were inspected. Older documents
  intentionally preserve superseded `.claude`/`.opencode` paths or pre-S1 mechanisms; they are
  historical evidence, not automatically the current implementation contract.

### 2.3 Code and tests

Targeted module/API/test inspection covered:

- `crates/storage`: manifest/versioning, local backend abstraction, data files, encoding, statistics,
  typed errors, crash checkpoints, conformance tests, and crash-abort integration tests;
- `crates/txn`: `Dataset`, `Transaction`, snapshots, commit log, row-id allocation, typed conflicts,
  normal concurrency tests, and the loom models in `dataset.rs`;
- `crates/index`: the from-scratch HNSW graph, node/slot structures, immutable segment format,
  readers/writers/segment set, filtered search, normal tests, and loom coverage;
- `crates/query`: predicates, compound predicates, pruning, predicate keys, group-by, and property
  tests;
- `crates/bindings`, `crates/cli`, and `crates/chaos-worker`: the current minimal binding stub,
  CLI surface, process-abort workload, acknowledgement protocol, and live reader checks;
- integration tests under `crates/*/tests` and `tests/sim`, plus benchmark and fuzz target inventory.

This inspection matters to agent behavior because the current implementation uses immutable,
manifest-listed per-commit index segments. The root `AGENTS.md` phrase “append-only delta log” is
stale and must not be carried into the Codex guidance.

### 2.4 Migration state and Git history

- The branch is `main`, ahead of `origin/main` by two commits, with substantial user changes.
- All tracked `.opencode/` agents, commands, rules, and the historical Caveman files are deleted in
  the worktree. `opencode.json` and `skills-lock.json` are also deleted.
- `.gitignore` has an existing unstaged user addition that ignores `skills-lock.json`.
- Rust and architecture files are modified, and several decision/design files are untracked.
- Commit `4b30fc0` migrated an earlier `.claude/` setup to `.opencode/`; commit `b2b1682` then
  rewrote many stale path references to `.opencode/`. The present deletions are therefore an
  in-progress second migration, not files to restore.
- HEAD's `opencode.json` embedded a Context7 credential. The new project configuration must contain
  no credential, token, provider, auth, telemetry, or machine-specific path.

### 2.5 Current Codex contract

The current official Codex manual was fetched and checked for configuration, `AGENTS.md`, and
subagent behavior. It confirms:

- root and nested `AGENTS.md` files are durable project instruction layers;
- trusted repositories may provide `.codex/config.toml` project overrides;
- project custom agents are standalone `.codex/agents/*.toml` files;
- each custom agent requires `name`, `description`, and `developer_instructions` and may override
  model, reasoning effort, sandbox, and other supported configuration keys;
- project `.codex/` layers are skipped when the repository is not trusted;
- multi-agent support is stable and can be made explicit through `[features].multi_agent` and
  `[agents].enabled`; and
- parent live permission choices can constrain spawned agents even when an agent file has a
  different default.

The locally installed `codex.exe` could be located but could not be executed from this managed
planning sandbox (`Access denied`), including after a scoped escalation. The implementation plan
therefore includes a semantic smoke test for Terra to run in the execution environment; TOML syntax
is independently testable with Python 3.14's `tomllib`, which is available here.

## 3. Source-of-truth precedence

The new guidance will define this precedence for engineering work:

1. The user's current request and explicit approvals.
2. Root or closer-scoped `AGENTS.md` instructions.
3. Accepted current ADRs and the living phase-0 transaction/format specification.
4. Current source code, tests, `Cargo.toml`, toolchain, and CI configuration.
5. Current design specifications and amendments that match the checked-out implementation.
6. Historical plans/specifications and Git history, used as rationale but not copied blindly.

When two documents disagree about mechanism, agents must identify which one is superseded and verify
against current code/tests. In particular:

- ADR 0005 supersedes ADRs 0002 and 0004 for language/toolchain.
- ADR 0008 supersedes ADR 0007 for index layout.
- immutable manifest-listed segments supersede the old index delta-log/shared-graph mechanism.
- ADR 0006 is proposed, not accepted or implemented.
- snapshot isolation remains the accepted isolation level; no agent may “upgrade” it to
  serializability as a cleanup.

This approach preserves historical records while stopping stale mechanics from propagating into new
work.

## 4. Approaches considered

### 4.1 Recommended: layered project guidance plus custom agents

Keep shared engineering truth in root `AGENTS.md`; add a small trusted-project `.codex/config.toml`;
define Luna, Sol, and Terra as project-scoped custom agents; document activation and machine-local
boundaries in `.codex/README.md`.

This is the narrowest approach that provides durable repository guidance, explicit models/reasoning,
multi-agent routing, and sandbox defaults while remaining portable with the repository.

### 4.2 AGENTS-only

An `AGENTS.md`-only migration would remove stale OpenCode prose but could not provide custom-agent
discovery, per-role model/reasoning defaults, or per-role sandbox intent. It is simpler but does not
meet the approved Luna/Sol/Terra objective.

### 4.3 Project-local plugin/skill vendoring

Vendoring Superpowers or recreating the deleted Caveman skill inside the repository would add an
independent packaging and update lifecycle. Superpowers is already exposed by the installed plugin
in this session, while Caveman is not exposed. The repository should request relevant available
skills by behavior, not copy machine-owned plugin assets or pretend unavailable skills exist.

This approach is rejected as unnecessary scope expansion.

## 5. Exact committed tree

The implementation creates exactly this project-local Codex tree:

```text
.codex/
├── README.md
├── config.toml
└── agents/
    ├── luna.toml
    ├── sol.toml
    └── terra.toml
```

No hooks, MCP servers, rules, prompts, project-local skills, plugin manifests, auth files, profile
files, or session data are part of this design.

The full committed file set for implementation is:

```text
.gitignore
AGENTS.md
.codex/README.md
.codex/config.toml
.codex/agents/luna.toml
.codex/agents/sol.toml
.codex/agents/terra.toml
```

The two design/plan documents from this planning task are already separate deliverables and are not
part of Terra's configuration edit set.

## 6. Configuration-layer design

### 6.1 Root `AGENTS.md`

`AGENTS.md` remains the compact, repository-wide engineering contract. It will:

- identify itself as Codex project guidance rather than OpenCode memory;
- retain Strata's mission, crate map, commands, safety rules, and non-negotiable transaction/index
  invariants;
- correct the index architecture from a delta log to immutable per-commit segments published in the
  same manifest transaction as row data;
- route agents to `docs/architecture.md`, the phase-0 spec, conventions, ADRs, and current tests;
- describe the Luna → Sol → Terra flow and exact handoff/review gate;
- require relevant exposed Superpowers workflows and forbid claims about unavailable tools;
- replace the hard Context7/Caveman assumptions with capability-aware documentation guidance;
- preserve dirty-worktree and non-destructive Git rules; and
- keep verification proportional for documentation/config-only changes while retaining full Cargo,
  feature, loom, and chaos gates for Rust changes that affect them.

It will not point at deleted `.opencode` paths or rewrite historical documents.

### 6.2 `.codex/config.toml`

The project config will select Luna's current documented model for the primary session defaults,
enable multi-agent support explicitly, use a bounded concurrency cap, and keep the normal safe
workspace/approval posture:

- `model = "gpt-5.6-luna"`
- `model_reasoning_effort = "medium"`
- `sandbox_mode = "workspace-write"`
- `approval_policy = "on-request"`
- `[features].multi_agent = true`
- `[agents].enabled = true`
- `[agents].max_concurrent_threads_per_session = 4`
- `[agents].interrupt_message = true`

The primary sandbox remains `workspace-write` because parent live permission policy constrains
spawned agents; making Luna's parent session read-only could prevent Terra from executing approved
changes. Luna is behaviorally restricted by `AGENTS.md` and its role instructions from directly
implementing non-trivial work. A separately spawned Luna agent is hard-defaulted to read-only.

No provider, auth, notification, telemetry, profile, global MCP, or Windows-machine setting belongs
in this file.

### 6.3 `.codex/README.md`

The README is operational documentation, not another instruction layer. It explains:

- trust is required for Codex to load project `.codex/` layers;
- root `AGENTS.md` is the durable behavioral contract;
- the role chain and review loop;
- the committed versus machine-local boundary;
- no `CODEX_HOME` override should point at the repository's `.codex/` directory; and
- the smoke-test and troubleshooting commands.

It must not promise that every Codex account exposes every named model. If a configured model is not
available, execution stops with the exact error and asks the user whether to inherit the parent
model; it does not silently substitute another model.

## 7. Agent responsibilities and defaults

| Agent | Model | Effort | Default sandbox | Responsibility |
|---|---|---|---|---|
| Luna | `gpt-5.6-luna` | `medium` | `read-only` when spawned | Fast intake, repository/status triage, task classification, dispatch, progress consolidation, and final gate ownership. |
| Sol | `gpt-5.6-sol` | `high` | `workspace-write` | Architecture, concurrency reasoning, design specs, ADR/plan work, scope decomposition, and an independent review mode. |
| Terra | `gpt-5.6-terra` | `high` | `workspace-write` | Execute one approved plan/task, apply TDD when code behavior changes, preserve scope, run task checks, and return evidence. |

### 7.1 Luna

Luna owns the primary conversation and is the only role that consolidates final status for the user.
It begins with read-only evidence: active instructions, `git status`, requested scope, relevant docs,
and whether the task is a question, diagnosis, design, implementation, review, or release action.

Luna may answer read-only questions and perform truly trivial orchestration. A non-trivial behavior
change, multi-file refactor, concurrency change, on-disk format change, or new architecture goes to
Sol before Terra. Luna must not use dispatch as permission to broaden scope or allow simultaneous
writers on overlapping files.

### 7.2 Sol

Sol has three explicit modes selected by the handoff:

1. **Design mode:** inspect current code/docs/history, use brainstorming when exposed and applicable,
   compare approaches, present the design for approval, and write the validated spec.
2. **Planning mode:** use the exposed writing-plans workflow, create exact task boundaries and
   verification, and hand an approved executable plan back to Luna.
3. **Review mode:** make no edits; compare Terra's diff and evidence against the spec, plan,
   `AGENTS.md`, and Strata invariants; return findings or approval.

Sol does not implement Rust behavior while in design/planning mode. It may write only the explicitly
authorized design, plan, or ADR artifacts for that assignment.

### 7.3 Terra

Terra receives an approved plan or one isolated task from it. It:

- re-reads root `AGENTS.md`, the controlling spec, its task, and every file it will modify;
- records the dirty baseline before editing;
- uses test-driven development for features and bug fixes when the skill is exposed;
- makes the smallest plan-conforming change and stops on a requirement conflict rather than
  redesigning silently;
- runs the task's full fresh verification and reports exact outcomes;
- does not commit, push, open a pull request, add dependencies, or change plan/spec scope without
  explicit authority; and
- returns control to Luna for independent review.

## 8. Routing and handoff contract

### 8.1 Normal flow

```text
User request
  → Luna classifies and captures baseline
  → Sol designs/specifies and obtains approval when a design decision is needed
  → Sol writes the implementation plan
  → Luna dispatches one bounded task at a time to Terra
  → Terra implements and verifies the task
  → fresh Sol review for non-trivial work
  → Terra resolves accepted findings and re-verifies
  → Luna runs/reads the final gate and reports evidence
```

Parallel read-only exploration is allowed when tasks are independent. Parallel writes to overlapping
files are forbidden. Sequential task execution is the default for this repository because
`crates/txn`, `crates/index`, manifests, and their tests are tightly coupled.

### 8.2 Luna → Sol handoff

Every handoff includes:

- objective and success criteria;
- task type and requested Sol mode;
- exact in-scope and forbidden files/actions;
- dirty-worktree baseline and ownership notes;
- controlling architecture/spec/ADR/test references;
- known assumptions, decisions already made, and open questions;
- required Superpowers workflow if exposed; and
- expected artifact and return format.

### 8.3 Sol → Luna planning return

Sol returns:

- artifact paths;
- chosen approach and rejected alternatives;
- task dependency/order map;
- exact verification and review gates;
- unresolved risks or user decisions; and
- whether the plan is safe for Terra to execute without new architecture work.

### 8.4 Luna → Terra handoff

Each Terra task contains:

- controlling spec and exact plan task;
- files it may create/modify and files it must not touch;
- inputs/interfaces from prior tasks and outputs later tasks rely on;
- required tests/checks with expected results;
- current dirty baseline and instructions to preserve unrelated changes;
- no-commit/no-push authority unless explicitly changed by the user; and
- the required return evidence.

### 8.5 Terra → Luna return

Terra returns only evidence useful to review:

- files changed;
- tests/checks run, exit codes, and failure counts;
- deviations from the plan, if any;
- remaining concerns/blockers; and
- a concise diff summary. It does not declare the whole task complete.

## 9. Superpowers and capability policy

Agents inspect the skills actually exposed in the current session before acting. When available and
applicable, the workflow is:

- conversation start: `superpowers:using-superpowers`;
- feature or configuration design: `superpowers:brainstorming`;
- bug/test failure: `superpowers:systematic-debugging`;
- multi-step implementation: `superpowers:writing-plans` before edits;
- behavior implementation: `superpowers:test-driven-development`;
- plan execution: `superpowers:subagent-driven-development` or
  `superpowers:executing-plans`, as selected by Luna;
- completion claims: `superpowers:verification-before-completion`;
- branch integration when authorized: `superpowers:finishing-a-development-branch`.

The relevant skill must be read and followed; naming it is not sufficient. If a skill or tool is not
exposed, the agent states that limitation and follows the closest evidence-first fallback.

**Caveman is unavailable in this planning session.** It appears only in the deleted historical
OpenCode setup and is not in the currently exposed skill catalog. The new configuration must not
claim to load or require it. Concise output is handled by direct prompt/role guidance unless a future
session actually exposes Caveman.

Context7 is likewise not exposed in this session. External crate/API questions should use an
available authoritative documentation connector when one is exposed; otherwise use official docs,
installed crate source, and build/test evidence. No project config will hard-code a connector or
credential.

## 10. Review and verification gate

### 10.1 Independent review

- Every non-trivial implementation receives a fresh Sol review after Terra's task verification.
- Changes affecting `crates/txn` or `crates/index` always receive Sol review, even when the diff is
  small.
- Review mode is read-only by instruction: findings first, no hidden fixes.
- Any correctness, durability, snapshot-isolation, row/index atomicity, on-disk compatibility, or
  missing-test finding blocks completion until Terra fixes it or the user explicitly rejects it
  with recorded reasoning.

The three-agent design intentionally reuses Sol for independent review rather than creating a fourth
`reviewer.toml`; the handoff starts a fresh Sol context in review mode so the authoring rationale is
not treated as proof.

### 10.2 Verification by change type

- Guidance/config-only changes: TOML parse, Codex semantic smoke test, instruction/agent discovery,
  stale-reference scan, secret scan, diff whitespace check, scope diff, and dirty-worktree
  comparison.
- Ordinary Rust changes: targeted tests first, then `cargo build --workspace`,
  `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo fmt --check`.
- `parallel-insert` changes: include the feature-specific test and clippy commands from CI.
- Interleaving-sensitive `crates/txn`/`crates/index` changes: add/run a targeted loom model using the
  crate-scoped `cargo rustc ... -- --cfg loom` pattern; never set workspace-wide `RUSTFLAGS=--cfg
  loom`.
- Durability/chaos behavior: run the relevant fast chaos tests and the opt-in thorough tier when the
  plan's exit criterion requires it.
- Documentation-only work does not claim the unrelated dirty Rust worktree is green. If a full
  workspace command is run and fails in pre-existing user changes, report it without editing those
  files.

No success claim is based on an agent report alone; Luna reads the diff and fresh command output.

## 11. Committed versus machine-local state

### 11.1 Committed

- root `AGENTS.md`;
- `.codex/config.toml`;
- `.codex/README.md`;
- `.codex/agents/luna.toml`;
- `.codex/agents/sol.toml`;
- `.codex/agents/terra.toml`; and
- the one `.gitignore` entry for optional project-local Codex diagnostic logs.

These files contain no secrets and use only relative repository paths.

### 11.2 Machine-local and never committed

- trust decisions and permission approvals;
- `~/.codex/config.toml`, profiles, auth state, sessions, memories, caches, plugin installation, and
  connector authorization;
- API keys, provider/base URLs, telemetry destinations, notification commands, and machine-specific
  executable paths;
- generated Codex logs under `/.codex-log/`; and
- any skill/plugin lock or cache produced by a user's personal installation.

Do not point `CODEX_HOME` at the committed project `.codex/` directory. Project layers are discovered
automatically after trust; using the repository as Codex home would mix credentials/session state
with committed configuration and undermine the boundary above.

## 12. `.gitignore` decision

Preserve the user's existing `skills-lock.json` ignore hunk exactly. Add only:

```gitignore
# Codex machine-local diagnostic logs
/.codex-log/
```

Do not ignore `.codex/` or any of the five committed files beneath it. No other ignore pattern is
needed because ordinary Codex auth, cache, session, and plugin data live outside the repository.

## 13. Validation

Implementation is accepted only when fresh evidence confirms:

1. All four TOML files parse with Python `tomllib`.
2. Agent files contain the required `name`, `description`, and `developer_instructions` keys, and
   names are exactly `luna`, `sol`, and `terra`.
3. The project config contains the exact multi-agent and bounded-thread settings in §6.2.
4. Root `AGENTS.md` is under 200 lines and under Codex's default 32 KiB project-instruction budget.
5. No current `AGENTS.md`/`.codex/` file references `.opencode`, OpenCode model IDs, deleted agent
   handles, Context7, or Caveman as an available requirement.
6. No credential-like assignment appears under `.codex/`.
7. `.codex/config.toml` and the agent files are not ignored by Git; `/.codex-log/` is ignored.
8. A fresh Codex read-only smoke test loads root guidance and can discover/spawn `sol`.
9. `git diff --check` passes for the seven implementation files.
10. The final diff is limited to the approved files and preserves all pre-existing dirty changes.

If the Codex smoke test reports an unavailable model or unsupported key, Terra stops with the exact
error. It does not change model names or drop safety settings without user approval.

## 14. Rollback and safety

- Capture `git status --short` and the existing `.gitignore` diff before implementation.
- Use patch-based edits only; never use reset, checkout, clean, or broad file replacement against the
  worktree.
- Never restore deleted `.opencode/`, `opencode.json`, or `skills-lock.json` as part of this work.
- Never edit Rust files, existing technical docs, untracked ADRs/specs, or the user's architecture
  changes while implementing this plan.
- If rollback is requested before commit, reverse only the exact new `.codex/` files and the exact
  `AGENTS.md`/`.gitignore` hunks introduced by this implementation. Re-check the baseline afterward.
- If an implementation file unexpectedly already exists when Terra starts, stop and inspect it;
  do not overwrite it based on this plan.
- Do not stage or commit unless the user explicitly authorizes it. If authorized later, stage only
  the exact approved files and inspect the staged diff before committing.

## 15. Known limitations

- Project `.codex/` loading depends on the user trusting the repository.
- Named model availability can vary by account/workspace policy. The design uses the models in the
  current official Codex manual; unavailable-model fallback requires a user decision.
- This planning session did not expose callable subagent-management tools, so the present design and
  plan receive the requested self-review rather than a dispatched reviewer pass. The implementation
  setup itself explicitly enables multi-agent support for future Luna/Sol/Terra runs.
- Existing source and historical docs still contain `.opencode` references. They are outside this
  task's authorized edit scope. Root guidance will stop routing agents to those paths and will direct
  them to living docs/current source instead; a separate documentation migration can address the
  residual references later if the user requests it.

## 16. Out of scope

- Creating the actual `.codex/` files or editing `AGENTS.md` in this planning session.
- Editing any Rust source, test, benchmark, existing technical document, CI workflow, or deleted
  `.opencode` artifact.
- Installing/migrating plugins, skills, MCP servers, hooks, prompts, or connectors.
- Recreating Caveman or Context7.
- Changing Strata's architecture, transaction semantics, dependencies, model of isolation, Git
  branch, commits, remote state, or pull requests.
