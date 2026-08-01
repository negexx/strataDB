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
