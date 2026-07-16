---
name: plan
description: Produce an implementation plan before touching code. Required for any change spanning 3+ files or introducing new architecture.
argument-hint: "<feature or task description>"
model: fable
---

# /plan — Implementation planning

You are about to plan work on **Strata**. The user's intent: `$ARGUMENTS`

Produce a plan, not code. The plan should be the kind of thing a competent teammate could pick up and execute without re-asking the same questions.

## Output format

```markdown
## Goal
<one sentence — what changes after this is done>

## Context I checked
- <files/folders you read, with a one-line takeaway>
- <relevant existing patterns to follow>
- <related ADRs in .claude/docs/decisions/>

## Approach
<2-3 paragraphs explaining the strategy and why this over alternatives>

## Steps
1. <concrete step, file paths included>
2. <next step>
...

## Tests
- <what to assert, and where the test lives — for anything touching crates/txn/ or crates/index/, this must include a concurrency/conflict scenario, not just a happy path>

## Risks / Open questions
- <anything you're unsure about — flag for the user>
```

## Rules

- Read before planning. Don't propose changes to files you haven't read.
- Match existing patterns. If a similar feature already exists, copy its shape.
- No yak shaving — only touch what the task requires.
- For anything touching `crates/txn/` (transaction/conflict layer) or `crates/index/` (vector index delta log), call out review needed — these are the flagship subsystem and mistakes here are the most expensive to undo.
- End with a one-line "ready to execute?" and stop. Don't start coding.

## When to escalate the model

This command runs on Fable 5 by default — planning and architecture is exactly the tier it's for. If Fable 5 isn't available in this environment, fall back to Opus 4.8.

If the plan additionally involves:
- Architectural decisions (isolation level, conflict granularity, on-disk format changes)
- The transaction/conflict layer or vector index delta log
- Ambiguity the user hasn't resolved

…flag that explicitly in the "Risks / Open questions" section so the user knows this plan needed the deepest reasoning tier, not just the default one.
