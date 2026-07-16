---
name: ship
description: Open a PR for the current branch after running verification. Drafts a clean conventional-commit-style PR title and body.
argument-hint: "[--draft]"
---

# /ship — Open a pull request

Ship the current branch as a PR. The user expects this command to do the boring parts (verify, push, draft PR body) so they can review and hit merge.

## Steps

1. **Sanity check** — confirm we're not on `main` and there are commits ahead of `origin/main`. If on `main`, refuse and tell the user to branch first.

2. **Run /verify** — build + tests + clippy + format must all pass. If the branch touches `crates/txn/` or `crates/index/`, the loom pass must also be green. If anything fails, stop and report.

3. **Review** — dispatch the `reviewer` subagent (Opus 4.8) against the full diff. Resolve or explicitly accept every `Critical`/`Important` finding before continuing — a `REQUEST_CHANGES` verdict blocks shipping until addressed.

4. **Push** — `git push -u origin HEAD` (only if branch isn't already tracking remote).

5. **Draft PR title and body** based on the commits between `main` and `HEAD`:
   - Title: conventional-commit style, ≤70 chars
   - Body: Summary (bullets), Test plan (checklist), any flagged risks

6. **Create PR** with `gh pr create` — pass `--draft` if the flag is set.

7. **Report the URL.**

## PR body template

```markdown
## Summary
- <what changed, 1-3 bullets>
- <why>

## Test plan
- [ ] <how to verify>
- [ ] <edge case to check>
- [ ] <loom pass, if crates/txn/ or crates/index/ touched>

## Notes
<anything reviewers should pay attention to — on-disk format changes, isolation-level implications, index format changes>
```

## Rules

- Never push directly to `main`.
- Never force-push unless the user asked.
- If verification fails, do NOT push or open the PR — fix first.
- Never skip the Opus 4.8 review step, even under time pressure — it's the mandatory gate, not an optional nicety.
- Include any on-disk format changes, manifest schema changes, or isolation-level implications in the Notes section explicitly — these are the hardest things to walk back once shipped.
