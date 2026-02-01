# Branching model (Helios)

Default branch: **`main`**. Older docs sometimes say `master`; here **`master` is a mirror of `main`** only if you want both names (CI, old scripts). Prefer one primary name to avoid drift.

| Branch        | Role |
|---------------|------|
| **`main`**    | Protected, release-quality. Merges from `develop` after review, or hotfix flows. |
| **`develop`** | Day-to-day integration. Feature branches merge here first. |
| **`production`** | Optional: exactly what you deploy to prod, or tag-based deploys from `main`. Some teams skip this and deploy from `main` + tags. |
| **`feature/<name>`** | Short-lived work (e.g. `feature/wal-framing`). Branch from `develop`, open PR back to `develop`. |
| **`fix/<issue>`** | Small bugfixes, same flow as features. |

Release tags (`v0.1.0`) on `main` are the durable record of what shipped, not fake commit dates.

## Human-led, AI-assisted workflow

- **You** decide scope, architecture, and what gets merged. Use AI for drafts, refactors, or tests; **read the diff** before every commit.
- Prefer **small commits** with a **clear imperative subject** (`Add WAL frame CRC`, not `changes`).
- Avoid single huge “AI dump” commits: split by concern (storage vs config vs tests).
- If a tool wrote a file, your commit message should still say what *you* intended (e.g. “Add property test for compaction invariant”).

## About commit dates

Use your **real** author time for work you did when you did it. Rewriting history with artificial `GIT_AUTHOR_DATE` ranges to simulate months of activity is easy to detect and erodes trust if a reviewer runs `git log --format=fuller` or checks against your story. For a portfolio, a believable narrative is: steady small commits, good messages, and branches/PRs — not a backdated graph.

If you truly did work in Jan–Mar 2026, those commits should already carry those dates on the machine where you committed; do not bulk-rewrite unless you are fixing a one-off misconfiguration and you are transparent about it.

## Quick commands

```bash
# First-time setup after clone
git checkout -b develop main
git push -u origin develop

# Start a feature
git fetch origin
git checkout develop
git pull
git checkout -b feature/your-topic
# ... commit ...
git push -u origin feature/your-topic
# Open PR: feature/your-topic → develop, then develop → main when ready
```

Optional: set `production` to track deploys only:

```bash
git checkout -b production main
git push -u origin production
# CI deploys from production; merge main → production when you release
```
