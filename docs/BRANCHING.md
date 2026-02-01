# Branching model (Helios)

Default branch: **`main`**. Use that name everywhere (CI, GitHub, scripts).

| Branch        | Role |
|---------------|------|
| **`main`**    | Default branch; protected, release-quality. Merges from `develop` (or `hotfix/*` in emergencies) with **`git merge --no-ff`**. |
| **`develop`** | Integration target. `feature/*` and `fix/*` merge here first (use **`--no-ff`** on the PR/merge to keep feature integration visible). |
| **`production`** | Deploy line. Merge **`main` → `production`** with **`--no-ff`** when you promote a build to production. |
| **`feature/<name>`** | Short-lived work. Branch from `develop`, open PR to `develop`. |
| **`fix/<issue>`** | Small bugfixes; same flow as features. |

Release tags (`v0.1.0`) on `main` are the durable record of what shipped, not fake commit dates.

## Genuine commits and merges (non-negotiable)

- **Every commit must have a real diff** (documentation, code, or config that changes). Do **not** use `git commit --allow-empty` to “shape” the history graph.
- **The message must match the diff.** If a commit only edits `docs/`, do not say you changed the WAL; if you only wire one flag, the subject should say that.
- **A merge should combine two lines of work that differ.** If you would merge with **no file change** and no real integration story, use **fast-forward** instead of `--no-ff`. Reserve merge commits for when feature branches (or `develop` vs `main`) actually add commits on one side.
- **Tools:** Optional footers in commit messages (`Co-authored-by`, etc.) are fine if true; do not paste generic “AI generated” stamps that are not part of your project’s convention.

## Why merge commits (not only fast-forwards)

Use **`git merge --no-ff`** when a branch actually brings **new commits** (not the same tree as the target):

- a **feature** or **fix** branch lands in **`develop`**
- **`develop` → `main`** or **`main` → `production`** and the source branch is **not** a simple fast-forward of the same work (e.g. you reviewed a batch of real commits and want a named integration point)

If `develop` and `main` would fast-forward with **identical trees**, prefer **`git merge --ff-only`** (or `git switch` + `reset --hard` after review) so you do not create an empty **merge** story.

That keeps a **readable graph** where each merge bubble is a real integration (e.g. *Merge branch 'feature/xyz' into develop*).

**Inspect history:**

```bash
git log --oneline --graph --decorate -25 --all
```

**GitHub / GitLab:** “Create a merge commit” in PR/MR settings, not “squash” or “rebase only”, if you want the same on the server.

## Clear, divided commits (before you merge)

- **One concern per commit** (e.g. sstable in one commit, config in another), not a single giant “everything” change.
- **Subject line:** imperative, ~50 characters, no trailing period (`Add remote read time bounds`, not `fixed stuff`).
- **Body** (optional): what/why, not a transcript. Helps reviewers and future you.
- **Split** large changes before opening the PR: `git add -p` or separate commits in the feature branch; the merge to `develop` can still be one PR with many commits **or** one squash **only** if the team agrees (squash removes per-commit story).

## Human-led, AI-assisted workflow

- You decide scope and merges; use AI for drafts; **read every diff** before commit.
- If a tool wrote a file, the message should still state **your** intent (e.g. *Add test for max query window*).

## About commit dates

Use **real** author time for the work. Artificial bulk backdating is easy to notice (`git log --format=fuller`) and hurts trust. Steady, small, honest commits plus branches and merge bubbles read better than a faked timeline.

## Promote work between branches (merge, no `branch -f`)

Do **not** `git branch -f develop main` if you want to preserve merge history. Use merges.

### 1) Feature → develop

```bash
git checkout develop && git pull
git merge --no-ff feature/your-topic -m "Merge branch 'feature/your-topic' into develop"
git push origin develop
# delete remote feature when done: git push origin --delete feature/your-topic
```

### 2) develop → main

```bash
git checkout main && git pull
git merge --no-ff develop -m "Merge branch 'develop' into main"
git push origin main
# Keep develop in sync with main:
git checkout develop && git merge main && git push origin develop
```

### 3) main → production

```bash
git checkout production && git pull
git merge --no-ff main -m "Merge branch 'main' into production"
git push origin production
```

## Quick start (clone, feature, PRs)

```bash
git clone <url> && cd Helios
git fetch origin
git checkout develop
git pull
git checkout -b feature/your-topic
# commit(s), then:
git push -u origin feature/your-topic
# Open PR: feature/your-topic → develop
# When ready: PR develop → main (merge commit), then merge main → production as above
```

## Optional: new long-lived branch from `main`

```bash
git checkout -b develop main
git push -u origin develop
```
