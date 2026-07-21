# GitHub Conventions (shared reference)

Shared conventions for branching, commits, pull requests, issues, and releases. Referenced by agents (developer, test-engineer, code-reviewer, security-auditor, requirements-analyst, project-manager, devops-engineer) so everyone works against the same rules instead of improvising per-agent.

## Branching Strategy (GitHub Flow)

- `main` is always deployable — production-ready at every commit.
- All work happens on a short-lived branch off `main`, merged back via PR, then deleted. There is no permanent `develop` branch.
- Branch types: `feature/` (new functionality), `bugfix/` (non-urgent fixes), `hotfix/` (urgent production fixes — may skip the usual review depth if explicitly flagged as urgent, but still goes through a PR).

## Branch Naming

- Lowercase letters, words separated by hyphens.
- Format: `type/issue-id-short-description` — include the issue number if one exists.
- Example: `feature/42-user-profile-page`, `hotfix/api-crash` (no issue number available).

## Commit Message Convention (Conventional Commits)

- `feat` — a new feature for the user.
- `fix` — a bug fix for the user.
- `docs` — documentation changes only.
- `style` — formatting, missing semicolons, etc. (no production code change).
- `refactor` — restructuring production code (no bug fix, no new feature).
- `test` — adding or correcting tests.
- `chore` — build tasks, package manager configs, tooling.
- Format: `type(scope): short summary in present tense`
- Example: `feat(auth): add google oauth2 login`

## Working in parallel (worktrees)

This project routinely runs several sessions at once, each in its own `.claude/worktrees/*` git
worktree working a different issue. Two collision classes have caused real problems in practice —
these rules exist to prevent both.

- **Worktree/branch naming:** `.claude/worktrees/issue-<N>-<slug>`, branch `type/issue-id-slug`
  (same format as [Branch Naming](#branch-naming) above — don't invent a variant like
  `worktree-issue-N-...`). See `.claude/worktrees/README.md` for the create/cleanup commands.
- **Rebase before opening a PR**, and periodically on longer-lived branches:
  `git fetch origin && git rebase origin/main`. This surfaces conflicts while they're small and
  fresh in context, instead of at merge time days later.
- **PR-per-issue.** Don't batch multiple issues into one PR — it widens the merge surface and makes
  a rebase conflict harder to resolve correctly.
- **Editing shared docs** (`docs/backlog.md`, `docs/retrospectives.md`): pull/rebase immediately
  before editing, keep the edit scoped to your own row/section, and commit it on its own rather than
  buried inside a large feature diff — a conflict in a one-line commit is trivial to resolve, a
  conflict inside a 400-line feature diff is not.
- **Docker/cluster actions** from a worktree follow CLAUDE.md's ownership-check rule — a running
  `sparkpb` cluster may belong to a different worktree's session.

## Pull Request Etiquette

- Link the corresponding issue with a closing keyword (e.g. `Closes #45`).
- Keep PRs small — roughly under 400 lines of change is a useful guideline, not a hard gate; split larger work into independently reviewable pieces where practical.
- Require at least one independent review before merging (the `code-reviewer` agent's pass counts, but Blockers must be resolved by the developer first).
- All CI checks must pass before merge.

## Issue Tagging & Labels

### Filing an issue
```bash
gh issue create \
  --title "<concise, specific title>" \
  --body "<what, where, repro/impact, suggested next step>" \
  --label "<role-label>,<type-label>" \
  --milestone "<exact milestone title, if one applies>"
```

### Standard labels
- Type: `bug`, `enhancement`, `task`, `security`, `tech-debt`, `question`, `documentation`, `good first issue`, `wontfix`
- Role (who raised it): `from:dev`, `from:qa`, `from:review`, `from:security`, `from:requirements`

### Before filing
- Search first to avoid duplicates: `gh issue list --search "<keywords>" --state all`
- If a matching issue exists, comment on it instead: `gh issue comment <number> --body "..."`

### Rules for all agents
- File issues only within your role's concern (see each agent's own instructions).
- Only attach a milestone if you know the exact title — list them with `gh issue list` context or ask the project-manager agent. Do not invent milestone names.
- Never create, edit, or close milestones — that's the project-manager's job.
- Write issue bodies that a teammate could act on without you: what, where (file/line/endpoint), how to reproduce or why it matters, and a suggested next step.
- Never fabricate an issue number or claim an issue was filed without actually running the command and reporting the resulting URL.

## Release Tagging (Semantic Versioning)

- Format: `vMAJOR.MINOR.PATCH` (e.g. `v1.4.2`).
- `MAJOR` — breaking changes.
- `MINOR` — new backward-compatible functionality.
- `PATCH` — backward-compatible bug fixes.
- Release-level milestones (owned by `project-manager`) should use this format in their title, e.g. `v1.2 — Payments`.
