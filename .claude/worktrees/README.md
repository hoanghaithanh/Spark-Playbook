# Parallel-session worktrees

This directory holds one git worktree per Claude Code session working a separate GitHub issue at
the same time. Contents here are gitignored (except this file) — each worktree is a full checkout,
not something to commit.

## Convention

- **One worktree = one GitHub issue = one branch = one session.** Don't reuse a worktree across
  unrelated issues, and don't have two sessions share one worktree.
- **Directory naming:** `.claude/worktrees/issue-<N>-<slug>`, e.g. `.claude/worktrees/issue-65-kafka-consumers-groups`.
- **Branch naming:** follow `.claude/GITHUB_CONVENTIONS.md`'s existing `type/issue-id-short-description`
  format, e.g. `feature/65-kafka-consumers-groups` — not a `worktree-issue-N-...` variant. (Earlier
  worktrees drifted from this; new ones should match the documented convention.)
- **Creating one:**
  ```bash
  git worktree add .claude/worktrees/issue-<N>-<slug> -b <type>/<N>-<slug> origin/main
  ```
- **Docker cluster actions from a worktree:** before spawning, killing, or tearing down the
  `sparkpb` cluster, confirm ownership per CLAUDE.md's existing rule (`docker inspect <container>
  --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}'`) — a live cluster
  may belong to a concurrent session's worktree, not yours.
- **Cleanup after merge:**
  ```bash
  git worktree remove .claude/worktrees/issue-<N>-<slug>
  git branch -d <type>/<N>-<slug>
  ```

See `.claude/GITHUB_CONVENTIONS.md` ("Working in parallel (worktrees)") for the rebase and
shared-doc-editing discipline that goes with this.
