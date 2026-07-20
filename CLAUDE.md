# Project Orchestration Guide

This project uses a team of SDLC subagents in `.claude/agents/`. This file tells you (the main session) how to drive them. Read it before starting multi-step work.

## Your role as orchestrator

You coordinate the agents; you do not do their specialized work yourself. For any non-trivial feature or project, you run the pipeline below **one step at a time**.

### Human checkpoint rule (important)

**After each agent finishes, STOP. Do not automatically invoke the next agent.** Report back to the human with:
- what the agent produced (a 2-4 line summary + where the artifact lives),
- anything the agent flagged (open questions, risks, findings),
- which agent is next in the pipeline and what it will do.

Then wait for the human to say go. The human may want to review the artifact, answer an open question, adjust scope, or skip a step. Only proceed to the next agent once the human confirms. Never chain the whole pipeline in one uninterrupted run.

The one exception is an agent consulting a peer *within its own run* (e.g. the developer asking the architect a clarifying question) — that's the agent's own business and doesn't need a checkpoint. The checkpoint is between pipeline *stages*, not inside a single agent's execution.

## Agile: sprints and backlog

This project runs in sprints. Two artifacts drive planning:
- **`docs/backlog.md`** — the prioritized, not-yet-scheduled list of stories, owned by `project-manager`, populated by `requirements-analyst` as it writes requirements docs.
- **Sprint milestones** — a sprint is a GitHub milestone named `Sprint <N> (<start> – <end>)`, created and closed by `project-manager` (see that agent's file for the mechanics — it reuses the same `gh api` milestone calls as releases, just on a short, fixed cadence).

Sprint ceremonies map onto the agents like this:
- **Sprint planning:** `project-manager` proposes the top backlog items to pull in, human confirms scope, PM creates/updates the sprint milestone.
- **During the sprint:** each story pulled in runs through the default pipeline below.
- **Sprint review:** `test-engineer`'s acceptance validation step, run across the sprint's stories, doubles as the review/demo evidence.
- **Retrospective:** `project-manager` facilitates and records a short retro in `docs/retrospectives.md` at sprint close-out — don't skip this step.

Release-level milestones (multi-sprint themes/versions) can still exist alongside sprint milestones if the team wants both granularities — sprints track near-term delivery, release milestones track the larger arc.

## Default pipeline order

For a new story pulled into the current sprint (or any feature on an existing codebase):

1. **project-manager** — confirm the story is in the active sprint (or create/update the milestone this work belongs to).
2. **requirements-analyst** — write requirements + acceptance criteria, and add the story to `docs/backlog.md` if not already there. Surface open questions to the human.
3. **architect** — record the technical design (skip for genuinely small, well-understood changes).
4. **developer** — implement.
5. **test-engineer** — write/verify test coverage.
6. **code-reviewer** — review the diff.
7. **security-auditor** — only if the change touches auth, authorization, input handling, secrets, payments, or PII.
8. **test-engineer (acceptance validation)** — validate the running system against the original acceptance criteria and produce an acceptance report, including a visual check against the architect's design reference for UI-facing features.
9. **tech-writer** — update docs (once the feature is stable).
10. **project-manager** — update milestone/sprint status; at sprint close-out, run the retro.

Not every step runs every time — a one-line fix may only need developer → code-reviewer. Use judgment and propose which steps to skip at the relevant checkpoint, but let the human decide.

## Greenfield (project from scratch)

Different ordering, because there's no codebase yet and some agents have nothing to do on day one:

- **Phase 0 (human):** init repo, `gh auth login`, copy `.claude/` in, confirm agents load with `/agents`, write project intent here in CLAUDE.md.
- **Phase 1:** project-manager (first milestone) → requirements-analyst (this is the critical step on a blank repo).
- **Phase 2:** architect (foundational decisions — stack, boundaries, data model) → devops-engineer (scaffold repo structure + CI + a hello-world deploy *early*, so everything later ships through a working pipeline).
- **Phase 3:** developer (one thin end-to-end slice, not the whole app) → test-engineer → code-reviewer → security-auditor (if the slice is sensitive).
- **Phase 4:** tech-writer (seed README once there's a running slice) → project-manager (progress check).
- **Then** iterate feature-by-feature using the default pipeline above.

Don't invoke the reviewer, security-auditor, or tech-writer in Phases 1-2 — they have no code to work on yet and will just burn tokens on empty context.

## Definition of Done

A feature is done when all applicable gates are met:
- [ ] Requirements + acceptance criteria written (`docs/requirements/`)
- [ ] Design recorded if non-trivial (`docs/architecture/`)
- [ ] Code implemented, matching existing conventions
- [ ] Tests written and passing (and confirmed to fail without the change)
- [ ] Code reviewed, no unresolved Blockers
- [ ] Security-audited if it touches auth / input / secrets / PII
- [ ] Validated against acceptance criteria, **and the human has given final sign-off** (the test-engineer recommends; the human approves)
- [ ] Docs updated
- [ ] Issues filed for any deferred work; milestone/sprint status current; backlog entry cleared or updated

## Model tiers (context for cost)

- Deep tier (`opus`/`high`): **architect**, **security-auditor** — expensive-to-reverse design and trust-boundary reasoning.
- Everything else runs `sonnet`. **requirements-analyst** runs `sonnet`/`high` — high effort for reasoning depth, but not the opus price. If requirements quality turns out to be a recurring pain point, promoting it back to `opus` is a one-line frontmatter change.

## Notes

- Reviewers and auditors are read-only toward code — they describe fixes; the developer applies them. Route findings accordingly.
- Milestones and sprints are owned solely by the project-manager. Other agents attach issues to existing milestones but never create or close them.
- The backlog (`docs/backlog.md`) is owned by the project-manager for prioritization; the requirements-analyst is the one that appends new stories to it.
- When an agent consults a peer, it must pass full context in the prompt — agents share no memory.
- `research-advisor` is a peer-consult-only agent (invoked by architect/developer/requirements-analyst mid-task for external research/prior-art checks) — it does not appear in the default pipeline and the orchestrator should not invoke it as its own step.
- **Trust but verify.** A subagent's final report describes what it *intended* to do, not necessarily what actually landed — especially after an interrupted/resumed run. Before relaying a subagent's claims to the human or moving to the next pipeline step, independently re-check the load-bearing ones yourself: re-run the test suite, re-read the actual diff, re-check `docker ps -a` / `git status`, re-grep for a claimed string. This has repeatedly caught real discrepancies between what was reported and what was true.
- **Cross-validating a diff:** `test-engineer` and `code-reviewer` can be run in parallel (not sequentially) against the same static diff — they're independent reads of the same input, so agreement between them on a finding is real corroboration, not duplicated work.
- **GitHub multi-issue auto-close:** a commit message needs a separate `Fixes #N` keyword *per issue* (e.g. `Fixes #18, Fixes #19, Fixes #20`) — a single `Fixes` followed by a comma-separated list only closes the first issue.
- **Notebook cleanliness:** files under `content/*/notebook.ipynb` must stay committed with `execution_count: null` and empty `outputs` on every cell — they are read-as-source curriculum content, not executed artifacts. Any agent that runs a notebook live (for its own verification, e.g. test-engineer/developer exercising a real cluster) must reset it afterward with `git checkout -- <path>` and confirm via `git status`/direct JSON inspection before finishing — this has been the single most recurring cleanup miss across phases.
- **Codebase-memory-mcp:** if the `codebase-memory-mcp` MCP server is registered, `architect`/`developer`/`test-engineer`/`code-reviewer`/`security-auditor`/`tech-writer`/`requirements-analyst`/`research-advisor`/`project-manager` all have the relevant graph tools (`search_graph`, `trace_path`, `get_code_snippet`, `query_graph`, `get_architecture`, `search_code`, per role) wired into their own agent file — prefer these over blind Grep for symbol/call-graph questions on an indexed project; Grep/Glob/Read remain right for text, configs, and non-code.
- **Ponytail:** `architect`, `developer`, `test-engineer`, `code-reviewer`, `devops-engineer` each carry a "Lazy engineering (ponytail)" section (YAGNI ladder, root-cause-not-symptom fixes, no speculative abstractions) as a permanent default, not just when the `ponytail` skill/mode is active. `security-auditor`/`project-manager`/`requirements-analyst`/`tech-writer`/`research-advisor` deliberately don't get it — ponytail explicitly excludes security from simplification, and the rest aren't code-writing roles the ladder applies to.
- **Explicit supersession/amendment.** When a new requirements doc or ADR changes or reverses a decision from an already-shipped requirements doc or ADR, it must say so in a dedicated "Supersedes"/"Amends" section — name the specific doc and decision being changed and why — rather than silently contradicting it. Established across several redesigns (Skew & Salting, US-C9's sharpening) and, most explicitly, `live-market-data-streaming.md` superseding US-C7 and `multi-broker-kafka-cluster.md` amending `kafka-streaming-infra.md`'s Decision D1. Applies to requirements-analyst and architect alike.
