# Retrospectives

Recorded at each sprint close-out per the process in `CLAUDE.md`. This file starts with Sprint 2 —
Sprint 1's retro wasn't captured to a file at the time.

## Sprint 2 (2026-07-14 – 2026-07-18), closed 2026-07-15

**Scope:** Phase 2.5 realtime cluster monitoring dashboard (backlog #9–#13) — live per-node
CPU/RAM, per-node task/partition detail, derived ETA, diagnostic signal surfacing, UI placement.

**Outcome:** All five stories shipped and passed acceptance (`docs/acceptance/phase-2-5.md`,
US-5.1–US-5.6, D-A all PASS). One live-reproduced defect (issue #22, SSE OOB swaps breaking
client-side view switching) was found during acceptance validation and fixed same-day
(`6a52d8a`). Signed off 2026-07-15 on the strength of that fix being a scoped, low-risk
correction, without a separate live re-check round.

**What went well:**
- The acceptance-validation pass caught a real, user-visible defect (#22) that unit tests alone
  hadn't surfaced — validating the "run it against a real cluster, not just unit tests" acceptance
  methodology this project uses.
- Cross-validation between agents held up: issues #18–#21 were independently re-verified during
  the #22 investigation rather than assumed still-fixed, and the re-verification found no
  regressions.

**What didn't go well:**
- The acceptance report (`docs/acceptance/phase-2-5.md`) was committed in the same commit as the
  fix for the defect it was reporting on (`6a52d8a`) — so the report's own "issue #22 is open,
  do not sign off" language was stale the moment it was written. This is a process gap: an
  acceptance report and the fix for a finding it just raised shouldn't land in the same commit
  without the report being updated to reflect the fix. Caught later by an unrelated
  documentation-alignment audit, not at commit time.
- The Phase 2.5 sign-off sat pending for a full day (Sprint 2's due date came and went with the
  milestone still open) while unrelated Sprint 3 planning work proceeded in parallel — the human
  had to be prompted to actually give sign-off rather than it happening as a natural checkpoint.

**Try next sprint:**
- When an acceptance report and a same-day fix for one of its own findings land together, update
  the report's recommendation in that same commit instead of leaving stale "do not sign off"
  language for a later pass to catch.
- Don't let a pending human sign-off silently sit past a milestone's due date while other work
  continues in parallel — surface it as a blocking item more assertively at the point the due date
  passes, rather than only being caught, again, when the human happens to ask.

## Sprint 3 (2026-07-15 – 2026-07-19), closed 2026-07-16

**Scope:** Shell-first redesign sprint — topic-page shell + Cluster Monitor dashboard-panel
migration (issue #23), Job Detail freeze fix (issue #24), elapsed-time placeholder fix (issue #17),
true per-task duration quantiles (issue #8).

**Outcome:** All 4 issues shipped and closed. The topic-page shell redesign plus an unrelated driver
port-discovery fix landed together early in the sprint in one large commit (`e9c69aa`). The
remaining three issues (#23 dashboard-panel migration, #17, #8) were implemented in parallel by
independent developer sub-tasks against disjoint file sets, then code-reviewed and test-checked by
a code-reviewer/test-engineer pass run in parallel against the combined diff, then live-verified
against a real cluster with screenshots before all three were pushed to `main` in `4772f00`,
`b68cc77`, `1595011` (each with its own `Fixes #N`, auto-closing on push).

**What went well:**
- Running independent developer sub-tasks against disjoint files, then cross-validating the
  combined diff with a parallel code-reviewer + test-engineer pass, caught 2 real Major bugs (a
  template-gating rendering bug in the quantiles columns, and an unoffloaded blocking REST call
  that doubled the event-loop-freeze exposure this project has hit before) and 2 Minor ones — all
  fixed before the commits landed, per `1595011`'s own commit message ("Also fixes two issues found
  in review"). This validates the CLAUDE.md cross-validation pattern as more than a token-saving
  trick — it found bugs neither the reviewer nor the test-engineer would likely have caught alone.
- Ambiguous scope calls (#17/#8 were carried-over Phase 2.5 precision gaps, not scheduled in the
  confirmed Sprint 4-10 plan) were resolved by asking the human directly rather than defaulting or
  guessing at intent.

**What didn't go well:**
- A test-engineer sub-task ran a destructive `git checkout --` on a file that had uncommitted work
  mid-task, wiping it, and had to recover from its own ad-hoc backup. The recovery held — the
  orchestrator independently verified no data loss — but running destructive git commands against
  shared working-tree state without stashing first is a real process risk that got lucky this time
  rather than being prevented.

**Try next sprint:**
- Any subagent that needs to run `git checkout --`, `git reset --hard`, or similar against the
  working tree should `git stash` (or otherwise snapshot) first, not rely on ad-hoc backups
  improvised after the fact.
- Keep running the parallel cross-validation pattern (independent dev sub-tasks on disjoint files +
  parallel reviewer/test-engineer pass on the combined diff) for sprints with multiple
  independent-looking stories — it's now caught real bugs two sprints in a row (Sprint 2's #22,
  Sprint 3's template-gating + blocking-call pair).

## Sprint 4 (2026-07-16 – 2026-07-20), closed 2026-07-16

**Scope:** Two shell-dependent content/UI stories, sequenced deliberately — Catalyst plans topic
page (issue #25, backlog #31) then the data-driven topics-index landing page (issue #26, backlog
#24), which depended on #25's manifest existing to render correctly.

**Outcome:** Both issues shipped and closed the same day the sprint opened. #25 shipped
`content/catalyst-plans/{manifest.yaml,concept.md,notebook.ipynb}`, validated against all 6
US-SH8 criteria on a live 3-worker cluster (evidence in `docs/qa/screenshots/catalyst-plans/`),
with one code-reviewer Minor (ambiguous shared "pushed-down" label) fixed via clarifying prose
before sign-off. #26 then went through the full pipeline (developer → code-reviewer, no Blockers
→ test-engineer coverage review → test-engineer acceptance validation) and passed all 3 US-SH5
criteria live: `GET /` renders all 5 real topics correctly ordered/titled/blurbed from
`manifest.yaml`; the "add/remove/reorder a topic folder needs zero code changes" criterion was
proven with a second live app instance pointed at a scratch content directory, with a topic
deleted from it on the running server without a restart and the page updating correctly; and a
grep pass confirmed no topic-id special-casing remains anywhere in the implementation. Human
sign-off given on both. Sprint 4 milestone (#5) closed with 0 open / 2 closed.

**What went well:**
- The dependency ordering called out at sprint planning (#26 needs #25's manifest to render
  correctly) held with no rework — #25 shipped first, #26 built cleanly on top of it.
- The zero-code-change acceptance criterion for #26 (US-SH5) was validated with an actual live
  removal against a running second instance rather than just inspecting the code path, which is a
  stronger proof than a static read of the manifest-loading logic would have been.
- Both stories cleared code review with no Blockers and one Minor total (on #25) — the shell
  pattern from Sprint 3 is proving reusable without per-topic special-casing creeping back in,
  which is exactly what #26's grep check was designed to catch.

**What didn't go well / open item:**
- This retro is being recorded from the pipeline's own reported facts (commits, review outcomes,
  live acceptance evidence) rather than from a separate human "what went well / what didn't"
  conversation this round — flagging that gap explicitly rather than inventing sentiment on the
  human's behalf. If there's a process observation from this sprint not captured above, add it
  here before Sprint 5 planning.

**Try next sprint:**
- Continue sequencing dependent stories explicitly at planning time (as done here for #25→#26)
  rather than discovering the dependency mid-sprint.
- Sprint 5 (row #25 DAG & Lazy Eval, #29 Serialization Formats, #14 Caching, #15 Window Functions —
  4×S per the confirmed Sprint 4-10 plan) has not yet been proposed as its own milestone; do that
  as a separate sprint-planning step, not bundled into this close-out.
