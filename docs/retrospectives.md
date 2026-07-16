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
