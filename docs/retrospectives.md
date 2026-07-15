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
