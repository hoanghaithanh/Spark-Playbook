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

## Sprint 5 (2026-07-17 – 2026-07-21), closed 2026-07-17

**Scope:** Four independent curriculum-topic stories from the confirmed Sprint 4-10 plan — DAG &
Lazy Evaluation (issue #27, backlog #25), Caching/Persistence (issue #28, backlog #14), Window
Functions (issue #29, backlog #15), Serialization Formats (issue #30, backlog #29). No sequencing
dependency between them, unlike Sprint 4's #25→#26 chain.

**Outcome:** All 4 issues shipped and closed, all merged to `main` the same day the sprint opened
(#27, #28, #29 on 2026-07-16; #30 on 2026-07-17 via `465874c`, `Fixes #30` auto-closing on push).
Each topic was validated live against a real 3-worker cluster and a real JupyterLab kernel rather
than by static inspection. #30's acceptance evidence (`docs/qa/serialization-formats-acceptance.md`)
covers all 4 US-C8 criteria: CSV baseline shows no column pruning (~246.2MB read, ~full file),
identical data as Parquet drops to ~24.1MB (~10x), partition-column filtering on partitioned
Parquet does whole-file skipping (15.0MB → 1.9MB, matching the 1/8 partition ratio, with
`PartitionFilters` confirmed in the plan), and the Self-check Reveal flow surfaces all four exact
`inputBytes` numbers from real stage/task REST data. Code review found no Blockers across the
sprint's stories. Human gave final sign-off on #30 2026-07-17. Sprint 5 milestone (#6) closed with
0 open / 4 closed.

**What went well:**
- Four independent, unsequenced stories all completed the same day the sprint opened — the
  independent-story pattern (no cross-story dependency to manage, unlike Sprint 4's #25→#26)
  removed the sequencing overhead entirely.
- Live acceptance validation against a real cluster kept holding up as the standard: #30's
  numbers (column pruning ratio, partition-skip ratio) were pulled from actual Spark REST stage/task
  data, not asserted from reading the plan alone.

**What didn't go well:**
- During the #30 acceptance pass, a background notebook-execution process appeared to hang with no
  output — turned out to be a false alarm: stdout was fully block-buffered because it wasn't
  attached to a tty, not an actual stall. Resolved by re-running with `python -u` and explicit
  per-message logging. No real bug, but it cost investigation time before the buffering explanation
  was confirmed (full account in `docs/qa/serialization-formats-acceptance.md`).
- As with Sprint 4, this retro is recorded from the pipeline's own reported facts (commits, review
  outcomes, live acceptance evidence) rather than a separate human "what went well / what didn't"
  conversation this round — flagging that gap explicitly rather than inventing sentiment.

**Try next sprint:**
- When kicking off a background process whose progress needs to be monitored live (notebook
  execution, long-running jobs), default to unbuffered/line-buffered output (`python -u` or
  equivalent) and explicit progress logging from the start, rather than diagnosing an apparent
  hang after the fact.
- Sprint 6 has not yet been proposed as its own milestone; that's a separate sprint-planning step
  for the human to kick off, not bundled into this close-out.

## Sprint 6 (2026-07-17 – 2026-07-21), closed 2026-07-18

**Scope:** Three curriculum-topic stories from the confirmed Sprint 4-10 plan — Executor Tuning
(issue #34, backlog #27), Memory Management (issue #36, backlog #32), Skew & Salting (issue #35,
backlog #26) — plus a pre-existing tech-debt issue pulled in at the human's request, #31
(`plan_parser.py` tokenizer first-word-only limitation).

**Outcome:** All 4 issues shipped and closed. #36 merged `facb2e6` 2026-07-17; #34 merged `d4e410f`
(`Fixes #34, Fixes #37`) 2026-07-17; #31 merged `8d172d5` (doc-only fix, human-approved YAGNI
decision not to extend the tokenizer without a concrete need, no code-reviewer findings); #35 merged
`15e1c12` (`Fixes #35, Fixes #46`) 2026-07-18. Milestone #7 closed 0 open / 5 closed
(#34, #35, #36, #31, #46).

**What went well:**
- Executor Tuning (#34) and Memory Management (#36) shared the new `fetch_executors()` reveal-time
  REST-pull mechanism as planned at sprint-planning time, and both shipped clean with human sign-off
  the same day the sprint opened — the pairing rationale held up in practice.
- The pipeline's live-acceptance-validation step did its job on #35: a clean code review told us
  nothing about whether the taught Spark behavior actually matched what was claimed, and it was the
  live run against a real 3-worker cluster that caught it.

**What didn't go well:**
- Skew & Salting (#35) failed 2 of 4 acceptance criteria on the first live-acceptance pass, across 3
  independent trials — not a bug in the code, but a wrong claim about Spark internals.
  `groupBy(key).count()`'s map-side partial aggregation absorbs row-count skew before it ever reaches
  the shuffle, so the taught operation structurally could not produce the straggler the lesson
  depended on, no matter how skewed the input data was. This triggered a live architect redesign
  mid-sprint (`docs/architecture/skew-salting-demo-mechanism.md`): switch the taught operation to
  `groupBy(key).agg(F.collect_list(...))`, which isn't map-side-combinable, and rephrase the salting
  claim from "flattens the distribution" to "cuts the straggler's load by >=3x".
- The redesign itself needed a second correction: during reimplementation the developer found the
  architect's original "flattens" framing was mathematically impossible at the chosen salt-bucket
  count (true flattening at 10 buckets/200 partitions would need tens of thousands of buckets) and
  routed it back to the architect rather than quietly softening the wording itself — the fix landed
  as a same-day amendment before re-validation. Re-validation then passed clean on all 4 criteria
  (154.19x un-salted straggler ratio vs. 2x bar; 6.9x salted load reduction vs. 3x bar).
- A test-engineer run mid-sprint stopped early, citing a nonexistent "background probe task" as the
  reason to stop, and had to be resumed manually — a stall pattern worth flagging, not yet understood.

**Try next sprint:**
- For any curriculum topic whose entire pedagogical claim rests on "these are the real measured
  numbers" (timing ratios, byte ratios, flattening claims), treat the live-acceptance pass as
  load-bearing, not a formality — a clean code review is not evidence the underlying platform
  behavior matches what's taught, as #35 showed twice over (once for the original operation choice,
  once for the redesign's own numeric claim).
- Watch for the test-engineer early-stop-on-fabricated-blocker pattern (citing work that doesn't
  exist as a reason to halt) recurring in future sprints; one instance isn't enough to change process
  yet, but a second should prompt investigation into the agent's tool-use loop.

## Sprint 8 (2026-07-21 – 2026-07-25), closed 2026-07-19

**Scope:** Two issues — Checkpointing curriculum topic (issue #47, backlog row #28, US-C4), plus a
pre-existing tech-debt issue pulled in alongside it, #38 (cross-worktree Docker Compose
cluster-collision fix, found during Sprint 6's Memory Management acceptance pass) — same
riding-alongside pattern as #31 in Sprint 6.

**Outcome:** Both issues shipped and closed. #47 merged `1e7b80c`: content-only change
(`content/checkpointing/{manifest.yaml,concept.md,notebook.ipynb}`) plus one new manifest
`plan_nodes` rule (`checkpoint-truncated-scan`), zero engine code changes. All 4 US-C4 acceptance
criteria PASS with live evidence against a real 3-worker cluster (`docs/qa/checkpointing-acceptance.md`):
a 40-nested-join `.explain()` chain, `df.checkpoint()` collapsing the plan to a single
`Scan ExistingRDD` node exactly as the architect's `topic-shell-redesign.md` addendum predicted, and
the new manifest rule verified live through the real Self-check Reveal endpoint. Unit suite
unchanged (324 passed before/after). #38 merged `d543f79`
(`fix(lifecycle): guard spawn/teardown against cross-worktree cluster collisions`). Human gave final
sign-off on #47 2026-07-19. Milestone #10 closed 2026-07-19 with 0 open / 2 closed.

**What went well:**
- The riding-alongside-a-curriculum-story pattern for pre-existing tech-debt (first used for #31 in
  Sprint 6, repeated here for #38) continues to work cleanly — no scope conflict or sequencing issue
  between the two issues.
- #47's acceptance evidence again validated the "run it against a real cluster" methodology: the
  architect's specific prediction (plan collapsing to a single `Scan ExistingRDD` node) was confirmed
  live rather than just asserted from the design doc, consistent with every prior curriculum-topic
  sprint.

**What didn't go well:**
- Both #47 and #38 were closed directly by their landing commits (`1e7b80c`, `d543f79`) without a
  `Fixes #N` keyword, a departure from this repo's established close-at-merge convention (contrast
  #35/#46 via `15e1c12`'s `Fixes #35, Fixes #46`, or #34/#37 via `d4e410f`'s `Fixes #34, Fixes #37`).
  This is the first sprint where *every* landing commit skipped the keyword rather than most of them
  using it — worth treating as a convention that needs restating to the developer role, not a
  one-off.
- Unrelated to Sprint 8's own scope, but discovered while updating `docs/backlog.md` for this
  close-out: the backlog's story table had a rendering bug (an HTML example-comment sitting between
  the table's header-separator row and its first data row broke GFM table parsing, collapsing all 44
  rows into one unrendered paragraph in preview). Found and fixed same session (commit `ec7e11b`). Not
  a Sprint 8 defect, but a good example of a doc-structure issue that only surfaces when someone
  actually renders the file rather than just editing its source.
- Sprint 7 (milestone #9) was closed without a retro ever being recorded — noted here as a gap in
  this sprint's history for visibility, not being remediated as part of this close-out.

**Try next sprint:**
- Restate the `Fixes #N` close-at-merge convention explicitly (e.g. in the developer agent's
  commit-message guidance) rather than relying on it being picked up implicitly — two sprints in a
  row now (partially in earlier sprints, fully in Sprint 8) have shipped commits that closed issues
  without the keyword.
- After any large edit to `docs/backlog.md` (or other GFM tables), do a quick rendered-preview check
  before considering the edit done — the header-separator/example-comment collision this session was
  a purely structural GFM issue that a source-only read wouldn't catch.

## Sprint 9 (2026-07-19 – 2026-07-23), closed 2026-07-19

**Scope:** One issue — Fault Tolerance & Lineage curriculum topic (issue #49, backlog row #30,
US-C9), solo (same pattern as Checkpointing in Sprint 8: an L-sized story with its own distinct
engine consideration, no natural pairing candidate in the backlog). Self-check evidence sourcing
was already settled by the architect 2026-07-15 (Decision A — reveal-time REST pull); the
worker-kill safety UX open question was resolved to ship as a documented manual `docker kill` step
rather than an in-app control, so no architect gate was needed ahead of this sprint.

**Outcome:** #49 shipped and closed, content-only change (`content/fault-tolerance-lineage/`,
commit `8c03676`), plus one new engine helper: reveal-time `_task_retry_evidence()` reusing
`app_client.fetch_task_list()` and a shared `retries_by_index()` helper extracted from the
dashboard collector (Decision A as designed). All 5 US-C9 acceptance criteria PASS with live
evidence against a real 3-worker cluster across two independent runs
(`docs/qa/fault-tolerance-lineage-acceptance.md`): a killed worker mid-job produced a real, measured
partial retry (7 of 423 tasks across 2 stages on run 1, reproduced qualitatively on run 2 with a
different kill target/timing), never a full job restart; the killed-worker run's result matched a
clean run exactly (40-category signature, byte-identical); the new REST-pull evidence rendered
correctly matching the notebook's own numbers; the worker-kill step ships as documented manual
`docker kill`, no in-app control built; `concept.md` covers the recomputation-from-lineage model and
the lineage-cost tie-in to Checkpointing/Caching. Code-reviewer found 1 Major (a FAILED/resubmitted
stage's own row falsely reporting "0 retried" instead of pointing to the real evidence) and 2 Minor
findings, all fixed and re-verified (350 passed, 2 skipped, up from 335 — test-engineer added 15 new
unit tests pinning both retry-detection shapes and the fix). One caveat surfaced, not blocking: AC3's
exact FAILED-status-on-a-superseded-attempt branch didn't occur naturally in either live run (both
times the superseded attempt's REST status came back `COMPLETE`), so that branch is verified by a
mocked unit test rather than independent live reproduction — the fix is logically sound and reviewed
clean regardless. Human gave final sign-off 2026-07-19. Milestone #11 closed 2026-07-19 with 0 open /
1 closed.

A same-day, unrelated CI commit (`93d8876`, `ci(deploy-lan): skip doc-only pushes from triggering
homelab build`) landed right after the content commit. It is not Sprint 9 scope — it's a deploy-lan
pipeline efficiency fix (adds `paths-ignore` for docs/README/architecture-note edits, deliberately
excluding `content/**/concept.md` since `app/topics/loader.py` renders it at runtime) that happened
to land in the same window. Noted here for completeness, not counted as sprint output.

**What went well:**
- The Sprint 6/8 riding-alongside-tech-debt pattern was correctly *not* applied here: pre-existing
  issue #48 (driver Spark UI deep links) was assessed as a candidate at sprint planning and
  deliberately left out, since it's scoped to the public-deploy surface the human already ruled out
  of future scope — a good example of the pattern being applied with judgment rather than by rote.
- The Open Question 2 (worker-kill safety UX) resolution — ship as a documented manual step instead
  of blocking on an in-app control — avoided a repeat of the "gap before Sprint 8" architect-pass
  delay that the original Sprint 4-10 plan had built in for this exact story.
- Live acceptance validation again earned its keep: the AC1 partial-retry claim was independently
  reproduced twice with different kill targets/timing rather than asserted from a single run, and
  the code-reviewer's Major finding (mislabeled "0 retried" row) was a real defect a static read of
  the REST-pull logic could plausibly have missed.

**What didn't go well:**
- Same gap as Sprint 4/5's retros noted: this entry is written from the pipeline's own reported
  facts (backlog row #30's narrative, commits, review outcomes, acceptance evidence) rather than a
  separate human "what went well / what didn't" conversation this round.
- AC3's exact failure-status branch (a superseded attempt reporting `FAILED` rather than `COMPLETE`)
  didn't occur naturally in either live run and is only covered by a mocked unit test — not a defect
  in what shipped, but a live-reproduction gap worth remembering if this code path is touched again.

**Try next sprint:**
- Continue assessing tech-debt-ride-along candidates against genuine relevance to active scope
  (as done here for #48) rather than defaulting to pairing whenever a pre-existing open issue exists.
- If a future change touches `_task_retry_evidence()` or `retries_by_index()`, prioritize getting a
  live repro of the FAILED-on-superseded-attempt branch rather than relying on the mocked test alone.
- Sprint 10 has not yet been proposed as its own milestone; that's a separate sprint-planning step
  for the human to kick off, not bundled into this close-out.
