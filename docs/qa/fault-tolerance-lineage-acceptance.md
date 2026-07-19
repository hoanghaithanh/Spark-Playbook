# Fault Tolerance & Lineage — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-19, against uncommitted working-tree content/code on `main` — issue #49, backlog
      row #30, Sprint 9 (sole story). Covers `content/fault-tolerance-lineage/` (new topic) plus
      the `task_retry_evidence` plumbing (`app/annotation/manifest.py`,
      `app/monitoring/collector.py`, `app/web/routes/annotation.py`,
      `app/web/templates/fragments/stage_table.html`) and its post-code-review fix (a `superseded`
      flag suppressing the misleading "0 retried" row for a FAILED/resubmitted stage attempt).
Scope: US-C9 (`docs/requirements/curriculum-topics-2026-07.md`), all 5 acceptance criteria,
       verified against a real 3-worker cluster spawned through the app's own routes, driven via
       a real JupyterLab kernel (not just reading the .ipynb source), with **two independent live
       `docker kill` runs** — this is an independent re-validation, not a re-run of the developer's
       own build-time claims.

## Method

**Unit suite**, run after this pass: `py -3.9 -m pytest tests/unit -q` → **350 passed**, matching
the reported baseline from the unit-test-coverage pass that preceded this one. No unit-test
changes made or needed during this pass.

**Live cluster.** `docker ps -a` was empty before starting. The FastAPI app was started fresh
(`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8002`) and a cluster was spawned through
the app's own route (`POST /topics/fault-tolerance-lineage/spawn`, this topic's own
`cluster_defaults`: 3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE off) — never
`compose/cli.py` or `docker compose` directly. `docker ps --filter "name=spark-"` confirmed
`spark-master`, `spark-worker-1/2/3`, and `spark-driver` all up, with `spark-driver` publishing
`:4040-4042` and `:8888` (JupyterLab).

**Notebook execution.** Same technique as prior topics' acceptance passes: a script drove the
real JupyterLab kernel REST/WebSocket API (`POST /api/kernels` + the kernel's `channels`
websocket) to execute `content/fault-tolerance-lineage/notebook.ipynb`'s 7 code cells in file
order against a real kernel, with a background thread issuing a real `docker kill
spark-worker-2` a few seconds into the killed-worker run cell (cell 4). The notebook file itself
was never modified for this run (confirmed below). A second, independent run (fresh kernel,
different killed worker, different kill delay) reused the already-written parquet data to
specifically probe the FAILED-row fix — see AC3 below.

## AC1 — worker killed mid-job shows partial task retry, not full job restart

**PASS**, real measured evidence from the primary run (`docker kill spark-worker-2` issued ~4s
into the second `run_job()` call, landing while the sort/shuffle stages (stage 12, 15 tasks) were
active):

```
[clean] wall=23.40s categories=40
>>> issuing docker kill spark-worker-2 now <<<
26/07/19 12:50:22 ERROR TaskSchedulerImpl: Lost executor 1 on 172.19.0.4: worker lost
[killed-worker] wall=17.08s categories=40
Stages inspected: 6, total tasks: 423, retried tasks: 7
  stage 11: 2 of 6 tasks retried
  stage 12: 5 of 15 tasks retried
7 of 423 tasks were retried after the worker kill (416 kept their original results)
```

7 of 423 tasks retried — strictly fewer than the total, and at least one retried, exactly the
testable claim the requirements doc specifies ("not... exactly 2 of 50", the real measured count
is what matters). The job never restarted from stage 0; stages 0/1/2/3/4 (dataset build, already
complete before the kill) and the unaffected 200-task join/groupBy stages (13, 14 — 0 retried
each) kept their original results untouched. The notebook's own hard assert
(`retried_tasks < total_tasks`) passed. A second independent run (`docker kill spark-worker-1`,
6s delay, different stage) reproduced the same qualitative shape (partial retry only: 2 of 3
tasks in the affected map stage), confirming this isn't a one-off artifact of a specific timing
value.

## AC2 — final result after recovery matches a clean run with no worker killed

**PASS**, real measured evidence, notebook cell 5:

```
Correctness check PASSED: 40 categories, killed-worker run matches clean run exactly.
```

This is a genuine second clean run compared against the killed-worker run (both runs executed
live in the same session, not an assumption) — `clean_signature == kill_signature`, an
order-independent, float-rounded fingerprint of all 40 category rows' `(category, total_amount,
n)`, asserted equal and printed. Both runs also completed with `SUCCESS` overall despite one
losing a worker mid-flight.

## AC3 — self-check Reveal evidence sourced from the new REST pull, including the FAILED-row fix

**PASS with a caveat**, verified live via the actual Reveal UI/route (`POST
/topics/fault-tolerance-lineage/annotation/reveal`), not just a manifest/code read. With the
killed-worker run's checkpoint written (`driver.playbook.checkpoint(kill_df,
topic="fault-tolerance-lineage")`) and the kernel's Spark session still live at `:4040`, Reveal's
stage table rendered a "tasks retried" column sourced from the new `_task_retry_evidence()` REST
pull, matching the notebook's own directly-queried numbers exactly:

```
14 (attempt 0) COMPLETE 0 of 200 retried (200 kept results)
13 (attempt 0) COMPLETE 0 of 200 retried (200 kept results)
12 (attempt 0) COMPLETE 5 of 15 retried (10 kept results)
11 (attempt 1) COMPLETE 2 of 6 retried (4 kept results)
11 (attempt 0) COMPLETE 0 of 6 retried (6 kept results)
```

Stage 11's resubmitted attempt (`attempt 1`) correctly reports the real recomputed count (2 of 6)
sourced from `numTasks` per the resubmission-shape branch, not a misleading zero. Stage 11's
original attempt (`attempt 0`) correctly reports 0 of its own 6 tasks retried — accurate, since
none of that attempt's own tasks were individually retried (it completed cleanly; a *downstream*
stage's fetch failure is what triggered the resubmission).

**The specific case the fix targets — a stage attempt whose own REST `status` is `FAILED` and
which is superseded by a later attempt — did not occur naturally in either of the two independent
live kill runs.** In both runs, the original (superseded) attempt's REST status came back
`COMPLETE`, not `FAILED`, even though it was superseded by a resubmitted attempt. That's a
legitimately different code path from the one the fix guards (`stage.get("status") == "FAILED"
and superseded`): the fix's target condition simply wasn't reached, so this pass could not
independently confirm the exact "0 retried" masking bug reproduces or stays fixed **against a
live FAILED-status stage** — only that the closely-related COMPLETE-status superseded case
renders correctly (which it does, and always has). The fix's own unit tests
(`tests/unit/test_annotation_routes.py::TestTaskRetryEvidence::test_superseded_failed_attempt_suppresses_misleading_zero_retry`)
directly pin the FAILED-status behavior with a mocked stage list, and that test does pass — this
pass's live runs simply never manufactured that exact REST status value to re-confirm it outside
mocks. Recommend the human treat this as "unit-verified, not independently live-reproduced" for
that one specific status value, not as a live failure — nothing here contradicts the fix; two
live runs just didn't happen to exercise that state.

No engine change was needed or made: `git diff -- app/annotation/plan_parser.py
app/annotation/engine.py` is empty; the new evidence path is entirely in
`app/web/routes/annotation.py`/`app/monitoring/collector.py`/`app/annotation/manifest.py`
plumbing, per Decision A.

## AC4 — worker-kill is a documented manual step, not an in-app control

**PASS**, confirmed two ways. (1) `content/fault-tolerance-lineage/notebook.ipynb`'s section 4
instructs the learner to run `docker kill spark-worker-2` from a terminal outside the notebook —
there is no in-notebook cell that performs the kill itself. (2) `Grep` across `app/web/routes/`
for kill/simulate-failure endpoints found none — the only topic-related POST routes are
`/topics/{topic_id}/spawn` and `/topics/{topic_id}/teardown` (`app/web/routes/topics.py`) and
`/topics/{topic_id}/annotation/reveal` (`app/web/routes/annotation.py`); no route triggers a
container kill. The only code mentioning "docker kill" is explanatory docstring text in
`app/web/routes/annotation.py`, not executable code.

## AC5 — concept.md covers the recomputation/lineage-cost/checkpointing tie-in

**PASS**, content read directly (content-only criterion, no live evidence required per the
requirements doc). `content/fault-tolerance-lineage/concept.md`'s "Why it matters" section:

- **(a) Recomputation from lineage, not replication; driver reschedules only lost tasks** — "Only
  the lost tasks are retried — the driver does not restart the job" bullet, matching the story's
  first sub-requirement verbatim in substance.
- **(b) Recomputation cost scales with lineage length; tie-in to Checkpointing/Caching** —
  "Recomputation cost scales with lineage length" bullet explicitly connects to
  `../checkpointing/concept.md` and `../caching-persistence/concept.md`, stating both shorten the
  recompute path and are "just as much resilience optimizations" as performance ones — exactly
  the "why checkpointing/short chains matter for resilience" tie-in the AC requires, following the
  same connect-the-dots pattern the requirements doc cites from `content/checkpointing/concept.md`.

Both sub-criteria are unambiguously present; no gaps found.

## Coverage review

No unit-test gaps found requiring new tests. `tests/unit/test_annotation_routes.py`'s
`TestTaskRetryEvidence` class already covers the opt-in gating, both retry shapes, the repeated-
resubmission edge case, and the FAILED-row fix itself (via mocks) — this pass's job was live
re-validation, not new test authorship, per the task scope.

## Blockers / gaps for human attention

One partial-verification gap, not a blocker: **AC3's FAILED-status superseded-row suppression
could not be independently reproduced live** in two real kill runs (both times the superseded
attempt's REST status was `COMPLETE`, not `FAILED`) — it remains verified only via the unit test's
mocked stage list. This does not mean the fix is wrong (the COMPLETE-status case, which *did*
occur live twice, renders correctly both times), only that this pass's live evidence for that one
specific branch is narrower than for the rest of the story. If the human wants a live
FAILED-status repro before sign-off, it would likely need either a different failure-injection
technique (e.g. killing two workers in quick succession, or a build with
`spark.stage.maxConsecutiveAttempts` tuned down) rather than a single `docker kill` — out of scope
for this pass to chase further.

## Cleanup confirmation

- `docker ps -a` returned empty after `POST /topics/fault-tolerance-lineage/teardown` and a final
  `docker ps -a` check — cluster fully torn down.
- Notebook cleanliness confirmed by direct JSON inspection (not just `git status`): every code
  cell in `content/fault-tolerance-lineage/notebook.ipynb` still carries `execution_count: null`
  and `outputs: []` — the live kernel execution (both runs, three kernels total) went through the
  JupyterLab kernel API directly and never wrote back into the `.ipynb` file. `git status
  --porcelain -- content/fault-tolerance-lineage/notebook.ipynb` shows only `??` (untracked/new),
  never `M` (modified), before and after this pass.
- `git status --porcelain` after this pass shows exactly the same file set as before this pass
  started (the pre-existing modified/untracked files from the prior unit-test and code-review
  passes), plus this new report — nothing else touched, nothing committed.
- The `uvicorn` process started for this validation pass (port 8002) was killed; a follow-up
  connection attempt to `http://127.0.0.1:8002/` returned no response (connection refused).

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final
sign-off before marking US-C9 (issue #49) done. 4 of 5 acceptance criteria pass with full live
evidence; AC3 passes with one narrower caveat (the FAILED-status branch of the review fix is
unit-verified but not independently live-reproduced in this pass — see "Blockers / gaps" above).
Nothing found here contradicts the fix or the developer's own build-time validation; recommend
accepting, with the FAILED-status caveat surfaced for the human's awareness rather than treated
as silently resolved.
