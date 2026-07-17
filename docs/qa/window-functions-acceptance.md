# Window Functions — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-16, against the uncommitted working-tree content for issue #29
      (`content/window-functions/{manifest.yaml,concept.md,notebook.ipynb}`), following the
      code-reviewer and first test-engineer coverage passes (static-diff reads, no live run)
Scope: US-C6 (`docs/requirements/curriculum-topics-2026-07.md`), all 3 acceptance criteria, verified
       against a real 3-worker cluster spawned through the app's own routes, not just re-reading the
       diff or trusting the dev screenshots already in `docs/qa/screenshots/window-functions/dev/`.

## Method

Unit suite re-run before starting: `py -3.9 -m pytest tests/unit -q` → **280 passed**. `docker ps -a`
was empty before beginning (clean state).

The FastAPI app was started fresh (`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`) and
a cluster was spawned through the app's own route (`POST /topics/window-functions/spawn`, this topic's
own `cluster_defaults`: 3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE off) — never
`compose/cli.py` or `docker compose` directly. `docker ps` confirmed `spark-master`,
`spark-worker-1/2/3`, and `spark-driver` all up, with `spark-driver` publishing `:4040-4042`, `:8888`
(JupyterLab, per `app/config.JUPYTER_URL`) as expected.

**Notebook execution.** `content/window-functions/notebook.ipynb`'s `notebook_relpath` plumbing
(`app/topics/loader.py::Topic.notebook_relpath`) deep-links a learner into the real JupyterLab server
at `:8888` — the app itself doesn't execute notebooks, a human does, cell-by-cell, in that iframe. To
reproduce that faithfully rather than just reading the code, this pass drove the same JupyterLab
kernel REST/WebSocket API JupyterLab's own UI uses (`POST /api/kernels` + `/api/kernels/<id>/channels`
websocket), executing the notebook's 8 code cells in file order against a freshly started kernel, and
deliberately **left the kernel running** afterward (not shut down) so `:4040` stayed live for the
Self-check Reveal exercise below — matching what a learner's own open JupyterLab tab would do, unlike
an `nbconvert --execute` run (tried first, then discarded for this reason: nbconvert's ephemeral kernel
shuts itself down the moment the script finishes, which kills the SparkContext and makes the checkpoint
immediately "stale" by the time Reveal is clicked — not representative of real use).

All 8 cells executed with **no errors and all in-notebook `assert`s passing** (row-count check, running-
total-vs-independent-baseline check, `Window`/`Sort`/`Exchange`-present check, `numTasks > 1` check,
`numTasks == 1` check, `correct > bad` task-count check).

## US-C6, criterion 1 — `Window` plan node preceded by Sort/Exchange, correct-usage case

**PASS**, verified live. Cell 4's `totals_df.explain(mode="formatted")` (for
`row_number()` over `partitionBy("user_id").orderBy("ts")` plus
`.rowsBetween(Window.unboundedPreceding, 0)`), captured live from the running kernel:

```
== Physical Plan ==
Window (4)
+- * Sort (3)
   +- Exchange (2)
      +- * Scan ExistingRDD (1)

(2) Exchange
Arguments: hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=308]

(3) Sort [codegen id : 2]
Arguments: [user_id#0 ASC NULLS FIRST, ts#1L ASC NULLS FIRST], false, 0

(4) Window
Arguments: [row_number() windowspecdefinition(user_id#0, ts#1L ASC NULLS FIRST, ...) AS rn#13,
            sum(amount#2) windowspecdefinition(user_id#0, ts#1L ASC NULLS FIRST, ...) AS running_total#31],
           [user_id#0], [ts#1L ASC NULLS FIRST]
```

`Window` (4) fed by `Sort` (3) fed by `Exchange` (2) — exactly the shape the manifest's `plan_nodes`
mapping expects. Correctness of the underlying computation was also verified live, not just the plan
shape: `ranked_df.filter(rn == 1).count()` → exactly `2000` (one per user, `NUM_USERS`), and the
running-total-at-last-event vs. an independently computed `groupBy("user_id").agg(sum("amount"))`
baseline → **0 mismatches** across all 2,000 users.

The stage that actually does this work was isolated via the notebook's own before/after stage-id diff
(not `max(stageId)`, which would pick the trailing single-task sum-collapse stage instead — see the
notebook's own inline comment): **Stage 12: numTasks=200**.

The Self-check tab's plan panel (exercised live below, criterion 3) confirms the same three nodes are
labeled per the manifest's mapping (`window-function` / `window-partition-sort` /
`window-partition-shuffle`), unchanged from the existing US-2.1/US-4.2 mechanism.

**Criterion 1: PASS.**

## US-C6, criterion 2 — dropped `partitionBy`, entire dataset funneled onto one partition/task

**PASS**, verified live, both via the driver's own log WARN and via real stage/task REST data — a
deliberate contrasting example (cells 6-7), not just the correct-usage case above.

**Driver log evidence**, captured both from the live kernel's own stream output and independently
confirmed present in `docker logs spark-driver` (i.e., this is really the container's log, not just
notebook stdout the kernel happened to echo):

```
26/07/17 02:26:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a
single partition, this can cause serious performance degradation.
```

This is Spark's own WARN, logged at the exact moment `bad_totals_df.agg(...).collect()` ran (cell 7,
`Window.orderBy("ts")` with `partitionBy` dropped) — the precise wording US-C6's acceptance criterion
names.

**Task-count evidence**, from the same before/after stage-id-diff technique used in criterion 1: the
missing-`partitionBy` query's window-reduce stage came back as **Stage 15: numTasks=1** — the entire
200,000-row dataset on a single task, versus Stage 12's **200 tasks** for the correctly-partitioned
version run moments earlier against the identical dataset and cluster. Cell 7's own side-by-side
assertion (`correct_num_tasks > bad_num_tasks`) passed: `200 tasks` vs. `1 task(s)`.

**Criterion 2: PASS**, both sub-parts of the acceptance criterion (plan/task data showing the collapse,
and it being a genuine contrasting example alongside the correct-usage query, not a replacement for it).

## US-C6, criterion 3 — Self-check tab Reveal surfaces the task-count evidence from existing REST data

**PASS**, verified live by actually clicking through the flow (via direct HTTP calls to the same routes
the Reveal button hits), not just reading `app/web/routes/annotation.py`.

`checkpoint(totals_df, topic="window-functions")` (cell 5) wrote a fresh dump to
`scratch/shared/annotations/window-functions/` while the kernel/SparkContext was still live. With the
same cluster/app still up (`app-20260717022617-0001`, confirmed reachable on `:4040`),
`POST /topics/window-functions/annotation/reveal` returned, live:

- **No stale-checkpoint warning** (confirms the checkpoint's `app_id` was cross-checked against the
  live driver and matched — this is itself evidence the "must still be live" precondition genuinely
  gates the panel, not just present-but-unenforced: a first attempt at this exact same reveal, made
  right after an `nbconvert`-executed run whose ephemeral kernel had already exited, correctly *did*
  render `"No Spark application is currently reachable on any driver UI port"` instead of a plan — the
  stale-check path was independently exercised, not just the happy path).
- **Plan panel**, correctly labeling all 3 manifest-mapped nodes from the real captured plan:
  `Window` → *"Window node -- row_number()/running-total computed per partition..."*,
  `Sort` → *"Sort -- orders each partition..."*, `Exchange` → *"Shuffle boundary (Exchange)..."*.
- **Stage-metrics table**, populated with real numbers pulled live from
  `/api/v1/applications/<id>/stages` (not fixtures — 16 real stage rows, `numTasks` spotlighted per the
  manifest): **stage 12 shows `numTasks=200`** (the correctly-partitioned window) and **stage 15 shows
  `numTasks=1`** (the missing-`partitionBy` case) side by side in the same table — this *is* the
  criterion's required evidence, sourced from existing stage/task REST data with no new
  annotation-engine capability, exactly as the manifest's header comment and US-C6's third bullet
  require.

The panel only populated with real numbers *after* `checkpoint()` had run — confirmed by contrast with
the pre-checkpoint state (`GET /topics/window-functions/annotation` before any `checkpoint()` call in
this session, and separately the stale-warning render above), consistent with the "pull, not push"
self-check design (manifest header comment / G3).

**Criterion 3: PASS.**

## Teardown

```
DELETE /api/kernels/<kernel_id>           → 204, GET /api/kernels → []
POST /topics/window-functions/teardown    → 200
docker ps -a                              → (empty)
uvicorn process                           → stopped
py -3.9 -m pytest tests/unit -q           → 280 passed
```

**Notebook cleanliness check** (this session's cells were executed directly against a JupyterLab
kernel via the REST/WebSocket API, never by opening/saving `notebook.ipynb` itself through the
Jupyter UI or `nbconvert --output` in place — so the file on disk was never written to during this
pass, and no reset was actually needed. Verified rather than assumed):

```
grep -c '"execution_count":' content/window-functions/notebook.ipynb        → 8
grep -o '"execution_count": [^,]*' ...                                      → "execution_count": null  (x8)
grep -c '"outputs": \[\]' content/window-functions/notebook.ipynb           → 8
grep -c '"outputs":' content/window-functions/notebook.ipynb                → 8
git status --short                                                          → only test files + the
                                                                                already-untracked
                                                                                content/window-functions/
                                                                                and its screenshots dir
                                                                                (both pre-existing from
                                                                                before this pass began)
```

All 8 code cells confirmed at `execution_count: null` with empty `outputs: []`, matching the state
before this pass started — no live-execution artifacts leaked into the committed-eligible tree.

## Overall recommendation

**All 3 of US-C6's acceptance criteria PASS, live-verified against a real 3-worker cluster and a real
JupyterLab kernel** — not re-derived from the diff or the developer's dev-time screenshots. The
correct-usage plan shape (`Window`→`Sort`→`Exchange`) and its result correctness, the deliberate
missing-`partitionBy` contrasting example (both the driver's own WARN log line and the task-count
collapse to 1), and the Self-check Reveal flow surfacing that same task-count contrast from real
stage/task REST data, were all independently reproduced this pass, not assumed from the manifest/
notebook source alone.

No defects found; nothing filed. This is a recommendation, not an approval — per this project's
Definition of Done, please review this report and give explicit sign-off (or flag anything that needs
a second look) before Sprint 5 / issue #29 is considered done.

## Human sign-off

Given 2026-07-16 — all 3 US-C6 acceptance criteria accepted as PASS, no further changes requested.
