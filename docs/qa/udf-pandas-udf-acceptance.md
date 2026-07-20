# UDF vs pandas UDF: Serialization Cost — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against the `content/udf-pandas-udf/` topic on branch
      `feature/51-udf-pandas-udf-serialization-cost`, following the developer-implementation,
      unit-test-coverage, and code-review passes (already checkpointed, no blockers found).
Scope: US-4.3 (`docs/requirements/spark-playbook-mvp.md`), Sprint 11's sole story (issue #51), all 5
       given/when/then acceptance criteria added 2026-07-20, verified against a real 3-worker cluster
       spawned through the app's own routes and a real JupyterLab kernel run — not by re-reading the
       diff or trusting the developer's own dev-time numbers already quoted in `concept.md`.

## Method

Unit suite re-run before starting: `py -3.9 -m pytest tests/unit -q` → **400 passed**. `docker ps -a
--filter "name=spark-"` and a port-8000/8888 check were both empty/free immediately before starting.

The FastAPI app was started fresh (`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`) and
a cluster was spawned through the app's own route (`POST /topics/udf-pandas-udf/spawn`, form-encoded
body matching this topic's own `cluster_defaults`: 3 workers, 2 cores, 4GB, 200 shuffle partitions, AQE
off) — never `compose/cli.py` or `docker compose` directly. `docker ps` confirmed `spark-master`,
`spark-worker-1/2/3`, and `spark-driver` all up, with `spark-driver` publishing `:4040-4042` and `:8888`
(JupyterLab, tokenless per `driver/jupyter_config.py`) as expected.

**Cross-worktree collision, observed live.** Partway through the first notebook run, `docker ps -a
--filter "name=spark-"` unexpectedly came back completely empty — all five containers gone, not merely
stopped — with no `teardown` call in this session's own uvicorn log. This matches exactly the failure
mode `docs/architecture/worktree-cluster-isolation.md` (the ownership-guard ADR) documents: the
`sparkpb` project name/container names/host ports are Docker-daemon-global, and the other session
working Kafka issues in the main checkout shares the same daemon. The guard code
(`app.lifecycle.compose_ops.running_owner`, `compose/cli.py`'s `_running_owner`) is present in this
worktree's code, but the ADR itself is still `Status: Proposed`, and the harm this pass hit is exactly
the ADR's own stated residual risk (R-WT-3, simultaneous-cold-start TOCTOU, or a raw `docker compose
down`/`docker rm` issued outside either app's guarded path in the other session, which no in-app guard
can intercept). Per this task's instructions, this was treated as "wait and retry," not forced: the
cluster was respawned cleanly via `POST /topics/udf-pandas-udf/spawn` (no ownership refusal on retry —
nothing was running at that point), and the rest of this pass completed without a second collision.
**This is a pre-existing platform/tooling risk, not a defect in the udf-pandas-udf topic under test**,
and is called out here rather than filed as a new topic-scoped bug — it reproduces the exact, already-
tracked ADR #38 risk, not a new one.

**Notebook execution.** `content/udf-pandas-udf/notebook.ipynb`'s `notebook_relpath` plumbing deep-links
a learner into the real JupyterLab server at `:8888`; the app itself doesn't execute notebooks. To
reproduce that faithfully, this pass drove the same JupyterLab kernel REST/WebSocket API JupyterLab's
own UI uses (`POST /api/kernels` + `/api/kernels/<id>/channels` websocket), executing the notebook's 8
code cells in file order against a freshly started kernel, deliberately leaving the kernel running
afterward (not shut down) so `:4040` stayed live for the Self-check Reveal exercise below — matching
what a learner's own open JupyterLab tab would do. All 8 cells executed cleanly with no errors and every
in-notebook `assert` passing (pandas-UDF-faster on both wall-clock and `executorRunTime`, both plan-node
presence checks, both plan-node exclusivity checks).

## US-4.3, criterion 1 — measured, never-hardcoded wall-clock/`executorRunTime` gap between `udf()` and `pandas_udf()`

**PASS**, verified live. Cell 7 (row-at-a-time `udf()`, `_row_calc` = `sin(x)*cos(x)+sqrt(|x|+1)` over
20M rows / 48 partitions) and cell 10 (identical computation as `pandas_calc`, a `pandas_udf` scalar)
reported, live, sourced from `run_time_for()`'s live `/api/v1/applications/<id>/stages` diff (never a
hardcoded multiplier):

```
[row-udf]    wall=9.33s  executorRunTime=48287ms  tasks=7
[pandas-udf] wall=4.09s  executorRunTime=20122ms  tasks=7

Measured speedup this run -- wall-clock: 2.28x, executorRunTime: 2.40x
```

The pandas UDF run was measurably faster on both dimensions this run (the notebook's own hard assertions
— `pandas_wall_s < udf_wall_s` and `pandas_run_time_ms < udf_run_time_ms` — both passed, no
`AssertionError`). Per the criterion's own explicit discipline ("the testable claim is 'the pandas UDF
run is measurably faster, and the notebook reports both real numbers,' not 'exactly Nx faster'"), the
2.28x/2.40x figures measured this run are in the same direction and rough order of magnitude as
`concept.md`'s own quoted "~2.8–3.2x" range from the developer's dev-time runs, but are not required to
match it exactly, and don't — this pass's 3-worker dev-machine load at the time (concurrent with the
cross-worktree collision noted above) plausibly explains the smaller-than-quoted gap; the notebook makes
no claim that would be falsified by that variance.

**Criterion 1: PASS.**

## US-4.3, criterion 2 — physical plans show `BatchEvalPython` (row UDF) vs. `ArrowEvalPython` (pandas UDF)

**PASS**, verified live. `.explain(mode="formatted")`, captured live from cells 7 and 10:

```
-- udf_df.select("y") --
* Project (4)
+- BatchEvalPython (3)
   +- * ColumnarToRow (2)
      +- Scan parquet  (1)

-- pandas_df.select("y") --
* Project (4)
+- ArrowEvalPython (3)
   +- * ColumnarToRow (2)
      +- Scan parquet  (1)
```

Cell 13's four exclusivity assertions (`BatchEvalPython` present in the row-UDF plan and absent from the
pandas-UDF plan; `ArrowEvalPython` present in the pandas-UDF plan and absent from the row-UDF plan) all
passed live — printed as `row-udf plan node: BatchEvalPython present = True` /
`pandas-udf plan node: ArrowEvalPython present = True`, with no assertion failure on the negative checks.
Both are single-word operator tokens, confirmed live against this project's real Spark 4.0.3 cluster
(matching the requirements doc's Open Question 4's demand for a live capture rather than an assumed node
name), and both are compatible with `plan_parser.py`'s first-word-only tokenizer.

**Criterion 2: PASS.**

## US-4.3, criterion 3 — Self-check Reveal sources plan-node evidence from manifest `plan_nodes` rules (`python-udf-eval` / `pandas-udf-eval`)

**PASS**, verified live by actually hitting `GET /topics/udf-pandas-udf/annotation` and
`POST /topics/udf-pandas-udf/annotation/reveal` directly via HTTP, not by reading
`app/web/routes/annotation.py`.

Before the Reveal, `GET /topics/udf-pandas-udf/annotation` rendered the collapsed panel ("self-check.
Nothing is shown here until you Reveal below" / a "Reveal self-check" button) — US-2.1's pull-not-push
default state.

`checkpoint(pandas_df.select("y"), topic="udf-pandas-udf")` (cell 11) had already written a fresh dump
to `scratch/shared/annotations/udf-pandas-udf/` while the kernel/SparkContext was still live. With that
same cluster/app still up (`app-20260720110003-0000`, confirmed reachable on `:4040`),
`POST /topics/udf-pandas-udf/annotation/reveal` returned, live — no stale-checkpoint warning (the
checkpoint's `app_id` cross-checked against the live driver and matched) — and the plan-node list showed:

```
Project           -- unknown / unannotated
ArrowEvalPython   -- Vectorized pandas UDF evaluation (Arrow-batched, one Python call per batch) [pandas-udf-eval]
ColumnarToRow     -- unknown / unannotated
Scan              -- unknown / unannotated
```

To independently confirm the sibling rule too (the pandas-UDF checkpoint was the most recent one, so the
first Reveal only exercised one of the two manifest entries), a fresh `checkpoint(udf_df.select("y"),
topic="udf-pandas-udf")` was issued live against the same still-running kernel, and Reveal was called
again:

```
Project           -- unknown / unannotated
BatchEvalPython   -- Row-at-a-time Python UDF evaluation (opaque bytecode, one Python call per row) [python-udf-eval]
ColumnarToRow     -- unknown / unannotated
Scan              -- unknown / unannotated
```

Both manifest entries (`BatchEvalPython` → `python-udf-eval`, `ArrowEvalPython` → `pandas-udf-eval`)
rendered correctly with their exact `concept.md`-matching labels, sourced purely from
`manifest.yaml`'s `plan_nodes` list with no annotation-engine code change — exactly the "same shape as
`content/catalyst-plans/manifest.yaml`'s already-shipped `BatchEvalPython` rule" disposition the
requirements doc calls for. `Project`/`ColumnarToRow`/`Scan` correctly render as `unknown /
unannotated`, matching the manifest's deliberately narrow two-entry `plan_nodes` list.

**Criterion 3: PASS.**

## US-4.3, criterion 4 — Self-check Reveal sources the timing-comparison evidence from the existing `stage_metrics` spotlight

**PASS**, verified live, same two Reveal calls as criterion 3. Both responses included a "Runtime stage
metrics (self-check, US-2.2)" table with `executorRunTime` spotlighted (per `manifest.yaml`'s
`stage_metrics: [{key: executorRunTime, spotlight: true}, {key: numTasks}]`), populated with real
numbers pulled live from `/api/v1/applications/<id>/stages`:

```
Stage 6: executorRunTime=14    numTasks=1
Stage 5: executorRunTime=20108 numTasks=6
Stage 4: executorRunTime=18    numTasks=1
Stage 3: executorRunTime=127   numTasks=1
Stage 2: executorRunTime=48160 numTasks=6
Stage 1: executorRunTime=27    numTasks=1
Stage 0: executorRunTime=54477 numTasks=48
```

These are not just present — they reconcile exactly against the notebook's own live `run_time_for()`
sums: stage 5 (20108) + stage 6 (14) = **20122**, tasks 6+1=**7**, matching cell 10's printed
`[pandas-udf] ... executorRunTime=20122ms tasks=7` exactly. Stage 2 (48160) + stage 3 (127) = **48287**,
tasks 6+1=**7**, matching cell 7's printed `[row-udf] ... executorRunTime=48287ms tasks=7` exactly. This
*is* the criterion's required timing evidence, sourced from existing stage REST data via the existing
`stage_metrics` spotlighting mechanism (`engine.spotlight_stage_metrics()`), with no new annotation-
engine capability and no new REST surface, exactly as the manifest's header comment and US-4.3's own
disposition require.

**Criterion 4: PASS.**

## US-4.3, criterion 5 — `concept.md` explicitly connects to `content/catalyst-plans/`'s existing `BatchEvalPython` framing

**PASS**, verified by reading both files. `content/udf-pandas-udf/concept.md`'s "Why it matters" section
opens with: *"This directly connects to [Catalyst plans'](../catalyst-plans/concept.md)
`BatchEvalPython`/`ArrowEvalPython` framing. That topic teaches these two operators from the
*optimizer's* angle ... This topic teaches the *execution-cost* distinction the same two operators
represent — same operators, a complementary angle."* It goes further than a bare cross-link: it
explicitly disambiguates the "batch" naming (`BatchEvalPython`'s "batch" refers to inter-process pipe
I/O batching, not vectorized per-call execution — each row still gets its own Python call), a nuance
that only makes sense once the learner has already seen `content/catalyst-plans/`'s framing.
`content/catalyst-plans/concept.md` independently confirms the operator this topic is building on
(`BatchEvalPython`/`ArrowEvalPython`, lines 47–50 and 79) is a real, already-shipped concept in this
repo, not a forward reference to something that doesn't exist yet. This is a complementary angle
(execution cost) on the same operator, not a re-derivation from scratch, matching the criterion's exact
wording.

**Criterion 5: PASS.**

## Teardown

```
DELETE /api/kernels/d2a5af7e-dd90-4c5b-9895-b3ae123d8c2f  → 204, GET /api/kernels → []
POST /topics/udf-pandas-udf/teardown                      → 200
docker ps -a --filter "name=spark-"                       → (empty)
uvicorn process (PID 16636, port 8000)                     → killed, confirmed no listener on :8000 or :8888
py -3.9 -m pytest tests/unit -q                             → 400 passed
```

**Notebook cleanliness check** (this session's cells were executed directly against a JupyterLab kernel
via the REST/WebSocket API, never by opening/saving `notebook.ipynb` itself through the Jupyter UI — the
file on disk was never written to during this pass):

```
grep -c '"execution_count":' content/udf-pandas-udf/notebook.ipynb   → 8
grep -o '"execution_count": [^,]*' ...                                → "execution_count": null  (x8)
grep -c '"outputs": \[\]' content/udf-pandas-udf/notebook.ipynb       → 8
git status                                                            → content/udf-pandas-udf/ still
                                                                          untracked as a whole (new
                                                                          topic, not yet committed);
                                                                          no diff inside notebook.ipynb
                                                                          from this pass, and no other
                                                                          file outside this report
                                                                          changed
```

All 8 code cells confirmed at `execution_count: null` with empty `outputs: []`; the only working-tree
change this pass produced is this report file itself. (`docs/backlog.md`,
`docs/requirements/spark-playbook-mvp.md`, `tests/unit/test_manifest.py`, and
`tests/unit/test_topics_loader.py` were already modified before this pass started, per the pre-existing
checked-out worktree state, and were untouched by this validation.)

## Overall recommendation

**All 5 of US-4.3's acceptance criteria PASS, live-verified against a real 3-worker cluster and a real
JupyterLab kernel** — not re-derived from the diff or the developer's own dev-time numbers already
quoted in `concept.md`. The measured wall-clock/`executorRunTime` gap (2.28x/2.40x this run, pandas UDF
measurably faster on both dimensions, real numbers sourced from live stage REST data rather than a
hardcoded multiplier), the distinct `BatchEvalPython`/`ArrowEvalPython` plan-node tokens (live-confirmed
against this project's real Spark 4.0.3 cluster, resolving the requirements doc's Open Question 4), the
Self-check Reveal flow surfacing both manifest `plan_nodes` rules with their exact labels, the same
Reveal flow's stage-metrics table reconciling exactly against the notebook's own printed
`executorRunTime`/task sums for both runs, and `concept.md`'s explicit, non-superficial tie-in to
`content/catalyst-plans/`'s existing `BatchEvalPython` framing, were all independently reproduced this
pass.

One platform-level event is worth the human's attention even though it isn't a defect in this topic:
mid-pass, the cluster this session owned was silently torn down by activity in a different worktree on
the same Docker daemon (documented above under Method) — a live reproduction of the exact risk
`docs/architecture/worktree-cluster-isolation.md` (issue #38) already tracks, whose ADR is still
`Status: Proposed`. No new issue is being filed for this, since it isn't a udf-pandas-udf-topic defect
and the risk is already tracked under #38 with the human's own awareness of that ADR's open status — but
it's flagged here as a live data point (the collision is real, not merely theoretical) in case it should
move that ADR's implementation priority up.

No defects found in the udf-pandas-udf topic itself; nothing filed against it. This is a
recommendation, not an approval — per this project's Definition of Done, please review this report and
give explicit sign-off (or flag anything that needs a second look) before issue #51 is considered done.

## Human sign-off

**Given, 2026-07-20.** All 5 US-4.3 acceptance criteria approved as PASS; issue #51 considered done pending remaining pipeline steps (tech-writer, project-manager close-out).
