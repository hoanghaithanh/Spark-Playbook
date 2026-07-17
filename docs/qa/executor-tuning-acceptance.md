# Executor Tuning — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-17, against commits `684f9e7` (feat) and `98a52fb` (docs deviation note) on
      `worktree-issue-34-executor-tuning`, following a coverage-review/test-addition pass (this same
      report) and a live run.
Scope: US-C3 (`docs/requirements/curriculum-topics-2026-07.md`), all 3 acceptance criteria, verified
       against a real 3-worker cluster spawned through the app's own routes, plus the reveal-time
       `executor_metrics` plumbing (Decision A, `docs/architecture/topic-shell-redesign.md`) exercised
       live at both the "live app" and "no active application" ends of its degrade path.

## Method

Unit suite before adding new tests: `py -3 -m pytest tests/unit -q` -> **305 passed** (matches the
task's stated baseline). After adding 3 new route-level tests for gaps found in coverage review (empty
executor list, malformed executor-list shape, and the "no live application resolves at all" degrade
path -- see Coverage review below): **308 passed**, 0 failed.

`docker ps --filter "name=spark-"` was empty before starting. The FastAPI app was started fresh
(`py -3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`) and a cluster was spawned through the
app's own route (`POST /topics/executor-tuning/spawn`, this topic's own `cluster_defaults`: 3 workers,
4 cores, 8GB, 200 shuffle partitions, AQE off) -- never `compose/cli.py` or `docker compose` directly.
`docker ps` confirmed `spark-master`, `spark-worker-1/2/3`, and `spark-driver` all up, with
`spark-driver` publishing `:4040-4042` and `:8888` (JupyterLab).

**Notebook execution.** Same technique as the prior serialization-formats acceptance pass: a script
drove the real JupyterLab kernel REST/WebSocket API (`POST /api/kernels` + `/api/kernels/<id>/channels`
websocket) to execute `content/executor-tuning/notebook.ipynb`'s 7 code cells in file order against a
freshly started kernel, unbuffered (`python -u`) with per-message logging, leaving the kernel running
afterward so `:4040` stayed live for the Self-check Reveal exercise -- matching what a learner's own
open JupyterLab tab would do. The notebook file itself was never modified for this run.

## US-C3, criterion 1 — measurable wall-clock and GC-time-fraction difference between fat and right-sized executor runs

**PASS with a caveat -- see filed bug.** Cell-by-cell execution against the real cluster produced:

```
             executors  wall (s)  GC time (ms)  duration (ms)  GC fraction
fat                  3     12.61          3458         135520       0.0255
right-sized          6     15.93          4678         177512       0.0264
```

- Executor counts matched the notebook's own expectation exactly: 3 executors for the fat run (1 per
  node), 6 for the right-sized run (2 per node) -- both against the identical 3-worker cluster, confirming
  the manifest.yaml deviation note's cluster-fixed/executor-shape-varied design actually produces the
  intended executor topology live, not just in theory.
- **Wall-clock**: measurable and non-degenerate (12.61s vs 15.93s) -- consistent with the doc's already-
  accepted finding that wall-clock does not reliably favor the right-sized run at this dev-cluster scale
  (here the fat run was faster). This half of criterion 1 is satisfied exactly as the doc's 2026-07-17
  deviation note anticipates.
- **GC-time fraction**: measurable (0.0255 vs 0.0264, both nonzero, a real ~3.4% relative difference) --
  but in the **opposite direction** from what the requirements doc's 5-trial claim and the notebook's own
  hard `assert fat_gc_fraction > rightsized_gc_fraction` require. The right-sized run's GC-fraction came
  out *higher* than the fat run's on this live trial, not lower. Cell 13 raised `AssertionError` and the
  notebook execution stopped there (cells 0-12 all completed `ok`; cell 13 `error`) -- full log excerpt:

  ```
  AssertionError: expected the fat run's GC-time fraction (4 concurrent tasks sharing one heap) to be
  higher than the right-sized run's (2 concurrent tasks per heap, double the per-task headroom)
  ```

  This directly contradicts the doc's characterization of GC-time fraction as the *reliable* half of this
  criterion (only wall-clock was flagged as unreliable). "Measurable" is satisfied (a real, nonzero
  difference exists both times), but the notebook's own hard assertion on that difference's *direction* is
  not safe on every run, unlike what the deviation note implies.

  A second run attempted immediately after (for a 2nd data point) could not proceed: because the failed
  assertion in run 1 aborted the cell before `spark_rightsized.stop()` (the same cell's last line) ever
  ran, the right-sized session's 6 executors continued holding the entire cluster's 12-core capacity,
  starving the second run's job of any resources ("Initial job has not accepted any resources...",
  repeating). The stuck kernel was force-killed to free the cluster and complete this validation pass
  cleanly. **Filed as issue #37** (`bug`, `from:acceptance`) with the full repro, numbers, and a suggested
  fix (move `.stop()` to a `finally`, and soften the GC-fraction assertion the same way the doc already
  softened the wall-clock one).

**Criterion 1: PASS on "measurable" (both numbers), with a real, filed defect on the GC-fraction
assertion's reliability and its resource-leak-on-failure side effect. Not a blocker for the topic's
pedagogical content (the concept and honest reporting framing are sound), but the notebook's hard assert
can break a learner's run non-deterministically and should be fixed before this is fully done.**

## US-C3, criterion 2 — self-check evidence reuses the existing `/executors` REST source (`totalGCTime`)

**PASS**, verified live via the actual Reveal UI (`POST /topics/executor-tuning/annotation/reveal`), not
just the unit-tested route in isolation. With the fat run's checkpoint still fresh and the right-sized
run's Spark session still live at `:4040`, Reveal rendered a real 7-row executor table (6 real executors,
ids `0`-`5`, plus the `driver` entry the REST API also reports), e.g.:

```
Executor  totalGCTime  totalDuration  totalTasks
driver    244          189970         0
5         816          28387          76
4         736          30857          78
3         585          30416          73
2         663          27966          74
1         1026         29833          61
0         852          30053          67
```

`totalGCTime` rendered with the `spotlight` CSS class (per `manifest.yaml`'s `spotlight: true`),
`totalDuration`/`totalTasks` rendered unspotlighted -- exactly matching the manifest's declared
`executor_metrics` rules and confirming `app_client.fetch_executors()` -> `spotlight_executor_metrics()`
-> `executor_table.html` is real, live data end to end, not a mocked path.

**Criterion 2: PASS.**

## US-C3, criterion 3 — annotation engine extended via `executor_metrics`, not the plan-node matcher

**PASS**, verified against the actual diff, not just re-reading the architecture doc's intent:

- `git diff main -- app/annotation/plan_parser.py` is empty -- untouched.
- `git diff main -- app/annotation/engine.py` has zero deleted lines against `main` -- the only change is
  the new, additive `spotlight_executor_metrics()` function, structurally mirroring
  `spotlight_stage_metrics()` exactly (confirmed by reading both side by side).
- `content/executor-tuning/manifest.yaml` declares no `plan_nodes` section at all (confirmed by
  `test_no_plan_nodes` in `tests/unit/test_manifest.py`, and independently by inspecting the file), so
  there is nothing for the plan-node matcher to even attempt on this topic.
- The live Reveal response confirms this too: the "Plan nodes" section renders `ColumnarToRow`/`Scan` as
  `unknown / unannotated` (no manifest rules to match against), while the executor-metrics section below
  it renders fully labeled, spotlighted evidence -- exactly the split Decision A specifies.

**Criterion 3: PASS.**

## Degrade path — "no active application" while `executor_metrics` is declared

Per the architecture doc's stated trade-off that this evidence source depends on a live `:4040`, this was
exercised live, not just via the unit-mocked route test. After both notebook kernels were killed (freeing
`:4040`'s driver process along with them) but with the fat run's checkpoint file still on disk, Reveal was
called again:

```
Stale checkpoint: No Spark application is currently reachable on any driver UI port (4040-4042)...
Could not reach the Spark REST API at :4040 for app app-20260717120846-0002.   <- stage_metrics panel
Could not reach the Spark REST API at :4040 for per-executor memory metrics.  <- executor_metrics panel
```

All three warnings rendered together, correctly: the pre-existing stale-checkpoint check (issue #16) and
the pre-existing stage-metrics unreachable message both still work unchanged, and the new executor-metrics
panel independently produces its own equivalent "could not reach" message rather than silently omitting
the section or rendering stale/empty data as if current. No blocker found here.

## Coverage review and additions

Existing developer-added tests (`tests/unit/test_annotation_routes.py::TestRevealExecutorMetrics`,
`test_engine.py::TestSpotlightExecutorMetrics`, `test_manifest.py::TestLoadRealExecutorTuningManifest` /
`TestValidManifestExecutorMetrics`, `test_topics_loader.py::TestLoadRealExecutorTuningTopic`) already
covered: manifest section parsing (valid, missing key, non-list, defaults-to-empty), spotlighting
(declared keys only, missing key -> None value, no rules -> empty dict), the route-level happy path
(spotlighted values rendered), the topic-doesn't-declare-the-section skip path, and the
`fetch_executors()`-returns-`None` (unreachable) path.

Gaps found and filled (3 new tests added to `TestRevealExecutorMetrics` in
`tests/unit/test_annotation_routes.py`):
- `test_empty_executor_list_shows_no_executors_message` -- `fetch_executors()` reachable but returning
  `[]` (e.g. app just started) is a distinct code path from "unreachable" (`None`) and was untested;
  confirmed it renders "No executors reported yet" and *not* the error message.
- `test_malformed_executors_shape_shows_clear_message_not_500` -- mirrors the existing `_stage_rows()`
  malformed-shape guard test (issue #13) for `_executor_rows()`; an unexpected shape (dict instead of
  list) from `fetch_executors()` must degrade the same as unreachable, not raise while iterating.
- `test_no_live_application_shows_could_not_reach` -- distinct from the existing "resolve succeeds but
  fetch_executors fails" test: here `_resolve_app_ref()` itself returns `None` (no live app resolves at
  all), and `_executor_rows()` must short-circuit without ever calling `fetch_executors()`. This is the
  scenario the task explicitly asked to confirm, and it was the one gap not already covered.

All 3 new tests pass; full suite is 308/308 passing after the additions
(`py -3 -m pytest tests/unit -q`).

## Blockers / gaps for human attention

1. **Issue #37 (filed, not a hold-the-line blocker but should be fixed before calling this fully done):**
   the notebook's hard GC-fraction assertion is not reliable on every real run (confirmed failing on this
   pass's live trial), and its failure strands the cluster's entire executor capacity because
   `.stop()` is unreached. Suggested fix already included in the issue.
2. Only one full live notebook trial was completed for this pass (a second attempt was blocked by the
   resource-leak side effect of finding #1 and was aborted/cleaned up rather than completed) -- this
   report's "5 trials reliably favor right-sizing" cross-check is therefore a single live data point
   contradicting that claim, not a fresh 5-trial study. A human may want more trials before deciding
   whether issue #37's fix should also soften the doc's "reliably favors" language for GC-fraction, or
   whether this was an unlucky one-off run.

## Cleanup confirmation

- `docker ps --filter "name=spark-"` returned empty after `POST /topics/executor-tuning/teardown` --
  cluster torn down.
- Both notebook kernels killed via `DELETE /api/kernels/<id>` before teardown; `GET /api/kernels`
  confirmed empty.
- `git status --porcelain` shows only the intended test-file change
  (`tests/unit/test_annotation_routes.py`) plus this report -- `content/executor-tuning/notebook.ipynb`
  was never modified by this run (no live execution writes back into the `.ipynb` file itself; the kernel
  API executes cells without touching `execution_count`/`outputs` in the committed file), confirmed via
  `git diff main -- content/executor-tuning/notebook.ipynb` being empty.

## Recommendation

This is a **recommendation, not final sign-off** -- the human should review, especially issue #37 (does
the assertion-reliability/resource-leak bug need to block sprint close-out for #34, or can it be deferred
to a follow-up within the same sprint?), before marking US-C3 done.

## Addendum, 2026-07-17 -- live re-check of issue #37's fix (commit `05c473a`)

Re-ran this validation against `05c473a` (`fix(executor-tuning): stop rightsized session before assert,
soften GC-fraction claim`, `worktree-issue-34-executor-tuning`), which the developer verified only
statically (another worktree held the shared Docker container names at the time). `docker ps -a` was
empty before starting, so this pass had a real cluster to itself.

**Method:** same as above -- cluster spawned via `POST /topics/executor-tuning/spawn` with this topic's
own `cluster_defaults` (3 workers/4 cores/8GB/200 shuffle partitions/AQE off), notebook's 7 code cells
executed in file order against a fresh JupyterLab kernel via the REST/WebSocket API (unmodified `.ipynb`
on disk), reusing the same driver script as the prior pass.

**Result: issue #37 is resolved.**

- All 7 code cells completed `ok`, including the former cell 13 (now the notebook's last code cell) --
  no `AssertionError` anywhere. This run's numbers happened to land in the heuristic's predicted direction
  (fat=0.0266 > right-sized=0.0180), unlike the prior run that triggered #37, but the fix no longer
  depends on direction: the cell prints `"...came out {higher|NOT higher} than..."` either way and only
  hard-asserts on executor counts (3 fat / 6 right-sized) and wall-clock non-degeneracy, confirmed by
  reading the executed source and its clean pass.
- Confirmed via the Spark master's REST API (`http://127.0.0.1:8080/json/`) immediately after the last
  cell finished: `activeapps: []`, both `executor-tuning-fat` and `executor-tuning-rightsized` show up
  only in `completedapps` with state `FINISHED`, and all 3 workers report `coresused=0`/`memoryused=0`.
  No stray application held executor capacity -- `spark_rightsized.stop()` running before the assertions
  (source-confirmed: it's now the first statement after the results are printed, ahead of every `assert`)
  genuinely releases the cluster, not just in theory.
- `py -3 -m pytest tests/unit -q` -> **308 passed**, unchanged from before the fix (no app code touched
  by `05c473a`, as expected).
- Cleanup: kernel deleted, `POST /topics/executor-tuning/teardown` returned `state: idle`, `docker ps -a`
  empty afterward. `git status --porcelain` and `git diff -- content/executor-tuning/notebook.ipynb` both
  empty; every code cell still has `execution_count: null` and `outputs: []` -- the live run did not
  disturb the committed notebook file.

No new issues found. Issue #37 can be closed once the human gives final sign-off.
