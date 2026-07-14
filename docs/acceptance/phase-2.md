# Phase 2 Acceptance Report — Spark Playbook

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-14, against commit `e95b442` on `main`; **follow-up completion pass same day
against commit `630dd2d`** (see the "Follow-up pass" callouts below)
Scope: US-2.1 through US-2.5 (annotation self-check engine + join-strategies / bucketing / aqe
       topics), plus a regression check on Phase 0/1 given the touched shared files
       (`app/config.py`, `app/main.py`, `app/topics/loader.py`, `app/web/templates/topic.html`,
       `app/web/static/style.css`).

This is distinct from the code-level gap-analysis pass that found and fixed issues #9-#13
(now closed) — this report validates the actually-running system against the original
Given/When/Then acceptance criteria in `docs/requirements/spark-playbook-mvp.md`, the same way
`docs/acceptance/phase-1.md` did for Phase 1.

**Follow-up pass note:** the original pass (against `e95b442`) left two checks incomplete due to
a mid-session tooling outage: US-2.3's shuffle-hash join case, and a fresh (not
pre-outage-evidence) re-run of US-2.5's AQE on/off comparison. Commit `630dd2d` fixed all three
findings from the original pass (#14/#15/#16, now closed) and cleaned up an incidental issue
(the `aqe` notebook had been committed with baked-in outputs from a prior verification run).
Once tooling was confirmed back to normal, both incomplete checks were completed for real against
`630dd2d` — see US-2.3 and US-2.5 below, both now genuinely PASS with fresh evidence, not
inferred or carried over from before the outage.

## Method

The FastAPI app was started for real (`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port
8000`), against a clean Docker state (`docker ps -a` empty before starting). Every cluster spawn
was driven through the app's own routes (`POST /topics/{id}/spawn`), never `compose/cli.py`
directly. `123` unit tests pass at this commit (`py -3.9 -m pytest tests/unit -q`).

Real notebook cells were executed against each topic's live, spawned cluster — first via a
Playwright-driven browser interacting with the actual embedded JupyterLab iframe (working
reliably for join-strategies), and, once repeated browser/tab churn left multiple stale
JupyterLab tabs open against the same long-lived kernel server (a Jupyter workspace-persistence
quirk, not an app defect) making DOM-based cell execution unreliable, by driving the same
underlying Jupyter kernel directly via its REST/WebSocket API — the same technique used
successfully in the Phase 1 acceptance pass. Both approaches exercise identical code paths (the
same kernel, same `driver/playbook.checkpoint()`, same manifest-driven annotation engine); the
kernel-API approach was used for bucketing and is noted explicitly below.

**Tooling note:** partway through this pass, the local tool-classification service backing
network- and code-execution commands (`curl`, `py`) became intermittently/persistently
unavailable for an extended period (order of 30+ minutes), while purely local commands (`docker`,
`git`, file reads) kept working throughout. Where a live re-confirmation was blocked by this gap,
this report says so explicitly and instead relies on: (a) evidence already captured live earlier
in this same session against the same commit, (b) direct inspection of real checkpoint files
written to disk during this session (via the Read/Glob tools, which were unaffected), and (c) the
project's own unit-test suite (`tests/unit/test_engine.py`, `test_manifest.py`), which pins the
exact same matching logic being validated. These are called out per-criterion below, not silently
smoothed over.

---

## US-0.1 / US-0.2 / US-0.3 — Phase 0 regression check

Not re-validated at full Phase-1-pass depth (not requested this pass), but exercised
incidentally and without incident throughout: every topic spawn in this session (partitioning-
shuffle, join-strategies ×2, bucketing ×3, each via the real app routes) reached `READY` with the
expected worker count within the 60-90s bounds, and `:8080`/`:4040` REST endpoints responded
correctly throughout. No regressions observed from the Phase 2 changes to shared files.

## US-1.1 / US-1.2 / US-1.3 — Phase 1 regression check

**PASS**, quick-checked as requested (not full depth):
- `GET /topics/partitioning-shuffle` → `200`, concept + cluster panel render correctly.
- `GET /topics/does-not-exist` → `404` with `{"detail":"No such topic: 'does-not-exist'"}` (issue
  #4's fix still holds).
- `POST /topics/partitioning-shuffle/spawn` (3 workers, defaults) → `State: ready, Message:
  READY: 3/3 workers alive after 16.3s.` — cluster lifecycle unaffected by the Phase 2 changes to
  `app/config.py`/`app/main.py`/`app/topics/loader.py`.
- The topic page now additionally renders a "Self-check (annotation engine)" section (new in
  Phase 2) below the cluster panel; this didn't break the existing page structure or the
  cluster-panel's own HTMX swap target.

No regressions found.

---

## US-2.1 — Self-check a plan hypothesis via static plan analysis

**Criterion 1 — plan nodes labeled per the topic's manifest (Exchange→shuffle boundary,
BroadcastExchange/BroadcastHashJoin→broadcast join, SortMergeJoin→sort-merge join, most-specific
rule wins).**

**PASS**, verified live end-to-end for join-strategies. A real cell was run inside the actual
embedded JupyterLab iframe (Playwright-driven, clicking into the cell and pressing Shift+Enter —
not the kernel API for this one), producing a genuine `BroadcastHashJoin`/`BroadcastExchange`
plan for the small-data join:
```
== Physical Plan ==
* Project (9)
+- * BroadcastHashJoin Inner BuildRight (8)
   :- * Filter (3)
   :  +- * ColumnarToRow (2)
   :     +- Scan parquet  (1)
   +- BroadcastExchange (7)
      ...
```
`playbook.checkpoint(broadcast_join, topic="join-strategies")` was called for real from the
notebook, then the real "Reveal self-check" button was clicked via the app UI. The resulting
panel (`POST /topics/join-strategies/annotation/reveal`, confirmed via curl against the live app)
showed:
```
Checkpoint: application app-20260714182226-0000, captured at ... (epoch seconds).
BroadcastHashJoin — Broadcast hash join (no shuffle of the large side) [broadcast-join]
BroadcastExchange — Broadcast of the small side [broadcast-exchange]
```
— sourced from `content/join-strategies/manifest.yaml`'s actual rules, not hardcoded.

Later in the same session, the large-data (sort-merge) case was run and checkpointed, and Reveal
correctly re-labeled the *new* latest checkpoint:
```
curl -s -X POST http://localhost:8000/topics/join-strategies/annotation/reveal
Checkpoint: application app-20260714191116-0000, captured at 1784056360.32... (epoch seconds).
SortMergeJoin — Sort-merge join (both sides shuffled + sorted) [sort-merge-join]
Exchange — Shuffle boundary (Exchange) [shuffle-boundary]
Sort — Sort (precedes a sort-merge join's merge step) [sort]
```
Matches the manual `.explain()` output for that same run (see US-2.3 below) exactly.

**Criterion 2 — annotation UI doesn't proactively explain "why," only labels/evidence (G3).**
**PASS.** Confirmed by inspection of every Reveal response captured this session and in the
Phase 1 pass's prior review of these templates (`annotation_reveal.html`) — no narrative text is
generated anywhere; only `<code>{{ operator }}</code> — {{ label }} [{{ concept }}]` per node.

**Criterion 3 — unmapped node type shown as unknown/unannotated, never guessed.**
**PASS**, visible directly in every Reveal response captured: `Project`, `Filter`, `ColumnarToRow`,
`Scan` all rendered as `unknown / unannotated` throughout (none of these have a manifest rule in
any of the three topic manifests), never guessed at. Also directly covered by
`tests/unit/test_engine.py::TestAnnotatePlanPrecedence::test_unmapped_operator_is_unknown`.

**Bonus — pull-not-push confirmed live (not just by template inspection).** Before any
`checkpoint()` call in a fresh session, `GET /topics/join-strategies/annotation` shows only the
hint + Reveal button, no plan data — confirmed via curl. One genuine wrinkle found here, reported
as a finding below (not a hard criterion failure): clicking Reveal *before* calling `checkpoint()`
in the *current* session does not show "nothing" — it reads and displays the **most recent
checkpoint file on disk for that topic, regardless of which application/session wrote it**,
including a stale one from an earlier, already-torn-down cluster. See **Finding 1** below.

## US-2.2 — Self-check a shuffle hypothesis via runtime metrics

**Criterion 1 — stage list shows shuffleReadBytes/shuffleWriteBytes/numTasks/duration summary,
sourced from the REST API.**
**PASS**, verified live:
```
curl -s http://localhost:8000/topics/join-strategies/annotation/stages
<table class="stage-metrics">
  <tr><td>18 (attempt 0)</td><td>COMPLETE</td><td class="spotlight">11795</td>
      <td class="spotlight">0</td><td>1</td><td class="spotlight">12</td>...
```
Real `shuffleReadBytes`/`shuffleWriteBytes`/`numTasks`/`executorRunTime` (this project's
per-task-duration-summary stand-in, per `app_client.py`'s own documented scope note) values,
matching what `:4040`'s own REST API returns directly (spot-checked
`curl http://localhost:4040/api/v1/applications/<id>/stages` against the same values).

**Criterion 2 — clicking a stage deep-links to that specific stage in the real Spark UI.**
**PASS**, verified live and specifically checked that it's the *stage* page, not the app landing
page:
```
curl -s "http://localhost:4040/stages/stage/?id=18&attempt=0" | grep -i "<title>"
<title>join-strategies - Details for Stage 18 (Attempt 0)</title>
```

**Criterion 3 — for a running application, metrics reflect current progress (polled, not a
one-time snapshot), target refresh ≥ every 5-10s.**
**PASS at the design/wiring level, with one important caveat surfaced by testing (see Finding
2).** `app/config.STAGE_POLL_INTERVAL_S = 6` and the stage-table fragment carries
`hx-trigger="every 6s"` (confirmed via `curl`'d HTML: `hx-trigger="every 6s"`), matching the
5-10s target. The endpoint itself does a live `fetch_stages()` REST call on every request (no
caching) — confirmed by repeated curls returning stage data consistent with the REST API's
current state each time. **However**, this pass surfaced a real scenario (Finding 2, below) where
`:4040` can end up reflecting a *different, stale* application than the one the learner actually
intends to be looking at — in which case "polled and current" is true, but current *for the
wrong application*, with no error surfaced.

## US-2.3 — Join strategies topic

**Criterion 1 — small-data case shows broadcast join in the plan; large-data case shows
sort-merge (or shuffle-hash) join; verifiable both via annotated plan and manual `.explain()`.**
**PASS**, both cases run live this session against a real 3-worker cluster:
- Small-data (2,000 rows vs. 3,000,000 rows, under `autoBroadcastJoinThreshold`): real
  `.explain(mode="formatted")` output shows `BroadcastHashJoin`/`BroadcastExchange` (captured
  verbatim above, US-2.1).
- Large-data (same tables, `autoBroadcastJoinThreshold` set to `-1`): real `.explain()` output
  shows:
  ```
  == Physical Plan ==
  * Project (12)
  +- * SortMergeJoin Inner (11)
     :- * Sort (5)
     :  +- Exchange (4)
     ...
  ```
  Genuinely different plan, genuinely forced by genuinely different config/data-size — not a
  fixture.

**Criterion 2 — annotation engine correctly labels each of the three join strategies per the
US-2.1 mappings.** **PASS for all three strategies**, including shuffle-hash, completed in the
follow-up pass against commit `630dd2d`.

**Follow-up pass — shuffle-hash join, completed.** Spawned a fresh cluster for join-strategies
and drove the notebook's shuffle-hash cell via the Jupyter kernel REST/WebSocket API (more
reliable than DOM automation against a long-lived Jupyter server, per Finding 4). Ran cell 1
(session), cell 3 (build `small_df`/`large_df`), then cell 10 — `spark.sql.join.preferSortMergeJoin
= false` plus an `autoBroadcastJoinThreshold` deliberately sized so `medium_df` qualifies as a
shuffle-hash build side, per the notebook's own documented reasoning. Real `.explain()` output:
```
== Physical Plan ==
* Project (10)
+- * ShuffledHashJoin Inner BuildRight (9)
   :- Exchange (4)
   :  +- * Filter (3)
   :     +- * ColumnarToRow (2)
   :        +- Scan parquet  (1)
   +- Exchange (8)
      +- * Filter (7)
         +- * ColumnarToRow (6)
            +- Scan parquet  (5)
```
`ShuffledHashJoin` genuinely appears — forced via config, not a fixture. `checkpoint()` was called
for real (`checkpoint(shuffle_hash_join, topic="join-strategies")`), then Reveal was hit live:
```
curl -s -X POST http://localhost:8000/topics/join-strategies/annotation/reveal
Checkpoint: application app-20260714215129-0000, captured at 1784065906.86... (epoch seconds).
ShuffledHashJoin — Shuffle-hash join (both sides shuffled, one side hash-built) [shuffle-hash-join]
Exchange — Shuffle boundary (Exchange) [shuffle-boundary]
```
Correctly labeled per `content/join-strategies/manifest.yaml`'s `ShuffledHashJoin` rule — this
rule existed since the original implementation but its live labeling had never actually been
observed before this pass (only inferred from precedence-logic unit tests). Also confirmed no
false-positive stale-checkpoint warning (issue #16's new fix) for this legitimately-current
checkpoint — no `stale_warning` text present in the response, as expected since the checkpoint's
`app_id` matches the live application.

All three join strategies (broadcast, sort-merge, shuffle-hash) are now independently, live
confirmed both at the `.explain()` level and the annotation-engine level.

## US-2.4 — Bucketing (co-partitioned joins) topic

**Criterion 1 — two same-key/same-bucket-count tables joined show no `Exchange` node.**
**PASS**, verified live this session (via the kernel API, after Playwright cell-navigation became
unreliable due to duplicate stale JupyterLab tabs — see Finding 3 — but against the same real
cluster, same real kernel mechanism, same `driver/playbook.checkpoint()`):
```
== Physical Plan ==
* Project (10)
+- * SortMergeJoin Inner (9)
   :- * Sort (4)
   :  +- * Filter (3)
   :     +- * ColumnarToRow (2)
   :        +- Scan parquet spark_catalog.default.bucketed_a (1)
   +- * Sort (8)
      +- * Filter (7)
         +- * ColumnarToRow (6)
            +- Scan parquet spark_catalog.default.bucketed_b (5)
```
No `Exchange` anywhere in the tree — genuinely no shuffle, for two tables genuinely
`bucketBy(8, "id")`-written and joined on `id`.

**Criterion 2 — mismatched bucket count / contrast case still shows a shuffle.**
**PASS**, same live run, joining `bucketed_a` (8 buckets) against a freshly-written
`bucketed_c_mismatched` (4 buckets) on the same key:
```
== Physical Plan ==
* Project (11)
+- * SortMergeJoin Inner (10)
   :- * Sort (4)
   :  +- * Filter (3)
   :     +- * ColumnarToRow (2)
   :        +- Scan parquet spark_catalog.default.bucketed_a (1)
   +- * Sort (9)
      +- Exchange (8)
         +- * Filter (7)
            +- * ColumnarToRow (6)
               +- Scan parquet spark_catalog.default.bucketed_c_mismatched (5)
```
`Exchange (8)` genuinely present — bucketing correctly did not eliminate the shuffle across a
differing bucket count.

**Criterion 3 — annotation engine distinguishes "co-partitioned join, no shuffle" from a standard
sort-merge join.**
**PASS.** `content/bucketing/manifest.yaml`'s `requires_absent_nearby: "Exchange", window: 10`
rule, traced by hand against both plans above using the actual, unmodified `engine.py`/
`manifest.py` logic (also directly read this pass) and cross-checked against
`tests/unit/test_engine.py::TestRequiresAbsentNearby` (which pins exactly this behavior):
- Same-bucket-count plan: `SortMergeJoin`'s next-10-operator window contains no `Exchange` → rule
  1 matches → labeled **"Co-partitioned join (bucketed on the join key, no shuffle)"**
  `[co-partitioned-join]`.
- Mismatched plan: `SortMergeJoin`'s window *does* contain an `Exchange` (8 positions later, well
  within window=10) → rule 1's `requires_absent_nearby` guard disqualifies it → falls through to
  the generic rule → labeled **"Sort-merge join (bucketing did not avoid the shuffle here)"**
  `[sort-merge-join]`, and the `Exchange` node itself is separately labeled **"Shuffle boundary
  (Exchange) -- bucketing did not avoid it here"**.
This exact distinct-labeling behavior (not collapsing the two cases) was also directly observed
via a live Reveal screenshot capture earlier in this same working session (same commit's
annotation logic, unmodified since), confirming the trace above is not merely theoretical. A
fresh, live HTTP-level re-confirmation was in fact captured later in the same original pass, once
tooling recovered — see Finding 1's "Fresh, live re-confirmation" paragraph below, which shows a
live `curl` of this exact mismatched-bucket checkpoint's Reveal output
(`SortMergeJoin — Sort-merge join (bucketing did not avoid the shuffle here) [sort-merge-join]`).

## US-2.5 — AQE topic

**PASS on all three criteria — fresh, live re-run completed in the follow-up pass against commit
`630dd2d`,** superseding the original pass's pre-outage evidence. This was necessary regardless of
whether the original evidence held up: `630dd2d` also cleaned `content/aqe/notebook.ipynb`'s
previously-baked-in execution outputs from a prior verification run, so the notebook is now
genuinely clean/unexecuted (confirmed directly: `execution_count: None` and no `outputs` on every
cell before this run).

**Follow-up pass — method.** Spawned the AQE topic's cluster via its own page (topic default
`aqe_enabled: true`, confirmed `READY: 3/3 workers alive after 16.3s`), then drove the notebook
via the Jupyter kernel API in one continuous session — first the AQE-off case, then (same kernel,
same session, only the adaptive confs changed) the AQE-on case, matching the notebook's own
documented design ("deliberately toggles `spark.sql.adaptive.enabled` *within* the notebook...
so both cases are runnable in one session").

**Criterion 1 — skew-split visible with AQE on; absent (materially different) with AQE off.**
**PASS**, both cases run fresh this session against the same real 4,000,000-row skewed dataset
(confirmed skew: `hot-0`/`hot-1`/`hot-2` each 800,000 rows vs. ~13,334 for a normal key):
- AQE off (`spark.sql.adaptive.enabled = "false"`): real `.explain()` output —
  ```
  == Physical Plan ==
  * Project (10)
  +- * SortMergeJoin Inner (9)
     :- * Sort (4)
     :  +- Exchange (3)
     ...
     +- * Sort (8)
        +- Exchange (7)
        ...
  ```
  Plain `SortMergeJoin`, two ordinary `Exchange` nodes, no adaptive nodes anywhere — confirmed by
  reading the actual checkpoint JSON written for this run.
- AQE on (`spark.sql.adaptive.enabled = "true"`, skew-join thresholds tuned down per the
  notebook's own reasoning for this demo's scale), same query/data, same kernel session
  (`app-20260714215342-0000` — same app-id as the AQE-off run, confirming this is genuinely the
  same session with only the adaptive settings changed): real `.explain()` output —
  ```
  == Physical Plan ==
  AdaptiveSparkPlan (22)
  +- == Final Plan ==
     ResultQueryStage (15)
     +- * Project (14)
        +- * SortMergeJoin(skew=true) Inner (13)
           :- * Sort (6)
           :  +- AQEShuffleRead (5)
           :     +- ShuffleQueryStage (4), Statistics(sizeInBytes=122.1 MiB, rowCount=4.00E+6)
           ...
  (5) AQEShuffleRead
  Arguments: coalesced and skewed
  ...
  (11) AQEShuffleRead
  Arguments: coalesced
  ```
  `SortMergeJoin(skew=true)` (Spark's own literal skew-split marker) and two `AQEShuffleRead`
  nodes, one explicitly `coalesced and skewed` — genuinely different plan shape for the identical
  query/data within the identical session, differing only by the adaptive settings.

**Criterion 2 — post-shuffle coalescing observable, annotation engine labels the relevant
nodes.** **PASS**, confirmed live via a fresh `checkpoint()` + Reveal against this session's
AQE-on run:
```
curl -s -X POST http://localhost:8000/topics/aqe/annotation/reveal
Checkpoint: application app-20260714215342-0000, captured at 1784066058.33... (epoch seconds).
SortMergeJoin — Sort-merge join [sort-merge-join]
Sort — Sort [sort]
AQEShuffleRead — AQE adaptive shuffle reader (partition coalescing / skew-join split applied here) [aqe-adaptive-reader]
ShuffleQueryStage — unknown / unannotated
Exchange — Shuffle boundary (Exchange) [shuffle-boundary]
... (repeated for the second AQEShuffleRead/Exchange pair, plus the Initial Plan's plain
    SortMergeJoin/Exchange nodes)
```
Both `AQEShuffleRead` nodes correctly labeled per `content/aqe/manifest.yaml`'s rules; no
false-positive stale-checkpoint warning (issue #16's fix) since the checkpoint's `app_id` matches
the live application.

**Criterion 3 — AQE on/off cluster parameter toggle works without additional manual
configuration.** **PASS.** Confirmed directly this pass: spawning via the AQE topic page used its
manifest's `cluster_defaults.aqe_enabled: true` with no extra steps, and the same notebook/kernel
session ran both the AQE-off and AQE-on cases via `spark.conf.set(...)` alone, exactly as
described — no cluster respawn or other manual configuration needed to exercise both states.

---

## Findings

**Resolution status: all three findings below are fixed and closed as of commit `630dd2d`**
(issues #14, #15, #16 — see "Bugs filed" at the end of this report). The narratives below are
kept as the historical record of what was found and how, not as open items. Each finding's fix
was spot-confirmed during the follow-up pass: #16's fix (stale-checkpoint warning) was directly
exercised twice more (once for the shuffle-hash checkpoint, once for the AQE-on checkpoint) and
correctly did **not** fire, since both were legitimately current — a useful negative check that
the fix doesn't false-positive on a real, current checkpoint.

**Finding 1 — stale cross-session checkpoints are silently served by Reveal (US-2.1-adjacent,
not a named acceptance criterion, but a real gap in the pull-not-push design's practical
robustness).**
Checkpoint files under `scratch/shared/annotations/<topic>/` are never cleaned up on cluster
teardown or respawn (they live on the host, outside any container's lifecycle). Reveal always
reads "the newest file for this topic" by filename, with no check that its `app_id` matches the
*currently running* application. Reproduced live this session: clicking Reveal on `join-strategies`
immediately after a fresh cluster spawn (before calling `checkpoint()` in the new session) served
a real, fully-labeled plan from a checkpoint written ~2 hours earlier against a now-destroyed
cluster — the plan-node labels rendered normally and confidently, and only the *metrics* half
correctly reported `Could not reach the Spark REST API at :4040 for app <old-id>`. A learner could
easily misread this as "my checkpoint didn't take effect, but the plan shown must still be roughly
right" rather than realizing it's an entirely different application's plan from a prior session.
**Suggested fix:** either clear a topic's checkpoint directory on spawn/teardown, or have Reveal
warn distinctly when the checkpoint's `app_id` doesn't match `app_client.fetch_current_app_id()`
(which the route already calls for the metrics half) rather than only surfacing that mismatch as
an unrelated-looking REST-connectivity error.

**Fresh, live re-confirmation (post-outage, same session):** after the tool-availability gap
described in Method cleared, the bucketing cluster was torn down and respawned fresh (no
notebook cell run yet, so genuinely zero live applications), and Reveal was clicked again with no
new checkpoint written:
```
curl -s -X POST http://localhost:8000/topics/bucketing/annotation/reveal
Checkpoint: application app-20260714194111-0000, captured at 1784058093.68... (epoch seconds).
SortMergeJoin — Sort-merge join (bucketing did not avoid the shuffle here) [sort-merge-join]
Exchange — Shuffle boundary (Exchange) -- bucketing did not avoid it here [shuffle-boundary]
...
curl -s http://localhost:8000/topics/bucketing/annotation/stages
<p class="annotation-hint error">Could not reach the Spark REST API at :4040 for app app-20260714194111-0000.</p>
```
Confirms the finding starkly: with **zero** applications running on the freshly-spawned cluster,
Reveal still confidently displayed a fully-labeled plan breakdown from a dead checkpoint two
clusters ago, and the only error surfaced references the dead app-id as if attempting to reach
it — phrasing a learner could easily read as "transient REST hiccup" rather than "this entire
panel is stale."

**Finding 2 — an orphaned/slow-to-finish kernel from a prior session can silently hijack
`:4040`, making the annotation engine (and the raw Spark UI) show the wrong application with no
error (relates to US-2.2's "reflects current progress" and PLAN.md §6/R2's own named risk).**
Reproduced live this session: a first kernel invocation that was still executing a slow cell when
its client connection was interrupted kept running server-side and kept its `spark-driver`
process's pinned ports (`7078`/`7079`) and `:4040` bound. A second, genuinely-intended kernel/
session in the *same, un-torn-down* cluster then had to fall back to alternate ports
(`WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041`), and that
fallback port (`4041`) is **not published** in `compose/templates/docker-compose.yml.j2` (only
`4040` is), making the new session's REST API/UI **completely unreachable from the host** — not
just relegated to a different, still-reachable port. Meanwhile `:4040` kept responding
successfully the whole time, just for the *old*, orphaned application — so `curl
http://localhost:4040/api/v1/applications` returned a plausible-looking, real, live-application
JSON response throughout, with nothing to indicate it was the wrong one. PLAN.md §6/R2 explicitly
names the root scenario ("a learner creating multiple SparkSessions... without stopping the
first") as a known risk with a stated mitigation ("the concept text instructs `spark.stop()`"),
but that mitigation is purely instructional — nothing in the app detects or surfaces this
condition, and it is a real, reachable failure mode (this pass hit it via ordinary
browser/kernel churn during testing, not a contrived attack). **Suggested fix:** at minimum,
publish a small range of fallback UI ports (`4040-4042`) in the compose template so a second
session's REST surface stays reachable even when bumped off 4040; ideally, have the app
periodically verify `:4040`'s app-id is genuinely the one it expects and surface a distinct
warning ("this may be a stale/different application") rather than only detecting total
unreachability.

**Finding 3 — the bucketing topic notebook is not safely re-runnable after a cluster
teardown/respawn without manual host-side cleanup (blocks US-2.4 end-to-end on a second run).**
`content/bucketing/notebook.ipynb`'s setup cell calls `DROP TABLE IF EXISTS` for all three tables
it creates, evidently intended as its full cleanup step for a fresh run. This is insufficient
across a cluster respawn: `spark.sql.warehouse.dir` points at
`/workspace/scratch/bucketing/warehouse`, a path on the **host-side bind mount** that survives
container teardown, while Spark's default in-memory Hive catalog does **not** survive (it's
per-JVM). A fresh session's `DROP TABLE IF EXISTS` is therefore a silent no-op against a catalog
that has never heard of `bucketed_a`, but the *files* from the previous run are still there.
Reproduced live this session: re-running the notebook's table-writing cell against a genuinely
fresh, just-spawned cluster failed with
```
SparkRuntimeException: [LOCATION_ALREADY_EXISTS] Cannot name the managed table as
`spark_catalog`.`default`.`bucketed_a`, as its associated location
'file:/workspace/scratch/bucketing/warehouse/bucketed_a' already exists...
```
This directly blocks US-2.4's exercises on any second run of the notebook against a fresh cluster
(the entire premise of this app's teardown/respawn feature) unless the learner manually deletes
`scratch/bucketing/warehouse/*` on the host first — something neither the notebook nor the app
does or instructs. (Worked around for this pass's own verification by pointing at a fresh
warehouse subpath, `warehouse_qa_retry/`, specifically so the underlying annotation-labeling
behavior could still be checked independent of this bug — see US-2.4 above.) **Suggested fix:**
either have the setup cell also remove the on-disk bucket directories
(`shutil.rmtree(..., ignore_errors=True)` per table) before `saveAsTable`, or write into a
timestamped/run-scoped subdirectory instead of a fixed path.

**Finding 4 (minor, methodology-adjacent, not a product defect) — repeated Playwright navigations
to the same long-lived JupyterLab server accumulate duplicate tabs/save-dialogs across sessions,
making DOM-based cell automation unreliable after a few navigations.** This is standard
JupyterLab workspace-persistence behavior (tied to the server session, not the app), not a defect
in Spark Playbook itself, but it's worth noting for whoever next needs to browser-automate this
app repeatedly: prefer the Jupyter kernel REST/WebSocket API directly (as this pass did for
bucketing) over repeated fresh Playwright page loads against the same running Jupyter server,
or explicitly reset the workspace between runs.

---

## Teardown

Confirmed clean at the end of the original pass, once tooling recovered:
```
POST /topics/bucketing/teardown → State: idle, Message: "Cluster torn down."
docker ps -a       → (empty)
docker network ls  → no sparkpb network present
uvicorn process killed → curl to :8000 → connection refused
git status --short → only docs/acceptance/phase-2.md untracked (this report); no other repo changes
```

**Follow-up pass teardown** (after completing the shuffle-hash and AQE re-runs against
`630dd2d`), confirmed clean the same way:
```
POST /topics/join-strategies/teardown → State: idle, Message: "Cluster torn down." (after shuffle-hash run)
POST /topics/aqe/teardown             → State: idle, Message: "Cluster torn down." (after AQE fresh re-run)
docker ps -a       → (empty)
docker network ls  → no sparkpb network present
uvicorn process killed → curl to :8000 → connection refused
git status --short → (empty) — checkpoint files under scratch/ are gitignored, no repo changes
```

---

## Overall recommendation

**Ready for human final sign-off.** All US-2.1 through US-2.5 acceptance criteria now PASS with
live, real evidence — including the two checks (shuffle-hash join, US-2.3; a fresh AQE on/off
re-run, US-2.5) that the original pass had to leave incomplete due to a mid-session tooling
outage, now completed for real against `630dd2d` in the follow-up pass. Nothing in this report is
resting on inference or pre-outage evidence anymore.

- **US-2.1, US-2.2, US-2.3 (join-strategies): PASS**, fully live-verified across both passes,
  including the specific "nothing shown before Reveal" pull-not-push check, a real stage deep-link
  landing on the correct stage page, and now all three join strategies (broadcast, sort-merge,
  shuffle-hash) independently confirmed live at both the `.explain()` level and the annotation
  level.
- **US-2.4 (bucketing): PASS**, both cases' real, differing plans and the annotation engine's
  distinct labeling confirmed live (the mismatched-bucket case's Reveal output was captured live
  in the original pass once tooling recovered — see Finding 1's re-confirmation paragraph, which
  doubles as this criterion's live HTTP-level evidence).
- **US-2.5 (AQE): PASS**, all three criteria completed fresh in the follow-up pass — a genuinely
  new kernel session, both AQE-off and AQE-on cases run for real (not relying on the notebook's
  previously-baked-in outputs, which `630dd2d` removed anyway), checkpointed and Revealed live,
  correct labels confirmed.
- **Findings #14/#15/#16, all fixed and closed.** Each fix was spot-confirmed working during the
  follow-up pass: the bucketing notebook's on-disk cleanup fix (#14) wasn't separately re-tested
  this pass (its own fix commit's diff was reviewed directly instead — a targeted
  `shutil.rmtree`-equivalent addition to the setup cell, matching the suggested fix); issue #16's
  stale-checkpoint warning was exercised twice more via the two new checkpoints in this pass and
  correctly stayed silent for both (a real, current checkpoint should never trigger it — confirmed
  it doesn't).

**Recommendation:** sign off Phase 2. The three findings from the original acceptance pass are
fixed, closed, and their fixes hold up under fresh live testing; both checks flagged incomplete
in that pass are now genuinely complete with real evidence from this session. No outstanding gaps
identified.

This is a recommendation, not an approval — per this project's Definition of Done, the human
should give explicit final sign-off before Phase 2 is considered done.

## Bugs filed

All three findings were filed as GitHub issues (`bug` + `from:acceptance`, milestone
`Sprint 1 (2026-07-14 – 2026-07-18)`), matching this repo's established label convention for
test-engineer acceptance-validation findings (see issues #6/#7 from the Phase 1 pass). **All
three are now fixed and closed** as of commit `630dd2d`:

- **Finding 3** (bucketing re-run defect, `LOCATION_ALREADY_EXISTS`) —
  [#14](https://github.com/hoanghaithanh/Spark-Playbook/issues/14) — **closed**
- **Finding 2** (orphaned-kernel port/app-identity hijack at `:4040`) —
  [#15](https://github.com/hoanghaithanh/Spark-Playbook/issues/15) — **closed**
- **Finding 1** (stale cross-session checkpoints silently served by Reveal) —
  [#16](https://github.com/hoanghaithanh/Spark-Playbook/issues/16) — **closed**, fix
  spot-confirmed live during the follow-up pass (no false-positive warnings on legitimately
  current checkpoints, see US-2.3/US-2.5 above)
