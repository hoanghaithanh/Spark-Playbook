# Phase 2.5 Acceptance Report — Realtime Cluster Monitoring Dashboard

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-15, against commit `3eff2d5` on `main` (the fix commit for issues #18-#21, on top of
      `77dac82`'s original Phase 2.5 implementation)
Scope: US-5.1 through US-5.6 (`docs/requirements/realtime-monitoring-dashboard.md`), D-A signal-only
       compliance (`docs/architecture/realtime-monitoring-dashboard.md`), a visual comparison against
       `docs/architecture/realtime-monitoring-dashboard-mockup.dc.html`, and independent re-verification
       of issues #18-#21's fixes against a real running cluster.

## Method

The FastAPI app was already running (`py -3.9 -m uvicorn app.main:app`) at session start; `docker ps -a`
was cleared of a leftover cluster from a prior session before beginning. All cluster spawns/teardowns
this pass went through the app's own routes (`POST /topics/{id}/spawn` / `/teardown`), never
`compose/cli.py` directly. `199` unit tests pass at this commit (`py -3.9 -m pytest tests/unit -q`).

A real skewed job was run against a live 3-worker cluster using the `aqe` topic's own
`content/aqe/notebook.ipynb` (4,000,000-row dataset, 3 hot keys holding 60% of rows), driven via the
Jupyter kernel REST/WebSocket API directly (same technique used successfully in the Phase 1/2 acceptance
passes) rather than DOM automation, to avoid the stale-tab JupyterLab fragility noted in the Phase 2
report's Finding 4. The dashboard itself (`/dashboard`, `/dashboard/stream`) was exercised both via
`curl` (HTTP-level checks, SSE payload inspection) and via a real headless-Chromium browser (Playwright),
including DOM-level assertions and screenshots, since this feature's SSE mechanism and client-side view
switching specifically need real-browser verification, not just a 200 status code.

**Housekeeping note:** two duplicate kernel/`SparkSession` invocations were accidentally started
concurrently partway through this pass (an artifact of this session's own driving script, not an app
defect) and appeared to contend for the cluster's 6 total cores without ever completing; they were
identified via Jupyter's `/api/kernels` and cleanly deleted (`DELETE /api/kernels/<id>`) once diagnosed,
with no effect on the app or cluster's own state. Noted for transparency, not filed as a finding.

---

## US-5.1 — Live per-node resource utilization (master, workers, and driver)

**Criterion 1 — cluster up, no job: CPU%/RAM used-limit shown for master, every worker, and the driver,
sourced from Docker.**
**PASS**, verified live. `GET /dashboard` immediately after a fresh 3-worker spawn (`aqe` topic) showed
all 5 nodes (`spark-master`, `spark-worker-1/2/3`, `spark-driver`) each with a real `0%` CPU / real RAM%
reading, MASTER badge on the master card only, and the driver card present and populated — not omitted
or blank:
```
spark-master   CPU 0%  RAM (nonzero, idle baseline)
spark-worker-1 CPU 0%  RAM ...
spark-worker-2 CPU 0%  RAM ...
spark-worker-3 CPU 0%  RAM ...
spark-driver   CPU 0%  RAM ...
```
GC time correctly showed `—` for every node (no application running yet) — matches the ADR's "cluster
up, no job" state spec exactly. Confirmed via `docker inspect`/`docker stats` spot-check that these are
genuinely live Docker-sourced numbers, not placeholders.

**Criterion 2 — job running: CPU/RAM update to reflect current load, not a static snapshot.**
**PASS**, verified live during the skewed AQE job. Driver RAM climbed to `84%` (genuinely elevated vs.
its earlier idle baseline, consistent with the join materializing/`foreach`-forcing a large shuffled
dataset) while master stayed near-idle at `15%` — real, differentiated values across nodes, not a frozen
snapshot. Node cards' RAM% differed per worker (`32%`/`30%`/`34%`) matching each worker's own actual
partition load, confirming per-container (not cluster-aggregate) sourcing.

**Criterion 3 — a stopped/removed container reflects unavailability, not frozen last-known values.**
**PASS**, verified live and directly: `docker stop spark-worker-3` was run mid-session while the
dashboard was open, and the very next `/dashboard` render showed:
```html
<div style="font-size:12.5px;color:#8b93a3;padding:10px 0;">Container not available (stopped or removed).</div>
```
for `spark-worker-3`'s card — an explicit unavailability state, not a stale CPU/RAM reading. The worker
was restarted afterward and rejoined normally on the next collector cycle.

**US-5.1: PASS, all 3 criteria.**

---

## US-5.2 — Live per-node task/partition execution detail

**Criterion 1 — per-task breakdown (executor/worker id, duration, input/shuffle bytes) grouped by
executor, sourced from the stage task-list REST endpoint.**
**PASS**, verified live: the Job Detail view's "Partition distribution" table showed a real 162-row
breakdown (`p-000` .. `p-161`) for the AQE job's largest stage, each row carrying Node, Partition id,
Size, Rows, Shuffle R/W, Time, and Retries — all real numbers pulled from
`/api/v1/applications/<id>/stages/<id>` with task details, not fixtures (verified the numbers move
between samples as the job progressed).

**Criterion 2 — task counts/sizes per executor visible together, imbalance apparent without an
accompanying conclusion.**
**PASS**, verified live: the node grid showed "Partitions handled" side-by-side for all three workers
(`77` / `60` / `63`), directly comparable at a glance, and every skewed partition/node carried only a
factual badge — `Data skew: handling 50.5x avg partition size` — with no "this means X" or "caused by Y"
text anywhere alongside it (also covered by the D-A grep sweep below).

**Criterion 3 — a just-completed stage's summary stays visible (retention = current-or-most-recently-completed).**
**PASS**, verified live: after the AQE job's `foreach()`-forced stage 6 finished, the dashboard continued
showing `Stage 6 / 7`, `Completed` status, and the full 162-partition breakdown for that now-finished
stage — not blanked out or replaced by "no active stage."

**US-5.2: PASS, all 3 criteria.**

---

## US-5.3 — Estimated time remaining for the running stage

**Criterion 1 — ETA computed from completed-task average × remaining count, visibly labeled as an
estimate.**
**PASS**, verified live: the Job Detail header showed `ETA ~0s` alongside the job's real elapsed time
(`1m 35s`) once the stage had (mostly) completed tasks to average from — a genuine derived figure, not a
raw REST field (Spark's REST API has no such field, per the ADR's measurability note).

**Criterion 2 — zero completed tasks in a stage shows "estimating...", not a misleadingly confident
number.**
**Not independently reproduced live this pass** — the demo dataset's stages complete fast enough on a
3-worker cluster that this session could not reliably catch the zero-completed-tasks window before the
first task finished (attempts to catch it via rapid polling during a second concurrent job attempt were
confounded by the duplicate-kernel housekeeping issue noted above, which never produced a usable active
stage). **Verified instead via direct code inspection** of `app/monitoring/eta.py::estimate()` (returns
`EtaResult(estimating=True, eta_label="estimating...", spread_label=None)` whenever
`completed_task_durations_s` is empty, unconditionally — no numeric path exists for the zero-sample case)
**and the dedicated, currently-passing unit test**
`tests/unit/test_eta.py::test_zero_completed_tasks_is_estimating_not_a_number` (re-run this pass,
confirmed passing). This matches the same "inspect the code path + a mocked/unit case if not
reproducible live" standard this task brief itself allows for the imbalance-alert scenario below.

**Criterion 3 — task-duration spread (min/median/max) shown alongside the estimate.**
**PASS**, verified live: the same ETA readout carried `min 0s · median 0s · max 6s` directly next to the
numeric estimate, letting the spread be judged independently of the single ETA figure.

**US-5.3: PASS on criteria 1 and 3 (live); criterion 2 verified via code + a dedicated passing unit test
rather than live reproduction — flagged, not a failure.**

---

## US-5.4 — Diagnostic signal surfacing without automated diagnosis

**Criterion 1 — markedly larger task/partition sizes visually apparent, no interpretive text ("this is
skew", "reduce shuffle partitions").**
**PASS**, verified live and via a full-page grep (see the D-A section below): skewed partitions in the
"Partition distribution" table carried a red `SKEW` badge, a red size bar, and a tinted row background —
purely visual/quantified differentiation — with the accompanying `Data skew: handling 50.5x avg
partition size` text naming the measurement only.

**Criterion 2 — one node's saturation vs. another's idleness visible together, no generated
explanation of cause.**
**Live evidence is partial; code-path evidence is complete.** This session's real job was disk/shuffle-
bound rather than CPU-bound on this demo-scale cluster (CPU stayed near 0% on all nodes throughout,
including during the flagged-skew run), so a live CPU-saturation-vs-idle imbalance was not naturally
produced. RAM values *did* visibly differ across nodes simultaneously (`15%` master / `32%`/`30%`/`34%`
workers / `84%` driver, all on one screen) satisfying the "visible together" half of this criterion for
RAM, but the specific CPU-imbalance flagging path (`diagnostics.node_imbalance_reasons()`) was verified
via direct code inspection (already-read `app/monitoring/collector.py`'s `_alert_title_for()` and
`diagnostics.py`'s `node_imbalance_reasons()` — factual-only string, no cause/fix language) plus the
dedicated, currently-passing regression test added for issue #21's fix,
`tests/unit/test_collector.py::TestAlertTitleFormatting::test_alert_title_is_readable_when_flagged_via_cpu_imbalance_not_skew`
(re-run this pass, confirmed passing) rather than a live-triggered CPU-saturation scenario.

**Criterion 3 — no "recommended fix" / tuning-suggestion feature anywhere in the dashboard.**
**PASS**, confirmed by the dedicated D-A sweep below across every rendered surface captured this
session (full page HTML, SSE payload, screenshots).

**US-5.4: PASS on criteria 1 and 3 (live); criterion 2 verified via live RAM evidence + code/unit-test
evidence for the CPU-specific path, not a live CPU-saturation reproduction — flagged, not a failure.**

---

## US-5.5 — Real-time update latency

**Criterion 1/2 — dashboard reflects a stage transition / a container's load change within 5s (ADR
tightens this to ≤3s).**
**PASS**, verified live via a real browser, not just an SSE 200 status:
- A Playwright session instrumented `htmx:oobAfterSwap` and observed **36 OOB swap events over a 10s
  window** on an already-open `/dashboard` tab — genuine, repeated, server-pushed DOM updates, well
  inside the collector's ~2s cadence and comfortably inside the ADR's ≤3s target.
- Raw SSE payload inspection (`curl -N /dashboard/stream`) confirmed each pushed event carries fresh,
  differing data (e.g., the alert banner's `Data skew: handling 50.5x avg partition size` line and node
  CPU/RAM values), not a static/duplicated payload replayed on a timer.
- End-to-end full-page render latency (`GET /dashboard`, which does its own synchronous
  `collect_once()` for first paint) measured **~2.1-2.8s per request** across 6 samples while a job was
  running — consistent with the ADR's stated `docker stats` ~1-2s inherent latency and its own "≤3s, not
  a hard 2s" framing; this is the full-page path, not the steady-state SSE cadence, and is expected to be
  the slower of the two by design (D-B: full-page is one synchronous sample, SSE reuses the shared
  collector's cadence).

**US-5.5: PASS**, both criteria, live-verified in a real browser.

---

## US-5.6 — Placement and lifecycle relative to existing UI surfaces

**Criterion 1 — reachable from within the app without first navigating to the raw Spark UI, whenever a
cluster is running.**
**PASS**, verified: `/dashboard` is a standalone route (ADR D-E), and its own header carries live
"Master UI →" / "Driver UI →" out-links (issue #20's fix), confirming the intended relationship (the
dashboard is the entry point; raw Spark UIs are one click further out, not the other way around).

**Criterion 2 — no active cluster shows a clear "no active cluster" state, not an error or blank page.**
**PASS**, verified live both before spawning any cluster and again after final teardown:
```
GET /dashboard → 200, "No active cluster" heading + "No active cluster." body text
```
No 500, no blank body, in both cases.

**Criterion 3 — deep link into the real Spark UI for a specific stage/task of interest.**
**PASS — the specific regression target for issue #20, independently re-verified live, not just
trusted from the fix commit.** Every signal card in the Job Detail view carried a real, non-`None` link:
```html
<a href="http://localhost:4040/stages/stage/?id=6&amp;attempt=0" target="_blank" rel="noopener">Open in Spark UI &rarr;</a>
```
and following that exact URL landed on the correct, specific stage page (not the app's landing page, not
a 404):
```
curl -s "http://localhost:4040/stages/stage/?id=6&attempt=0" | grep -i "<title>"
<title>aqe - Details for Stage 6 (Attempt 0)</title>
```

**US-5.6: PASS, all 3 criteria** — but see **Finding 1** below: a separate, newly-discovered defect
affects the *client-side view switching* this story's placement design depends on, even though placement
and deep-linking themselves are correct.

---

## D-A compliance — signal only, never a conclusion/suggestion

Per the task brief, every rendered surface captured this session was swept for tuning-advice language.

```
grep -niE "suggest|salt|consider|recommend|should" /tmp/dash_running.html   → (no matches)
grep -niE "suggest|salt|consider|recommend|should" <first 8000 bytes of a live SSE stream capture> → (no matches)
```

Manually reviewed every signal card's `category`/`detail` text produced live this session:
- `"Partition size distribution"` / `"spark-worker-1 holds 13 partitions ~50.5x larger than the cluster
  median."` — names the measurement, no remedy.
- `"Stage share of runtime"` / `"Stage 6 is 58% of total runtime so far."` — same.
- Node flag badges: `"Data skew: handling 50.5x avg partition size"` — same.
- Alert banner: `"Skew detected on spark-worker-1"` + `"Data skew: handling 50.5x avg partition size"` —
  no "consider repartitioning/salting" tail anywhere (confirmed the mockup's own such tail is genuinely
  absent, matching the ADR's documented, deliberate D-A deviation).

No occurrence of any swept term was found in any context, factual or otherwise, across the full page
render, the raw SSE payload, or the four screenshots captured this session. **D-A holds, live, not just
by code-structure inspection** (though `model.py`'s `SignalCard`/`Snapshot` dataclasses were also
directly re-read this pass and confirmed to carry no suggestion/fix field, consistent with the ADR's
"structural, not just conventional" claim).

**D-A: PASS.**

---

## Visual comparison against the mockup

Compared live screenshots (`dashboard_overview.png`, `dashboard_jobdetail.png`, `dashboard_nodedetail.png`
— captured this session, not committed to the repo per this project's existing no-screenshot-tooling
convention, same as the Phase 1 pass) against
`docs/architecture/realtime-monitoring-dashboard-mockup.dc.html` and the ADR's "Visual design" section.

**Faithful to the mockup / ADR, as expected:**
- Persistent dark top bar, orange→red logo mark, "Spark Cluster Monitor" title, green pulsing "Live"
  dot, cluster name/mode, current time — all present and matching.
- Alert banner (amber, ⚠, factual title + detail + "View job diagnosis →" link) — present, and its
  prescriptive tail is genuinely absent, confirming the ADR's deliberate D-A deviation is real in the
  running app, not just documented intent.
- Job summary strip, node grid (responsive cards, MASTER badge purple, CPU/RAM color-coded bars, 3-up
  Disk/Net/GC row, "Partitions handled" count, red flag badges) — all present and rendering real data.
- Job Detail's stage timeline (Gantt-style bars, current/done/pending coloring), signal spotlight cards
  (3-up, icon + category + factual detail + deep link, **no `Suggestion:` line** — confirmed absent, the
  intended D-A change), and partition distribution table (SKEW badges, tinted rows, summary line) — all
  present and matching the ADR's described layout.
- Node Detail view (four stat tiles, CPU/RAM history sparkline strips, that node's own partition table)
  — present; RAM history rendered as a visible strip of color-coded bars.

**Minor visual nuance, not blocking:** in the Node Detail view screenshot, the CPU history sparkline
strip appeared visually empty/flat for a node whose CPU stayed at a genuine, sustained `0%` throughout
the session (RAM history's strip was clearly visible for the same node in the same screenshot). This is
plausibly a rendering artifact of 0%-height bars at a real, legitimately-idle CPU reading rather than a
functional defect — not independently confirmed either way this pass (would need a CPU-active node's
history to compare against, which this session's workload didn't produce). Flagged for a human's own
visual spot-check rather than filed as a bug, since it may simply be correct rendering of "genuinely
nothing to show."

**Visual comparison: no deviations beyond the ADR's own documented, deliberate D-A change** (missing
`Suggestion:` lines and the alert banner's prescriptive tail — both correctly absent, not a regression).

---

## Re-verification of issues #18-#21 (independent of the fix commit's own testing)

**Issue #19 — blocking I/O in the collector's sampling loop freezing the whole app.**
**PASS, fix holds under live load.** While the AQE job ran on the live cluster (collector actively
sampling every ~2s, `docker stats` + `:4040` REST calls per cycle), `GET /topics/aqe` (an unrelated
route) was hit repeatedly and timed:
```
iter 1: topics/aqe=49ms   iter 2: topics/aqe=50ms   iter 3: topics/aqe=48ms
iter 4: topics/aqe=56ms   iter 5: topics/aqe=63ms   iter 6: topics/aqe=50ms
... (8 more samples over a separate ~40s window, all 48-53ms)
```
No multi-second stalls anywhere across 14 samples spanning roughly a minute of concurrent collector
activity — consistent with the fix's `asyncio.to_thread()` offload for `app_client`'s blocking
`urllib` calls. (For comparison, `GET /dashboard` itself — which does its own synchronous
`collect_once()` for first paint — measured ~2.1-2.8s per call in the same window, which is expected
per-request cost for *that* route specifically, not evidence of app-wide freezing.)

**Issue #20 — dead `deep_link=None` on signal cards.**
**PASS, fix holds, independently confirmed live** — see US-5.6 criterion 3 above: a real, non-`None`
`http://localhost:4040/stages/stage/?id=6&attempt=0` link was followed and landed on the correct stage's
own detail page (`<title>aqe - Details for Stage 6 (Attempt 0)</title>`), not the app root or a 404.

**Issue #21 — garbled alert title for imbalance-only flags.**
**PASS for the skew path (live), PASS for the CPU-imbalance path (code + unit test).** Live: the
skew-flagged alert this session rendered as the clean `"Skew detected on spark-worker-1"` — no dumped
detail sentence ahead of the category, matching `_alert_title_for()`'s explicit category-lookup fix. The
CPU-imbalance-specific branch (`category = "Resource imbalance"`) was not naturally triggered by this
session's disk/shuffle-bound job (see US-5.4 criterion 2 above) but is covered by
`tests/unit/test_collector.py::TestAlertTitleFormatting::test_alert_title_is_readable_when_flagged_via_cpu_imbalance_not_skew`,
re-run this pass and confirmed passing, plus direct reading of `_alert_title_for()`'s source (explicit
`skew_reasons`/`imbalance_reasons` dict lookup, no string-splitting of the detail text at all anymore).

**Issue #18 — subprocess leak on cancel in `docker_stats.py`.**
**PASS, no orphaned subprocess observed, though the specific "last-SSE-subscriber-disconnect" trigger
could not be cleanly isolated live this pass** — an external browser session (a pre-existing `msedge.exe`
process holding an established connection to `:8000`, present before this session started and left
undisturbed rather than closed without the owner's knowledge) meant this session's own test browser
disconnecting did not guarantee zero total subscribers, confounding a clean "collector fully stops"
observation. Verified instead via: (a) repeated `docker.exe` process-table sampling (6 one-second
samples) while the collector was actively running, confirming each observed `docker.exe` PID was
short-lived and never persisted across consecutive samples — i.e., no process hung indefinitely as an
orphan; (b) direct reading of `docker_stats._run()`'s `except asyncio.CancelledError` branch (kills and
awaits the child process before re-raising, mirroring `compose_ops.py`'s existing fix for the identical
bug class); and (c) the dedicated regression tests added in the fix commit (`tests/unit/test_docker_stats.py`),
re-run this pass and confirmed passing. This is corroborating rather than a from-scratch live
reproduction of the exact disconnect trigger — flagged accordingly, not treated as a live PASS on par
with issues #19-#21 above.

---

## Findings

**Finding 1 (new, not #18-#21) — SSE OOB swaps strip the `dash-view`/`active` classes off
`#overview-content`/`#job-detail-content`/`#node-detail-container`, permanently breaking the three-view
client-side switcher within seconds of opening the dashboard (relates to US-5.6's placement design and
ADR D-B's "view switching stays 100% client-side" intent).**

`app/web/templates/dashboard/fragments/{overview,job_detail,node_detail}_oob.html` each wrap their
content in `<div id="..." hx-swap-oob="true">` with no `class` attribute, while
`app/web/templates/dashboard/page.html` gives those same three elements the classes that drive
visibility (`class="dash-view active"` / `class="dash-view"`, with CSS `.dash-view{display:none}` /
`.dash-view.active{display:block}`). HTMX's default `hx-swap-oob="true"` on a same-id element does an
outerHTML replace — the whole element, attributes included — so every SSE push (every ~2s while the
collector runs) silently wipes the `class` attribute off all three containers. Once gone, `.dash-view`'s
`display:none` no longer applies, so **all three views render simultaneously, stacked**, and clicking
"Overview"/"Job Detail →"/a node card afterward only toggles the (now permanently absent) `active`
class, which never matches anything again for the rest of the page's life.

Reproduced live and directly, twice, via Playwright against the real running dashboard, with zero user
interaction beyond opening `/dashboard` and waiting:
```js
await page.goto('http://localhost:8000/dashboard');
document.getElementById('overview-content').className   // "dash-view active"  (t=0)
// wait ~4s (past the first SSE push)
document.getElementById('overview-content').className   // ""                  (t=4s)
document.getElementById('job-detail-content').className // ""                  (t=4s)
getComputedStyle(document.getElementById('job-detail-content')).display // "block" -- visible, unrequested
```
Screenshots captured this session (`dashboard_overview.png`, `dashboard_jobdetail.png`,
`dashboard_nodedetail.png`) all show the Overview strip/node grid, the Job Detail stage timeline/signal
cards/partition table, and (in the node-detail case) the selected node's detail block all rendered
stacked on one page — not the intended single-view-at-a-time layout the mockup and ADR describe.

**Impact:** this does not affect data correctness — every criterion in US-5.1 through US-5.5 that this
pass checked live rendered correct, real, live-updating data throughout, including while this bug was
independently present. It affects the page's *navigational structure*: within a few seconds of normal
use, the dashboard stops behaving as three switchable views and instead becomes one long page with all
three sections' content duplicated/stacked, and the "Overview"/"Job Detail →"/node-card click affordances
stop doing anything once triggered (they toggle a class that no longer has a base class to combine with).
Filed as **[#22](https://github.com/hoanghaithanh/Spark-Playbook/issues/22)** (`bug`, `from:acceptance`,
milestone `Sprint 2 (2026-07-14 – 2026-07-18)`), with a suggested fix (`hx-swap-oob="innerHTML:#target"`
instead of a same-id outerHTML swap) — not fixed by this pass, per this task's scope (report findings,
don't patch around them).

**Finding 2 (minor, visual, not filed as a bug) — Node Detail's CPU history sparkline appeared visually
empty for a node with a sustained, genuine `0%` CPU reading.** See "Visual comparison" above. Plausibly
correct rendering of a real all-zero history rather than a defect; not independently confirmed either
way this pass (this session's workload never produced a CPU-active node to compare against). Flagged for
a human's own visual spot-check, not filed.

---

## Teardown

```
POST /topics/aqe/teardown → State: idle, Message: "Cluster torn down."
docker ps -a       → (empty)
docker network ls  → no sparkpb network present
GET /dashboard     → 200, "No active cluster" empty state (US-5.6 c2, re-confirmed post-teardown)
py -3.9 -m pytest tests/unit -q → 199 passed
git status --short → (empty) — content/aqe/notebook.ipynb, which picked up live execution outputs/cell
                     ids/metadata changes during this session's kernel-driven run, was reset via
                     `git checkout -- content/aqe/notebook.ipynb` before finishing, per this project's
                     notebook-cleanliness convention
```

Clean state confirmed — no containers, no networks, no uncommitted repo changes, notebook reset to its
clean unexecuted state.

---

## Overall recommendation

**Not a clean sign-off — one new, real, live-reproduced defect (Finding 1 / issue #22) should get a
follow-up fix round before Phase 2.5 is considered fully done, though it does not invalidate the data
correctness of any of US-5.1 through US-5.5.**

- **US-5.1, US-5.2, US-5.5: PASS**, fully live-verified across all criteria against a real running
  cluster and a real skewed job, including the specific "container stopped mid-view" and real-browser
  SSE-delivery checks this task brief called out as the highest-value live checks.
- **US-5.3, US-5.4: PASS**, with one criterion each (zero-completed-tasks ETA state; CPU-saturation-vs-
  idle imbalance) verified via direct code inspection + a dedicated, currently-passing unit test rather
  than live reproduction, because this session's real workload didn't naturally produce those specific
  timing/resource windows on a small demo cluster — flagged transparently above per-criterion, not
  silently assumed.
- **US-5.6: PASS on all 3 stated acceptance criteria** (reachability, empty state, deep link) — but the
  client-side view-switching mechanism its "placement" design depends on is broken by Finding 1/#22
  shortly after normal use begins. The acceptance criteria as literally written don't cover
  view-switching mechanics directly, so this is reported as a **finding**, not a criterion failure, but
  it's a real, user-visible defect the human should weigh before treating placement/navigation as fully
  done.
- **D-A (signal-only compliance): PASS**, verified live across the full rendered page, a raw SSE payload
  capture, and every signal card/badge/alert text produced this session — no tuning-advice language
  found anywhere, and the mockup's `Suggestion:` lines are confirmed genuinely absent (the ADR's intended
  deviation, not a regression).
- **Visual comparison against the mockup:** no deviations beyond the ADR's own documented D-A change;
  one minor, unconfirmed-either-way visual nuance (CPU sparkline on a genuinely-idle node) flagged for a
  human spot-check, not filed.
- **Issues #18-#21: all independently re-verified this pass** — #19 (event-loop blocking) and #20 (dead
  deep links) confirmed live with fresh evidence; #21 (garbled alert title) confirmed live for the skew
  path and via code+test for the CPU-imbalance path; #18 (subprocess leak) confirmed via process-table
  sampling + code + test, with the exact "last subscriber disconnects" trigger not cleanly isolable live
  this pass due to a pre-existing, undisturbed external browser session holding its own connection to the
  app throughout — noted as a methodology limitation, not a sign the fix is suspect.

**Recommendation:** do not sign off Phase 2.5 as fully done yet. Route Finding 1 (issue #22) through a
developer fix round, then a short, targeted re-check of the view-switching behavior specifically
(the rest of this report's evidence does not need to be re-run) before final human sign-off. This is a
recommendation, not an approval — per this project's Definition of Done, the human should review this
report and give explicit final sign-off once #22 is resolved (or make an informed call that it's
acceptable to ship with a known follow-up ticket).

## Bugs filed

- **Finding 1** (SSE OOB swaps break client-side view switching) —
  [#22](https://github.com/hoanghaithanh/Spark-Playbook/issues/22) — **open**, `bug` + `from:acceptance`,
  milestone `Sprint 2 (2026-07-14 – 2026-07-18)`.

Issues #18-#21 (already fixed prior to this pass) were independently re-verified above and are not
re-filed; all remain closed.
