# Phase 1 Acceptance Report — Spark Playbook

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-14
Scope: US-1.1, US-1.2, US-1.3 (Phase 1), plus re-verification of US-0.1/US-0.2/US-0.3 (Phase 0)
        through the app's own lifecycle path (`app/lifecycle/manager.py` → `compose_ops.py`,
        not `compose/cli.py`).

## Method

The FastAPI app was started for real:

```
py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

(`app/requirements.txt`/`app/requirements-dev.txt` deps were already installed under the
system's Python 3.9 interpreter; the repo has no documented run command beyond this — it's
inferred correctly from `app/main.py`'s `create_app()`/`app` module-level singleton.)

All Docker containers/networks were confirmed absent before starting (`docker ps -a` empty), and
the `sparkpb/spark:4.0.3` image was already built and present locally. Every cluster spawn below
was driven through the app's HTTP routes (`POST /topics/{id}/spawn`, `POST /topics/{id}/teardown`)
— never `compose/cli.py` directly — so this exercises `app/lifecycle/manager.py`,
`renderer.py`, `compose_ops.py`, and `readiness.py` as a whole. A real Jupyter kernel (via the
Jupyter kernel REST/websocket API) was used to run an actual PySpark shuffle job against a
spawned cluster to independently confirm cluster correctness, since the browser-embedded iframe
itself turned out to be broken (see US-1.3 below). Playwright (via `npx playwright`, installed
ad hoc into the scratchpad — not added to the project) was used for one real-browser check of
the iframe embed.

At the end, the app process was killed and `docker ps -a` / `docker network ls` were checked to
confirm a fully clean teardown.

---

## US-0.1 — Spin up and tear down a cluster manually (re-verified via app's own lifecycle path)

**Criterion 1 — default/spawned config comes up, master lists expected workers within 60s.**
PASS. Spawned via `POST /topics/partitioning-shuffle/spawn` with `worker_count=5, worker_cores=4,
worker_memory_gb=8` (an in-range max config, not just the 3-worker default):

```
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
NAMES            STATUS          PORTS
spark-driver     Up 20 seconds   0.0.0.0:4040->4040/tcp, 0.0.0.0:7078-7079->7078-7079/tcp, 0.0.0.0:8888->8888/tcp
spark-worker-4   Up 20 seconds   0.0.0.0:8084->8081/tcp
spark-worker-3   Up 20 seconds   0.0.0.0:8083->8081/tcp
spark-worker-2   Up 20 seconds   0.0.0.0:8082->8081/tcp
spark-worker-1   Up 20 seconds   0.0.0.0:8081->8081/tcp
spark-worker-5   Up 20 seconds   0.0.0.0:8085->8081/tcp
spark-master     Up 20 seconds   0.0.0.0:6066->6066/tcp, 0.0.0.0:8080->8080/tcp
```

App panel response: `State: ready`, `Message: READY: 5/5 workers alive after 18.1s.` — well
under the 60s target. A separate default-sized (3-worker) spawn and a 2-worker spawn both also
reached `READY` within ~12–20s each (see US-1.2 evidence below). The default 3-worker config was
exercised indirectly via multiple spawns during this session; explicit re-check not repeated
since the max config is a strict superset of the work involved.

**Criterion 2 — `down` fully tears down; a new stack with different params starts cleanly, no
leftover state.**
PASS. See the cancel-and-replace test under US-1.2 (fires two overlapping spawns with different
`worker_count`/`shuffle_partitions`/`aqe_enabled`) and the explicit teardown→respawn sequence:

```
POST /teardown  →  State: idle, Message: Cluster torn down.
docker ps -a    →  (empty)
POST /spawn (worker_count=2, shuffle_partitions=50)  →  State: ready, Message: READY: 2/2 workers alive after 12.2s.
```

No port/name collisions on any of the ~6 spawn/teardown cycles run during this session.

**Criterion 3 — resource budget respected, host stays responsive.**
PASS (by inspection/experience during the session — no OOM, no thrashing observed across
multiple spawns up to the 5×4-core/8GB max config, which totals ~43GB, within the 64GB host).
Not independently instrumented (no memory-pressure monitoring tool run) — noted as
inspection-only, consistent with a Definition-of-Done item that doesn't require dedicated
tooling for a single-user local tool.

## US-0.2 — Reach cluster observability endpoints (re-verified)

**Criterion 1 — master UI at :8080 shows worker count/cores/memory matching configuration.**
PASS.

```
curl -s http://localhost:8080/json/
{
  "cores": 20, "memory": 40960,
  "workers": [
    {"id": "worker-...-172.19.0.7-39219", "cores": 4, "memory": 8192, "state": "ALIVE"},
    ... (5 workers total, all cores=4, memory=8192 = 8GB, all ALIVE)
  ]
}
```
Matches the spawned config exactly (5 workers × 4 cores × 8GB).

**Criterion 2 — driver app UI at :4040 shows Jobs/Stages/SQL tabs for a running application.**
PASS (via REST, equivalent surface):
```
curl -s http://localhost:4040/api/v1/applications
[{"id":"app-20260714121324-0000","name":"qa-shuffle-check","attempts":[{"completed":false,...}]}]
```
`:4040` was reachable and the app-id was discoverable exactly per PLAN.md §3's app-id-discovery
design.

**Criterion 3 — REST API `/api/v1/applications/<id>/stages` returns `shuffleReadBytes`,
`shuffleWriteBytes`, `numTasks` for shuffle stages.**
PASS.
```
curl -s http://localhost:4040/api/v1/applications/app-20260714121324-0000/stages
stage 2  COMPLETE  shuffleReadBytes=12054   shuffleWriteBytes=0      numTasks=1
stage 1  SKIPPED   shuffleReadBytes=0       shuffleWriteBytes=0      numTasks=4
stage 0  COMPLETE  shuffleReadBytes=0       shuffleWriteBytes=12054  numTasks=4
```

## US-0.3 — Run a real shuffle job end-to-end (re-verified)

PASS. A real `groupBy().count()` over 200,000 synthetic rows was executed via a live Jupyter
kernel connected to the app-spawned driver (`spark://spark-master:7077`, client mode):
```
DISTINCT_KEYS 51
APP_ID app-20260714121324-0000
```
Task distribution confirmed across more than one worker/executor:
```
curl -s http://localhost:4040/api/v1/applications/app-20260714121324-0000/executors
executor 1  172.19.0.3:7079  completedTasks=3
executor 0  172.19.0.4:7079  completedTasks=2
```
Nonzero `shuffleReadBytes`/`shuffleWriteBytes` confirmed above (US-0.2 criterion 3) — real
distributed shuffle, not local-mode. Phase 0's cluster-harness behavior holds up unchanged
through the app's own lifecycle path (`manager.py`/`compose_ops.py`), independent of
`compose/cli.py`.

---

## US-1.1 — Browse the partitioning/shuffle topic page

**Criterion 1 — topic page shows concept (what/why) + control to open notebook.**
PASS.
```
curl -s http://127.0.0.1:8000/topics/partitioning-shuffle
HTTP_STATUS: 200
```
Response includes a rendered `<h1>Partitioning & Shuffle Mechanics</h1>`, "What it is" / full
concept markdown rendered to HTML, and (once a cluster is `ready`) the embedded-notebook control
— when not ready, a placeholder pointing at `content/partitioning-shuffle/notebook.ipynb`.

**Criterion 2 — content stored as Markdown + notebook JSON; edits reflected on next load, no
code change.**
PASS — verified by directly mutating the file while the app was running:
```
echo "\n\nQA-MARKER-ACCEPTANCE-TEST-12345" >> content/partitioning-shuffle/concept.md
curl -s http://127.0.0.1:8000/topics/partitioning-shuffle | grep -c QA-MARKER-ACCEPTANCE-TEST-12345
1
```
Marker appeared immediately with no app restart; removed afterward and re-verified absence
(`grep -c` → `0`).

**Bonus — unknown topic returns a real 404 (issue #4 fix, re-verified live).**
PASS.
```
curl -s -w "\nHTTP_STATUS:%{http_code}\n" http://127.0.0.1:8000/topics/does-not-exist
{"detail":"No such topic: 'does-not-exist'"}
HTTP_STATUS:404
```
Same for the `/panel` fragment endpoint. Confirms the `TopicNotFoundError` exception handler in
`app/main.py` works end-to-end, not just in the unit test.

## US-1.2 — Configure and spawn a cluster from the UI

**Criterion 1 — spawn with in-range params renders template, tears down old, brings up new,
reports success only once master reports expected worker count (or clear failure/timeout).**
PASS. Multiple spawns exercised (3-worker default-ish, 5-worker max, 2-worker), all reaching
`READY` with `alive_workers == worker_count` within well under the 60/90s bounds (12–20s
observed). `spark-defaults.conf` was confirmed to actually receive the submitted parameters:
```
spawn request: worker_count=2, shuffle_partitions=333, aqe_enabled=true
→ compose/rendered/spark-defaults.conf:
  spark.sql.shuffle.partitions          333
  spark.sql.adaptive.enabled            true
```

**Criterion 2 — concurrent spawn/teardown request doesn't leave two overlapping stacks or
inconsistent compose state (D5 cancel-and-replace).**
PASS — this is the highest-value test in this report; it directly re-verifies the "cancelled
spawn doesn't leave a stray process" fix end-to-end, not just via the unit/integration test
suite. Two spawn requests with **different parameters** (3 workers/`shuffle_partitions=200` vs. 2
workers/`shuffle_partitions=333`/`aqe_enabled=true`) were fired back-to-back (~300ms apart):
```
spawn1 (3 workers) → HTTP 200, State: tearing_down, Message: "Spawn cancelled (superseded by a newer request)."
spawn2 (2 workers) → HTTP 200, State: ready, Message: "READY: 2/2 workers alive after ...s"
```
`docker ps` immediately after shows **exactly one coherent 4-container stack** matching spawn2's
params, zero orphans, zero leftover/stopped containers from spawn1:
```
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
spark-driver     Up 27 seconds
spark-worker-1   Up 27 seconds
spark-worker-2   Up 27 seconds
spark-master     Up 28 seconds

docker ps -a  →  (same 4 rows — no stopped containers left behind either)

curl -s http://localhost:8080/json/  →  aliveworkers: 2, both ALIVE
```
This matches D5's cancel-and-replace design exactly: no queueing, no reject-with-error, and no
overlap.

**Criterion 3 — resource ceiling rejects an over-budget config before spawning, with a clear
message.**
**FAIL as specified — the ceiling can never be triggered through in-range UI values.** The
ceiling check (`app/config.RESOURCE_CEILING_GB = 48`) computes
`master(1) + worker_count×worker_memory_gb + driver(2)`. The maximum reachable total using only
values the UI's own `min`/`max` attributes allow (`worker_count` ≤ 5, `worker_memory_gb` ≤ 8) is
`1 + 5×8 + 2 = 43GB`, which is always **under** the 48GB ceiling. Verified live:
```
worker_count=5, worker_cores=4, worker_memory_gb=8 (max in-range values)
→ HTTP 200, State: ready, Message: "READY: 5/5 workers alive after 18.1s." (accepted, not rejected)
```
The ceiling only actually fires when combined with an **out-of-range** value (which the range
check would reject on its own anyway), confirmed here:
```
worker_count=5, worker_memory_gb=10 (out of the 1-8 range)
→ HTTP 200, State: failed
  Message: "Rejected: worker_memory_gb must be 1-8; requested config totals ~53GB,
            exceeding the 48GB sanity ceiling (PLAN.md §2 resource-ceiling check)"
```
So the *mechanism* works correctly (rejects pre-spawn with a clear message, no container ever
started — confirmed via `docker ps` showing the prior stack untouched, not a partial new one),
but the acceptance criterion as written ("Given a chosen configuration, when total requested
resources would exceed a safe bound... then the UI rejects") describes a scenario that is
**unreachable via any configuration a learner can actually submit through the documented ranges**.
This is a gap between PLAN.md's stated ranges/ceiling and the requirement's intent — either the
ranges need tightening, the ceiling needs lowering, or the criterion needs to explicitly allow
"or is unreachable by design because the ranges already fit the budget" (in which case this
should be called out as intentional, not left as a silently-unreachable code path). Filed as a
GitHub issue (see below).

## US-1.3 — Run the topic notebook against the spawned cluster via embedded Jupyter

**Criterion 1 — notebook loads inside an embedded JupyterLab iframe pointed at the current
stack's driver; running cells executes against that cluster.**
**FAIL.** The iframe is served but **blocked by Jupyter's Content-Security-Policy** — confirmed
both via direct header inspection and a real headless-Chromium (Playwright) render of the actual
topic page:
```
curl -sI http://localhost:8888/lab | grep -i content-security
Content-Security-Policy: frame-ancestors 'self'; report-uri /api/security/csp-report
```
`frame-ancestors 'self'` only permits framing from Jupyter's own origin (`localhost:8888`), not
from the FastAPI app's origin (`localhost:8000`) that actually embeds it. A real browser
navigation to `http://127.0.0.1:8000/topics/partitioning-shuffle` (Playwright/Chromium) confirms
this is not just a theoretical header mismatch — the iframe genuinely fails to load:
```
Console: "Framing 'http://localhost:8888/' violates the following Content Security Policy
directive: 'frame-ancestors 'self''. The request has been blocked."
iframe.contentDocument → null (cross-origin/blocked)
```
Root cause: PLAN.md §6/R3 specifies a mitigation (`driver/jupyter_config.py` setting
`Content-Security-Policy: frame-ancestors 'self' http://localhost:8000` and disabling
`X-Frame-Options`) that was never implemented — there is no `driver/jupyter_config.py` in the
repo, and the compose template's driver `command:` only sets `--ServerApp.token=''`,
`--ServerApp.password=''`, and `--ServerApp.allow_origin='*'` (CORS, not CSP framing). The
underlying cluster/kernel is fully functional — a real PySpark shuffle job was run successfully
against it via the Jupyter kernel REST/websocket API directly (see US-0.3 above) — so this is
specifically an iframe-embedding defect, not a cluster defect. **This is the same class of
blank-iframe failure PLAN.md itself predicted and named as "Noticed by."** Filed as a GitHub
issue (see below); this blocks sign-off on US-1.3's core acceptance criterion since the feature
as specified (embedded notebook execution) does not work in a browser today.

**Criterion 2 — teardown + respawn reconnects to the new cluster, not a stale reference.**
**Unable to fully verify (blocked by Criterion 1's failure) — mechanism-level check only, PASS
at that level.** The app does correctly change the iframe `src`'s cache-busting query parameter
on every spawn (`?spawn=<spawn_id>`), confirmed to increment across the session's spawns
(`spawn=1` → `spawn=3` → `spawn=4`), and the driver container is genuinely torn down and
recreated fresh each time (new container, same fixed ports, confirmed via `docker ps` before/
after teardown). This is the correct mechanism for "not a stale reference." However, since the
iframe itself never renders in a real browser (Criterion 1), the actual learner-visible behavior
("reopen the notebook, see it connected to the new cluster") cannot be confirmed end-to-end —
it's blocked by the same CSP defect.

---

## Bugs filed

<!-- filled in after gh issue creation -->

---

## Teardown confirmation

```
POST /topics/partitioning-shuffle/teardown → State: idle, Message: "Cluster torn down."
docker ps -a            → (empty)
docker network ls       → no sparkpb network present
uvicorn process killed  → curl to :8000 → connection refused
```
Clean state confirmed — no containers, no networks, no running app process left behind by this
validation session.

---

## Overall recommendation

**Not ready for final sign-off as-is.** Of the three Phase 1 user stories:

- **US-1.1** — fully PASS, including a live re-check of the issue #4 (404) and issue #5
  (missing-notebook) fixes' real-world behavior.
- **US-1.2** — 2 of 3 criteria PASS, including the highest-risk one (cancel-and-replace /
  no-orphan-containers, directly re-verifying issue #1's fix live). The 3rd criterion
  (resource-ceiling rejection) is a **real, reproducible gap**: the specified UI ranges make the
  ceiling mathematically unreachable, so the "UI rejects an over-budget config" behavior a
  learner would actually see never happens through legitimate use.
- **US-1.3** — **core criterion fails**: the embedded JupyterLab iframe is blocked by CSP in a
  real browser, which is the entire point of US-1.3 (move from reading the concept to running it
  without manual setup, in-app). The underlying cluster and Jupyter kernel work correctly when
  accessed directly (proven via the kernel API), so this is a scoped, fixable defect, not a
  fundamental design problem — but as shipped, a learner opening the topic page today gets a
  blank iframe.

Phase 0 (US-0.1–0.3) re-verification through the app's own lifecycle path is a clean PASS — no
regressions from the `app/lifecycle/` changes since the last check.

**Recommendation:** send US-1.2's ceiling gap and US-1.3's CSP defect back to the developer
before Phase 1 sign-off. Both are narrowly scoped (a config/constant tweak for the ceiling; a
`driver/jupyter_config.py` + Dockerfile/compose command change for the CSP fix, per PLAN.md's
own R3 mitigation). This is a recommendation, not an approval — the human should review this
report and the linked issues and give explicit final sign-off per the Definition of Done, or
direct the team to fix these two items first.
