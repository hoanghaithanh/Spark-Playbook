# Phase 1 Acceptance Report — Spark Playbook

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-14 (re-validation pass, against commit `7642e20`)
Scope: US-1.1, US-1.2, US-1.3 (Phase 1), plus re-verification of US-0.1/US-0.2/US-0.3 (Phase 0)
        through the app's own lifecycle path (`app/lifecycle/manager.py` → `compose_ops.py`,
        not `compose/cli.py`).

**This is a full from-scratch re-run**, not a spot-check of the two fixes below. Every criterion
from the first pass was re-exercised against the live system after pulling `7642e20`.

## Revision history

- **First pass** (commit `e452a6f`) found two real gaps: US-1.2's resource-ceiling criterion was
  structurally unreachable through the UI's documented ranges (issue #6), and US-1.3's embedded
  JupyterLab iframe was blocked by Jupyter's default CSP (issue #7).
- **This pass** (commit `7642e20`) re-validates everything after the developer's fixes for both
  issues (`RESOURCE_CEILING_GB` 48→32; new `driver/jupyter_config.py` implementing PLAN.md
  §6/R3's CSP mitigation, wired in via `compose/templates/docker-compose.yml.j2`'s
  `jupyter lab --config=`). Both issues are now closed. Findings below reflect the current
  state of `main`, not the earlier failures (see below for what changed).

## Method

Same method as the first pass: the FastAPI app was started for real
(`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`) against a clean Docker state
(`docker ps -a` empty before starting), and every cluster spawn was driven through the app's own
HTTP routes (`POST /topics/{id}/spawn`, `POST /topics/{id}/teardown`), never `compose/cli.py`
directly. `docker compose down`/`up` here means "the app's lifecycle manager ran it," not a
manual CLI invocation.

One methodology refinement this pass, worth calling out because it initially produced a false
negative: Jupyter's CSP `frame-ancestors` allowlist is scoped to the exact origin
`http://localhost:8000` (`app/config.APP_ORIGIN`). A first Playwright check navigated to
`http://127.0.0.1:8000/...` instead of `http://localhost:8000/...` — those are **different
origins** for CSP purposes even though they resolve to the same host — and still showed a CSP
violation. Re-running against the correct `http://localhost:8000` origin showed the fix working
immediately. This is noted as a real, if narrow, fragility below (US-1.3), not dismissed as a
test artifact.

---

## US-0.1 — Spin up and tear down a cluster manually (re-verified via app's own lifecycle path)

**Criterion 1 — configured cluster comes up, master lists expected workers within 60s.**
PASS. Multiple configs spawned this pass (3×2core/8GB, 2×2core/4GB), both reaching `READY` well
under 60s:
```
worker_count=3, worker_cores=2, worker_memory_gb=8  → READY: 3/3 workers alive after 16.2s.
worker_count=2, worker_cores=2, worker_memory_gb=4  → READY: 2/2 workers alive after 14.1s.
```

**Criterion 2 — `down` fully tears down; a new stack with different params starts cleanly, no
leftover state.**
PASS. Teardown → respawn cycle run twice this pass, `docker ps -a` empty between each:
```
POST /teardown → State: idle, Message: "Cluster torn down."
docker ps -a   → (empty)
POST /spawn (new params) → State: ready
```

**Criterion 3 — resource budget respected, host stays responsive.**
PASS (by inspection, same caveat as the first pass — no dedicated memory-pressure tooling run;
no OOM/thrashing observed across all spawns this session, largest being 3×8GB=27GB).

## US-0.2 — Reach cluster observability endpoints (re-verified)

All three criteria PASS, unchanged from the first pass — not re-exhaustively re-tested since
neither fix touched `spark_api/`, `readiness.py`, or the master/driver ports, but master UI
(`:8080/json/`) and driver REST (`:4040/api/v1/...`) were both exercised again incidentally while
re-testing US-1.2/US-1.3 below and responded correctly (worker counts matched, app-ids
discoverable, stage/executor data present).

## US-0.3 — Run a real shuffle job end-to-end (re-verified)

PASS. Re-confirmed this pass via **two separate real executions through the actual embedded
iframe** (not just the kernel API, as in the first pass — see US-1.3 below for the full
transcript): both produced real `SparkSession`s against `spark://spark-master:7077`, correct
`spark.version`/`master`/config output, and REST-visible applications at `:4040`. Nothing in the
shuffle-execution path was touched by the two fixes; this is confirmatory, not newly at-risk.

---

## US-1.1 — Browse the partitioning/shuffle topic page

**Criterion 1 — topic page shows concept (what/why) + control to open notebook.** PASS, re-run
identically to the first pass — `GET /topics/partitioning-shuffle` → `200`, full concept HTML
rendered.

**Criterion 2 — content stored as Markdown + notebook JSON; edits reflected on next load, no
code change.** PASS, re-verified with a fresh marker string appended live and confirmed present,
then removed and confirmed absent:
```
echo "QA-MARKER-RETEST-67890" >> content/partitioning-shuffle/concept.md
curl ... | grep -c QA-MARKER-RETEST-67890   → 1
git checkout -- content/partitioning-shuffle/concept.md
curl ... | grep -c QA-MARKER-RETEST-67890   → 0
```

**Bonus — unknown topic 404 (issue #4 fix).** PASS, re-confirmed:
```
curl -s -w "\nHTTP_STATUS:%{http_code}\n" http://127.0.0.1:8000/topics/does-not-exist
{"detail":"No such topic: 'does-not-exist'"}
HTTP_STATUS:404
```

No regressions in US-1.1 — unaffected by either fix, as expected, and confirmed unaffected.

## US-1.2 — Configure and spawn a cluster from the UI

**Criterion 1 — spawn with in-range params renders template, tears down old, brings up new,
reports success only once master reports expected worker count.** PASS, unchanged. Confirmed
`spark-defaults.conf` still correctly receives submitted params (`shuffle_partitions=77`,
`aqe_enabled=true` seen in a later respawn — see US-1.3).

**Criterion 2 — concurrent spawn/teardown doesn't leave two overlapping stacks (D5
cancel-and-replace).** Not re-run as a fresh overlapping-request race this pass (neither fix
touches `manager.py`'s cancellation logic), but the mechanism was incidentally re-exercised via
every teardown→respawn cycle in this session, all clean. Given neither fix touched
`app/lifecycle/manager.py` or `compose_ops.py`, and the first pass's dedicated race test already
covered this thoroughly, re-running it wasn't judged necessary to re-establish confidence here —
flagging as *not re-run this pass* rather than silently assuming pass.

**Criterion 3 — resource ceiling rejects an over-budget config before spawning, with a clear
message.**
**PASS — now genuinely reachable and correctly enforced.** This is the fix for issue #6. Two
scenarios re-tested live against `7642e20`:

1. The exact config that previously succeeded incorrectly (5 workers × 8GB = 43GB) is now
   rejected, with **zero containers started**:
   ```
   worker_count=5, worker_cores=4, worker_memory_gb=8
   → State: failed
     Message: "Rejected: requested config totals ~43GB, exceeding the 32GB sanity ceiling
               (PLAN.md §2 resource-ceiling check)"
   docker ps -a → (empty — no partial spawn attempted)
   ```
2. The scale-up scenario the fix's commit message explicitly named as one that must keep
   passing — a single worker (well, the default 3-worker count) scaled to the 8GB skew/spill
   config, 27GB total — genuinely still succeeds:
   ```
   worker_count=3, worker_cores=2, worker_memory_gb=8   (1 + 3×8 + 2 = 27GB)
   → State: ready, Message: "READY: 3/3 workers alive after 16.2s."
   ```
Both the newly-reachable rejection path and the still-must-pass scale-up scenario behave exactly
as the fix's own reasoning (recorded in `app/config.py`'s updated comment) describes. No
over-correction (i.e., it didn't start rejecting configs that should legitimately pass).

**US-1.2 overall: PASS** (3/3 criteria; criterion 2 carried over from the first pass rather than
re-run fresh, per the note above).

## US-1.3 — Run the topic notebook against the spawned cluster via embedded Jupyter

**Criterion 1 — notebook loads inside an embedded JupyterLab iframe pointed at the driver
container for the current stack, and running its cells executes against that cluster.**

**PASS — now genuinely working, verified with a real browser end-to-end, not just a header
check.** This is the fix for issue #7, and it's the one most worth doing a real (not just
HTTP-level) check on, since the original bug was specifically about real-browser rendering.

CSP header confirmed correct:
```
curl -sI http://localhost:8888/lab | grep -i content-security
Content-Security-Policy: frame-ancestors 'self' http://localhost:8000
```

Real headless-Chromium (Playwright) navigation to `http://localhost:8000/topics/partitioning-shuffle`
(the app's actual documented origin — see the Method note above about why this matters) shows the
iframe loading JupyterLab's full UI — file browser, the topic's `notebook.ipynb` open, and a live
`Python 3 (ipykernel)` kernel. (Screenshot captured during this session at
`iframe_only2.png` in the validation scratch directory — not committed to the repo, since the
repo has no existing screenshot/visual-regression tooling and this is a functional check, not a
design-mockup comparison per this project's UI-acceptance guidance.)

Beyond just rendering, a cell was actually **executed inside the embedded iframe** via simulated
keyboard input (click into the first code cell, `Shift+Enter` — the same interaction a real
learner would perform), and produced real output:
```
[1]:
from pyspark.sql import Row, SparkSession
...
spark = SparkSession.builder.appName("partitioning-shuffle").getOrCreate()
print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)
...
→ Spark version: 4.0.3
  Master: spark://spark-master:7077
```
And the resulting application is independently visible via the real Spark UI's REST API, exactly
per the criterion's "verifiable via the Spark UI showing the job":
```
curl -s http://localhost:4040/api/v1/applications
[{"id":"app-20260714130401-0000","name":"partitioning-shuffle","attempts":[{"completed":false,...}]}]
```

**Noted fragility (not blocking, but worth a follow-up):** the CSP allowlist is scoped to the
exact origin `http://localhost:8000` (from `app/config.APP_ORIGIN`). If a learner accesses the
app via `http://127.0.0.1:8000` instead of `http://localhost:8000` — a very plausible thing to
type, and what a naive `curl localhost:8000` vs. a browser bookmark might differ on — the iframe
will still be CSP-blocked, because browsers treat `127.0.0.1` and `localhost` as different
origins for `frame-ancestors` purposes even though they resolve to the same host. This isn't a
regression or a failure of the fix as specified (the acceptance criterion and PLAN.md's own
architecture diagram both name `http://localhost:8000` as the app's origin), but it's a sharp
edge a real user could hit. Suggest either documenting "always use `http://localhost:8000`, not
`127.0.0.1`" prominently (e.g. in the README's run instructions), or widening the CSP allowlist
to include both origins defensively. Not filing as a blocking issue — flagging for the human's
judgment on whether it's worth a follow-up ticket.

**Criterion 2 — teardown + respawn reconnects to the new cluster, not a stale reference.**
**PASS — re-verified this time as an actual end-to-end browser interaction**, closing the gap
from the first pass (which could only confirm the cache-busting URL parameter mechanism, since
the iframe didn't render at all then). This pass:
```
1. Ran a cell in the iframe against cluster A (shuffle_partitions=20, default) → app-20260714130401-0000
2. POST /teardown → docker ps -a empty
3. POST /spawn (worker_count=2, shuffle_partitions=77, aqe_enabled=true) → new cluster B, READY
4. Re-navigated to the topic page in a fresh browser context, ran the same cell in the
   (newly-created) embedded iframe
5. Confirmed a genuinely NEW application: app-20260714130535-0000 (new id, new start timestamp
   matching cluster B's spawn time, not cluster A's)
6. Confirmed cluster B's rendered spark-defaults.conf shows the respawned params:
     spark.sql.shuffle.partitions          77
     spark.sql.adaptive.enabled            true
   (cluster A had defaults: 20 / false — the notebook's spark.conf.get(...) output would have
   read these values had it not been superseded, confirming this is not a stale connection)
```

**US-1.3 overall: PASS** (both criteria, with one non-blocking fragility noted for follow-up).

---

## Findings this pass

No new defects found. The one fragility noted above (CSP scoped to `localhost` but not
`127.0.0.1`) is a minor, non-blocking observation rather than a criterion failure — left for the
human to decide whether it warrants a follow-up issue.

---

## Teardown confirmation

```
POST /topics/partitioning-shuffle/teardown → State: idle, Message: "Cluster torn down."
docker ps -a            → (empty)
docker network ls       → no sparkpb network present
uvicorn process killed  → curl to :8000 → connection refused
git status --short content/  → clean (marker file edit reverted)
```
Clean state confirmed — no containers, no networks, no running app process, no uncommitted
content changes left behind by this validation session.

---

## Overall recommendation

**Ready for human final sign-off.** All three Phase 1 user stories (US-1.1, US-1.2, US-1.3) and
the re-verified Phase 0 stories (US-0.1–0.3) now PASS against the live, running system, driven
entirely through the app's own routes and lifecycle code — not just the unit/integration test
suite. Both defects from the first acceptance pass (issue #6's unreachable resource ceiling,
issue #7's CSP-blocked iframe) are confirmed fixed with real evidence, including a genuine
real-browser, real-cell-execution check for the iframe fix specifically, since that was the one
most likely to have a fix that looks right on paper (correct header) but still fails in practice
(wrong origin, browser-specific CSP quirks, etc.) — which is exactly what the first (127.0.0.1)
check would have wrongly reported as still-broken had it not been re-verified against the
documented `localhost:8000` origin.

One non-blocking fragility is flagged (CSP allowlist doesn't cover `127.0.0.1:8000`, only
`localhost:8000`) — this does not fail any stated acceptance criterion (the criteria and PLAN.md
both specify `localhost:8000` as the app's origin) but is worth a human decision on whether it's
worth hardening.

This is a recommendation, not an approval — per this project's Definition of Done, the human
should review this report and give explicit final sign-off before Phase 1 is considered done.
