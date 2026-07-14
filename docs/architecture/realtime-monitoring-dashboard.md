# ADR: Realtime Cluster Monitoring Dashboard (Phase 2.5)

Status: Accepted (architect handoff)
Date: 2026-07-14
Requirements: `docs/requirements/realtime-monitoring-dashboard.md` (US-5.1–US-5.6)
Design reference: `docs/architecture/realtime-monitoring-dashboard-mockup.dc.html`
Builds on: PLAN.md D1–D6, §1–§4, §6; `app/lifecycle/`, `app/spark_api/master_client.py`, `app/config.py`

---

## Context

Phase 1 gives the learner a live cluster but only two windows into a running job: the raw Spark master UI
(`:8080`) and the driver application UI (`:4040`), plus the Phase 2 pull-based annotation self-check. None of
these put "which node is doing what, how much, and how long" in one continuously-live view, and none show
container-level CPU/RAM. This feature adds a single, always-on, cluster-wide diagnostic page so a learner can
practice the interview-realistic skill of diagnosing a slow job *in real time*.

The dominant constraints are: (1) it must stay a **diagnostic aid the learner interprets, not an automated
advisor** (G3 — the same philosophy that makes the Phase 2 annotation engine pull-and-reveal rather than
auto-explain); (2) it must fit the existing single-process, Jinja2+HTMX, single-slot-cluster architecture (D4/D5)
without introducing a parallel frontend stack or a standing observability platform (G1 — curriculum depth over
platform polish); and (3) two of the metrics it needs (container CPU/RAM) have **no Spark REST source at all** and
must come from Docker, which is a genuinely new data source for this codebase.

The single most consequential decision here is how to reconcile the supplied mockup — whose "bottleneck" cards
carry explicit tuning **conclusions** ("Suggestion: salt the join key…") — with G3 and with US-5.4's explicit
acceptance criterion that the dashboard "does not include an automated 'recommended fix' or
configuration-tuning-suggestion feature."

---

## Decision

Five decisions, stated plainly. D-A is the load-bearing one.

### D-A — Signal, not conclusions: drop the prescriptive "Suggestion" line; keep the factual signal (full G3 compliance)

The mockup's bottleneck cards are split into two kinds of content, and G3 draws the line between them:

- **Factual/observational content** — the quantified `detail` line ("worker-2 holds 3 partitions ~5x larger than
  the cluster median in the join stage"), the SKEW badge, the color-threshold bars, the CPU/RAM imbalance made
  visible side-by-side. This is **signal**. G-RTD4 *explicitly mandates* making skew, uneven processing time, and
  resource imbalance "visually obvious." Surfacing it is the whole point.
- **Prescriptive content** — the `Suggestion: salt the join key or repartition…` line, and the "— consider
  repartitioning or salting the join key" tail on the alert banner. This is a **conclusion / recommended fix**. It
  is the exact thing US-5.4's third acceptance criterion forbids and the exact thing G3 exists to prevent.

**Chosen: option (b).** Remove every prescriptive clause. Keep the bottleneck cards, but reframe them as neutral
**"signal spotlight" cards**: icon + a category label describing *what was observed* (e.g. "Partition size
distribution", "GC / memory", "Stage share of runtime") + the quantified factual `detail` line + a deep link into
the relevant view or the real Spark UI. No `Suggestion:` line. The alert banner keeps its observational half
("worker-2: partition size 1.7x cluster median") and its "View job diagnosis →" link, and drops the "consider
repartitioning…" tail.

Category labels stay descriptive of the *measurement*, not the remedy. This mirrors the annotation engine's
established precedent exactly: the manifest is allowed to label an `Exchange` node "shuffle boundary" (naming the
*what*), but never generates the *why* or the *fix* (US-2.1). "This partition is 5x the median; here is a badge and
a link" is the same side of the line as "this is a shuffle boundary"; "salt the join key" is not.

**Why not (a) keep the suggestions:** it violates a written acceptance criterion (US-5.4 c3), not merely a
philosophy, and it defeats the pedagogical purpose — if the app hands over the fix, there is no hypothesis left for
the learner to form.

**Why not (c) reveal-gate the suggestions:** a reveal-gated recommendation is still a recommendation feature (still
fails US-5.4 c3), and gating re-imports the annotation engine's explicit checkpoint-then-reveal gesture into a tool
the human *deliberately kept separate and always-on* (requirements §"Relationship to the Phase 2 annotation
engine"). In practice a reflexively-clicked reveal changes nothing pedagogically. Option (c) gets the worst of both:
it neither honors the always-on separation nor removes the conclusion. If we ever want a "check your tuning
hypothesis" experience, its natural home is the *annotation engine / capstone*, which already owns the reveal
pattern — not this dashboard.

Net effect: the dashboard becomes very good at *directing the learner's attention to where a problem is and
quantifying it*, and stops short of naming the cause or the cure. That is precisely G3-aligned "surface signal, not
conclusions," and it keeps almost all of the mockup's visual value.

### D-B — Real-time mechanism: single server-side collector + Server-Sent Events (via the HTMX SSE extension)

Decouple **collection** from **delivery**:

- **Collector** — one `asyncio` background task samples *all* data sources **once per cycle** into an in-process
  snapshot (see D-C/D-D). It runs only while a cluster is READY and at least one dashboard client is connected;
  it stops otherwise (satisfies US-5.5's "polling stops when the browser leaves").
- **Delivery** — the browser holds one **SSE** connection (`text/event-stream` via FastAPI `StreamingResponse`).
  On each new snapshot the server pushes named events; the page uses the **HTMX SSE extension**
  (`hx-ext="sse"`, `sse-swap="..."`) to swap the affected server-rendered fragments in place. View switching
  (Overview / Job Detail / Node Detail) stays 100% client-side (show/hide), so a view change is not a navigation
  and does not drop the stream.

This is the smallest defensible step up from D4's HTMX polling, chosen because this feature is exactly the case
D4's 6s single-panel polling was *not* designed for: **three panels updating simultaneously at ~2s** (node grid,
stage timeline, partition table). Three independent `hx-trigger="every 2s"` pollers would mean N drifting
connections each firing its own backend fetch; SSE gives one connection, server-controlled cadence (no client-timer
drift or overshoot), and — critically — the collector-behind-it means the slow Docker sample happens **once per
cycle regardless of panel or client count**, not once per fragment. It stays within the HTMX idiom (still
server-rendered fragments, still one process, one language) rather than jumping to a client-state framework.

WebSocket is rejected: the data flow is strictly server→browser (read-only dashboard; the only client→server action
is view switching, which is local). WS's bidirectionality and framing/reconnect complexity buy nothing here.

**Latency target (resolves requirements Open Question 3):** effective end-to-end **≤ 3 s**, collector cadence ~2 s.
This tightens the requirements doc's proposed 5 s toward the mockup's "updates every 2s" feel, and it's honestly
achievable: `docker stats` for the whole (≤6-container) stack is one sub-2s call, the Spark REST pulls are fast, and
SSE push adds negligible latency on localhost. We commit to ≤3 s rather than a hard 2 s because `docker stats`
inherently spends ~1–2 s producing CPU% (it needs a sample delta), so a strict 2 s ceiling would be fragile; ≤3 s is
comfortably inside US-5.5's 5 s bound while matching the intended live feel.

### D-C — CPU/RAM/Disk/Net source: Docker directly via `docker stats`, no cAdvisor sidecar (resolves Open Question 1)

Source container CPU%, memory used/limit, block (disk) I/O, and net I/O from **Docker itself**, invoked the same
way the codebase already talks to Docker — shelling out to the CLI via `asyncio.create_subprocess_exec`, mirroring
`app/lifecycle/compose_ops.py`. Concretely: `docker stats --no-stream --format '{{json .}}'` scoped to the
`sparkpb` project's containers. This returns *exactly* the mockup's fields already computed (CPU %, MemUsage/Limit,
NetIO, BlockIO) in one call for the whole stack — no per-container CPU%-delta math, and **no new pip dependency**.

**No cAdvisor.** cAdvisor is a standing sidecar container that exists purely for platform polish, adds a moving
part and a memory footprint that counts against the 32 GB ceiling (`config.RESOURCE_CEILING_GB`), and buys nothing
on a single-host cluster of ≤6 containers. That is squarely the G1 "curriculum depth over platform polish" tradeoff
the requirements flagged, and it lands on "not worth it." The Docker socket is already reachable per D1, and the
same mechanism covers the driver container (it's on `sparkpb-net`, per the resolved driver-in-scope decision) with
no extra source. The known "overhead when polled per-container repeatedly" concern is neutralized by D-B's
single-collector design: one `docker stats` call per ~2 s cycle for all containers, never per-panel or per-client.

Container discovery is by compose project label (`com.docker.compose.project=sparkpb`) so it tracks whatever
`worker_count` was spawned, and a container that stops simply drops out of the next sample — which is how US-5.1's
"reflect that stats are no longer available" is satisfied (stale nodes are marked unavailable, not frozen at
last-known values).

**CPU% caveat (implementation note):** `docker stats` reports CPU% relative to total host cores. The color
thresholds must mean "saturating its *allotted* cores," so the collector normalizes CPU% against each container's
configured cpu limit (`deploy.resources.limits.cpus` in the compose template — 1 for master, `worker_cores` per
worker, 2 for driver), i.e. 100% = fully using its allocation.

### D-D — GC time source: Spark's `/executors` REST endpoint, not Docker (resolves the mockup's JVM-metric gap)

GC time is a JVM metric and cannot come from Docker stats. It is, however, already exposed by Spark's application
REST API: `GET http://localhost:4040/api/v1/applications/<id>/executors` returns per-executor `totalGCTime` (and
`totalDuration`, `memoryUsed`, etc.), and the driver appears in that list as executor id `driver`. This reconciles
with the requirements doc's "CPU/RAM has no Spark REST source" framing — that remains true for **CPU/RAM
specifically**; GC time is a *different* metric with a different, already-available source.

The two sources are joined per node: a Spark executor's host (`spark-worker-1`, `spark-driver`) equals the Docker
`container_name`/`hostname` in the compose template, so the join key is the service name — no IP mapping needed.
GC time is only meaningful while an application is running (no executors ⇒ no GC data); with no active job the GC
field shows "—", consistent with the "cluster up, no job" state (US-5.1 first criterion).

This reuses the Phase 2 `spark_api/app_client.py` REST client as a **library dependency** (both `/executors` and
`/stages` live at `:4040`), exactly the "possibly reusing the existing REST client" the requirements anticipated —
no annotation-engine logic is shared.

### D-E — Placement, scope, and cross-linking

- **Placement (resolves Open Question 2):** a **standalone page** at `/dashboard`, with its own top bar branding,
  matching the mockup unambiguously. It is *not* embedded in a topic page or the cluster control panel. It's
  reachable from a persistent nav link (shown whenever a cluster is running) and from the cluster panel. With no
  active cluster it renders a clear "no active cluster" empty state (US-5.6). It deep-links *out* to the real Spark
  UI for per-stage detail (US-5.6, G-RTD5), rather than duplicating it.
- **Non-goals (resolves Open Question 4):** confirmed — no persistent/queryable history, no alerting/paging, no
  auto-tuning (the last now settled by D-A). Two nuances made explicit so they aren't mistaken for violations:
  - The Node Detail sparklines (20 buckets of CPU/RAM history) are rendered from a **bounded in-memory ring buffer
    held by the collector** — ephemeral, lost on process/cluster restart, not queryable. This is *not* the
    time-series storage the no-history non-goal rules out.
  - The Overview "alert banner" is a **live, in-page, derived visual flag** computed from the current snapshot,
    present only while the learner is looking. It is not a notification, is never persisted, and never fires when
    the page is closed — so it is signal surfacing (G-RTD4), not "alerting."
  - Retention boundary confirmed: the dashboard keeps the **current or most-recently-completed stage** (US-5.2 c3),
    nothing older.
- **Topic cross-linking (resolves Open Question 5):** the dashboard is **topic-agnostic infrastructure** with
  **optional soft links in from the two topics where its signal is most pointed** — AQE (US-2.5) and memory/spill
  (US-4.4). Those topics' `concept.md` may include a "watch this live on the monitor while the job runs" pointer.
  The dashboard itself has **zero topic-specific behavior or coupling** — the links are one-directional markdown
  references, cheap to add and cheap to remove, capturing the pedagogical value without entangling the tool.

---

## Alternatives considered

| Decision | Alternative | Why not chosen |
|---|---|---|
| D-A signal-only | Keep mockup's suggestions | Violates US-5.4 c3 (a written criterion, not just G3) and removes the learner's hypothesis. |
| D-A signal-only | Reveal-gate suggestions (option c) | Still a recommendation feature (fails US-5.4 c3); re-imports the annotation engine's reveal gesture into a tool the human deliberately kept always-on and separate. |
| D-B SSE + collector | HTMX polling at 2s per fragment (D4 default) | 3 drifting pollers × frequency × clients, each re-fetching; multi-panel 2s cadence is exactly what D4's 6s single-panel polling wasn't built for. |
| D-B SSE + collector | WebSocket | Bidirectional/reconnect complexity for a strictly server→browser read-only feed; nothing to send upstream. |
| D-C Docker stats | cAdvisor sidecar | Standing container purely for polish; costs RAM against the 32GB ceiling and adds a moving part — the G1 tradeoff lands on "no." |
| D-C Docker stats | Docker Python SDK | Works, but adds a pip dep; the CLI (`docker stats --no-stream --format json`) matches the existing `compose_ops` subprocess convention with zero new deps. Noted as fallback if CLI formatting proves limiting. |
| D-D Spark `/executors` | Drop GC time entirely | Mockup shows it and it's a real, JVM-native diagnostic signal already exposed at `:4040` — no reason to omit. |
| Phase label | Renumber this to Phase 3 (streaming→4, curriculum→5) | Cleaner in isolation, but the backlog (items 9–13, 19, 22) and requirements docs already fix "Phase 2.5 = dashboard, Phase 3 = streaming, Phase 4 = curriculum"; renumbering would desync the backlog and any filed issues. Adopt "Phase 2.5" as the real label. |

---

## Consequences

**Accepted trade-offs:**

- **We deviate from the supplied mockup** by removing its `Suggestion:` lines and the banner's prescriptive tail.
  This is a deliberate, principled deviation for G3 compliance, documented so a later visual check knows the
  suggestion text's *absence is correct*, not a missing element.
- **SSE + a background collector is more moving parts than a single `hx-trigger` poll** — there's now a lifecycle
  (start on first client + READY cluster, stop on last disconnect / teardown) and an in-memory snapshot to manage.
  This is new surface area relative to Phase 1/2's stateless request/response fragments. It buys the multi-panel 2s
  cadence and bounds Docker's cost; the complexity is real and isolated to the new module.
- **`docker stats` CPU% has inherent ~1–2s latency**, so we commit to ≤3s, not a hard 2s. Honest about the feel:
  "very live," not "instantaneous."
- **The Docker-container ↔ Spark-executor join depends on the compose naming convention** (service name ==
  hostname == executor host). It's clean today but is a coupling to the compose template (see Risks).
- **What becomes harder:** if the project later wants real historical charts, alerting, or multi-app views, the
  in-memory-snapshot design is deliberately a dead-end for those — they'd need the storage/observability layer this
  ADR explicitly declines to build. That's the intended boundary, not an oversight.

---

## Component / data design

New module `app/monitoring/` plus one standalone page. Reuses `spark_api/` clients; shares no code with
`annotation/`.

```
app/
  monitoring/
    docker_stats.py    # async `docker stats --no-stream --format json`, scoped to project label;
                       #   parse -> per-container {cpuPct(norm), memUsed/limit, blockIo, netIo}
    collector.py       # background asyncio task: every ~2s, sample docker_stats + spark executors + stages,
                       #   normalize/join into one Snapshot, push to a ring buffer + notify SSE subscribers.
                       #   Lifecycle-gated: runs only while manager.state==READY and >=1 SSE client attached.
    model.py           # Snapshot dataclasses: NodeStat, JobSummary, StageBar, PartitionRow, SignalCard
    diagnostics.py     # PURE signal derivation ONLY: skew flag (task/partition size vs median),
                       #   node-imbalance flag, ETA + duration spread. NO fix/suggestion text (D-A).
    eta.py             # avg(completed task duration) * remaining tasks; min/median/max spread (US-5.3)
  spark_api/
    app_client.py      # (Phase 2) reused: /applications, /applications/<id>/executors, /stages/<id> (+task list)
    master_client.py   # (existing) unchanged
  web/
    routes/dashboard.py   # GET /dashboard (full page), GET /dashboard/stream (SSE), fragment renderers
    templates/dashboard/  # overview.html, job_detail.html, node_detail.html, fragments/*.html
```

**Data flow (one collector cycle):**

```
 collector tick (~2s, only if READY + clients>0)
   ├─ docker_stats.sample()            → per-container CPU%/RAM/disk/net   (Docker CLI, project-scoped)
   ├─ app_client.executors(app_id)     → per-executor totalGCTime          (:4040 /executors)   [if job active]
   ├─ app_client.active_stage(app_id)  → per-task list for current/last stage (:4040 /stages)   [if job active]
   ├─ join on service-name (container ↔ executor host)  →  Snapshot
   ├─ diagnostics.derive(snapshot)     → skew/imbalance FLAGS + ETA (signal only, no fixes)
   ├─ ring_buffer.append(snapshot)     → last N samples (sparklines; ephemeral)
   └─ notify SSE subscribers           → server renders + pushes affected fragments (HTMX sse-swap)
```

**Snapshot shape (in-memory only, per cycle):**

- `nodes: [NodeStat]` — name, role(master|worker|driver), cpuPct(normalized)+color, ram used/limit+color,
  diskIo, netIo, gcMs+color (or n/a), partitionCount (workers, when job active), flagged+flagReason(factual).
- `job: JobSummary | None` — name, appId, statusLabel, stageLabel (e.g. "Stage 3/5"), stageName, elapsed,
  eta+spread. `None` ⇒ "cluster up, no job" state.
- `stages: [StageBar]` — for the Gantt strip: label, startPct, widthPct, durationLabel, state(done|current|pending).
- `partitions: [PartitionRow]` — node, task/partition id, size+bar+color, rows, shuffle R/W, time/ETA, retries,
  isSkewed(factual flag). "Partition" == "task" per the requirements' measurability note.
- `signalCards: [SignalCard]` — icon, category label (observational), factual detail line, deepLink. **No
  suggestion field** (D-A).

**Reused / unchanged:** `lifecycle/manager.py` singleton is read (not modified) for cluster state; `config.py`
gains dashboard constants (route, collector interval, color thresholds, ring-buffer length). No change to the
lifecycle state machine, the compose template, or the annotation engine.

**Threshold color system (from the mockup, kept):** green `#16a34a` / amber `#d97706` / red `#dc2626` by
threshold (CPU warn 70 / crit 88; RAM warn 75 / crit 90; GC amber >20ms / red >40ms), MASTER badge purple
`#7c3aed`. These live as constants in `config.py` so the server-side fragment renderers apply them (server-rendered,
not client JS).

---

## Visual design

Source of truth: `docs/architecture/realtime-monitoring-dashboard-mockup.dc.html` (a Claude Design-Component file;
read as a spec, not rendered). It is faithfully translated **except the D-A change below**. A single-page app with
three client-switched views (`state.view`), no page navigation between them.

**Persistent top bar (all views):** dark `#12151c`; orange→red gradient logo mark; "Spark Cluster Monitor";
cluster name · mode; a green pulsing dot + "Live · updates every ~2s"; current time (right).

**Overview view:**
- *Alert banner* (conditional, only when a node is flagged): amber `#fff4e5` bar, ⚠ + factual title
  ("Skew detected on worker-2") + factual detail ("partition size 1.7x cluster median") + "View job diagnosis →"
  link. **D-A: the "— consider repartitioning / salting…" prescriptive tail is removed.**
- *Job summary strip* (white card row): Job name/id, Status pill (color by state), Stage ("Stage 3 / 5" +
  stage name), Elapsed, ETA, and a "Job Detail →" button.
- *Node grid*: responsive `repeat(auto-fill,minmax(280px,1fr))` cards. Each card: status dot + node name +
  MASTER badge (purple, master only) + host; CPU% and RAM% as color-coded progress bars; a 3-up row of Disk I/O /
  Net I/O / GC time (GC colored); "Partitions handled" count (workers only, when a job is active); and a red
  factual flag badge ("Data skew: handling 1.7x avg partition size") when flagged. Cards are clickable → Node
  Detail.

**Job Detail view:**
- Back-to-Overview header with job identity + status pill + elapsed/ETA.
- *Stage timeline* (horizontal Gantt): one row per stage, proportional bar widths by duration, current stage red,
  done stages green, pending stages grey `#c7cad2`, each with a note.
- *Signal spotlight cards* (3-up) — **this is the D-A-modified region.** Each card: icon + observational category
  label + factual detail line + deep link. Concretely, the three become: "Partition size distribution" (skew
  facts), "GC / memory" (GC-time + RAM facts), "Stage share of runtime" (critical-path facts). **The italic
  `Suggestion: …` line present in the mockup is removed from every card.** A later screenshot check should confirm
  these cards show *no* remedy text.
- *Partition distribution table*: columns Node / Partition (id + SKEW badge + size bar) / Size / Rows /
  Shuffle R/W / Time·ETA / Retries. Skew rows tinted `#fef9f2`; size/retry values color-coded. Summary line
  ("N partitions · avg X MB · max Y MB (Zx skew)").

**Node Detail view:**
- Back header with node identity + MASTER badge + factual flag (if any).
- Four stat tiles: CPU / RAM / GC (current values, colored) / Disk·Net.
- Two sparkline strips (CPU history, RAM history) — 20 color-coded bars from the ring buffer.
- That node's own partition table (same columns as Job Detail, minus the Node column).

**Distinct states to verify (beyond "it works"):**
- *No active cluster:* the whole page shows a single clear "no active cluster" empty state (US-5.6), not an error
  or blank.
- *Cluster up, no job:* node grid shows CPU/RAM/disk/net; GC shows "—"; no partition counts; no job strip
  (or an explicit "no active application" state); no signal cards.
- *Job running:* full Overview populated; ETA numeric with spread.
- *Zero completed tasks in stage:* ETA shows "estimating…", not a number (US-5.3).
- *Node/container stopped mid-view:* that node marked unavailable, not frozen at last values (US-5.1).
- *Skew present:* flagged node badge + alert banner + SKEW-tinted rows all visible; **no suggestion text anywhere.**

---

## Risks

- **R-Dash-1 — Docker↔Spark join breaks if the compose naming convention changes.** The node join relies on
  container `service name == hostname == Spark executor host`. If the template ever renames services or an
  executor reports an unexpected host, GC time and partition counts silently fail to attach to the right card.
  *Noticed by:* a node card showing CPU/RAM but a permanently blank GC / partition count while a job is clearly
  running. *Mitigation:* join defensively (fall back to "—" on no match, never mis-attach); a small integration
  test asserts the join against a real 1-worker spawn.
- **R-Dash-2 — `docker stats` latency/overhead drifts the cadence.** If per-cycle sampling creeps past ~2s under
  load, effective latency approaches the 5s bound. *Noticed by:* the "Live" cadence visibly lagging the Spark UI.
  *Mitigation:* single project-scoped call per cycle; measure cadence from completion; the ≤3s target already
  budgets for this. Docker SDK is the escape hatch if the CLI is the bottleneck.
- **R-Dash-3 — Collector lifecycle leaks (keeps sampling after teardown or after all clients leave).** A stray
  collector polling a torn-down stack wastes cycles and logs errors. *Noticed by:* `docker stats` errors in logs
  with no dashboard open. *Mitigation:* gate strictly on `manager.state == READY` **and** subscriber count > 0;
  stop on cluster teardown (observe the manager) and on last SSE disconnect.
- **R-Dash-4 — SSE connection drops (browser sleep, network blip) leave a stale page.** *Noticed by:* frozen
  values while the job runs. *Mitigation:* rely on the browser's built-in SSE auto-reconnect; render a
  last-updated timestamp so staleness is visible; HTMX SSE extension reconnects the swap targets.
- **R-Dash-5 — Multiple `SparkSession`s bump the driver UI to 4041 (PLAN.md R2).** Same failure mode as Phase 2:
  the dashboard would find no app at `:4040`. *Mitigation:* reuse the existing "no active application on 4040"
  handling from `app_client`; surface it as the dashboard's "no active job" state rather than polling a stale port.
- **R-Dash-6 — Scope creep back toward conclusions.** The signal-spotlight cards are one product-pressure step
  away from re-growing "Suggestion" text. *Noticed by:* review/acceptance against D-A. *Mitigation:* the
  `SignalCard` model has **no** suggestion field by construction, and diagnostics.py is documented as
  signal-derivation-only — adding a fix means adding a field, which forces a conscious G3 conversation.

---

## PLAN.md §5 update (Phase 2.5 insertion)

PLAN.md §5 has been updated to formally insert **Phase 2.5 — Realtime Cluster Monitoring Dashboard** between
Phase 2 and Phase 3, adopting the requirements doc's and backlog's existing "Phase 2.5" label (Phase 3 =
streaming and Phase 4 = curriculum are left unchanged, to stay consistent with backlog items 9–13/19/22 and any
filed issues). See PLAN.md §5.
