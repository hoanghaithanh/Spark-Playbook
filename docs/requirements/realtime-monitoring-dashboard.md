# Realtime Cluster Monitoring Dashboard — Requirements (Phase 2.5)

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-14

**Sequencing (resolved by the human, 2026-07-14):** this feature is pulled forward in the roadmap, sequenced
immediately after Phase 2 (annotation engine + join strategies + bucketing + AQE) and before Phase 3
(streaming/Kafka) — not last as originally proposed. This doc uses the working label **"Phase 2.5"** rather than
inventing a fixed phase number, since renumbering PLAN.md's Phase 3/4 labels (if that's even wanted) is a
PLAN.md-owning decision for the architect/project-manager, not something this requirements doc does on their
behalf. See the removed "sequencing" open question (formerly Open Question 7) — no longer open.

## Problem statement

Today the only windows into a running job are the raw Spark Standalone master UI (`:8080`), the driver's
application UI (`:4040`), and the app's own on-request annotation self-check (Phase 2, pull-based). None of these
show node-level resource utilization (CPU/RAM per container), and none put "which node is doing what, how much,
and how long it's taking" in one continuously-live view. A learner trying to practice the interview-realistic
skill of "diagnose why this job is slow, in real time, while it's running" currently has to tab-hop between two
Spark UIs and mentally reconstruct the picture themselves. This feature adds one live, cluster-wide diagnostic
view — complementary to, not a replacement for, the existing Spark Builtin UI and the app's own surfaces — so the
learner has a single place to watch resource and execution signals as a job runs and practice spotting what's
tunable.

## Relationship to G1 and to the Phase 2 annotation engine

**Connection to G1 (interview-depth over platform polish).** Diagnosing a slow/stuck Spark job from live signals
(one executor pegged while others idle, a handful of oversized partitions, a stage crawling toward a fuzzy ETA) is
itself a common interview question ("walk me through how you'd debug a slow job"). This feature is squarely in
scope of G1's priority, not a platform-polish detour — assuming it stays a *diagnostic aid the learner interprets*,
not an automated advisor (see G3 below and US-5.4). This same interview-relevance is also why the human pulled
this feature's sequencing forward to right after Phase 2 rather than leaving it last (see the sequencing note
above).

**Relationship to the Phase 2 annotation engine — resolved: genuinely separate tool.** The annotation engine
(PLAN.md §3) is a deliberately **pull-based self-check**: the learner calls `playbook.checkpoint(df, topic=...)`
and then explicitly clicks "Reveal" before any plan/metric interpretation appears, per G3. This dashboard, as
requested, is the opposite trigger model — a **continuously-live, always-on** view with no explicit "reveal"
gesture, not scoped to a specific topic's manifest, and covering a different data surface (container resource
utilization + task/partition execution, not plan-node labeling). The human has confirmed these stay **separate**
rather than being merged or treated as one evolving somewhat into the other:

- **Different trigger model.** Always-on/continuous (this dashboard) vs. explicit checkpoint-then-reveal
  (annotation engine, G3).
- **Different data source.** Docker container stats + Spark's task list (this dashboard) vs. `explain()` plan
  text + stage metrics pulled on Reveal (annotation engine).
- **Different scope.** Cluster-wide, tied to whatever job/application is currently running, independent of topic
  (this dashboard) vs. one topic's manifest-declared plan-node/metric mappings (annotation engine).
- **No shared code** between `annotation/` and this feature is expected, beyond possibly reusing the existing
  `spark_api/app_client.py` REST client as a library dependency (both need to talk to `:4040`'s REST API, so
  reusing that thin client is sensible; the engine/manifest/plan-parser logic itself is not shared).

What this doc still commits to regardless of the separate-tool decision: whatever is built should preserve G3's
underlying principle (the dashboard surfaces signal; it does not do the diagnosis for the learner). Note that the
user's own framing of the goal — "allowing user to diagnose the job... and detect stages/steps/configuration that
can be tuned" — already centers the learner as the one doing the diagnosing, so this is likely consistent with G3
without needing to be reframed; see US-5.4's acceptance criteria for where this is made concrete and testable.

## Goals / Non-goals

### Goals

- **G-RTD1 — Live per-node resource utilization (master, workers, and driver).** Show CPU and RAM utilization for
  the master container, each worker container, **and the driver container**, while the cluster is up, refreshed
  continuously (not a one-time snapshot). Driver inclusion is resolved — see the Non-goals note below and US-5.1.
- **G-RTD2 — Live per-node job execution detail.** While a job is running, show how many tasks each
  executor/worker is currently handling, the size of data each task is processing, and each task's duration —
  the practical stand-in for "partitions per node" (see the measurability note below on why this is task-level,
  not a literal separate "partition" concept).
- **G-RTD3 — Estimated stage completion time.** Surface a derived, clearly-labeled-as-an-estimate ETA for the
  currently running stage.
- **G-RTD4 — Diagnostic signal legibility.** Make skew (uneven partition/task sizes), uneven per-node processing
  time, and resource imbalance across nodes visually obvious, so the learner can form and test their own tuning
  hypothesis — consistent with G3, not in spite of it.
- **G-RTD5 — Complement, not replace.** The dashboard sits alongside the raw Spark Builtin UI (`:8080`/`:4040`)
  and the app's existing cluster control panel (Phase 1) and annotation engine (Phase 2); it deep-links into the
  real Spark UI where useful (matching the existing US-2.2 pattern) rather than re-implementing everything Spark
  already shows well.

### Non-goals (proposed — flagged for human confirmation, not assumed settled)

- **No historical/long-term metrics storage.** This is a live view of the currently running (or most recently
  completed) job/stage, not a time-series store the learner can query days later. **Flagged: confirm with the
  user** — the request describes a live diagnostic tool, but "recently completed stage" retention (US-5.2's third
  criterion) brushes up against this line and should be confirmed as the intended boundary.
- **No alerting/notifications.** No paging, no threshold-crossing alerts, no background monitoring when the
  learner isn't looking at the page. This is a single-user, ad-hoc, foreground diagnostic tool, not a production
  observability platform.
- **No automated tuning recommendations or auto-generated "why" narratives.** Per G3's existing precedent (see
  above) — the dashboard shows signal, not conclusions.
- **The driver IS in scope for resource monitoring (resolved).** Unlike the FastAPI app process, the browser, or
  the WSL2/Windows host — which remain explicitly out of scope, since none of them are the thing being taught —
  the driver container is a Spark cluster component the learner directly interacts with (e.g. `collect()`-driven
  memory pressure, per the existing US-4.4 memory/spill topic) and is included alongside master and workers in
  the resource-utilization view. See G-RTD1 and US-5.1.
- **No multi-cluster or multi-application history.** Consistent with the existing single-slot cluster lifecycle
  (D5, cancel-and-replace) — there is one active cluster and at most one active application at a time; the
  dashboard reflects that one instance, not a fleet.
- **No new production-grade metrics infrastructure** (e.g., a standing Prometheus/Grafana stack) is required by
  this doc — but a lightweight new *data source* likely is (see Open Question 1). The choice of mechanism is the
  architect's, not decided here; this non-goal only rules out over-building a general-purpose observability
  platform.

## What's actually measurable — the gap between the request and what Spark/Docker expose

This section exists so the acceptance criteria below don't promise something the underlying platforms cannot
deliver.

- **CPU/RAM utilization per master/worker/driver container is *not* exposed by Spark's own REST API at all.**
  Spark's `/api/v1/applications/...` surface reports task/stage/executor *execution* metrics (bytes shuffled, task
  duration, memory spill), not container-level CPU%/RAM usage. That data has to come from **Docker itself** — the
  Docker Engine API's per-container stats endpoint (`GET /containers/{id}/stats`, what `docker stats` uses) or an
  agent like cAdvisor reading `/proc`/cgroups inside each container. Which mechanism is the architect's call
  (Open Question 1), but the requirements doc is explicit that this is a **different data source** than the
  existing `spark_api/` REST clients, not an extension of them. Since the driver is, per PLAN.md's D1 architecture,
  a container on the same Docker network as master/workers (not a host process), the same Docker-stats mechanism
  covers it too — no separate data source is needed to satisfy the resolved driver-inclusion decision.
- **"Number of partitions each node is handling" and "size of each partition" are approximated by task-level
  data, which Spark's REST API *does* expose.** Spark's execution model runs (in the common case) one task per
  partition within a stage, so the per-task fields already returned by
  `/api/v1/applications/<id>/stages/<id>` (with task details) — executor ID, input/shuffle bytes, task duration —
  are a faithful stand-in for "partitions per node" and "partition size." This doc treats "partition" and "task"
  as interchangeable for this feature's purposes and says so explicitly, rather than inventing a partition-level
  API that doesn't exist.
- **"Processing time" per task/partition is directly available** (task duration, from the same endpoint).
- **"ETA" is not directly exposed by Spark and must be derived/estimated.** The only defensible approach is
  something like *average duration of completed tasks in the current stage × remaining task count*, which is a
  rough estimate that gets worse under skew (the very condition the learner is trying to diagnose) and does not
  account for AQE re-planning mid-job. Acceptance criteria below require this to be **visibly labeled as an
  estimate** and to expose the underlying variance rather than presenting a single confident number.

## User stories and acceptance criteria

**US-5.1 — Live per-node resource utilization (master, workers, and driver).**
As a learner, I want to see live CPU and RAM utilization for each running Spark container — the master, each
worker, **and the driver** — so that I can correlate resource saturation with job behavior in real time, including
driver-side pressure (e.g. from `collect()`-heavy operations, per the existing US-4.4 memory/spill topic).

- *Given* a running cluster with no job active, *when* I open the dashboard, *then* I see current CPU% and RAM
  usage (used/limit) for the master container, each worker container, **and the driver container**, sourced from
  Docker's own container stats (not from Spark's REST API, which does not expose this — see the measurability
  note above).
- *Given* a job actively running on the cluster, *when* I view the dashboard during execution, *then* the
  CPU/RAM values update to reflect current load on each container — master, workers, and driver alike — not a
  static snapshot taken at spawn time (see US-5.5 for the latency bound).
- *Given* any monitored container (master, a worker, or the driver) that stops or is removed (e.g., the cluster is
  torn down while I'm viewing the dashboard), *then* the dashboard reflects that the container's stats are no
  longer available rather than silently continuing to show its last-known values indefinitely.

**US-5.2 — Live per-node task/partition execution detail.**
As a learner, I want to see, for the stage currently running, how many tasks (partitions) each
executor/worker is handling, the size of the data each task is processing, and how long each task has taken, so I
can spot skew or an imbalanced workload across nodes while the job is running.

- *Given* a running application with an active stage, *when* I view the dashboard, *then* I see a per-task
  breakdown for that stage — executor/worker id, task duration (or elapsed time for still-running tasks), and
  per-task input/shuffle read-or-write bytes — sourced from the stage's task-list REST data
  (`/api/v1/applications/<id>/stages/<id>` with task details), grouped/visualized by executor so per-node load is
  directly comparable.
- *Given* a stage with some tasks completed and some still running, *when* I view the per-node breakdown, *then*
  task counts and sizes currently assigned to each executor are visible together, so an imbalance (one worker
  handling far more or far larger tasks) is visually apparent without any accompanying "this node is overloaded"
  conclusion from the app.
- *Given* a stage that has just completed, *when* I view the dashboard, *then* I still see that stage's final
  per-task/per-node summary (not only whatever stage is currently running), so a learner who looks a moment too
  late isn't locked out of the data. Retention beyond "the most recently completed stage" is out of scope per the
  proposed historical-storage non-goal above — flagged for confirmation.

**US-5.3 — Estimated time remaining for the running stage.**
As a learner, I want an estimated time-to-completion for the currently running stage, so I can gauge whether a
job is on track or stalled without guessing.

- *Given* a running stage with at least one completed task, *when* I view the dashboard, *then* I see an
  estimated remaining time computed from completed-task average duration × remaining task count, visibly labeled
  as an estimate (not presented as an authoritative figure) — since Spark's REST API exposes no true ETA.
- *Given* a stage with zero completed tasks so far, *when* I view the dashboard, *then* no numeric ETA is shown
  (an estimate from zero samples would be misleading); the dashboard shows an "estimating..." or equivalent state
  instead.
- *Given* a stage where task durations vary widely (skew), *when* I view the ETA, *then* the dashboard also shows
  the underlying task-duration spread (e.g., min/median/max) alongside the single estimate, so the learner can
  judge how much to trust the number themselves rather than the app asserting confidence it doesn't have.

**US-5.4 — Diagnostic signal surfacing without automated diagnosis.**
As a learner, I want skewed partition sizes, uneven per-node processing time, and resource saturation on one node
versus idle others to be visually obvious, so I can practice diagnosing what in the job or cluster configuration
might be tunable — without the app telling me the answer.

- *Given* a stage where one task's input/shuffle bytes are markedly larger than the others, *when* I view the
  per-task breakdown, *then* the disparity is visually apparent (e.g., sortable or visually differentiated
  values) with no accompanying interpretive text (e.g., no "this is skew" or "reduce shuffle partitions" label) —
  matching G3's established self-check precedent for the annotation engine.
- *Given* one worker's CPU/RAM utilization sitting near saturation while another is comparatively idle during the
  same job, *when* I view the dashboard, *then* both nodes' current utilization are visible together so the
  imbalance is directly observable, again with no generated explanation of cause.
- *Given* this story's scope, *when* the dashboard is built, *then* it does not include an automated
  "recommended fix" or configuration-tuning-suggestion feature — surfacing the raw signal is the target; forming
  and testing a tuning hypothesis remains the learner's task.

**US-5.5 — Real-time update latency.**
As a learner actively watching a job run, I want the dashboard to reflect what's actually happening in the
cluster within a few seconds, so it's useful for live diagnosis rather than a delayed report.

- *Given* a job actively running and a stage transition occurring in the real Spark UI (a stage completes and the
  next one starts), *when* I am viewing the dashboard at the same time, *then* the dashboard reflects that same
  transition within **5 seconds** (proposed target — flagged for confirmation; matches/tightens the existing
  US-2.2 5–10s target already established for the Phase 2 self-check panel, and this feature's explicit "realtime"
  framing plausibly warrants at least matching that bar).
- *Given* the CPU/RAM utilization view specifically, *when* a container's load changes measurably (e.g., a job
  starts consuming CPU on a previously idle worker), *then* the displayed value updates within the same latency
  bound above.
- The specific mechanism achieving this bound (WebSocket, Server-Sent Events, fast polling) is explicitly left to
  the architect; this criterion constrains only observable behavior.

**US-5.6 — Placement and lifecycle relative to existing UI surfaces.**
As a learner, I want the dashboard available whenever a cluster is running, positioned clearly relative to the
topic pages, the cluster control panel, and the raw Spark UIs, so I know where to find it and it doesn't compete
with or duplicate those surfaces.

- *Given* a spawned/running cluster (regardless of which topic, if any, is currently open), *when* I want to
  check live node/job diagnostics, *then* I can reach the dashboard from within the app without first navigating
  to the raw Spark UI. Exact placement — a standalone page, a panel on the cluster control page, or a panel
  attached to each topic page — is an open design question for the architect (Open Question 2), not decided here.
- *Given* no cluster is currently running, *when* I navigate to the dashboard, *then* it shows a clear "no active
  cluster" state rather than an error or a blank page.
- *Given* a specific stage/task of interest on the dashboard, *when* I want more detail than the dashboard shows,
  *then* it offers a deep link into the real Spark UI for that stage — matching the existing US-2.2 pattern — so
  the dashboard is additive to the Spark Builtin UI rather than a walled-off duplicate of it, per G-RTD5.

## Open questions

The human has already resolved three of the original eight open questions (driver-in-scope, sequencing, and the
relationship to the annotation engine — see above); those are no longer listed here. The remaining five are
genuine ambiguity for the **architect** to decide and justify next, not resolved by this update:

1. **CPU/RAM data source and whether it requires a new dependency in the compose stack.** Docker's own container
   stats API (`docker stats` / `GET /containers/{id}/stats`) is directly usable with no new container, but is
   known to have non-trivial overhead/latency characteristics when polled per-container repeatedly. cAdvisor (a
   separate container) is more standard for this but is an additional moving part in a compose stack that
   currently has none dedicated purely to monitoring — worth checking against G1's "curriculum depth over
   platform polish" priority: is a monitoring sidecar container itself in scope, or should this stay to the
   Docker Engine stats API directly? Left to the architect, but the human should be aware a real dependency
   decision sits here.
2. **Where in the app this lives.** The user said "in addition to Spark Builtin UI" but did not say where within
   the app itself — a new standalone page, a panel on the existing cluster control panel (Phase 1), or a
   panel attached to each topic page. Not inferable from the request; needs an explicit answer before UI design.
3. **Confirm the 5-second latency target (US-5.5)** and the "most recently completed stage only" retention
   boundary (US-5.2) — both are this doc's proposed interpretations of "realtime," not numbers the user specified.
4. **Confirm the no-history/no-alerting non-goals.** Reasonable defaults for a single-user ad-hoc diagnostic tool,
   but not stated by the user — flagged rather than assumed.
5. **Is this feature topic-agnostic infrastructure, or should specific curriculum topics reference/link to it?**
   E.g., the AQE topic (US-2.5) and memory/spill topic (US-4.4) are the ones where this dashboard's signal would
   be most pedagogically pointed. Whether the curriculum content should explicitly point learners at the
   dashboard during those topics, or whether it's presented as general-purpose tooling independent of any topic,
   is undecided.

## Constraints

- Builds on top of, and must not break, the existing cluster lifecycle (Phase 1, D5 cancel-and-replace single
  active stack) and the existing `spark_api/` REST clients (Phase 1/2) — this is additive, not a redesign of
  those.
- Same platform constraints as the rest of the project: Windows/WSL2 or Linux, Docker + Docker Compose,
  `localhost`-only, no auth, single user, 64GB RAM host (per the MVP doc's resource budget) — any new
  container/dependency this feature adds (see Open Question 1) counts against the existing resource-ceiling check
  (PLAN.md §2), not a separate budget.
- **Sequencing is resolved (see note at top of doc):** this feature is built right after Phase 2, before Phase 3
  (streaming/Kafka) — not last after the full Phase 0–4 roadmap as originally proposed. Any renumbering of
  PLAN.md's own phase labels to reflect this is left to whoever next updates that document.

## Scope note — this is not a small addition

This is new scope beyond the existing Phase 0–2 curriculum work it now follows, and is realistically **several
independently shippable stories**, not a single unit of work: (a) node resource-utilization monitoring is a
genuinely separate data source and UI surface from (b) per-node task/partition execution detail, which is separate
again from (c) the derived ETA/variance display, and (d) UI placement/integration depends on decisions not yet
made (Open Question 2). The user stories above (US-5.1–US-5.6) are written as that split already, so they can be
pulled into sprints independently rather than as one monolithic "build the dashboard" ticket — recommend the
project-manager sequence US-5.1 and US-5.2 (the two distinct data sources) before US-5.3/US-5.4 (derived/diagnostic
views that depend on that raw data existing), with US-5.6 (placement) resolved early since it affects how the
others are built.
