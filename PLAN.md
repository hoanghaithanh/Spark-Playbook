# Spark Playbook — Technical Design (PLAN.md)

Status: Architecture handoff
Input: `docs/requirements/spark-playbook-mvp.md` (primary, authoritative)
Scope: Solo learning tool, single-user, `localhost` only. No auth, no CI/CD, no deployment target.

Everything in the requirements doc's **Constraints** and **Non-goals** sections is treated as locked.
This document decides the open technical questions the requirements doc deferred to the architect and
lays out a concrete, buildable plan.

---

## Key decisions & alternatives considered

These four choices were explicitly delegated to the architect. Defaults are chosen and justified below —
no further user input required.

### D1 — Where the FastAPI app runs: **inside WSL2 (Ubuntu), repo on the ext4 filesystem**

Docker Desktop is installed on the Windows host. The FastAPI process runs in the WSL2 Linux distro, with
the whole repository living under the WSL2 home directory (e.g. `~/Spark-Playbook`), **not** under `/mnt/c/...`.

- **Docker socket:** From WSL2 with Docker Desktop's WSL integration enabled, the Docker daemon is reachable
  at the standard `unix:///var/run/docker.sock`. The Docker Python SDK and `docker compose` CLI both work with
  zero extra config. Running FastAPI Windows-native would force the SDK onto the Windows named pipe
  (`npipe:////./pipe/docker_engine`), which is supported but fiddlier and less well-trodden.
- **Volume-mount performance:** Bind-mounting generated datasets, notebooks, and the shared annotation dir
  from ext4 into containers is fast. Mounting from a Windows drive (`/mnt/c`) crosses the 9P/drvfs boundary and
  is slow enough to distort the very spill/shuffle timings this tool exists to teach.
- **File-watching:** `uvicorn --reload` and Jinja2 template edits rely on inotify, which works on ext4 but not
  on Windows-mounted paths in WSL2.

**Alternatives considered:**
- *Windows-native Python* — rejected: named-pipe socket + slow `/mnt/c` mounts + broken inotify.
- *Bare-metal Linux* — rejected only because the user's machine is Windows; the design is otherwise identical
  and portable to native Linux with no changes.

### D2 — Spark image: **thin custom `Dockerfile` FROM the official `apache/spark` image**

One image is built locally and reused for master, worker, and driver/Jupyter roles (differentiated only by the
compose `command`). It is `FROM apache/spark:<4.x-tag>` (see D3 — currently the `4.0.3` python tag, e.g.
`apache/spark:4.0.3-scala2.13-java17-python3`) and layers on JupyterLab, PyArrow, the data-gen dependencies
(`numpy`/`faker`), the Kafka SQL connector jars, and the `playbook` helper package. The bundled JDK (Java 17)
and Python (3.11) come from the chosen tag, so Spark 4.0's Java-17+/Python-3.9+ floors are satisfied by tag
selection, not by any extra image work.

**Alternatives considered:**
- *Bitnami `bitnami/spark`* — rejected. Convenient env-driven config, but the 2025 Broadcom/VMware repackaging
  deprecated the free Bitnami Docker Hub tags (moved to `bitnamilegacy`, no further free updates). That is a real
  longevity/supply risk for a tool the user will maintain over time. Not worth the coupling.
- *`apache/spark` official image used directly* — rejected as the day-to-day image because it ships neither
  JupyterLab, PyArrow, the Kafka connector, nor our helpers, and requires full `spark-class` invocations. It is
  the right **base** to build FROM, which is what D2 does.
- *From-scratch `python:3.x` + `pip install pyspark`* — rejected: reinvents the JAVA_HOME/entrypoint plumbing the
  official image already gets right.

### D3 — Spark version: **4.x (pin the latest stable 4.x patch at build time — currently `4.0.3`)**

- **No architectural cost to being on 4.x.** The official `apache/spark` image publishes 4.x tags directly on
  Docker Hub (e.g. `apache/spark:4.0.3-python3`, `4.0.3-scala2.13-java17-python3-r-ubuntu`), so D2's `FROM` line
  is the only version-bearing line and it's a one-liner. The Java-17+ and Python-3.9+ floors Spark 4.0 introduced
  are already met by those tags (Java 17 + Python 3.11 are bundled), so this is a **mechanical requirement
  satisfied by the tag choice, not a design concern**. The Kafka connector is the same `--packages
  spark-sql-kafka-0-10_2.13:<ver>` mechanism with the version string bumped; pandas/PyArrow just need
  4.x-compatible pins in the pip layer (routine).
- **Serves the user's explicit goal.** The user wants to learn the latest Spark line specifically; pinning 4.x
  is the direct way to do that, and nothing in the core architecture (Standalone cluster, Jinja2 templating,
  client-mode driver networking in §6/R1, the annotation engine, the REST polling design) depends on the Spark
  major version — this is a self-contained version swap, not a redesign.
- **Interview-relevant concepts are version-stable.** AQE (on-by-default, mature since 3.2 — skew-join splitting
  and post-shuffle coalescing, US-2.5/US-0.4), Arrow-based vectorized / pandas UDFs (US-4.3), and Structured
  Streaming + the KRaft-mode Kafka connector (Phase 3) all behave the same on 4.x and are what interviews probe.
- **ANSI SQL mode is on by default in Spark 4.0+ (`spark.sql.ansi.enabled=true`)** — e.g. integer overflow now
  throws instead of silently returning null, so some exercise output differs from 3.x-era references. **Decision:
  embrace it, do not override it.** Given G1's interview-depth-first priority, ANSI mode is treated as a teaching
  opportunity (it is itself an increasingly common interview topic) rather than something to suppress. See the
  §6 R6 note so whoever builds Phase 0/1/2 exercises expects the behavioral difference.

**Version pin.** Track the latest stable 4.x patch available as an official `apache/spark` tag at image-build
time. As of this writing that is **4.0.3** (the confirmed-published official tag). A 4.1.x line exists upstream
(Delta Lake 4.1.0 targets Spark 4.1.0), but 4.0.3 is the version with a confirmed official base image; moving to
a 4.1.x tag once one is officially published is the same one-line `FROM` bump.

**Alternative considered:** *Spark 3.5.x (latest 3.5 patch)* — demoted, not chosen. It is the most widely
deployed line in production today, so it is arguably the most "interview-representative." But for a single-user
personal learning tool whose entire point is depth (G1) and whose owner explicitly wants the latest line,
version-representativeness of the currently-deployed release is a real but secondary concern — and the depth
concepts above are version-stable anyway.

### D4 — Frontend: **server-rendered Jinja2 HTML + HTMX** (plus one small charting lib for streaming/metrics)

FastAPI already renders Jinja2. The UI surface is modest — a cluster control panel, topic pages, an embedded
Jupyter iframe, poll-and-refresh stage/metrics tables, and one live streaming-progress chart. HTMX's
`hx-trigger="every 6s"` polling maps exactly onto the US-2.2 (5–10 s stage-metrics refresh) and US-3.3
(streaming progress) requirements with no client-side state framework. A single lightweight chart library
(uPlot or Chart.js, loaded as a static asset) covers the streaming chart.

**Alternative considered:** *Light Next.js* — rejected. Adds a Node build toolchain, a second runtime, and an
SSR/CSR split plus cross-origin concerns, for a single-user local tool whose north star (G1) is curriculum depth
over platform polish. HTMX keeps the entire app one process, one language.

### D5 — Concurrent-spawn behavior (Open Question 1 / US-1.2): **cancel-and-replace**

A second spawn (or teardown) request arriving while a spawn is in flight **cancels the in-flight operation, awaits
a guaranteed teardown of whatever stack it started, then proceeds with the new request.** Chosen because:
- It's a single user driving one cluster; the human intent behind "spawn again" is almost always *"I changed the
  parameters, give me that one instead,"* not "run two."
- Queueing adds latency and acts on now-stale intent; rejecting is annoying when iterating on worker/memory knobs.
- Correctness (never two overlapping stacks) is guaranteed structurally: `down` is always awaited to completion
  before any `up`. See §2 for the state machine.

**Alternatives considered:** *queue* (stale intent, added latency) and *reject-with-error* (friction while tuning
params) — both rejected for a single-user tool.

### D6 — Delta vs Iceberg (Open Question 2, Phase 4, optional): **Delta Lake**

Added only for the optional US-4.5 topic via `--packages io.delta:delta-spark:<ver>`; more common in interview
contexts (Databricks lineage) and trivial to attach. Explicitly optional and descopable per US-4.5.

**Version-lag risk (resolved).** Delta historically lagged new Spark majors, which was a genuine concern when this
topic was scoped against a 3.5.x base. That risk no longer applies: **Delta Lake 4.1.0 (Feb 2026) fully supports
Spark 4.1.0 while maintaining compatibility with the Spark 4.0.x line**, so it pairs cleanly with our D3 pin
(`4.0.3`). Pick the Delta 4.x release matching the pinned Spark patch in the `--packages` coordinate at the time
the topic is built.

---

## 1. Final architecture + ASCII component diagram

The system is two planes: a **host/WSL2 plane** (the FastAPI app + the browser) and a **Docker plane** (one Spark
Standalone stack at a time, on its own bridge network). The FastAPI app lives *outside* the Docker network, so it
reaches every Spark component through **host-mapped ports**, never container DNS. Executors and the driver reach
each other *inside* the network by container DNS name.

```
                          HOST (Windows) — browser at http://localhost:8000
                                          |
  ============================ WSL2 (Ubuntu, ext4) =============================
  |                                                                            |
  |   FastAPI app  (uvicorn, :8000)                                            |
  |   ├─ web/       Jinja2 + HTMX pages, static assets, chart lib              |
  |   ├─ lifecycle/ render Jinja2 compose  →  docker compose down/up           |
  |   │              →  poll readiness                                         |
  |   ├─ spark_api/ master_client (:8080/json/), app_client (:4040 REST)       |
  |   ├─ annotation/ plan_parser + manifest + engine (self-check, on request)  |
  |   └─ topics/    load content/<topic>/ (manifest + concept.md + notebook)   |
  |        |                          |                    |                   |
  |    docker.sock            host ports 8080/4040     reads /shared volume    |
  |        |                  8888/6066/9092             (explain dumps)        |
  ==========|=========================|====================|===================
            v                         v                    v
  ============================ Docker Desktop engine ==========================
  |                                                                            |
  |   bridge network:  sparkpb-net   (project = sparkpb)                       |
  |                                                                            |
  |   +----------------+     +----------------+  +----------------+            |
  |   | spark-master   |     | spark-worker-1 |  | spark-worker-N |            |
  |   | 7077 cluster   |<----| registers      |  | ...            |            |
  |   | 8080 web/json  |     | 8081 web UI    |  |                |            |
  |   | 6066 REST subm |     +----------------+  +----------------+            |
  |   +----------------+            ^  executors                               |
  |          ^                      |  connect back to driver by DNS           |
  |          | client mode         |  spark-driver:7078 / :7079                |
  |          | spark://spark-master:7077                                       |
  |   +-------------------------------+                                        |
  |   | spark-driver  (JupyterLab)    |   mounts: content/, /shared,           |
  |   |  8888 JupyterLab              |           generated data, playbook/    |
  |   |  4040 app UI + /api/v1 REST   |                                        |
  |   |  7078 driver.port (pinned)    |                                        |
  |   |  7079 blockManager.port       |                                        |
  |   +-------------------------------+                                        |
  |                                                                            |
  |   [streaming topics only]  +----------------+                              |
  |                            | kafka (KRaft)  | 9092                         |
  |                            +----------------+                              |
  ==============================================================================
```

**Docker network topology.** Every stack runs under a fixed compose **project name `sparkpb`**, giving a dedicated
bridge network `sparkpb_default` (referred to here as `sparkpb-net`). Containers get stable service names
(`spark-master`, `spark-worker-1..N`, `spark-driver`, `kafka`) that resolve via Docker's embedded DNS on that
network. This is what makes `spark://spark-master:7077` resolve from the driver, and executor→driver callbacks
resolve to `spark-driver` — no host IPs anywhere inside the cluster.

**Host port map** (only what the host actually needs to reach):

| Host port | Container:port | Purpose | Consumer |
|-----------|----------------|---------|----------|
| 8000 | (WSL2 process) | FastAPI app | Browser |
| 8080 | spark-master:8080 | Master web UI **and** `/json/` readiness endpoint | Browser + `master_client` |
| 6066 | spark-master:6066 | Standalone REST submission server (present, unused — we use client mode) | — |
| 4040 | spark-driver:4040 | Driver application UI **and** the metrics REST API `/api/v1/...` | Browser + `app_client` |
| 8888 | spark-driver:8888 | JupyterLab (embedded in iframe) | Browser |
| 8081 | spark-worker-1:8081 | Worker UI (optional, for deep links) | Browser |
| 9092 | kafka:9092 | Kafka broker (streaming topics only) | Producer + driver |

**Clarification on "the Spark REST API".** There are two distinct HTTP surfaces and the design uses both:
- **Cluster/master JSON** at `http://localhost:8080/json/` — returns the alive-worker count/cores/memory. Used for
  **readiness** (§2), *not* for application metrics.
- **Application metrics REST** at `http://localhost:4040/api/v1/applications[/<id>/stages]` — served by the running
  driver's Spark UI (port 4040, path `/api/v1/...`). This is the source for app-id discovery and stage metrics
  (§3). It is **not** master port 8080; it is the driver's 4040. (Port 6066 is the standalone *submission* REST
  server — a different thing again, and unused here since the driver runs in client mode.)

---

## 2. Cluster lifecycle design

### Template variables (Jinja2 → `docker-compose.yml`)

| Variable | Default | Range (US-1.2) | Effect |
|----------|---------|----------------|--------|
| `worker_count` | 3 | 1–5 | number of `spark-worker-*` services |
| `worker_cores` | 2 | 1–4 | `SPARK_WORKER_CORES` per worker |
| `worker_memory_gb` | 4 | 1–8 | `SPARK_WORKER_MEMORY` per worker |
| `driver_memory_gb` | 2 | fixed | `spark.driver.memory` |
| `shuffle_partitions` | 200 | any positive int | `spark.sql.shuffle.partitions` |
| `aqe_enabled` | false | on/off | `spark.sql.adaptive.enabled` |
| `include_kafka` | false | bool (US-3.1) | conditionally renders the `kafka` service |
| `project_name` | `sparkpb` | fixed | compose `-p` project (single-slot) |

These land in two rendered artifacts: `docker-compose.yml` (services, ports, network, mounts) and a
`spark-defaults.conf` (or env vars) baked into the driver so a plain `SparkSession.builder.getOrCreate()` in a
notebook already has `master`, `shuffle.partitions`, `aqe`, and the driver networking settings (§6) applied — the
learner writes no connection boilerplate (US-0.5).

**Resource ceiling (US-1.2 sanity check, pre-spawn).** Before rendering, the app computes
`master(1GB) + Σ worker_memory_gb + driver(2GB) + (kafka ≈ 2GB if included)` and rejects the configuration if it
exceeds a conservative ceiling (e.g. 48GB, leaving headroom on the 64GB host) with a clear message, *before* any
container starts.

### Up / down / wait-for-ready sequence

Driven by a single lifecycle module. Because of D5 (cancel-and-replace), there is **at most one lifecycle
operation in flight**, guarded by an `asyncio.Lock` plus a cancellable task handle. State machine:

```
   IDLE ──spawn──▶ TEARING_DOWN(old) ──▶ RENDERING ──▶ STARTING ──▶ WAITING_READY ──▶ READY
    ▲                                                                                   │
    └───────────────────────────── teardown ◀──────────────────────────────────────────┘

  New spawn/teardown arriving in any non-IDLE state:
    1. cancel the in-flight task
    2. await guaranteed `docker compose -p sparkpb down --remove-orphans` (idempotent)
    3. begin the new operation from RENDERING
```

Concrete steps for a spawn:
1. **Validate** params + resource ceiling. Reject early on failure.
2. **Render** Jinja2 → write `compose/rendered/docker-compose.yml` (+ `spark-defaults.conf`).
3. **Tear down old:** `docker compose -p sparkpb down --remove-orphans` — **awaited to exit 0** (this is what
   prevents overlap and port/name races; see §6).
4. **Start:** `docker compose -p sparkpb up -d`.
5. **Wait for ready:** poll `http://localhost:8080/json/` every ~2 s until
   `aliveworkers == worker_count` (and master reachable), with a bounded timeout — **60 s** target for the default
   3-worker config (US-0.1), **90 s** hard cap for larger configs (US-1.2). On timeout: report a clear failure,
   leave the (partially-up) stack for inspection, transition back toward IDLE via teardown on next action.
6. **READY:** surface success; the topic page can now load the Jupyter iframe pointed at `localhost:8888` and the
   metrics panel can begin polling `localhost:4040`.

**How the app knows the cluster is healthy:** the `/json/` alive-worker count matching the requested `worker_count`
is the readiness gate (worker registration is the meaningful health signal in Standalone mode — a master with no
workers is up but useless). Container "running" state alone is *not* sufficient and is not used as the gate.

**Implementation mechanism:** shell out to the `docker compose` CLI via `asyncio.create_subprocess_exec`. The
requirements allow either the Docker Python SDK or the CLI; the CLI is chosen because Compose v2 semantics
(project isolation, `--remove-orphans`, dependency ordering) are first-class and battle-tested there, and because
awaiting a subprocess to exit is the natural "down fully completed" barrier we need in step 3.

---

## 3. Execution + annotation design

### App-id discovery

The driver runs in-container in **client mode**, so there is exactly one active Spark application per running
driver. The app discovers its id by calling the driver's application REST API through the host port:

```
GET http://localhost:4040/api/v1/applications
  → [ { "id": "app-2026...-0001", "name": "...", "attempts": [ { "endTime": "1969-...", ... } ] } ]
```

The one entry whose latest attempt has no real `endTime` (still running) is the current app; its `id` is cached
for subsequent stage queries. No coordination with the notebook kernel is required for id discovery — the 4040
REST surface is authoritative.

### Polling stage metrics (US-2.2)

```
GET http://localhost:4040/api/v1/applications/<id>/stages
  → per-stage JSON incl. shuffleReadBytes, shuffleWriteBytes, numTasks,
    memoryBytesSpilled, diskBytesSpilled, task duration summary
```

- **Interval:** the app polls every **6 s** while a job is running (inside the 5–10 s US-2.2 target), via HTMX
  `hx-trigger="every 6s"` on the stage-table fragment; the fragment re-renders server-side from a fresh REST pull.
  Polling stops when the browser leaves the panel and when no application is active.
- Metrics are shown **as returned** by the REST API — never re-derived or estimated by the app (US-2.2).
- **Deep links:** each stage row links to the real Spark UI at
  `http://localhost:4040/stages/stage/?id=<stageId>&attempt=<n>` — the specific stage page, not the app landing
  page.

### Static plan self-check — pull, never push (G3)

The critical G3 constraint: the annotation engine is a **self-check the learner consults after forming a
hypothesis**, never an auto-explainer. The data flow enforces this with two deliberate learner actions and zero
automatic pushes:

```
 In the notebook (driver container):                In the app:
 ───────────────────────────────────                ──────────────
 learner forms hypothesis (markdown cell)
        │
        │  learner *chooses* to call the helper:
        ▼
   playbook.checkpoint(df, topic="join-strategies")
        │  writes df.explain(mode="formatted") text
        │  + current app-id + timestamp
        ▼
   /shared/annotations/<ts>.json  ◀───────────  (nothing read yet)
                                                        │
                              learner *chooses* to click "Reveal self-check"
                                                        ▼
                                        annotation.engine reads the newest dump,
                                        parses nodes, maps via topic manifest,
                                        renders labels + spotlighted metrics
```

- The learner must (a) explicitly call `playbook.checkpoint(...)` and (b) explicitly click **Reveal** — the UI
  shows no annotations until both happen. For capstone "diagnose cold" exercises (US-4.6) the Reveal control is
  the same mechanism; it simply starts hidden and the concept text tells the learner to hypothesize first.
- No narrative "why" text is generated — only mapped labels + evidence (US-2.1), so the learner compares against
  their own read.

`playbook` is a small helper package mounted into the driver container (`driver/playbook/`); `checkpoint()` writes
to a shared volume that FastAPI also mounts read-only. This avoids FastAPI having to execute code in the Jupyter
kernel.

### Plan parser + engine

- **`plan_parser`** tokenizes `explain(mode="formatted")` output into an ordered list of plan-node operators by
  reading each node's operator name (e.g. `Exchange`, `BroadcastExchange`, `BroadcastHashJoin`, `SortMergeJoin`,
  `Window`, `Sort`, `BatchEvalPython`). It keeps tree order but assigns no meaning.
- **`engine`** maps each parsed operator to a concept **using only the topic manifest** (G7). Match precedence is
  **most-specific-first** so `BroadcastExchange` doesn't get swallowed by a generic `Exchange` rule. Any operator
  with no manifest mapping is rendered as **"unknown / unannotated"**, never guessed (US-2.1).
- For runtime metrics, the engine spotlights exactly the `stage_metrics` keys the manifest declares for that topic.

### Per-topic manifest schema

One `manifest.yaml` per topic folder. **No hardcoded line numbers or per-topic code** — annotations are driven by
node-type matches and metric keys declared here (locked constraint / G7).

```yaml
id: join-strategies
title: "Join Strategies: Broadcast vs Sort-Merge vs Shuffle-Hash"
order: 3
content: concept.md
notebook: notebook.ipynb

# cluster this topic wants when spawned from its page
cluster_defaults:
  worker_count: 3
  worker_cores: 2
  worker_memory_gb: 4
  shuffle_partitions: 200
  aqe_enabled: false
requires_kafka: false          # true only for streaming topics (US-3.1)

annotation:
  # plan-node → concept. `match` is tested against the node's operator name,
  # most-specific rule first. No line numbers.
  plan_nodes:
    - match: "BroadcastHashJoin"   ; concept: broadcast-join       ; label: "Broadcast hash join (no shuffle of large side)"
    - match: "BroadcastExchange"   ; concept: broadcast-exchange   ; label: "Broadcast of small side"
    - match: "SortMergeJoin"       ; concept: sort-merge-join      ; label: "Sort-merge join (both sides shuffled+sorted)"
    - match: "ShuffledHashJoin"    ; concept: shuffle-hash-join    ; label: "Shuffle-hash join"
    - match: "Exchange"            ; concept: shuffle-boundary     ; label: "Shuffle boundary (Exchange)"
    # a co-partitioned/bucketed join is 'SortMergeJoin with NO child Exchange' —
    # the bucketing topic's manifest declares that distinction explicitly (US-2.4)

  # stage-metric keys to spotlight from /api/v1/.../stages
  stage_metrics:
    - key: shuffleReadBytes      ; spotlight: true
    - key: shuffleWriteBytes     ; spotlight: true
    - key: numTasks
    - key: memoryBytesSpilled
    - key: diskBytesSpilled
```

(YAML `;`-separated inline form shown for brevity; real files use standard block mapping.)

---

## 4. Repo / folder structure

Phase-0 artifacts (`compose/`, `content/<first-topic>/`, `driver/playbook/`, `tools/datagen/`) are **buildable and
testable standalone**, before any of `app/` exists (US-0.5). The web app is added on top in Phase 1.

```
Spark-Playbook/
  PLAN.md                       # this file
  README.md
  CLAUDE.md
  docs/
    requirements/spark-playbook-mvp.md
    architecture/               # future ADRs, if any
    backlog.md
    retrospectives.md           # (created at sprint close by project-manager)

  compose/                      # ── Phase 0: the cluster harness ──
    Dockerfile.spark            # custom image (D2/D3): apache/spark 4.x + Jupyter + pyarrow + kafka jars + playbook
    build.sh
    templates/
      docker-compose.yml.j2     # master + N workers + driver/Jupyter (+ conditional kafka)
      spark-defaults.conf.j2    # master URL, shuffle.partitions, aqe, driver networking (§6)
    rendered/                   # gitignored: last-rendered compose + conf

  tools/                        # ── standalone utilities (no web app needed) ──
    datagen/                    # US-0.4 synthetic skewed-data generator
      generate.py               # CLI: --rows, --skew, --hot-keys, --out
      skew.py                   # Zipfian / hot-key distribution
      README.md
    kafka_producer/             # US-3.2 (Phase 3): rate-controlled synthetic producer
      produce.py

  driver/                       # ── mounted into the spark-driver container ──
    playbook/                   # helper package importable in notebooks
      __init__.py
      annotate.py               # checkpoint(df, topic=...) → writes explain dump to /shared
      datagen.py                # thin notebook wrappers over tools/datagen
    jupyter_config.py           # token off, CSP relaxed for iframe (§6)

  content/                      # ── curriculum: one folder per topic ──
    partitioning-shuffle/       # Phase 1
      manifest.yaml
      concept.md                # what-it-is / why-it-matters (US-1.1)
      notebook.ipynb
    join-strategies/            # Phase 2
    bucketing/                  # Phase 2
    aqe/                        # Phase 2
    structured-streaming/       # Phase 3  (requires_kafka: true)
    caching/                    # Phase 4
    window-functions/           # Phase 4
    udf-vs-pandas-udf/          # Phase 4
    memory-management-spill/    # Phase 4
    delta/                      # Phase 4 (optional, D6)
    tuning-capstone/            # Phase 4 (US-4.6 diagnose-cold)

  app/                          # ── Phase 1+: FastAPI backend ──
    main.py                     # app factory, routes, static mount
    config.py
    lifecycle/
      renderer.py               # Jinja2 render → rendered/
      manager.py                # state machine, asyncio lock, cancel-and-replace (§2)
      compose_ops.py            # async subprocess: docker compose up/down
      readiness.py              # poll :8080/json/ for alive worker count
    spark_api/
      master_client.py          # :8080/json/
      app_client.py             # :4040 /api/v1/applications, /stages, deep links
    annotation/
      manifest.py               # load + validate topic manifest.yaml
      plan_parser.py            # tokenize explain(mode="formatted")
      engine.py                 # node→concept mapping, metric spotlighting
    topics/
      loader.py                 # read content/<topic>/ (manifest + md + notebook)
    web/
      routes/                   # topic pages, cluster panel, metrics fragments, reveal
      templates/                # Jinja2 + HTMX
      static/                   # htmx.min.js, chart lib (uPlot/Chart.js), css

  scratch/                      # gitignored: generated datasets, /shared, checkpoints
```

---

## 5. Phased roadmap

Phase boundaries match the requirements doc exactly.

### Phase 0 — Cluster harness proven manually (interview-depth-first; nothing here is deferred)

All of the following are **first-class Phase 0 deliverables**, testable with no web app:
- `compose/Dockerfile.spark` + `build.sh` — the custom Spark 4.x image (D2/D3).
- `compose/templates/docker-compose.yml.j2` + `spark-defaults.conf.j2`, rendered by hand for the default
  3-worker/2-core/4GB config; `docker compose up -d` brings up master + 3 workers + driver/Jupyter (US-0.1).
- Observability reachable from host: master UI + `/json/` at `:8080`, driver app UI + `/api/v1/...` REST at
  `:4040` (US-0.2).
- A real shuffle job (groupBy/agg or non-broadcast join) runs from a notebook in client mode against
  `spark://spark-master:7077`, distributed across workers, with nonzero shuffle bytes in the REST stages
  response (US-0.3).
- `tools/datagen/` — synthetic skewed-data generator with tunable rows + skew, verified to force spill and to
  make AQE coalescing observable; fails cleanly on invalid input (US-0.4).
- `driver/playbook/` + JupyterLab — unguided notebook practice against the live cluster, standalone (US-0.5).

Exit criteria: master + 3 workers + driver/Jupyter reachable at `:8080` / `:4040` / REST; a real shuffle runs;
skewed data can be generated on demand; a learner can freely experiment in Jupyter — all without any web UI.

### Phase 1 — Partitioning/shuffle topic end-to-end in the web app

- FastAPI app skeleton (`app/`), Jinja2+HTMX (D4).
- `content/partitioning-shuffle/` topic page: concept (what/why) + link to notebook, content-as-data (US-1.1).
- Cluster control panel: worker count / cores / memory / shuffle-partitions / AQE, spawn with resource-ceiling
  check, cancel-and-replace lifecycle, readiness-gated success/timeout (US-1.2, §2/D5).
- Embedded JupyterLab iframe pointed at the current stack's driver; reconnects correctly after respawn (US-1.3).

### Phase 2 — Annotation engine (self-check) + join strategies + bucketing + AQE

- `annotation/` engine: manifest-driven plan-node → concept mapping, on-request Reveal, unknown-node handling
  (US-2.1); pull-not-push flow per G3/§3.
- Runtime-metrics self-check: 6 s polling of `:4040` stages, deep links into the real Spark UI (US-2.2).
- Topics: `join-strategies` (US-2.3), `bucketing` — including the co-partitioned-no-shuffle vs still-shuffles
  contrast (US-2.4), `aqe` — skew-split + coalescing, AQE on/off comparison (US-2.5).

### Phase 3 — Streaming + Kafka

- Conditional `kafka` (KRaft, no ZooKeeper) service in the compose template, included only when
  `requires_kafka: true`, within the resource ceiling (US-3.1).
- `tools/kafka_producer/` — rate-controlled synthetic producer; checkpoint recovery genuinely works (US-3.2).
- `content/structured-streaming/` topic: watermarks, stateful aggregation, checkpoint recovery, plus a live
  progress chart sourced from `query.lastProgress`/`recentProgress` (US-3.3).

### Phase 4 — Remaining curriculum

- `caching` (US-4.1), `window-functions` (US-4.2), `udf-vs-pandas-udf` (US-4.3),
  `memory-management-spill` — deliberate spill + controlled OOM, learner diagnoses before any hint (US-4.4).
- `delta` — optional table-format topic (US-4.5, D6); descopable.
- `tuning-capstone` — "diagnose cold" exercises with the annotation views hidden behind a deliberate Reveal;
  at least one exercise each for shuffle-misconfig, join-strategy misdiagnosis, skew, and memory/spill-or-OOM,
  all sourced from realistic `datagen` data (US-4.6, G8).

---

## 6. Named risks + mitigations

### R1 — Client-mode driver networking / callback addresses

**Risk:** In client mode, executors must connect *back* to the driver. The classic failure is a driver on the
host behind Docker NAT that advertises an unreachable address, so executors hang and the job never starts.

**Why this design avoids it:** the driver runs **inside a container on the same `sparkpb-net` bridge** as the
master and workers, with a resolvable DNS name (`spark-driver`). Executors resolve and reach it directly — no host
NAT boundary is crossed. The required settings (baked into `spark-defaults.conf.j2`):
- `spark.driver.host = spark-driver` — advertise the container's DNS name (what executors dial), *not* a host IP.
- `spark.driver.bindAddress = 0.0.0.0` — bind inside the container to all interfaces.
- `spark.driver.port = 7078` and `spark.blockManager.port = 7079` — **pinned** so the ports are deterministic
  and stable across sessions (rather than random ephemeral ports).

**Noticed by:** if misconfigured, jobs stall in "waiting for executors" / tasks never launch; the master UI shows
workers but no running application making progress.

### R2 — Port collision when a second Spark app bumps 4040 → 4041

**Risk:** Spark increments the UI port (4040→4041→…) when 4040 is taken. If that happened, the app would poll
`:4040` for the wrong (or a dead) application.

**Why it's largely avoided:** by design there is **one stack, one driver, one application at a time**
(cancel-and-replace, D5), and teardown is awaited before spawn, so 4040 is free when the new driver binds it.
The one way to hit 4041 is a learner creating multiple `SparkSession`s in one notebook without stopping the
first. Mitigations:
- `spark.ui.port = 4040` pinned, and the `playbook`/`spark-defaults` convention is a single
  `SparkSession.builder.getOrCreate()` reused across cells.
- The concept text instructs `spark.stop()` before recreating a session.
- If the app ever finds no live application at `:4040`, it surfaces a clear "no active application on 4040"
  message rather than silently polling a stale port. (We deliberately do not chase 4041+ — that would mask the
  real cause.)

**Noticed by:** the metrics panel shows "no active application" while a job is visibly running in Jupyter →
signals a stray extra session.

### R3 — iframe / CSP for embedded JupyterLab

**Risk:** JupyterLab defaults to `X-Frame-Options: SAMEORIGIN` and a CSP `frame-ancestors 'self'`, which blocks
embedding it in the FastAPI page when they are on different origins (`localhost:8000` vs `localhost:8888`), so the
iframe renders blank.

**Mitigation (primary, chosen for simplicity — no auth/security is required, per constraints):** configure the
Jupyter server (`driver/jupyter_config.py`) to permit framing from the app origin:
- `ServerApp.tornado_settings = {'headers': {'Content-Security-Policy': "frame-ancestors 'self' http://localhost:8000"}}`
- disable `X-Frame-Options` (Jupyter emits none when CSP `frame-ancestors` is set as above),
- `ServerApp.allow_origin = 'http://localhost:8000'` (or `*` locally), `ServerApp.token = ''`,
  `ServerApp.disable_check_xsrf = True` — acceptable because there is nothing to protect on a single-user
  localhost tool.

**Alternative (more robust, heavier):** reverse-proxy JupyterLab under the FastAPI origin (`/jupyter/*` →
`spark-driver:8888`), making it same-origin and sidestepping CSP entirely. This requires WebSocket proxying for
kernel traffic, so it is deferred unless the CSP relaxation proves flaky.

**Noticed by:** blank iframe + a browser-console CSP/`X-Frame-Options` refusal message.

### R4 — Cluster teardown races (old containers not fully down before new ones bind ports/names)

**Risk:** Starting a new stack while the previous one is still releasing ports (8080/4040/7077) or names causes
bind failures or a partially-overlapping stack.

**Mitigations:**
- **Fixed project name `sparkpb`** for the single active slot, so `docker compose -p sparkpb down --remove-orphans`
  targets exactly the prior stack's containers, network, and (optionally) volumes.
- **`down` is awaited to exit 0 before `up`** (§2 step 3) — the subprocess-completion barrier guarantees prior
  resources are released first. This is the structural guarantee behind D5's "never two overlapping stacks."
- A short **port-free pre-check** (8080/4040/7077 not bound) with brief backoff before `up`, to absorb the small
  window where Docker is still tearing down.
- `--remove-orphans` cleans up any leftover service (e.g. a Kafka container from a prior streaming stack that the
  current non-streaming stack no longer declares).

**Noticed by:** `up` fails with "port is already allocated" or "network has active endpoints" → indicates the
await barrier or pre-check was bypassed.

### R5 (bonus) — Resource exhaustion / host thrash

**Risk:** A large worker/memory config plus Kafka could exceed comfortable host RAM.

**Mitigation:** the pre-spawn resource-ceiling check (§2) rejects configs above ~48GB requested before any
container starts, keeping headroom on the 64GB host (US-0.1, US-1.2). **Noticed by:** a clear pre-spawn rejection
message rather than a mid-spawn OOM.

### R6 — Spark 4.0 ANSI SQL mode changes example behavior vs 3.x references

**Risk:** Spark 4.0+ ships with `spark.sql.ansi.enabled=true` by default (a change from 3.x). Operations that
silently returned `null` in 3.x — integer overflow, invalid casts, division edge cases, out-of-range access —
now **throw** at runtime. Exercises or notebooks copied from 3.x-era tutorials/references may therefore error or
produce different output than their source material, which could read as a bug to whoever authors Phase 0/1/2
content.

**Decision (D3):** **embrace ANSI mode, do not override it.** Per G1 (interview-depth-first), stricter ANSI
semantics are a teaching feature — ANSI behavior is itself a rising interview topic — not something to suppress
with a global config flag. Exercise authors should expect and, where relevant, *teach* the difference (e.g. an
overflow-throws demo) rather than pinning `spark.sql.ansi.enabled=false` to make old examples pass unchanged.

**Noticed by:** a notebook that "worked in the tutorial" raising `ArithmeticException` / `SparkNumberFormatException`
/ cast errors on 4.x → expected, document the ANSI behavior rather than disabling the flag. If a *specific*
exercise has a defensible pedagogical reason to demonstrate legacy (silent-null) behavior, set the flag locally
in that one notebook and call out that it is deliberately reverting a 4.0 default — never flip it cluster-wide in
`spark-defaults.conf.j2`.

---

## Open questions remaining

- **Capstone hypothesis recording (Open Question 3):** the design assumes the informal markdown-cell approach
  consistent with the no-grading non-goal — no formal answer-submission system. No architectural blocker; flagged
  only so the user can override if they later want structure.
- Everything else the requirements doc left to the architect (D1–D6 above) is now decided.
