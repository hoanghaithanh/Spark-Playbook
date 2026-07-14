# Spark Playbook — MVP Requirements

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-13

## Problem statement

The user needs deep, hands-on PySpark experience at a mid/advanced level — specifically the kind of experience that holds up under Spark interview questions about shuffle, joins, AQE, memory management, and streaming — and generic tutorials/toy datasets don't produce the real cluster behavior (spill, skew, broadcast decisions, stage-level shuffle costs) that those questions probe. Spark Playbook is a local, single-user, self-hosted web app that lets the user spin up a real Spark Standalone cluster on demand, run notebooks against it, generate data large/skewed enough to trigger real Spark behavior, and use a curriculum plus a self-check annotation engine to verify their own understanding of what a plan or a stage's metrics are telling them.

This is a personal learning tool, not a product. It runs entirely on the user's own machine (Windows/WSL2 or Linux, 64GB RAM, i7 11th gen, Docker + Docker Compose available), with no other users, no network exposure, and no deployment target beyond `localhost`.

## Goals / Non-goals

### Goals

- **G1 — Interview-depth over platform polish (load-bearing, applies to every phase).** The primary success criterion for this project is that the user can explain, reproduce, and diagnose real Spark behavior (shuffle, joins, AQE, memory/spill, streaming semantics) well enough to answer interview-style questions — not that the web app is polished or feature-complete. Where a choice exists between spending effort on UI/platform sophistication versus on curriculum depth or realistic cluster/data behavior, curriculum depth and cluster realism win. Concretely, this means:
  - Raw, unguided cluster access must never be gated behind unfinished app features (see G2).
  - The annotation engine is a *self-check* consulted after the learner forms a hypothesis, not an explainer that removes the need to form one (see G3, Phase 2 acceptance criteria).
  - Every curriculum topic must be backed by data/parameters realistic enough to actually exhibit the behavior being taught (see G4).
- **G2 — Unguided practice is available as soon as the bare cluster exists.** A learner must be able to open Jupyter and run arbitrary PySpark code against a real master+workers+driver cluster before any curriculum content, annotation engine, or web UI beyond basic cluster controls exists. This is a Phase 0 acceptance criterion, not deferred to a later phase.
- **G3 — Annotation engine is a self-check tool.** Its UX and acceptance criteria are written around "confirm or correct a hypothesis the learner already formed," not "explain the plan/metrics to the learner from scratch."
- **G4 — Synthetic data generation with tunable skew/volume is first-class.** A utility (script or in-app feature) must be able to produce datasets large and skewed enough to force real shuffle spill, real AQE partition coalescing, and real broadcast-vs-shuffle-join threshold decisions on the resource budget below. This exists starting Phase 0.
- **G5 — On-demand, parameterized cluster lifecycle.** The learner can create and tear down a Spark Standalone cluster with chosen worker count, cores/memory per worker, `spark.sql.shuffle.partitions`, and AQE on/off, without hand-editing compose files once Phase 1 UI exists (Phase 0 does this via a manually-run template/script).
- **G6 — Guided curriculum, one topic at a time.** Each topic ships with what-it-is / why-it-matters content and a runnable notebook, built incrementally per the phase roadmap below, culminating in the full ladder (partitioning/shuffle → Catalyst plans → join strategies → bucketing → caching → AQE → window functions → UDF/pandas UDF → memory management/spill → streaming/Kafka → Delta/Iceberg (optional) → tuning/debugging capstone).
- **G7 — Plan- and metrics-based annotation, mapped from data not hardcoded.** Static plan analysis (from `df.explain(mode="formatted")`) and runtime metrics (from the Spark REST API) drive annotations via each topic's manifest of plan-node-types / stage-metrics of interest — not hardcoded line numbers or one-off logic per topic.
- **G8 — "Diagnose cold" exercises exist as a distinct exercise type.** At least the tuning/debugging capstone (Phase 4) includes exercises that present a slow/broken job with no annotation-engine hints available up front, forcing the learner to hypothesize before consulting any automated help — mirroring how interview debugging questions are actually posed.
- **G9 — Streaming topics conditionally include Kafka.** Kafka (KRaft mode, no ZooKeeper) plus a small producer script are added to the compose template only for streaming topics, with live charting of `query.lastProgress`/`recentProgress`.

### Non-goals

- **No auth or access control** of any kind — single local user, nothing to protect against.
- **No multi-tenancy** — one learner, one cluster at a time.
- **No cloud or remote deployment** — `localhost` / local Docker only, no cloud provider integration, no remote access, no TLS.
- **No production hardening** — no rate limiting, no input sanitization beyond what's needed to avoid the app crashing on obviously malformed input, no secrets management, no audit logging.
- **No custom code editor.** Embedded JupyterLab via iframe is a locked-in decision for MVP; a custom Monaco-based editor is an explicitly deferred future upgrade, not in scope.
- **No Kubernetes.** Spark Standalone mode (master + N workers) is a locked-in decision; Kubernetes/YARN deployment modes are out of scope.
- **No local (non-cluster) execution mode.** Every exercise runs against a real multi-container Standalone cluster; there is no "quick local mode" fallback to build or maintain.
- **No grading, accounts, or progress-tracking system.** The app does not track completion, scores, or spaced repetition — that's out of scope for MVP.
- **No mobile/responsive design requirement** — desktop browser on the same machine only.

## Resource budget (constraint on every cluster-related acceptance criterion)

- Master: 1 core / 1GB
- Default: 3 workers × 2 cores / 4GB each
- Driver: 2GB
- A single worker may be scaled up to 8GB (for skew/spill demos)
- Total footprint must fit comfortably within 64GB RAM on the host in all supported configurations described in this document.

## Curriculum ladder (target order, referenced by phase below)

1. Partitioning & shuffle mechanics
2. Catalyst plans & `.explain`
3. Join strategies (broadcast vs sort-merge vs shuffle-hash)
4. Bucketing (co-partitioned joins)
5. Caching/persistence
6. AQE (skew join, partition coalescing, plan-changes-at-runtime)
7. Window functions
8. UDF vs pandas UDF serialization cost
9. Memory management & spill (unified memory manager, execution vs storage memory, off-heap, OOM diagnosis)
10. Structured Streaming + Kafka (watermarks, stateful aggregation, checkpoint recovery)
11. Delta/Iceberg (optional)
12. Tuning/debugging capstone (including "diagnose cold" exercises)

---

## User stories and acceptance criteria, by phase

### Phase 0 — Cluster harness proven manually

**US-0.1 — Spin up and tear down a cluster manually.**
As a learner, I want to generate a docker-compose stack for a Spark Standalone cluster (master + 3 workers + a driver/Jupyter container) from a template with chosen parameters, and bring it up/down via `docker compose`, so that I have a working, disposable cluster to practice against.

- *Given* a Jinja2-templated compose file and a chosen worker count (default 3), cores/memory per worker (default 2 cores/4GB), *when* I render the template and run `docker compose up -d`, *then* all containers reach a running state and the Spark master UI at `http://localhost:8080` lists exactly the expected number of registered workers within 60 seconds.
- *Given* a running cluster, *when* I run `docker compose down`, *then* all containers for that stack stop and are removed, and rendering + starting a new stack with different parameters succeeds without leftover state from the previous one (e.g., port or network name collisions).
- *Given* the resource budget above, *when* I start the default 3-worker configuration, *then* total container memory requested does not exceed the budget and the host remains responsive (no OOM/thrashing) with 64GB RAM available.

**US-0.2 — Reach cluster observability endpoints.**
As a learner, I want the Spark master UI, an application's Spark UI, and the Spark REST API reachable from the host browser/HTTP client, so I can inspect cluster and job state directly.

- *Given* a running cluster, *when* I open `http://localhost:8080`, *then* I see the master UI showing worker count, cores, and memory matching what I configured.
- *Given* a running Spark application (e.g., a notebook job), *when* I open the driver's application UI (`http://localhost:4040` or the driver's mapped port), *then* I see the Jobs/Stages/SQL tabs for that application.
- *Given* a running application, *when* I query the Spark REST API (`/api/v1/applications/<id>/stages`), *then* I receive stage-level JSON including `shuffleReadBytes`, `shuffleWriteBytes`, and `numTasks` for stages that performed a shuffle.

**US-0.3 — Run a real shuffle job end-to-end.**
As a learner, I want to run a PySpark job that performs a shuffle (e.g., a `groupBy` or join requiring repartitioning) against the cluster, so I can confirm the harness produces real distributed execution, not local-mode behavior.

- *Given* a running cluster and a notebook connected as the driver in client mode (`spark://spark-master:7077`), *when* I run a job with a `groupBy().agg()` or a non-broadcast join on data spread across workers, *then* the job completes successfully, the Spark UI shows tasks distributed across more than one worker/executor, and at least one stage in the REST API response reports non-zero `shuffleReadBytes`/`shuffleWriteBytes`.

**US-0.4 — Generate synthetic datasets with tunable skew and volume.**
As a learner, I want a data-generation utility that produces datasets with configurable size and configurable key skew, so that exercises can force real shuffle spill, real AQE coalescing, and real broadcast-vs-shuffle-join decisions instead of running on toy data.

- *Given* the utility, *when* I request a dataset with a specified row count and a skew parameter (e.g., a Zipfian or configurable-hot-key distribution) for a join/group-by key, *then* the generated data's key-frequency distribution measurably matches the requested skew (e.g., top-N keys account for the requested share of rows, verifiable by a `groupBy(key).count()` check).
- *Given* the default 3-worker/2-core/4GB budget, *when* I generate a dataset sized for the spill exercise and run a memory-constrained aggregation/join against it, *then* the job produces observable spill (nonzero spill metrics in the Spark UI/REST API `stages` response) rather than completing entirely in memory.
- *Given* a dataset generated with default (low/no) skew and default AQE settings, *when* I run a `groupBy` after an upstream filter that shrinks partition sizes, *then* AQE's partition-coalescing is observable (fewer post-shuffle partitions than `spark.sql.shuffle.partitions` was set to, visible in the SQL plan/REST API).
- *Given* a dataset generation request, *when* the requested size or skew is invalid (e.g., negative row count), *then* the utility fails with a clear error rather than silently producing an empty or malformed dataset.

**US-0.5 — Unguided notebook practice against the live cluster.**
As a learner, I want to open JupyterLab connected to the driver container and run arbitrary PySpark code against the running cluster, independent of any curriculum content or annotation engine, so I can start practicing immediately once the harness exists.

- *Given* a running cluster with a driver/Jupyter container on the same Docker network as the master, *when* I open JupyterLab in a browser, *then* I can create a `SparkSession` using `spark://spark-master:7077` in client mode without additional host configuration, and run and re-run cells against the live cluster.
- *Given* this capability, *when* it is available, *then* it does not depend on the curriculum browser, the annotation engine, or any Phase 1+ web app feature being built yet — it is testable standalone with only Phase 0 artifacts (compose template, driver/Jupyter container, generated data).

---

### Phase 1 — One topic (partitioning/shuffle) end-to-end in the web app

**US-1.1 — Browse the partitioning/shuffle topic page.**
As a learner, I want a web page for the "Partitioning & shuffle mechanics" topic showing what it is, why/when it matters, and a link to its runnable notebook, so I have a structured entry point instead of working from raw files.

- *Given* the app is running, *when* I navigate to the partitioning/shuffle topic, *then* I see its concept description (what/why) and a control to open its notebook.
- *Given* the topic's content is stored as Markdown + notebook JSON in a per-topic folder, *when* the content is edited, *then* the change is reflected on next page load without a code change (content is data, not hardcoded into app logic).

**US-1.2 — Configure and spawn a cluster from the UI.**
As a learner, I want to set worker count, cores/memory per worker, `spark.sql.shuffle.partitions`, and AQE on/off in the web UI and spawn a cluster matching those parameters, so I don't have to hand-edit compose files or the CLI for routine practice.

- *Given* the cluster control panel, *when* I set parameters within supported ranges (workers 1–5, cores 1–4, memory 1–8GB per worker, shuffle partitions any positive integer, AQE on/off) and click "Spawn," *then* the app renders the compose template with those values, tears down any previous stack, brings up the new one, and reports success only once the master reports the expected worker count (or reports a clear failure/timeout after a bounded wait, e.g., 90 seconds).
- *Given* an in-progress spawn, *when* I request another spawn or a teardown before it completes, *then* the app either queues/rejects the concurrent request with a clear message or safely cancels the first — it must not leave two overlapping stacks running or an inconsistent compose state.
- *Given* a chosen configuration, *when* total requested resources would exceed a safe bound (e.g., an explicit sanity ceiling below 64GB, leaving headroom for the host), *then* the UI rejects the configuration before spawning with a clear message, rather than attempting it and failing mid-spawn.

**US-1.3 — Run the topic notebook against the spawned cluster via embedded Jupyter.**
As a learner, I want the topic's notebook to open in an embedded JupyterLab iframe already connected to the cluster I just spawned, so I can move from reading the concept to running it without manual setup.

- *Given* a spawned cluster and the partitioning/shuffle topic page, *when* I open its notebook, *then* it loads inside an embedded JupyterLab iframe pointed at the driver container for the current stack, and running its cells executes against that cluster (verifiable via the Spark UI showing the job).
- *Given* I tear down and respawn a cluster, *when* I reopen the topic notebook, *then* it connects to the newly spawned cluster (not a stale reference to the torn-down one).

---

### Phase 2 — Annotation engine (self-check) + join strategies + bucketing + AQE

**US-2.1 — Self-check a plan hypothesis via static plan analysis.**
As a learner, I want to submit/select a `df.explain(mode="formatted")` plan (or trigger it for the notebook's current query) and see it annotated with concept labels for nodes I recognize (or don't), so I can confirm or correct my own read of the plan after I've formed a hypothesis, without the app pre-explaining it to me.

- *Given* a plan containing an `Exchange` node, *when* the plan is analyzed, *then* it is labeled as a shuffle boundary; given a `BroadcastExchange`/`BroadcastHashJoin`, labeled broadcast join; given `SortMergeJoin`, labeled shuffle (sort-merge) join; given `Window`, labeled shuffle+sort — using the topic manifest's declared node-type-to-concept mapping, not hardcoded per-topic logic.
- *Given* the annotation UI, *when* it displays results, *then* it does not proactively explain *why* the plan looks that way (e.g., no auto-generated narrative walkthrough) — it surfaces the mapped labels/evidence so the learner can compare them against their own hypothesis, consistent with G3.
- *Given* a plan node type with no mapping declared in the current topic's manifest, *when* analyzed, *then* it is shown as unannotated/unknown rather than guessed at.

**US-2.2 — Self-check a shuffle hypothesis via runtime metrics.**
As a learner, I want the app to poll the Spark REST API for a running/completed application's stage metrics and let me look up which stage produced the shuffle I predicted, with a deep link into the real Spark UI for that stage, so I can verify my hypothesis against ground truth.

- *Given* a completed application with at least one shuffle stage, *when* I view its stage list in the app, *then* each stage shows `shuffleReadBytes`, `shuffleWriteBytes`, `numTasks`, and per-task duration summary, sourced from the REST API (not re-derived/estimated by the app).
- *Given* a stage entry in the app, *when* I click it, *then* I am deep-linked to that specific stage in the real Spark UI (not just the application's landing page).
- *Given* an application still running, *when* I view its stages, *then* metrics reflect current progress (polled, not a one-time snapshot) — polling interval is left to the architect, but staleness must not exceed a human-noticeable delay during active debugging (target: refresh at least every 5–10 seconds while a job is running).

**US-2.3 — Join strategies topic.**
As a learner, I want a topic covering broadcast join, sort-merge join, and shuffle-hash join with runnable examples that force each strategy, so I can recognize each in a plan and know when Spark picks which.

- *Given* the topic's example notebooks, *when* run against data sized below/above `spark.sql.autoBroadcastJoinThreshold`, *then* the resulting plan shows a broadcast join in the small-data case and a sort-merge (or shuffle-hash, where forced via config) join in the large-data case — verifiable by both the annotated plan (US-2.1) and manual `.explain()` reading.
- *Given* the topic manifest, *when* the annotation engine analyzes its notebook's plans, *then* it correctly labels each of the three join strategies per the mappings in US-2.1.

**US-2.4 — Bucketing (co-partitioned joins) topic.**
As a learner, I want a dedicated topic on bucketing that demonstrates how pre-bucketed, co-partitioned tables avoid a shuffle at join time, so I understand this as a distinct optimization from broadcast/sort-merge joins (a common interview topic on its own).

- *Given* two tables written with `bucketBy` on the same key and bucket count, *when* joined on that key, *then* the resulting plan shows no `Exchange` (shuffle) node for that join — verifiable via `.explain()` and the annotation engine.
- *Given* two tables with mismatched bucket counts or a join key that doesn't match the bucketing column, *when* joined, *then* the plan shows a shuffle occurring anyway, and the topic notebook includes this as a contrasting example (not just the success case).
- *Given* the topic's manifest, *when* the annotation engine analyzes these plans, *then* it distinguishes "co-partitioned join, no shuffle" from a standard sort-merge join rather than collapsing both into one label.

**US-2.5 — AQE topic.**
As a learner, I want a topic demonstrating Adaptive Query Execution — skew join handling, partition coalescing, and plan changes decided at runtime — with before/after comparisons (AQE off vs on), so I can recognize AQE's effect on a plan and on runtime metrics.

- *Given* a skewed dataset (from US-0.4) and AQE enabled, *when* a join on the skewed key runs, *then* the executed plan/metrics show Spark splitting the skewed partition (observable via the SQL tab's "Number of skewed partitions" or equivalent AQE-specific metric), compared against the same job with AQE disabled showing no such split and a materially longer runtime for the skewed task(s).
- *Given* AQE-coalescing behavior (also exercised in US-0.4), *when* the topic notebook runs, *then* the learner can compare the initial shuffle-partition plan against the final/executed plan and see the coalesced partition count, with the annotation engine labeling the relevant nodes.
- *Given* the AQE on/off cluster parameter (US-1.2), *when* toggled, *then* the topic's exercises are runnable in both states without additional manual configuration.

---

### Phase 3 — Streaming + Kafka

**US-3.1 — Kafka available conditionally for streaming topics.**
As a learner, I want Kafka (KRaft mode, no ZooKeeper) added to the compose stack only when a streaming topic's cluster is spawned, so non-streaming exercises stay lightweight and streaming exercises have a real broker to work against.

- *Given* a non-streaming topic, *when* I spawn its cluster, *then* no Kafka container is included in the stack.
- *Given* the Structured Streaming topic, *when* I spawn its cluster, *then* the stack includes a KRaft-mode Kafka broker (no ZooKeeper container) reachable from the driver container, and stays within the resource budget (Kafka's footprint counted against the same ceiling as US-1.2).

**US-3.2 — Synthetic streaming producer.**
As a learner, I want a small producer script that publishes synthetic events to Kafka at a controllable rate, so I can exercise streaming aggregation, watermarking, and checkpoint recovery against realistic, ongoing data.

- *Given* the producer script, *when* started with a target rate (events/sec) and topic name, *then* it publishes to that Kafka topic at approximately the requested rate until stopped.
- *Given* a running producer, *when* the streaming job is stopped and restarted pointed at the same checkpoint location, *then* it resumes without reprocessing or dropping events beyond what the checkpoint/watermark semantics being taught would predict (i.e., checkpoint recovery genuinely works, not just "the job restarts").

**US-3.3 — Structured Streaming topic.**
As a learner, I want a topic covering watermarks, stateful aggregation, and checkpoint recovery with a runnable streaming notebook, plus a live chart of query progress, so I can see streaming-specific behavior (late data handling, state growth, recovery) in real time.

- *Given* a running streaming query, *when* I view its progress in the app, *then* I see a live-updating chart sourced from `query.lastProgress`/`recentProgress` (e.g., input rate, processing rate, batch duration) that updates at least every few seconds while the query runs.
- *Given* late-arriving synthetic events (beyond the configured watermark), *when* the streaming aggregation runs, *then* the topic notebook demonstrates and the learner can observe (via output or state metrics) that late data past the watermark is dropped from the aggregate, while data within the watermark window is included.
- *Given* a checkpoint directory, *when* the streaming job is killed and restarted against the same checkpoint, *then* it resumes processing from the correct offset per Kafka + Structured Streaming checkpoint semantics (per US-3.2).

---

### Phase 4 — Remaining curriculum

**US-4.1 — Caching/persistence topic.**
As a learner, I want a topic on `.cache()`/`.persist()` with different storage levels, showing the effect on repeated-access performance and on the storage tab of the Spark UI, so I understand when caching helps versus wastes memory.

- *Given* a job that reuses a DataFrame multiple times, *when* run without caching versus with `.cache()`, *then* the cached run shows fewer recomputed stages (visible in the Spark UI DAG) and the Storage tab shows the cached RDD/DataFrame with its storage level and size.
- *Given* different storage levels (e.g., `MEMORY_ONLY` vs `MEMORY_AND_DISK`), *when* data exceeds available cache memory, *then* the topic notebook demonstrates the eviction/spill-to-disk behavior differing between levels.

**US-4.2 — Window functions topic.**
As a learner, I want a topic on window functions (ranking, running aggregates, lead/lag) that shows the shuffle+sort cost associated with `Window` plan nodes, so I connect the earlier partitioning/shuffle concept to a concrete, commonly-asked feature.

- *Given* a window function query, *when* `.explain()` is run, *then* the plan shows a `Window` node preceded by a sort/exchange as appropriate, and the annotation engine labels it per the US-2.1 mapping.
- *Given* a window query partitioned on a skewed key, *when* run, *then* the topic notebook demonstrates the performance impact and connects it back to the skew/AQE topic (US-2.5) rather than treating it as unrelated.

**US-4.3 — UDF vs pandas UDF topic.**
As a learner, I want a topic comparing regular (row-at-a-time, serialized) UDFs against pandas UDFs (vectorized, Arrow-based), with measurable timing/serialization differences, so I can explain the performance gap in an interview.

- *Given* the same transformation implemented as a standard UDF and as a pandas UDF, *when* both run against an identically-sized dataset, *then* the topic notebook captures and displays a measurable timing difference (e.g., stage duration from the Spark UI/REST API) attributable to serialization overhead.
- *Given* the two implementations, *when* their plans are inspected, *then* the notebook or annotation output distinguishes `BatchEvalPython`/vectorized execution from row-at-a-time Python UDF execution.

**US-4.4 — Memory management & spill topic.**
As a learner, I want a dedicated topic on Spark's unified memory manager — execution vs storage memory, off-heap memory, and diagnosing OOM/spill — with exercises that deliberately trigger spill and (in a controlled way) executor OOM, so I can reason about memory tuning in an interview (this is a distinct topic from joins/AQE, not a sub-bullet of either).

- *Given* a worker scaled up to the 8GB skew/spill configuration and a dataset sized via US-0.4 to exceed available execution memory for a given operation (e.g., a large sort or aggregation), *when* the job runs, *then* spill metrics (memory spill / disk spill bytes) are nonzero and visible in the Spark UI/REST API, and the topic notebook has the learner locate and interpret them before any annotation-engine hint is shown.
- *Given* a deliberately under-provisioned executor and an oversized in-memory operation (e.g., a broadcast forced past a safe threshold, or a large collect), *when* run, *then* the job fails with an OOM (executor or driver) and the topic notebook walks through reading the resulting error/Spark UI evidence to diagnose *which* memory region was exhausted — without the app pre-diagnosing it for the learner.
- *Given* the unified memory manager's execution/storage boundary, *when* the topic notebook runs a caching operation concurrently with a memory-intensive shuffle, *then* it demonstrates storage memory being evicted to make room for execution memory (or vice versa, per Spark's actual eviction policy), observable via the Storage tab before/after.

**US-4.5 — Delta/Iceberg topic (optional).**
As a learner, I want an optional topic introducing a table format (Delta Lake or Iceberg) covering ACID writes, time travel, and schema evolution on top of the same cluster, so I have baseline exposure even though it's lower priority than the core Spark-engine topics.

- *Given* the compose stack with the chosen table format's dependency added, *when* I write a table using it and then perform an update/merge, *then* the topic notebook demonstrates time-travel query (reading a prior version) and confirms ACID behavior (e.g., no partial/corrupt state after a concurrent or interrupted write, within what's feasible to demonstrate locally).
- This topic may be descoped or deprioritized relative to US-4.1–4.4 and Phase 0–3 without affecting the MVP's core goal (G1); it is explicitly marked optional in the backlog.

**US-4.6 — Tuning/debugging capstone with "diagnose cold" exercises.**
As a learner, I want a capstone set of exercises that present a slow or broken Spark job with no annotation-engine hints available up front, so I practice the interview-realistic skill of diagnosing a problem before consulting any tool.

- *Given* a capstone exercise, *when* I open it, *then* the annotation engine's plan/metrics views are not shown or are explicitly hidden behind a "reveal" action I must deliberately trigger — they are never displayed automatically alongside the problem.
- *Given* I've formed a hypothesis (recorded informally, e.g., in a notebook markdown cell — no formal answer-submission system required for MVP) and then trigger the reveal, *then* the annotation engine's static-plan and runtime-metrics views (US-2.1, US-2.2) become available to check my hypothesis against.
- *Given* the capstone set, *when* assembled, *then* it includes at least one exercise per major category already covered: a shuffle/partitioning misconfiguration, a join-strategy misdiagnosis (e.g., a join that should broadcast but doesn't), a skew problem, and a memory/spill or OOM problem — each sourced from realistic data via US-0.4, not contrived one-line bugs.

---

## Open questions

The user has indicated most of this is decision-complete; this list is deliberately short and limited to items that are genuinely blocking or ambiguous rather than manufactured for completeness.

1. **Concurrent-spawn behavior (US-1.2).** The requirement states the app must not leave two overlapping stacks running, but whether a second spawn request while one is in-flight should be *queued*, *rejected with an error*, or *cancel-and-replace* the first is left as an implementation choice for the architect — flagging in case the user has a preference (e.g., "always cancel-and-replace" is simplest for a single-user tool).
2. **Delta vs Iceberg choice (US-4.5).** The topic is explicitly optional and either format satisfies the learning goal; no decision is needed before Phase 0–3 work, but the architect or user should pick one before Phase 4 design to avoid building both.
3. **Capstone "hypothesis recording" (US-4.6).** The acceptance criteria assume an informal method (e.g., a markdown cell) rather than a formal answer-submission/scoring system, consistent with the no-grading non-goal. Flagging this assumption explicitly in case the user wants something more structured — otherwise no action needed.

No other open questions are being raised; the phased scope, resource budget, locked technology decisions (JupyterLab iframe, Standalone mode, driver-in-container, FastAPI backend, Jinja2-templated compose), and curriculum ladder (including bucketing and memory-management as standalone topics) are treated as settled inputs to the architect's design.

## Constraints

- **Platform:** Windows with WSL2, or Linux; Docker + Docker Compose required. No support requirement for macOS, though nothing here should deliberately break it.
- **Hardware:** 64GB RAM, Intel i7 11th gen (8 cores/16 threads) — all default and scaled-up configurations must fit comfortably within this, per the resource budget above.
- **Locked technology decisions (not open for architect reconsideration):**
  - Embedded JupyterLab in an iframe, not a custom editor.
  - Spark Standalone mode (master + N workers), not local mode or Kubernetes.
  - Driver runs in a container on the cluster's Docker network in client mode, not on the host.
  - Backend: FastAPI. Frontend framework choice (plain HTML+HTMX vs. light Next.js) is deferred to the architect.
  - Cluster lifecycle driven by a Jinja2-templated `docker-compose.yml`, applied via the Docker Python SDK or by shelling out to `docker compose`.
  - Content stored as Markdown + notebook JSON, one folder per topic, each with a manifest declaring plan-node-types/stage-metrics of interest (no hardcoded per-topic annotation logic).
- **No auth/security hardening** is required or expected anywhere in this system (see Non-goals) — do not add it as a hidden requirement in later design/implementation phases.
- **Single-machine, single-user assumption** applies to every acceptance criterion above (e.g., "concurrent spawn" in Open Question 1 means concurrent *requests from the same user*, not multi-user concurrency).
