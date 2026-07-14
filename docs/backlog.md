# Backlog

The single prioritized list of work not yet pulled into a sprint. Owned by the `project-manager` agent (ordering/prioritization); populated by the `requirements-analyst` agent (new stories) as it writes requirements docs.

Ordered top-to-bottom by priority. Entries move out of this list into a sprint milestone during sprint planning, and back in if they're descoped or carried over undecided.

| Priority | Story | Size | Requirements doc | Status |
|---|---|---|---|---|
<!-- Example row:
| 1 | As a user, I want to reset my password via email | M | [docs/requirements/password-reset.md](requirements/password-reset.md) | Backlog |
-->
| 1 | Phase 0 — cluster harness proven manually: spawn/teardown a Spark Standalone cluster (master + 3 workers + driver/Jupyter) from a compose template, reach :8080/:4040/REST API, run a real shuffle job, generate tunable skewed/large synthetic data, and allow unguided notebook practice against the live cluster | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 2 | Curriculum topic: Partitioning & shuffle mechanics — what/why content + runnable notebook | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 3 | Phase 1 — partitioning/shuffle topic end-to-end in the web app: topic page, UI cluster spawn/teardown with configurable parameters, embedded Jupyter wired to the spawned cluster | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 4 | Curriculum topic: Catalyst plans & `.explain` — what/why content + runnable notebook | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 5 | Curriculum topic: Join strategies (broadcast vs sort-merge vs shuffle-hash) — what/why content + runnable notebook forcing each strategy | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 6 | Curriculum topic: Bucketing (co-partitioned joins) — what/why content + runnable notebook showing shuffle-free joins vs mismatched-bucket contrast case | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 7 | Curriculum topic: AQE (skew join, partition coalescing, plan-changes-at-runtime) — what/why content + before/after (AQE on/off) notebook | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 8 | Phase 2 — annotation engine as a self-check tool: static plan-node labeling (Exchange/BroadcastExchange/SortMergeJoin/Window etc.) + runtime stage-metrics lookup with deep link into the real Spark UI, driven by per-topic manifests | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Done (Sprint 1) |
| 9 | Phase 2.5 — Realtime monitoring dashboard: live per-node (master/worker) CPU & RAM utilization, sourced from Docker container stats (not Spark's own REST API) | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | In sprint (Sprint 2) |
| 10 | Phase 2.5 — Realtime monitoring dashboard: live per-node task/partition execution detail for the running stage (task count, size, duration per executor), sourced from existing Spark stage/task REST API | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | In sprint (Sprint 2) |
| 11 | Phase 2.5 — Realtime monitoring dashboard: derived stage ETA + task-duration variance display, clearly labeled as an estimate | S | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | In sprint (Sprint 2) |
| 12 | Phase 2.5 — Realtime monitoring dashboard: diagnostic signal surfacing (skew, node-saturation visualization) without automated tuning suggestions, per G3 self-check philosophy | M | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | In sprint (Sprint 2) |
| 13 | Phase 2.5 — Realtime monitoring dashboard: UI placement/integration with existing cluster panel, topic pages, and deep links into the real Spark UI | S | [docs/requirements/realtime-monitoring-dashboard.md](requirements/realtime-monitoring-dashboard.md) | In sprint (Sprint 2) |
| 14 | Curriculum topic: Caching/persistence — storage levels, eviction/spill-to-disk demo | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 15 | Curriculum topic: Window functions — ranking/running aggregates/lead-lag, shuffle+sort cost, skew tie-in | S | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 16 | Curriculum topic: UDF vs pandas UDF serialization cost — timing comparison + plan distinction | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 17 | Curriculum topic: Memory management & spill (unified memory manager, execution vs storage memory, off-heap, OOM diagnosis) — dedicated topic, not folded into joins/AQE | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 18 | Curriculum topic: Structured Streaming + Kafka (watermarks, stateful aggregation, checkpoint recovery) — notebook + live query-progress chart | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 19 | Phase 3 — streaming + Kafka integration: conditional Kafka (KRaft) in the compose template, synthetic producer script, wired into the streaming topic | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 20 | Curriculum topic: Delta/Iceberg (optional) — ACID writes, time travel, schema evolution | M | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 21 | Curriculum topic: Tuning/debugging capstone with "diagnose cold" exercises — annotation engine hidden until deliberately revealed, covering shuffle/partitioning, join misdiagnosis, skew, and memory/spill scenarios | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |
| 22 | Phase 4 — remaining curriculum integration: caching, window functions, UDF/pandas UDF, memory/spill, Delta/Iceberg (optional), tuning/debugging capstone, all wired end-to-end in the web app | L | [docs/requirements/spark-playbook-mvp.md](requirements/spark-playbook-mvp.md) | Backlog |

## Known issues / watch-items

Non-feature operational notes that don't need a story/acceptance-criteria entry in the table above, but are worth tracking if they recur.

| Date observed | Item | Status | Revisit condition |
|---|---|---|---|
| 2026-07-14 | Docker Desktop engine threw transient `EOF` pipe errors twice during `docker compose up` calls (e.g. `error during connect: Post "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/...": EOF`) while devops-engineer ran the Phase 0 cluster harness validation (build/up/wait-for-ready/smoke-test against a live Docker daemon, `sparkpb` compose stack). Both times a plain retry of the same command succeeded cleanly with no lasting effect; appears to be engine-level flakiness in this machine's Docker Desktop install, unrelated to our compose config. | Low-priority / Watching | Reprioritize and investigate only if it recurs with higher frequency, or if a retry stops resolving it. |
