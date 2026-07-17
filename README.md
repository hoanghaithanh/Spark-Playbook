# Spark Playbook

A local, single-user, self-hosted web app for learning PySpark at a mid/advanced level — the kind
of depth that holds up under interview questions about shuffle, joins, AQE, memory management, and
streaming.

## What it is

Generic tutorials and toy datasets don't produce the real cluster behavior (spill, skew, broadcast
decisions, stage-level shuffle costs) that Spark interview questions actually probe. Spark Playbook
spins up a real Spark Standalone cluster on demand (master + N workers + a driver/Jupyter
container, all in Docker), lets you run notebooks against it, generates data large/skewed enough to
trigger real Spark behavior, and pairs that with a guided curriculum plus — once built — a
self-check annotation engine and a realtime monitoring dashboard.

It runs entirely on your own machine (Windows/WSL2 or Linux, Docker + Docker Compose), with no
other users, no network exposure beyond `localhost`, and no deployment target.

**The project's real differentiator (G1 — interview-depth over platform polish):** where a choice
exists between spending effort on UI/platform sophistication versus on curriculum depth or
realistic cluster/data behavior, curriculum depth and cluster realism win. Raw, unguided cluster
access is never gated behind unfinished app features, and the self-check annotation engine is
designed to be consulted *after* you've formed a hypothesis, not to explain things for you. See
`docs/requirements/spark-playbook-mvp.md` for the full problem statement and goals.

## Current status

- **Phase 0 — cluster harness.** Built, tested, and working: render a Jinja2-templated
  `docker-compose.yml`, bring up master + N workers + a driver/JupyterLab container, reach the
  master UI/REST API and the driver's app UI/REST API from the host, run a real shuffle job, and
  generate tunable skewed synthetic data. No web app required — see `compose/README.md`.
- **Phase 1 — partitioning/shuffle topic, end-to-end in the web app.** Built, tested, and
  acceptance-validated: a FastAPI + Jinja2/HTMX app with a topic page, a cluster control panel
  (spawn/teardown with configurable workers/cores/memory/shuffle-partitions/AQE), and an embedded
  JupyterLab iframe wired to the spawned cluster. Full acceptance results in
  [`docs/acceptance/phase-1.md`](docs/acceptance/phase-1.md).
- **Phase 2 — annotation engine + join strategies + bucketing + AQE.** Built, tested, and closed
  out in Sprint 1: the self-check annotation engine, plus the join-strategies, bucketing, and AQE
  curriculum topics.
- **Phase 2.5 — realtime monitoring dashboard.** Built and bug-fixed. Acceptance report in
  [`docs/acceptance/phase-2-5.md`](docs/acceptance/phase-2-5.md), pending final human sign-off
  (all findings from the acceptance pass, including issue #22, are now resolved — see that
  report's addendum for the corrected status).
- **Sprint 3 — topic-page shell redesign.** Built, tested, and closed out: a unified topic-page
  shell (Concept/Notebook/Self-check tabs, cluster-config drawer, breadcrumb topic switcher) and
  the Phase 2.5 dashboard moving from a standalone route to an in-page slide-in panel. See
  [`docs/requirements/topic-shell-redesign.md`](docs/requirements/topic-shell-redesign.md) and
  [`docs/architecture/topic-shell-redesign.md`](docs/architecture/topic-shell-redesign.md).
  Six new curriculum topics (DAG & Lazy Evaluation, Skew & Salting, Executor Tuning,
  Checkpointing, Serialization Formats, Fault Tolerance & Lineage) are scoped alongside this in
  [`docs/requirements/curriculum-topics-2026-07.md`](docs/requirements/curriculum-topics-2026-07.md)
  but sequenced after the shell per a shell-first prioritization decision — not yet built.
- **Sprint 4 — Catalyst plans topic + data-driven topics-index landing page.** Built, tested, and
  closed out: a dedicated `content/catalyst-plans/` curriculum topic (parse → analyze → optimize
  → physical-plan phases, DataFrame/SQL/UDF predicate-pushdown comparison, three-cell notebook
  walkthrough), and `GET /` now rendering a real topics-index landing page — one card per
  `content/*/manifest.yaml` topic (id, title, order, a blurb derived from each topic's
  `concept.md`), sorted by `order`, with no hardcoded topic list — replacing the previous
  307-redirect to the first topic. See [`docs/requirements/topic-shell-redesign.md`](docs/requirements/topic-shell-redesign.md)
  (US-SH5, US-SH8) and [`docs/backlog.md`](docs/backlog.md) rows #31/#24 for acceptance evidence.
- **Sprint 5 — four more curriculum topics.** In progress: `content/dag-lazy-evaluation/` (DAG &
  Lazy Evaluation, issue #27), `content/caching-persistence/` (Caching & Persistence, issue #28),
  and `content/window-functions/` (Window Functions, issue #29) are each built, tested,
  code-reviewed with no blockers, and live-acceptance-validated against a real cluster (all
  acceptance criteria PASS, human sign-off given); Serialization Formats (issue #30) remains open.
  These bring the topics-index landing page to 8 real topics. See
  [`docs/backlog.md`](docs/backlog.md) rows #25/#14/#15 and
  [`docs/qa/window-functions-acceptance.md`](docs/qa/window-functions-acceptance.md) for the latest
  acceptance evidence.
- **Phase 3 (streaming/Kafka) and Phase 4 (remaining curriculum topics)** are backlogged, not yet
  started.

For the full prioritized list of remaining work, see [`docs/backlog.md`](docs/backlog.md). For the
phased roadmap and architecture in detail, see [`PLAN.md`](PLAN.md).

## Quickstart

### Prerequisites

- Docker Desktop with the WSL2 backend, Compose v2 (`docker compose ...`) on PATH.
- Python 3.9+, with `pip install -r app/requirements.txt` (Phase 1 web app) and `pip install
  jinja2` (Phase 0 CLI, if used standalone).

### Phase 0 — cluster harness only (no web app)

```bash
# Build the custom Spark image (once, and after editing Dockerfile.spark)
bash compose/build.sh

# Render the compose stack with defaults (3 workers x 2 cores/4GB, driver 2GB)
python compose/cli.py render

# Bring the cluster up (tears down any previous stack first)
python compose/cli.py up

# Wait for all workers to register (polls :8080/json/)
python compose/cli.py wait-for-ready

# Run the shuffle smoke test
docker exec -it spark-driver /opt/spark/bin/spark-submit /workspace/compose/smoke_test.py

# Tear down
python compose/cli.py down
```

See [`compose/README.md`](compose/README.md) for full details, including known deviations from a
literal reading of `PLAN.md` and Windows/Git Bash notes.

### Phase 1 — the web app

The web app drives the same cluster lifecycle through its own routes (spawn/teardown), rather than
`compose/cli.py` directly:

```bash
pip install -r app/requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Then open **`http://localhost:8000`** in a browser (`http://127.0.0.1:8000` also works — the
embedded JupyterLab iframe's CSP allowlist covers both loopback origins, since browsers otherwise
treat them as distinct origins for `frame-ancestors` purposes even though they resolve to the same
host).

From there: open the partitioning/shuffle topic, configure and spawn a cluster from the control
panel, and the topic's notebook opens in an embedded JupyterLab pointed at that cluster.

## Project structure

```
compose/      Phase 0 cluster harness — Dockerfile, Jinja2 compose templates, standalone CLI
app/          Phase 1+ FastAPI web app — cluster lifecycle, topic pages, (later) annotation engine
content/      Curriculum topics — one folder per topic (concept.md, notebook.ipynb, manifest.yaml)
docs/         Requirements, backlog, acceptance reports, architecture notes
PLAN.md       Full technical design: architecture, key decisions, phased roadmap, named risks
```

## Further reading

- [`PLAN.md`](PLAN.md) — full architecture and design (component diagram, cluster lifecycle,
  annotation engine design, phased roadmap, named risks and mitigations).
- [`CLAUDE.md`](CLAUDE.md) — how this repo's own development process works (the SDLC pipeline,
  sprint cadence, definition of done). The mechanics of the underlying agent team are documented
  separately in [`docs/AGENT_TEAM.md`](docs/AGENT_TEAM.md).
