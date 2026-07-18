"""Spark Playbook — app configuration (PLAN.md §2, §4).

Central place for filesystem paths and the template-variable defaults/ranges
table from PLAN.md §2. Mirrors `compose/cli.py`'s DEFAULTS/validation so the
FastAPI app and the standalone Phase 0 CLI agree on the same numbers.
"""
from __future__ import annotations

import os
from pathlib import Path

# Repo layout. app/config.py lives at <repo_root>/app/config.py.
APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent

COMPOSE_DIR = REPO_ROOT / "compose"
TEMPLATES_DIR = COMPOSE_DIR / "templates"
RENDERED_DIR = COMPOSE_DIR / "rendered"
COMPOSE_FILE = RENDERED_DIR / "docker-compose.yml"
SPARK_DEFAULTS_FILE = RENDERED_DIR / "spark-defaults.conf"

CONTENT_DIR = REPO_ROOT / "content"

# Pull-not-push self-check shared volume (PLAN.md §3, G3). This is a plain
# subdirectory of the repo, NOT the `/shared` path baked into
# `compose/Dockerfile.spark` -- that path is never bind-mounted anywhere in
# `compose/templates/docker-compose.yml.j2` (only the whole repo, at
# /workspace, is), so anything written to `/shared` inside a container would
# be invisible to this host-side FastAPI process. `scratch/` is already
# gitignored and already the convention for generated/scratch data, and is
# visible identically on both sides via the existing /workspace bind mount
# (REPO_ROOT here == /workspace inside every container), so
# `driver/playbook/annotate.py::checkpoint()` writes here instead.
SHARED_DIR = REPO_ROOT / "scratch" / "shared"
ANNOTATIONS_DIR = SHARED_DIR / "annotations"

WEB_TEMPLATES_DIR = APP_DIR / "web" / "templates"
WEB_STATIC_DIR = APP_DIR / "web" / "static"

PROJECT_NAME = "sparkpb"
IMAGE_NAME = "sparkpb/spark:4.0.3"

# The FastAPI app's own origin, per PLAN.md §1's architecture diagram
# ("browser at http://localhost:8000"). Referenced by driver/jupyter_config.py
# (PLAN.md §6/R3, issue #7) for the CSP `frame-ancestors` allowlist that lets
# the embedded JupyterLab iframe render on this app's page — see that file's
# module docstring for why it can't just import this constant (different
# process/container, plain-Python config file with no access to app/).
APP_PORT = 8000
APP_ORIGIN = f"http://localhost:{APP_PORT}"

# --- Public-deploy config split (docs/architecture/public-deploy.md D3) ---
#
# Two different audiences now read different URLs for the same cluster:
#   - the app's own server-side fetches (master_client.py, app_client.py,
#     manager.py) need a host that's reachable *from inside the app
#     container* -- CLUSTER_HOST, default "127.0.0.1" (the deploy stack sets
#     it to "host.docker.internal"). Literal IP, not the hostname
#     "localhost": verified live on Windows that Python's urllib resolves
#     "localhost" via a slow IPv6 (::1) attempt before falling back to IPv4
#     -- ~2s wasted *per call*, and app_client._probe_ports() makes three
#     such calls (DRIVER_APP_UI_PORTS) every ~2s collector cycle while the
#     Cluster Monitor panel is open, which was making the dashboard look
#     hung/unresponsive. Docker Desktop only ever publishes these ports on
#     127.0.0.1 (IPv4) anyway, so the IP is also the more precise default.
#   - the browser needs either a directly-reachable host:port (dev, no
#     proxy in front) or a same-origin proxy subpath (deployed, behind
#     nginx) -- JUPYTER_URL / MASTER_UI_URL, each independently
#     env-overridable and defaulting to today's literal localhost URLs so
#     the undeployed dev workflow is unchanged.
# All defaults below reproduce pre-public-deploy behavior exactly when no
# env vars are set (constraint: don't break `uvicorn app.main:app` run
# directly for local dev).
CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "127.0.0.1")

MASTER_JSON_URL = f"http://{CLUSTER_HOST}:8080/json/"
DRIVER_APP_UI_URL = f"http://{CLUSTER_HOST}:4040"

JUPYTER_URL = os.environ.get("JUPYTER_URL", "http://localhost:8888")
MASTER_UI_URL = os.environ.get("MASTER_UI_URL", "http://localhost:8080")

# Public HTTPS origin of a deployed instance (e.g. "https://spark.example.com"),
# empty in dev. Forwarded into the spawned driver container as
# SPARKPB_PUBLIC_ORIGIN (see compose/templates/docker-compose.yml.j2 +
# driver/jupyter_config.py) so the embedded JupyterLab iframe's CSP
# `frame-ancestors` allows the deployed origin, not just localhost.
PUBLIC_ORIGIN = os.environ.get("PUBLIC_ORIGIN", "")

# Driver Spark-UI/REST ports (issue #24). A learner switching topic notebooks
# without shutting down the prior Jupyter kernel leaves that kernel's
# SparkContext alive and holding :4040; Spark's own SparkUI then silently
# rebinds to :4041 (then :4042) for the next still-alive SparkContext rather
# than failing (confirmed live: two concurrently-open kernels really do land
# on different ports). `compose/templates/docker-compose.yml.j2` already
# publishes this same `4040-4042` range, so the multi-port scenario was
# anticipated by the compose template but never wired into `app_client.py`,
# which used to look at :4040 only -- the direct cause of the dashboard's
# Job Detail view getting stuck on whichever application first grabbed that
# port. Fixed small list mirroring the literal compose port mapping, not a
# variable-driven range like WORKER_COUNT_RANGE.
DRIVER_APP_UI_PORTS = (4040, 4041, 4042)

# Template variable defaults (PLAN.md §2 table).
DEFAULTS = {
    "worker_count": 3,
    "worker_cores": 2,
    "worker_memory_gb": 4,
    "driver_memory_gb": 2,
    "shuffle_partitions": 200,
    "aqe_enabled": False,
}

# Ranges (PLAN.md §2 / US-1.2).
WORKER_COUNT_RANGE = (1, 5)
WORKER_CORES_RANGE = (1, 4)
WORKER_MEMORY_GB_RANGE = (1, 8)

# Shuffle-partitions UI range (topic-shell redesign, US-SH2 -- settled
# 2026-07-15). PLAN.md §2's underlying `shuffle_partitions` template variable
# stays "any positive integer" semantically; this is only the drawer's UI
# bound (docs/requirements/topic-shell-redesign.md), replacing the previous
# unbounded `min="1"`-only input the pre-redesign panel used.
SHUFFLE_PARTITIONS_RANGE = (1, 300)

# Resource ceiling, GB (PLAN.md §2 resource-ceiling check / R5).
#
# Issue #6 (test-engineer acceptance validation, Phase 1): PLAN.md §2 named
# "e.g. 48GB" only illustratively, as headroom on a 64GB host — it was never
# checked against the UI's own documented ranges. With WORKER_COUNT_RANGE
# capped at 5 and WORKER_MEMORY_GB_RANGE capped at 8, and driver_memory_gb
# fixed at DEFAULTS["driver_memory_gb"] (2GB, not user-adjustable via the
# form), the maximum total any legitimate UI spawn can ever request is
# 1 (master) + 5*8 (workers) + 2 (driver) = 43GB — always under 48GB. That
# made US-1.2's "the UI rejects an over-budget config" acceptance criterion
# structurally unreachable through real use: the ceiling check existed in
# code but could never actually fire.
#
# Decision: lower the ceiling to 32GB (option (a) from the filed issue, not
# widening the ranges (b) or declaring the ceiling deliberately unreachable
# (c)) — a routine number tweak, not a design change, so handled directly
# rather than escalated to the architect. 32GB was chosen, not just any
# reachable value, to satisfy two constraints simultaneously:
#   - It must stay >= the resource budget doc's explicitly *supported*
#     scale-up scenario ("a single worker may be scaled up to 8GB, for
#     skew/spill demos" — docs/requirements/spark-playbook-mvp.md). Phase 1's
#     template applies worker_memory_gb uniformly to every worker (no
#     per-worker override yet), so the closest reachable equivalent today is
#     the *default worker count* at 8GB each: 1 + 3*8 + 2 = 27GB. That must
#     keep passing, so the ceiling must be > 27.
#   - It must still be low enough that some in-range combinations legitimately
#     exceed it, so the rejection path is actually reachable in normal usage
#     (not just at the extreme top-right corner of every range at once) --
#     e.g. worker_count=4, worker_memory_gb=8 -> 1+32+2=35GB, and
#     worker_count=5, worker_memory_gb=8 -> 1+40+2=43GB, both now correctly
#     rejected, while worker_count=5, worker_memory_gb=4 -> 1+20+2=23GB and
#     the 27GB single-scale-up demo above both still pass.
# 32GB comfortably satisfies both, and still leaves 32GB of headroom on the
# 64GB host (R5's actual safety intent), so it isn't a weakening of the
# safety margin -- just a number that makes the existing check reachable.
RESOURCE_CEILING_GB = 32
MASTER_MEMORY_GB = 1

# Readiness wait bounds (PLAN.md §2).
READY_POLL_INTERVAL_S = 2
READY_TIMEOUT_DEFAULT_S = 60
READY_TIMEOUT_MAX_S = 90

# Runtime stage-metrics polling interval (PLAN.md §3, US-2.2: target 5-10s,
# D4's HTMX `hx-trigger="every 6s"` idiom).
STAGE_POLL_INTERVAL_S = 6

# Realtime cluster monitoring dashboard (Phase 2.5, ADR
# docs/architecture/realtime-monitoring-dashboard.md, D-B/D-C).
#
# Collector cadence: the ADR commits to an effective end-to-end latency of
# <=3s (not a hard 2s) because `docker stats` inherently needs ~1-2s to
# produce a CPU% delta sample -- see ADR D-B.
DASHBOARD_COLLECTOR_INTERVAL_S = 2.0
# Ring buffer length for the Node Detail sparklines (ADR D-E: "20 buckets of
# CPU/RAM history" -- ephemeral, in-memory only, not the no-history non-goal).
DASHBOARD_HISTORY_LENGTH = 20

# Color threshold system (ADR "Component / data design" -- Threshold color
# system, kept from the mockup). Applied server-side by the fragment
# renderers, not client JS.
DASHBOARD_COLOR_GREEN = "#16a34a"
DASHBOARD_COLOR_AMBER = "#d97706"
DASHBOARD_COLOR_RED = "#dc2626"
DASHBOARD_COLOR_MASTER_BADGE = "#7c3aed"

DASHBOARD_CPU_WARN_PCT = 70
DASHBOARD_CPU_CRIT_PCT = 88
DASHBOARD_RAM_WARN_PCT = 75
DASHBOARD_RAM_CRIT_PCT = 90
DASHBOARD_GC_WARN_MS = 20
DASHBOARD_GC_CRIT_MS = 40

# Skew threshold used by diagnostics.py (task/partition size vs. the stage's
# own median) -- a partition at or above this multiple of the median is
# flagged. Chosen to match the mockup's own worked example (worker-2's ~1.7x
# median partitions, ~5x for the most extreme ones) while staying low enough
# to actually fire on realistic AQE-topic skew data, not just extreme cases.
DASHBOARD_SKEW_MEDIAN_MULTIPLE = 1.5

# CPU core limits for containers whose `deploy.resources.limits.cpus` in
# `compose/templates/docker-compose.yml.j2` isn't driven by a per-worker
# variable (worker cpu limit is `worker_cores`, already available on
# `ClusterParams`) -- master is hardcoded `"1"`, driver is hardcoded `"2"` in
# the template (ADR D-C caveat: CPU% normalization needs the real limit).
DASHBOARD_MASTER_CPU_CORES = 1.0
DASHBOARD_DRIVER_CPU_CORES = 2.0
