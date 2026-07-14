"""Spark Playbook — app configuration (PLAN.md §2, §4).

Central place for filesystem paths and the template-variable defaults/ranges
table from PLAN.md §2. Mirrors `compose/cli.py`'s DEFAULTS/validation so the
FastAPI app and the standalone Phase 0 CLI agree on the same numbers.
"""
from __future__ import annotations

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

MASTER_JSON_URL = "http://localhost:8080/json/"
MASTER_UI_URL = "http://localhost:8080"
DRIVER_APP_UI_URL = "http://localhost:4040"
JUPYTER_URL = "http://localhost:8888"

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
