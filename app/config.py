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

WEB_TEMPLATES_DIR = APP_DIR / "web" / "templates"
WEB_STATIC_DIR = APP_DIR / "web" / "static"

PROJECT_NAME = "sparkpb"
IMAGE_NAME = "sparkpb/spark:4.0.3"

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
RESOURCE_CEILING_GB = 48
MASTER_MEMORY_GB = 1

# Readiness wait bounds (PLAN.md §2).
READY_POLL_INTERVAL_S = 2
READY_TIMEOUT_DEFAULT_S = 60
READY_TIMEOUT_MAX_S = 90
