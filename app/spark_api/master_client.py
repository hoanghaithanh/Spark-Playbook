"""Spark Playbook — master `:8080/json/` client (PLAN.md §1, §2, §4).

The cluster/master JSON endpoint is used for readiness (alive-worker count),
*not* application metrics (that's `app_client.py` / `:4040`, Phase 2).
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Optional

from app import config


def fetch_master_json(timeout_s: float = 3.0) -> Optional[dict]:
    """Synchronous fetch of `http://localhost:8080/json/`.

    Returns None if the master isn't reachable yet (still starting, or torn
    down) — callers treat that as "not ready" rather than an error, matching
    `compose/cli.py::_fetch_master_json`.
    """
    try:
        with urllib.request.urlopen(config.MASTER_JSON_URL, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, ConnectionError, TimeoutError, ValueError, OSError):
        return None


def alive_worker_count(data: Optional[dict]) -> Optional[int]:
    if data is None:
        return None
    return data.get("aliveworkers")
