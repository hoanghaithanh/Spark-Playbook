"""Spark Playbook — driver `:4040` application-metrics client (PLAN.md §1, §3, §4).

Distinct from `master_client.py` (`:8080/json/`, cluster readiness). This
talks to the running driver's own Spark UI REST surface
(`http://localhost:4040/api/v1/...`), used for:
  - app-id discovery (PLAN.md §3 "App-id discovery") -- the one entry whose
    latest attempt has no real `endTime` is the current application;
  - per-stage runtime metrics (US-2.2) -- `shuffleReadBytes`,
    `shuffleWriteBytes`, `numTasks`, spill bytes, etc., used as returned,
    never re-derived/estimated. `executorRunTime` (this stage's task-time
    total, summed across all tasks) stands in for US-2.2's "per-task
    duration summary" -- it's a real, REST-API-sourced aggregate, not a true
    per-task quantile distribution (min/p25/p50/p75/max), which would need
    the separate `/stages/<id>/<attempt>?withSummaries=true` endpoint. Left
    as a follow-up (see the filed issue) rather than in scope here;
  - a deep-link URL builder into the real per-stage Spark UI page (not just
    the app landing page).
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from app import config

_TRANSIENT_ERRORS = (urllib.error.URLError, ConnectionError, TimeoutError, ValueError, OSError)


def _get_json(url: str, timeout_s: float) -> Optional[Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except _TRANSIENT_ERRORS:
        return None


def _attempt_is_running(attempt: Dict[str, Any]) -> bool:
    """An attempt still in progress reports a sentinel epoch `endTime`
    (`"1969-12-31T23:59:59.999GMT"` / `"1970-01-01T00:00:00.000GMT"`
    depending on Spark version/timezone) rather than a real completion time."""
    end_time = attempt.get("endTime")
    if not end_time:
        return True
    end_time = str(end_time)
    return end_time.startswith("1969-12-31") or end_time.startswith("1970-01-01")


def fetch_current_app_id(timeout_s: float = 3.0) -> Optional[str]:
    """Returns the id of the one application whose latest attempt is still
    running, or None if `:4040` isn't reachable, no application is active, or
    the response isn't shaped the way the REST API is documented to shape it
    (e.g. `{"error": "..."}` instead of a list -- degrade the same as
    "unreachable" rather than raising) (PLAN.md §3 "App-id discovery"). By
    design (single driver, one stack at a time, D5 cancel-and-replace) there
    is at most one such application."""
    apps = _get_json(f"{config.DRIVER_APP_UI_URL}/api/v1/applications", timeout_s)
    if not isinstance(apps, list):
        return None
    for app in apps:
        if not isinstance(app, dict):
            continue
        attempts = app.get("attempts")
        if isinstance(attempts, list) and attempts and isinstance(attempts[-1], dict):
            if _attempt_is_running(attempts[-1]):
                return app.get("id")
    return None


def fetch_all_app_ids(timeout_s: float = 3.0) -> Optional[List[str]]:
    """All application ids known to whatever driver process currently answers
    at `:4040` -- both still-running *and* completed attempts within that
    process's lifetime. Distinct from `fetch_current_app_id()`, which only
    considers an application whose latest attempt is still actively running
    (US-2.2's narrower "is a job in progress" question).

    Used by the annotation Reveal flow (issue #16) to distinguish "this
    checkpoint's app_id belongs to the driver session that's live right now"
    (same process, possibly a just-completed job -- the legitimate case) from
    "this checkpoint is from an entirely different/prior session" (a
    torn-down-and-respawned cluster's driver is a brand-new process with no
    memory of the old app_id at all). Returns `None` if `:4040` isn't
    reachable or the response isn't shaped as documented -- same
    degrade-gracefully contract as `fetch_current_app_id()`. Returns `[]`
    (not `None`) if `:4040` is reachable but has genuinely recorded no
    applications yet -- that's a meaningful "reachable, but this id isn't
    here" answer, not an error.
    """
    apps = _get_json(f"{config.DRIVER_APP_UI_URL}/api/v1/applications", timeout_s)
    if not isinstance(apps, list):
        return None
    return [app["id"] for app in apps if isinstance(app, dict) and isinstance(app.get("id"), str)]


def fetch_stages(app_id: str, timeout_s: float = 3.0) -> Optional[List[Dict[str, Any]]]:
    """Raw stage list from `/api/v1/applications/<id>/stages`, as returned by
    the REST API (US-2.2 -- "sourced from the REST API, not re-derived").
    Callers iterating the result should still guard against an unexpected
    shape (e.g. a dict instead of a list) -- this function passes the parsed
    JSON through unmodified rather than validating it itself, since the
    "is this shaped right" check belongs with whoever iterates it (see
    `app.web.routes.annotation._stage_rows`)."""
    url = f"{config.DRIVER_APP_UI_URL}/api/v1/applications/{app_id}/stages"
    return _get_json(url, timeout_s)


def stage_ui_url(stage_id: int, attempt_id: int = 0) -> str:
    """Deep link to the specific stage's page in the real Spark UI (US-2.2 --
    "not just the application's landing page")."""
    return f"{config.DRIVER_APP_UI_URL}/stages/stage/?id={stage_id}&attempt={attempt_id}"


def fetch_executors(app_id: str, timeout_s: float = 3.0) -> Optional[List[Dict[str, Any]]]:
    """Raw executor list from `/api/v1/applications/<id>/executors` (ADR D-D
    -- GC time is a JVM metric with no Docker-stats source, but Spark already
    exposes it per-executor here, including the driver as executor id
    `"driver"`). Reused as a library dependency by
    `app/monitoring/collector.py`; passed through unvalidated like
    `fetch_stages()`, same "unreachable vs. unexpected shape" contract."""
    url = f"{config.DRIVER_APP_UI_URL}/api/v1/applications/{app_id}/executors"
    return _get_json(url, timeout_s)


def fetch_task_list(
    app_id: str, stage_id: int, attempt_id: int = 0, length: int = 1000, timeout_s: float = 3.0
) -> Optional[List[Dict[str, Any]]]:
    """Raw per-task list for one stage attempt, from
    `/api/v1/applications/<id>/stages/<id>/<attempt>/taskList` -- the
    per-task executor id / duration / input+shuffle bytes the monitoring
    dashboard treats as "partitions" (requirements doc's measurability note:
    "partition" and "task" are interchangeable for this feature).

    `length` is passed through explicitly -- found by actually running this
    against a real stage with 200 tasks: the endpoint silently paginates to
    only the first **20** tasks if `length` is omitted, which made the
    dashboard's partition table/skew detection look at an arbitrary task
    subset instead of the whole stage. `length=1000` comfortably covers this
    project's realistic worker/partition counts (PLAN.md's resource-ceiling
    range) without needing real pagination support.
    """
    url = (
        f"{config.DRIVER_APP_UI_URL}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList"
        f"?length={length}"
    )
    return _get_json(url, timeout_s)
