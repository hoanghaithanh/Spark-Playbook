"""Spark Playbook — driver application-metrics client (PLAN.md §1, §3, §4;
issue #24 port-discovery fix, docs/architecture/driver-port-discovery.md).

Distinct from `master_client.py` (`:8080/json/`, cluster readiness). This
talks to the running driver's own Spark UI REST surface
(`http://localhost:<port>/api/v1/...`), used for:
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

Issue #24: a learner switching topic notebooks without shutting down the
prior Jupyter kernel leaves that kernel's SparkContext alive holding `:4040`;
Spark's own SparkUI then silently rebinds to `:4041` (then `:4042`) for the
next still-alive SparkContext rather than failing. This module used to
hardcode `:4040` everywhere, so the dashboard/self-check stayed locked onto
whichever application first grabbed that port, forever -- the direct cause of
the Cluster Monitor's Job Detail view freezing on the first job of a session.
`resolve_current_app()` / `resolve_app()` below probe every port in
`config.DRIVER_APP_UI_PORTS` (matching the compose template's own `4040-4042`
port mapping) and return an `AppRef(app_id, base_url)` that downstream
fetchers take instead of a bare `app_id`, so every call in one cycle/request
provably agrees on the same port (see the ADR for why a bare-string
`app_id` + a module-level "last resolved port" cache was rejected).
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app import config

_TRANSIENT_ERRORS = (urllib.error.URLError, ConnectionError, TimeoutError, ValueError, OSError)


@dataclass(frozen=True)
class AppRef:
    """An application id paired with the base URL of the driver REST port
    that actually serves it (issue #24) -- the two travel together by
    construction so callers can never accidentally query one app's id
    against a different app's port."""

    app_id: str
    base_url: str


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


def _probe_ports(timeout_s: float) -> List[Tuple[str, List[dict]]]:
    """`[(base_url, applications_json), ...]` for every `DRIVER_APP_UI_PORTS`
    entry that answered with a list-shaped `/api/v1/applications` response.
    Ports that are unreachable or wrong-shaped are simply absent -- same
    degrade-gracefully contract as every other fetch in this module, just
    applied across the whole candidate range instead of one fixed URL."""
    results: List[Tuple[str, List[dict]]] = []
    for port in config.DRIVER_APP_UI_PORTS:
        base_url = f"http://localhost:{port}"
        apps = _get_json(f"{base_url}/api/v1/applications", timeout_s)
        if isinstance(apps, list):
            results.append((base_url, apps))
    return results


def resolve_current_app(timeout_s: float = 3.0) -> Optional[AppRef]:
    """The most-recently-started still-running application across every port
    in `DRIVER_APP_UI_PORTS` (issue #24) -- replaces the old
    `fetch_current_app_id()` as the collector's/annotation's entry point.
    "Most recent `startTimeEpoch` wins" when more than one port has a
    running application concurrently (two still-open kernels): the dashboard
    should follow whatever the learner most recently kicked off, not
    whichever kernel happened to grab :4040 first. Returns `None` if no port
    is reachable or none has a running application."""
    best: Optional[Tuple[int, AppRef]] = None
    for base_url, apps in _probe_ports(timeout_s):
        for app in apps:
            if not isinstance(app, dict):
                continue
            attempts = app.get("attempts")
            if not (isinstance(attempts, list) and attempts and isinstance(attempts[-1], dict)):
                continue
            latest = attempts[-1]
            if not _attempt_is_running(latest):
                continue
            app_id = app.get("id")
            if not isinstance(app_id, str):
                continue
            start_epoch = latest.get("startTimeEpoch") or 0
            if best is None or start_epoch > best[0]:
                best = (start_epoch, AppRef(app_id=app_id, base_url=base_url))
    return best[1] if best is not None else None


def resolve_app(app_id: str, timeout_s: float = 3.0) -> Optional[AppRef]:
    """Which port currently serves a *specific* app_id, running or completed
    (issue #24). Used by the annotation Reveal flow, whose `app_id` comes
    from a checkpoint file (no port recorded there) rather than from live
    discovery -- `resolve_current_app()` can't be used there because the
    checkpoint's app may no longer be the *most recent* one. Returns `None`
    if no probed port knows this id."""
    for base_url, apps in _probe_ports(timeout_s):
        for app in apps:
            if isinstance(app, dict) and app.get("id") == app_id:
                return AppRef(app_id=app_id, base_url=base_url)
    return None


def fetch_all_app_ids(timeout_s: float = 3.0) -> Optional[List[str]]:
    """Union of application ids -- both still-running *and* completed
    attempts -- known to *any* reachable driver port in
    `DRIVER_APP_UI_PORTS` (issue #24: a learner's checkpoint may belong to a
    kernel that isn't the current/most-recent one, but is still a live,
    legitimate session). Distinct from `resolve_current_app()`, which only
    considers the single most-recent still-running application.

    Used by the annotation Reveal flow (issue #16) to distinguish "this
    checkpoint's app_id belongs to a driver session that's live right now"
    from "this checkpoint is from an entirely different/prior session" (a
    torn-down-and-respawned cluster's driver is a brand-new process with no
    memory of the old app_id at all). Returns `None` only if *no* probed
    port is reachable/shaped as documented -- same degrade-gracefully
    contract as before, just aggregated across ports. Returns `[]` (not
    `None`) if at least one port is reachable but genuinely has recorded no
    applications yet -- that's a meaningful "reachable, but this id isn't
    here" answer, not an error.
    """
    probed = _probe_ports(timeout_s)
    if not probed:
        return None
    ids: List[str] = []
    for _base_url, apps in probed:
        ids.extend(app["id"] for app in apps if isinstance(app, dict) and isinstance(app.get("id"), str))
    return ids


def fetch_stages(app: AppRef, timeout_s: float = 3.0) -> Optional[List[Dict[str, Any]]]:
    """Raw stage list from `/api/v1/applications/<id>/stages`, as returned by
    the REST API (US-2.2 -- "sourced from the REST API, not re-derived"),
    queried against `app.base_url` (issue #24) rather than a hardcoded port.
    Callers iterating the result should still guard against an unexpected
    shape (e.g. a dict instead of a list) -- this function passes the parsed
    JSON through unmodified rather than validating it itself, since the
    "is this shaped right" check belongs with whoever iterates it (see
    `app.web.routes.annotation._stage_rows`)."""
    url = f"{app.base_url}/api/v1/applications/{app.app_id}/stages"
    return _get_json(url, timeout_s)


def stage_ui_url(app: AppRef, stage_id: int, attempt_id: int = 0) -> str:
    """Deep link to the specific stage's page in the real Spark UI (US-2.2 --
    "not just the application's landing page"), built from `app.base_url`
    (issue #24) so a `:4041`/`:4042` application's deep links actually
    resolve instead of always pointing at `:4040`."""
    return f"{app.base_url}/stages/stage/?id={stage_id}&attempt={attempt_id}"


def fetch_executors(app: AppRef, timeout_s: float = 3.0) -> Optional[List[Dict[str, Any]]]:
    """Raw executor list from `/api/v1/applications/<id>/executors` (ADR D-D
    -- GC time is a JVM metric with no Docker-stats source, but Spark already
    exposes it per-executor here, including the driver as executor id
    `"driver"`), queried against `app.base_url` (issue #24). Reused as a
    library dependency by `app/monitoring/collector.py`; passed through
    unvalidated like `fetch_stages()`, same "unreachable vs. unexpected
    shape" contract."""
    url = f"{app.base_url}/api/v1/applications/{app.app_id}/executors"
    return _get_json(url, timeout_s)


def fetch_task_list(
    app: AppRef, stage_id: int, attempt_id: int = 0, length: int = 1000, timeout_s: float = 3.0
) -> Optional[List[Dict[str, Any]]]:
    """Raw per-task list for one stage attempt, from
    `/api/v1/applications/<id>/stages/<id>/<attempt>/taskList` -- the
    per-task executor id / duration / input+shuffle bytes the monitoring
    dashboard treats as "partitions" (requirements doc's measurability note:
    "partition" and "task" are interchangeable for this feature). Queried
    against `app.base_url` (issue #24).

    `length` is passed through explicitly -- found by actually running this
    against a real stage with 200 tasks: the endpoint silently paginates to
    only the first **20** tasks if `length` is omitted, which made the
    dashboard's partition table/skew detection look at an arbitrary task
    subset instead of the whole stage. `length=1000` comfortably covers this
    project's realistic worker/partition counts (PLAN.md's resource-ceiling
    range) without needing real pagination support.
    """
    url = (
        f"{app.base_url}/api/v1/applications/{app.app_id}/stages/{stage_id}/{attempt_id}/taskList"
        f"?length={length}"
    )
    return _get_json(url, timeout_s)
