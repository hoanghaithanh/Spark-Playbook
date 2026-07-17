"""Spark Playbook — annotation self-check routes (US-2.1, US-2.2; PLAN.md §3, G3).

Pull-not-push (G3): nothing here is reached except via an explicit learner
action --
  - GET  .../annotation         renders the collapsed "Reveal self-check"
                                 control only (no plan/metrics shown yet);
  - POST .../annotation/reveal  the explicit Reveal click. Reads the newest
                                 `playbook.checkpoint()` dump for the topic (if
                                 any), parses + annotates its plan via the
                                 manifest-driven engine, and renders the
                                 stage-metrics fragment alongside it;
  - GET  .../annotation/stages  the stage-metrics fragment on its own, polled
                                 by HTMX every ~6s once Reveal has happened
                                 (US-2.2), independent of the plan annotation
                                 (a still-running job's metrics keep changing
                                 after the one-time plan parse).

No narrative "why" text is generated anywhere in this module -- only mapped
labels + evidence (US-2.1), consistent with G3.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app import config
from app.annotation import engine, plan_parser
from app.annotation.manifest import ManifestError, load_annotation_manifest
from app.spark_api import app_client
from app.topics import loader

router = APIRouter()
templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))


def _latest_checkpoint(topic_id: str) -> Optional[Dict[str, Any]]:
    """Newest `playbook.checkpoint()` dump for `topic_id`, or None if the
    learner hasn't called checkpoint() yet (PLAN.md §3 -- "nothing read yet"
    until this point, then only on an explicit Reveal)."""
    topic_dir = config.ANNOTATIONS_DIR / topic_id
    if not topic_dir.is_dir():
        return None
    files = sorted(topic_dir.glob("*.json"))  # epoch-microsecond filenames sort chronologically
    if not files:
        return None
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


async def _stale_checkpoint_warning(checkpoint_data: Dict[str, Any]) -> Optional[str]:
    """Issue #16: a checkpoint's app_id is never checked against what's
    actually live on any driver UI port before Reveal renders it -- so a
    checkpoint from a torn-down-and-respawned (or simply much older) session
    renders a fully confident, fully-labeled plan with nothing to indicate
    it's stale (the starkest repro: zero live applications, yet Reveal still
    showed a 2-hour-old plan as if current). Returns None if the checkpoint's
    app_id is known to any driver currently answering across
    `DRIVER_APP_UI_PORTS` (covers the legitimate "job just completed but
    driver isn't torn down yet" case -- `fetch_all_app_ids()` includes
    completed attempts, not just running ones), or a clear warning string
    otherwise.

    Issue #24 follow-up: `fetch_all_app_ids()` now probes multiple ports
    sequentially (blocking network I/O per port), so it's offloaded via
    `asyncio.to_thread` -- called synchronously inside this `async def`
    route it would freeze the entire single-process event loop (including
    the live SSE dashboard stream) for the duration of every probe, same bug
    class as issue #19."""
    checkpoint_app_id = checkpoint_data.get("app_id")
    known_ids = await asyncio.to_thread(app_client.fetch_all_app_ids, timeout_s=2.0)

    if known_ids is None:
        return (
            "No Spark application is currently reachable on any driver UI port (4040-4042), so this "
            f"checkpoint's application ({checkpoint_app_id}) cannot be confirmed as the current session -- "
            "it may be from a prior, already-torn-down cluster. Re-run playbook.checkpoint(df, topic=...) "
            "against a live session before trusting this plan."
        )
    if checkpoint_app_id not in known_ids:
        return (
            f"This checkpoint's application ({checkpoint_app_id}) is not known to any driver currently "
            "running on a driver UI port (4040-4042) -- it's from a different or prior session, not the one "
            "you're looking at now. Re-run playbook.checkpoint(df, topic=...) against the current session."
        )
    return None


def _duration_quantiles(app_ref: app_client.AppRef, stage: Dict[str, Any], manifest) -> Optional[Dict[str, Any]]:
    """Issue #8: a second REST call per stage (`?withSummaries=true`),
    made only when the topic's manifest opts in -- skips the extra request
    entirely for the common case where no manifest declares
    `task_duration_quantiles: true`."""
    if not manifest.task_duration_quantiles:
        return None
    detail = app_client.fetch_stage_task_summary(app_ref, stage.get("stageId"), stage.get("attemptId", 0))
    return engine.spotlight_task_duration_quantiles(detail, manifest)


def _executor_rows(app_ref: Optional[app_client.AppRef], manifest) -> Optional[List[Dict[str, Any]]]:
    """Reveal-time evidence for US-C10/US-C3 (Decision A): per-executor
    `executor_metrics` spotlighting from `/api/v1/applications/<id>/executors`,
    mirroring `_stage_rows()`'s fetch->spotlight shape exactly, one level down
    (executors instead of stages)."""
    if app_ref is None:
        return None
    executors = app_client.fetch_executors(app_ref)
    if not isinstance(executors, list):
        return None
    rows = []
    for executor in executors:
        if not isinstance(executor, dict):
            continue
        rows.append({"id": executor.get("id"), "metrics": engine.spotlight_executor_metrics(executor, manifest)})
    return rows


def _stage_rows(app_ref: Optional[app_client.AppRef], manifest) -> Optional[List[Dict[str, Any]]]:
    if app_ref is None:
        return None
    stages = app_client.fetch_stages(app_ref)
    # fetch_stages() passes the REST response through unvalidated (see its
    # docstring) -- guard here against both "unreachable" (None) and an
    # unexpected shape (e.g. a dict instead of a list), treating both the
    # same way rather than raising on the latter.
    if not isinstance(stages, list):
        return None
    rows = []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        rows.append(
            {
                "stageId": stage.get("stageId"),
                "attemptId": stage.get("attemptId", 0),
                "status": stage.get("status"),
                "metrics": engine.spotlight_stage_metrics(stage, manifest),
                "duration_quantiles": _duration_quantiles(app_ref, stage, manifest),
                "ui_url": app_client.stage_ui_url(app_ref, stage.get("stageId"), stage.get("attemptId", 0)),
            }
        )
    return rows


async def _stages_context(request: Request, topic, checkpoint_data: Optional[Dict[str, Any]]) -> dict:
    base = {
        "request": request,
        "topic": topic,
        "poll_interval_s": config.STAGE_POLL_INTERVAL_S,
        "quantiles_enabled": False,
    }
    try:
        manifest = load_annotation_manifest(topic.id)
    except ManifestError as exc:
        # Mirrors reveal_annotation()'s own ManifestError handling (issue #9):
        # this endpoint is HTMX-polled every ~6s independent of Reveal, so
        # without this guard a broken manifest.yaml raised an unhandled 500
        # on every single poll cycle for as long as the panel stayed open,
        # instead of the same clear message Reveal already shows for the
        # identical failure mode.
        return {**base, "app_id": None, "stages": None, "manifest_error": str(exc)}

    # Issue #24: a checkpoint's app_id records no port, and its application
    # may no longer be the *most recent* one live (a learner may have since
    # opened another topic's notebook without shutting down this one's
    # kernel, which pushes the new one onto :4041/:4042) -- so resolve
    # specifically which port still serves *this* checkpoint's app_id rather
    # than assuming :4040 or reusing "most recent" discovery. Absent a
    # checkpoint, fall back to the most-recent live application, same as the
    # dashboard.
    # Offloaded via `asyncio.to_thread` -- both `resolve_app()` and
    # `resolve_current_app()` now probe multiple ports sequentially
    # (blocking network I/O), and this is called from an `async def` route,
    # same reasoning as `_stale_checkpoint_warning()` above.
    if checkpoint_data:
        checkpoint_app_id = checkpoint_data.get("app_id")
        app_ref = (
            await asyncio.to_thread(app_client.resolve_app, checkpoint_app_id, timeout_s=2.0)
            if checkpoint_app_id
            else None
        )
        # Keep showing the checkpoint's own id even if no port currently
        # serves it (matches the existing "show the id regardless of
        # reachability" behavior `_stale_checkpoint_warning` already covers).
        app_id = checkpoint_app_id
    else:
        app_ref = await asyncio.to_thread(app_client.resolve_current_app, timeout_s=2.0)
        app_id = app_ref.app_id if app_ref else None

    return {
        **base,
        "app_id": app_id,
        "app_ref": app_ref,
        "quantiles_enabled": manifest.task_duration_quantiles,
        # Issue #8 follow-up: fetch_stage_task_summary() (called inside
        # _stage_rows()) is a second blocking REST call per stage, on top of
        # the pre-existing fetch_stages() call -- same event-loop-freezing
        # hazard as resolve_app()/resolve_current_app() above, so offload the
        # whole synchronous helper the same way.
        "stages": await asyncio.to_thread(_stage_rows, app_ref, manifest),
        "manifest_error": None,
    }


@router.get("/topics/{topic_id}/annotation", response_class=HTMLResponse)
async def annotation_panel(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    return templates.TemplateResponse(
        request, "fragments/annotation_panel.html", {"request": request, "topic": topic}
    )


@router.post("/topics/{topic_id}/annotation/reveal", response_class=HTMLResponse)
async def reveal_annotation(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    checkpoint_data = _latest_checkpoint(topic_id)

    ctx: Dict[str, Any] = {
        "request": request,
        "topic": topic,
        "checkpoint": checkpoint_data,
        "manifest_error": None,
        "stale_warning": None,
        "executor_metrics_enabled": False,
        "executors": None,
    }

    if checkpoint_data is not None:
        try:
            manifest = load_annotation_manifest(topic_id)
        except ManifestError as exc:
            ctx["manifest_error"] = str(exc)
        else:
            operators = plan_parser.parse_operators(checkpoint_data.get("explain_formatted", ""))
            ctx["annotated_nodes"] = engine.annotate_plan(operators, manifest)
            ctx["stale_warning"] = await _stale_checkpoint_warning(checkpoint_data)
            stages_ctx = await _stages_context(request, topic, checkpoint_data)
            ctx.update(stages_ctx)

            # US-C10/US-C3 (Decision A): only pulled when the topic's manifest
            # actually declares executor_metrics -- most topics don't, and
            # this stays a single reveal-time REST read, not a poll (unlike
            # stage_metrics above, which _stages_context also feeds to the
            # ~6s-polled stage_metrics_fragment() route). Reuses the app_ref
            # _stages_context() already resolved above (issue #24 code-review
            # finding) instead of re-running the identical checkpoint-vs-live
            # port-probe a second time per request.
            if manifest.executor_metrics:
                ctx["executor_metrics_enabled"] = True
                app_ref = stages_ctx.get("app_ref")
                ctx["executors"] = await asyncio.to_thread(_executor_rows, app_ref, manifest)

    return templates.TemplateResponse(request, "fragments/annotation_reveal.html", ctx)


@router.get("/topics/{topic_id}/annotation/stages", response_class=HTMLResponse)
async def stage_metrics_fragment(request: Request, topic_id: str) -> HTMLResponse:
    """Polled every ~6s by HTMX (US-2.2) once Reveal has happened."""
    topic = loader.load_topic(topic_id)
    checkpoint_data = _latest_checkpoint(topic_id)
    ctx = await _stages_context(request, topic, checkpoint_data)
    return templates.TemplateResponse(request, "fragments/stage_table.html", ctx)
