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


def _stage_rows(app_id: Optional[str], manifest) -> Optional[List[Dict[str, Any]]]:
    if not app_id:
        return None
    stages = app_client.fetch_stages(app_id)
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
                "ui_url": app_client.stage_ui_url(stage.get("stageId"), stage.get("attemptId", 0)),
            }
        )
    return rows


def _stages_context(request: Request, topic, checkpoint_data: Optional[Dict[str, Any]]) -> dict:
    base = {
        "request": request,
        "topic": topic,
        "poll_interval_s": config.STAGE_POLL_INTERVAL_S,
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

    app_id = checkpoint_data.get("app_id") if checkpoint_data else app_client.fetch_current_app_id()
    return {
        **base,
        "app_id": app_id,
        "stages": _stage_rows(app_id, manifest),
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

    ctx: Dict[str, Any] = {"request": request, "topic": topic, "checkpoint": checkpoint_data, "manifest_error": None}

    if checkpoint_data is not None:
        try:
            manifest = load_annotation_manifest(topic_id)
        except ManifestError as exc:
            ctx["manifest_error"] = str(exc)
        else:
            operators = plan_parser.parse_operators(checkpoint_data.get("explain_formatted", ""))
            ctx["annotated_nodes"] = engine.annotate_plan(operators, manifest)
            ctx.update(_stages_context(request, topic, checkpoint_data))

    return templates.TemplateResponse(request, "fragments/annotation_reveal.html", ctx)


@router.get("/topics/{topic_id}/annotation/stages", response_class=HTMLResponse)
async def stage_metrics_fragment(request: Request, topic_id: str) -> HTMLResponse:
    """Polled every ~6s by HTMX (US-2.2) once Reveal has happened."""
    topic = loader.load_topic(topic_id)
    checkpoint_data = _latest_checkpoint(topic_id)
    ctx = _stages_context(request, topic, checkpoint_data)
    return templates.TemplateResponse(request, "fragments/stage_table.html", ctx)
