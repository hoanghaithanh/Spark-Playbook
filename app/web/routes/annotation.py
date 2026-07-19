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
from app.monitoring import collector
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


def _task_retry_evidence(
    app_ref: app_client.AppRef,
    stage: Dict[str, Any],
    manifest,
    original_num_tasks: Optional[int],
    superseded: bool = False,
) -> Optional[Dict[str, Any]]:
    """Reveal-time evidence for US-C9 (Decision A): "N of M tasks retried"
    for one stage. Two distinct REST-observable shapes, both found by
    actually running a real `docker kill` against a live cluster while
    building this topic (`content/fault-tolerance-lineage/notebook.ipynb`'s
    own comments record the same finding):

    1. A task that was itself actively running on the killed worker gets
       rescheduled *within the same stage attempt* -- the REST task-list
       shows a second record for that partition `index` with `attempt >= 1`.
       This is the case `collector.retries_by_index()` (the same grouping
       `DashboardCollector._build_partitions()` already does post-hoc for the
       monitoring dashboard, reused rather than duplicated) directly covers.
    2. A worker holding *shuffle data* another stage needs to fetch gets
       killed, which raises a `FetchFailedException` on the reading stage --
       Spark recovers by resubmitting that stage as a new `attemptId`
       (`/stages` then lists the same `stageId` twice: the original attempt
       FAILED, plus a later attempt whose `numTasks` is only however many
       partitions actually needed recomputing). This is the *more common*
       real-world case for a mid-shuffle kill (found empirically -- a worker
       loss almost always lands during a stage long enough for a human to
       react to, and those are exactly the shuffle stages this applies to),
       and it doesn't show up as a same-attempt task `attempt` bump at all,
       since a resubmitted attempt's own tasks each start fresh at
       `attempt=0`. `original_num_tasks` (the stage's attempt-0 `numTasks`,
       precomputed once per stageId by the caller) is what lets a later
       attempt's row report "this attempt's task count is how many of the
       original total were recomputed" instead of reporting zero.

    A FAILED attempt that was itself superseded by a later attempt (i.e. it
    triggered the resubmission) reports no evidence at all here (`None`) --
    running the same-attempt fallback against a doomed, partially-executed
    attempt would report "0 retried" on the exact row that caused the
    retries, which is backwards for a topic whose point is "did this stage
    get restarted?". The resubmitted attempt's own row (case 2 above) is
    where the real count lives.

    ponytail: only the *latest* attempt's numTasks is used against
    original_num_tasks, so repeated resubmission (attempt0 FAILED -> attempt1
    FAILED -> attempt2 COMPLETE) reports each later attempt independently
    against the original total rather than cumulatively. Acceptable for this
    topic's one-kill demonstration; revisit if a multi-kill scenario ever
    needs an accurate running total.

    Same optional-per-stage-pull shape as `_duration_quantiles()` above
    (gated by a manifest boolean): at Reveal time it isn't known which stage
    the killed worker's tasks landed in, so every stage is checked, unlike
    `_executor_rows()`'s single per-app pull."""
    if not manifest.task_retry_evidence:
        return None

    attempt_id = stage.get("attemptId", 0)
    if attempt_id and original_num_tasks is not None:
        retried_count = stage.get("numTasks") or 0
        total_count = max(original_num_tasks, retried_count)
        return {
            "total_count": total_count,
            "retried_count": retried_count,
            "kept_count": total_count - retried_count,
        }

    if stage.get("status") == "FAILED" and superseded:
        return None

    num_tasks = stage.get("numTasks") or 0
    tasks = app_client.fetch_task_list(app_ref, stage.get("stageId"), attempt_id, length=max(1000, num_tasks))
    if not isinstance(tasks, list) or not tasks:
        return None
    retries = collector.retries_by_index(tasks)
    retried_count = sum(1 for attempt in retries.values() if attempt > 0)
    return {
        "total_count": len(retries),
        "retried_count": retried_count,
        "kept_count": len(retries) - retried_count,
    }


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
    # US-C9: attempt-0's own numTasks per stageId, needed by
    # _task_retry_evidence() to report "N of the original total" for a later
    # (resubmitted) attempt of the same stage -- see that function's
    # docstring for why a resubmitted attempt's own numTasks alone isn't
    # enough context on its own.
    original_num_tasks_by_stage_id = {
        s.get("stageId"): s.get("numTasks")
        for s in stages
        if isinstance(s, dict) and s.get("attemptId", 0) == 0
    }
    # US-C9 fix: an attempt is "superseded" if a later attempt of the same
    # stageId exists -- that's what lets _task_retry_evidence() suppress the
    # misleading "0 retried" same-attempt fallback on the FAILED attempt that
    # actually triggered the resubmission (see that function's docstring).
    max_attempt_by_stage_id: Dict[Any, int] = {}
    for s in stages:
        if not isinstance(s, dict):
            continue
        sid = s.get("stageId")
        max_attempt_by_stage_id[sid] = max(max_attempt_by_stage_id.get(sid, 0), s.get("attemptId", 0))
    rows = []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        superseded = stage.get("attemptId", 0) < max_attempt_by_stage_id.get(stage.get("stageId"), 0)
        rows.append(
            {
                "stageId": stage.get("stageId"),
                "attemptId": stage.get("attemptId", 0),
                "status": stage.get("status"),
                "metrics": engine.spotlight_stage_metrics(stage, manifest),
                "duration_quantiles": _duration_quantiles(app_ref, stage, manifest),
                "task_retry": _task_retry_evidence(
                    app_ref,
                    stage,
                    manifest,
                    original_num_tasks_by_stage_id.get(stage.get("stageId")),
                    superseded,
                ),
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
        "retry_evidence_enabled": False,
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
        "retry_evidence_enabled": manifest.task_retry_evidence,
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
