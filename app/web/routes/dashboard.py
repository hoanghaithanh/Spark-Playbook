"""Spark Playbook — realtime cluster monitoring dashboard routes (US-5.1-5.6,
ADR D-B, D-E; topic-shell redesign US-SH4, Decision B, issue #23).

The standalone `/dashboard` page is retired (US-SH4) -- the Cluster Monitor
panel embedded in the topic-page shell (`shell.html`) is the sole entry
point now. Three routes:

  - GET  /dashboard         307 redirect to `/topics/<first-topic>?monitor=open`
                             (reusing `topics.index`'s first-topic resolution),
                             so existing bookmarks/links land on a real page
                             with the panel auto-opened (Decision B2) instead
                             of dead-ending.
  - GET  /dashboard/panel   the panel body: top bar + all three views
                             (client-side switched, ADR D-B) + the SSE
                             listener element (`dashboard/_dashboard_body.html`).
                             Server-rendered inline with a fresh snapshot so
                             the first paint isn't blank while waiting for
                             the first SSE push. Fetched by the shell's
                             Cluster Monitor panel on open.
  - GET  /dashboard/stream  the SSE feed (`text/event-stream`). One
                             connection per client; subscribes to the shared
                             `collector` singleton, which does the actual
                             sampling (ADR D-B -- collection decoupled from
                             delivery). Each pushed event is one HTML blob
                             containing out-of-band (`hx-swap-oob`) fragments
                             for the overview strip, job detail, and every
                             node's detail block, so a single connection
                             keeps all three views current regardless of
                             which one is currently visible client-side.
                             Unchanged by the panel migration (Decision B).
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app import config
from app.lifecycle.manager import ClusterState, manager
from app.monitoring.collector import collector
from app.monitoring.model import Snapshot
from app.spark_api import app_client
from app.topics import loader

router = APIRouter()
templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))
# Templates reference dashboard color/interval constants directly
# (dashboard/macros.html, dashboard/page.html) so the threshold color system
# stays a single source of truth in config.py (ADR "Component / data
# design").
templates.env.globals["config"] = config

# How often the SSE generator wakes up to check for a new snapshot / for the
# client having disconnected, independent of the collector's own sampling
# cadence -- keeps disconnect detection responsive even between collector
# cycles.
_STREAM_POLL_S = 1.0


def _idle_snapshot() -> Snapshot:
    return Snapshot(cluster_active=False, has_job=False)


async def _current_snapshot() -> Snapshot:
    """Best-effort snapshot for the initial full-page render -- a real
    collector sample if the cluster is READY, otherwise the empty state.
    Never raises: a transient Docker/Spark hiccup on first paint degrades to
    the empty/idle rendering rather than a 500 (the SSE stream will recover
    it on the next cycle)."""
    if manager.state != ClusterState.READY:
        return _idle_snapshot()
    try:
        return await collector.collect_once()
    except Exception:
        return collector.inactive_snapshot()


async def _driver_ui_url() -> str:
    """Best-effort "open Spark UI" header link (issue #24, same bug class as
    the Job Detail freeze, but low priority -- a header link, not the
    freeze): points at the currently-resolved application's actual port when
    one is discoverable, falling back to the historical `:4040` default
    otherwise (e.g. no cluster/application yet)."""
    try:
        app_ref = await asyncio.to_thread(app_client.resolve_current_app, timeout_s=2.0)
    except Exception:
        app_ref = None
    return app_ref.base_url if app_ref else config.DRIVER_APP_UI_URL


@router.get("/dashboard")
async def dashboard_page() -> RedirectResponse:
    """Retired standalone page (US-SH4, issue #23) -- redirects to the
    Cluster Monitor panel on a real topic page, reusing `topics.index`'s
    first-topic resolution (Decision B2) so existing bookmarks/links keep
    working instead of 404ing."""
    topics = loader.list_topics()
    topic_id = topics[0].id if topics else "partitioning-shuffle"
    return RedirectResponse(url=f"/topics/{topic_id}?monitor=open", status_code=307)


@router.get("/dashboard/panel", response_class=HTMLResponse)
async def dashboard_panel(request: Request) -> HTMLResponse:
    snapshot = await _current_snapshot()
    return templates.TemplateResponse(
        request,
        "dashboard/_dashboard_body.html",
        {
            "request": request,
            "snapshot": snapshot,
            "master_ui_url": config.MASTER_UI_URL,
            "driver_ui_url": await _driver_ui_url(),
        },
    )


def _render_oob_payload(request: Request, snapshot: Snapshot) -> str:
    """One HTML blob: overview + job-detail + a container of every node's
    detail block, each carrying `hx-swap-oob="true"` so the HTMX SSE
    extension's single `sse-swap` listener element (which itself swaps
    nothing, `hx-swap="none"`) fans the update out to all three views at
    once (ADR D-B)."""
    ctx = {"request": request, "snapshot": snapshot}
    overview = templates.get_template("dashboard/fragments/overview_oob.html").render(ctx)
    job_detail = templates.get_template("dashboard/fragments/job_detail_oob.html").render(ctx)
    node_detail = templates.get_template("dashboard/fragments/node_detail_oob.html").render(ctx)
    return overview + job_detail + node_detail


@router.get("/dashboard/stream")
async def dashboard_stream(request: Request) -> StreamingResponse:
    async def event_generator():
        queue = await collector.subscribe()
        try:
            while True:
                # No explicit `await request.is_disconnected()` poll here --
                # Starlette's StreamingResponse already races its own
                # internal disconnect listener against this generator on the
                # same ASGI receive channel, and cancels this task on a real
                # client disconnect. A second consumer of that same channel
                # here would contend with Starlette's own listener for
                # messages and can starve both (found by actually running
                # this against a live ASGI stack, not just code review) --
                # relying on the `finally` below (invoked by the resulting
                # CancelledError/GeneratorExit) is both correct and simpler.
                collector.ensure_running()
                try:
                    snapshot = await asyncio.wait_for(queue.get(), timeout=_STREAM_POLL_S)
                except asyncio.TimeoutError:
                    continue
                html = _render_oob_payload(request, snapshot)
                # SSE payloads must not contain literal newlines per line --
                # collapse the rendered HTML onto a single `data:` line.
                data = html.replace("\r\n", " ").replace("\n", " ")
                yield f"event: message\ndata: {data}\n\n"
        finally:
            collector.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
