"""Spark Playbook — topic page + cluster-panel routes (US-1.1, US-1.2, US-1.3).

Server-rendered Jinja2 + HTMX (PLAN.md D4). The cluster panel/status is a
fragment reused for the initial page load and for the spawn/teardown POST
responses, so htmx can swap it in place without a full page reload.
"""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app import config
from app.lifecycle.manager import manager
from app.lifecycle.renderer import ClusterParams
from app.topics import loader

router = APIRouter()
templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))


def _panel_context(request: Request, topic: loader.Topic) -> dict:
    status = manager.status()
    is_current_topic = status.params is not None  # single-slot cluster; no per-topic tracking needed for MVP
    return {
        "request": request,
        "topic": topic,
        "status": status,
        "defaults": topic.cluster_defaults,
        "worker_count_range": config.WORKER_COUNT_RANGE,
        "worker_cores_range": config.WORKER_CORES_RANGE,
        "worker_memory_gb_range": config.WORKER_MEMORY_GB_RANGE,
        "jupyter_url": config.JUPYTER_URL,
        "master_ui_url": config.MASTER_UI_URL,
        "is_ready": status.state.value == "ready",
        "is_busy": status.state.value in ("tearing_down", "rendering", "starting", "waiting_ready"),
    }


@router.get("/", response_class=RedirectResponse)
async def index() -> RedirectResponse:
    topics = loader.list_topics()
    if not topics:
        return RedirectResponse(url="/topics/partitioning-shuffle")
    return RedirectResponse(url=f"/topics/{topics[0].id}")


@router.get("/topics/{topic_id}", response_class=HTMLResponse)
async def topic_page(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    ctx = _panel_context(request, topic)
    ctx["concept_html"] = topic.concept_html()
    return templates.TemplateResponse(request, "topic.html", ctx)


@router.get("/topics/{topic_id}/panel", response_class=HTMLResponse)
async def cluster_panel(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    return templates.TemplateResponse(request, "fragments/cluster_panel.html", _panel_context(request, topic))


@router.post("/topics/{topic_id}/spawn", response_class=HTMLResponse)
async def spawn_cluster(
    request: Request,
    topic_id: str,
    worker_count: int = Form(...),
    worker_cores: int = Form(...),
    worker_memory_gb: int = Form(...),
    shuffle_partitions: int = Form(...),
    aqe_enabled: bool = Form(False),
) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    params = ClusterParams(
        worker_count=worker_count,
        worker_cores=worker_cores,
        worker_memory_gb=worker_memory_gb,
        driver_memory_gb=config.DEFAULTS["driver_memory_gb"],
        shuffle_partitions=shuffle_partitions,
        aqe_enabled=aqe_enabled,
    )
    # Bounded wait per PLAN.md §2: 60s default target, 90s hard cap for larger configs.
    timeout_s = config.READY_TIMEOUT_DEFAULT_S if worker_count <= 3 else config.READY_TIMEOUT_MAX_S
    await manager.spawn(params, timeout_s=timeout_s)
    return templates.TemplateResponse(request, "fragments/cluster_panel.html", _panel_context(request, topic))


@router.post("/topics/{topic_id}/teardown", response_class=HTMLResponse)
async def teardown_cluster(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    await manager.teardown()
    return templates.TemplateResponse(request, "fragments/cluster_panel.html", _panel_context(request, topic))
