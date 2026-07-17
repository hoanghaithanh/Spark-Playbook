"""Spark Playbook — topic page + cluster-panel routes (US-1.1, US-1.2, US-1.3;
topic-shell redesign, docs/architecture/topic-shell-redesign.md).

Server-rendered Jinja2 + HTMX (PLAN.md D4). `topic_page()` renders the one
shared shell (`shell.html`, US-SH1) for every topic, driven by that topic's
manifest.yaml + concept.md + notebook.ipynb -- no per-topic page markup.
The cluster drawer's parameter form (`fragments/_cluster_drawer.html`) is the
primary hx-target for spawn/teardown; the right-pane state visualization and
the top-bar state pill live elsewhere in the shell's DOM and ride along as
out-of-band swaps in the same response (`fragments/_cluster_update.html`),
so a single spawn/teardown POST keeps all three in sync (Decision C).
"""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
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
        "shuffle_partitions_range": config.SHUFFLE_PARTITIONS_RANGE,
        "jupyter_url": config.JUPYTER_URL,
        "master_ui_url": config.MASTER_UI_URL,
        "is_ready": status.state.value == "ready",
        "is_busy": status.state.value in ("tearing_down", "rendering", "starting", "waiting_ready"),
    }


def _shell_context(request: Request, topic: loader.Topic) -> dict:
    """Context for shell.html (topic-shell redesign, US-SH1/US-SH3) and for
    the spawn/teardown OOB update fragment -- both need the same
    drawer/right-pane/pill/breadcrumb data, just rendered into different
    templates."""
    ctx = _panel_context(request, topic)
    ctx["all_topics"] = loader.list_topics()
    return ctx


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Topics-index landing page (US-SH5) -- one card per
    content/*/manifest.yaml topic, sorted by `order`; adding/removing/
    reordering a topic folder changes this page with zero code changes."""
    topics = loader.list_topics()
    return templates.TemplateResponse(request, "topics_index.html", {"request": request, "topics": topics})


@router.get("/topics/{topic_id}", response_class=HTMLResponse)
async def topic_page(request: Request, topic_id: str) -> HTMLResponse:
    """Renders the shared topic-page shell (topic-shell redesign, US-SH1) --
    every topic (existing and future) goes through this one template, driven
    by that topic's manifest.yaml + concept.md + notebook.ipynb; no per-topic
    page markup."""
    topic = loader.load_topic(topic_id)
    ctx = _shell_context(request, topic)
    ctx["concept_html"] = topic.concept_html()
    ctx["walkthrough_steps"] = topic.walkthrough_steps()
    return templates.TemplateResponse(request, "shell.html", ctx)


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
    return templates.TemplateResponse(
        request, "fragments/_cluster_update.html", _shell_context(request, topic)
    )


@router.post("/topics/{topic_id}/teardown", response_class=HTMLResponse)
async def teardown_cluster(request: Request, topic_id: str) -> HTMLResponse:
    topic = loader.load_topic(topic_id)
    await manager.teardown()
    return templates.TemplateResponse(
        request, "fragments/_cluster_update.html", _shell_context(request, topic)
    )
