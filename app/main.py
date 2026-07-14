"""Spark Playbook — FastAPI app factory (PLAN.md §4 main.py).

Phase 1 scope: topic pages + cluster control panel + embedded JupyterLab
(US-1.1, US-1.2, US-1.3). Annotation engine and other topics are out of scope
(Phase 2+).
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app import config
from app.topics.loader import TopicNotFoundError
from app.web.routes import annotation, topics


def create_app() -> FastAPI:
    app = FastAPI(title="Spark Playbook")

    app.mount("/static", StaticFiles(directory=str(config.WEB_STATIC_DIR)), name="static")
    app.include_router(topics.router)
    app.include_router(annotation.router)

    @app.exception_handler(TopicNotFoundError)
    async def topic_not_found_handler(request: Request, exc: TopicNotFoundError) -> JSONResponse:
        # loader.load_topic() raises this for an unknown topic id (US-1.1) —
        # without this handler it propagated unhandled and surfaced as a raw
        # 500 to the client (issue #4).
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    return app


app = create_app()
