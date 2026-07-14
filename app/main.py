"""Spark Playbook — FastAPI app factory (PLAN.md §4 main.py).

Phase 1 scope: topic pages + cluster control panel + embedded JupyterLab
(US-1.1, US-1.2, US-1.3). Annotation engine and other topics are out of scope
(Phase 2+).
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app import config
from app.web.routes import topics


def create_app() -> FastAPI:
    app = FastAPI(title="Spark Playbook")

    app.mount("/static", StaticFiles(directory=str(config.WEB_STATIC_DIR)), name="static")
    app.include_router(topics.router)

    return app


app = create_app()
