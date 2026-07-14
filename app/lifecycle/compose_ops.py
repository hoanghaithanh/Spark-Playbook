"""Spark Playbook — async `docker compose` subprocess wrappers (PLAN.md §2, §4).

Shells out to the `docker compose` CLI via `asyncio.create_subprocess_exec`,
mirroring `compose/cli.py`'s `_run_compose`/`cmd_down`/`cmd_up` but async so the
FastAPI lifecycle manager can await completion without blocking the event loop
(PLAN.md §2: "awaiting a subprocess to exit is the natural 'down fully
completed' barrier we need").
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app import config


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


async def _run(*args: str) -> CommandResult:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return CommandResult(
        returncode=proc.returncode if proc.returncode is not None else -1,
        stdout=stdout.decode("utf-8", errors="replace"),
        stderr=stderr.decode("utf-8", errors="replace"),
    )


async def down() -> CommandResult:
    """`docker compose -p sparkpb down --remove-orphans` — idempotent (PLAN.md R4).

    Project-name-scoped so it works even if no compose file has been rendered
    yet in this process (matches `compose/cli.py::cmd_down`'s fallback).
    """
    if config.COMPOSE_FILE.exists():
        return await _run(
            "docker", "compose", "-p", config.PROJECT_NAME,
            "-f", str(config.COMPOSE_FILE), "down", "--remove-orphans",
        )
    return await _run(
        "docker", "compose", "-p", config.PROJECT_NAME, "down", "--remove-orphans",
    )


async def up() -> CommandResult:
    """`docker compose -p sparkpb up -d` against the already-rendered compose file."""
    return await _run(
        "docker", "compose", "-p", config.PROJECT_NAME,
        "-f", str(config.COMPOSE_FILE), "up", "-d",
    )
