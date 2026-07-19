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
from typing import Optional

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
    try:
        stdout, stderr = await proc.communicate()
    except asyncio.CancelledError:
        # The *task* awaiting this coroutine was cancelled (D5 cancel-and-
        # replace) — without this, the underlying `docker compose` OS process
        # is left running detached, able to keep creating/starting containers
        # after the cancelling caller has already issued its own fresh
        # down()/up(), undermining PLAN.md §6/R4's "awaited down before up"
        # guarantee. Kill it and reap it before propagating the cancellation.
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        raise
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


async def running_owner() -> Optional[str]:
    """Normalized `project.working_dir` of the worktree that owns the
    currently-running `sparkpb` Compose project, or `None` if nothing is
    running (issue #38 ownership guard,
    docs/architecture/worktree-cluster-isolation.md).

    Every worktree renders its compose file into its own
    `<worktree>/compose/rendered/`, so Compose's own
    `com.docker.compose.project.working_dir` container label is already a
    reliable per-worktree owner identity -- no template change needed.

    Never raises: any docker error (unreachable daemon, malformed output)
    degrades to `None` (fail-open by design -- a guard that can't read state
    must not itself block all spawns; `up()`'s own duplicate-name/bound-port
    failure remains the last-resort safety net).
    """
    try:
        ps_result = await _run(
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={config.PROJECT_NAME}",
        )
        if not ps_result.ok:
            return None
        ids = [line.strip() for line in ps_result.stdout.splitlines() if line.strip()]
        if not ids:
            return None

        inspect_result = await _run(
            "docker", "inspect", ids[0],
            "--format", '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}',
        )
        if not inspect_result.ok:
            return None
        label = inspect_result.stdout.strip()
        if not label:
            return None
        return config.norm_path(label)
    except asyncio.CancelledError:
        raise
    except OSError:
        # e.g. the `docker` executable isn't on PATH -- fail-open (see
        # docstring / ADR R-WT-2), same treatment `docker_stats.py` already
        # gives an unreachable daemon.
        return None
