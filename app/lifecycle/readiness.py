"""Spark Playbook — readiness polling (PLAN.md §2 step 5, §4 readiness.py).

Polls `:8080/json/` every ~2s until `aliveworkers == worker_count`, bounded by
a timeout (60s default / 90s hard cap per PLAN.md §2). Runs the blocking HTTP
call in a thread via `asyncio.to_thread` so it stays a normal, cancellable
coroutine — cancellation (D5 cancel-and-replace) takes effect between polls.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from app import config
from app.spark_api import master_client


@dataclass
class ReadinessResult:
    ready: bool
    alive_workers: Optional[int]
    timed_out: bool
    master_reachable: bool


async def wait_for_ready(
    worker_count: int,
    timeout_s: int = config.READY_TIMEOUT_DEFAULT_S,
    interval_s: int = config.READY_POLL_INTERVAL_S,
) -> ReadinessResult:
    deadline = time.monotonic() + timeout_s
    last_alive: Optional[int] = None
    ever_reachable = False

    while time.monotonic() < deadline:
        data = await asyncio.to_thread(master_client.fetch_master_json)
        if data is not None:
            ever_reachable = True
            alive = master_client.alive_worker_count(data)
            last_alive = alive
            if alive == worker_count:
                return ReadinessResult(
                    ready=True, alive_workers=alive, timed_out=False, master_reachable=True,
                )
        await asyncio.sleep(interval_s)

    return ReadinessResult(
        ready=False, alive_workers=last_alive, timed_out=True, master_reachable=ever_reachable,
    )
