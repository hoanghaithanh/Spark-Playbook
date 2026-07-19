"""Spark Playbook — cluster lifecycle state machine (PLAN.md §2, D5, §4 manager.py).

Single-slot lifecycle: at most one Spark stack (`sparkpb`) exists at a time.
Implements the state machine from PLAN.md §2:

    IDLE --spawn--> TEARING_DOWN(old) --> RENDERING --> STARTING --> WAITING_READY --> READY
                                                                                          |
                                                                       teardown <---------'

A spawn/teardown request arriving while another is in flight (D5
cancel-and-replace):
    1. cancel the in-flight task
    2. await a guaranteed teardown of whatever it started (idempotent)
    3. proceed with the new request from RENDERING

A module-level singleton (`manager`) is used by the web routes — this app is
single-user/single-process, so one in-memory instance is the whole state.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from app import config
from app.lifecycle import compose_ops, readiness, renderer
from app.lifecycle.renderer import ClusterParams, ValidationResult

logger = logging.getLogger(__name__)

# This worktree's own identity for the cross-worktree ownership guard (issue
# #38, docs/architecture/worktree-cluster-isolation.md). Computed once at
# import time -- RENDERED_DIR is fixed per process/worktree.
_SELF_OWNER = config.norm_path(config.RENDERED_DIR)


class ClusterState(str, Enum):
    IDLE = "idle"
    TEARING_DOWN = "tearing_down"
    RENDERING = "rendering"
    STARTING = "starting"
    WAITING_READY = "waiting_ready"
    READY = "ready"
    FAILED = "failed"


@dataclass
class ManagerStatus:
    state: ClusterState
    message: str
    params: Optional[ClusterParams]
    spawn_id: int
    alive_workers: Optional[int] = None
    error: Optional[str] = None


@dataclass
class SpawnOutcome:
    ok: bool
    status: ManagerStatus


class ClusterManager:
    def __init__(self) -> None:
        # Guards mutation of self._task / cancel-and-replace bookkeeping only —
        # NOT held for the full duration of a spawn, so a new request can
        # interrupt an in-flight one immediately (D5).
        self._mutate_lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

        self.state: ClusterState = ClusterState.IDLE
        self.message: str = "No cluster running."
        self.params: Optional[ClusterParams] = None
        self.spawn_id: int = 0
        self.alive_workers: Optional[int] = None
        self.error: Optional[str] = None

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #

    def status(self) -> ManagerStatus:
        return ManagerStatus(
            state=self.state,
            message=self.message,
            params=self.params,
            spawn_id=self.spawn_id,
            alive_workers=self.alive_workers,
            error=self.error,
        )

    def validate(self, params: ClusterParams) -> ValidationResult:
        return renderer.validate(params)

    async def spawn(self, params: ClusterParams, timeout_s: int = config.READY_TIMEOUT_DEFAULT_S) -> SpawnOutcome:
        """Validate, then cancel-and-replace into a fresh spawn (PLAN.md §2/D5)."""
        result = self.validate(params)
        if not result.ok:
            self.state = ClusterState.FAILED
            self.error = "; ".join(result.errors)
            self.message = f"Rejected: {self.error}"
            return SpawnOutcome(ok=False, status=self.status())

        if await self._refuse_if_foreign_owner():
            return SpawnOutcome(ok=False, status=self.status())

        async with self._mutate_lock:
            await self._cancel_and_teardown_locked()
            self.spawn_id += 1
            spawn_id = self.spawn_id
            self.params = params
            self.error = None
            self.alive_workers = None
            task = asyncio.create_task(self._run_spawn(spawn_id, params, timeout_s))
            self._task = task

        try:
            ok = await task
        except asyncio.CancelledError:
            # This request's task was itself cancelled and superseded by a
            # later spawn/teardown request (D5 cancel-and-replace) — report a
            # clear "superseded" outcome instead of propagating CancelledError
            # up into the HTTP handler as a 500.
            return SpawnOutcome(
                ok=False,
                status=ManagerStatus(
                    state=ClusterState.FAILED,
                    message="Spawn superseded by a newer request before it could complete.",
                    params=params,
                    spawn_id=spawn_id,
                    alive_workers=None,
                    error="superseded",
                ),
            )
        return SpawnOutcome(ok=ok, status=self.status())

    async def teardown(self) -> ManagerStatus:
        if await self._refuse_if_foreign_owner():
            return self.status()

        async with self._mutate_lock:
            await self._cancel_and_teardown_locked()
            self.state = ClusterState.IDLE
            self.message = "Cluster torn down."
            self.params = None
            self.alive_workers = None
            self.error = None
        return self.status()

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #

    async def _refuse_if_foreign_owner(self) -> bool:
        """Guard against a foreign worktree's live cluster (issue #38): if a
        `sparkpb` Compose project is already running and owned by a
        *different* worktree, refuse the operation instead of tearing it
        down. Returns True (and sets FAILED status) if the caller must stop;
        same-worktree or nothing-running returns False and changes nothing.

        # ponytail: naive read-then-act check, not a distributed lock --
        # a real cross-process lock is only worth building if simultaneous
        # cold spawns across worktrees become common (ADR R-WT-3).
        """
        owner = await compose_ops.running_owner()
        if owner is None or owner == _SELF_OWNER:
            return False

        self.state = ClusterState.FAILED
        self.error = (
            f"A '{config.PROJECT_NAME}' cluster is already running, owned by another "
            f"worktree ({owner}). Refusing to spawn/teardown -- it would tear down "
            f"that worktree's live cluster. Tear it down there first, or wait."
        )
        self.message = f"Refused: {self.error}"
        return True

    async def _cancel_and_teardown_locked(self) -> None:
        """Cancel any in-flight spawn task and guarantee teardown (D5 step 1-2).

        Caller must hold `self._mutate_lock`.
        """
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                # The failed task already recorded its own error/message on
                # self.error/self.message, so control flow doesn't change
                # here — but the exception itself must not vanish without a
                # durable trace (issue #1), since self.message gets
                # overwritten by the very next state transition below.
                logger.exception(
                    "Unexpected exception from a cancelled/superseded spawn task "
                    "during cancel-and-replace teardown."
                )

        self.state = ClusterState.TEARING_DOWN
        self.message = "Tearing down previous stack (if any)..."
        await compose_ops.down()  # idempotent; safe even if nothing was running

    async def _run_spawn(self, spawn_id: int, params: ClusterParams, timeout_s: int) -> bool:
        try:
            self.state = ClusterState.RENDERING
            self.message = "Rendering compose template..."
            renderer.render(params)

            # Step 3 (PLAN.md §2): tear down old, awaited to exit 0. Already done
            # once by _cancel_and_teardown_locked before this task was created;
            # repeating here is the literal per-spawn step and is idempotent/cheap.
            self.state = ClusterState.TEARING_DOWN
            self.message = "Tearing down previous stack..."
            down_result = await compose_ops.down()
            if not down_result.ok:
                self.message = f"WARNING: teardown exited {down_result.returncode}; continuing."
                # self.message above is transient status shown in the UI and
                # gets overwritten by the very next state transition a few
                # lines down — log durably too so the failure survives in
                # server logs (issue #1). Behavior is unchanged: still
                # continues to up() per PLAN.md's original design intent (a
                # real port/name collision in up() is the safety net if
                # teardown didn't fully complete).
                logger.warning(
                    "Pre-spawn teardown ('docker compose down') exited %s; "
                    "continuing to up() anyway. stderr: %s",
                    down_result.returncode,
                    down_result.stderr.strip(),
                )

            self.state = ClusterState.STARTING
            self.message = "Starting containers (docker compose up -d)..."
            up_result = await compose_ops.up()
            if not up_result.ok:
                self.state = ClusterState.FAILED
                self.error = up_result.stderr.strip() or f"docker compose up failed (exit {up_result.returncode})"
                self.message = f"Spawn failed: {self.error}"
                return False

            self.state = ClusterState.WAITING_READY
            self.message = f"Waiting for {params.worker_count} worker(s) to register..."
            t0 = time.monotonic()
            result = await readiness.wait_for_ready(params.worker_count, timeout_s=timeout_s)
            elapsed = time.monotonic() - t0
            self.alive_workers = result.alive_workers

            if result.ready:
                self.state = ClusterState.READY
                self.message = (
                    f"READY: {result.alive_workers}/{params.worker_count} workers alive "
                    f"after {elapsed:.1f}s."
                )
                return True

            self.state = ClusterState.FAILED
            if not result.master_reachable:
                self.error = f"Master never became reachable at {config.MASTER_JSON_URL} within {timeout_s}s."
            else:
                self.error = (
                    f"Timed out after {timeout_s}s: {result.alive_workers or 0}/"
                    f"{params.worker_count} workers alive. Stack left up for inspection."
                )
            self.message = f"Spawn timed out: {self.error}"
            return False
        except asyncio.CancelledError:
            self.message = "Spawn cancelled (superseded by a newer request)."
            raise


# Module-level singleton — single-user, single-process app (PLAN.md constraints).
manager = ClusterManager()
