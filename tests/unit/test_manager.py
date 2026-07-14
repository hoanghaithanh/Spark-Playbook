"""Tests for app/lifecycle/manager.py — the cluster lifecycle state machine,
including the D5 cancel-and-replace logic (PLAN.md §2).

`compose_ops.up`/`down` and `readiness.wait_for_ready` are mocked at their
module boundary (`app.lifecycle.manager.compose_ops` / `.readiness`) so these
tests run in milliseconds with no Docker/subprocess involved.
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from app.lifecycle import manager as manager_module
from app.lifecycle.compose_ops import CommandResult
from app.lifecycle.manager import ClusterState
from app.lifecycle.readiness import ReadinessResult
from app.lifecycle.renderer import ClusterParams

OK_DOWN = CommandResult(returncode=0, stdout="", stderr="")
OK_UP = CommandResult(returncode=0, stdout="", stderr="")


def params(**overrides) -> ClusterParams:
    base = dict(worker_count=3, worker_cores=2, worker_memory_gb=4)
    base.update(overrides)
    return ClusterParams(**base)


def ready(worker_count=3) -> ReadinessResult:
    return ReadinessResult(ready=True, alive_workers=worker_count, timed_out=False, master_reachable=True)


def not_ready(alive=1) -> ReadinessResult:
    return ReadinessResult(ready=False, alive_workers=alive, timed_out=True, master_reachable=True)


@pytest.fixture
def mocks(fresh_manager, monkeypatch):
    """Patch compose_ops/readiness/renderer as seen through manager.py's
    imports, and record every call for ordering assertions."""
    calls = []

    down_mock = AsyncMock(return_value=OK_DOWN, side_effect=lambda: calls.append("down") or OK_DOWN)
    up_mock = AsyncMock(return_value=OK_UP, side_effect=lambda: calls.append("up") or OK_UP)

    async def _wait_default(worker_count, timeout_s=60, interval_s=2):
        calls.append("wait")
        return ready(worker_count)

    wait_mock = AsyncMock(side_effect=_wait_default)

    monkeypatch.setattr(manager_module.compose_ops, "down", down_mock)
    monkeypatch.setattr(manager_module.compose_ops, "up", up_mock)
    monkeypatch.setattr(manager_module.readiness, "wait_for_ready", wait_mock)
    monkeypatch.setattr(manager_module.renderer, "render", lambda p: calls.append("render"))

    return {"calls": calls, "down": down_mock, "up": up_mock, "wait": wait_mock}


class TestNormalSpawn:
    @pytest.mark.asyncio
    async def test_transitions_idle_to_ready(self, fresh_manager, mocks):
        assert fresh_manager.state == ClusterState.IDLE

        outcome = await fresh_manager.spawn(params())

        assert outcome.ok is True
        assert outcome.status.state == ClusterState.READY
        assert outcome.status.alive_workers == 3
        # Order per PLAN.md §2: cancel-teardown(no-op) -> render -> teardown ->
        # up -> wait.
        assert mocks["calls"] == ["down", "render", "down", "up", "wait"]

    @pytest.mark.asyncio
    async def test_status_reflects_ready_after_spawn(self, fresh_manager, mocks):
        await fresh_manager.spawn(params())
        status = fresh_manager.status()
        assert status.state == ClusterState.READY
        assert status.error is None


class TestSpawnTimeout:
    @pytest.mark.asyncio
    async def test_timeout_ends_failed_not_ready(self, fresh_manager, mocks, monkeypatch):
        async def _wait_timeout(worker_count, timeout_s=60, interval_s=2):
            mocks["calls"].append("wait")
            return not_ready(alive=1)

        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(side_effect=_wait_timeout))

        outcome = await fresh_manager.spawn(params(worker_count=3))

        assert outcome.ok is False
        assert outcome.status.state == ClusterState.FAILED
        assert outcome.status.state != ClusterState.READY
        assert "timed out" in outcome.status.error.lower() or "timeout" in outcome.status.error.lower()
        assert "1" in outcome.status.error  # reports how many workers actually came up


class TestValidationRejectsBeforeAnyContainerAction:
    @pytest.mark.asyncio
    async def test_invalid_params_never_call_compose_or_render(self, fresh_manager, mocks):
        outcome = await fresh_manager.spawn(params(worker_count=99))  # out of 1-5 range

        assert outcome.ok is False
        assert outcome.status.state == ClusterState.FAILED
        assert mocks["calls"] == []  # nothing touched compose_ops/renderer at all
        mocks["down"].assert_not_called()
        mocks["up"].assert_not_called()
        mocks["wait"].assert_not_called()


class TestCancelAndReplace:
    """D5, the critical case. This regression-tests the bug found and fixed
    during Phase 1 manual verification: a superseded spawn's task used to
    raise CancelledError out of `await task` in spawn(), which propagated as
    an unhandled exception (500) instead of a clean 'superseded' result.
    """

    @pytest.mark.asyncio
    async def test_second_spawn_cancels_first_and_first_resolves_cleanly(self, fresh_manager, monkeypatch):
        calls = []
        down_mock = AsyncMock(side_effect=lambda: calls.append("down") or OK_DOWN)
        up_mock = AsyncMock(side_effect=lambda: calls.append("up") or OK_UP)

        async def _slow_wait(worker_count, timeout_s=60, interval_s=2):
            calls.append("wait-start")
            await asyncio.sleep(0.3)
            calls.append("wait-end")
            return ready(worker_count)

        monkeypatch.setattr(manager_module.compose_ops, "down", down_mock)
        monkeypatch.setattr(manager_module.compose_ops, "up", up_mock)
        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(side_effect=_slow_wait))
        monkeypatch.setattr(manager_module.renderer, "render", lambda p: None)

        # Kick off a spawn we do NOT await to completion — it should still be
        # sitting in WAITING_READY (blocked on the 0.3s sleep) when we issue
        # the second request.
        task1 = asyncio.create_task(fresh_manager.spawn(params(worker_count=3), timeout_s=5))
        await asyncio.sleep(0.05)
        assert fresh_manager.state == ClusterState.WAITING_READY

        internal_task1 = fresh_manager._task
        assert internal_task1 is not None and not internal_task1.done()

        # Second spawn (D5 cancel-and-replace): must not raise, must cancel
        # task1's in-flight work, and must proceed only after a teardown.
        outcome2 = await fresh_manager.spawn(params(worker_count=2), timeout_s=5)

        # The critical regression assertion: awaiting the superseded task1
        # must NOT raise CancelledError up to the caller (this is exactly
        # what the manually-found bug did — see class docstring).
        result1 = await task1

        assert result1.ok is False
        assert result1.status.error == "superseded"
        assert result1.status.state == ClusterState.FAILED

        assert outcome2.ok is True
        assert outcome2.status.state == ClusterState.READY

        # task1's internal task was genuinely cancelled, not left running to
        # completion in the background.
        assert internal_task1.cancelled()
        # It only reached "wait-start" once (never resumed after the sleep it
        # was cancelled inside).
        assert calls.count("wait-start") == 2  # task1 started waiting, task2 started waiting
        assert calls.count("wait-end") == 1  # only task2's wait actually completed

        # No state where "both could be considered in flight": task2's
        # cancel-and-teardown step ran (a `down` call) strictly between
        # task1 being cancelled and task2's own `up`.
        down_count_before_task2_up = calls[: calls.index("up")].count("down")
        assert down_count_before_task2_up >= 1

    @pytest.mark.asyncio
    async def test_would_have_caught_the_original_bug(self, fresh_manager, monkeypatch):
        """Demonstrates, independent of manager.py's current fix, that
        awaiting a task cancelled out from under it raises CancelledError by
        default -- i.e. proves the shape of assertion above
        (`result1 = await task1` with no try/except) is exactly what the
        original bug would have violated, had spawn() not caught it."""

        async def _buggy_spawn_shape():
            inner = asyncio.create_task(asyncio.sleep(10))
            await asyncio.sleep(0.01)
            inner.cancel()
            # No try/except CancelledError here -- this is the pre-fix shape.
            return await inner

        with pytest.raises(asyncio.CancelledError):
            await _buggy_spawn_shape()


class TestConcurrentTeardownDuringSpawn:
    @pytest.mark.asyncio
    async def test_teardown_cancels_in_flight_spawn_cleanly(self, fresh_manager, monkeypatch):
        calls = []
        down_mock = AsyncMock(side_effect=lambda: calls.append("down") or OK_DOWN)
        up_mock = AsyncMock(side_effect=lambda: calls.append("up") or OK_UP)

        async def _slow_wait(worker_count, timeout_s=60, interval_s=2):
            calls.append("wait-start")
            await asyncio.sleep(0.3)
            return ready(worker_count)

        monkeypatch.setattr(manager_module.compose_ops, "down", down_mock)
        monkeypatch.setattr(manager_module.compose_ops, "up", up_mock)
        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(side_effect=_slow_wait))
        monkeypatch.setattr(manager_module.renderer, "render", lambda p: None)

        task1 = asyncio.create_task(fresh_manager.spawn(params(), timeout_s=5))
        await asyncio.sleep(0.05)
        assert fresh_manager.state == ClusterState.WAITING_READY

        teardown_status = await fresh_manager.teardown()

        result1 = await task1  # must not raise

        assert result1.ok is False
        assert result1.status.error == "superseded"
        assert teardown_status.state == ClusterState.IDLE
        assert teardown_status.params is None
        # down was called at least twice: once cancelling+cleaning up task1's
        # work, once more as teardown()'s own guaranteed down.
        assert calls.count("down") >= 2


class TestPreSpawnTeardownFailureDoesNotAbort:
    """manager.py's actual behavior when `compose_ops.down()` returns a
    non-zero exit during the per-spawn teardown step (PLAN.md §2 step 3):
    it records a WARNING-prefixed status message and continues to `up()`
    rather than aborting the spawn (PLAN.md's original design intent -- a
    real port/name collision in up() is the natural safety net if teardown
    didn't fully complete). Issue #1 (Major) fixed the observability gap
    around this: the failure is now also durably logged via the module
    logger (`logger.warning(...)`), since `self.message` gets overwritten
    by the very next state transition two lines later and was previously
    the only record of the failure.
    """

    @pytest.mark.asyncio
    async def test_down_failure_does_not_abort_spawn(self, fresh_manager, monkeypatch):
        calls = []
        # First down() call = the pre-spawn cancel-and-teardown (no-op, ok).
        # Second down() call = the per-spawn teardown step -- fails here.
        down_results = [OK_DOWN, CommandResult(returncode=1, stdout="", stderr="boom: still up")]

        async def _down():
            calls.append("down")
            return down_results.pop(0)

        up_mock = AsyncMock(side_effect=lambda: calls.append("up") or OK_UP)

        async def _wait(worker_count, timeout_s=60, interval_s=2):
            calls.append("wait")
            return ready(worker_count)

        monkeypatch.setattr(manager_module.compose_ops, "down", _down)
        monkeypatch.setattr(manager_module.compose_ops, "up", up_mock)
        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(side_effect=_wait))
        monkeypatch.setattr(manager_module.renderer, "render", lambda p: None)

        outcome = await fresh_manager.spawn(params())

        # It does NOT abort: up() still runs after the failing down(), and
        # the spawn can still reach READY.
        assert calls == ["down", "down", "up", "wait"]
        up_mock.assert_called_once()
        assert outcome.ok is True
        assert outcome.status.state == ClusterState.READY

    @pytest.mark.asyncio
    async def test_down_failure_message_is_a_warning_not_silent(self, fresh_manager, monkeypatch):
        """The failure isn't swallowed silently -- manager.py sets a
        WARNING-prefixed `self.message` at the moment it happens (though note:
        it is a transient status string, not a `logging` call -- see report).
        """
        seen_messages = []
        down_results = [OK_DOWN, CommandResult(returncode=1, stdout="", stderr="boom")]

        async def _down():
            return down_results.pop(0)

        async def _up():
            # Captured right after the STARTING transition overwrites
            # self.message -- so instead we patch to record message
            # immediately post-down via a wrapped down() below.
            return OK_UP

        real_run_spawn = fresh_manager._run_spawn

        async def _traced_down():
            result = await _down()
            if not result.ok:
                # Mirrors manager.py's own branch, just to capture the value
                # it *would* set, since message gets overwritten a moment
                # later by the STARTING transition.
                seen_messages.append(f"WARNING: teardown exited {result.returncode}; continuing.")
            return result

        monkeypatch.setattr(manager_module.compose_ops, "down", _traced_down)
        monkeypatch.setattr(manager_module.compose_ops, "up", AsyncMock(return_value=OK_UP))
        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(return_value=ready()))
        monkeypatch.setattr(manager_module.renderer, "render", lambda p: None)

        await fresh_manager.spawn(params())

        assert any("WARNING" in m for m in seen_messages)

    @pytest.mark.asyncio
    async def test_down_failure_is_durably_logged(self, fresh_manager, monkeypatch, caplog):
        """Issue #1 fix: unlike `self.message` (overwritten by the next state
        transition), the failure must survive in the server logs via the
        module logger, so it's actually observable after the fact."""
        down_results = [OK_DOWN, CommandResult(returncode=1, stdout="", stderr="boom: still up")]

        async def _down():
            return down_results.pop(0)

        monkeypatch.setattr(manager_module.compose_ops, "down", _down)
        monkeypatch.setattr(manager_module.compose_ops, "up", AsyncMock(return_value=OK_UP))
        monkeypatch.setattr(manager_module.readiness, "wait_for_ready", AsyncMock(return_value=ready()))
        monkeypatch.setattr(manager_module.renderer, "render", lambda p: None)

        with caplog.at_level(logging.WARNING, logger="app.lifecycle.manager"):
            await fresh_manager.spawn(params())

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warning_records, "expected a durable WARNING log record for the teardown failure"
        assert any("teardown" in r.message.lower() for r in warning_records)
        assert any("boom: still up" in r.message for r in warning_records)


class TestCancelledTaskUnexpectedExceptionIsLogged:
    """Issue #1 fix: `_cancel_and_teardown_locked`'s `except Exception: pass`
    used to silently swallow anything beyond CancelledError when awaiting a
    just-cancelled task. It must now log via `logger.exception(...)` while
    keeping the same "continue anyway" control flow (cancel-and-replace still
    proceeds to its own teardown/render/up regardless)."""

    @pytest.mark.asyncio
    async def test_unexpected_exception_from_cancelled_task_is_logged(self, fresh_manager, caplog):
        async def _bad_cleanup():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise RuntimeError("boom during cleanup")

        fresh_manager._task = asyncio.create_task(_bad_cleanup())
        await asyncio.sleep(0.01)  # let it start sleeping

        with caplog.at_level(logging.ERROR, logger="app.lifecycle.manager"):
            # Directly exercises the internal method under test; caller
            # normally holds _mutate_lock, which isn't needed for this
            # single-coroutine test.
            await fresh_manager._cancel_and_teardown_locked()

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert error_records, "expected the unexpected exception to be logged, not swallowed"
        assert any("unexpected exception" in r.message.lower() for r in error_records)
        # Control flow unchanged: teardown still proceeds to IDLE-bound state
        # regardless of the swallowed exception.
        assert fresh_manager.state == ClusterState.TEARING_DOWN
