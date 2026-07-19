"""Tests for app/lifecycle/compose_ops.py — issue #3 (Blocker) regression.

Prior to the fix, `_run()` awaited `proc.communicate()` with no
`except asyncio.CancelledError` and no `proc.kill()`/`proc.terminate()`
anywhere in the file. When the asyncio *task* wrapping `_run()` was cancelled
(e.g. by `manager._cancel_and_teardown_locked()` cancelling an in-flight
spawn task while it's inside `compose_ops.up()`), the underlying OS process
(`docker compose up -d`) was left running detached — able to go on
creating/starting containers after the cancelling caller had already issued
its own fresh `down()`/`up()`. This undermines PLAN.md §6/R4's "awaited down
before up" guarantee, since that only protects against the *new* task's own
subprocess, not a leaked one from a cancelled prior task.

These tests exercise `_run()`'s actual cancellation path (not a
`compose_ops`-boundary mock, since that wouldn't touch this code at all): a
fake process object stands in for `asyncio.create_subprocess_exec`'s return
value, `communicate()` never resolves on its own, and we assert `.kill()` +
`.wait()` (to reap the zombie) are both called when the awaiting task is
cancelled, and that the cancellation still propagates to the caller.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.lifecycle import compose_ops


def _make_fake_proc():
    """A fake subprocess whose `communicate()` never resolves on its own —
    standing in for a real, still-running `docker compose` process."""
    fake_proc = MagicMock()
    fake_proc.returncode = None
    communicate_started = asyncio.Event()

    async def _communicate():
        communicate_started.set()
        await asyncio.sleep(10)  # never completes unless cancelled
        return b"", b""  # pragma: no cover - unreachable in these tests

    fake_proc.communicate = AsyncMock(side_effect=_communicate)
    fake_proc.kill = MagicMock()

    async def _wait():
        fake_proc.returncode = -9
        return -9

    fake_proc.wait = AsyncMock(side_effect=_wait)
    return fake_proc, communicate_started


class TestCancellationKillsAndReapsTheProcess:
    @pytest.mark.asyncio
    async def test_run_kills_process_when_task_is_cancelled(self, monkeypatch):
        fake_proc, communicate_started = _make_fake_proc()

        async def _fake_create_subprocess_exec(*args, **kwargs):
            return fake_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

        task = asyncio.create_task(compose_ops._run("docker", "compose", "up", "-d"))
        await communicate_started.wait()  # ensure we're genuinely inside proc.communicate()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The critical regression assertion: the leaked OS process must be
        # killed, not left running detached.
        fake_proc.kill.assert_called_once()
        # ...and reaped (awaited) so it doesn't linger as a zombie.
        fake_proc.wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_does_not_kill_an_already_finished_process(self, monkeypatch):
        """If the process already exited by the time cancellation is
        observed (returncode set), `_run()` must not call kill() on a
        process that's already gone."""
        fake_proc, communicate_started = _make_fake_proc()

        async def _communicate_then_finish():
            communicate_started.set()
            await asyncio.sleep(10)
            return b"", b""  # pragma: no cover

        fake_proc.communicate = AsyncMock(side_effect=_communicate_then_finish)

        async def _fake_create_subprocess_exec(*args, **kwargs):
            return fake_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

        task = asyncio.create_task(compose_ops._run("docker", "compose", "up", "-d"))
        await communicate_started.wait()

        # Simulate the process having already exited right before cancellation
        # is delivered/observed.
        fake_proc.returncode = 0

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        fake_proc.kill.assert_not_called()

    @pytest.mark.asyncio
    async def test_up_propagates_cancellation_and_kills_subprocess(self, monkeypatch):
        """End-to-end through the public `up()` wrapper (what `manager.py`
        actually calls), not just the private `_run()` helper."""
        fake_proc, communicate_started = _make_fake_proc()

        async def _fake_create_subprocess_exec(*args, **kwargs):
            return fake_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

        task = asyncio.create_task(compose_ops.up())
        await communicate_started.wait()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        fake_proc.kill.assert_called_once()
        fake_proc.wait.assert_awaited_once()


class TestNormalCompletionUnaffected:
    @pytest.mark.asyncio
    async def test_run_returns_command_result_when_not_cancelled(self, monkeypatch):
        fake_proc = MagicMock()
        fake_proc.returncode = 0
        fake_proc.communicate = AsyncMock(return_value=(b"out", b"err"))

        async def _fake_create_subprocess_exec(*args, **kwargs):
            return fake_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

        result = await compose_ops._run("docker", "compose", "down")

        assert result.ok is True
        assert result.stdout == "out"
        assert result.stderr == "err"
        fake_proc.kill.assert_not_called()


class TestRunningOwner:
    """`running_owner()` — issue #38 ownership guard
    (docs/architecture/worktree-cluster-isolation.md). Mocks `_run()` at the
    module boundary rather than real subprocesses/Docker."""

    @pytest.mark.asyncio
    async def test_returns_none_when_nothing_running(self, monkeypatch):
        ps_result = compose_ops.CommandResult(returncode=0, stdout="", stderr="")
        monkeypatch.setattr(compose_ops, "_run", AsyncMock(return_value=ps_result))

        assert await compose_ops.running_owner() is None

    @pytest.mark.asyncio
    async def test_returns_normalized_working_dir_when_running(self, monkeypatch):
        ps_result = compose_ops.CommandResult(returncode=0, stdout="abc123\n", stderr="")
        # Windows-style backslash path, as observed live per the ADR's
        # R-WT-1 empirical caveat.
        inspect_result = compose_ops.CommandResult(
            returncode=0, stdout="C:\\repo\\worktrees\\A\\compose\\rendered\n", stderr=""
        )
        run_mock = AsyncMock(side_effect=[ps_result, inspect_result])
        monkeypatch.setattr(compose_ops, "_run", run_mock)

        owner = await compose_ops.running_owner()

        assert owner == compose_ops.config.norm_path("C:\\repo\\worktrees\\A\\compose\\rendered")
        # docker ps then docker inspect on the first id found.
        assert run_mock.await_args_list[0].args[:2] == ("docker", "ps")
        assert run_mock.await_args_list[1].args[:2] == ("docker", "inspect")
        assert run_mock.await_args_list[1].args[2] == "abc123"

    @pytest.mark.asyncio
    async def test_returns_none_when_ps_fails(self, monkeypatch):
        ps_result = compose_ops.CommandResult(returncode=1, stdout="", stderr="daemon unreachable")
        monkeypatch.setattr(compose_ops, "_run", AsyncMock(return_value=ps_result))

        assert await compose_ops.running_owner() is None

    @pytest.mark.asyncio
    async def test_returns_none_when_inspect_fails(self, monkeypatch):
        ps_result = compose_ops.CommandResult(returncode=0, stdout="abc123\n", stderr="")
        inspect_result = compose_ops.CommandResult(returncode=1, stdout="", stderr="no such object")
        monkeypatch.setattr(compose_ops, "_run", AsyncMock(side_effect=[ps_result, inspect_result]))

        assert await compose_ops.running_owner() is None

    @pytest.mark.asyncio
    async def test_never_raises_when_docker_missing(self, monkeypatch):
        async def _raise(*args, **kwargs):
            raise FileNotFoundError("docker not found")

        monkeypatch.setattr(compose_ops, "_run", _raise)

        assert await compose_ops.running_owner() is None
