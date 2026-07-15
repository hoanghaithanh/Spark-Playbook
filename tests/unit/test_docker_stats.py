"""Tests for app/monitoring/docker_stats.py (ADR D-C) -- size/pair parsing
and `sample()`'s CPU% normalization against configured cpu limits.

`asyncio.create_subprocess_exec` is mocked so these run with no real Docker
involved, mirroring `tests/unit/test_compose_ops.py`'s convention for the
existing subprocess-wrapper module.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from app.monitoring import docker_stats


class TestParseSize:
    def test_binary_mebibytes(self):
        assert docker_stats._parse_size("15.5MiB") == round(15.5 * 1024**2)

    def test_decimal_kilobytes(self):
        assert docker_stats._parse_size("1.2kB") == round(1.2 * 1000)

    def test_zero_bytes(self):
        assert docker_stats._parse_size("0B") == 0

    def test_gibibytes(self):
        assert docker_stats._parse_size("1.9GiB") == round(1.9 * 1024**3)

    def test_unparsable_returns_none(self):
        assert docker_stats._parse_size("not-a-size") is None

    def test_unknown_unit_returns_none(self):
        assert docker_stats._parse_size("5.0XB") is None


class TestParsePair:
    def test_splits_used_and_limit(self):
        used, limit = docker_stats._parse_pair("15.5MiB / 1.943GiB")
        assert used == round(15.5 * 1024**2)
        assert limit == round(1.943 * 1024**3)

    def test_malformed_pair_returns_none_none(self):
        assert docker_stats._parse_pair("not a pair at all") == (None, None)


class _FakeProc:
    def __init__(self, stdout: bytes, returncode: int = 0):
        self._stdout = stdout
        self.returncode = returncode

    async def communicate(self):
        return self._stdout, b""

    def kill(self):
        pass

    async def wait(self):
        pass


class TestListContainerIds:
    @pytest.mark.asyncio
    async def test_returns_ids_one_per_line(self, monkeypatch):
        proc = _FakeProc(b"abc123\ndef456\n")
        monkeypatch.setattr(
            asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)
        )
        ids = await docker_stats.list_container_ids()
        assert ids == ["abc123", "def456"]

    @pytest.mark.asyncio
    async def test_empty_output_returns_empty_list(self, monkeypatch):
        proc = _FakeProc(b"")
        monkeypatch.setattr(
            asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)
        )
        assert await docker_stats.list_container_ids() == []

    @pytest.mark.asyncio
    async def test_nonzero_exit_returns_empty_list_not_raises(self, monkeypatch):
        proc = _FakeProc(b"", returncode=1)
        monkeypatch.setattr(
            asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)
        )
        assert await docker_stats.list_container_ids() == []


def _ndjson(*rows: dict) -> bytes:
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")


class TestSample:
    @pytest.mark.asyncio
    async def test_normalizes_cpu_against_container_limit(self, monkeypatch):
        # docker ps -> one id; docker stats -> one row using 100% of ONE
        # host core on a container limited to 2 cores -> normalized 50%.
        ps_proc = _FakeProc(b"abc123\n")
        stats_row = {
            "Name": "spark-worker-1",
            "CPUPerc": "100.00%",
            "MemUsage": "500MiB / 2GiB",
            "NetIO": "1kB / 2kB",
            "BlockIO": "3kB / 4kB",
        }
        stats_proc = _FakeProc(_ndjson(stats_row))

        calls = []

        async def fake_exec(*args, **kwargs):
            calls.append(args)
            return ps_proc if args[1] == "ps" else stats_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        result = await docker_stats.sample({"spark-worker-1": 2.0})
        assert len(result) == 1
        stat = result[0]
        assert stat.name == "spark-worker-1"
        assert stat.cpu_pct == 50.0
        assert stat.mem_used_bytes == round(500 * 1024**2)
        assert stat.mem_limit_bytes == round(2 * 1024**3)

    @pytest.mark.asyncio
    async def test_no_containers_short_circuits_without_calling_stats(self, monkeypatch):
        ps_proc = _FakeProc(b"")
        calls = []

        async def fake_exec(*args, **kwargs):
            calls.append(args)
            return ps_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        result = await docker_stats.sample({})
        assert result == []
        assert len(calls) == 1  # only the `docker ps` call, never `docker stats`

    @pytest.mark.asyncio
    async def test_missing_cpu_limit_leaves_cpu_pct_none(self, monkeypatch):
        ps_proc = _FakeProc(b"abc123\n")
        stats_row = {
            "Name": "spark-master",
            "CPUPerc": "10.00%",
            "MemUsage": "100MiB / 1GiB",
            "NetIO": "0B / 0B",
            "BlockIO": "0B / 0B",
        }
        stats_proc = _FakeProc(_ndjson(stats_row))

        async def fake_exec(*args, **kwargs):
            return ps_proc if args[1] == "ps" else stats_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        result = await docker_stats.sample({})  # no limit supplied for spark-master
        assert result[0].cpu_pct is None

    @pytest.mark.asyncio
    async def test_malformed_json_line_is_skipped_not_raised(self, monkeypatch):
        ps_proc = _FakeProc(b"abc123\n")
        stats_proc = _FakeProc(b"not json at all\n")

        async def fake_exec(*args, **kwargs):
            return ps_proc if args[1] == "ps" else stats_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        result = await docker_stats.sample({"spark-master": 1.0})
        assert result == []

    @pytest.mark.asyncio
    async def test_captures_container_id_from_stats_output(self, monkeypatch):
        """Needed by `container_ip_map()`'s IP-join fallback (found by
        actually running against a real cluster -- see that function's
        docstring): `docker stats`' own "ID" field is reused so no extra
        `docker ps` call is needed to resolve it."""
        ps_proc = _FakeProc(b"abc123\n")
        stats_row = {
            "ID": "abc123",
            "Name": "spark-worker-1",
            "CPUPerc": "10.00%",
            "MemUsage": "100MiB / 1GiB",
            "NetIO": "0B / 0B",
            "BlockIO": "0B / 0B",
        }
        stats_proc = _FakeProc(_ndjson(stats_row))

        async def fake_exec(*args, **kwargs):
            return ps_proc if args[1] == "ps" else stats_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        result = await docker_stats.sample({"spark-worker-1": 1.0})
        assert result[0].container_id == "abc123"


class _HangingProc:
    """A fake subprocess whose `communicate()` never resolves on its own --
    used to put `_run()` in a controlled, suspended `await` state so a test
    can cancel the awaiting task from outside and observe whether the
    subprocess itself gets cleaned up."""

    def __init__(self):
        self.returncode = None
        self.killed = False
        self.waited = False

    async def communicate(self):
        await asyncio.Event().wait()  # only ever resolves via cancellation

    def kill(self):
        self.killed = True

    async def wait(self):
        self.waited = True


class TestRunSubprocessCleanupOnCancel:
    @pytest.mark.asyncio
    async def test_cancelling_run_kills_the_subprocess_not_just_reraises(self, monkeypatch):
        """Regression test mirroring Phase 1 issue #3 (the `lifecycle/manager.py`
        subprocess-leak bug), found by re-reading `_run()`'s exception handling
        rather than by grep: on a real `asyncio.TimeoutError`, `_run()`
        correctly calls `proc.kill()` / `proc.wait()` before returning -- but
        on `asyncio.CancelledError` (e.g. the dashboard collector's own
        background task being cancelled by `DashboardCollector.unsubscribe()`
        while mid-`docker stats`/`docker inspect`, which is a routine, frequent
        event -- the last dashboard tab closing while a ~2s docker call is in
        flight), the `except (OSError, asyncio.CancelledError): raise` branch
        re-raises immediately without ever killing the child process. The
        `docker` CLI process is left running as an orphan of the app's process,
        never reaped -- a real resource leak on an entirely normal code path,
        not just a rare edge case."""
        proc = _HangingProc()

        async def fake_exec(*args, **kwargs):
            return proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

        task = asyncio.create_task(docker_stats._run("docker", "stats", timeout_s=5.0))
        await asyncio.sleep(0.05)  # let it actually reach `await proc.communicate()`
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert proc.killed, (
            "the subprocess was left running after the awaiting task was "
            "cancelled -- `_run()` must kill()/wait() it on CancelledError "
            "the same way it already does on TimeoutError"
        )


class TestContainerIpMap:
    @pytest.mark.asyncio
    async def test_maps_ip_to_container_name(self, monkeypatch):
        proc = _FakeProc(b"/spark-worker-1|172.19.0.3\n/spark-worker-2|172.19.0.4\n")
        monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))

        result = await docker_stats.container_ip_map(["abc123", "def456"])
        assert result == {"172.19.0.3": "spark-worker-1", "172.19.0.4": "spark-worker-2"}

    @pytest.mark.asyncio
    async def test_empty_ids_short_circuits_without_a_call(self, monkeypatch):
        mock = AsyncMock()
        monkeypatch.setattr(asyncio, "create_subprocess_exec", mock)
        assert await docker_stats.container_ip_map([]) == {}
        mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_line_is_skipped(self, monkeypatch):
        proc = _FakeProc(b"no-pipe-here\n/spark-master|172.19.0.2\n")
        monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))

        result = await docker_stats.container_ip_map(["abc123"])
        assert result == {"172.19.0.2": "spark-master"}
