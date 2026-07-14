"""Tests for app/monitoring/collector.py's lifecycle gating (ADR D-B,
R-Dash-3) -- the piece most likely to leak resources if done carelessly.

Mirrors `tests/unit/test_manager.py`'s approach: `docker_stats.sample` and
the `app_client` REST calls are mocked at their module boundary so these run
in milliseconds with no Docker/Spark involved. `app.lifecycle.manager.manager`
is the real module-level singleton (reset by the autouse
`_reset_singleton_manager` fixture in `tests/conftest.py`, same as the rest
of the suite) since `collector.py` imports and reads that exact singleton
object, not an injectable one.
"""
from __future__ import annotations

import asyncio

import pytest

from app import config
from app.lifecycle.manager import ClusterState, manager
from app.lifecycle.renderer import ClusterParams
from app.monitoring import docker_stats
from app.monitoring.collector import DashboardCollector
from app.monitoring.docker_stats import ContainerStat
from app.spark_api import app_client


def _make_ready(worker_count: int = 1) -> None:
    manager.state = ClusterState.READY
    manager.params = ClusterParams(worker_count=worker_count, worker_cores=1, worker_memory_gb=1)


@pytest.fixture(autouse=True)
def _fast_cadence(monkeypatch):
    """Collector cadence sped way up so lifecycle tests run in milliseconds
    rather than waiting out the real ~2s cycle."""
    monkeypatch.setattr(config, "DASHBOARD_COLLECTOR_INTERVAL_S", 0.01)


@pytest.fixture(autouse=True)
def _no_real_docker_or_spark(monkeypatch):
    """No test in this file should ever touch a real subprocess or REST
    endpoint -- every collector cycle's data sources are mocked out."""
    monkeypatch.setattr(docker_stats, "sample", _fake_sample)
    monkeypatch.setattr(app_client, "fetch_current_app_id", lambda timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_executors", lambda app_id, timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_stages", lambda app_id, timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: None)


async def _fake_sample(cpu_limits, timeout_s=3.0):
    return [
        ContainerStat(
            name="spark-master",
            cpu_pct=10.0,
            mem_used_bytes=100 * 1024**2,
            mem_limit_bytes=1024**3,
            net_rx_bytes=0,
            net_tx_bytes=0,
            block_read_bytes=0,
            block_write_bytes=0,
        )
    ]


async def _wait_until(predicate, timeout_s: float = 2.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


@pytest.fixture
def fresh_collector():
    return DashboardCollector()


class TestSubscribeGating:
    @pytest.mark.asyncio
    async def test_subscribe_when_not_ready_does_not_start_sampling(self, fresh_collector):
        manager.state = ClusterState.IDLE

        queue = await fresh_collector.subscribe()

        assert fresh_collector.subscriber_count() == 1
        assert fresh_collector.is_running() is False
        fresh_collector.unsubscribe(queue)

    @pytest.mark.asyncio
    async def test_subscribe_when_ready_starts_sampling(self, fresh_collector):
        _make_ready()

        queue = await fresh_collector.subscribe()
        try:
            await _wait_until(lambda: fresh_collector.is_running())
            snapshot = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert snapshot.cluster_active is True
        finally:
            fresh_collector.unsubscribe(queue)


class TestUnsubscribeStopsSampling:
    @pytest.mark.asyncio
    async def test_last_subscriber_leaving_cancels_the_task(self, fresh_collector):
        _make_ready()

        queue = await fresh_collector.subscribe()
        await _wait_until(lambda: fresh_collector.is_running())

        fresh_collector.unsubscribe(queue)

        await _wait_until(lambda: not fresh_collector.is_running())
        assert fresh_collector.subscriber_count() == 0

    @pytest.mark.asyncio
    async def test_one_of_two_subscribers_leaving_keeps_it_running(self, fresh_collector):
        _make_ready()

        q1 = await fresh_collector.subscribe()
        q2 = await fresh_collector.subscribe()
        await _wait_until(lambda: fresh_collector.is_running())

        fresh_collector.unsubscribe(q1)
        # Give the loop a moment to have a chance to (wrongly) stop, if it were going to.
        await asyncio.sleep(0.05)
        assert fresh_collector.is_running() is True

        fresh_collector.unsubscribe(q2)
        await _wait_until(lambda: not fresh_collector.is_running())


class TestClusterTeardownStopsSampling:
    @pytest.mark.asyncio
    async def test_cluster_leaving_ready_stops_the_loop_and_broadcasts_inactive(self, fresh_collector):
        """R-Dash-3: a stray collector polling a torn-down stack wastes
        cycles and logs errors -- the loop must notice `manager.state` is no
        longer READY and stop itself (not just idle), after one final
        broadcast so the client doesn't see frozen last-known values
        (US-5.1 c3)."""
        _make_ready()

        queue = await fresh_collector.subscribe()
        await _wait_until(lambda: fresh_collector.is_running())
        # Drain whatever's already queued from real sampling cycles so the
        # next item we read is unambiguously the post-teardown one.
        while not queue.empty():
            queue.get_nowait()

        manager.state = ClusterState.IDLE  # cluster torn down out from under the collector

        final_snapshot = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert final_snapshot.cluster_active is False

        await _wait_until(lambda: not fresh_collector.is_running())
        fresh_collector.unsubscribe(queue)

    @pytest.mark.asyncio
    async def test_ensure_running_restarts_sampling_once_ready_again(self, fresh_collector):
        """A client that opened the dashboard before a cluster existed (or
        while it was tearing down) must start seeing data once the cluster
        becomes READY, without reconnecting -- `ensure_running()` is called
        on every stream tick for exactly this reason."""
        manager.state = ClusterState.IDLE
        queue = await fresh_collector.subscribe()
        assert fresh_collector.is_running() is False

        _make_ready()
        fresh_collector.ensure_running()

        await _wait_until(lambda: fresh_collector.is_running())
        snapshot = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert snapshot.cluster_active is True
        fresh_collector.unsubscribe(queue)


class TestNoSubscribersMeansNoSampling:
    @pytest.mark.asyncio
    async def test_ensure_running_is_a_noop_with_zero_subscribers(self, fresh_collector):
        _make_ready()
        fresh_collector.ensure_running()
        await asyncio.sleep(0.05)
        assert fresh_collector.is_running() is False
