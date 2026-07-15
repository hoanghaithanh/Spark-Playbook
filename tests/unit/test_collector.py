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
import time

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
    monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
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


class TestUnsubscribeAndSubscribeRaces:
    """Scenarios called out explicitly as the highest-risk area for this
    module: double unsubscribe (a browser tab closing then a network blip
    both triggering disconnect) and a subscribe landing in the narrow window
    between the last unsubscribe's `task.cancel()` and that cancellation
    actually being delivered."""

    @pytest.mark.asyncio
    async def test_unsubscribe_called_twice_for_same_queue_is_safe(self, fresh_collector):
        _make_ready()
        queue = await fresh_collector.subscribe()
        await _wait_until(lambda: fresh_collector.is_running())

        fresh_collector.unsubscribe(queue)
        fresh_collector.unsubscribe(queue)  # duplicate call -- must not raise

        await _wait_until(lambda: not fresh_collector.is_running())
        assert fresh_collector.subscriber_count() == 0

    @pytest.mark.asyncio
    async def test_resubscribe_during_pending_cancellation_eventually_recovers(self, fresh_collector):
        """`unsubscribe()` calls `task.cancel()` synchronously but does not
        await the task's actual death. If a new `subscribe()` lands before
        that cancellation is delivered, `ensure_running()` sees `self._task`
        as not-None/not-done and no-ops -- then the old task dies from the
        pending cancellation anyway, orphaning the new subscriber until
        something calls `ensure_running()` again. `dashboard.py`'s SSE route
        calls `ensure_running()` on every ~1s stream-poll tick specifically
        to bound this window -- this test simulates that external retry and
        asserts the collector does recover, rather than staying dead forever."""
        _make_ready()
        q1 = await fresh_collector.subscribe()
        await _wait_until(lambda: fresh_collector.is_running())

        fresh_collector.unsubscribe(q1)  # cancels the task; not yet delivered
        q2 = await fresh_collector.subscribe()  # races the pending cancellation

        async def _poke_until_running():
            for _ in range(200):
                fresh_collector.ensure_running()
                if fresh_collector.is_running():
                    return
                await asyncio.sleep(0.01)

        await _poke_until_running()
        assert fresh_collector.is_running() is True, (
            "collector never recovered for the new subscriber after the "
            "subscribe/cancel race -- without an external caller repeatedly "
            "re-invoking ensure_running(), this subscriber would be "
            "permanently starved"
        )
        fresh_collector.unsubscribe(q2)


class TestCollectOnceDoesNotBlockTheEventLoop:
    @pytest.mark.asyncio
    async def test_synchronous_app_client_calls_do_not_starve_other_tasks(self, fresh_collector, monkeypatch):
        """`collect_once()` calls `app_client.fetch_current_app_id()` /
        `fetch_executors()` / `fetch_stages()` directly (no `await`, no
        `asyncio.to_thread`) -- those are synchronous, blocking
        `urllib.request.urlopen()` calls under the hood. Since this whole app
        is single-process/single-event-loop, a slow or unreachable `:4040`
        (R-Dash-5's known failure mode) blocks *every* concurrently running
        coroutine -- not just the dashboard -- for the duration of the call,
        repeated every ~2s for as long as any dashboard client is connected.
        This test proves the starvation directly: a concurrent ticker
        coroutine should keep making progress while `collect_once()` runs."""
        monkeypatch.setattr(docker_stats, "sample", _fake_sample)

        def _slow_blocking_fetch(*args, **kwargs):
            time.sleep(0.3)  # simulates a slow/degraded driver
            return None

        monkeypatch.setattr(app_client, "resolve_current_app", _slow_blocking_fetch)
        monkeypatch.setattr(app_client, "fetch_executors", lambda *a, **kw: None)
        monkeypatch.setattr(app_client, "fetch_stages", lambda *a, **kw: None)
        _make_ready()

        tick_count = 0

        async def _ticker():
            nonlocal tick_count
            while True:
                await asyncio.sleep(0.01)
                tick_count += 1

        ticker_task = asyncio.create_task(_ticker())
        await asyncio.sleep(0.03)

        await fresh_collector.collect_once()

        ticker_task.cancel()
        try:
            await ticker_task
        except asyncio.CancelledError:
            pass

        assert tick_count >= 15, (
            f"event loop was starved during collect_once() (only {tick_count} "
            "ticks recorded across a ~0.33s window that includes a 0.3s "
            "synchronous fetch) -- app_client's blocking urllib calls freeze "
            "the whole single-process app, not just the dashboard's own data"
        )


def _fake_sample_cpu_imbalance(cpu_limits, timeout_s=3.0):
    async def _inner():
        return [
            ContainerStat(
                name="spark-master", cpu_pct=5.0, mem_used_bytes=10 * 1024**2, mem_limit_bytes=1024**3,
                net_rx_bytes=0, net_tx_bytes=0, block_read_bytes=0, block_write_bytes=0,
            ),
            ContainerStat(
                name="spark-worker-1", cpu_pct=95.0, mem_used_bytes=900 * 1024**2, mem_limit_bytes=1024**3,
                net_rx_bytes=0, net_tx_bytes=0, block_read_bytes=0, block_write_bytes=0,
            ),
            ContainerStat(
                name="spark-worker-2", cpu_pct=10.0, mem_used_bytes=100 * 1024**2, mem_limit_bytes=1024**3,
                net_rx_bytes=0, net_tx_bytes=0, block_read_bytes=0, block_write_bytes=0,
            ),
            ContainerStat(
                name="spark-driver", cpu_pct=5.0, mem_used_bytes=100 * 1024**2, mem_limit_bytes=1024**3,
                net_rx_bytes=0, net_tx_bytes=0, block_read_bytes=0, block_write_bytes=0,
            ),
        ]
    return _inner()


class TestAlertTitleFormatting:
    @pytest.mark.asyncio
    async def test_alert_title_is_readable_when_flagged_via_cpu_imbalance_not_skew(
        self, fresh_collector, monkeypatch
    ):
        """`collect_once()` builds the banner title as
        `flag_reason.split(':')[0]`, which assumes every `flag_reason` has a
        colon. `node_skew_reasons()`'s strings do ("Data skew: handling ..."),
        but `node_imbalance_reasons()`'s strings don't ("CPU saturated (95%)
        while spark-worker-2 is idle (10%)") -- so when a node is flagged
        purely by CPU imbalance (no job/skew involved at all), `split(':')[0]`
        returns the *entire* sentence, and the title ends up dumping the whole
        detail text a second time ahead of its own "detected on <name>" tail,
        producing a garbled banner instead of a short factual title."""
        monkeypatch.setattr(docker_stats, "sample", _fake_sample_cpu_imbalance)
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: None)
        _make_ready(worker_count=2)

        snapshot = await fresh_collector.collect_once()

        assert snapshot.has_alert is True
        assert "idle" not in snapshot.alert_title, (
            f"alert title embeds the full imbalance detail sentence instead "
            f"of a short category label: {snapshot.alert_title!r}"
        )


def _skewed_task(idx: int, node: str, size_bytes: int) -> dict:
    return {
        "taskId": idx,
        "index": idx,
        "attempt": 0,
        "executorId": "0" if node == "spark-worker-1" else "1",
        "host": node,
        "status": "SUCCESS",
        "duration": 1000,
        "taskMetrics": {
            "inputMetrics": {"bytesRead": size_bytes, "recordsRead": 10},
            "shuffleReadMetrics": {"localBytesRead": 0, "remoteBytesRead": 0},
            "shuffleWriteMetrics": {"bytesWritten": 0},
        },
    }


class TestSignalCardDeepLinks:
    """Issue #20: every `SignalCard` used to hardcode `deep_link=None`, so
    US-5.6's "deep link into the real Spark UI" criterion was never actually
    exercised even though `app_client.stage_ui_url()` already existed for
    exactly this purpose (already used by `annotation.py`)."""

    @pytest.mark.asyncio
    async def test_signal_cards_link_to_the_current_stage_in_the_real_spark_ui(
        self, fresh_collector, monkeypatch
    ):
        app_ref = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        monkeypatch.setattr(docker_stats, "sample", _fake_sample)
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: app_ref)
        monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            app_client,
            "fetch_stages",
            lambda app, timeout_s=3.0: [
                {"stageId": 3, "attemptId": 0, "status": "ACTIVE", "numTasks": 2, "executorRunTime": 100}
            ],
        )
        monkeypatch.setattr(
            app_client,
            "fetch_task_list",
            lambda *a, **kw: [
                _skewed_task(0, "spark-worker-1", 100),
                _skewed_task(1, "spark-worker-1", 100),
                _skewed_task(2, "spark-worker-2", 5_000_000),
            ],
        )
        _make_ready(worker_count=2)

        snapshot = await fresh_collector.collect_once()

        assert snapshot.signal_cards, "expected at least one signal card (skew was seeded)"
        expected_link = app_client.stage_ui_url(app_ref, 3, 0)
        for card in snapshot.signal_cards:
            assert card.deep_link == expected_link, f"card {card.category!r} has no working deep link"

    @pytest.mark.asyncio
    async def test_no_current_stage_means_no_deep_link_not_a_crash(self, fresh_collector, monkeypatch):
        app_ref = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        monkeypatch.setattr(docker_stats, "sample", _fake_sample)
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: app_ref)
        monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: None)
        _make_ready()

        snapshot = await fresh_collector.collect_once()
        assert snapshot is not None  # no current stage -> no job -> no signal cards; must not raise


class TestSamplingLoopExceptionResilience:
    """code-reviewer note: `_run()`'s `except Exception: logger.exception(...);
    continue` is the right behavior (keep sampling for the next cycle rather
    than dying on one bad cycle) but was previously untested."""

    @pytest.mark.asyncio
    async def test_a_single_bad_cycle_does_not_kill_the_collector(self, fresh_collector, monkeypatch):
        call_count = 0
        real_fake_sample = _fake_sample

        async def _flaky_sample(cpu_limits, timeout_s=3.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated docker stats failure")
            return await real_fake_sample(cpu_limits, timeout_s=timeout_s)

        monkeypatch.setattr(docker_stats, "sample", _flaky_sample)
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: None)
        _make_ready()

        queue = await fresh_collector.subscribe()
        try:
            await _wait_until(lambda: fresh_collector.is_running())
            snapshot = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert snapshot.cluster_active is True
            assert fresh_collector.is_running() is True
            assert call_count >= 2, "collector never retried after the first cycle's exception"
        finally:
            fresh_collector.unsubscribe(queue)
