"""Tests for `collector.py`'s Kafka observability wiring (ADR D-MBK5,
US-MBK2) -- non-Kafka spawns must shell out zero times, a live spawn must
populate `Snapshot.kafka`, and the heavier CLI pull must only run on the
sub-cadence tick, not every 2s base cycle.

`kafka_stats`'s shellout functions are mocked at their module boundary
(mirrors `test_collector.py`'s convention for `docker_stats`/`app_client`).

Issue #57 finding 1: the sub-cadence CLI pull now runs as a detached
`asyncio.create_task` instead of being awaited inline in `collect_once()`
(so the base-cadence broadcast never blocks on it) -- `_await_kafka_refresh`
below explicitly awaits that background task where a test needs the CLI
data it produces, mirroring how the real event loop eventually gets around
to running it.
"""
from __future__ import annotations

import asyncio

import pytest

from app import config
from app.lifecycle.manager import ClusterState, manager
from app.lifecycle.renderer import ClusterParams
from app.monitoring import docker_stats, kafka_stats
from app.monitoring.collector import DashboardCollector
from app.monitoring.docker_stats import ContainerStat
from app.monitoring.model import KafkaSnapshot
from app.spark_api import app_client


def _make_ready(worker_count: int = 1, kafka_broker_count: int = 3) -> None:
    manager.state = ClusterState.READY
    manager.params = ClusterParams(
        worker_count=worker_count, worker_cores=1, worker_memory_gb=1,
        kafka_broker_count=kafka_broker_count,
    )


@pytest.fixture(autouse=True)
def _no_real_spark(monkeypatch):
    monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
    monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: None)


@pytest.fixture
def fresh_collector():
    return DashboardCollector()


def _stat(name: str) -> ContainerStat:
    return ContainerStat(
        name=name, cpu_pct=10.0, mem_used_bytes=100 * 1024**2, mem_limit_bytes=1024**3,
        net_rx_bytes=0, net_tx_bytes=0, block_read_bytes=0, block_write_bytes=0,
    )


class TestNonKafkaSpawnSkipsShellouts:
    @pytest.mark.asyncio
    async def test_no_kafka_containers_means_kafka_none_and_zero_cli_calls(
        self, fresh_collector, monkeypatch
    ):
        async def _fake_sample(cpu_limits, timeout_s=3.0):
            return [_stat("spark-master")]  # no spark-kafka-* containers

        monkeypatch.setattr(docker_stats, "sample", _fake_sample)

        calls = []
        monkeypatch.setattr(
            kafka_stats, "find_live_broker",
            lambda brokers, timeout_s=5.0: calls.append(brokers) or _async_none(),
        )
        _make_ready()

        snapshot = await fresh_collector.collect_once()

        assert snapshot.kafka is None
        assert calls == [], "find_live_broker was called even though no spark-kafka-* container was present"


async def _async_none():
    return None


async def _await_kafka_refresh(collector: DashboardCollector) -> None:
    """Lets a `collect_once()`-triggered background CLI refresh (issue #57
    finding 1) actually run and land in `collector._latest_kafka_cli` --
    `collect_once()` itself only schedules it, it doesn't await it."""
    task = collector._kafka_refresh_task
    if task is not None:
        await task


def _fake_kafka_cli_factory(calls: list):
    """Returns a `find_live_broker` stand-in that records each call and
    returns the first broker as 'live', plus fetch_* stubs returning empty
    results (enough to exercise the wiring, not the parsers -- those have
    their own `demo()` self-checks in kafka_stats.py)."""

    async def _find_live_broker(brokers, timeout_s=5.0):
        calls.append(list(brokers))
        return brokers[0] if brokers else None

    return _find_live_broker


class TestKafkaSpawnPopulatesSnapshot:
    @pytest.mark.asyncio
    async def test_kafka_containers_present_populates_kafka_snapshot(self, fresh_collector, monkeypatch):
        async def _fake_sample(cpu_limits, timeout_s=3.0):
            return [_stat("spark-master"), _stat("spark-kafka-1"), _stat("spark-kafka-2")]

        monkeypatch.setattr(docker_stats, "sample", _fake_sample)
        calls: list = []
        monkeypatch.setattr(kafka_stats, "find_live_broker", _fake_kafka_cli_factory(calls))
        monkeypatch.setattr(kafka_stats, "fetch_topics_describe", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_urp", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_offsets", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_state", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_quorum_status", lambda c, timeout_s=8.0: _async({}))
        monkeypatch.setattr(kafka_stats, "fetch_log_dirs", lambda c, timeout_s=8.0: _async({}))
        _make_ready(kafka_broker_count=3)

        snapshot = await fresh_collector.collect_once()

        assert snapshot.kafka is not None
        assert snapshot.kafka.brokers_online == 2
        assert snapshot.kafka.brokers_total == 3
        assert {b.container_name for b in snapshot.kafka.brokers} == {
            "spark-kafka-1", "spark-kafka-2", "spark-kafka-3",
        }
        await _await_kafka_refresh(fresh_collector)
        assert calls == [["spark-kafka-1", "spark-kafka-2"]]

    @pytest.mark.asyncio
    async def test_broker_1_down_falls_back_to_broker_2(self, fresh_collector, monkeypatch):
        """The broker-fallback ordering (US-MBK2): broker 1 absent from
        `docker stats` output (killed) -- the collector must still succeed
        via broker 2, never hardcoding broker 1 as the sole target."""
        async def _fake_sample(cpu_limits, timeout_s=3.0):
            return [_stat("spark-kafka-2"), _stat("spark-kafka-3")]  # broker 1 is down

        monkeypatch.setattr(docker_stats, "sample", _fake_sample)
        calls: list = []
        monkeypatch.setattr(kafka_stats, "find_live_broker", _fake_kafka_cli_factory(calls))
        monkeypatch.setattr(kafka_stats, "fetch_topics_describe", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_urp", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_offsets", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_state", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_quorum_status", lambda c, timeout_s=8.0: _async({}))
        monkeypatch.setattr(kafka_stats, "fetch_log_dirs", lambda c, timeout_s=8.0: _async({}))
        _make_ready(kafka_broker_count=3)

        snapshot = await fresh_collector.collect_once()

        assert snapshot.kafka.brokers_online == 2
        # Issue #57 finding 2: broker 1 (missing from `docker stats`, i.e.
        # killed) still gets a row, marked offline -- not silently dropped.
        broker_1 = next(b for b in snapshot.kafka.brokers if b.container_name == "spark-kafka-1")
        assert broker_1.online is False
        broker_2 = next(b for b in snapshot.kafka.brokers if b.container_name == "spark-kafka-2")
        assert broker_2.online is True

        await _await_kafka_refresh(fresh_collector)
        assert calls == [["spark-kafka-2", "spark-kafka-3"]], (
            "collector must try whichever brokers are actually live, in order, "
            "not assume spark-kafka-1"
        )


async def _async(value):
    return value


class TestKafkaSubCadence:
    @pytest.mark.asyncio
    async def test_cli_shellouts_only_run_on_the_subcadence_tick(self, fresh_collector, monkeypatch):
        """ADR D-MBK5: the heavier CLI pull runs every `KAFKA_COLLECTOR_
        SUBCADENCE_CYCLES`th collector cycle, reusing the last `KafkaSnapshot`
        in between -- not every 2s base cycle."""
        monkeypatch.setattr(config, "KAFKA_COLLECTOR_SUBCADENCE_CYCLES", 3)

        async def _fake_sample(cpu_limits, timeout_s=3.0):
            return [_stat("spark-kafka-1")]

        monkeypatch.setattr(docker_stats, "sample", _fake_sample)
        calls: list = []
        monkeypatch.setattr(kafka_stats, "find_live_broker", _fake_kafka_cli_factory(calls))
        monkeypatch.setattr(kafka_stats, "fetch_topics_describe", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_urp", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_offsets", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_consumer_groups_state", lambda c, timeout_s=8.0: _async([]))
        monkeypatch.setattr(kafka_stats, "fetch_quorum_status", lambda c, timeout_s=8.0: _async({}))
        monkeypatch.setattr(kafka_stats, "fetch_log_dirs", lambda c, timeout_s=8.0: _async({}))
        _make_ready(kafka_broker_count=1)

        # Cycle 1 (first ever cycle) always refreshes (no prior snapshot to
        # reuse) -- await the detached refresh task so it actually lands in
        # `_latest_kafka_cli` before cycle 2's due-for-refresh check runs.
        await fresh_collector.collect_once()
        await _await_kafka_refresh(fresh_collector)
        assert len(calls) == 1
        cycle_1_task = fresh_collector._kafka_refresh_task
        # Cycle 2: not on the sub-cadence tick -> reuses last cached CLI
        # result, no new task scheduled (still the same completed task object).
        await fresh_collector.collect_once()
        assert fresh_collector._kafka_refresh_task is cycle_1_task
        assert len(calls) == 1
        # Cycle 3: hits the sub-cadence tick (3rd cycle) -> schedules and runs
        # a new refresh.
        await fresh_collector.collect_once()
        await _await_kafka_refresh(fresh_collector)
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_collect_once_does_not_block_on_an_in_flight_refresh(self, fresh_collector, monkeypatch):
        """Issue #57 finding 1: a slow CLI pull must not stall `collect_once()`
        (and therefore the whole SSE broadcast, since Overview/Job/Node grids
        are built in the same call) -- it now only schedules the refresh as a
        background task instead of awaiting it inline."""
        async def _fake_sample(cpu_limits, timeout_s=3.0):
            return [_stat("spark-kafka-1")]

        monkeypatch.setattr(docker_stats, "sample", _fake_sample)

        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_refresh_kafka_cli(self, kafka_names):
            started.set()
            await release.wait()
            return KafkaSnapshot(running=True)

        monkeypatch.setattr(DashboardCollector, "_refresh_kafka_cli", _slow_refresh_kafka_cli)
        _make_ready(kafka_broker_count=1)

        snapshot = await asyncio.wait_for(fresh_collector.collect_once(), timeout=1.0)

        assert snapshot.kafka is not None, "collect_once() must return promptly, not hang on the CLI pull"
        assert started.is_set(), "the refresh should have been scheduled"
        assert not fresh_collector._kafka_refresh_task.done(), "the refresh must still be in flight"

        release.set()
        await fresh_collector._kafka_refresh_task
