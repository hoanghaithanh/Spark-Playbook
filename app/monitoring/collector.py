"""Spark Playbook — background snapshot collector for the monitoring
dashboard (ADR D-B, D-C, D-D).

One `asyncio` background task samples *all* data sources once per cycle
(~`config.DASHBOARD_COLLECTOR_INTERVAL_S`) into an in-process `Snapshot`,
joins them, derives factual signals (`diagnostics.py`, `eta.py`), and pushes
the result to every subscribed SSE client (`web/routes/dashboard.py`).

Lifecycle-gated (ADR D-B, R-Dash-3): the loop runs only while
`manager.state == READY` **and** at least one client is subscribed; it exits
(not just idles) otherwise, so there is no docker/Spark polling with nobody
watching. `ensure_running()` is cheap (no I/O) and is called both on
subscribe and on every SSE stream tick so the collector (re)starts promptly
if the cluster becomes READY while a client is already connected watching
the empty state, without a second persistent poller task.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from app import config
from app.lifecycle.manager import ClusterState, manager
from app.lifecycle.renderer import ClusterParams
from app.monitoring import diagnostics, docker_stats, eta, kafka_stats
from app.monitoring.docker_stats import ContainerStat
from app.monitoring.model import (
    ConsumerGroupRow,
    HistoryPoint,
    JobSummary,
    KafkaBrokerStat,
    KafkaSnapshot,
    KafkaTopicRow,
    NodeStat,
    PartitionLagRow,
    PartitionRow,
    SignalCard,
    Snapshot,
    StageBar,
)
from app.spark_api import app_client

logger = logging.getLogger(__name__)

_ICON_SKEW = "⚖️"
_ICON_MEMORY = "\U0001f9e0"
_ICON_CLOCK = "⏱️"


def _color_for_pct(pct: Optional[float], warn: float, crit: float) -> str:
    if pct is None:
        return "#8b93a3"
    if pct >= crit:
        return config.DASHBOARD_COLOR_RED
    if pct >= warn:
        return config.DASHBOARD_COLOR_AMBER
    return config.DASHBOARD_COLOR_GREEN


def _color_for_gc(gc_ms: Optional[float]) -> str:
    if gc_ms is None:
        return "#4a5061"
    if gc_ms > config.DASHBOARD_GC_CRIT_MS:
        return config.DASHBOARD_COLOR_RED
    if gc_ms > config.DASHBOARD_GC_WARN_MS:
        return config.DASHBOARD_COLOR_AMBER
    return "#4a5061"


def _rate_label(delta_bytes: Optional[int], elapsed_s: float) -> str:
    if delta_bytes is None or elapsed_s <= 0:
        return "—"
    rate = max(0.0, delta_bytes / elapsed_s) / (1000**2)  # MB/s
    return f"{rate:.1f} MB/s"


def _node_role(container_name: str) -> str:
    if container_name == "spark-master":
        return "master"
    if container_name == "spark-driver":
        return "driver"
    return "worker"


def _expected_containers(params: ClusterParams) -> List[str]:
    names = ["spark-master"]
    names += [f"spark-worker-{i}" for i in range(1, params.worker_count + 1)]
    names.append("spark-driver")
    return names


def _expected_kafka_containers(params: ClusterParams) -> List[str]:
    """Deliberately separate from `_expected_containers()` (ADR D-MBK5):
    that list feeds the Spark Overview/Node grids -- brokers live in the
    Kafka tab instead."""
    return [f"spark-kafka-{i}" for i in range(1, params.kafka_broker_count + 1)]


_KAFKA_CONTAINER_RE = re.compile(r"^spark-kafka-(\d+)$")


def _kafka_node_id(container_name: str) -> Optional[int]:
    m = _KAFKA_CONTAINER_RE.match(container_name)
    return int(m.group(1)) if m else None


def _cpu_limits(params: ClusterParams) -> Dict[str, float]:
    limits = {
        "spark-master": config.DASHBOARD_MASTER_CPU_CORES,
        "spark-driver": config.DASHBOARD_DRIVER_CPU_CORES,
    }
    for i in range(1, params.worker_count + 1):
        limits[f"spark-worker-{i}"] = float(params.worker_cores)
    for name in _expected_kafka_containers(params):
        limits[name] = config.DASHBOARD_KAFKA_CPU_CORES
    return limits


def _alert_title_for(node: NodeStat, skew_reasons: Dict[str, str], imbalance_reasons: Dict[str, str]) -> str:
    """Issue #21: the alert banner's title used to be derived by
    `flag_reason.split(':')[0]`, which silently assumed every flag reason
    has a colon. `diagnostics.node_skew_reasons()`'s strings do ("Data skew:
    handling ..."), but `diagnostics.node_imbalance_reasons()`'s don't ("CPU
    saturated (95%) while worker-2 is idle (10%)") -- so a node flagged
    purely by CPU imbalance produced a garbled title dumping the whole
    detail sentence. Look up which diagnostic actually flagged this node
    (both dicts are in scope where the flag itself gets set) and pick a
    short, factual category label deliberately instead of parsing the
    detail text."""
    if node.name in skew_reasons:
        category = "Skew"
    elif node.name in imbalance_reasons:
        category = "Resource imbalance"
    else:
        category = "Issue"
    return f"{category} detected on {node.name}"


def _parse_launch_time(launch_time: Optional[str]) -> Optional[datetime]:
    """Parse Spark REST's `launchTime` (ISO8601 with a trailing "GMT" instead
    of "Z"/an offset, e.g. "2024-01-01T12:00:00.000GMT"): strips the
    trailing "GMT", parses with `%Y-%m-%dT%H:%M:%S.%f`, and attaches UTC.
    Returns `None` (never fabricates a value) on anything missing/unparseable
    (a `ValueError` from `strptime`), per issue #17."""
    if not launch_time:
        return None
    text = str(launch_time)
    if text.endswith("GMT"):
        text = text[: -len("GMT")]
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _running_time_label(raw: dict) -> str:
    """Issue #17: a RUNNING task with no `duration` yet should show elapsed
    time (`now - launchTime`), not the bare "running..." placeholder --
    US-5.2 c1's parenthetical "or elapsed time for still-running tasks".
    Falls back to "running..." (never fabricating a duration) when
    `launchTime` is missing or unparseable, or the task isn't RUNNING."""
    if raw.get("status") != "RUNNING":
        return "running…"
    launch = _parse_launch_time(raw.get("launch_time"))
    if launch is None:
        return "running…"
    elapsed_s = (datetime.now(timezone.utc) - launch).total_seconds()
    return eta.format_seconds(elapsed_s)


def _select_current_stage(stages_raw: List[dict]) -> Optional[dict]:
    """The running stage if any, else the most recently completed one
    (US-5.2 c3 -- "most recently completed stage" retention boundary, ADR
    D-E). Ties broken by highest stageId (Spark assigns ids monotonically,
    so this is a reliable "most recent" proxy without timestamp parsing)."""
    valid = [s for s in stages_raw if isinstance(s, dict)]
    active = [s for s in valid if s.get("status") == "ACTIVE"]
    if active:
        return max(active, key=lambda s: s.get("stageId", -1))
    completed = [s for s in valid if s.get("status") == "COMPLETE"]
    if completed:
        return max(completed, key=lambda s: s.get("stageId", -1))
    return None


def retries_by_index(tasks_raw: List[dict]) -> Dict[int, int]:
    """Max `attempt` seen per task `index` across a stage's raw task-list
    records -- retries appear as separate records sharing the same `index`,
    so this is exactly "how many times was this partition retried" (0 = never
    retried). Shared by `DashboardCollector._build_partitions()` (post-hoc
    dashboard partition rows) and `app/web/routes/annotation.py`'s
    reveal-time task-retry evidence for Fault Tolerance & Lineage (US-C9,
    Decision A) -- same grouping logic, two call sites, extracted here rather
    than duplicated."""
    result: Dict[int, int] = {}
    for t in tasks_raw:
        if not isinstance(t, dict):
            continue
        idx = t.get("index")
        if idx is None:
            continue
        result[idx] = max(result.get(idx, 0), t.get("attempt", 0))
    return result


def _executor_host_map(executors_raw: Optional[List[dict]], ip_to_name: Dict[str, str]) -> Dict[str, str]:
    """executorId -> container hostname (ADR D-D join key: service name ==
    hostname == executor host).

    `ip_to_name` is the defensive fallback found necessary by actually
    running this against a real cluster (see `docker_stats.container_ip_map`'s
    docstring, ADR R-Dash-1): standalone Spark's workers report each
    executor's `hostPort` as the container's raw bridge-network IP, not its
    hostname, so a host that isn't already a recognized container name is
    looked up by IP before falling back (never mis-attaching) to the raw,
    unmapped value."""
    mapping: Dict[str, str] = {}
    if not isinstance(executors_raw, list):
        return mapping
    for e in executors_raw:
        if not isinstance(e, dict):
            continue
        exec_id = e.get("id")
        if exec_id == "driver":
            mapping[exec_id] = "spark-driver"
            continue
        host_port = e.get("hostPort") or ""
        host = host_port.split(":")[0] if host_port else None
        if exec_id and host:
            mapping[exec_id] = ip_to_name.get(host, host)
    return mapping


class DashboardCollector:
    def __init__(self) -> None:
        self._subscribers: Set[asyncio.Queue] = set()
        self._task: Optional[asyncio.Task] = None
        self._latest_snapshot: Optional[Snapshot] = None

        self._history: Dict[str, List[HistoryPoint]] = {}
        self._prev_containers: Dict[str, ContainerStat] = {}
        self._prev_gc_ms: Dict[str, float] = {}
        self._prev_ts: Optional[float] = None

        # Kafka observability sub-cadence state (ADR D-MBK5).
        self._kafka_cycle_count: int = 0
        self._latest_kafka: Optional[KafkaSnapshot] = None
        # Issue #57 finding 1: the raw CLI-derived snapshot is now cached
        # separately from `_latest_kafka` (the full assembled broadcast
        # snapshot) so `_build_kafka` can read "whatever the last completed
        # refresh produced" without awaiting an in-flight one -- decoupling
        # the sub-cadence refresh from the base-cadence broadcast.
        self._latest_kafka_cli: Optional[KafkaSnapshot] = None
        self._kafka_refresh_task: Optional[asyncio.Task] = None
        self._kafka_partitions_led: Dict[int, int] = {}
        self._prev_kafka_offsets: Dict[Tuple[str, int], int] = {}
        self._prev_kafka_offsets_ts: Optional[float] = None

    # ------------------------------------------------------------------ #
    # subscription lifecycle (ADR D-B / R-Dash-3)
    # ------------------------------------------------------------------ #

    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(queue)
        if self._latest_snapshot is not None:
            queue.put_nowait(self._latest_snapshot)
        self.ensure_running()
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)
        if not self._subscribers and self._task is not None and not self._task.done():
            logger.info("Dashboard collector stopping: last subscriber disconnected.")
            self._task.cancel()

    def ensure_running(self) -> None:
        """Cheap, no-I/O: (re)starts the sampling loop if the cluster is
        READY, at least one client is attached, and no loop is already
        running. Called on every subscribe and on every SSE stream tick so a
        cluster becoming READY while a client is already connected starts
        sampling promptly without a second always-on poller."""
        if manager.state != ClusterState.READY:
            return
        if not self._subscribers:
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        logger.info("Dashboard collector starting (subscribers=%d).", len(self._subscribers))
        try:
            while True:
                if not self._subscribers:
                    logger.info("Dashboard collector stopping: no subscribers left.")
                    break
                if manager.state != ClusterState.READY:
                    # One final broadcast so clients see "no longer
                    # available" instead of frozen last-known values
                    # (US-5.1 c3), then stop sampling entirely (R-Dash-3).
                    logger.info("Dashboard collector stopping: cluster no longer READY.")
                    self._broadcast(self.inactive_snapshot())
                    self._reset_deltas()
                    break
                try:
                    snapshot = await self.collect_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Dashboard collector cycle failed; will retry next cycle.")
                    await asyncio.sleep(config.DASHBOARD_COLLECTOR_INTERVAL_S)
                    continue
                self._latest_snapshot = snapshot
                self._broadcast(snapshot)
                await asyncio.sleep(config.DASHBOARD_COLLECTOR_INTERVAL_S)
        finally:
            self._task = None

    def _broadcast(self, snapshot: Snapshot) -> None:
        for q in list(self._subscribers):
            while not q.empty():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break
            q.put_nowait(snapshot)

    def _reset_deltas(self) -> None:
        self._prev_containers = {}
        self._prev_gc_ms = {}
        self._prev_ts = None
        self._history = {}
        self._kafka_cycle_count = 0
        self._latest_kafka = None
        self._latest_kafka_cli = None
        self._cancel_kafka_refresh()
        self._kafka_partitions_led = {}
        self._prev_kafka_offsets = {}
        self._prev_kafka_offsets_ts = None

    def _cancel_kafka_refresh(self) -> None:
        """Issue #57 finding 1: a background sub-cadence refresh (started by
        `_build_kafka`) must not outlive a cluster teardown / `_reset_deltas()`
        mid-refresh -- cancel it cleanly (mirrors `unsubscribe()`'s
        `self._task.cancel()` pattern for the outer collector loop; the
        cancelled `docker exec` child is reaped by `kafka_stats._run`'s own
        `CancelledError` handling, issue #18)."""
        if self._kafka_refresh_task is not None and not self._kafka_refresh_task.done():
            self._kafka_refresh_task.cancel()
        self._kafka_refresh_task = None

    def inactive_snapshot(self) -> Snapshot:
        return Snapshot(cluster_active=False, has_job=False, now_label=_now_label())

    # ------------------------------------------------------------------ #
    # one collector cycle
    # ------------------------------------------------------------------ #

    async def collect_once(self) -> Snapshot:
        status = manager.status()
        params = status.params or ClusterParams()

        cpu_limits = _cpu_limits(params)
        container_stats = await docker_stats.sample(cpu_limits, timeout_s=3.0)
        stats_by_name = {c.name: c for c in container_stats}

        now = time.monotonic()
        elapsed_s = (now - self._prev_ts) if self._prev_ts is not None else None
        self._prev_ts = now

        # Issue #19: `app_client.fetch_*()` are synchronous, blocking
        # `urllib.request.urlopen()` calls under the hood. This whole app is
        # single-process/single-event-loop, so calling them directly here
        # (no `await`, no thread offload) freezes *every* concurrently
        # running coroutine -- not just the dashboard -- for up to each
        # call's own timeout whenever the driver is slow/unreachable (a
        # real, already-documented failure mode, PLAN.md R2), repeated every
        # ~2s for as long as any dashboard client is connected. Offload each
        # to a worker thread so a slow/stuck driver only stalls this cycle's
        # own data, not the whole app.
        #
        # Issue #24: resolve the current application's `AppRef` (id + which
        # of DRIVER_APP_UI_PORTS actually serves it) once per cycle and
        # thread that single `AppRef` through every fetch below, instead of
        # each fetch independently assuming a fixed `:4040` -- otherwise a
        # second concurrently-open Jupyter kernel (whose SparkContext Spark
        # silently rebinds to :4041/:4042 once :4040 is already held) is
        # permanently invisible to the dashboard, which is exactly what froze
        # Job Detail on the first job of a session.
        app_ref = await asyncio.to_thread(app_client.resolve_current_app, timeout_s=2.0)
        app_id = app_ref.app_id if app_ref else None
        executors_raw = (
            await asyncio.to_thread(app_client.fetch_executors, app_ref, timeout_s=2.0) if app_ref else None
        )
        stages_raw = await asyncio.to_thread(app_client.fetch_stages, app_ref, timeout_s=2.0) if app_ref else None

        ip_to_name: Dict[str, str] = {}
        if app_id:
            container_ids = [c.container_id for c in container_stats if c.container_id]
            ip_to_name = await docker_stats.container_ip_map(container_ids, timeout_s=2.0)
        executor_hosts = _executor_host_map(executors_raw, ip_to_name)

        gc_by_host = self._gc_by_host(executors_raw, executor_hosts, elapsed_s)

        current_stage = _select_current_stage(stages_raw) if isinstance(stages_raw, list) else None
        tasks_raw: Optional[List[dict]] = None
        if app_ref is not None and current_stage is not None:
            # `length` passed explicitly -- found by actually running this
            # against a real 200-task stage: the REST endpoint silently caps
            # at 20 tasks otherwise (app_client.fetch_task_list's docstring).
            tasks_raw = await asyncio.to_thread(
                app_client.fetch_task_list,
                app_ref,
                current_stage.get("stageId"),
                current_stage.get("attemptId", 0),
                length=max(1000, current_stage.get("numTasks") or 0),
                timeout_s=2.0,
            )

        task_samples, partition_rows, partitions_by_node = self._build_partitions(tasks_raw, ip_to_name)
        skewed_ids = diagnostics.skewed_task_ids(task_samples)
        skew_reasons = diagnostics.node_skew_reasons(task_samples, skewed_ids)

        nodes = self._build_nodes(
            params, stats_by_name, gc_by_host, partitions_by_node, elapsed_s, skew_reasons
        )

        # Kafka observability layer (ADR D-MBK5) -- uses `self._prev_containers`
        # (still last cycle's values here, before the reassignment below) for
        # its own disk/net delta, the same pattern `_build_nodes` just used.
        kafka_snapshot = await self._build_kafka(params, stats_by_name, elapsed_s)

        self._prev_containers = stats_by_name

        resource_samples = [
            diagnostics.NodeResourceSample(node=n.name, cpu_pct=n.cpu_pct, ram_pct=n.ram_pct, gc_ms=n.gc_ms)
            for n in nodes
            if n.available
        ]
        imbalance_reasons = diagnostics.node_imbalance_reasons(resource_samples)
        for n in nodes:
            if n.name in imbalance_reasons and not n.flagged:
                n.flagged = True
                n.flag_reason = imbalance_reasons[n.name]
                n.status_color = config.DASHBOARD_COLOR_RED
                n.border_color = "#fecaca"

        job, stages_bars, stage_durations = self._build_job(app_id, current_stage, stages_raw, task_samples)

        signal_cards = self._build_signal_cards(
            task_samples, skewed_ids, resource_samples, stage_durations, job, current_stage, app_ref
        )

        flagged_node = next((n for n in nodes if n.flagged), None)
        has_alert = flagged_node is not None
        alert_title = (
            _alert_title_for(flagged_node, skew_reasons, imbalance_reasons) if flagged_node else ""
        )
        alert_detail = flagged_node.flag_reason if flagged_node else ""

        worker_count = sum(1 for n in nodes if n.role == "worker")

        return Snapshot(
            cluster_active=True,
            has_job=job is not None,
            now_label=_now_label(),
            nodes=nodes,
            node_count_label=f"1 master · {worker_count} workers",
            job=job,
            stages=stages_bars,
            partitions=partition_rows,
            partition_summary=_partition_summary(task_samples),
            signal_cards=signal_cards,
            has_alert=has_alert,
            kafka=kafka_snapshot,
            alert_title=alert_title,
            alert_detail=alert_detail,
        )

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #

    def _gc_by_host(
        self, executors_raw, executor_hosts: Dict[str, str], elapsed_s: Optional[float]
    ) -> Dict[str, float]:
        """Delta of `totalGCTime` (cumulative ms since executor start) across
        this cycle -- a proxy for "current" GC pressure comparable in
        magnitude to the mockup's 8-40ms figures, since the raw cumulative
        value only grows and would eventually always read as critical."""
        if not isinstance(executors_raw, list):
            return {}
        current: Dict[str, float] = {}
        result: Dict[str, float] = {}
        for e in executors_raw:
            if not isinstance(e, dict):
                continue
            exec_id = e.get("id")
            host = executor_hosts.get(exec_id)
            if not host:
                continue
            gc_ms = e.get("totalGCTime")
            if gc_ms is None:
                continue
            current[host] = float(gc_ms)

        for host, value in current.items():
            prev = self._prev_gc_ms.get(host)
            if prev is not None and value >= prev:
                result[host] = value - prev
            # else: first sample for this host, or executor restarted (value
            # went down) -- no delta available yet this cycle.
        self._prev_gc_ms = current
        return result

    def _build_partitions(self, tasks_raw, ip_to_name: Dict[str, str]):
        if not isinstance(tasks_raw, list):
            return [], [], {}

        # Keep only the latest attempt per partition index (retries appear
        # as separate task records sharing the same "index").
        latest_by_index: Dict[int, dict] = {}
        for t in tasks_raw:
            if not isinstance(t, dict):
                continue
            idx = t.get("index")
            if idx is None:
                continue
            existing = latest_by_index.get(idx)
            if existing is None or t.get("attempt", 0) >= existing.get("attempt", 0):
                latest_by_index[idx] = t
        retries_map = retries_by_index(tasks_raw)

        task_samples: List[diagnostics.TaskSample] = []
        raw_by_id: Dict[str, dict] = {}
        for idx, t in latest_by_index.items():
            # The task record itself carries "host" directly (verified
            # against a real stage) -- simpler and more robust than joining
            # back through executorId -> /executors' hostPort. Standalone
            # Spark reports this as the container's raw IP for workers, so
            # it still needs the same ip_to_name fallback (ADR R-Dash-1).
            raw_host = t.get("host") or t.get("executorId") or "unknown"
            node = ip_to_name.get(raw_host, raw_host)
            metrics = t.get("taskMetrics") or {}
            input_bytes = (metrics.get("inputMetrics") or {}).get("bytesRead", 0) or 0
            shuffle_read = metrics.get("shuffleReadMetrics") or {}
            shuffle_read_bytes = (shuffle_read.get("localBytesRead", 0) or 0) + (
                shuffle_read.get("remoteBytesRead", 0) or 0
            )
            shuffle_write_bytes = (metrics.get("shuffleWriteMetrics") or {}).get("bytesWritten", 0) or 0
            size_bytes = input_bytes + shuffle_read_bytes + shuffle_write_bytes
            duration_ms = t.get("duration")
            duration_s = (duration_ms / 1000.0) if isinstance(duration_ms, (int, float)) else None
            task_id = f"p-{idx:03d}"

            task_samples.append(
                diagnostics.TaskSample(
                    node=node,
                    task_id=task_id,
                    size_bytes=size_bytes,
                    duration_s=duration_s,
                    retries=retries_map.get(idx, 0),
                )
            )
            raw_by_id[task_id] = {
                "node": node,
                "rows": (metrics.get("inputMetrics") or {}).get("recordsRead", 0) or 0,
                "shuffle_read": shuffle_read_bytes,
                "shuffle_write": shuffle_write_bytes,
                "size_bytes": size_bytes,
                "duration_s": duration_s,
                "retries": retries_map.get(idx, 0),
                "status": t.get("status"),
                "launch_time": t.get("launchTime"),
            }

        skewed_ids = diagnostics.skewed_task_ids(task_samples)
        sizes = [ts.size_bytes for ts in task_samples if ts.size_bytes > 0]
        max_size = max(sizes) if sizes else 0

        partition_rows: List[PartitionRow] = []
        partitions_by_node: Dict[str, int] = {}
        for ts in sorted(task_samples, key=lambda t: t.task_id):
            raw = raw_by_id[ts.task_id]
            partitions_by_node[ts.node] = partitions_by_node.get(ts.node, 0) + 1
            size_mb = ts.size_bytes / (1000**2)
            size_color = (
                config.DASHBOARD_COLOR_RED
                if ts.task_id in skewed_ids
                else config.DASHBOARD_COLOR_AMBER
                if size_mb > 200
                else config.DASHBOARD_COLOR_GREEN
            )
            is_skewed = ts.task_id in skewed_ids
            time_label = f"{ts.duration_s:.0f}s" if ts.duration_s is not None else _running_time_label(raw)
            retries = raw["retries"]
            partition_rows.append(
                PartitionRow(
                    node=ts.node,
                    id=ts.task_id,
                    is_skewed=is_skewed,
                    size_label=f"{size_mb:.0f} MB",
                    size_bar_pct=min(100, round((ts.size_bytes / max_size) * 100)) if max_size else 0,
                    size_color=size_color,
                    rows_label=f"{raw['rows']:,}",
                    shuffle_label=f"{raw['shuffle_read']/1e6:.1f}/{raw['shuffle_write']/1e6:.1f} MB",
                    time_label=time_label,
                    retries_label=f"{retries} retries" if retries else "—",
                    retries_color=config.DASHBOARD_COLOR_RED if retries else "#8b93a3",
                    row_bg="#fef9f2" if is_skewed else "transparent",
                )
            )

        return task_samples, partition_rows, partitions_by_node

    def _build_nodes(
        self, params, stats_by_name, gc_by_host, partitions_by_node, elapsed_s, skew_reasons
    ) -> List[NodeStat]:
        nodes: List[NodeStat] = []
        for name in _expected_containers(params):
            role = _node_role(name)
            stat = stats_by_name.get(name)
            available = stat is not None
            prev = self._prev_containers.get(name)

            node = NodeStat(
                name=name,
                role=role,
                host=name,
                available=available,
                is_master=(role == "master"),
            )

            if available and stat.cpu_pct is not None:
                cpu_pct = max(0, round(stat.cpu_pct))
                node.cpu_pct = cpu_pct
                node.cpu_label = f"{cpu_pct}%"
                node.cpu_color = _color_for_pct(cpu_pct, config.DASHBOARD_CPU_WARN_PCT, config.DASHBOARD_CPU_CRIT_PCT)

            if available and stat.mem_used_bytes is not None and stat.mem_limit_bytes:
                ram_pct = round((stat.mem_used_bytes / stat.mem_limit_bytes) * 100)
                node.ram_pct = ram_pct
                node.ram_label = f"{ram_pct}%"
                node.ram_color = _color_for_pct(ram_pct, config.DASHBOARD_RAM_WARN_PCT, config.DASHBOARD_RAM_CRIT_PCT)

            if available and prev is not None and elapsed_s:
                disk_delta = None
                net_delta = None
                if stat.block_read_bytes is not None and prev.block_read_bytes is not None:
                    disk_delta = (stat.block_read_bytes - prev.block_read_bytes) + (
                        (stat.block_write_bytes or 0) - (prev.block_write_bytes or 0)
                    )
                if stat.net_rx_bytes is not None and prev.net_rx_bytes is not None:
                    net_delta = (stat.net_rx_bytes - prev.net_rx_bytes) + (
                        (stat.net_tx_bytes or 0) - (prev.net_tx_bytes or 0)
                    )
                node.disk_io = _rate_label(disk_delta, elapsed_s)
                node.net_io = _rate_label(net_delta, elapsed_s)

            gc_ms = gc_by_host.get(name)
            if gc_ms is not None:
                node.gc_ms = gc_ms
                node.gc_label = f"{gc_ms:.0f} ms"
                node.gc_color = _color_for_gc(gc_ms)

            if role == "worker":
                node.has_partitions = True
                node.partition_count = partitions_by_node.get(name, 0)

            if name in skew_reasons:
                node.flagged = True
                node.flag_reason = skew_reasons[name]
                node.status_color = config.DASHBOARD_COLOR_RED
                node.border_color = "#fecaca"
            elif not available:
                node.status_color = "#8b93a3"
                node.border_color = "#e5e7eb"
            else:
                node.status_color = node.cpu_color if node.cpu_pct is not None else config.DASHBOARD_COLOR_GREEN

            history = self._history.setdefault(name, [])
            if node.cpu_pct is not None and node.ram_pct is not None:
                history.append(
                    HistoryPoint(pct=node.cpu_pct, color=node.cpu_color)
                )
                if len(history) > config.DASHBOARD_HISTORY_LENGTH:
                    del history[: len(history) - config.DASHBOARD_HISTORY_LENGTH]
            node.cpu_history = list(self._history.get(name, []))
            ram_history_key = f"{name}:ram"
            ram_history = self._history.setdefault(ram_history_key, [])
            if node.ram_pct is not None:
                ram_history.append(HistoryPoint(pct=node.ram_pct, color=node.ram_color))
                if len(ram_history) > config.DASHBOARD_HISTORY_LENGTH:
                    del ram_history[: len(ram_history) - config.DASHBOARD_HISTORY_LENGTH]
            node.ram_history = list(ram_history)

            nodes.append(node)
        return nodes

    # ------------------------------------------------------------------ #
    # Kafka observability layer (ADR D-MBK5)
    # ------------------------------------------------------------------ #

    async def _build_kafka(
        self, params: ClusterParams, stats_by_name: Dict[str, ContainerStat], elapsed_s: Optional[float]
    ) -> Optional[KafkaSnapshot]:
        """Two cadences, one cycle: broker CPU/RAM/disk/net are rebuilt every
        call from this cycle's already-fetched `docker stats` batch (zero
        added cost). The heavier `docker exec` CLI shellouts are only
        *kicked off* every `config.KAFKA_COLLECTOR_SUBCADENCE_CYCLES`th call,
        as a detached background task (issue #57 finding 1) -- this call
        never awaits that pull, it just reads whatever `self._latest_kafka_cli`
        currently holds (the last *completed* refresh), so the base 2s
        collector cadence (and the whole SSE broadcast, which is built and
        sent in the same `collect_once()` call) keeps flowing independently
        of the ~12s CLI pull, matching the ADR's "two cadences, one cycle"
        framing. Returns None (no shellouts at all) when no `spark-kafka-*`
        container is in this cycle's stats -- a non-Kafka spawn never touches
        Kafka tooling."""
        kafka_names = sorted(
            (name for name in stats_by_name if _kafka_node_id(name) is not None),
            key=_kafka_node_id,
        )
        if not kafka_names:
            self._latest_kafka = None
            self._latest_kafka_cli = None
            self._cancel_kafka_refresh()
            self._kafka_cycle_count = 0
            self._prev_kafka_offsets = {}
            self._prev_kafka_offsets_ts = None
            return None

        self._kafka_cycle_count += 1
        due_for_refresh = (
            self._latest_kafka_cli is None
            or self._kafka_cycle_count % config.KAFKA_COLLECTOR_SUBCADENCE_CYCLES == 0
        )
        if due_for_refresh and (self._kafka_refresh_task is None or self._kafka_refresh_task.done()):
            self._kafka_refresh_task = asyncio.create_task(self._refresh_kafka_cli(kafka_names))
            self._kafka_refresh_task.add_done_callback(self._on_kafka_refresh_done)
        cli = self._latest_kafka_cli

        brokers_total = len(_expected_kafka_containers(params))
        brokers = self._build_kafka_brokers(params, stats_by_name, elapsed_s, cli)

        kafka = KafkaSnapshot(
            running=True,
            brokers_online=len(kafka_names),
            brokers_total=brokers_total,
            under_replicated_count=cli.under_replicated_count if cli else 0,
            active_controller_id=cli.active_controller_id if cli else None,
            throughput_label=cli.throughput_label if cli else "—",
            consumer_group_count=cli.consumer_group_count if cli else 0,
            total_lag=cli.total_lag if cli else 0,
            p99_latency_label="—",  # JMX-sourced (US-MBK3), not this sub-story
            jmx_available=False,
            brokers=brokers,
            topics=cli.topics if cli else [],
            consumer_groups=cli.consumer_groups if cli else [],
            isr_shrink_events=[],  # ISR-diff tracking is US-MBK5 (#60)
        )
        self._latest_kafka = kafka
        return kafka

    def _on_kafka_refresh_done(self, task: asyncio.Task) -> None:
        """Completion callback for the detached sub-cadence refresh task
        (issue #57 finding 1). Runs outside `collect_once()`'s call stack, so
        it only ever updates the cache the *next* cycle reads -- never blocks
        anything. A cancelled task (cluster teardown mid-refresh, see
        `_cancel_kafka_refresh`) is expected and silently dropped."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.exception("Kafka CLI refresh failed; will retry next sub-cadence tick.", exc_info=exc)
            return
        self._latest_kafka_cli = task.result()

    async def _refresh_kafka_cli(self, kafka_names: List[str]) -> KafkaSnapshot:
        """The heavier sub-cadence pull: find a live broker (fallback order
        1, 2, 3, ...), then run every CLI shellout concurrently
        (`asyncio.gather`) -- this is what makes the ~10s sub-cadence figure
        realistic instead of the ~7-14s a sequential sum of ~7 JVM-startup
        CLI invocations would take (ADR D-MBK5). Degrades to an empty-but-
        `running=True` snapshot (never raises) if every broker is
        unreachable this cycle -- a transient CLI hiccup must not take down
        the whole collector cycle (mirrors `docker_stats`'s "never raise"
        posture)."""
        live = await kafka_stats.find_live_broker(kafka_names, timeout_s=5.0)
        if live is None:
            self._kafka_partitions_led = {}
            return KafkaSnapshot(running=True)

        topics, urp, offset_rows, state_rows, quorum_status, _log_dirs = await asyncio.gather(
            kafka_stats.fetch_topics_describe(live),
            kafka_stats.fetch_urp(live),
            kafka_stats.fetch_consumer_groups_offsets(live),
            kafka_stats.fetch_consumer_groups_state(live),
            kafka_stats.fetch_quorum_status(live),
            kafka_stats.fetch_log_dirs(live),
        )

        topic_names = [t.name for t in topics]
        offsets_per_topic = (
            await asyncio.gather(*(kafka_stats.fetch_offsets(live, name) for name in topic_names))
            if topic_names
            else []
        )
        current_offsets: Dict[Tuple[str, int], int] = {}
        for topic_offsets in offsets_per_topic:
            current_offsets.update(topic_offsets)

        now = time.monotonic()
        throughput_label = "—"
        if self._prev_kafka_offsets_ts is not None:
            delta_elapsed = now - self._prev_kafka_offsets_ts
            common_keys = set(current_offsets) & set(self._prev_kafka_offsets)
            if delta_elapsed > 0 and common_keys:
                delta_msgs = sum(
                    max(0, current_offsets[k] - self._prev_kafka_offsets[k]) for k in common_keys
                )
                throughput_label = f"{delta_msgs / delta_elapsed:.1f} msg/s"
        self._prev_kafka_offsets = current_offsets
        self._prev_kafka_offsets_ts = now

        # Per-broker size (bytes), summed per topic from kafka-log-dirs.sh,
        # only needed at topic granularity here.
        topic_sizes: Dict[str, int] = {}
        for (t_name, _part), size in _log_dirs.items():
            topic_sizes[t_name] = topic_sizes.get(t_name, 0) + size

        urp_keys = {(p.topic, p.partition) for p in urp}
        topic_rows: List[KafkaTopicRow] = []
        partitions_led: Dict[int, int] = {}
        for t in topics:
            urp_count = sum(1 for p in t.partitions if (p.topic, p.partition) in urp_keys)
            for p in t.partitions:
                if p.leader is not None:
                    partitions_led[p.leader] = partitions_led.get(p.leader, 0) + 1
            size_bytes = topic_sizes.get(t.name)
            topic_rows.append(
                KafkaTopicRow(
                    name=t.name,
                    partitions=t.partition_count if t.partition_count is not None else len(t.partitions),
                    replication_factor=t.replication_factor,
                    under_replicated_count=urp_count,
                    isr_health_label="Under-replicated" if urp_count else "Healthy",
                    isr_health_color=config.DASHBOARD_COLOR_RED if urp_count else config.DASHBOARD_COLOR_GREEN,
                    size_label=f"{size_bytes / 1e6:.1f} MB" if size_bytes is not None else "—",
                )
            )
        self._kafka_partitions_led = partitions_led

        state_by_group = {row.group: row for row in state_rows}
        offsets_by_group: Dict[str, List[kafka_stats.GroupOffsetRow]] = {}
        for row in offset_rows:
            offsets_by_group.setdefault(row.group, []).append(row)

        group_rows: List[ConsumerGroupRow] = []
        total_lag = 0
        for group_name, rows in offsets_by_group.items():
            state = state_by_group.get(group_name)
            partitions = [
                PartitionLagRow(
                    topic=r.topic,
                    partition=r.partition if r.partition is not None else -1,
                    current_offset=r.current_offset,
                    log_end_offset=r.log_end_offset,
                    lag=r.lag,
                    lag_label=str(r.lag) if r.lag is not None else "—",
                )
                for r in rows
            ]
            group_lag = sum(p.lag for p in partitions if p.lag is not None)
            total_lag += group_lag
            group_rows.append(
                ConsumerGroupRow(
                    group=group_name,
                    state=state.state if state else "—",
                    members=state.members if state else None,
                    total_lag=group_lag,
                    total_lag_label=str(group_lag),
                    partitions=partitions,
                )
            )

        return KafkaSnapshot(
            running=True,
            under_replicated_count=len(urp),
            active_controller_id=kafka_stats.active_controller_id(quorum_status),
            throughput_label=throughput_label,
            consumer_group_count=len(group_rows),
            total_lag=total_lag,
            topics=topic_rows,
            consumer_groups=group_rows,
        )

    def _build_kafka_brokers(
        self,
        params: ClusterParams,
        stats_by_name: Dict[str, ContainerStat],
        elapsed_s: Optional[float],
        cli: Optional[KafkaSnapshot],
    ) -> List[KafkaBrokerStat]:
        """Broker CPU/RAM/disk/net rebuilt fresh every cycle from the
        already-fetched `docker stats` batch -- reused at zero added cost,
        same delta idiom `_build_nodes` uses for disk/net (ADR D-MBK5).

        Issue #57 finding 2: iterates *all* configured brokers
        (`_expected_kafka_containers`, ADR D-MBK7), not just the ones present
        in this cycle's `stats_by_name` -- a broker missing from `docker
        stats` (killed) still gets a row here, with `online=False` and its
        stat fields at their `"—"`/`None` defaults, same convention
        `_build_nodes` already uses for `NodeStat.available=False`, instead
        of silently vanishing from the list."""
        controller_id = cli.active_controller_id if cli else None
        brokers: List[KafkaBrokerStat] = []
        for name in _expected_kafka_containers(params):
            node_id = _kafka_node_id(name)
            stat = stats_by_name.get(name)
            prev = self._prev_containers.get(name)

            broker = KafkaBrokerStat(
                node_id=node_id,
                container_name=name,
                online=stat is not None,
                is_controller=(controller_id is not None and node_id == controller_id),
                partitions_led=self._kafka_partitions_led.get(node_id, 0),
            )

            if stat is not None and stat.cpu_pct is not None:
                cpu_pct = max(0, round(stat.cpu_pct))
                broker.cpu_pct = cpu_pct
                broker.cpu_label = f"{cpu_pct}%"
                broker.cpu_color = _color_for_pct(cpu_pct, config.DASHBOARD_CPU_WARN_PCT, config.DASHBOARD_CPU_CRIT_PCT)

            if stat is not None and stat.mem_used_bytes is not None and stat.mem_limit_bytes:
                ram_pct = round((stat.mem_used_bytes / stat.mem_limit_bytes) * 100)
                broker.ram_pct = ram_pct
                broker.ram_label = f"{ram_pct}%"
                broker.ram_color = _color_for_pct(ram_pct, config.DASHBOARD_RAM_WARN_PCT, config.DASHBOARD_RAM_CRIT_PCT)

            if stat is not None and prev is not None and elapsed_s:
                disk_delta = None
                net_delta = None
                if stat.block_read_bytes is not None and prev.block_read_bytes is not None:
                    disk_delta = (stat.block_read_bytes - prev.block_read_bytes) + (
                        (stat.block_write_bytes or 0) - (prev.block_write_bytes or 0)
                    )
                if stat.net_rx_bytes is not None and prev.net_rx_bytes is not None:
                    net_delta = (stat.net_rx_bytes - prev.net_rx_bytes) + (
                        (stat.net_tx_bytes or 0) - (prev.net_tx_bytes or 0)
                    )
                broker.disk_io = _rate_label(disk_delta, elapsed_s)
                broker.net_io = _rate_label(net_delta, elapsed_s)

            broker.status_color = (
                config.DASHBOARD_COLOR_MASTER_BADGE
                if broker.is_controller
                else (broker.cpu_color if broker.cpu_pct is not None else config.DASHBOARD_COLOR_GREEN)
                if broker.online
                else "#8b93a3"
            )

            brokers.append(broker)
        return brokers

    def _build_job(self, app_id, current_stage, stages_raw, task_samples):
        if not app_id:
            return None, [], []

        valid_stages = [s for s in stages_raw if isinstance(s, dict)] if isinstance(stages_raw, list) else []
        total_stages = len(valid_stages)
        completed = sum(1 for s in valid_stages if s.get("status") == "COMPLETE")
        active_index = None
        for i, s in enumerate(sorted(valid_stages, key=lambda s: s.get("stageId", 0))):
            if s.get("status") == "ACTIVE":
                active_index = i + 1

        stage_number = active_index or (completed if completed else 1)
        stage_label = f"Stage {stage_number} / {total_stages}" if total_stages else "Stage —"
        stage_name = (current_stage or {}).get("name", "—") if current_stage else "—"

        completed_durations = [t.duration_s for t in task_samples if t.duration_s is not None]
        remaining = sum(1 for t in task_samples if t.duration_s is None)
        eta_result = eta.estimate(completed_durations, remaining)

        is_active_job = any(s.get("status") == "ACTIVE" for s in valid_stages)
        status_label = "Running" if is_active_job else "Completed"
        status_bg = "#eff6ff" if is_active_job else "#f0fdf4"
        status_color = "#2563eb" if is_active_job else config.DASHBOARD_COLOR_GREEN

        elapsed_ms = sum(s.get("executorRunTime", 0) or 0 for s in valid_stages)
        elapsed_label = eta.format_seconds(elapsed_ms / 1000.0) if elapsed_ms else "—"

        job = JobSummary(
            name=app_id,
            app_id=app_id,
            status_label=status_label,
            status_bg=status_bg,
            status_color=status_color,
            stage_label=stage_label,
            stage_name=stage_name,
            elapsed=elapsed_label,
            eta_label=eta_result.eta_label,
            eta_spread=eta_result.spread_label,
        )

        sorted_stages = sorted(valid_stages, key=lambda s: s.get("stageId", 0))
        weights = [max(1, s.get("executorRunTime", 0) or 0) for s in sorted_stages]
        total_weight = sum(weights) or 1
        cumulative = 0.0
        bars: List[StageBar] = []
        stage_durations: List[diagnostics.StageDuration] = []
        for s, weight in zip(sorted_stages, weights):
            width_pct = round((weight / total_weight) * 100, 1)
            state = "current" if s.get("status") == "ACTIVE" else (
                "done" if s.get("status") == "COMPLETE" else "pending"
            )
            color = {
                "current": config.DASHBOARD_COLOR_RED,
                "done": config.DASHBOARD_COLOR_GREEN,
                "pending": "#c7cad2",
            }[state]
            rt_ms = s.get("executorRunTime", 0) or 0
            duration_label = eta.format_seconds(rt_ms / 1000.0) if rt_ms else "—"
            bars.append(
                StageBar(
                    label=f"Stage {s.get('stageId')}",
                    start_pct=cumulative,
                    width_pct=width_pct,
                    duration_label=duration_label,
                    note=(s.get("name") or "")[:40],
                    color=color,
                    state=state,
                )
            )
            stage_durations.append(
                diagnostics.StageDuration(
                    label=f"Stage {s.get('stageId')}", duration_s=rt_ms / 1000.0, is_current=(state == "current")
                )
            )
            cumulative += width_pct

        return job, bars, stage_durations

    def _build_signal_cards(
        self, task_samples, skewed_ids, resource_samples, stage_durations, job, current_stage, app_ref
    ) -> List[SignalCard]:
        if job is None:
            return []
        cards: List[SignalCard] = []

        # Issue #20: every card used to hardcode `deep_link=None`, so US-5.6's
        # "deep link into the real Spark UI" criterion was never actually
        # met even though `app_client.stage_ui_url()` already exists and is
        # used for the identical purpose in `annotation.py`. All three
        # signal cards are derived from the current/most-recently-completed
        # stage's own data, so they all deep-link to that same stage page.
        # Issue #24: built from the resolved `app_ref` (correct port), not
        # the fixed `:4040` -- otherwise a `:4041`/`:4042` application's
        # deep links always pointed at the wrong driver's landing page.
        deep_link = None
        if app_ref is not None and current_stage is not None and current_stage.get("stageId") is not None:
            deep_link = app_client.stage_ui_url(
                app_ref, current_stage.get("stageId"), current_stage.get("attemptId", 0)
            )

        skew_detail = diagnostics.partition_size_signal(task_samples, skewed_ids)
        if skew_detail:
            cards.append(
                SignalCard(
                    icon=_ICON_SKEW,
                    category="Partition size distribution",
                    detail=skew_detail,
                    color=config.DASHBOARD_COLOR_RED,
                    border_color="#fecaca",
                    deep_link=deep_link,
                )
            )

        gc_detail = diagnostics.memory_gc_signal(resource_samples)
        if gc_detail:
            cards.append(
                SignalCard(
                    icon=_ICON_MEMORY,
                    category="GC / memory",
                    detail=gc_detail,
                    color=config.DASHBOARD_COLOR_AMBER,
                    border_color="#fde68a",
                    deep_link=deep_link,
                )
            )

        cp_detail = diagnostics.critical_path_signal(stage_durations)
        if cp_detail:
            cards.append(
                SignalCard(
                    icon=_ICON_CLOCK,
                    category="Stage share of runtime",
                    detail=cp_detail,
                    color="#2563eb",
                    border_color="#bfdbfe",
                    deep_link=deep_link,
                )
            )

        return cards


def _now_label() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")


def _partition_summary(task_samples: List[diagnostics.TaskSample]) -> str:
    sizes = [t.size_bytes for t in task_samples if t.size_bytes > 0]
    if not sizes:
        return ""
    avg = sum(sizes) / len(sizes)
    mx = max(sizes)
    ratio = (mx / avg) if avg else 0
    return f"{len(sizes)} partitions · avg {avg/1e6:.0f} MB · max {mx/1e6:.0f} MB ({ratio:.1f}x skew)"


# Module-level singleton -- single-user, single-process app (mirrors
# `app.lifecycle.manager.manager`).
collector = DashboardCollector()
