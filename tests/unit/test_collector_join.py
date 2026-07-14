"""Regression test for the executor<->container join (ADR D-D join key,
R-Dash-1) -- found by actually running a real job against a real spawned
cluster (not by code review): standalone Spark's Worker registers each
executor's `hostPort` using the container's raw bridge-network IP, not its
hostname, so `_executor_host_map()` must fall back through
`docker_stats.container_ip_map()`'s IP->name table rather than assuming
`hostPort`'s host is already a usable name (the ADR's own anticipated
"join defensively... never mis-attach" mitigation, R-Dash-1)."""
from __future__ import annotations

from app.monitoring.collector import _executor_host_map


class TestExecutorHostMapIpFallback:
    def test_driver_uses_its_reported_hostname_directly(self):
        executors_raw = [{"id": "driver", "hostPort": "spark-driver:7079"}]
        assert _executor_host_map(executors_raw, ip_to_name={}) == {"driver": "spark-driver"}

    def test_worker_executor_reporting_raw_ip_is_resolved_via_ip_map(self):
        """The real-world case found live: worker executors report an IP,
        not a hostname."""
        executors_raw = [{"id": "0", "hostPort": "172.19.0.4:7079"}]
        ip_to_name = {"172.19.0.4": "spark-worker-1"}

        result = _executor_host_map(executors_raw, ip_to_name)

        assert result == {"0": "spark-worker-1"}

    def test_unresolvable_host_falls_back_to_the_raw_value_not_mis_attached(self):
        """No matching IP entry -- ADR R-Dash-1's "fall back to blank on no
        match, never mis-attach": the raw, unmapped host is kept (so it
        simply won't match any known node card) rather than silently
        guessing a container name."""
        executors_raw = [{"id": "0", "hostPort": "10.0.0.99:7079"}]
        result = _executor_host_map(executors_raw, ip_to_name={})
        assert result == {"0": "10.0.0.99"}

    def test_executor_already_reporting_a_real_hostname_is_used_as_is(self):
        executors_raw = [{"id": "0", "hostPort": "spark-worker-1:7079"}]
        result = _executor_host_map(executors_raw, ip_to_name={"172.19.0.4": "spark-worker-1"})
        assert result == {"0": "spark-worker-1"}
