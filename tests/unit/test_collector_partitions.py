"""Regression test for issue #17: `_build_partitions()` should show elapsed
time (`now - launchTime`) for RUNNING tasks with no `duration` yet, instead
of the bare "running..." placeholder -- US-5.2 c1's parenthetical "or
elapsed time for still-running tasks"."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.monitoring.collector import DashboardCollector


def _iso_gmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000") + "GMT"


class TestRunningTaskTimeLabel:
    def test_running_task_with_launch_time_shows_elapsed_not_placeholder(self):
        launch = datetime.now(timezone.utc) - timedelta(seconds=95)
        tasks_raw = [
            {
                "index": 0,
                "attempt": 0,
                "host": "spark-worker-1",
                "status": "RUNNING",
                "launchTime": _iso_gmt(launch),
                "taskMetrics": {},
            }
        ]

        _, partition_rows, _ = DashboardCollector()._build_partitions(tasks_raw, ip_to_name={})

        assert len(partition_rows) == 1
        # ~95s elapsed; allow a couple seconds of test-execution slack rather
        # than asserting an exact, timing-dependent second count.
        assert partition_rows[0].time_label in ("1m 35s", "1m 36s", "1m 37s")

    def test_running_task_without_launch_time_falls_back_to_placeholder(self):
        """Never fabricate a duration -- missing/unparseable `launchTime`
        keeps the honest "running..." fallback."""
        tasks_raw = [
            {
                "index": 0,
                "attempt": 0,
                "host": "spark-worker-1",
                "status": "RUNNING",
                "taskMetrics": {},
            }
        ]

        _, partition_rows, _ = DashboardCollector()._build_partitions(tasks_raw, ip_to_name={})

        assert partition_rows[0].time_label == "running…"

    def test_completed_task_still_uses_real_duration(self):
        tasks_raw = [
            {
                "index": 0,
                "attempt": 0,
                "host": "spark-worker-1",
                "status": "SUCCESS",
                "duration": 4200,
                "taskMetrics": {},
            }
        ]

        _, partition_rows, _ = DashboardCollector()._build_partitions(tasks_raw, ip_to_name={})

        assert partition_rows[0].time_label == "4s"
