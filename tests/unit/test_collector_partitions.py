"""Regression test for issue #17: `_build_partitions()` should show elapsed
time (`now - launchTime`) for RUNNING tasks with no `duration` yet, instead
of the bare "running..." placeholder -- US-5.2 c1's parenthetical "or
elapsed time for still-running tasks"."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.monitoring.collector import DashboardCollector, retries_by_index


def _iso_gmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000") + "GMT"


class TestRetriesByIndex:
    """US-C9 (issue #49): `retries_by_index()` was extracted out of
    `DashboardCollector._build_partitions()`'s inline loop into a
    module-level function, reused by `app/web/routes/annotation.py`'s new
    task-retry evidence pull -- pins the extraction is behavior-preserving
    (max `attempt` seen per task `index`, 0 for a never-retried partition)."""

    def test_max_attempt_per_index(self):
        tasks_raw = [
            {"index": 0, "attempt": 0},
            {"index": 0, "attempt": 1},  # retried once
            {"index": 1, "attempt": 0},  # never retried
        ]
        assert retries_by_index(tasks_raw) == {0: 1, 1: 0}

    def test_missing_index_skipped(self):
        assert retries_by_index([{"attempt": 0}, {"index": None, "attempt": 1}]) == {}

    def test_non_dict_entries_skipped(self):
        assert retries_by_index(["not-a-task", {"index": 2, "attempt": 0}]) == {2: 0}

    def test_missing_attempt_defaults_to_zero(self):
        assert retries_by_index([{"index": 0}]) == {0: 0}

    def test_empty_input(self):
        assert retries_by_index([]) == {}


class TestBuildPartitionsRetriesFieldPreserved:
    """Pins `_build_partitions()`'s own `retries` field (dashboard's
    straggler/retry coloring) still reflects `retries_by_index()` after the
    extraction -- the pre-extraction inline loop computed the exact same
    thing, this is the regression guard for issue #49's refactor."""

    def test_retried_partition_carries_nonzero_retries(self):
        tasks_raw = [
            {
                "index": 0,
                "attempt": 0,
                "host": "spark-worker-1",
                "status": "FAILED",
                "duration": 1000,
                "taskMetrics": {},
            },
            {
                "index": 0,
                "attempt": 1,
                "host": "spark-worker-2",
                "status": "SUCCESS",
                "duration": 2000,
                "taskMetrics": {},
            },
        ]

        _, partition_rows, _ = DashboardCollector()._build_partitions(tasks_raw, ip_to_name={})

        # Only the latest attempt (attempt 1) is kept as the partition row,
        # but it must carry the retry count from the whole index's history.
        assert len(partition_rows) == 1
        assert partition_rows[0].retries_label == "1 retries"


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
