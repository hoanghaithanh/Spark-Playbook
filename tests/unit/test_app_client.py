"""Tests for app/spark_api/app_client.py (US-2.2: driver REST client, app-id
discovery, deep links; issue #24: multi-port discovery across
DRIVER_APP_UI_PORTS)."""
from __future__ import annotations

import json
import urllib.error
from contextlib import contextmanager
from unittest.mock import patch

from app import config
from app.spark_api import app_client


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@contextmanager
def _mock_urlopen(payload):
    """Every probed port answers with the same payload -- fine for tests
    that only care about a single port's data."""
    with patch("urllib.request.urlopen", return_value=_FakeResponse(payload)) as m:
        yield m


@contextmanager
def _mock_urlopen_by_port(payload_by_port: dict):
    """Different payloads per port (keyed by the literal port int in the
    URL) -- for issue #24 multi-port resolution tests. Ports not present in
    the dict are treated as unreachable (URLError)."""

    def _fake_urlopen(url, timeout=None):
        for port, payload in payload_by_port.items():
            if f":{port}/" in url:
                return _FakeResponse(payload)
        raise urllib.error.URLError("connection refused")

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen) as m:
        yield m


class TestResolveCurrentApp:
    """Issue #24: replaces the old single-port fetch_current_app_id() as the
    collector's/annotation's entry point."""

    def test_returns_ref_for_the_only_running_app(self):
        apps = [
            {
                "id": "app-current",
                "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT", "startTimeEpoch": 100}],
            }
        ]
        with _mock_urlopen(apps):
            ref = app_client.resolve_current_app()
        assert ref is not None
        assert ref.app_id == "app-current"
        assert ref.base_url == "http://localhost:4040"

    def test_no_running_application_on_any_port_returns_none(self):
        apps = [
            {"id": "app-old", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT", "startTimeEpoch": 1}]}
        ]
        with _mock_urlopen(apps):
            assert app_client.resolve_current_app() is None

    def test_all_ports_unreachable_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            assert app_client.resolve_current_app() is None

    def test_picks_the_most_recently_started_running_app_across_ports(self):
        """The exact scenario behind issue #24: a learner leaves an earlier
        topic's Jupyter kernel alive (its SparkContext holding :4040) and
        opens a second topic's notebook, whose SparkContext Spark silently
        rebinds to :4041. The dashboard must follow the most recently
        started job, not whichever port happens to be :4040."""
        older = [
            {
                "id": "app-older-on-4040",
                "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT", "startTimeEpoch": 100}],
            }
        ]
        newer = [
            {
                "id": "app-newer-on-4041",
                "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT", "startTimeEpoch": 200}],
            }
        ]
        with _mock_urlopen_by_port({4040: older, 4041: newer}):
            ref = app_client.resolve_current_app()
        assert ref is not None
        assert ref.app_id == "app-newer-on-4041"
        assert ref.base_url == "http://localhost:4041"

    def test_missing_start_time_epoch_defaults_to_zero_not_raises(self):
        apps = [{"id": "app-current", "attempts": [{}]}]
        with _mock_urlopen(apps):
            ref = app_client.resolve_current_app()
        assert ref is not None
        assert ref.app_id == "app-current"

    def test_malformed_top_level_shape_is_skipped_not_raised(self):
        with _mock_urlopen({"error": "something went wrong"}):
            assert app_client.resolve_current_app() is None

    def test_non_dict_entries_in_application_list_are_skipped_not_raised(self):
        apps = ["not-a-dict", {"id": "app-current", "attempts": [{}]}]
        with _mock_urlopen(apps):
            ref = app_client.resolve_current_app()
        assert ref is not None and ref.app_id == "app-current"

    def test_non_list_attempts_field_is_skipped_not_raised(self):
        apps = [{"id": "app-weird", "attempts": {"not": "a list"}}]
        with _mock_urlopen(apps):
            assert app_client.resolve_current_app() is None


class TestResolveApp:
    """Issue #24: the annotation Reveal flow's checkpoint-driven lookup --
    finds which port serves a *specific* (possibly already-completed)
    app_id, rather than assuming it's the most recent one."""

    def test_finds_the_port_serving_a_specific_completed_app(self):
        apps = [{"id": "app-target", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT"}]}]
        with _mock_urlopen(apps):
            ref = app_client.resolve_app("app-target")
        assert ref is not None
        assert ref.app_id == "app-target"
        assert ref.base_url == "http://localhost:4040"

    def test_finds_a_non_default_port_for_the_target_id(self):
        on_4040 = [{"id": "app-other", "attempts": [{}]}]
        on_4041 = [{"id": "app-target", "attempts": [{}]}]
        with _mock_urlopen_by_port({4040: on_4040, 4041: on_4041}):
            ref = app_client.resolve_app("app-target")
        assert ref is not None
        assert ref.base_url == "http://localhost:4041"

    def test_unknown_app_id_returns_none(self):
        apps = [{"id": "app-other", "attempts": [{}]}]
        with _mock_urlopen(apps):
            assert app_client.resolve_app("app-target") is None

    def test_all_ports_unreachable_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            assert app_client.resolve_app("app-target") is None


class TestFetchAllAppIds:
    """Issue #16 (contract), extended by issue #24 to aggregate across every
    candidate port -- distinguishes 'this app_id belongs to some currently-
    live driver session' (running OR completed, on any port) from 'no such
    app_id at all'."""

    def test_includes_both_running_and_completed_ids(self):
        apps = [
            {"id": "app-completed", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT"}]},
            {"id": "app-running", "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT"}]},
        ]
        with _mock_urlopen_by_port({4040: apps}):
            ids = app_client.fetch_all_app_ids()
        assert ids == ["app-completed", "app-running"]

    def test_aggregates_ids_across_multiple_reachable_ports(self):
        on_4040 = [{"id": "app-on-4040", "attempts": [{}]}]
        on_4041 = [{"id": "app-on-4041", "attempts": [{}]}]
        with _mock_urlopen_by_port({4040: on_4040, 4041: on_4041}):
            ids = app_client.fetch_all_app_ids()
        assert set(ids) == {"app-on-4040", "app-on-4041"}

    def test_reachable_but_empty_returns_empty_list_not_none(self):
        with _mock_urlopen([]):
            assert app_client.fetch_all_app_ids() == []

    def test_unreachable_endpoint_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            assert app_client.fetch_all_app_ids() is None

    def test_malformed_top_level_shape_returns_none(self):
        with _mock_urlopen({"error": "unexpected shape"}):
            assert app_client.fetch_all_app_ids() is None

    def test_non_dict_entries_are_skipped(self):
        apps = ["not-a-dict", {"id": "app-1", "attempts": []}]
        with _mock_urlopen_by_port({4040: apps}):
            assert app_client.fetch_all_app_ids() == ["app-1"]


class TestFetchStages:
    def test_returns_raw_stage_list(self):
        stages = [{"stageId": 0, "shuffleReadBytes": 100, "numTasks": 4}]
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with _mock_urlopen(stages):
            result = app_client.fetch_stages(app)
        assert result == stages

    def test_queries_the_app_refs_own_base_url_not_a_fixed_port(self):
        """Issue #24: the whole point of AppRef -- a :4041 application's
        stages must be fetched from :4041, not the historical :4040."""
        stages = [{"stageId": 0}]
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4041")
        captured_urls = []

        def _fake_urlopen(url, timeout=None):
            captured_urls.append(url)
            return _FakeResponse(stages)

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            app_client.fetch_stages(app)

        assert captured_urls[0].startswith("http://localhost:4041/")

    def test_unreachable_endpoint_returns_none(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_stages(app) is None


class TestFetchExecutors:
    def test_returns_raw_executor_list(self):
        executors = [{"id": "driver", "hostPort": "spark-driver:7079"}]
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with _mock_urlopen(executors):
            assert app_client.fetch_executors(app) == executors

    def test_unreachable_endpoint_returns_none(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_executors(app) is None


class TestFetchTaskList:
    def test_returns_raw_task_list(self):
        tasks = [{"taskId": 1, "index": 0, "host": "172.19.0.3"}]
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with _mock_urlopen(tasks):
            result = app_client.fetch_task_list(app, stage_id=3, attempt_id=0)
        assert result == tasks

    def test_passes_a_length_param_so_the_endpoint_does_not_silently_paginate(self):
        """Issue found by running against a real 200-task stage: the REST
        endpoint defaults to returning only the first 20 tasks unless
        `length` is passed explicitly."""
        captured_urls = []
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")

        def _fake_urlopen(url, timeout=None):
            captured_urls.append(url)
            return _FakeResponse([])

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            app_client.fetch_task_list(app, stage_id=3, attempt_id=0, length=1000)

        assert "length=1000" in captured_urls[0]

    def test_unreachable_endpoint_returns_none(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_task_list(app, stage_id=3) is None


class TestFetchStageTaskSummary:
    """Issue #8: true per-task duration quantiles via the withSummaries=true
    stage-detail endpoint, distinct from fetch_stages()'s stage list."""

    def test_returns_raw_stage_detail_with_task_metrics_distributions(self):
        detail = {
            "stageId": 3,
            "attemptId": 0,
            "taskMetricsDistributions": {
                "quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                "duration": [100.0, 200.0, 250.0, 300.0, 900.0],
            },
        }
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with _mock_urlopen(detail):
            result = app_client.fetch_stage_task_summary(app, stage_id=3, attempt_id=0)
        assert result == detail

    def test_queries_the_withsummaries_endpoint_for_the_given_stage_and_attempt(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4041")
        captured_urls = []

        def _fake_urlopen(url, timeout=None):
            captured_urls.append(url)
            return _FakeResponse({})

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            app_client.fetch_stage_task_summary(app, stage_id=5, attempt_id=1)

        assert captured_urls[0] == (
            "http://localhost:4041/api/v1/applications/app-1/stages/5/1?withSummaries=true"
        )

    def test_unreachable_endpoint_returns_none(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_stage_task_summary(app, stage_id=3) is None


class TestStageUiUrl:
    def test_deep_links_to_specific_stage_not_landing_page(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        url = app_client.stage_ui_url(app, 3, attempt_id=1)
        assert url == "http://localhost:4040/stages/stage/?id=3&attempt=1"

    def test_default_attempt_is_zero(self):
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4040")
        url = app_client.stage_ui_url(app, 5)
        assert "attempt=0" in url

    def test_uses_the_apps_own_port_not_the_historical_default(self):
        """Issue #24: a :4041/:4042 application's deep links must actually
        resolve, instead of always pointing at :4040."""
        app = app_client.AppRef(app_id="app-1", base_url="http://localhost:4041")
        url = app_client.stage_ui_url(app, 3)
        assert url.startswith("http://localhost:4041/")


def test_driver_app_ui_ports_matches_the_compose_port_mapping():
    """The candidate-port list this module probes must match the range the
    compose template actually publishes -- both a self-check on
    config.DRIVER_APP_UI_PORTS and documentation of the coupling (issue
    #24)."""
    assert config.DRIVER_APP_UI_PORTS == (4040, 4041, 4042)
