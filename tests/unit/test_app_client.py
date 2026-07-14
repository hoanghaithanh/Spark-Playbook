"""Tests for app/spark_api/app_client.py (US-2.2: :4040 REST client, app-id discovery, deep links)."""
from __future__ import annotations

import json
import urllib.error
from contextlib import contextmanager
from unittest.mock import patch

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
    with patch("urllib.request.urlopen", return_value=_FakeResponse(payload)) as m:
        yield m


class TestFetchCurrentAppId:
    def test_returns_id_of_running_attempt(self):
        apps = [
            {"id": "app-old", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT"}]},
            {"id": "app-current", "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT"}]},
        ]
        with _mock_urlopen(apps):
            assert app_client.fetch_current_app_id() == "app-current"

    def test_missing_end_time_counts_as_running(self):
        apps = [{"id": "app-current", "attempts": [{}]}]
        with _mock_urlopen(apps):
            assert app_client.fetch_current_app_id() == "app-current"

    def test_no_running_application_returns_none(self):
        apps = [{"id": "app-old", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT"}]}]
        with _mock_urlopen(apps):
            assert app_client.fetch_current_app_id() is None

    def test_empty_application_list_returns_none(self):
        with _mock_urlopen([]):
            assert app_client.fetch_current_app_id() is None

    def test_unreachable_endpoint_returns_none_not_raises(self):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            assert app_client.fetch_current_app_id() is None

    def test_malformed_top_level_shape_returns_none_not_raises(self):
        """Issue #13: an unexpected top-level shape (e.g. an error object
        instead of a list of applications) must degrade the same as
        'unreachable', not raise an uncaught AttributeError from iterating a
        dict's keys as if they were application objects."""
        with _mock_urlopen({"error": "something went wrong"}):
            assert app_client.fetch_current_app_id() is None

    def test_non_dict_entries_in_application_list_are_skipped_not_raised(self):
        apps = ["not-a-dict", {"id": "app-current", "attempts": [{}]}]
        with _mock_urlopen(apps):
            assert app_client.fetch_current_app_id() == "app-current"

    def test_non_list_attempts_field_is_skipped_not_raised(self):
        apps = [{"id": "app-weird", "attempts": {"not": "a list"}}]
        with _mock_urlopen(apps):
            assert app_client.fetch_current_app_id() is None


class TestFetchAllAppIds:
    """Issue #16: distinguishes 'this app_id belongs to the currently-live
    driver session' (running OR completed) from 'no such app_id at all'."""

    def test_includes_both_running_and_completed_ids(self):
        apps = [
            {"id": "app-completed", "attempts": [{"endTime": "2026-01-01T00:00:00.000GMT"}]},
            {"id": "app-running", "attempts": [{"endTime": "1969-12-31T23:59:59.999GMT"}]},
        ]
        with _mock_urlopen(apps):
            ids = app_client.fetch_all_app_ids()
        assert ids == ["app-completed", "app-running"]

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
        with _mock_urlopen(apps):
            assert app_client.fetch_all_app_ids() == ["app-1"]


class TestFetchStages:
    def test_returns_raw_stage_list(self):
        stages = [{"stageId": 0, "shuffleReadBytes": 100, "numTasks": 4}]
        with _mock_urlopen(stages):
            result = app_client.fetch_stages("app-1")
        assert result == stages

    def test_unreachable_endpoint_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_stages("app-1") is None


class TestFetchExecutors:
    def test_returns_raw_executor_list(self):
        executors = [{"id": "driver", "hostPort": "spark-driver:7079"}]
        with _mock_urlopen(executors):
            assert app_client.fetch_executors("app-1") == executors

    def test_unreachable_endpoint_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_executors("app-1") is None


class TestFetchTaskList:
    def test_returns_raw_task_list(self):
        tasks = [{"taskId": 1, "index": 0, "host": "172.19.0.3"}]
        with _mock_urlopen(tasks):
            result = app_client.fetch_task_list("app-1", stage_id=3, attempt_id=0)
        assert result == tasks

    def test_passes_a_length_param_so_the_endpoint_does_not_silently_paginate(self):
        """Issue found by running against a real 200-task stage: the REST
        endpoint defaults to returning only the first 20 tasks unless
        `length` is passed explicitly."""
        captured_urls = []

        def _fake_urlopen(url, timeout=None):
            captured_urls.append(url)
            return _FakeResponse([])

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            app_client.fetch_task_list("app-1", stage_id=3, attempt_id=0, length=1000)

        assert "length=1000" in captured_urls[0]

    def test_unreachable_endpoint_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            assert app_client.fetch_task_list("app-1", stage_id=3) is None


class TestStageUiUrl:
    def test_deep_links_to_specific_stage_not_landing_page(self):
        url = app_client.stage_ui_url(3, attempt_id=1)
        assert url == "http://localhost:4040/stages/stage/?id=3&attempt=1"

    def test_default_attempt_is_zero(self):
        url = app_client.stage_ui_url(5)
        assert "attempt=0" in url
