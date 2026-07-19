"""Tests for driver/playbook/producer.py (docs/architecture/
kafka-streaming-infra.md D5) -- the thin subprocess wrapper around
tools/kafka_producer/produce.py that #18's streaming notebook will call.

This module is meant to run *inside* the driver container against a real
`/workspace` mount and a live broker -- full integration isn't unit-testable
here (same boundary test_playbook_annotate.py already draws for its sibling
module). At the unit level: start()'s fail-loud FileNotFoundError when
PRODUCE_SCRIPT isn't found (mocking Path.exists, same pattern as
test_playbook_annotate.py's TestTopicNameIsSanitized uses tmp_path/patch
rather than a real filesystem layout), and stop()'s terminate/timeout/kill
logic against a fake Popen-like object.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

DRIVER_DIR = Path(__file__).resolve().parents[2] / "driver"
if str(DRIVER_DIR) not in sys.path:
    sys.path.insert(0, str(DRIVER_DIR))

from playbook import producer  # noqa: E402


class TestStartFailsLoudlyWhenScriptMissing:
    def test_raises_file_not_found_when_produce_script_absent(self):
        with patch.object(producer.Path, "exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="produce.py"):
                producer.start()

    def test_does_not_attempt_to_launch_a_process_when_script_missing(self):
        with patch.object(producer.Path, "exists", return_value=False):
            with patch.object(producer.subprocess, "Popen") as mock_popen:
                with pytest.raises(FileNotFoundError):
                    producer.start()
                mock_popen.assert_not_called()

    def test_launches_popen_with_expected_args_when_script_present(self):
        with patch.object(producer.Path, "exists", return_value=True):
            with patch.object(producer.subprocess, "Popen") as mock_popen:
                producer.start(topic="events", rate=50, bootstrap="kafka:9092", partitions=3,
                                key_space=8, late_frac=0.05, late_seconds=60, count=100)

        mock_popen.assert_called_once()
        args = mock_popen.call_args.args[0]
        assert args[0] == sys.executable
        assert args[1] == producer.PRODUCE_SCRIPT
        assert "--topic" in args and "events" in args
        assert "--rate" in args and "50" in args
        assert "--bootstrap" in args and "kafka:9092" in args
        assert "--count" in args and "100" in args

    def test_count_none_omits_the_count_flag(self):
        with patch.object(producer.Path, "exists", return_value=True):
            with patch.object(producer.subprocess, "Popen") as mock_popen:
                producer.start(count=None)

        args = mock_popen.call_args.args[0]
        assert "--count" not in args


class _FakePopen:
    """Stands in for subprocess.Popen: records terminate()/kill() calls and
    lets a test control whether wait() succeeds or times out."""

    def __init__(self, already_exited=False, wait_raises_timeout=False):
        self._exited = already_exited
        self._wait_raises_timeout = wait_raises_timeout
        self.terminate_called = False
        self.kill_called = False

    def poll(self):
        return 0 if self._exited else None

    def terminate(self):
        self.terminate_called = True

    def kill(self):
        self.kill_called = True
        self._exited = True

    def wait(self, timeout=None):
        if self._wait_raises_timeout and not self.kill_called:
            raise subprocess.TimeoutExpired(cmd="produce.py", timeout=timeout)
        return 0


class TestStop:
    def test_already_exited_process_is_left_alone(self):
        proc = _FakePopen(already_exited=True)
        producer.stop(proc)
        assert proc.terminate_called is False

    def test_clean_exit_sends_terminate_and_does_not_kill(self):
        proc = _FakePopen(already_exited=False, wait_raises_timeout=False)
        producer.stop(proc)
        assert proc.terminate_called is True
        assert proc.kill_called is False

    def test_timeout_forces_a_kill(self):
        proc = _FakePopen(already_exited=False, wait_raises_timeout=True)
        producer.stop(proc, timeout=0.01)
        assert proc.terminate_called is True
        assert proc.kill_called is True
