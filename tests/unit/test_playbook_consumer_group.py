"""Tests for driver/playbook/consumer_group.py (US-KC4, issue #65) -- the
thin subprocess wrapper around tools/kafka_consumer_group/member.py that the
kafka-consumers-groups notebook uses to start/stop/crash group members.
Mirrors test_playbook_producer.py's coverage shape exactly (its sibling
module, same fail-loud-missing-script + terminate/kill logic).

This module is meant to run *inside* the driver container against a real
`/workspace` mount and a live broker -- full integration isn't unit-testable
here (same boundary test_playbook_producer.py draws). At the unit level:
start()'s fail-loud FileNotFoundError, the Popen args it builds, and
stop()/crash()'s terminate/kill logic against a fake Popen-like object.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

DRIVER_DIR = Path(__file__).resolve().parents[2] / "driver"
if str(DRIVER_DIR) not in sys.path:
    sys.path.insert(0, str(DRIVER_DIR))

from playbook import consumer_group  # noqa: E402


class TestStartFailsLoudlyWhenScriptMissing:
    def test_raises_file_not_found_when_member_script_absent(self):
        with patch.object(consumer_group.Path, "exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="member.py"):
                consumer_group.start(group="g")

    def test_does_not_attempt_to_launch_a_process_when_script_missing(self):
        with patch.object(consumer_group.Path, "exists", return_value=False):
            with patch.object(consumer_group.subprocess, "Popen") as mock_popen:
                with pytest.raises(FileNotFoundError):
                    consumer_group.start(group="g")
                mock_popen.assert_not_called()

    def test_launches_popen_with_expected_args_when_script_present(self):
        with patch.object(consumer_group.Path, "exists", return_value=True):
            with patch.object(consumer_group.subprocess, "Popen") as mock_popen:
                consumer_group.start(
                    group="cg-demo", label="m1", topic="consumer-groups-demo",
                    bootstrap="kafka-1:9092,kafka-2:9092,kafka-3:9092", commit_mode="manual",
                    auto_commit_interval_ms=5000, process_delay=0.3, batch_size=10, max_messages=5,
                )

        mock_popen.assert_called_once()
        args = mock_popen.call_args.args[0]
        assert args[0] == sys.executable
        assert args[1] == consumer_group.MEMBER_SCRIPT
        assert "--group" in args and "cg-demo" in args
        assert "--label" in args and "m1" in args
        assert "--topic" in args and "consumer-groups-demo" in args
        assert "--commit-mode" in args and "manual" in args
        assert "--max-messages" in args and "5" in args

    def test_max_messages_none_omits_the_flag(self):
        with patch.object(consumer_group.Path, "exists", return_value=True):
            with patch.object(consumer_group.subprocess, "Popen") as mock_popen:
                consumer_group.start(group="g", max_messages=None)

        args = mock_popen.call_args.args[0]
        assert "--max-messages" not in args

    def test_popen_redirects_stdout_and_stderr_so_a_drain_thread_can_read_them(self):
        """The internal drain thread (see TestStartDrainsStdoutIntoLog below)
        only works if stdout is piped and stderr is merged into it -- assert
        the exact kwargs so a future edit can't silently drop the redirection
        and reintroduce the pipe-buffer deadlock this wrapper exists to
        prevent."""
        with patch.object(consumer_group.Path, "exists", return_value=True):
            with patch.object(consumer_group.subprocess, "Popen") as mock_popen:
                consumer_group.start(group="g")

        kwargs = mock_popen.call_args.kwargs
        assert kwargs["stdout"] == subprocess.PIPE
        assert kwargs["stderr"] == subprocess.STDOUT
        assert kwargs["text"] is True


class TestStartDrainsStdoutIntoLog:
    """start() must spawn its own drain thread and return `(proc, log)` --
    per the module docstring, a caller following the documented usage
    example with no manual thread has to be safe by construction, not just
    the notebook's own hand-rolled drain thread."""

    def test_returns_proc_and_a_log_that_fills_with_stdout_lines(self):
        # Fake Popen whose stdout is an iterable of lines, exercised through
        # the real start()/_drain() so the drain thread genuinely runs.
        class _FakeProcWithStdout:
            def __init__(self):
                self.stdout = iter(["[m1] ASSIGNED [0]\n", "[m1] PROCESSED key=b'k' offset=0 total=1\n"])

        with patch.object(consumer_group.Path, "exists", return_value=True):
            with patch.object(consumer_group.subprocess, "Popen", return_value=_FakeProcWithStdout()):
                proc, log = consumer_group.start(group="g")

        for _ in range(50):
            if len(log) == 2:
                break
            time.sleep(0.05)
        assert list(log) == ["[m1] ASSIGNED [0]", "[m1] PROCESSED key=b'k' offset=0 total=1"]


class _FakePopen:
    """Stands in for subprocess.Popen: records terminate()/kill() calls and
    lets a test control whether wait() succeeds or times out -- copied
    verbatim in shape from test_playbook_producer.py's fake."""

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
            raise subprocess.TimeoutExpired(cmd="member.py", timeout=timeout)
        return 0


class TestStop:
    def test_already_exited_process_is_left_alone(self):
        proc = _FakePopen(already_exited=True)
        consumer_group.stop(proc)
        assert proc.terminate_called is False

    def test_clean_exit_sends_terminate_and_does_not_kill(self):
        proc = _FakePopen(already_exited=False, wait_raises_timeout=False)
        consumer_group.stop(proc)
        assert proc.terminate_called is True
        assert proc.kill_called is False

    def test_timeout_forces_a_kill(self):
        proc = _FakePopen(already_exited=False, wait_raises_timeout=True)
        consumer_group.stop(proc, timeout=0.01)
        assert proc.terminate_called is True
        assert proc.kill_called is True


class TestCrash:
    def test_already_exited_process_is_left_alone(self):
        proc = _FakePopen(already_exited=True)
        consumer_group.crash(proc)
        assert proc.kill_called is False

    def test_kills_a_running_process_without_terminate(self):
        proc = _FakePopen(already_exited=False)
        consumer_group.crash(proc)
        assert proc.kill_called is True
        assert proc.terminate_called is False  # never a graceful path -- crash() is SIGKILL only
