"""Spark Playbook — `driver/playbook/consumer_group.py` (US-KC4, issue #65).

Thin importable wrapper around `tools/kafka_consumer_group/member.py`, same
role `driver/playbook/producer.py` plays for `produce.py`: start/stop a
member from a notebook cell without hand-building a subprocess call. Each
member is launched as its own OS process (not a call into the current
kernel) -- deliberately, so the notebook can scale a consumer group up/down
by starting/stopping real, independent group members, and so the crash-demo
cell can `kill()` (SIGKILL) one of them for a genuine, uncatchable crash --
see `member.py`'s module docstring for why an in-kernel thread can't
simulate that faithfully.

From a notebook cell:

    import sys
    sys.path.insert(0, "/workspace")
    from driver.playbook import consumer_group

    m1, m1_log = consumer_group.start(group="cg-demo", label="m1")
    m2, m2_log = consumer_group.start(group="cg-demo", label="m2")
    ...
    consumer_group.stop(m1)   # graceful (SIGTERM)
    consumer_group.crash(m2)  # SIGKILL -- no commit, no clean group leave
    print(list(m1_log))       # stdout lines collected so far (e.g. ASSIGNED/PROCESSED)
"""
from __future__ import annotations

import subprocess
import sys
import threading
from collections import deque
from pathlib import Path
from typing import Deque, Optional, Tuple

# Bound on collected stdout lines per member -- same cap the notebook used
# when it built this deque itself, before the drain thread moved in here.
LOG_MAXLEN = 1000

# Matches producer.py's own convention exactly: this module runs inside the
# `spark-driver` container, where the whole repo is bind-mounted at
# /workspace, not the host-side `app` package.
MEMBER_SCRIPT = "/workspace/tools/kafka_consumer_group/member.py"

DEFAULT_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092"  # in-cluster DNS, 3-broker Kafka-track shape


def start(
    group: str,
    label: str = "member",
    topic: str = "consumer-groups-demo",
    bootstrap: str = DEFAULT_BOOTSTRAP,
    commit_mode: str = "manual",
    auto_commit_interval_ms: int = 5000,
    process_delay: float = 0.05,
    batch_size: int = 10,
    max_messages: Optional[int] = None,
) -> Tuple[subprocess.Popen, "Deque[str]"]:
    """Starts one consumer-group member as a background OS process and
    returns `(proc, log)`. Call `stop()` (graceful) or `crash()` (SIGKILL) on
    `proc` to end it -- otherwise it keeps consuming until the driver
    container itself stops. Raises `FileNotFoundError` if the repo isn't
    mounted where expected (fail loudly rather than silently launching
    nothing).

    `log` is a `deque` of the process's stdout/stderr lines (combined,
    stripped of trailing newlines), kept up to date by a daemon thread this
    function starts internally -- the pipe and the thread that drains it
    live together here rather than depending on every caller to remember to
    spin up its own drain thread. Without a drain, a member that writes
    enough stdout (e.g. `ASSIGNED`/`PROCESSED` lines) fills the OS pipe
    buffer and deadlocks."""
    if not Path(MEMBER_SCRIPT).exists():
        raise FileNotFoundError(
            f"{MEMBER_SCRIPT} not found -- is the repo bind-mounted at /workspace?"
        )
    args = [
        sys.executable, MEMBER_SCRIPT,
        "--group", group,
        "--label", label,
        "--topic", topic,
        "--bootstrap", bootstrap,
        "--commit-mode", commit_mode,
        "--auto-commit-interval-ms", str(auto_commit_interval_ms),
        "--process-delay", str(process_delay),
        "--batch-size", str(batch_size),
    ]
    if max_messages is not None:
        args += ["--max-messages", str(max_messages)]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    log: Deque[str] = deque(maxlen=LOG_MAXLEN)
    threading.Thread(target=_drain, args=(proc, log), daemon=True).start()
    return proc, log


def _drain(proc: subprocess.Popen, log: "Deque[str]") -> None:
    """Reads a member's combined stdout/stderr until it closes, appending
    each line to `log` -- keeps the pipe from filling and deadlocking the
    child (moved in from the notebook so every `start()` caller gets this
    for free, per the module docstring's usage example)."""
    for line in proc.stdout:
        log.append(line.rstrip("\n"))


def stop(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    """Graceful stop (SIGTERM -> member.py's own KeyboardInterrupt path ->
    clean commit/close/group-leave). Force-kills if it doesn't exit in time.
    Same terminate/timeout/kill shape as `producer.stop()`."""
    if proc.poll() is not None:
        return  # already exited
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def crash(proc: subprocess.Popen) -> None:
    """Simulates a hard crash: SIGKILL, uncatchable, no commit, no clean
    group leave -- the notebook's US-KC4 crash/restart acceptance
    criterion. Distinct from `stop()` on purpose; see module docstring."""
    if proc.poll() is not None:
        return  # already exited
    proc.kill()
    proc.wait()
