"""Spark Playbook — `driver/playbook/producer.py` (Phase 3, docs/architecture/
kafka-streaming-infra.md D5).

Thin importable wrapper around `tools/kafka_producer/produce.py`'s CLI, so
the streaming notebook (#18) can start/stop the synthetic producer from a
cell without hand-building a subprocess call. Launches `produce.py` as its
own OS process rather than calling into it as a function in the current
kernel -- D5: "independent of the streaming query's lifecycle... launched as
its own process... not from the same kernel that runs the streaming query" --
so stopping/restarting a streaming query against its checkpoint (US-3.2/
US-3.3) never stops or restarts the data feed.

Mounted into the `spark-driver` container the same way `driver/playbook/
annotate.py` already is -- the whole repo is bind-mounted at `/workspace`,
so no dedicated mount is needed. From a notebook cell:

    import sys
    sys.path.insert(0, "/workspace")
    from driver.playbook import producer

    proc = producer.start(topic="events", rate=100)
    ...
    producer.stop(proc)
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional

# Matches app/config.py's REPO_ROOT-relative path, expressed as a literal
# /workspace path rather than imported -- this module runs inside the
# `spark-driver` container's Python environment, which has no access to the
# host-side `app` package (same reasoning as `driver/playbook/annotate.py`'s
# DEFAULT_SHARED_ANNOTATIONS_DIR / `driver/jupyter_config.py`'s hardcoded
# APP_ORIGIN).
PRODUCE_SCRIPT = "/workspace/tools/kafka_producer/produce.py"

# In-cluster DNS (D3) -- the driver container is itself inside `sparkpb-net`,
# so this is the right default for a wrapper meant to run from a notebook
# cell (as opposed to produce.py's own CLI, which is also runnable from a
# host shell with --bootstrap 127.0.0.1:9092, OQ-1).
DEFAULT_BOOTSTRAP = "kafka:9092"


def start(
    topic: str = "events",
    rate: float = 100,
    bootstrap: str = DEFAULT_BOOTSTRAP,
    partitions: int = 3,
    key_space: int = 8,
    late_frac: float = 0.05,
    late_seconds: int = 60,
    count: Optional[int] = None,
) -> subprocess.Popen:
    """Starts `produce.py` as a background OS process and returns the
    handle. Call `stop(proc)` (or `proc.terminate()`) to end it --
    otherwise it keeps publishing until the driver container itself stops.
    Raises `FileNotFoundError` if the repo isn't mounted where expected
    (fail loudly rather than silently launching nothing)."""
    if not Path(PRODUCE_SCRIPT).exists():
        raise FileNotFoundError(
            f"{PRODUCE_SCRIPT} not found -- is the repo bind-mounted at /workspace?"
        )
    args = [
        sys.executable, PRODUCE_SCRIPT,
        "--topic", topic,
        "--rate", str(rate),
        "--bootstrap", bootstrap,
        "--partitions", str(partitions),
        "--key-space", str(key_space),
        "--late-frac", str(late_frac),
        "--late-seconds", str(late_seconds),
    ]
    if count is not None:
        args += ["--count", str(count)]
    return subprocess.Popen(args)


def stop(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    """Sends SIGTERM and waits briefly for a clean flush-and-exit (produce.py
    routes SIGTERM through the same shutdown path as Ctrl-C); force-kills if
    it doesn't exit in time."""
    if proc.poll() is not None:
        return  # already exited
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
