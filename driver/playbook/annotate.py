"""Spark Playbook — `playbook.checkpoint()` (PLAN.md §3 "Static plan self-check — pull, never push").

This is the *only* action that starts the self-check data flow, and it is
entirely learner-initiated (G3):

    learner forms a hypothesis (markdown cell)
            |  learner *chooses* to call:
            v
      playbook.checkpoint(df, topic="join-strategies")
            |  writes df.explain(mode="formatted") text
            |  + current app-id + timestamp
            v
      <repo>/scratch/shared/annotations/<topic>/<ts_us>.json
                                                    |
                          learner *chooses* to click "Reveal self-check" in the app
                                                    v
                                    app/web/routes/annotation.py reads the newest
                                    dump for the topic and renders labels + metrics

Nothing is displayed automatically anywhere in this flow -- `checkpoint()`
only ever writes a file; the FastAPI app only ever reads one on an explicit
Reveal click (see `app/web/routes/annotation.py`).
"""
from __future__ import annotations

import contextlib
import io
import json
import time
from pathlib import Path

# Matches app/config.py::ANNOTATIONS_DIR, expressed from the container's own
# mount point rather than imported -- this module runs inside the
# `spark-driver` container's Python environment, which has no access to the
# host-side `app` package (same reasoning as `driver/jupyter_config.py`'s
# hardcoded APP_ORIGIN -- see that file's docstring). The whole repo is
# bind-mounted at /workspace (docker-compose.yml.j2), and
# app/config.py::ANNOTATIONS_DIR resolves to `<repo_root>/scratch/shared/annotations`
# on the host side of that same mount, so this path lines up byte-for-byte.
DEFAULT_SHARED_ANNOTATIONS_DIR = "/workspace/scratch/shared/annotations"


def _validate_topic_name(topic: str) -> None:
    """`topic` is joined directly into a filesystem path below with no other
    checks -- a typo like `topic="../join-strategies"` (or an absolute path,
    or an embedded path separator) would otherwise silently write outside the
    intended per-topic annotations directory with no error, which is a real
    correctness/debuggability footgun for a learner even though there's no
    security boundary to protect here (single-user, no-auth tool). Reject it
    clearly instead."""
    if not topic or "/" in topic or "\\" in topic or ".." in topic:
        raise ValueError(
            f"invalid topic={topic!r}: must be a plain topic name with no path separators or '..' "
            "(e.g. \"join-strategies\", not a path)"
        )


def checkpoint(df, topic: str, shared_dir: str = DEFAULT_SHARED_ANNOTATIONS_DIR) -> Path:
    """Writes `df.explain(mode="formatted")` output, the current Spark
    application id, and a timestamp to the shared annotation directory for
    `topic`, so the app's "Reveal self-check" action can later parse and
    annotate it (US-2.1, US-2.2). Returns the path written.

    Deliberately does *not* print/return the plan, and does not itself talk
    to the FastAPI app or the :4040 REST API -- it only writes a file. The
    learner must separately click Reveal in the app for anything to appear
    (G3 pull-not-push).
    """
    _validate_topic_name(topic)

    spark = df.sparkSession
    app_id = spark.sparkContext.applicationId

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    explain_text = buf.getvalue()

    ts = time.time()
    topic_dir = Path(shared_dir) / topic
    topic_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "topic": topic,
        "app_id": app_id,
        "timestamp": ts,
        "explain_formatted": explain_text,
    }

    # Microsecond-epoch filename: sortable lexicographically, so the app can
    # find "the newest dump" (PLAN.md §3) with a plain sorted glob -- no
    # separate index/database needed for a single-user tool. Microsecond (not
    # millisecond) resolution keeps back-to-back checkpoint() calls in the
    # same notebook run from colliding on the same filename.
    out_path = topic_dir / f"{int(ts * 1_000_000)}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path
