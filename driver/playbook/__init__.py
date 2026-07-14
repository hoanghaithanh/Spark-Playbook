"""Spark Playbook — notebook-side helper package (PLAN.md §3, §4 `driver/playbook/`).

Mounted into the `spark-driver` container the same way `content/` and
`driver/jupyter_config.py` already are -- the whole repo is bind-mounted at
`/workspace` (see `compose/templates/docker-compose.yml.j2`), so this package
needs no dedicated mount of its own. From a notebook cell (whose kernel cwd is
not guaranteed to be `/workspace`):

    import sys
    sys.path.insert(0, "/workspace")
    from driver.playbook import checkpoint

    checkpoint(df, topic="join-strategies")
"""
from __future__ import annotations

from .annotate import checkpoint

__all__ = ["checkpoint"]
