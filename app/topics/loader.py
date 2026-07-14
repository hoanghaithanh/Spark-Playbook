"""Spark Playbook — topic content loader (PLAN.md §3 manifest schema, §4 topics/loader.py).

Reads `content/<topic>/` (manifest.yaml + concept.md + notebook.ipynb) as data —
editing those files and reloading the page reflects the change with no code
change (US-1.1). The `annotation:` section of the manifest is parsed but unused
in Phase 1 (annotation engine itself is Phase 2 scope).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import markdown
import yaml

from app import config


class TopicNotFoundError(Exception):
    pass


@dataclass
class ClusterDefaults:
    worker_count: int = config.DEFAULTS["worker_count"]
    worker_cores: int = config.DEFAULTS["worker_cores"]
    worker_memory_gb: int = config.DEFAULTS["worker_memory_gb"]
    shuffle_partitions: int = config.DEFAULTS["shuffle_partitions"]
    aqe_enabled: bool = config.DEFAULTS["aqe_enabled"]


@dataclass
class Topic:
    id: str
    title: str
    order: int
    content_path: Path
    notebook_path: Path
    cluster_defaults: ClusterDefaults
    requires_kafka: bool
    annotation: Dict[str, Any] = field(default_factory=dict)

    @property
    def notebook_relpath(self) -> str:
        """Path relative to the repo root, as mounted at /workspace in every
        container — used to deep-link JupyterLab (US-1.3)."""
        return f"content/{self.id}/{self.notebook_path.name}"

    def concept_html(self) -> str:
        raw = self.content_path.read_text(encoding="utf-8")
        return markdown.markdown(raw, extensions=["fenced_code", "tables"])


def _topic_dir(topic_id: str) -> Path:
    d = config.CONTENT_DIR / topic_id
    if not d.is_dir():
        raise TopicNotFoundError(f"No such topic: {topic_id!r}")
    return d


def load_topic(topic_id: str) -> Topic:
    topic_dir = _topic_dir(topic_id)
    manifest_path = topic_dir / "manifest.yaml"
    if not manifest_path.exists():
        raise TopicNotFoundError(f"Topic {topic_id!r} has no manifest.yaml")

    try:
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        # Genuinely malformed YAML (bad indentation, unterminated flow
        # sequence, etc.) used to propagate a raw yaml.YAMLError -- not a
        # TopicNotFoundError, so main.py's global exception handler (issue #4)
        # didn't catch it and it surfaced as an unhandled 500 across every
        # topic/annotation route (issue #10). Re-raise as TopicNotFoundError,
        # matching the existing "fail clearly at load time" precedent set by
        # the missing-manifest/missing-notebook checks in this function.
        raise TopicNotFoundError(f"Topic {topic_id!r} has a malformed manifest.yaml: {exc}") from exc

    cd = data.get("cluster_defaults") or {}
    cluster_defaults = ClusterDefaults(
        worker_count=cd.get("worker_count", config.DEFAULTS["worker_count"]),
        worker_cores=cd.get("worker_cores", config.DEFAULTS["worker_cores"]),
        worker_memory_gb=cd.get("worker_memory_gb", config.DEFAULTS["worker_memory_gb"]),
        shuffle_partitions=cd.get("shuffle_partitions", config.DEFAULTS["shuffle_partitions"]),
        aqe_enabled=cd.get("aqe_enabled", config.DEFAULTS["aqe_enabled"]),
    )

    notebook_path = topic_dir / data.get("notebook", "notebook.ipynb")
    if not notebook_path.exists():
        # Unlike concept.md (which fails loudly the moment concept_html() is
        # called), a manifest with a typo'd/missing `notebook:` path used to
        # load silently and only surface later as a 404 inside the Jupyter
        # iframe (issue #5). Fail clearly at load time instead, consistent
        # with how a missing manifest.yaml is already handled above.
        raise TopicNotFoundError(
            f"Topic {topic_id!r} declares notebook {notebook_path!r} but that file does not exist"
        )

    return Topic(
        id=data.get("id", topic_id),
        title=data.get("title", topic_id),
        order=data.get("order", 0),
        content_path=topic_dir / data.get("content", "concept.md"),
        notebook_path=notebook_path,
        cluster_defaults=cluster_defaults,
        requires_kafka=bool(data.get("requires_kafka", False)),
        annotation=data.get("annotation") or {},
    )


def list_topics() -> List[Topic]:
    if not config.CONTENT_DIR.is_dir():
        return []
    topics = []
    for child in config.CONTENT_DIR.iterdir():
        if child.is_dir() and (child / "manifest.yaml").exists():
            topics.append(load_topic(child.name))
    return sorted(topics, key=lambda t: t.order)
