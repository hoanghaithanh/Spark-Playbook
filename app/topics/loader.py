"""Spark Playbook — topic content loader (PLAN.md §3 manifest schema, §4 topics/loader.py).

Reads `content/<topic>/` (manifest.yaml + concept.md + notebook.ipynb) as data —
editing those files and reloading the page reflects the change with no code
change (US-1.1). The `annotation:` section of the manifest is parsed but unused
in Phase 1 (annotation engine itself is Phase 2 scope).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import markdown
import yaml

from app import config

# Matches this repo's own numbered-markdown-cell convention (every existing
# notebook -- partitioning-shuffle, join-strategies, bucketing, aqe -- already
# uses "## N. Title" markdown cells to structure the walkthrough), so the
# Notebook tab's step list (US-SH7 / topic-shell-redesign shell.html) can be
# derived straight from notebook.ipynb content instead of a new manifest
# field -- no bespoke per-topic markup or schema addition needed (G-SH1/G7).
_STEP_HEADING_RE = re.compile(r"^##\s+(\d+)\.\s*(.+?)\s*$")

# Matches this repo's own concept.md convention (every existing concept.md --
# aqe, bucketing, catalyst-plans, join-strategies, partitioning-shuffle --
# starts with "# Title" then a "## What it is" section), so the topics-index
# landing page (US-SH5) can derive a blurb from that content instead of a new
# manifest field -- same content-as-data precedent as _STEP_HEADING_RE above.
_WHAT_IT_IS_RE = re.compile(r"^##\s+What it is\s*$", re.IGNORECASE)

# Strips the exact inline markdown emphasis/code markers actually used in the
# 5 shipped concept.md "What it is" paragraphs (**bold**, *italic*, `code`) --
# blurb() renders as auto-escaped plain text (not run through the `markdown`
# library like concept_html() is), so raw markers would otherwise show up as
# literal asterisks/backticks on the topics-index card.
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_CODE_RE = re.compile(r"`(.+?)`")
_ITALIC_RE = re.compile(r"\*(.+?)\*")


class TopicNotFoundError(Exception):
    pass


@dataclass
class ClusterDefaults:
    worker_count: int = config.DEFAULTS["worker_count"]
    worker_cores: int = config.DEFAULTS["worker_cores"]
    worker_memory_gb: int = config.DEFAULTS["worker_memory_gb"]
    shuffle_partitions: int = config.DEFAULTS["shuffle_partitions"]
    aqe_enabled: bool = config.DEFAULTS["aqe_enabled"]
    # Multi-broker Kafka ADR (docs/architecture/multi-broker-kafka-cluster.md
    # D-MBK1): optional per-topic suggested broker count, pre-populating the
    # drawer's Kafka broker-count field (not a hard gate -- see Topic.requires_kafka).
    kafka_broker_count: int = config.DEFAULTS["kafka_broker_count"]


@dataclass
class WalkthroughStep:
    number: str
    title: str
    detail: str
    cell: int


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

    def blurb(self) -> str:
        """First paragraph of prose under concept.md's "## What it is"
        heading -- the topics-index landing page's (US-SH5) card blurb.
        Deliberately not a manifest field (see module docstring/G-SH1
        precedent): every existing concept.md already has this section, so
        no schema change or per-topic special-casing is needed. Returns ""
        if a concept.md doesn't follow the convention rather than raising --
        the index page should still render (blurb-less card), not 500."""
        raw = self.content_path.read_text(encoding="utf-8")
        paragraph: List[str] = []
        in_section = False
        for line in raw.splitlines():
            if _WHAT_IT_IS_RE.match(line):
                in_section = True
                continue
            if not in_section:
                continue
            stripped = line.strip()
            if stripped.startswith("#"):
                break  # next heading ends the section
            if not stripped:
                if paragraph:
                    break  # blank line ends the first paragraph
                continue
            paragraph.append(stripped)
        text = " ".join(paragraph)
        # Bold before italic -- **x** would otherwise be partially consumed
        # by the single-* pattern first, leaving stray asterisks behind.
        text = _BOLD_RE.sub(r"\1", text)
        text = _CODE_RE.sub(r"\1", text)
        text = _ITALIC_RE.sub(r"\1", text)
        return text

    def walkthrough_steps(self) -> List["WalkthroughStep"]:
        """Notebook tab step list (topic-shell redesign, US-SH7), parsed from
        this topic's own notebook.ipynb rather than a new manifest field --
        every existing notebook already numbers its walkthrough with
        "## N. Title" markdown cells (see module docstring), so this is
        content-as-data (G-SH1/G7), not bespoke per-topic markup. Malformed
        or missing notebook JSON degrades to an empty list rather than
        raising -- the Notebook tab still renders (just without steps), and
        `load_topic()` already validates notebook_path.exists() at load time
        (issue #5), so an empty result here means "no numbered steps found",
        not "notebook missing"."""
        try:
            raw = json.loads(self.notebook_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []

        steps: List[WalkthroughStep] = []
        cells = raw.get("cells", [])
        for idx, cell in enumerate(cells):
            if cell.get("cell_type") != "markdown":
                continue
            source = cell.get("source", [])
            text = "".join(source) if isinstance(source, list) else str(source)
            lines = text.splitlines()
            if not lines:
                continue
            match = _STEP_HEADING_RE.match(lines[0])
            if not match:
                continue
            detail = "\n".join(lines[1:]).strip()
            # The next *code* cell (if any) is what this step's markdown
            # introduces -- "Cell N" is the 1-indexed position in the
            # notebook file, a stable content-driven reference (not tied to
            # execution_count, which stays null for unexecuted notebooks
            # per CLAUDE.md's notebook-cleanliness convention). Skip forward
            # past any intervening markdown (e.g. an explanatory aside cell)
            # rather than assuming the very next cell is code -- all 4
            # existing notebooks happen to alternate markdown/code strictly,
            # but nothing enforces that, so a naive idx+2 could point a
            # step's badge at another markdown cell in a future notebook.
            # Falls back to the heading's own 1-indexed position -- same
            # graceful degradation the original code used when the heading
            # was the last cell in the notebook -- if no code cell follows
            # before the notebook ends.
            cell_number = idx + 1
            for lookahead_idx in range(idx + 1, len(cells)):
                if cells[lookahead_idx].get("cell_type") == "code":
                    cell_number = lookahead_idx + 1
                    break
            steps.append(
                WalkthroughStep(
                    number=match.group(1),
                    title=match.group(2),
                    detail=detail,
                    cell=cell_number,
                )
            )
        return steps


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
        kafka_broker_count=cd.get("kafka_broker_count", config.DEFAULTS["kafka_broker_count"]),
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
