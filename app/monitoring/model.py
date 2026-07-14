"""Spark Playbook — dashboard Snapshot dataclasses (ADR "Component / data
design", D-A).

These are the shapes `collector.py` produces once per cycle and
`web/routes/dashboard.py` renders into HTML fragments. Values here are mostly
pre-formatted display strings + colors (server-rendered, not client JS, per
the ADR's "Threshold color system" note) rather than raw numbers, to keep the
Jinja2 templates dumb.

D-A is enforced structurally here: `SignalCard` has no suggestion/fix field,
and nothing in this module has a field for one. Adding such a field is a
deliberate, conscious change (see ADR R-Dash-6) — this file is the mechanism
by which that stays true by construction, not just by convention.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HistoryPoint:
    pct: int
    color: str


@dataclass
class NodeStat:
    name: str
    role: str  # "master" | "worker" | "driver"
    host: str
    available: bool  # False once the container has stopped/been removed (US-5.1 c3)

    cpu_pct: Optional[int] = None  # normalized against the container's cpu limit (ADR D-C caveat)
    cpu_label: str = "—"
    cpu_color: str = "#8b93a3"

    ram_pct: Optional[int] = None
    ram_label: str = "—"
    ram_color: str = "#8b93a3"

    disk_io: str = "—"
    net_io: str = "—"

    gc_ms: Optional[float] = None
    gc_label: str = "—"
    gc_color: str = "#4a5061"

    partition_count: Optional[int] = None  # workers only, when a job is active
    has_partitions: bool = False

    flagged: bool = False
    flag_reason: str = ""  # factual only (D-A) -- e.g. "Data skew: handling 1.7x avg partition size"

    status_color: str = "#16a34a"
    border_color: str = "#e5e7eb"
    is_master: bool = False

    cpu_history: List[HistoryPoint] = field(default_factory=list)
    ram_history: List[HistoryPoint] = field(default_factory=list)


@dataclass
class JobSummary:
    name: str
    app_id: str
    status_label: str
    status_bg: str
    status_color: str
    stage_label: str  # e.g. "Stage 3 / 5"
    stage_name: str
    elapsed: str
    eta_label: str  # "estimating..." or "~2m 40s"
    eta_spread: Optional[str] = None  # "min 4s · median 9s · max 41s"


@dataclass
class StageBar:
    label: str
    start_pct: float
    width_pct: float
    duration_label: str
    note: str
    color: str
    state: str  # "done" | "current" | "pending"


@dataclass
class PartitionRow:
    node: str
    id: str
    is_skewed: bool
    size_label: str
    size_bar_pct: int
    size_color: str
    rows_label: str
    shuffle_label: str
    time_label: str
    retries_label: str
    retries_color: str
    row_bg: str


@dataclass
class SignalCard:
    """Factual signal only (D-A) -- icon + observational category label +
    quantified detail line + a deep link. Deliberately NO suggestion/fix
    field -- see this module's docstring."""

    icon: str
    category: str  # names the *measurement*, never the remedy (D-A)
    detail: str  # factual, quantified (e.g. "worker-2 holds 3 partitions ~5x larger than the median")
    color: str
    border_color: str
    deep_link: Optional[str] = None


@dataclass
class Snapshot:
    cluster_active: bool  # manager.state == READY at sample time
    has_job: bool
    now_label: str = ""

    nodes: List[NodeStat] = field(default_factory=list)
    node_count_label: str = ""

    job: Optional[JobSummary] = None
    stages: List[StageBar] = field(default_factory=list)
    partitions: List[PartitionRow] = field(default_factory=list)
    partition_summary: str = ""
    signal_cards: List[SignalCard] = field(default_factory=list)

    has_alert: bool = False
    alert_title: str = ""
    alert_detail: str = ""  # factual only (D-A) -- no "consider..." tail
