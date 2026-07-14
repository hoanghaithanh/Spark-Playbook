"""Spark Playbook — dashboard signal derivation (ADR D-A, US-5.4).

PURE signal derivation ONLY. Every function here returns a quantified,
factual observation string (or a flag) -- never a fix, a cause, or a
recommendation. This is the single most important constraint in this
feature (ADR D-A, "the load-bearing decision"); see `model.py`'s
`SignalCard` docstring for the structural half of this guarantee (no field
exists to put a suggestion in even if someone wanted to).

No I/O, no Docker/Spark clients imported here -- everything is plain data in,
plain data (numbers + factual strings) out, so this is trivial to unit-test
in isolation from `collector.py`'s orchestration.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from app import config


@dataclass
class TaskSample:
    node: str
    task_id: str
    size_bytes: int  # input + shuffle-read bytes, this feature's task~=partition stand-in
    duration_s: Optional[float]  # None while still running
    retries: int = 0


def skewed_task_ids(
    tasks: List[TaskSample],
    multiple: float = config.DASHBOARD_SKEW_MEDIAN_MULTIPLE,
) -> Set[str]:
    """Task ids whose `size_bytes` is at or above `multiple` times the
    median size across all tasks in the same stage (US-5.4 c1 -- "one task's
    input/shuffle bytes are markedly larger than the others")."""
    sizes = [t.size_bytes for t in tasks if t.size_bytes > 0]
    if len(sizes) < 2:
        return set()
    median = statistics.median(sizes)
    if median <= 0:
        return set()
    return {t.task_id for t in tasks if t.size_bytes >= multiple * median}


def node_skew_reasons(tasks: List[TaskSample], skewed_ids: Set[str]) -> Dict[str, str]:
    """Per-node factual flag reason for nodes holding >=1 skewed task, e.g.
    "Data skew: handling 1.7x avg partition size" (matches the mockup's own
    factual phrasing, kept verbatim since it names the measurement, not a
    remedy -- D-A)."""
    sizes = [t.size_bytes for t in tasks if t.size_bytes > 0]
    if not sizes:
        return {}
    median = statistics.median(sizes)
    if median <= 0:
        return {}

    reasons: Dict[str, str] = {}
    by_node: Dict[str, List[TaskSample]] = {}
    for t in tasks:
        by_node.setdefault(t.node, []).append(t)

    for node, node_tasks in by_node.items():
        node_skewed = [t for t in node_tasks if t.task_id in skewed_ids]
        if not node_skewed:
            continue
        max_ratio = max(t.size_bytes / median for t in node_skewed)
        reasons[node] = f"Data skew: handling {max_ratio:.1f}x avg partition size"
    return reasons


def partition_size_signal(tasks: List[TaskSample], skewed_ids: Set[str]) -> Optional[str]:
    """Factual detail line for the "Partition size distribution" signal card
    (US-5.4 -- category names the measurement, not the fix)."""
    if not skewed_ids:
        return None
    sizes = [t.size_bytes for t in tasks if t.size_bytes > 0]
    if not sizes:
        return None
    median = statistics.median(sizes)
    if median <= 0:
        return None

    by_node: Dict[str, int] = {}
    max_ratio = 0.0
    for t in tasks:
        if t.task_id in skewed_ids:
            by_node[t.node] = by_node.get(t.node, 0) + 1
            max_ratio = max(max_ratio, t.size_bytes / median)

    if not by_node:
        return None
    worst_node = max(by_node, key=by_node.get)
    count = by_node[worst_node]
    plural = "s" if count != 1 else ""
    return f"{worst_node} holds {count} partition{plural} ~{max_ratio:.1f}x larger than the cluster median."


@dataclass
class NodeResourceSample:
    node: str
    cpu_pct: Optional[float]
    ram_pct: Optional[float]
    gc_ms: Optional[float]


def node_imbalance_reasons(
    samples: List[NodeResourceSample],
    warn_pct: float = config.DASHBOARD_CPU_WARN_PCT,
    crit_pct: float = config.DASHBOARD_CPU_CRIT_PCT,
) -> Dict[str, str]:
    """Flags a node whose CPU is at/above `crit_pct` while at least one other
    node in the same sample set sits below `warn_pct` at the same moment
    (US-5.4 c2 -- "one worker's utilization sitting near saturation while
    another is comparatively idle... both visible together")."""
    valid = [s for s in samples if s.cpu_pct is not None]
    if len(valid) < 2:
        return {}

    reasons: Dict[str, str] = {}
    for s in valid:
        if s.cpu_pct is None or s.cpu_pct < crit_pct:
            continue
        idle_peers = [o for o in valid if o.node != s.node and o.cpu_pct is not None and o.cpu_pct < warn_pct]
        if idle_peers:
            idlest = min(idle_peers, key=lambda o: o.cpu_pct)
            reasons[s.node] = f"CPU saturated ({s.cpu_pct:.0f}%) while {idlest.node} is idle ({idlest.cpu_pct:.0f}%)"
    return reasons


def memory_gc_signal(samples: List[NodeResourceSample]) -> Optional[str]:
    """Factual detail line for the "GC / memory" signal card -- the node with
    the highest GC time and its RAM%, only when GC crosses the amber
    threshold (otherwise there's nothing noteworthy to surface)."""
    candidates = [s for s in samples if s.gc_ms is not None and s.gc_ms > config.DASHBOARD_GC_WARN_MS]
    if not candidates:
        return None
    worst = max(candidates, key=lambda s: s.gc_ms)
    ram_part = f", RAM at {worst.ram_pct:.0f}%" if worst.ram_pct is not None else ""
    return f"{worst.node} GC time at {worst.gc_ms:.0f}ms{ram_part}."


@dataclass
class StageDuration:
    label: str
    duration_s: float
    is_current: bool


def critical_path_signal(stages: List[StageDuration]) -> Optional[str]:
    """Factual detail line for the "Stage share of runtime" signal card --
    which stage accounts for the largest share of elapsed runtime so far."""
    total = sum(s.duration_s for s in stages if s.duration_s > 0)
    if total <= 0:
        return None
    longest = max(stages, key=lambda s: s.duration_s)
    if longest.duration_s <= 0:
        return None
    share_pct = (longest.duration_s / total) * 100
    current_note = " and is the current stage" if longest.is_current else ""
    return f"{longest.label} is {share_pct:.0f}% of total runtime so far{current_note}."
