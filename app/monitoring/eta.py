"""Spark Playbook — stage ETA estimation (US-5.3, ADR "Component / data
design").

`avg(completed task duration) * remaining task count`, the only defensible
approach given Spark's REST API exposes no true ETA (requirements doc's
measurability note) -- a rough estimate that gets worse under skew and
ignores AQE re-planning, so this module also always returns the underlying
min/median/max spread rather than a single confident number (US-5.3 c3).

Pure function, no I/O -- easy to unit-test in isolation.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class EtaResult:
    estimating: bool  # True <=> zero completed tasks so far (US-5.3 c2) -- no numeric ETA shown
    eta_label: str  # "estimating..." or "~2m 40s"
    spread_label: Optional[str] = None  # "min 4s · median 9s · max 41s", only when estimating is False


def format_seconds(seconds: float) -> str:
    seconds = max(0, round(seconds))
    minutes, secs = divmod(seconds, 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def estimate(completed_task_durations_s: List[float], remaining_task_count: int) -> EtaResult:
    """`completed_task_durations_s` -- durations (seconds) of tasks that have
    finished in the current/most-recent stage. `remaining_task_count` -- tasks
    not yet completed in that same stage."""
    if not completed_task_durations_s:
        return EtaResult(estimating=True, eta_label="estimating...", spread_label=None)

    avg = statistics.mean(completed_task_durations_s)
    eta_seconds = avg * max(0, remaining_task_count)

    spread = (
        f"min {format_seconds(min(completed_task_durations_s))} · "
        f"median {format_seconds(statistics.median(completed_task_durations_s))} · "
        f"max {format_seconds(max(completed_task_durations_s))}"
    )

    return EtaResult(estimating=False, eta_label=f"~{format_seconds(eta_seconds)}", spread_label=spread)
