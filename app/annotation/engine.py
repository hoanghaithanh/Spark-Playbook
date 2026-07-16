"""Spark Playbook — annotation engine (PLAN.md §3, §4 engine.py).

Maps each parsed plan-node operator (from `plan_parser.parse_operators()`) to
a concept using *only* the topic's manifest (G7 -- no hardcoded per-topic
logic here, no per-topic branching). Match precedence is most-specific-first:
`manifest.plan_nodes` rules are tried in the order declared in the manifest,
first match wins (this is what lets `content/join-strategies/manifest.yaml`
put `BroadcastExchange` ahead of the generic `Exchange` rule so the latter
doesn't swallow it -- see US-2.1's acceptance criteria and
`manifest.PlanNodeRule`'s docstring for the `requires_absent_nearby`
adjacency extension bucketing needs, US-2.4).

Any operator with no matching rule renders as unknown/unannotated (US-2.1
c3) -- never guessed.

For runtime metrics, `spotlight_stage_metrics()` extracts exactly the
`stage_metrics` keys the manifest declares for the topic, from a single
stage's REST API JSON -- values are used as returned, never re-derived
(US-2.2).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.annotation.manifest import AnnotationManifest, PlanNodeRule


@dataclass(frozen=True)
class AnnotatedNode:
    index: int
    operator: str
    concept: Optional[str]
    label: Optional[str]

    @property
    def is_known(self) -> bool:
        return self.concept is not None


def _rule_matches(rule: PlanNodeRule, index: int, operators: List[str]) -> bool:
    if rule.match not in operators[index]:
        return False
    if rule.requires_absent_nearby:
        window = operators[index + 1 : index + 1 + rule.window]
        if any(rule.requires_absent_nearby in op for op in window):
            return False
    return True


def annotate_plan(operators: List[str], manifest: AnnotationManifest) -> List[AnnotatedNode]:
    """Maps each operator in `operators` (plan_parser's flat, ordered output)
    to the first matching rule in `manifest.plan_nodes`, in manifest order.
    Unmatched operators come back with `concept=None, label=None`
    (unknown/unannotated, US-2.1 c3)."""
    annotated: List[AnnotatedNode] = []
    for i, op in enumerate(operators):
        matched_rule = None
        for rule in manifest.plan_nodes:
            if _rule_matches(rule, i, operators):
                matched_rule = rule
                break
        if matched_rule is not None:
            annotated.append(AnnotatedNode(i, op, matched_rule.concept, matched_rule.label))
        else:
            annotated.append(AnnotatedNode(i, op, None, None))
    return annotated


def spotlight_stage_metrics(stage: Dict[str, Any], manifest: AnnotationManifest) -> Dict[str, Dict[str, Any]]:
    """Extracts exactly the manifest-declared `stage_metrics` keys from a
    single stage's REST API JSON entry (`/api/v1/applications/<id>/stages`),
    tagged with whether the manifest marks each `spotlight: true`. Values are
    passed through unmodified from the REST response (US-2.2)."""
    result: Dict[str, Dict[str, Any]] = {}
    for rule in manifest.stage_metrics:
        result[rule.key] = {"value": stage.get(rule.key), "spotlight": rule.spotlight}
    return result


_QUANTILE_LABELS = ("min", "p25", "median", "p75", "max")


def spotlight_task_duration_quantiles(
    stage_detail: Optional[Dict[str, Any]], manifest: AnnotationManifest
) -> Optional[Dict[str, Any]]:
    """True per-task duration quantiles (issue #8) from a single stage
    attempt's `?withSummaries=true` detail (`fetch_stage_task_summary()`),
    distinct from `spotlight_stage_metrics()`'s stage-wide aggregates.
    `taskMetricsDistributions.duration` is Spark's 5-element per-task
    wall-clock duration distribution at quantiles `[0, 0.25, 0.5, 0.75, 1.0]`;
    mapped here to `{"min": ..., "p25": ..., "median": ..., "p75": ...,
    "max": ...}`. Returns None if the topic's manifest didn't opt in
    (`task_duration_quantiles: true`), the detail fetch failed, or the
    response isn't shaped as expected (same "unreachable vs. unexpected
    shape -> degrade gracefully" contract as the rest of this module) --
    never partially filled."""
    if not manifest.task_duration_quantiles:
        return None
    if not isinstance(stage_detail, dict):
        return None
    distributions = stage_detail.get("taskMetricsDistributions")
    if not isinstance(distributions, dict):
        return None
    duration = distributions.get("duration")
    if not (isinstance(duration, list) and len(duration) == len(_QUANTILE_LABELS)):
        return None
    return dict(zip(_QUANTILE_LABELS, duration))
