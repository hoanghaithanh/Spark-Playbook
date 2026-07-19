"""Spark Playbook — annotation manifest loader/validator (PLAN.md §3 schema, §4 manifest.py).

Loads + validates the `annotation:` section of a topic's `manifest.yaml`.
This is the *only* per-topic data source `engine.py` may read from (G7 -- no
hardcoded per-topic logic in the engine itself). File discovery/YAML parsing
is delegated to `app.topics.loader` (single source of truth for manifest.yaml
I/O) -- this module only shapes and validates the `annotation:` sub-tree.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.topics import loader as topics_loader


class ManifestError(Exception):
    """Raised for a structurally invalid `annotation:` section."""


@dataclass(frozen=True)
class PlanNodeRule:
    # Matched against only the plan node's first word as tokenized by
    # `plan_parser.parse_operators()` (e.g. `Sort`, not `Sort [asc]` or
    # `Scan parquet`) -- multi-word/qualified matching is not supported
    # (issue #31). If a topic needs to distinguish two operators that share a
    # first word, use `stage_metrics`/`task_duration_quantiles` instead, as
    # Serialization Formats (#30) and Skew & Salting (#35) both did.
    match: str
    concept: str
    label: str
    # Optional adjacency qualifier -- a manifest-driven extension of PLAN.md's
    # base schema (still fully data-driven, no engine-side per-topic logic).
    # Motivating case: content/bucketing/manifest.yaml distinguishes a
    # co-partitioned `SortMergeJoin` (no shuffle) from a standard sort-merge
    # join (US-2.4) without either concept being hardcoded into engine.py.
    #
    # If set, this rule only matches a node when no operator whose name
    # contains `requires_absent_nearby` appears within the next `window`
    # operators in plan order. `explain(mode="formatted")` prints a join's
    # child `Exchange` nodes (if any) *after* the join node itself, in the
    # top-down tree order `plan_parser.parse_operators()` preserves -- so
    # "no Exchange within the next N nodes" is a reliable, purely
    # manifest-driven way to express "this join has no shuffling child".
    requires_absent_nearby: Optional[str] = None
    window: int = 8


@dataclass(frozen=True)
class StageMetricRule:
    key: str
    spotlight: bool = False


@dataclass(frozen=True)
class AnnotationManifest:
    topic_id: str
    plan_nodes: List[PlanNodeRule] = field(default_factory=list)
    stage_metrics: List[StageMetricRule] = field(default_factory=list)
    # US-C10/US-C3 (Decision A, docs/architecture/topic-shell-redesign.md):
    # same shape as stage_metrics -- a list of REST-field keys to spotlight,
    # just sourced from `/api/v1/applications/<id>/executors` (per-executor)
    # instead of `/stages` (per-stage). Reuses StageMetricRule rather than a
    # parallel dataclass since the shape (key + spotlight bool) is identical.
    executor_metrics: List[StageMetricRule] = field(default_factory=list)
    # Issue #8: distinct from stage_metrics (which spotlights single-value
    # REST fields as-is) -- opts a topic into the true per-task duration
    # quantile distribution (min/p25/median/p75/max), which needs a second
    # REST call per stage (`fetch_stage_task_summary`) rather than a key
    # lookup on the stage list already fetched for stage_metrics. A single
    # boolean, not a list of keys like stage_metrics: there's exactly one
    # quantile distribution Spark's REST API exposes this way (task
    # duration), so a per-key list would be a parallel mechanism with
    # nothing else to key on.
    task_duration_quantiles: bool = False
    # US-C9 (Decision A, docs/architecture/topic-shell-redesign.md): gates a
    # reveal-time per-stage `fetch_task_list()` pull, same optional-boolean
    # shape as task_duration_quantiles above (there's exactly one task-retry
    # signal to opt into, not a list of keys). Every stage is checked (like
    # stage_metrics, unlike executor_metrics' single per-app pull) because at
    # Reveal time it isn't known in advance which stage the killed worker's
    # tasks landed in.
    task_retry_evidence: bool = False


def _parse_plan_node(raw: Any, topic_id: str, index: int) -> PlanNodeRule:
    if not isinstance(raw, dict):
        raise ManifestError(f"{topic_id}: annotation.plan_nodes[{index}] must be a mapping")

    match = raw.get("match")
    label = raw.get("label")
    concept = raw.get("concept")
    if not match or not isinstance(match, str):
        raise ManifestError(f"{topic_id}: annotation.plan_nodes[{index}] is missing a string 'match'")
    if not concept or not isinstance(concept, str):
        raise ManifestError(f"{topic_id}: annotation.plan_nodes[{index}] ({match!r}) is missing a string 'concept'")
    if not label or not isinstance(label, str):
        raise ManifestError(f"{topic_id}: annotation.plan_nodes[{index}] ({match!r}) is missing a string 'label'")

    requires_absent_nearby = raw.get("requires_absent_nearby")
    if requires_absent_nearby is not None and not isinstance(requires_absent_nearby, str):
        raise ManifestError(
            f"{topic_id}: annotation.plan_nodes[{index}] ({match!r}) 'requires_absent_nearby' must be a string"
        )

    window = int(raw.get("window", 8))
    if requires_absent_nearby is not None and window <= 0:
        # engine._rule_matches() slices operators[index+1 : index+1+window] to
        # look for a disqualifying nearby operator -- for window<=0 that slice
        # is always empty, so the adjacency check can never find anything and
        # requires_absent_nearby silently becomes a permanent no-op (the rule
        # then matches unconditionally). Reject at load time rather than let a
        # typo'd/misunderstood window value produce a manifest that always
        # mislabels a real shuffle as a no-shuffle case.
        raise ManifestError(
            f"{topic_id}: annotation.plan_nodes[{index}] ({match!r}) has 'requires_absent_nearby' but a "
            f"non-positive 'window'={window}, which would silently disable the adjacency check"
        )

    return PlanNodeRule(
        match=match,
        concept=concept,
        label=label,
        requires_absent_nearby=requires_absent_nearby,
        window=window,
    )


def _parse_metric_rule(raw: Any, topic_id: str, section: str, index: int) -> StageMetricRule:
    """Shared parser for `stage_metrics` and `executor_metrics` -- both are a
    plain list of `{key, spotlight}` mappings, identical shape, just sourced
    from a different REST endpoint downstream (see AnnotationManifest's
    `executor_metrics` docstring)."""
    if not isinstance(raw, dict):
        raise ManifestError(f"{topic_id}: annotation.{section}[{index}] must be a mapping")

    key = raw.get("key")
    if not key or not isinstance(key, str):
        raise ManifestError(f"{topic_id}: annotation.{section}[{index}] is missing a string 'key'")

    return StageMetricRule(key=key, spotlight=bool(raw.get("spotlight", False)))


def load_annotation_manifest(topic_id: str) -> AnnotationManifest:
    """Loads and validates just the `annotation:` section for `topic_id`.

    Raises `app.topics.loader.TopicNotFoundError` if the topic itself doesn't
    exist, or `ManifestError` if its `annotation:` section is structurally
    invalid.
    """
    topic = topics_loader.load_topic(topic_id)
    raw: Dict[str, Any] = topic.annotation or {}

    plan_nodes_raw = raw.get("plan_nodes") or []
    if not isinstance(plan_nodes_raw, list):
        raise ManifestError(f"{topic_id}: annotation.plan_nodes must be a list")
    plan_nodes = [_parse_plan_node(r, topic_id, i) for i, r in enumerate(plan_nodes_raw)]

    stage_metrics_raw = raw.get("stage_metrics") or []
    if not isinstance(stage_metrics_raw, list):
        raise ManifestError(f"{topic_id}: annotation.stage_metrics must be a list")
    stage_metrics = [_parse_metric_rule(r, topic_id, "stage_metrics", i) for i, r in enumerate(stage_metrics_raw)]

    executor_metrics_raw = raw.get("executor_metrics") or []
    if not isinstance(executor_metrics_raw, list):
        raise ManifestError(f"{topic_id}: annotation.executor_metrics must be a list")
    executor_metrics = [
        _parse_metric_rule(r, topic_id, "executor_metrics", i) for i, r in enumerate(executor_metrics_raw)
    ]

    task_duration_quantiles = bool(raw.get("task_duration_quantiles", False))
    task_retry_evidence = bool(raw.get("task_retry_evidence", False))

    return AnnotationManifest(
        topic_id=topic_id,
        plan_nodes=plan_nodes,
        stage_metrics=stage_metrics,
        executor_metrics=executor_metrics,
        task_duration_quantiles=task_duration_quantiles,
        task_retry_evidence=task_retry_evidence,
    )
