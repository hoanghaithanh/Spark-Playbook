"""Tests for app/annotation/engine.py (US-2.1 mapping precedence, US-2.2 spotlighting)."""
from __future__ import annotations

from app.annotation.engine import (
    annotate_plan,
    spotlight_executor_metrics,
    spotlight_stage_metrics,
    spotlight_task_duration_quantiles,
)
from app.annotation.manifest import AnnotationManifest, PlanNodeRule, StageMetricRule


def _manifest(plan_nodes, stage_metrics=None, executor_metrics=None, task_duration_quantiles=False):
    return AnnotationManifest(
        topic_id="test-topic",
        plan_nodes=plan_nodes,
        stage_metrics=stage_metrics or [],
        executor_metrics=executor_metrics or [],
        task_duration_quantiles=task_duration_quantiles,
    )


class TestAnnotatePlanPrecedence:
    def test_most_specific_rule_wins_over_generic(self):
        """BroadcastExchange must not be swallowed by a generic Exchange rule
        (PLAN.md §3, US-2.1) -- this only works if the manifest declares the
        specific rule first and the engine takes the first match."""
        manifest = _manifest(
            [
                PlanNodeRule(match="BroadcastExchange", concept="broadcast-exchange", label="Broadcast of small side"),
                PlanNodeRule(match="Exchange", concept="shuffle-boundary", label="Shuffle boundary"),
            ]
        )
        annotated = annotate_plan(["BroadcastExchange", "Exchange"], manifest)
        assert annotated[0].concept == "broadcast-exchange"
        assert annotated[1].concept == "shuffle-boundary"

    def test_generic_rule_first_would_swallow_specific_one(self):
        """Sanity-check the inverse: if the manifest orders the generic rule
        first, precedence follows manifest order (first match wins) -- this
        documents *why* topic manifests must order specific-before-generic,
        it isn't the engine silently 'fixing' bad manifest order."""
        manifest = _manifest(
            [
                PlanNodeRule(match="Exchange", concept="shuffle-boundary", label="Shuffle boundary"),
                PlanNodeRule(match="BroadcastExchange", concept="broadcast-exchange", label="Broadcast of small side"),
            ]
        )
        annotated = annotate_plan(["BroadcastExchange"], manifest)
        assert annotated[0].concept == "shuffle-boundary"

    def test_unmapped_operator_is_unknown(self):
        manifest = _manifest([PlanNodeRule(match="Exchange", concept="shuffle-boundary", label="Shuffle boundary")])
        annotated = annotate_plan(["HashAggregate"], manifest)
        assert annotated[0].concept is None
        assert annotated[0].label is None
        assert annotated[0].is_known is False

    def test_index_and_operator_preserved_in_order(self):
        manifest = _manifest([])
        annotated = annotate_plan(["Filter", "Scan"], manifest)
        assert [(n.index, n.operator) for n in annotated] == [(0, "Filter"), (1, "Scan")]


class TestRequiresAbsentNearby:
    def test_bucketed_join_with_no_exchange_matches_copartitioned_rule(self):
        manifest = _manifest(
            [
                PlanNodeRule(
                    match="SortMergeJoin",
                    concept="co-partitioned-join",
                    label="Co-partitioned join (bucketed, no shuffle)",
                    requires_absent_nearby="Exchange",
                    window=5,
                ),
                PlanNodeRule(match="SortMergeJoin", concept="sort-merge-join", label="Sort-merge join"),
            ]
        )
        operators = ["SortMergeJoin", "Sort", "Scan", "Sort", "Scan"]  # no Exchange within window
        annotated = annotate_plan(operators, manifest)
        assert annotated[0].concept == "co-partitioned-join"

    def test_standard_join_with_nearby_exchange_falls_through_to_generic_rule(self):
        manifest = _manifest(
            [
                PlanNodeRule(
                    match="SortMergeJoin",
                    concept="co-partitioned-join",
                    label="Co-partitioned join (bucketed, no shuffle)",
                    requires_absent_nearby="Exchange",
                    window=5,
                ),
                PlanNodeRule(match="SortMergeJoin", concept="sort-merge-join", label="Sort-merge join"),
            ]
        )
        operators = ["SortMergeJoin", "Sort", "Exchange", "Scan"]  # Exchange within window disqualifies rule 1
        annotated = annotate_plan(operators, manifest)
        assert annotated[0].concept == "sort-merge-join"

    def test_window_zero_silently_disables_the_adjacency_check(self):
        """GAP found in QA gap-analysis pass (Phase 2 review), reproduced directly
        against engine.py (see test_manifest.py's companion
        test_non_positive_window_on_requires_absent_nearby_rule_rejected for the
        load-time validation half, issue #11): `window=0` makes
        `operators[index+1 : index+1+0]` always the empty slice, so
        `requires_absent_nearby` can never find anything nearby and the rule
        matches unconditionally -- even with an Exchange sitting immediately
        next to the SortMergeJoin, which is precisely the shuffle the rule exists
        to detect and exclude. This test pins engine.py's runtime behavior for a
        raw `PlanNodeRule(window=0)` constructed directly (bypassing
        manifest.py's loader, which now rejects window<=0 at load time per issue
        #11) -- it documents that engine.py itself has no independent guard
        against this, by design (manifest.py is the single validation point),
        so this footgun doesn't regress silently if that split ever changes.
        """
        manifest = _manifest(
            [
                PlanNodeRule(
                    match="SortMergeJoin",
                    concept="co-partitioned-join",
                    label="Co-partitioned join (bucketed, no shuffle)",
                    requires_absent_nearby="Exchange",
                    window=0,
                ),
                PlanNodeRule(match="SortMergeJoin", concept="sort-merge-join", label="Sort-merge join"),
            ]
        )
        operators = ["SortMergeJoin", "Exchange", "Scan"]  # Exchange is the very next node
        annotated = annotate_plan(operators, manifest)
        # Documents the bug: this *should* be "sort-merge-join" (Exchange means a
        # real shuffle happened), but window=0 makes the exclusion a no-op.
        assert annotated[0].concept == "co-partitioned-join"

    def test_exchange_outside_window_does_not_disqualify(self):
        manifest = _manifest(
            [
                PlanNodeRule(
                    match="SortMergeJoin",
                    concept="co-partitioned-join",
                    label="Co-partitioned join (bucketed, no shuffle)",
                    requires_absent_nearby="Exchange",
                    window=2,
                ),
            ]
        )
        operators = ["SortMergeJoin", "Sort", "Scan", "Exchange"]  # Exchange at index 3, outside window=2
        annotated = annotate_plan(operators, manifest)
        assert annotated[0].concept == "co-partitioned-join"


class TestSpotlightStageMetrics:
    def test_extracts_declared_keys_only(self):
        manifest = _manifest(
            [],
            stage_metrics=[
                StageMetricRule(key="shuffleReadBytes", spotlight=True),
                StageMetricRule(key="numTasks", spotlight=False),
            ],
        )
        stage = {"shuffleReadBytes": 1024, "shuffleWriteBytes": 2048, "numTasks": 10}
        spotlighted = spotlight_stage_metrics(stage, manifest)
        assert spotlighted == {
            "shuffleReadBytes": {"value": 1024, "spotlight": True},
            "numTasks": {"value": 10, "spotlight": False},
        }
        assert "shuffleWriteBytes" not in spotlighted

    def test_missing_key_in_stage_data_yields_none_value(self):
        manifest = _manifest([], stage_metrics=[StageMetricRule(key="diskBytesSpilled", spotlight=False)])
        spotlighted = spotlight_stage_metrics({}, manifest)
        assert spotlighted["diskBytesSpilled"]["value"] is None


class TestSpotlightExecutorMetrics:
    """US-C10/US-C3 (Decision A): structurally identical to
    spotlight_stage_metrics, sourced from an executor REST entry instead of a
    stage one."""

    def test_extracts_declared_keys_only(self):
        manifest = _manifest(
            [],
            executor_metrics=[
                StageMetricRule(key="memoryUsed", spotlight=True),
                StageMetricRule(key="maxMemory", spotlight=False),
            ],
        )
        executor = {"memoryUsed": 512_000, "maxMemory": 2_048_000, "totalGCTime": 40}
        spotlighted = spotlight_executor_metrics(executor, manifest)
        assert spotlighted == {
            "memoryUsed": {"value": 512_000, "spotlight": True},
            "maxMemory": {"value": 2_048_000, "spotlight": False},
        }
        assert "totalGCTime" not in spotlighted

    def test_missing_key_in_executor_data_yields_none_value(self):
        manifest = _manifest([], executor_metrics=[StageMetricRule(key="memoryUsed", spotlight=False)])
        spotlighted = spotlight_executor_metrics({}, manifest)
        assert spotlighted["memoryUsed"]["value"] is None

    def test_no_declared_executor_metrics_yields_empty_dict(self):
        manifest = _manifest([])
        assert spotlight_executor_metrics({"memoryUsed": 1}, manifest) == {}


class TestSpotlightTaskDurationQuantiles:
    """Issue #8: true per-task duration quantiles from a
    ?withSummaries=true stage detail, distinct from spotlight_stage_metrics'
    stage-wide aggregates."""

    def test_extracts_min_p25_median_p75_max_from_duration_distribution(self):
        manifest = _manifest([], task_duration_quantiles=True)
        stage_detail = {
            "taskMetricsDistributions": {
                "quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                "duration": [100.0, 200.0, 250.0, 300.0, 900.0],
            }
        }
        result = spotlight_task_duration_quantiles(stage_detail, manifest)
        assert result == {"min": 100.0, "p25": 200.0, "median": 250.0, "p75": 300.0, "max": 900.0}

    def test_manifest_not_opted_in_returns_none(self):
        manifest = _manifest([], task_duration_quantiles=False)
        stage_detail = {"taskMetricsDistributions": {"duration": [1, 2, 3, 4, 5]}}
        assert spotlight_task_duration_quantiles(stage_detail, manifest) is None

    def test_missing_task_metrics_distributions_returns_none(self):
        manifest = _manifest([], task_duration_quantiles=True)
        assert spotlight_task_duration_quantiles({}, manifest) is None

    def test_none_stage_detail_returns_none_not_raises(self):
        manifest = _manifest([], task_duration_quantiles=True)
        assert spotlight_task_duration_quantiles(None, manifest) is None

    def test_malformed_duration_shape_returns_none(self):
        """Wrong-length list (not the expected 5 quantile points) is treated
        as an unexpected shape, same degrade-gracefully contract as the rest
        of the annotation engine -- never partially filled."""
        manifest = _manifest([], task_duration_quantiles=True)
        stage_detail = {"taskMetricsDistributions": {"duration": [1, 2, 3]}}
        assert spotlight_task_duration_quantiles(stage_detail, manifest) is None
