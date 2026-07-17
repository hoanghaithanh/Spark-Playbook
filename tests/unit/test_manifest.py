"""Tests for app/annotation/manifest.py (manifest loading/validation)."""
from __future__ import annotations

from unittest.mock import patch

import pytest
import yaml

from app import config
from app.annotation.manifest import ManifestError, load_annotation_manifest
from app.topics.loader import TopicNotFoundError


def _write_topic(tmp_path, topic_id: str, annotation: dict):
    topic_dir = tmp_path / topic_id
    topic_dir.mkdir()
    manifest = {
        "id": topic_id,
        "title": topic_id,
        "content": "concept.md",
        "notebook": "notebook.ipynb",
        "annotation": annotation,
    }
    (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
    (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
    (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")
    return topic_dir


class TestLoadRealJoinStrategiesManifest:
    def test_loads_precedence_ordered_rules(self):
        manifest = load_annotation_manifest("join-strategies")
        matches = [r.match for r in manifest.plan_nodes]
        # BroadcastExchange must precede the generic Exchange rule (most-
        # specific-first precedence, US-2.1 / engine.py's contract).
        assert matches.index("BroadcastExchange") < matches.index("Exchange")
        assert matches.index("BroadcastHashJoin") < matches.index("Exchange")

    def test_stage_metrics_loaded(self):
        manifest = load_annotation_manifest("join-strategies")
        keys = [r.key for r in manifest.stage_metrics]
        assert "shuffleReadBytes" in keys
        assert "shuffleWriteBytes" in keys

    def test_task_duration_quantiles_opted_in(self):
        """Issue #8: join-strategies opts into the true per-task duration
        quantile distribution alongside stage_metrics' executorRunTime."""
        manifest = load_annotation_manifest("join-strategies")
        assert manifest.task_duration_quantiles is True


class TestLoadRealDagLazyEvaluationManifest:
    """US-C1 (issue #27): this topic's annotation section is a deliberately
    minimal placeholder (job-count evidence comes from the notebook's own
    REST /jobs check, not plan-node labeling) -- still exercised for real
    against the shipped manifest.yaml, same as the other real-topic classes
    above."""

    def test_plan_nodes_include_shuffle_boundary(self):
        manifest = load_annotation_manifest("dag-lazy-evaluation")
        matches = [r.match for r in manifest.plan_nodes]
        assert "Exchange" in matches

    def test_stage_metrics_loaded(self):
        manifest = load_annotation_manifest("dag-lazy-evaluation")
        keys = [r.key for r in manifest.stage_metrics]
        assert "shuffleReadBytes" in keys

    def test_no_task_duration_quantiles_opt_in(self):
        manifest = load_annotation_manifest("dag-lazy-evaluation")
        assert manifest.task_duration_quantiles is False


class TestLoadRealCachingPersistenceManifest:
    """US-C5 (issue #28): this topic's fraction-cached/storage-level self-check
    evidence comes from the notebook's own REST /storage/rdd check (no
    storage-shaped rule type exists in this schema), same disposition as
    dag-lazy-evaluation's job-count check above -- the annotation section here
    only labels the plan-shape/shuffle-cost side of the underlying join."""

    def test_plan_nodes_include_cache_hit_scan(self):
        manifest = load_annotation_manifest("caching-persistence")
        matches = [r.match for r in manifest.plan_nodes]
        assert "InMemoryTableScan" in matches

    def test_stage_metrics_loaded(self):
        manifest = load_annotation_manifest("caching-persistence")
        keys = [r.key for r in manifest.stage_metrics]
        assert "shuffleReadBytes" in keys

    def test_no_task_duration_quantiles_opt_in(self):
        manifest = load_annotation_manifest("caching-persistence")
        assert manifest.task_duration_quantiles is False


class TestValidManifest:
    def test_full_rule_set(self, tmp_path):
        annotation = {
            "plan_nodes": [
                {"match": "BroadcastExchange", "concept": "broadcast-exchange", "label": "Broadcast"},
                {"match": "Exchange", "concept": "shuffle-boundary", "label": "Shuffle"},
            ],
            "stage_metrics": [
                {"key": "shuffleReadBytes", "spotlight": True},
                {"key": "numTasks"},
            ],
        }
        _write_topic(tmp_path, "custom-topic", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            manifest = load_annotation_manifest("custom-topic")
        assert len(manifest.plan_nodes) == 2
        assert manifest.plan_nodes[0].match == "BroadcastExchange"
        assert manifest.stage_metrics[0].spotlight is True
        assert manifest.stage_metrics[1].spotlight is False

    def test_requires_absent_nearby_and_window_parsed(self, tmp_path):
        annotation = {
            "plan_nodes": [
                {
                    "match": "SortMergeJoin",
                    "concept": "co-partitioned-join",
                    "label": "Co-partitioned join (no shuffle)",
                    "requires_absent_nearby": "Exchange",
                    "window": 5,
                },
                {"match": "SortMergeJoin", "concept": "sort-merge-join", "label": "Sort-merge join"},
            ],
        }
        _write_topic(tmp_path, "bucketing-like", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            manifest = load_annotation_manifest("bucketing-like")
        assert manifest.plan_nodes[0].requires_absent_nearby == "Exchange"
        assert manifest.plan_nodes[0].window == 5

    def test_empty_annotation_section_yields_empty_manifest(self, tmp_path):
        _write_topic(tmp_path, "no-annotation-topic", {})
        with patch.object(config, "CONTENT_DIR", tmp_path):
            manifest = load_annotation_manifest("no-annotation-topic")
        assert manifest.plan_nodes == []
        assert manifest.stage_metrics == []
        assert manifest.task_duration_quantiles is False

    def test_task_duration_quantiles_parsed_true(self, tmp_path):
        annotation = {"task_duration_quantiles": True}
        _write_topic(tmp_path, "quantile-topic", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            manifest = load_annotation_manifest("quantile-topic")
        assert manifest.task_duration_quantiles is True


class TestInvalidManifest:
    def test_missing_match_raises(self, tmp_path):
        annotation = {"plan_nodes": [{"concept": "x", "label": "X"}]}
        _write_topic(tmp_path, "bad-topic", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="match"):
                load_annotation_manifest("bad-topic")

    def test_missing_label_raises(self, tmp_path):
        annotation = {"plan_nodes": [{"match": "Exchange", "concept": "x"}]}
        _write_topic(tmp_path, "bad-topic-2", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="label"):
                load_annotation_manifest("bad-topic-2")

    def test_missing_concept_raises(self, tmp_path):
        annotation = {"plan_nodes": [{"match": "Exchange", "label": "X"}]}
        _write_topic(tmp_path, "bad-topic-3", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="concept"):
                load_annotation_manifest("bad-topic-3")

    def test_plan_nodes_not_a_list_raises(self, tmp_path):
        annotation = {"plan_nodes": "not-a-list"}
        _write_topic(tmp_path, "bad-topic-4", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="plan_nodes"):
                load_annotation_manifest("bad-topic-4")

    def test_stage_metric_missing_key_raises(self, tmp_path):
        annotation = {"stage_metrics": [{"spotlight": True}]}
        _write_topic(tmp_path, "bad-topic-5", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="key"):
                load_annotation_manifest("bad-topic-5")

    def test_unknown_topic_raises_topic_not_found(self):
        with pytest.raises(TopicNotFoundError):
            load_annotation_manifest("does-not-exist")

    def test_non_positive_window_on_requires_absent_nearby_rule_rejected(self, tmp_path):
        annotation = {
            "plan_nodes": [
                {
                    "match": "SortMergeJoin",
                    "concept": "co-partitioned-join",
                    "label": "Co-partitioned join (no shuffle)",
                    "requires_absent_nearby": "Exchange",
                    "window": 0,
                },
            ],
        }
        _write_topic(tmp_path, "zero-window-topic", annotation)
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(ManifestError, match="window"):
                load_annotation_manifest("zero-window-topic")
