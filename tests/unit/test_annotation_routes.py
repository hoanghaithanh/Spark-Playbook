"""Route-level tests for app/web/routes/annotation.py (US-2.1, US-2.2, G3 pull-not-push).

Mocks `app.spark_api.app_client` (never touches a real :4040) and writes real
checkpoint-shaped JSON fixture files into a temp ANNOTATIONS_DIR (patched via
app.config) so these tests exercise the real reveal/parse/annotate path
end-to-end at the route level, without Docker.
"""
from __future__ import annotations

import json
import time
from unittest.mock import patch

import yaml
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.web.routes import annotation as annotation_module

client = TestClient(app)

BROADCAST_PLAN = """== Physical Plan ==
* BroadcastHashJoin Inner BuildRight (7)
:- * Filter (2)
:  +- Scan parquet default.small (1)
+- BroadcastExchange HashedRelationBroadcastMode (6)
   +- * Filter (5)
      +- Scan parquet default.large (4)

(1) Scan parquet default.small
"""


def _write_checkpoint(tmp_path, topic_id: str, app_id: str = "app-test-0001", explain_text: str = BROADCAST_PLAN):
    topic_dir = tmp_path / topic_id
    topic_dir.mkdir(parents=True, exist_ok=True)
    payload = {"topic": topic_id, "app_id": app_id, "timestamp": time.time(), "explain_formatted": explain_text}
    ts_ms = int(payload["timestamp"] * 1000)
    (topic_dir / f"{ts_ms}.json").write_text(json.dumps(payload), encoding="utf-8")
    return topic_dir


class TestAnnotationPanelBeforeReveal:
    def test_get_panel_shows_reveal_control_not_results(self):
        resp = client.get("/topics/join-strategies/annotation")
        assert resp.status_code == 200
        assert "Reveal self-check" in resp.text
        assert "BroadcastHashJoin" not in resp.text  # nothing pushed before Reveal (G3)


class TestRevealWithNoCheckpoint:
    def test_reveal_with_no_checkpoint_shows_clear_message_not_annotations(self, tmp_path):
        with patch.object(config, "ANNOTATIONS_DIR", tmp_path / "annotations"):
            resp = client.post("/topics/join-strategies/annotation/reveal")
        assert resp.status_code == 200
        assert "No checkpoint found" in resp.text


class TestRevealWithCheckpoint:
    def test_reveal_annotates_real_plan_against_manifest(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app_id, timeout_s=3.0: [])

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        # BroadcastHashJoin/BroadcastExchange from content/join-strategies/manifest.yaml's
        # most-specific-first rules, not swallowed by the generic Exchange rule.
        assert "Broadcast hash join" in resp.text
        assert "Broadcast of the small side" in resp.text
        assert "app-test-0001" in resp.text

    def test_reveal_shows_unknown_for_unmapped_operator(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        plan_with_unknown_node = """== Physical Plan ==
* SomeBrandNewOperatorNobodyMapped (1)
+- Scan parquet default.x (0)

(0) Scan parquet default.x
"""
        _write_checkpoint(annotations_dir, "join-strategies", explain_text=plan_with_unknown_node)
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app_id, timeout_s=3.0: [])

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert "unknown / unannotated" in resp.text  # US-2.1 c3 -- never guessed


class TestStageMetricsFragment:
    def test_invalid_manifest_shows_clear_message_not_500(self, tmp_path, monkeypatch):
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "bad-manifest-topic"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "bad-manifest-topic",
            "title": "Bad",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            # Missing 'concept' -- an invalid annotation.plan_nodes entry,
            # same failure mode Reveal already handles gracefully.
            "annotation": {"plan_nodes": [{"match": "Exchange", "label": "no concept here"}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", content_dir):
            resp = client.get("/topics/bad-manifest-topic/annotation/stages")

        assert resp.status_code == 200
        assert "manifest" in resp.text.lower()

    def test_no_active_application_shows_clear_message(self, tmp_path, monkeypatch):
        with patch.object(config, "ANNOTATIONS_DIR", tmp_path / "annotations"):
            monkeypatch.setattr(annotation_module.app_client, "fetch_current_app_id", lambda timeout_s=3.0: None)
            resp = client.get("/topics/join-strategies/annotation/stages")
        assert resp.status_code == 200
        assert "No active" in resp.text

    def test_stage_rows_include_deep_link_and_spotlighted_metrics(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0002")

        stages = [
            {
                "stageId": 3,
                "attemptId": 0,
                "status": "COMPLETE",
                "shuffleReadBytes": 12345,
                "shuffleWriteBytes": 6789,
                "numTasks": 24,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
            }
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app_id, timeout_s=3.0: stages)

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "12345" in resp.text
        assert "/stages/stage/?id=3&amp;attempt=0" in resp.text or "/stages/stage/?id=3&attempt=0" in resp.text
        assert f"every {config.STAGE_POLL_INTERVAL_S}s" in resp.text  # HTMX polling interval (US-2.2)

    def test_unreachable_rest_api_shows_clear_message(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0003")
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app_id, timeout_s=3.0: None)

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text

    def test_malformed_stages_shape_shows_clear_message_not_500(self, tmp_path, monkeypatch):
        """Issue #13: fetch_stages() returning an unexpected shape (e.g. a
        dict instead of a list) must degrade the same as 'unreachable', not
        raise while iterating it as a stage list."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0004")
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_stages",
            lambda app_id, timeout_s=3.0: {"error": "unexpected shape"},
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text
