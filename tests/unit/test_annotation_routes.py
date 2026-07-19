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
from app.spark_api.app_client import AppRef
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

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        # Fresh checkpoint (issue #16): app_id is known to the "current" driver,
        # so no stale-checkpoint warning -- keeps this test focused on plan
        # annotation and avoids a real network call. Issue #24: resolve_app()
        # is what _stages_context() now calls for a checkpoint-driven lookup.
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-0001"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        # BroadcastHashJoin/BroadcastExchange from content/join-strategies/manifest.yaml's
        # most-specific-first rules, not swallowed by the generic Exchange rule.
        assert "Broadcast hash join" in resp.text
        assert "Broadcast of the small side" in resp.text
        assert "app-test-0001" in resp.text
        assert "Stale checkpoint" not in resp.text

    def test_reveal_shows_unknown_for_unmapped_operator(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        plan_with_unknown_node = """== Physical Plan ==
* SomeBrandNewOperatorNobodyMapped (1)
+- Scan parquet default.x (0)

(0) Scan parquet default.x
"""
        _write_checkpoint(annotations_dir, "join-strategies", explain_text=plan_with_unknown_node)
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-0001"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert "unknown / unannotated" in resp.text  # US-2.1 c3 -- never guessed


class TestRevealStaleCheckpointWarning:
    """Issue #16: Reveal must not silently render a confident, fully-labeled
    plan from a checkpoint that doesn't belong to the currently-live driver
    session at :4040."""

    def test_zero_live_applications_shows_stale_warning(self, tmp_path, monkeypatch):
        """Starkest repro from acceptance validation: a freshly-spawned
        cluster with zero live applications must not silently show a
        confident, fully-labeled plan from an old checkpoint."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-two-hours-old")
        monkeypatch.setattr(annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: None)
        monkeypatch.setattr(annotation_module.app_client, "resolve_current_app", lambda timeout_s=3.0: None)
        monkeypatch.setattr(annotation_module.app_client, "resolve_app", lambda app_id, timeout_s=3.0: None)
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: None)

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert "Stale checkpoint" in resp.text
        assert "app-two-hours-old" in resp.text
        # The plan is still shown (not hidden), just not silently/confidently:
        assert "Broadcast hash join" in resp.text

    def test_checkpoint_from_different_session_shows_stale_warning(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-old-session")
        # A different application is live right now -- the checkpoint's
        # app_id belongs to neither a running nor a completed app in the
        # current driver process.
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-new-session"]
        )
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
        # The checkpoint's own id ("app-old-session") isn't known to any
        # probed port -- resolve_app() must reflect that.
        monkeypatch.setattr(annotation_module.app_client, "resolve_app", lambda app_id, timeout_s=3.0: None)

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert "Stale checkpoint" in resp.text
        assert "app-old-session" in resp.text

    def test_just_completed_application_does_not_show_stale_warning(self, tmp_path, monkeypatch):
        """Legitimate case (PLAN.md §3 design): a job just finished but the
        driver isn't torn down yet -- fetch_all_app_ids() includes completed
        (not just running) attempts, so this must NOT be flagged stale."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-just-finished")
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-just-finished"]
        )
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert "Stale checkpoint" not in resp.text


class TestRevealExecutorMetrics:
    """US-C10/US-C3 (Decision A): executor_metrics is a reveal-time-only
    pull, gated on the topic's manifest declaring the section -- distinct
    from stage_metrics, which is also fed to the ~6s poll route."""

    def test_topic_without_executor_metrics_section_skips_the_pull(self, tmp_path, monkeypatch):
        """join-strategies' manifest has no executor_metrics section."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-exec-0001")
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-exec-0001"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )
        calls = []
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_executors", lambda app, timeout_s=3.0: calls.append(app) or []
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/join-strategies/annotation/reveal")

        assert resp.status_code == 200
        assert calls == []  # no executors REST call made
        assert "Per-executor memory metrics" not in resp.text

    def test_topic_with_executor_metrics_section_renders_spotlighted_values(self, tmp_path, monkeypatch):
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "exec-metrics-topic"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "exec-metrics-topic",
            "title": "Exec Metrics",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"executor_metrics": [{"key": "memoryUsed", "spotlight": True}, {"key": "maxMemory"}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "exec-metrics-topic", app_id="app-test-exec-0002")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-exec-0002"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )
        executors = [
            {"id": "0", "memoryUsed": 123456, "maxMemory": 999999},
            {"id": "1", "memoryUsed": 654321, "maxMemory": 999999},
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_executors", lambda app, timeout_s=3.0: executors)

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/exec-metrics-topic/annotation/reveal")

        assert resp.status_code == 200
        assert "Per-executor memory metrics" in resp.text
        assert "123456" in resp.text
        assert "654321" in resp.text

    def test_unreachable_executors_rest_shows_clear_message(self, tmp_path, monkeypatch):
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "exec-metrics-unreachable"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "exec-metrics-unreachable",
            "title": "Exec Metrics Unreachable",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"executor_metrics": [{"key": "memoryUsed", "spotlight": True}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "exec-metrics-unreachable", app_id="app-test-exec-0003")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-exec-0003"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )
        monkeypatch.setattr(annotation_module.app_client, "fetch_executors", lambda app, timeout_s=3.0: None)

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/exec-metrics-unreachable/annotation/reveal")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text

    def test_empty_executor_list_shows_no_executors_message(self, tmp_path, monkeypatch):
        """`fetch_executors()` reachable but returning `[]` (e.g. app just
        started, no executors registered yet) is distinct from "unreachable"
        (None) -- must not be conflated with the error message above."""
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "exec-metrics-empty"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "exec-metrics-empty",
            "title": "Exec Metrics Empty",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"executor_metrics": [{"key": "memoryUsed", "spotlight": True}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "exec-metrics-empty", app_id="app-test-exec-0004")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-exec-0004"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )
        monkeypatch.setattr(annotation_module.app_client, "fetch_executors", lambda app, timeout_s=3.0: [])

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/exec-metrics-empty/annotation/reveal")

        assert resp.status_code == 200
        assert "No executors reported yet" in resp.text
        assert "Could not reach" not in resp.text

    def test_malformed_executors_shape_shows_clear_message_not_500(self, tmp_path, monkeypatch):
        """Same guard as stage_metrics' issue #13 fix (_stage_rows): an
        unexpected shape (dict instead of list) from fetch_executors() must
        degrade the same as 'unreachable', not raise while iterating it."""
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "exec-metrics-malformed"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "exec-metrics-malformed",
            "title": "Exec Metrics Malformed",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"executor_metrics": [{"key": "memoryUsed", "spotlight": True}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "exec-metrics-malformed", app_id="app-test-exec-0005")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: [])
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: ["app-test-exec-0005"]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_executors",
            lambda app, timeout_s=3.0: {"error": "unexpected shape"},
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/exec-metrics-malformed/annotation/reveal")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text

    def test_no_live_application_shows_could_not_reach(self, tmp_path, monkeypatch):
        """Distinct code path from test_unreachable_executors_rest_shows_clear_message:
        there, `resolve_app()` succeeds but `fetch_executors()` fails. Here, no
        live application resolves at all (per the architecture doc's stated
        trade-off that this evidence source depends on a live :4040) --
        `_stages_context()` resolves `app_ref` as None and `_executor_rows()`
        must short circuit without ever calling fetch_executors()."""
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "exec-metrics-no-app"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "exec-metrics-no-app",
            "title": "Exec Metrics No App",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"executor_metrics": [{"key": "memoryUsed", "spotlight": True}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "exec-metrics-no-app", app_id="app-torn-down")

        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: None)
        monkeypatch.setattr(annotation_module.app_client, "resolve_app", lambda app_id, timeout_s=3.0: None)
        calls = []
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_executors", lambda app, timeout_s=3.0: calls.append(app) or []
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.post("/topics/exec-metrics-no-app/annotation/reveal")

        assert resp.status_code == 200
        assert calls == []  # fetch_executors() never called when no app resolves
        assert "Could not reach" in resp.text
        assert "Stale checkpoint" in resp.text  # also flagged by the existing staleness check


class TestTaskRetryEvidence:
    """US-C9 (issue #49, Decision A): task_retry_evidence is a reveal-time,
    every-stage `fetch_task_list()` pull gated by the topic manifest,
    covering two distinct REST-observable retry shapes -- see
    `_task_retry_evidence()`'s own docstring in
    app/web/routes/annotation.py. Mirrors TestRevealExecutorMetrics'
    structure above (own manifest fixtures, mocked app_client)."""

    def _write_retry_topic(self, content_dir, topic_id="retry-topic"):
        topic_dir = content_dir / topic_id
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": topic_id,
            "title": "Retry Topic",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"task_retry_evidence": True},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")
        return topic_dir

    def _resolve(self, monkeypatch, app_id):
        monkeypatch.setattr(
            annotation_module.app_client, "fetch_all_app_ids", lambda timeout_s=3.0: [app_id]
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda aid, timeout_s=3.0: AppRef(app_id=aid, base_url="http://localhost:4040"),
        )

    def test_topic_without_opt_in_skips_task_list_call(self, tmp_path, monkeypatch):
        """join-strategies' manifest has no task_retry_evidence section."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-retry-0000")
        self._resolve(monkeypatch, "app-retry-0000")
        stages = [{"stageId": 1, "attemptId": 0, "status": "COMPLETE", "numTasks": 4}]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)
        calls = []
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_task_list",
            lambda app, stage_id, attempt_id=0, length=1000, timeout_s=3.0: calls.append(stage_id) or [],
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert calls == []
        assert "tasks retried" not in resp.text

    def test_same_attempt_retry_shape(self, tmp_path, monkeypatch):
        """Shape 1: a task rescheduled within the same stage attempt shows a
        second task-list record for the same `index` with `attempt >= 1`."""
        content_dir = tmp_path / "content"
        self._write_retry_topic(content_dir)
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "retry-topic", app_id="app-retry-0001")
        self._resolve(monkeypatch, "app-retry-0001")

        stages = [{"stageId": 5, "attemptId": 0, "status": "COMPLETE", "numTasks": 4}]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)

        tasks = [
            {"index": 0, "attempt": 0, "status": "FAILED"},
            {"index": 0, "attempt": 1, "status": "SUCCESS"},  # retried
            {"index": 1, "attempt": 0, "status": "SUCCESS"},
            {"index": 2, "attempt": 0, "status": "SUCCESS"},
            {"index": 3, "attempt": 0, "status": "SUCCESS"},
        ]
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_task_list",
            lambda app, stage_id, attempt_id=0, length=1000, timeout_s=3.0: tasks,
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/retry-topic/annotation/stages")

        assert resp.status_code == 200
        assert "1 of 4 retried" in resp.text
        assert "3 kept results" in resp.text

    def test_stage_resubmission_shape(self, tmp_path, monkeypatch):
        """Shape 2: the same stageId appears twice in /stages -- attempt 0
        (original, FAILED after a FetchFailedException) and attempt 1 (the
        resubmission, whose numTasks is only the recomputed partitions).
        Per the docstring, this shape is read directly off the stage list's
        numTasks -- no fetch_task_list() call needed for the resubmitted row.
        Attempt 0's own row is superseded (it triggered the resubmission), so
        it reports no evidence at all and never calls fetch_task_list()
        either -- fixed after code review flagged the original fallback
        behavior as backwards (reporting "0 retried" on the exact row that
        caused the retries)."""
        content_dir = tmp_path / "content"
        self._write_retry_topic(content_dir)
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "retry-topic", app_id="app-retry-0002")
        self._resolve(monkeypatch, "app-retry-0002")

        stages = [
            {"stageId": 7, "attemptId": 0, "status": "FAILED", "numTasks": 50},
            {"stageId": 7, "attemptId": 1, "status": "COMPLETE", "numTasks": 2},
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)

        attempt0_tasks = [{"index": i, "attempt": 0, "status": "FAILED"} for i in range(50)]
        calls = []
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_task_list",
            lambda app, stage_id, attempt_id=0, length=1000, timeout_s=3.0: calls.append(attempt_id) or attempt0_tasks,
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/retry-topic/annotation/stages")

        assert resp.status_code == 200
        # Resubmitted attempt's row: 2 of the original 50 retried, 48 kept --
        # computed straight from numTasks, no task-list fetch for attempt 1.
        assert "2 of 50 retried" in resp.text
        assert "48 kept results" in resp.text
        # Attempt 0 is superseded + FAILED -- suppressed entirely, so
        # fetch_task_list() is never called for either row.
        assert calls == []

    def test_repeated_stage_resubmission_each_row_independent_against_original_total(self, tmp_path, monkeypatch):
        """Known-risk edge case flagged for review: Spark can resubmit a
        stage more than once. The current implementation computes each
        resubmitted attempt's row independently against the original
        attempt-0 total (max(original_num_tasks, this attempt's numTasks)),
        not cumulatively across resubmissions -- pinning that as the actual,
        intentional-if-imperfect behavior rather than leaving it unverified."""
        content_dir = tmp_path / "content"
        self._write_retry_topic(content_dir)
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "retry-topic", app_id="app-retry-0003")
        self._resolve(monkeypatch, "app-retry-0003")

        stages = [
            {"stageId": 9, "attemptId": 0, "status": "FAILED", "numTasks": 50},
            {"stageId": 9, "attemptId": 1, "status": "FAILED", "numTasks": 2},
            {"stageId": 9, "attemptId": 2, "status": "COMPLETE", "numTasks": 1},
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)
        attempt0_tasks = [{"index": i, "attempt": 0, "status": "FAILED"} for i in range(50)]
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_task_list",
            lambda app, stage_id, attempt_id=0, length=1000, timeout_s=3.0: attempt0_tasks,
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/retry-topic/annotation/stages")

        assert resp.status_code == 200
        # Each resubmitted row is independently "N of the *original* 50",
        # not cumulative across the two resubmissions.
        assert "2 of 50 retried" in resp.text
        assert "48 kept results" in resp.text
        assert "1 of 50 retried" in resp.text
        assert "49 kept results" in resp.text

    def test_superseded_failed_attempt_suppresses_misleading_zero_retry(self, tmp_path, monkeypatch):
        """Fix for the bug both reviewers flagged: the original (attempt 0)
        row of a stage that went on to be resubmitted used to fall through to
        the same-attempt fallback and report "0 of N retried" -- backwards,
        since that attempt is exactly what triggered the resubmission. It
        must now report no evidence at all (renders as "-"), while the
        resubmitted attempt's own row still correctly reports the real
        count."""
        content_dir = tmp_path / "content"
        self._write_retry_topic(content_dir)
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "retry-topic", app_id="app-retry-0004")
        self._resolve(monkeypatch, "app-retry-0004")

        stages = [
            {"stageId": 11, "attemptId": 0, "status": "FAILED", "numTasks": 10},
            {"stageId": 11, "attemptId": 1, "status": "COMPLETE", "numTasks": 3},
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)
        # None of attempt 0's own tasks show a same-attempt retry bump -- the
        # stage attempt was simply abandoned mid-flight when the worker
        # holding shuffle data died, not individually retried task-by-task.
        # fetch_task_list must not even be consulted for the superseded
        # attempt 0 row now that it's suppressed before any REST call.
        attempt0_tasks = [{"index": i, "attempt": 0, "status": "FAILED"} for i in range(10)]
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_task_list",
            lambda app, stage_id, attempt_id=0, length=1000, timeout_s=3.0: attempt0_tasks,
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/retry-topic/annotation/stages")

        assert resp.status_code == 200
        # Attempt 0's own row no longer claims "0 of 10 retried" -- it has no
        # task_retry evidence at all now that it's known to be superseded.
        assert "0 of 10 retried" not in resp.text
        # Attempt 1's row still correctly reports the resubmission shape.
        assert "3 of 10 retried" in resp.text


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
            monkeypatch.setattr(annotation_module.app_client, "resolve_current_app", lambda timeout_s=3.0: None)
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
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)
        # join-strategies opts into task_duration_quantiles (issue #8), which
        # triggers a second per-stage REST call -- mocked here so this test
        # doesn't attempt a real network call to :4040.
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_stage_task_summary",
            lambda app, stage_id, attempt_id=0, timeout_s=3.0: {
                "taskMetricsDistributions": {
                    "quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                    "duration": [50.0, 60.0, 75.0, 90.0, 150.0],
                }
            },
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "12345" in resp.text
        assert "/stages/stage/?id=3&amp;attempt=0" in resp.text or "/stages/stage/?id=3&attempt=0" in resp.text
        assert f"every {config.STAGE_POLL_INTERVAL_S}s" in resp.text  # HTMX polling interval (US-2.2)
        # True per-task duration quantiles rendered alongside the aggregate
        # executorRunTime metric above (issue #8), not replacing it.
        assert "75.0" in resp.text  # median

    def test_quantile_columns_render_when_first_stage_has_none_but_later_stage_does(self, tmp_path, monkeypatch):
        """Regression for a code-review finding: the header/placeholder used
        to gate on `stages[0].duration_quantiles` instead of the manifest's
        opt-in, so a first stage with no quantiles yet (e.g. RUNNING, no
        `taskMetricsDistributions` populated) hid the header even though a
        later stage in the same table had real quantile data -- broken
        column count/alignment. Must key off the manifest flag instead."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0006")

        stages = [
            {"stageId": 1, "attemptId": 0, "status": "RUNNING", "shuffleReadBytes": 111},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "shuffleReadBytes": 222},
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)

        def _fetch_stage_task_summary(app, stage_id, attempt_id=0, timeout_s=3.0):
            if stage_id == 1:
                return None  # not yet populated for the running stage
            return {
                "taskMetricsDistributions": {
                    "quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                    "duration": [50.0, 60.0, 75.0, 90.0, 150.0],
                }
            }

        monkeypatch.setattr(
            annotation_module.app_client, "fetch_stage_task_summary", _fetch_stage_task_summary
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "task duration min" in resp.text  # header must still render
        assert "75.0" in resp.text  # stage 2's real median
        assert "colspan=\"5\"" in resp.text  # stage 1's placeholder for the missing quantiles

    def test_unreachable_rest_api_shows_clear_message(self, tmp_path, monkeypatch):
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0003")
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: None)
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text

    def test_topic_without_opt_in_skips_extra_rest_call_and_renders_no_quantiles(self, tmp_path, monkeypatch):
        """Issue #8: `task_duration_quantiles` is opt-in per topic manifest --
        a topic that doesn't declare it must not trigger the second
        `?withSummaries=true` REST call at all, and the stage table must not
        render the quantile columns."""
        content_dir = tmp_path / "content"
        topic_dir = content_dir / "no-quantiles-topic"
        topic_dir.mkdir(parents=True)
        manifest = {
            "id": "no-quantiles-topic",
            "title": "No Quantiles",
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "annotation": {"stage_metrics": [{"key": "shuffleReadBytes", "spotlight": True}]},
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "no-quantiles-topic", app_id="app-test-0005")

        stages = [
            {
                "stageId": 3,
                "attemptId": 0,
                "status": "COMPLETE",
                "shuffleReadBytes": 12345,
            }
        ]
        monkeypatch.setattr(annotation_module.app_client, "fetch_stages", lambda app, timeout_s=3.0: stages)

        calls = []
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_stage_task_summary",
            lambda app, stage_id, attempt_id=0, timeout_s=3.0: calls.append(stage_id),
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "CONTENT_DIR", content_dir), patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/no-quantiles-topic/annotation/stages")

        assert resp.status_code == 200
        assert calls == []  # no extra REST call made
        assert "task duration min" not in resp.text
        assert "12345" in resp.text  # the opted-in stage_metrics still render

    def test_malformed_stages_shape_shows_clear_message_not_500(self, tmp_path, monkeypatch):
        """Issue #13: fetch_stages() returning an unexpected shape (e.g. a
        dict instead of a list) must degrade the same as 'unreachable', not
        raise while iterating it as a stage list."""
        annotations_dir = tmp_path / "annotations"
        _write_checkpoint(annotations_dir, "join-strategies", app_id="app-test-0004")
        monkeypatch.setattr(
            annotation_module.app_client,
            "fetch_stages",
            lambda app, timeout_s=3.0: {"error": "unexpected shape"},
        )
        monkeypatch.setattr(
            annotation_module.app_client,
            "resolve_app",
            lambda app_id, timeout_s=3.0: AppRef(app_id=app_id, base_url="http://localhost:4040"),
        )

        with patch.object(config, "ANNOTATIONS_DIR", annotations_dir):
            resp = client.get("/topics/join-strategies/annotation/stages")

        assert resp.status_code == 200
        assert "Could not reach" in resp.text
