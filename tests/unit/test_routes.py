"""Route-level tests (US-1.1, US-1.2, US-1.3) via FastAPI's TestClient.

The lifecycle manager singleton is mocked/faked at the method level
(`manager.spawn`/`manager.teardown`/`manager.status`) so these tests never
touch Docker -- they verify routing, status codes, and that the rendered
panel reflects whatever the manager reports.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.lifecycle.manager import ClusterState, ManagerStatus, SpawnOutcome
from app.lifecycle.renderer import ClusterParams
from app.main import app
from app.web.routes import topics as topics_module

client = TestClient(app)


def _status(state: ClusterState, **overrides) -> ManagerStatus:
    base = dict(
        state=state,
        message="test message",
        params=ClusterParams(worker_count=3, worker_cores=2, worker_memory_gb=4),
        spawn_id=1,
        alive_workers=None,
        error=None,
    )
    base.update(overrides)
    return ManagerStatus(**base)


class TestTopicPage:
    def test_get_topic_page_returns_200_with_concept_content(self):
        resp = client.get("/topics/partitioning-shuffle")
        assert resp.status_code == 200
        assert "Partitioning" in resp.text
        assert "shuffle" in resp.text.lower()
        assert "Exchange" in resp.text  # concept.md content actually rendered, not a stub

    def test_get_unknown_topic_errors(self):
        # loader.load_topic raises TopicNotFoundError for an unknown id.
        # app/main.py registers an @app.exception_handler(TopicNotFoundError)
        # (issue #4 fix) that converts it into a clean 404 instead of letting
        # it propagate as an unhandled exception (which a real ASGI server
        # would otherwise turn into a raw 500).
        resp = client.get("/topics/does-not-exist")
        assert resp.status_code == 404
        assert "does-not-exist" in resp.text

    def test_index_redirects_to_a_topic(self):
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert "/topics/" in resp.headers["location"]


class TestSpawnValid:
    def test_valid_spawn_returns_200_and_shows_ready(self, monkeypatch):
        ready_status = _status(ClusterState.READY, message="READY: 3/3 workers alive.", alive_workers=3)
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=True, status=ready_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={
                "worker_count": 3,
                "worker_cores": 2,
                "worker_memory_gb": 4,
                "shuffle_partitions": 200,
            },
        )

        assert resp.status_code == 200
        assert "ready" in resp.text.lower()
        assert "READY: 3/3 workers alive." in resp.text
        mock_spawn.assert_awaited_once()


class TestSpawnInvalidParams:
    def test_out_of_range_worker_count_reaches_manager_and_is_cleanly_rejected(self, monkeypatch):
        """The route does NOT pre-validate -- it always calls manager.spawn(),
        which performs validate() internally and returns a clean rejection
        (per US-1.2, 'rejects the configuration before spawning with a clear
        message, rather than attempting it and failing mid-spawn' -- the
        rejection happens inside spawn() before any container action, not in
        the route). This test verifies that real behavior."""
        rejected_status = _status(
            ClusterState.FAILED,
            message="Rejected: worker_count must be 1-5",
            error="worker_count must be 1-5",
            params=None,
        )
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=False, status=rejected_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: rejected_status)

        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={
                "worker_count": 99,  # out of the 1-5 range
                "worker_cores": 2,
                "worker_memory_gb": 4,
                "shuffle_partitions": 200,
            },
        )

        assert resp.status_code == 200  # route always 200s; failure is shown in the panel
        assert "worker_count must be 1-5" in resp.text
        mock_spawn.assert_awaited_once()  # reaches the manager; manager does the real rejection

    def test_non_integer_worker_count_is_a_422_form_validation_error(self):
        """Form(...) declares worker_count: int -- FastAPI/Pydantic itself
        rejects a non-coercible value before the route body even runs."""
        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={
                "worker_count": "not-a-number",
                "worker_cores": 2,
                "worker_memory_gb": 4,
                "shuffle_partitions": 200,
            },
        )
        assert resp.status_code == 422


class TestSpawnSuperseded:
    def test_superseded_spawn_shows_clean_message_not_a_500(self, monkeypatch):
        superseded_status = ManagerStatus(
            state=ClusterState.FAILED,
            message="Spawn superseded by a newer request before it could complete.",
            params=ClusterParams(worker_count=2, worker_cores=2, worker_memory_gb=4),
            spawn_id=2,
            alive_workers=None,
            error="superseded",
        )
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=False, status=superseded_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: superseded_status)

        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={
                "worker_count": 2,
                "worker_cores": 2,
                "worker_memory_gb": 4,
                "shuffle_partitions": 200,
            },
        )

        assert resp.status_code == 200  # not a 500 -- this is exactly the regression this app must avoid
        assert "superseded" in resp.text.lower()


class TestTeardown:
    def test_teardown_returns_200_and_idle_panel(self, monkeypatch):
        idle_status = _status(ClusterState.IDLE, message="Cluster torn down.", params=None)
        mock_teardown = AsyncMock(return_value=idle_status)
        monkeypatch.setattr(topics_module.manager, "teardown", mock_teardown)
        monkeypatch.setattr(topics_module.manager, "status", lambda: idle_status)

        resp = client.post("/topics/partitioning-shuffle/teardown")

        assert resp.status_code == 200
        assert "Cluster torn down." in resp.text
        mock_teardown.assert_awaited_once()


class TestJupyterIframeReflectsCurrentSpawn:
    """US-1.3: the embedded iframe must point at the *current* stack, not a
    stale reference after teardown+respawn -- the panel template keys the
    iframe src off `status.spawn_id`, so a new spawn_id must appear."""

    def test_iframe_present_when_ready_with_current_spawn_id(self, monkeypatch):
        ready_status = _status(ClusterState.READY, message="READY", alive_workers=3, spawn_id=7)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        resp = client.get("/topics/partitioning-shuffle/panel")

        assert resp.status_code == 200
        assert "<iframe" in resp.text
        assert "spawn=7" in resp.text

    def test_no_iframe_when_not_ready(self, monkeypatch):
        idle_status = _status(ClusterState.IDLE, message="No cluster running.", params=None)
        monkeypatch.setattr(topics_module.manager, "status", lambda: idle_status)

        resp = client.get("/topics/partitioning-shuffle/panel")

        assert resp.status_code == 200
        assert "<iframe" not in resp.text
