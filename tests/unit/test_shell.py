"""Tests for the topic-page shell redesign (US-SH1/US-SH2/US-SH3/US-SH7):
tab structure, breadcrumb topic switcher, cluster-config drawer relocation,
and the combined spawn/teardown out-of-band response.

`topic.html` was deleted and replaced by `shell.html` -- these tests target
the new template/route wiring introduced in that diff, distinct from the
pre-existing US-1.1/US-1.2/US-1.3 coverage in test_routes.py (which still
passes unchanged against the new shell, since it only asserts on text
fragments common to both templates).
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


BUILT_TOPICS = ["partitioning-shuffle", "join-strategies", "bucketing", "aqe"]


class TestSharedShellAcrossAllTopics:
    """US-SH1: every topic renders through the one shell component -- no
    bespoke per-topic markup -- with the Concept/Notebook/Self-check tab
    structure (G-SH2)."""

    def test_all_four_built_topics_render_the_shared_shell(self):
        for topic_id in BUILT_TOPICS:
            resp = client.get(f"/topics/{topic_id}")
            assert resp.status_code == 200, topic_id
            assert 'class="tab-btn active" data-tab="concept"' in resp.text, topic_id
            assert 'data-tab="notebook"' in resp.text, topic_id
            assert 'data-tab="selfcheck"' in resp.text, topic_id
            assert 'id="cluster-drawer"' in resp.text, topic_id
            assert 'id="breadcrumb-menu"' in resp.text, topic_id

    def test_notebook_tab_shows_walkthrough_steps_from_real_notebook_content(self):
        resp = client.get("/topics/partitioning-shuffle")
        assert resp.status_code == 200
        assert "walkthrough-steps" in resp.text
        assert "Build a keyed dataset spread across input partitions" in resp.text

    def test_shell_js_and_style_are_wired_in(self):
        resp = client.get("/topics/partitioning-shuffle")
        assert '/static/shell.js' in resp.text
        assert '/static/style.css' in resp.text


class TestBreadcrumbTopicSwitcher:
    """US-SH3: breadcrumb dropdown lists all topics from
    content/*/manifest.yaml, current topic visually distinguished, and
    switching topics is a plain full-page navigation (no cluster
    side-effect)."""

    def test_dropdown_lists_every_built_topic(self):
        resp = client.get("/topics/partitioning-shuffle")
        for topic_id in BUILT_TOPICS:
            assert f'href="/topics/{topic_id}"' in resp.text

    def test_current_topic_is_marked_current_and_others_are_not(self):
        resp = client.get("/topics/aqe")
        # The current topic's breadcrumb-item link carries the "current" class.
        assert 'href="/topics/aqe" class="breadcrumb-item current"' in resp.text
        assert 'href="/topics/bucketing" class="breadcrumb-item "' in resp.text

    def test_switching_topics_is_a_plain_link_not_a_form_post(self):
        """Navigation must not implicitly tear down/respawn a cluster as a
        side effect -- verified structurally: breadcrumb entries are plain
        <a href> full-page links, never a <form>/hx-post."""
        resp = client.get("/topics/partitioning-shuffle")
        import re

        menu = re.search(r'id="breadcrumb-menu".*?</div>\s*</div>', resp.text, re.DOTALL)
        assert menu is not None
        assert "hx-post" not in menu.group(0)
        assert "<form" not in menu.group(0)


class TestClusterDrawerRelocatedRanges:
    """US-SH2: the drawer exposes the locked parameter ranges -- worker
    count 1-5, cores 1-4, memory 1-8GB, shuffle partitions 1-300 -- and the
    spawn/teardown forms are functionally equivalent to the old panel, just
    relocated into the drawer markup."""

    def test_drawer_present_with_locked_ranges(self):
        resp = client.get("/topics/partitioning-shuffle")
        assert 'id="cluster-drawer"' in resp.text
        # Shuffle partitions: 1-300 (US-SH2, not the old unbounded min-only input).
        assert 'name="shuffle_partitions" min="1"' in resp.text
        assert 'max="300"' in resp.text
        assert 'max="8"' in resp.text  # memory GB upper bound (locked at 1-8, not the mockup's 1-16)
        assert 'max="16"' not in resp.text  # the mockup's wider memory range must not ship

    def test_drawer_spawn_and_teardown_forms_present(self):
        resp = client.get("/topics/partitioning-shuffle")
        assert f'hx-post="/topics/partitioning-shuffle/spawn"' in resp.text
        assert f'hx-post="/topics/partitioning-shuffle/teardown"' in resp.text


class TestCombinedSpawnTeardownOobResponse:
    """Decision C / friction note 3: the spawn/teardown POST response updates
    the drawer's #cluster-panel (primary swap target) *and* OOB-swaps the
    right pane + top-bar state pill in the same response, so all three stay
    in sync without an extra poller."""

    def test_spawn_response_includes_primary_target_and_two_oob_swaps(self, monkeypatch):
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
        # Primary target: the drawer body, no hx-swap-oob on it.
        assert 'id="cluster-panel" class="drawer-body"' in resp.text
        # OOB swaps: right pane + top-bar pill, each carrying hx-swap-oob="true".
        assert 'id="cluster-right-pane"' in resp.text
        assert 'id="top-bar-pill"' in resp.text
        assert resp.text.count('hx-swap-oob="true"') == 2

    def test_teardown_response_also_oob_swaps_right_pane_and_pill(self, monkeypatch):
        idle_status = _status(ClusterState.IDLE, message="Cluster torn down.", params=None)
        mock_teardown = AsyncMock(return_value=idle_status)
        monkeypatch.setattr(topics_module.manager, "teardown", mock_teardown)
        monkeypatch.setattr(topics_module.manager, "status", lambda: idle_status)

        resp = client.post("/topics/partitioning-shuffle/teardown")

        assert resp.status_code == 200
        assert resp.text.count('hx-swap-oob="true"') == 2
        assert 'state-pill-idle' in resp.text
        assert 'right-pane-idle' in resp.text

    def test_pill_reflects_busy_state_on_oob_swap(self, monkeypatch):
        busy_status = _status(ClusterState.WAITING_READY, message="Waiting for workers...")
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=True, status=busy_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: busy_status)

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
        assert "state-pill-waiting_ready" in resp.text
        assert "right-pane-busy" in resp.text
