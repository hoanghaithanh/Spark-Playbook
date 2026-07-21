"""Route-level tests (US-1.1, US-1.2, US-1.3) via FastAPI's TestClient.

The lifecycle manager singleton is mocked/faked at the method level
(`manager.spawn`/`manager.teardown`/`manager.status`) so these tests never
touch Docker -- they verify routing, status codes, and that the rendered
panel reflects whatever the manager reports.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import yaml
from fastapi.testclient import TestClient

from app import config
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

    def test_index_renders_topics_landing_page(self):
        """Issue #26: GET / renders an actual topics-index landing page
        (one card per content/*/manifest.yaml topic) instead of redirecting
        straight to the first topic."""
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 200
        assert "Topics" in resp.text
        for topic_id in ("partitioning-shuffle", "join-strategies", "bucketing", "aqe", "catalyst-plans"):
            assert f'href="/topics/{topic_id}"' in resp.text


class TestTopicsIndexPage:
    """Deeper checks on GET / (issue #26/US-SH5) beyond "all 5 ids appear
    somewhere on the page": each card must pair the *right* order/title/
    notebook together (not just have all values present anywhere), and an
    empty content directory must render gracefully rather than erroring."""

    def _write_topic(self, content_dir, topic_id, order, title, notebook_name="notebook.ipynb"):
        topic_dir = content_dir / topic_id
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": topic_id, "title": title, "order": order, "notebook": notebook_name}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text(
            f"# {title}\n\n## What it is\n\nBlurb for {title}.\n", encoding="utf-8"
        )
        (topic_dir / notebook_name).write_text("{}", encoding="utf-8")

    def test_each_card_pairs_its_own_order_title_and_notebook(self, tmp_path):
        self._write_topic(tmp_path, "alpha-topic", order=2, title="Alpha Topic", notebook_name="alpha.ipynb")
        self._write_topic(tmp_path, "beta-topic", order=1, title="Beta Topic", notebook_name="beta.ipynb")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            resp = client.get("/")

        assert resp.status_code == 200
        # Cards render sorted by order -- beta (order 1) before alpha (order 2).
        beta_pos = resp.text.index("Beta Topic")
        alpha_pos = resp.text.index("Alpha Topic")
        assert beta_pos < alpha_pos
        assert "TOPIC 01" in resp.text and "TOPIC 02" in resp.text
        # Each card's own notebook name must sit next to its own title, not
        # the other card's -- slice the page around each title to check.
        alpha_card = resp.text[alpha_pos - 200 : alpha_pos + 200]
        beta_card = resp.text[beta_pos - 200 : beta_pos + 200]
        assert "alpha.ipynb" in alpha_card and "beta.ipynb" not in alpha_card
        assert "beta.ipynb" in beta_card and "alpha.ipynb" not in beta_card

    def test_empty_content_dir_renders_page_with_no_cards_not_an_error(self, tmp_path):
        with patch.object(config, "CONTENT_DIR", tmp_path):
            resp = client.get("/")

        assert resp.status_code == 200
        assert "Topics" in resp.text

    def test_spark_and_kafka_track_headings_render_in_order_with_own_cards(self):
        """Kafka-curriculum ADR (docs/architecture/kafka-curriculum.md D-KC1):
        GET / must actually render the two-section grouping (not just have
        `list_topics_by_track()` correct at the loader level, which
        TestTrackGrouping in test_topics_loader.py already covers) -- a
        heading per non-empty track, Spark before Kafka, and the real
        kafka-architecture-kraft card sitting under the Kafka heading rather
        than the Spark one."""
        resp = client.get("/")

        assert resp.status_code == 200
        assert '<h2 class="topics-track-heading">Spark</h2>' in resp.text
        assert '<h2 class="topics-track-heading">Kafka</h2>' in resp.text
        spark_heading_pos = resp.text.index('<h2 class="topics-track-heading">Spark</h2>')
        kafka_heading_pos = resp.text.index('<h2 class="topics-track-heading">Kafka</h2>')
        assert spark_heading_pos < kafka_heading_pos

        kafka_card_pos = resp.text.index('href="/topics/kafka-architecture-kraft"')
        assert kafka_heading_pos < kafka_card_pos
        # And not accidentally also linked under the Spark section.
        assert 'href="/topics/kafka-architecture-kraft"' not in resp.text[:kafka_heading_pos]


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


class TestSpawnIncludeKafkaFromForm:
    """docs/architecture/multi-broker-kafka-cluster.md D-MBK1 (the D1
    reversal): spawn_cluster() reads `include_kafka`/`kafka_broker_count`
    from the submitted FORM, not from `topic.requires_kafka` directly --
    the manifest only pre-checks the drawer's default checked-state
    (see TestPanelContextKafkaDefaults below), it never overrides what was
    actually submitted. Verified in both directions per US-MBK1's
    acceptance criteria, via a synthetic requires_kafka: true topic and the
    existing requires_kafka: false partitioning-shuffle topic."""

    def _write_streaming_topic(self, tmp_path):
        topic_dir = tmp_path / "streaming-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({
                "id": "streaming-topic", "title": "Streaming Topic", "content": "concept.md",
                "notebook": "notebook.ipynb", "requires_kafka": True,
            }),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok\n\n## What it is\n\nblurb.\n", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

    def test_non_streaming_topic_no_kafka_field_submitted_stays_false(self, monkeypatch):
        ready_status = _status(ClusterState.READY, message="READY", alive_workers=3)
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=True, status=ready_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={"worker_count": 3, "worker_cores": 2, "worker_memory_gb": 4, "shuffle_partitions": 200},
        )

        assert resp.status_code == 200
        mock_spawn.assert_awaited_once()
        params_arg = mock_spawn.await_args.args[0]
        assert params_arg.include_kafka is False

    def test_requires_kafka_topic_submitted_without_include_kafka_stays_false(self, tmp_path, monkeypatch):
        """(a) A requires_kafka: true topic's spawn form submitted WITHOUT
        include_kafka checked -> include_kafka=False is honored. The
        manifest only pre-checks the drawer's default -- it must not
        silently override an unchecked box at spawn time (the D1
        reversal's whole point)."""
        self._write_streaming_topic(tmp_path)

        ready_status = _status(ClusterState.READY, message="READY", alive_workers=3)
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=True, status=ready_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        with patch.object(config, "CONTENT_DIR", tmp_path):
            resp = client.post(
                "/topics/streaming-topic/spawn",
                # include_kafka intentionally omitted -- as if the learner unchecked it.
                data={"worker_count": 3, "worker_cores": 2, "worker_memory_gb": 4, "shuffle_partitions": 200},
            )

        assert resp.status_code == 200
        mock_spawn.assert_awaited_once()
        params_arg = mock_spawn.await_args.args[0]
        assert params_arg.include_kafka is False

    def test_non_streaming_topic_submitted_with_include_kafka_checked_honored(self, monkeypatch):
        """(b) A requires_kafka: false topic's spawn form submitted WITH
        include_kafka checked and a broker count -> include_kafka=True and
        the broker count are honored -- Kafka is available on any topic,
        not gated to streaming topics only."""
        ready_status = _status(ClusterState.READY, message="READY", alive_workers=3)
        mock_spawn = AsyncMock(return_value=SpawnOutcome(ok=True, status=ready_status))
        monkeypatch.setattr(topics_module.manager, "spawn", mock_spawn)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        resp = client.post(
            "/topics/partitioning-shuffle/spawn",
            data={
                "worker_count": 3, "worker_cores": 2, "worker_memory_gb": 4, "shuffle_partitions": 200,
                "include_kafka": "true", "kafka_broker_count": 4,
            },
        )

        assert resp.status_code == 200
        mock_spawn.assert_awaited_once()
        params_arg = mock_spawn.await_args.args[0]
        assert params_arg.include_kafka is True
        assert params_arg.kafka_broker_count == 4


class TestPanelContextKafkaDefaults:
    """docs/architecture/multi-broker-kafka-cluster.md D-MBK1's other half:
    `topic.requires_kafka` still pre-checks the drawer's "Include Kafka" box
    by default (via `_cluster_form.html`'s `{% if topic.requires_kafka %}
    checked{% endif %}`) -- a UI-level default, not a spawn override.
    Verified via the rendered GET /topics/<id> page markup, the same way
    TestTopicPage checks rendered content."""

    def test_requires_kafka_true_topic_renders_kafka_checkbox_pre_checked(self, tmp_path):
        topic_dir = tmp_path / "streaming-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({
                "id": "streaming-topic", "title": "Streaming Topic", "content": "concept.md",
                "notebook": "notebook.ipynb", "requires_kafka": True,
            }),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok\n\n## What it is\n\nblurb.\n", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            resp = client.get("/topics/streaming-topic")

        assert resp.status_code == 200
        checkbox_pos = resp.text.index('name="include_kafka"')
        checkbox_tag = resp.text[checkbox_pos - 40 : checkbox_pos + 120]
        assert "checked" in checkbox_tag

    def test_requires_kafka_false_topic_renders_kafka_checkbox_unchecked(self):
        resp = client.get("/topics/partitioning-shuffle")

        assert resp.status_code == 200
        checkbox_pos = resp.text.index('name="include_kafka"')
        checkbox_tag = resp.text[checkbox_pos - 40 : checkbox_pos + 120]
        assert "checked" not in checkbox_tag


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
    stale reference after teardown+respawn -- the shell's right pane keys the
    iframe src off `status.spawn_id`, so a new spawn_id must appear.

    Retargeted from the retired `/topics/{id}/panel` route (topic-shell
    redesign, code-review finding: `/panel` was UI-unreachable dead code once
    the shell's right pane -- not the standalone panel template -- became the
    only place the iframe renders) onto `/topics/{id}`, which is the shell's
    real entry point and renders the same iframe via
    `fragments/_cluster_right_pane.html`."""

    def test_iframe_present_when_ready_with_current_spawn_id(self, monkeypatch):
        ready_status = _status(ClusterState.READY, message="READY", alive_workers=3, spawn_id=7)
        monkeypatch.setattr(topics_module.manager, "status", lambda: ready_status)

        resp = client.get("/topics/partitioning-shuffle")

        assert resp.status_code == 200
        assert "<iframe" in resp.text
        assert "spawn=7" in resp.text

    def test_no_iframe_when_not_ready(self, monkeypatch):
        idle_status = _status(ClusterState.IDLE, message="No cluster running.", params=None)
        monkeypatch.setattr(topics_module.manager, "status", lambda: idle_status)

        resp = client.get("/topics/partitioning-shuffle")

        assert resp.status_code == 200
        assert "<iframe" not in resp.text
