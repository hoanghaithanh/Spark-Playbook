"""Coverage for issue #59 / US-MBK4 (sub-story d) — the 4th "Kafka" tab in
the Cluster Monitor panel (`_kafka_body.html` / `kafka_oob.html`).

Mirrors the fixture/assertion style already established in
`test_dashboard_routes.py` for the other three views: build `Snapshot`
fixtures directly (no live cluster/collector), render the Jinja2 templates
in isolation, and check `_render_oob_payload()` end to end.
"""
from __future__ import annotations

from fastapi.templating import Jinja2Templates

from app import config
from app.monitoring.model import (
    IsrShrinkEvent,
    KafkaBrokerStat,
    KafkaSnapshot,
    KafkaTopicRow,
    Snapshot,
)
from app.web.routes.dashboard import _render_oob_payload

templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))
templates.env.globals["config"] = config


def _render_kafka_body(snapshot: Snapshot) -> str:
    return templates.get_template("dashboard/fragments/_kafka_body.html").render(
        {"snapshot": snapshot}
    )


def _idle_snapshot() -> Snapshot:
    return Snapshot(cluster_active=False, has_job=False)


def _healthy_kafka_snapshot() -> Snapshot:
    kafka = KafkaSnapshot(
        running=True,
        brokers_online=3,
        brokers_total=3,
        under_replicated_count=0,
        active_controller_id=1,
        throughput_label="120 msg/s",
        consumer_group_count=1,
        p99_latency_label="—",
        brokers=[
            KafkaBrokerStat(
                node_id=1,
                container_name="spark-kafka-1",
                online=True,
                is_controller=True,
                partitions_led=6,
                cpu_pct=12,
                cpu_label="12%",
                cpu_color=config.DASHBOARD_COLOR_GREEN,
            ),
            KafkaBrokerStat(
                node_id=2,
                container_name="spark-kafka-2",
                online=True,
                partitions_led=6,
            ),
        ],
        topics=[KafkaTopicRow(name="prices", partitions=6, replication_factor=3)],
    )
    return Snapshot(cluster_active=True, has_job=False, kafka=kafka)


def _faulted_kafka_snapshot() -> Snapshot:
    kafka = KafkaSnapshot(
        running=True,
        brokers_online=2,
        brokers_total=3,
        under_replicated_count=2,
        active_controller_id=1,
        brokers=[
            KafkaBrokerStat(node_id=1, container_name="spark-kafka-1", online=True, is_controller=True),
            KafkaBrokerStat(node_id=2, container_name="spark-kafka-2", online=False),
        ],
        isr_shrink_events=[
            IsrShrinkEvent(
                topic="prices",
                partition=0,
                dropped_replica_id=2,
                timestamp_label="10:02:31",
                detail_label="prices-0 ISR shrank to {1,3}",
            )
        ],
    )
    return Snapshot(cluster_active=True, has_job=False, kafka=kafka)


class TestKafkaNotRunningState:
    """Given no Kafka broker containers (kafka is None), the tab renders a
    clear empty state, never an error/blank/stale render (US-MBK4)."""

    def test_renders_kafka_not_running_message(self):
        html = _render_kafka_body(_idle_snapshot())

        assert "Kafka not running" in html
        # None of the populated-state sections should appear.
        assert "Brokers online" not in html
        assert "ISR-shrink events" not in html


class TestKafkaHealthyState:
    """Given a live Kafka spawn with no fault, the health strip/broker grid
    render real data and — per D-MBK8 (G3, no suggestion/fix field anywhere,
    enforced structurally by the model) — no remedy text appears anywhere in
    the rendered output."""

    def test_renders_health_strip_and_broker_grid(self):
        html = _render_kafka_body(_healthy_kafka_snapshot())

        assert "Brokers online" in html
        assert "3/3" in html
        assert "kafka-1" in html
        assert "kafka-2" in html
        assert "CONTROLLER" in html
        assert "prices" in html  # topics table

    def test_no_incident_cards_and_no_suggestion_text_when_healthy(self):
        html = _render_kafka_body(_healthy_kafka_snapshot())

        assert "No ISR changes observed" in html
        assert "Suggestion" not in html
        # The health-strip tile is expected; the *incident card* wording
        # ("... below their replication factor.") must not appear absent a
        # fault, since under_replicated_count is 0 in this fixture.
        assert "below their replication factor" not in html

    def test_no_suggestion_text_anywhere_structurally(self):
        # D-MBK8: KafkaSnapshot and every nested dataclass carries no
        # suggestion/fix field, so this can never regress via new data --
        # still worth a direct assertion on the rendered output.
        html = _render_kafka_body(_healthy_kafka_snapshot())
        assert "suggestion" not in html.lower()


class TestKafkaFaultState:
    """Given under_replicated_count > 0 and a non-empty isr_shrink_events
    ring buffer, the diagnostics cards and ISR-shrink feed populate with
    factual, non-prescriptive text (US-MBK5 groundwork / D-MBK8)."""

    def test_diagnostics_card_and_isr_feed_populate(self):
        html = _render_kafka_body(_faulted_kafka_snapshot())

        assert "Under-replicated partitions" in html
        assert "2 partition(s) below their replication factor." in html
        assert "prices-0 ISR shrank to {1,3}" in html
        assert "10:02:31" in html
        assert "Container not available" in html  # offline broker-2 card

    def test_no_suggestion_text_even_with_a_real_fault(self):
        html = _render_kafka_body(_faulted_kafka_snapshot())
        assert "suggestion" not in html.lower()


class TestOobPayloadIncludesKafkaAsFourthSwap:
    """`_render_oob_payload()` must append the Kafka fragment as a 4th OOB
    swap over the same shared SSE connection (US-MBK4, D-MBK7) -- no second
    connection, additive to the existing overview/job/node swaps."""

    def test_payload_contains_kafka_oob_container(self):
        payload = _render_oob_payload(request=None, snapshot=_idle_snapshot())

        assert 'id="kafka-content" hx-swap-oob="innerHTML:#kafka-content"' in payload
        assert "Kafka not running" in payload

    def test_payload_reflects_kafka_snapshot_data(self):
        payload = _render_oob_payload(request=None, snapshot=_healthy_kafka_snapshot())

        assert "kafka-1" in payload
        assert "120 msg/s" in payload


class TestPanelBodyExposesKafkaAsAFourthView:
    """`_dashboard_body.html` must define the `#kafka-content` `.dash-view`
    container and the Overview/Kafka nav buttons, mirroring the panel's
    existing three-view container contract (US-MBK4, D-MBK7)."""

    def _render_panel_body(self, snapshot: Snapshot) -> str:
        return templates.get_template("dashboard/_dashboard_body.html").render(
            {
                "request": None,
                "snapshot": snapshot,
                "master_ui_url": "http://localhost:8080",
                "driver_ui_url": "http://localhost:4040",
            }
        )

    def test_kafka_view_container_and_nav_buttons_present(self):
        html = self._render_panel_body(_idle_snapshot())

        assert 'id="kafka-content" class="dash-view"' in html
        assert 'id="dash-nav-overview"' in html
        assert 'id="dash-nav-kafka"' in html
        assert "dashGoTo('kafka')" in html

    def test_kafka_view_renders_populated_kafka_data_inline(self):
        html = self._render_panel_body(_healthy_kafka_snapshot())

        assert "kafka-1" in html
