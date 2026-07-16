"""Regression coverage for issue #22 — SSE OOB fragments must not clobber the
`dash-view`/`active` class attribute on `#overview-content` /
`#job-detail-content` / `#node-detail-container`.

Background (see docs/acceptance/phase-2-5.md, Finding 1, and issue #22):
`app/web/templates/dashboard/_dashboard_body.html` (formerly `page.html`,
folded into the Cluster Monitor panel body by issue #23) gives these three containers
`class="dash-view active"` / `class="dash-view"`, and the client-side view
switcher (`page.html`'s inline `<script>`) toggles the `active` class to keep
exactly one view visible at a time (ADR D-B). The three `*_oob.html`
fragments used to re-declare the same ids with a bare `hx-swap-oob="true"`,
which does an *outerHTML* swap — replacing the whole element, attributes
included — silently stripping the `class` attribute off all three containers
on every SSE push (~every 2s), permanently breaking the view switcher after
the first push.

The fix targets each OOB swap at the container's *contents* only
(`hx-swap-oob="innerHTML:#<id>"`), so the wrapping `<div>` — and its `class`
attribute — is never touched by the swap, only its children.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from app import config
from app.lifecycle.manager import ClusterState, manager
from app.lifecycle.renderer import ClusterParams
from app.main import app
from app.monitoring import docker_stats
from app.monitoring.collector import DashboardCollector
from app.monitoring.docker_stats import ContainerStat
from app.monitoring.model import JobSummary, Snapshot
from app.spark_api import app_client
from app.web.routes import dashboard as dashboard_module
from app.web.routes.dashboard import _render_oob_payload

templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))
templates.env.globals["config"] = config

client = TestClient(app)


def _idle_snapshot() -> Snapshot:
    return Snapshot(cluster_active=False, has_job=False)


def _snapshot_with_job(app_id: str, stage_label: str, elapsed: str) -> Snapshot:
    return Snapshot(
        cluster_active=True,
        has_job=True,
        job=JobSummary(
            name=app_id,
            app_id=app_id,
            status_label="Completed",
            status_bg="#f0fdf4",
            status_color="#16a34a",
            stage_label=stage_label,
            stage_name="count at NativeMethodAccessorImpl.java:0",
            elapsed=elapsed,
            eta_label="~0s",
        ),
    )


class TestOobFragmentsPreserveContainerClass:
    """Each OOB fragment must swap only the target's innerHTML, never its
    own attributes (including `class`) via an outerHTML/same-id swap."""

    def test_overview_oob_targets_innerhtml_not_outerhtml(self):
        html = templates.get_template("dashboard/fragments/overview_oob.html").render(
            {"snapshot": _idle_snapshot()}
        )
        assert 'hx-swap-oob="innerHTML:#overview-content"' in html
        # A bare `hx-swap-oob="true"` would do an outerHTML swap and strip
        # the page's own `class="dash-view active"` off this element --
        # explicitly guard against regressing back to that form.
        assert 'hx-swap-oob="true"' not in html

    def test_job_detail_oob_targets_innerhtml_not_outerhtml(self):
        html = templates.get_template("dashboard/fragments/job_detail_oob.html").render(
            {"snapshot": _idle_snapshot()}
        )
        assert 'hx-swap-oob="innerHTML:#job-detail-content"' in html
        assert 'hx-swap-oob="true"' not in html

    def test_node_detail_oob_targets_innerhtml_not_outerhtml(self):
        html = templates.get_template("dashboard/fragments/node_detail_oob.html").render(
            {"snapshot": _idle_snapshot()}
        )
        assert 'hx-swap-oob="innerHTML:#node-detail-container"' in html
        assert 'hx-swap-oob="true"' not in html


class TestPageDefinesTheClassesTheFragmentsMustNotClobber:
    """Guards the other half of the contract: `_dashboard_body.html` must
    still define `dash-view`/`active` classes on these three containers, and
    the OOB fragments must not also declare a `class` attribute of their own
    (which would fight with whatever `_dashboard_body.html` set, even under
    an innerHTML swap)."""

    def _render_page(self) -> str:
        return templates.get_template("dashboard/_dashboard_body.html").render(
            {
                "snapshot": _idle_snapshot(),
                "master_ui_url": "http://localhost:8080",
                "driver_ui_url": "http://localhost:4040",
            }
        )

    def test_page_gives_overview_the_active_class_by_default(self):
        html = self._render_page()
        assert 'id="overview-content" class="dash-view active"' in html
        assert 'id="job-detail-content" class="dash-view"' in html
        assert 'id="node-detail-container" class="dash-view"' in html

    def test_oob_fragments_declare_no_class_attribute_of_their_own(self):
        for name, container_id in (
            ("overview_oob.html", "overview-content"),
            ("job_detail_oob.html", "job-detail-content"),
            ("node_detail_oob.html", "node-detail-container"),
        ):
            html = templates.get_template(f"dashboard/fragments/{name}").render(
                {"snapshot": _idle_snapshot()}
            )
            assert f'id="{container_id}"' in html
            assert "class=" not in html.split(f'id="{container_id}"')[1].split(">")[0]


class TestJobDetailOobReflectsEachSnapshot:
    """Regression coverage for issue #24 — the Job Detail view froze on the
    first job of a session and never updated to later jobs.

    Root cause (confirmed by live reproduction against a real cluster, not
    just code reading): `config.DRIVER_APP_UI_URL` / `app_client.py` were
    hardcoded to `:4040` only. A learner switching topic notebooks without
    shutting down the prior Jupyter kernel leaves an earlier job's
    SparkContext alive holding `:4040`; Spark silently rebinds the next
    still-alive SparkContext's UI to `:4041`. The dashboard kept re-querying
    `:4040` forever, so it stayed locked onto whichever application first
    grabbed that port — even after a much larger, different job was running
    elsewhere. This was NOT an SSE/OOB delivery bug (issue #22's fix, which
    made the OOB payload swap `innerHTML` every cycle, was already correct
    and confirmed working via a live stream capture); the OOB payload was
    being faithfully regenerated every cycle from REST data that itself
    never advanced.

    These tests guard the two halves of the fix directly: (1) the rendered
    `#job-detail-content` OOB fragment is a genuine function of the
    snapshot's job data (would have failed if the template/route cached or
    otherwise failed to re-derive content per snapshot), and (2)
    `app_client.resolve_current_app()` actually follows the most-recently
    started application across `DRIVER_APP_UI_PORTS` rather than being stuck
    on a fixed port (would have failed against the pre-fix `fetch_current_app_id()`,
    which only ever looked at `:4040`)."""

    def test_rendered_job_detail_content_differs_across_snapshots_with_different_jobs(self):
        first = _snapshot_with_job("app-20260715183750-0001", "Stage 8 / 8", "14s")
        second = _snapshot_with_job("app-20260715183826-0002", "Stage 3 / 6", "58s")

        html_first = templates.get_template("dashboard/fragments/job_detail_oob.html").render(
            {"snapshot": first}
        )
        html_second = templates.get_template("dashboard/fragments/job_detail_oob.html").render(
            {"snapshot": second}
        )

        assert "app-20260715183750-0001" in html_first
        assert "app-20260715183826-0002" not in html_first
        assert "app-20260715183826-0002" in html_second
        assert "app-20260715183750-0001" not in html_second
        assert html_first != html_second

    def test_full_oob_payload_reflects_a_newly_resolved_app_not_a_stale_one(self):
        """The route-level entry point (`_render_oob_payload`, what
        `/dashboard/stream` actually sends every SSE tick) must not itself
        introduce any caching that the fragment-level test above wouldn't
        catch."""
        first = _snapshot_with_job("app-old", "Stage 8 / 8", "14s")
        second = _snapshot_with_job("app-new-and-much-larger", "Stage 1 / 40", "2s")

        payload_first = _render_oob_payload(request=None, snapshot=first)
        payload_second = _render_oob_payload(request=None, snapshot=second)

        assert "app-old" in payload_first
        assert "app-new-and-much-larger" in payload_second
        assert "app-old" not in payload_second


class TestResolveCurrentAppFollowsTheMostRecentJob:
    """Issue #24: `collector.collect_once()` must resolve the current
    application via `app_client.resolve_current_app()` (which probes every
    `DRIVER_APP_UI_PORTS` entry and follows the most recently started
    running app) rather than a fixed-port lookup."""

    @pytest.fixture(autouse=True)
    def _fast_cadence(self, monkeypatch):
        monkeypatch.setattr(config, "DASHBOARD_COLLECTOR_INTERVAL_S", 0.01)

    async def _fake_sample(self, cpu_limits, timeout_s=3.0):
        return [
            ContainerStat(
                name="spark-master",
                cpu_pct=10.0,
                mem_used_bytes=100 * 1024**2,
                mem_limit_bytes=1024**3,
                net_rx_bytes=0,
                net_tx_bytes=0,
                block_read_bytes=0,
                block_write_bytes=0,
            )
        ]

    @pytest.mark.asyncio
    async def test_collect_once_reports_whichever_app_resolve_current_app_returns(self, monkeypatch):
        """Simulates a second, later-started application landing on a
        non-default port (:4041) while an earlier one (:4040) is still
        alive -- exactly the scenario reproduced live against a real
        cluster. `collect_once()` must surface the newer one, because
        that's what `resolve_current_app()` (tested directly in
        test_app_client.py) is defined to return."""
        monkeypatch.setattr(docker_stats, "sample", self._fake_sample)
        newer_app = app_client.AppRef(app_id="app-newer-on-4041", base_url="http://localhost:4041")
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=2.0: newer_app)
        monkeypatch.setattr(app_client, "fetch_executors", lambda app, timeout_s=2.0: [])
        monkeypatch.setattr(
            app_client,
            "fetch_stages",
            lambda app, timeout_s=2.0: [
                {"stageId": 5, "attemptId": 0, "status": "ACTIVE", "numTasks": 4, "executorRunTime": 1000}
            ],
        )
        monkeypatch.setattr(app_client, "fetch_task_list", lambda *a, **kw: [])
        manager.state = ClusterState.READY
        manager.params = ClusterParams(worker_count=1, worker_cores=1, worker_memory_gb=1)

        snapshot = await DashboardCollector().collect_once()

        assert snapshot.job is not None
        assert snapshot.job.app_id == "app-newer-on-4041"


class TestDashboardRedirect:
    """Issue #23: the standalone `/dashboard` page is retired -- it must 307
    redirect to a real topic page with `?monitor=open` so bookmarks/links to
    the old URL keep working instead of dead-ending (Decision B2), reusing
    `topics.index`'s first-topic resolution."""

    @pytest.fixture(autouse=True)
    def _idle_cluster(self):
        # Avoid a real docker_stats/app_client probe from `_driver_ui_url()`
        # (which runs regardless of cluster state) contending with whatever
        # `manager.state` a previous test left behind.
        manager.state = ClusterState.IDLE
        yield

    def test_redirects_to_first_topic_with_monitor_open(self, monkeypatch):
        topics = [SimpleNamespace(id="bucketing"), SimpleNamespace(id="aqe")]
        monkeypatch.setattr(dashboard_module.loader, "list_topics", lambda: topics)

        resp = client.get("/dashboard", follow_redirects=False)

        assert resp.status_code == 307
        assert resp.headers["location"] == "/topics/bucketing?monitor=open"

    def test_redirects_to_fallback_topic_when_no_topics_exist(self, monkeypatch):
        monkeypatch.setattr(dashboard_module.loader, "list_topics", lambda: [])

        resp = client.get("/dashboard", follow_redirects=False)

        assert resp.status_code == 307
        assert resp.headers["location"] == "/topics/partitioning-shuffle?monitor=open"


class TestDashboardPanelRoute:
    """Issue #23: `/dashboard/panel` serves the extracted panel body (the
    three dash-view containers + the SSE-connect element) that the shell's
    Cluster Monitor panel HTMX-fetches into `#monitor-body` on open."""

    @pytest.fixture(autouse=True)
    def _idle_cluster(self, monkeypatch):
        manager.state = ClusterState.IDLE
        monkeypatch.setattr(app_client, "resolve_current_app", lambda timeout_s=2.0: None)

    def test_panel_route_renders_the_three_dash_views_and_sse_element(self):
        resp = client.get("/dashboard/panel")

        assert resp.status_code == 200
        assert 'id="overview-content" class="dash-view active"' in resp.text
        assert 'id="job-detail-content" class="dash-view"' in resp.text
        assert 'id="node-detail-container" class="dash-view"' in resp.text
        assert 'sse-connect="/dashboard/stream"' in resp.text
