"""Regression coverage for issue #22 — SSE OOB fragments must not clobber the
`dash-view`/`active` class attribute on `#overview-content` /
`#job-detail-content` / `#node-detail-container`.

Background (see docs/acceptance/phase-2-5.md, Finding 1, and issue #22):
`app/web/templates/dashboard/page.html` gives these three containers
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

from fastapi.templating import Jinja2Templates

from app import config
from app.monitoring.model import Snapshot

templates = Jinja2Templates(directory=str(config.WEB_TEMPLATES_DIR))
templates.env.globals["config"] = config


def _idle_snapshot() -> Snapshot:
    return Snapshot(cluster_active=False, has_job=False)


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
    """Guards the other half of the contract: `page.html` must still define
    `dash-view`/`active` classes on these three containers, and the OOB
    fragments must not also declare a `class` attribute of their own (which
    would fight with whatever `page.html` set, even under an innerHTML
    swap)."""

    def _render_page(self) -> str:
        return templates.get_template("dashboard/page.html").render(
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
