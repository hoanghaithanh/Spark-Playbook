"""Real end-to-end smoke test: spawns an *actual* Spark Standalone cluster via
Docker Compose (the developer's Phase 1 manual verification, automated) and
tears it down again.

Explicitly separate from the primary suite (tests/unit/) per the test plan:
this needs Docker Desktop running and the `sparkpb/spark:4.0.3` image already
built (`compose/build.sh`), takes tens of seconds, and is not deterministic in
the way the mocked suite is (real network/container timing). Skipped by
default; opt in with:

    RUN_DOCKER_TESTS=1 pytest tests/integration/

(marked `integration` for `pytest -m integration -v` visibility/reporting,
but the actual skip gate is the RUN_DOCKER_TESTS env var, checked below.)

`compose/smoke_test.py` (Phase 0) already covers running a real shuffle job
*inside* the cluster via spark-submit -- that's a separate, existing artifact
and out of scope to duplicate here. This test instead exercises the Phase 1
app code path end-to-end: `ClusterManager.spawn()` against the real
`compose_ops`/`readiness` modules (no mocking) -> real `docker compose up` ->
real `:8080/json/` readiness poll -> `ClusterManager.teardown()`.
"""
from __future__ import annotations

import os
import urllib.error
import urllib.request

import pytest

from app import config
from app.lifecycle.manager import ClusterManager, ClusterState
from app.lifecycle.renderer import ClusterParams

pytestmark = pytest.mark.integration

_SKIP_REASON = (
    "Real Docker integration test skipped by default. Run with "
    "`RUN_DOCKER_TESTS=1 pytest tests/integration/` (requires Docker Desktop "
    "running and the sparkpb/spark:4.0.3 image already built via compose/build.sh)."
)


def _docker_tests_enabled() -> bool:
    return os.environ.get("RUN_DOCKER_TESTS") == "1"


@pytest.mark.skipif(not _docker_tests_enabled(), reason=_SKIP_REASON)
@pytest.mark.asyncio
async def test_spawn_a_real_single_worker_cluster_and_tear_it_down():
    manager = ClusterManager()
    # 1 worker / minimal cores+memory so this runs as fast as possible while
    # still exercising the real master<->worker registration handshake.
    params = ClusterParams(worker_count=1, worker_cores=1, worker_memory_gb=1, driver_memory_gb=1)

    try:
        outcome = await manager.spawn(params, timeout_s=90)

        assert outcome.ok is True, f"spawn failed: {outcome.status.message}"
        assert outcome.status.state == ClusterState.READY
        assert outcome.status.alive_workers == 1
    finally:
        final = await manager.teardown()
        assert final.state == ClusterState.IDLE


@pytest.mark.skipif(not _docker_tests_enabled(), reason=_SKIP_REASON)
@pytest.mark.asyncio
async def test_jupyter_csp_header_allows_the_app_origin_to_frame_it():
    """Issue #7 regression test (PLAN.md §6/R3, US-1.3): the embedded
    JupyterLab iframe used to render blank because Jupyter's default CSP
    (`frame-ancestors 'self'`) blocked framing from the FastAPI app's
    different origin. `driver/jupyter_config.py` (loaded via
    `jupyter lab --config=/workspace/driver/jupyter_config.py`, wired in
    `compose/templates/docker-compose.yml.j2`) fixes this by setting a CSP
    that explicitly allows `app.config.APP_ORIGIN`.

    This can only be verified against a real running Jupyter server (the
    header is emitted by the actual Tornado process reading the mounted
    config file inside the container), so it lives here in the Docker
    integration suite rather than as a pure unit test. A full Playwright
    browser-level check (confirming the iframe actually renders real
    JupyterLab content, not just the header) was performed manually during
    development -- see the developer's report for that evidence; this test
    is the automatable, CI-friendly subset of that verification.
    """
    manager = ClusterManager()
    params = ClusterParams(worker_count=1, worker_cores=1, worker_memory_gb=1, driver_memory_gb=1)

    try:
        outcome = await manager.spawn(params, timeout_s=90)
        assert outcome.ok is True, f"spawn failed: {outcome.status.message}"

        req = urllib.request.Request(f"{config.JUPYTER_URL}/lab", method="HEAD")
        try:
            resp = urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as e:
            # Jupyter's /lab endpoint can 405 on HEAD depending on version --
            # the headers are still present on the error response itself.
            resp = e

        csp = resp.headers.get("Content-Security-Policy", "")
        assert "frame-ancestors" in csp, f"no frame-ancestors directive in CSP: {csp!r}"
        assert config.APP_ORIGIN in csp, f"app origin {config.APP_ORIGIN!r} not allowed by CSP: {csp!r}"
        # test-engineer re-validation of #7: browsers treat localhost:8000 and
        # 127.0.0.1:8000 as different origins for frame-ancestors purposes,
        # so a learner opening the app via either spelling must both work --
        # driver/jupyter_config.py defensively allows both even though
        # app.config.APP_ORIGIN itself stays the canonical localhost value.
        assert "http://127.0.0.1:8000" in csp, f"127.0.0.1 origin not allowed by CSP: {csp!r}"
        assert "'self'" in csp

        # Jupyter must not also emit the legacy X-Frame-Options header, which
        # would independently block framing regardless of the CSP above.
        assert resp.headers.get("X-Frame-Options") is None
    finally:
        final = await manager.teardown()
        assert final.state == ClusterState.IDLE
