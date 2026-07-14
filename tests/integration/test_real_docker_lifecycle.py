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

import pytest

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
