"""Unit-test-only fixtures (scoped to tests/unit/, unlike tests/conftest.py
which also covers tests/integration/ against real Docker).
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.lifecycle import compose_ops


@pytest.fixture(autouse=True)
def _default_no_foreign_owner(request, monkeypatch):
    """`ClusterManager.spawn()`/`teardown()` now call
    `compose_ops.running_owner()` (issue #38 ownership guard) before doing
    anything else. Default it to "nothing running" for every unit test so
    the guard doesn't shell out to real Docker or spuriously refuse — tests
    that specifically exercise the guard (or `running_owner()` itself, in
    test_compose_ops.py) override/skip this with their own monkeypatch.

    Excludes test_compose_ops.py: those tests exercise `running_owner()`'s
    own implementation directly, so patching it out here would shadow the
    real function under test.
    """
    if request.module.__name__.endswith("test_compose_ops"):
        return
    monkeypatch.setattr(compose_ops, "running_owner", AsyncMock(return_value=None))
