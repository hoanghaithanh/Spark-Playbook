"""Shared fixtures for the Spark Playbook test suite.

`app.lifecycle.manager.manager` is a module-level singleton (by design, per
PLAN.md's "single-user/single-process" note in manager.py's docstring), so
tests that touch it must reset its state before/after each test to avoid
cross-test leakage.
"""
from __future__ import annotations

import pytest

from app.lifecycle.manager import ClusterManager, ClusterState


@pytest.fixture
def fresh_manager():
    """A brand-new ClusterManager instance, isolated from the module singleton.

    Prefer this over the shared `app.lifecycle.manager.manager` singleton in
    unit tests so tests can run in any order/in parallel without leaking
    state (in-flight tasks, spawn_id counters, etc).
    """
    return ClusterManager()


@pytest.fixture(autouse=True)
def _reset_singleton_manager():
    """Reset the module-level singleton around every test.

    Route-level tests go through `app.web.routes.topics`, which imports the
    singleton `manager` directly, so those tests can't use `fresh_manager`
    and need the singleton itself reset instead.
    """
    from app.lifecycle import manager as manager_module

    manager_module.manager.state = ClusterState.IDLE
    manager_module.manager.message = "No cluster running."
    manager_module.manager.params = None
    manager_module.manager.spawn_id = 0
    manager_module.manager.alive_workers = None
    manager_module.manager.error = None
    manager_module.manager._task = None
    yield
    manager_module.manager.state = ClusterState.IDLE
    manager_module.manager.message = "No cluster running."
    manager_module.manager.params = None
    manager_module.manager.spawn_id = 0
    manager_module.manager.alive_workers = None
    manager_module.manager.error = None
    manager_module.manager._task = None
