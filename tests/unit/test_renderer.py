"""Tests for app/lifecycle/renderer.py — US-1.2's "reject before spawning
with a clear message" requirement.

Covers: in-range params pass; out-of-range worker_count/cores/memory are
rejected with clear messages; the ~48GB resource-ceiling math at/around its
boundary; and (at the manager level, in test_manager.py) that a rejected
validation never reaches compose_ops/renderer.render.
"""
from __future__ import annotations

from unittest.mock import patch

from app import config
from app.lifecycle import renderer
from app.lifecycle.renderer import ClusterParams


def _default_params(**overrides) -> ClusterParams:
    base = dict(
        worker_count=config.DEFAULTS["worker_count"],
        worker_cores=config.DEFAULTS["worker_cores"],
        worker_memory_gb=config.DEFAULTS["worker_memory_gb"],
        driver_memory_gb=config.DEFAULTS["driver_memory_gb"],
        shuffle_partitions=config.DEFAULTS["shuffle_partitions"],
        aqe_enabled=config.DEFAULTS["aqe_enabled"],
    )
    base.update(overrides)
    return ClusterParams(**base)


class TestInRangeAccepted:
    def test_defaults_pass(self):
        result = renderer.validate(_default_params())
        assert result.ok
        assert result.errors == []

    def test_boundary_low_values_pass(self):
        result = renderer.validate(
            _default_params(worker_count=1, worker_cores=1, worker_memory_gb=1)
        )
        assert result.ok

    def test_boundary_high_values_pass(self):
        # 1 (master) + 5*8 (workers) + 2 (driver) = 43GB, under the 48GB ceiling.
        result = renderer.validate(
            _default_params(worker_count=5, worker_cores=4, worker_memory_gb=8)
        )
        assert result.ok

    def test_large_shuffle_partitions_pass(self):
        result = renderer.validate(_default_params(shuffle_partitions=10_000))
        assert result.ok


class TestOutOfRangeRejected:
    def test_worker_count_too_low(self):
        result = renderer.validate(_default_params(worker_count=0))
        assert not result.ok
        assert any("worker_count" in e for e in result.errors)

    def test_worker_count_too_high(self):
        result = renderer.validate(_default_params(worker_count=6))
        assert not result.ok
        assert any("worker_count" in e for e in result.errors)

    def test_worker_cores_too_low(self):
        result = renderer.validate(_default_params(worker_cores=0))
        assert not result.ok
        assert any("worker_cores" in e for e in result.errors)

    def test_worker_cores_too_high(self):
        result = renderer.validate(_default_params(worker_cores=5))
        assert not result.ok
        assert any("worker_cores" in e for e in result.errors)

    def test_worker_memory_too_low(self):
        result = renderer.validate(_default_params(worker_memory_gb=0))
        assert not result.ok
        assert any("worker_memory_gb" in e for e in result.errors)

    def test_worker_memory_too_high(self):
        result = renderer.validate(_default_params(worker_memory_gb=9))
        assert not result.ok
        assert any("worker_memory_gb" in e for e in result.errors)

    def test_shuffle_partitions_not_positive(self):
        result = renderer.validate(_default_params(shuffle_partitions=0))
        assert not result.ok
        assert any("shuffle_partitions" in e for e in result.errors)

    def test_shuffle_partitions_negative(self):
        result = renderer.validate(_default_params(shuffle_partitions=-5))
        assert not result.ok
        assert any("shuffle_partitions" in e for e in result.errors)

    def test_driver_memory_not_positive(self):
        result = renderer.validate(_default_params(driver_memory_gb=0))
        assert not result.ok
        assert any("driver_memory_gb" in e for e in result.errors)

    def test_errors_are_clear_strings(self):
        """Errors must be human-readable messages, not codes/exceptions."""
        result = renderer.validate(_default_params(worker_count=99))
        assert not result.ok
        for e in result.errors:
            assert isinstance(e, str)
            assert len(e) > 0


class TestResourceCeilingBoundary:
    """Exercises the ~48GB ceiling math: master(1) + worker_count*worker_memory_gb
    + driver_memory_gb (PLAN.md §2), at and around the boundary."""

    def test_exactly_at_ceiling_passes(self):
        # 1 + 5*8 + 7 = 48 -> exactly at the ceiling, should pass.
        params = _default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=7)
        result = renderer.validate(params)
        assert result.total_gb == 48
        assert result.ok

    def test_one_gb_over_ceiling_rejected(self):
        # 1 + 5*8 + 8 = 49 -> one over, should be rejected.
        params = _default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=8)
        result = renderer.validate(params)
        assert result.total_gb == 49
        assert not result.ok
        assert any("ceiling" in e or "48" in e for e in result.errors)

    def test_ceiling_message_is_clear(self):
        params = _default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=20)
        result = renderer.validate(params)
        assert not result.ok
        msg = "; ".join(result.errors)
        assert "48" in msg  # names the actual ceiling, not a vague "too big"
        assert "GB" in msg

    def test_well_under_ceiling_passes(self):
        params = _default_params(worker_count=1, worker_memory_gb=1, driver_memory_gb=1)
        result = renderer.validate(params)
        assert result.total_gb == 3
        assert result.ok

    def test_ceiling_uses_actual_requested_values_not_defaults(self):
        """Regression guard: total_gb must scale with worker_count *and*
        worker_memory_gb, not just one of them."""
        small = renderer.validate(_default_params(worker_count=1, worker_memory_gb=1, driver_memory_gb=1))
        large = renderer.validate(_default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=1))
        assert small.total_gb < large.total_gb


class TestRejectionHappensBeforeAnyContainerAction:
    """US-1.2: 'the UI rejects the configuration before spawning with a clear
    message, rather than attempting it and failing mid-spawn.' At the
    renderer layer this means validate() itself never touches the
    filesystem/templates — render() is a fully separate call the caller must
    explicitly invoke after checking validate().ok."""

    def test_validate_does_not_render(self):
        with patch.object(renderer, "render") as mock_render:
            renderer.validate(_default_params(worker_count=99))
            mock_render.assert_not_called()

    def test_validate_is_pure_no_filesystem_writes(self, tmp_path):
        with patch.object(config, "RENDERED_DIR", tmp_path / "rendered"):
            renderer.validate(_default_params(worker_count=99))
            assert not (tmp_path / "rendered").exists()
