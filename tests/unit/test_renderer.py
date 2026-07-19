"""Tests for app/lifecycle/renderer.py — US-1.2's "reject before spawning
with a clear message" requirement.

Covers: in-range params pass; out-of-range worker_count/cores/memory are
rejected with clear messages; the ~48GB resource-ceiling math at/around its
boundary; and (at the manager level, in test_manager.py) that a rejected
validation never reaches compose_ops/renderer.render.
"""
from __future__ import annotations

import importlib
from unittest.mock import patch

import yaml

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

    def test_boundary_high_values_pass_when_under_the_ceiling(self):
        # worker_count=5 and worker_cores=4 are both at their range max, but
        # worker_memory_gb is kept modest (4, not 8) so the total stays under
        # the 32GB ceiling: 1 (master) + 5*4 (workers) + 2 (driver) = 23GB.
        # Isolates "range boundaries are accepted" from the ceiling check --
        # see TestResourceCeilingBoundary for the case where the true
        # all-ranges-maxed combination (worker_memory_gb=8 too) now correctly
        # gets rejected by the ceiling (issue #6's fix).
        result = renderer.validate(
            _default_params(worker_count=5, worker_cores=4, worker_memory_gb=4)
        )
        assert result.ok
        assert result.total_gb == 23

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
    """Exercises the 32GB ceiling math: master(1) + worker_count*worker_memory_gb
    + driver_memory_gb (PLAN.md §2), at and around the boundary.

    Issue #6 (test-engineer acceptance validation): the ceiling was originally
    48GB, which was mathematically unreachable through the UI's own documented
    ranges (max in-range total was 43GB) -- the "UI rejects an over-budget
    config" acceptance criterion could never actually fire. Lowered to 32GB;
    see `app/config.py::RESOURCE_CEILING_GB` for the full rationale. These
    tests use `config.RESOURCE_CEILING_GB` rather than a hardcoded literal so
    they track the real value instead of re-encoding a number that could drift
    out of sync with it again.
    """

    def test_exactly_at_ceiling_passes(self):
        # 1 + 3*8 + 7 = 32 -> exactly at the ceiling, should pass.
        params = _default_params(worker_count=3, worker_memory_gb=8, driver_memory_gb=7)
        result = renderer.validate(params)
        assert result.total_gb == config.RESOURCE_CEILING_GB
        assert result.ok

    def test_one_gb_over_ceiling_rejected(self):
        # 1 + 3*8 + 8 = 33 -> one over, should be rejected.
        params = _default_params(worker_count=3, worker_memory_gb=8, driver_memory_gb=8)
        result = renderer.validate(params)
        assert result.total_gb == config.RESOURCE_CEILING_GB + 1
        assert not result.ok
        assert any("ceiling" in e or str(config.RESOURCE_CEILING_GB) in e for e in result.errors)

    def test_ceiling_message_is_clear(self):
        params = _default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=20)
        result = renderer.validate(params)
        assert not result.ok
        msg = "; ".join(result.errors)
        assert str(config.RESOURCE_CEILING_GB) in msg  # names the actual ceiling, not a vague "too big"
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

    def test_max_in_range_combo_is_now_reachable_and_rejected(self):
        """The specific regression issue #6 reports: the UI's own maximum
        in-range configuration (worker_count=5, worker_cores=4,
        worker_memory_gb=8) must actually be rejectable through legitimate
        use, not just theoretically over some unreachable ceiling."""
        params = _default_params(worker_count=5, worker_cores=4, worker_memory_gb=8)
        result = renderer.validate(params)
        assert result.total_gb == 43  # 1 + 5*8 + 2
        assert result.total_gb > config.RESOURCE_CEILING_GB
        assert not result.ok

    def test_named_supported_single_worker_scale_up_demo_still_passes(self):
        """docs/requirements/spark-playbook-mvp.md's resource budget explicitly
        calls out 'a single worker may be scaled up to 8GB, for skew/spill
        demos' as a *supported* configuration. Phase 1's template applies
        worker_memory_gb uniformly (no per-worker override yet), so the
        closest reachable equivalent is the default worker count at 8GB each
        -- this must still pass under the new, lower ceiling."""
        params = _default_params(worker_count=3, worker_memory_gb=8)
        result = renderer.validate(params)
        assert result.total_gb == 27  # 1 + 3*8 + 2
        assert result.ok


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


class TestIncludeKafka:
    """docs/architecture/kafka-streaming-infra.md D1/resource-ceiling
    accounting: `include_kafka` defaults false (R-K1 -- a non-streaming
    spawn must not stand up Kafka or shift the ceiling math), adds +2GB to
    validate()'s total when set, and renders exactly one `kafka:` service
    block with the fixed container/hostname (D2/D3) when set."""

    def test_default_false_leaves_ceiling_total_unaffected(self):
        result = renderer.validate(_default_params())
        assert result.ok
        assert result.total_gb == 1 + 3 * 4 + 2  # master + 3*4GB workers + 2GB driver, no Kafka

    def test_include_kafka_true_adds_kafka_memory_gb_to_total(self):
        without = renderer.validate(_default_params(include_kafka=False))
        with_kafka = renderer.validate(_default_params(include_kafka=True))
        assert with_kafka.total_gb == without.total_gb + config.KAFKA_MEMORY_GB

    def test_include_kafka_true_can_push_total_over_the_ceiling(self):
        # 1 + 5*8 + 8 = 49, already over the 32GB ceiling without Kafka;
        # confirms include_kafka doesn't get skipped/ignored once already
        # over budget, and the +2GB is still reflected in total_gb.
        params = _default_params(worker_count=5, worker_memory_gb=8, driver_memory_gb=8, include_kafka=True)
        result = renderer.validate(params)
        assert result.total_gb == 1 + 5 * 8 + 8 + config.KAFKA_MEMORY_GB
        assert result.total_gb > config.RESOURCE_CEILING_GB
        assert not result.ok

    def test_include_kafka_true_tips_an_otherwise_in_budget_config_over_the_ceiling(self):
        # Exactly at the ceiling without Kafka (see
        # TestResourceCeilingBoundary.test_exactly_at_ceiling_passes: 1 +
        # 3*8 + 7 = 32) -- adding Kafka's +2GB must now correctly reject it,
        # the case the ADR's resource-ceiling accounting section exists for.
        params = _default_params(worker_count=3, worker_memory_gb=8, driver_memory_gb=7, include_kafka=True)
        result = renderer.validate(params)
        assert result.total_gb == config.RESOURCE_CEILING_GB + config.KAFKA_MEMORY_GB
        assert not result.ok

    def test_include_kafka_false_renders_no_kafka_service(self, tmp_path):
        """R-K1: the single most important regression to pin -- a
        non-streaming spawn (include_kafka defaults false) must render no
        `kafka` service block at all."""
        with patch.object(config, "RENDERED_DIR", tmp_path):
            with patch.object(config, "COMPOSE_FILE", tmp_path / "docker-compose.yml"):
                renderer.render(_default_params(include_kafka=False))
                rendered = (tmp_path / "docker-compose.yml").read_text(encoding="utf-8")
        assert "kafka:" not in rendered
        assert "spark-kafka" not in rendered

    def test_include_kafka_true_renders_exactly_one_kafka_service(self, tmp_path):
        with patch.object(config, "RENDERED_DIR", tmp_path):
            with patch.object(config, "COMPOSE_FILE", tmp_path / "docker-compose.yml"):
                renderer.render(_default_params(include_kafka=True))
                rendered = (tmp_path / "docker-compose.yml").read_text(encoding="utf-8")

        parsed = yaml.safe_load(rendered)
        assert "kafka" in parsed["services"]
        kafka_service = parsed["services"]["kafka"]
        assert kafka_service["container_name"] == "spark-kafka"
        assert kafka_service["hostname"] == "kafka"
        # Exactly one kafka service block -- not duplicated.
        assert rendered.count("container_name: spark-kafka") == 1
