"""Tests for app/monitoring/eta.py (US-5.3)."""
from __future__ import annotations

from app.monitoring import eta


class TestEstimate:
    def test_zero_completed_tasks_is_estimating_not_a_number(self):
        result = eta.estimate([], remaining_task_count=10)
        assert result.estimating is True
        assert result.eta_label == "estimating..."
        assert result.spread_label is None

    def test_computes_avg_times_remaining(self):
        result = eta.estimate([10.0, 20.0, 30.0], remaining_task_count=4)
        # avg = 20s, remaining = 4 -> eta = 80s = 1m 20s
        assert result.estimating is False
        assert result.eta_label == "~1m 20s"

    def test_always_includes_spread_when_not_estimating(self):
        result = eta.estimate([4.0, 9.0, 41.0], remaining_task_count=2)
        assert result.spread_label is not None
        assert "min" in result.spread_label
        assert "median" in result.spread_label
        assert "max" in result.spread_label

    def test_zero_remaining_tasks_gives_zero_eta(self):
        result = eta.estimate([5.0, 5.0], remaining_task_count=0)
        assert result.eta_label == "~0s"


class TestFormatSeconds:
    def test_sub_minute(self):
        assert eta.format_seconds(42) == "42s"

    def test_over_a_minute(self):
        assert eta.format_seconds(160) == "2m 40s"

    def test_negative_clamped_to_zero(self):
        assert eta.format_seconds(-5) == "0s"
