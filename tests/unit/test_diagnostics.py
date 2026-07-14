"""Tests for app/monitoring/diagnostics.py (ADR D-A, US-5.4).

Every test in `TestNoConclusionsEverLeak` exists specifically to guard D-A --
the single most important constraint in this feature (see the ADR). If any
of these ever fail, it means a fix/suggestion/conclusion crept into a
"factual" detail string, which must never happen by design.
"""
from __future__ import annotations

from app.monitoring import diagnostics

_FORBIDDEN_WORDS = ("suggest", "consider", "salt", "recommend", "should", "fix", "try ")


def _assert_no_suggestions(text: str) -> None:
    lowered = text.lower()
    for word in _FORBIDDEN_WORDS:
        assert word not in lowered, f"found prescriptive language {word!r} in {text!r}"


def _tasks(*, sizes_by_node):
    samples = []
    i = 0
    for node, sizes in sizes_by_node.items():
        for size in sizes:
            samples.append(
                diagnostics.TaskSample(node=node, task_id=f"p-{i:03d}", size_bytes=size, duration_s=10.0)
            )
            i += 1
    return samples


class TestSkewedTaskIds:
    def test_flags_tasks_at_or_above_multiple_of_median(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 110, 100, 900]})
        skewed = diagnostics.skewed_task_ids(tasks, multiple=1.5)
        assert len(skewed) == 1
        assert tasks[3].task_id in skewed

    def test_no_skew_when_sizes_are_uniform(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 105, 98, 102]})
        assert diagnostics.skewed_task_ids(tasks, multiple=1.5) == set()

    def test_fewer_than_two_sized_tasks_returns_empty(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100]})
        assert diagnostics.skewed_task_ids(tasks) == set()


class TestNodeSkewReasons:
    def test_only_flags_nodes_holding_a_skewed_task(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 100], "worker-2": [900, 950, 100]})
        skewed = diagnostics.skewed_task_ids(tasks, multiple=1.5)
        reasons = diagnostics.node_skew_reasons(tasks, skewed)
        assert "worker-2" in reasons
        assert "worker-1" not in reasons
        assert "x avg partition size" in reasons["worker-2"]


class TestPartitionSizeSignal:
    def test_returns_none_when_nothing_skewed(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 105]})
        assert diagnostics.partition_size_signal(tasks, set()) is None

    def test_factual_detail_names_worst_node_and_ratio(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 100], "worker-2": [900, 950, 100]})
        skewed = diagnostics.skewed_task_ids(tasks, multiple=1.5)
        detail = diagnostics.partition_size_signal(tasks, skewed)
        assert detail is not None
        assert "worker-2" in detail
        assert "median" in detail


class TestNodeImbalanceReasons:
    def test_flags_saturated_node_with_an_idle_peer(self):
        samples = [
            diagnostics.NodeResourceSample(node="worker-1", cpu_pct=91, ram_pct=80, gc_ms=10),
            diagnostics.NodeResourceSample(node="worker-2", cpu_pct=40, ram_pct=30, gc_ms=5),
        ]
        reasons = diagnostics.node_imbalance_reasons(samples, warn_pct=70, crit_pct=88)
        assert "worker-1" in reasons
        assert "worker-2" not in reasons

    def test_no_flag_when_all_nodes_busy(self):
        samples = [
            diagnostics.NodeResourceSample(node="worker-1", cpu_pct=91, ram_pct=80, gc_ms=10),
            diagnostics.NodeResourceSample(node="worker-2", cpu_pct=85, ram_pct=75, gc_ms=5),
        ]
        assert diagnostics.node_imbalance_reasons(samples, warn_pct=70, crit_pct=88) == {}

    def test_single_node_never_flagged(self):
        samples = [diagnostics.NodeResourceSample(node="worker-1", cpu_pct=99, ram_pct=99, gc_ms=99)]
        assert diagnostics.node_imbalance_reasons(samples) == {}


class TestMemoryGcSignal:
    def test_none_when_all_below_warn_threshold(self):
        samples = [diagnostics.NodeResourceSample(node="worker-1", cpu_pct=50, ram_pct=50, gc_ms=5)]
        assert diagnostics.memory_gc_signal(samples) is None

    def test_reports_worst_gc_node(self):
        samples = [
            diagnostics.NodeResourceSample(node="worker-1", cpu_pct=50, ram_pct=50, gc_ms=25),
            diagnostics.NodeResourceSample(node="worker-2", cpu_pct=91, ram_pct=84, gc_ms=45),
        ]
        detail = diagnostics.memory_gc_signal(samples)
        assert "worker-2" in detail
        assert "45" in detail


class TestCriticalPathSignal:
    def test_reports_longest_stage_share(self):
        stages = [
            diagnostics.StageDuration(label="Stage 1", duration_s=22, is_current=False),
            diagnostics.StageDuration(label="Stage 3", duration_s=82, is_current=True),
        ]
        detail = diagnostics.critical_path_signal(stages)
        assert "Stage 3" in detail
        assert "%" in detail

    def test_none_when_no_duration_data(self):
        assert diagnostics.critical_path_signal([]) is None


class TestNoConclusionsEverLeak:
    """D-A: every factual string this module can produce must stay free of
    prescriptive/remedy language, across the realistic range of inputs."""

    def test_partition_size_signal_has_no_suggestion(self):
        tasks = _tasks(sizes_by_node={"worker-1": [100, 100], "worker-2": [900, 950, 100]})
        skewed = diagnostics.skewed_task_ids(tasks, multiple=1.5)
        detail = diagnostics.partition_size_signal(tasks, skewed)
        _assert_no_suggestions(detail)

    def test_node_skew_reason_has_no_suggestion(self):
        tasks = _tasks(sizes_by_node={"worker-2": [900, 950, 100]})
        skewed = diagnostics.skewed_task_ids(tasks, multiple=1.5)
        for reason in diagnostics.node_skew_reasons(tasks, skewed).values():
            _assert_no_suggestions(reason)

    def test_node_imbalance_reason_has_no_suggestion(self):
        samples = [
            diagnostics.NodeResourceSample(node="worker-1", cpu_pct=91, ram_pct=80, gc_ms=10),
            diagnostics.NodeResourceSample(node="worker-2", cpu_pct=40, ram_pct=30, gc_ms=5),
        ]
        for reason in diagnostics.node_imbalance_reasons(samples).values():
            _assert_no_suggestions(reason)

    def test_memory_gc_signal_has_no_suggestion(self):
        samples = [diagnostics.NodeResourceSample(node="worker-2", cpu_pct=91, ram_pct=84, gc_ms=45)]
        _assert_no_suggestions(diagnostics.memory_gc_signal(samples))

    def test_critical_path_signal_has_no_suggestion(self):
        stages = [diagnostics.StageDuration(label="Stage 3", duration_s=82, is_current=True)]
        _assert_no_suggestions(diagnostics.critical_path_signal(stages))
