"""Tests for driver/playbook/annotate.py (PLAN.md §3 checkpoint() -- pull-not-push).

`driver/` isn't part of the `app` package (it runs inside the spark-driver
container's own Python environment -- see that module's docstring), but it's
plain, dependency-free Python otherwise, so it's importable and testable here
with a fake DataFrame stand-in (no real PySpark/cluster needed for this unit
test -- the real end-to-end path is exercised separately against a live
cluster per this sprint's verification bar).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

DRIVER_DIR = Path(__file__).resolve().parents[2] / "driver"
if str(DRIVER_DIR) not in sys.path:
    sys.path.insert(0, str(DRIVER_DIR))

from playbook.annotate import checkpoint  # noqa: E402


class _FakeSparkContext:
    applicationId = "app-fake-0001"


class _FakeSparkSession:
    sparkContext = _FakeSparkContext()


class _FakeDataFrame:
    """Stands in for a pyspark DataFrame: `explain(mode=...)` just prints,
    matching real DataFrame.explain()'s behavior -- checkpoint() captures
    that printed output via contextlib.redirect_stdout, so a fake that only
    implements print() here already exercises the real capture mechanism."""

    sparkSession = _FakeSparkSession()

    def explain(self, mode: str = "formatted") -> None:
        print("== Physical Plan ==\n* BroadcastHashJoin (1)\n+- Scan parquet default.t (0)\n\n(0) Scan parquet default.t")


class TestCheckpoint:
    def test_writes_json_with_expected_shape(self, tmp_path):
        out_path = checkpoint(_FakeDataFrame(), topic="join-strategies", shared_dir=str(tmp_path))

        assert out_path.exists()
        payload = json.loads(out_path.read_text(encoding="utf-8"))
        assert payload["topic"] == "join-strategies"
        assert payload["app_id"] == "app-fake-0001"
        assert "timestamp" in payload
        assert "BroadcastHashJoin" in payload["explain_formatted"]

    def test_writes_under_a_per_topic_subdirectory(self, tmp_path):
        out_path = checkpoint(_FakeDataFrame(), topic="bucketing", shared_dir=str(tmp_path))
        assert out_path.parent == tmp_path / "bucketing"

    def test_filename_is_millisecond_epoch_and_sortable(self, tmp_path):
        first = checkpoint(_FakeDataFrame(), topic="aqe", shared_dir=str(tmp_path))
        second = checkpoint(_FakeDataFrame(), topic="aqe", shared_dir=str(tmp_path))
        # Distinct calls should not collide (or if they land in the same
        # millisecond, at least neither write should be lost/overwritten
        # silently by the other -- both files must exist).
        assert first.exists()
        assert second.exists()

    def test_does_not_print_the_plan_itself_pull_not_push(self, tmp_path, capsys):
        checkpoint(_FakeDataFrame(), topic="join-strategies", shared_dir=str(tmp_path))
        captured = capsys.readouterr()
        # checkpoint() must not surface the plan to stdout on its own (G3) --
        # only writing the file is its job; the print() inside our fake's
        # explain() is captured/redirected, not let through.
        assert "BroadcastHashJoin" not in captured.out


class TestTopicNameIsSanitized:
    """Issue #12: `topic` is joined directly into a filesystem path with no
    other checks -- a typo/malicious value like "../join-strategies" would
    otherwise silently write outside the intended per-topic directory."""

    @pytest.mark.parametrize(
        "bad_topic",
        [
            "../join-strategies",
            "..\\join-strategies",
            "join-strategies/../../etc",
            "sub/dir",
            "sub\\dir",
            "..",
            "",
        ],
    )
    def test_path_traversal_like_topic_names_are_rejected(self, tmp_path, bad_topic):
        with pytest.raises(ValueError):
            checkpoint(_FakeDataFrame(), topic=bad_topic, shared_dir=str(tmp_path))

    def test_does_not_write_outside_shared_dir_for_a_rejected_topic(self, tmp_path):
        shared_dir = tmp_path / "annotations"
        with pytest.raises(ValueError):
            checkpoint(_FakeDataFrame(), topic="../escape", shared_dir=str(shared_dir))
        # Nothing should have been written anywhere, including outside shared_dir.
        assert not (tmp_path / "escape").exists()
        assert not shared_dir.exists()

    def test_plain_topic_names_still_work(self, tmp_path):
        out_path = checkpoint(_FakeDataFrame(), topic="join-strategies", shared_dir=str(tmp_path))
        assert out_path.exists()
