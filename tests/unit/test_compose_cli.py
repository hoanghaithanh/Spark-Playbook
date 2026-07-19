"""Tests for compose/cli.py's Kafka ADR additions (docs/architecture/
kafka-streaming-infra.md): the `--include-kafka` flag and its
`_validate_ranges` ceiling-mirror accounting, which must stay in sync with
`app/lifecycle/renderer.py::validate()`'s +2GB Kafka reservation.

compose/cli.py predates any test coverage (it's the standalone Phase 0
script, importable but not previously exercised by pytest) -- this file is
scoped to just the new Kafka-related surface, not a full retroactive suite
for the rest of the script.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

COMPOSE_DIR = Path(__file__).resolve().parents[2] / "compose"
if str(COMPOSE_DIR) not in sys.path:
    sys.path.insert(0, str(COMPOSE_DIR))

import cli as compose_cli  # noqa: E402


def _args(**overrides):
    base = dict(
        worker_count=3,
        worker_cores=2,
        worker_memory_gb=4,
        driver_memory_gb=2,
        shuffle_partitions=200,
        include_kafka=False,
    )
    base.update(overrides)
    return compose_cli.argparse.Namespace(**base)


class TestIncludeKafkaFlagDefaultsFalse:
    def test_render_parser_default_is_false(self):
        parser = compose_cli.build_parser()
        args = parser.parse_args(["render"])
        assert args.include_kafka is False

    def test_flag_sets_true(self):
        parser = compose_cli.build_parser()
        args = parser.parse_args(["render", "--include-kafka"])
        assert args.include_kafka is True


class TestValidateRangesKafkaCeilingMirror:
    """Mirrors app/lifecycle/renderer.py::validate()'s ceiling accounting
    (TestIncludeKafka in tests/unit/test_renderer.py) -- same +2GB when set,
    same KAFKA_MEMORY_GB constant, kept independently in sync per the ADR's
    Consequences note that the CLI mirror is 'the easy one to forget'."""

    def test_include_kafka_false_does_not_raise_for_default_config(self):
        compose_cli._validate_ranges(_args(include_kafka=False))  # should not raise/exit

    def test_include_kafka_true_does_not_raise_when_still_under_the_48gb_ceiling(self):
        # 1 + 3*4 + 2 + 2(kafka) = 17GB, well under the CLI's 48GB sanity ceiling.
        compose_cli._validate_ranges(_args(include_kafka=True))  # should not raise/exit

    def test_include_kafka_true_pushes_an_otherwise_in_budget_config_over_the_cli_ceiling(self, capsys):
        # Without Kafka: 1 + 5*8 + 8 = 49 -> already over 48GB regardless of Kafka;
        # use a config exactly at 48 without Kafka so the +2GB is what tips it.
        # 1 + 5*8 + 5 = 46; +2 (kafka) = 48 -> still exactly at ceiling, passes.
        compose_cli._validate_ranges(_args(worker_count=5, worker_memory_gb=8, driver_memory_gb=5, include_kafka=True))

        # 1 + 5*8 + 6 = 47 without Kafka (under ceiling); +2 (kafka) = 49 -> now rejected.
        with pytest.raises(SystemExit):
            compose_cli._validate_ranges(
                _args(worker_count=5, worker_memory_gb=8, driver_memory_gb=6, include_kafka=True)
            )
        captured = capsys.readouterr()
        assert "49GB" in captured.err
