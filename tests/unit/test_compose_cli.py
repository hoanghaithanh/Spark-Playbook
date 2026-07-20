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
        kafka_broker_count=3,
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
    (TestIncludeKafka in tests/unit/test_renderer.py) -- same
    KAFKA_MEMORY_GB * kafka_broker_count formula (docs/architecture/
    multi-broker-kafka-cluster.md D-MBK4, supersedes the flat +2GB), same
    32GB ceiling (the 48GB->32GB drift fix OQ-MBK4 flagged), kept
    independently in sync per the ADR's Consequences note that the CLI
    mirror is 'the easy one to forget'."""

    def test_include_kafka_false_does_not_raise_for_default_config(self):
        compose_cli._validate_ranges(_args(include_kafka=False))  # should not raise/exit

    def test_include_kafka_true_does_not_raise_when_still_under_the_32gb_ceiling(self):
        # 1 + 3*4 + 2 + 2*3(kafka, 3 brokers) = 21GB, under the CLI's 32GB ceiling.
        compose_cli._validate_ranges(_args(include_kafka=True, kafka_broker_count=3))  # should not raise/exit

    def test_include_kafka_true_pushes_an_otherwise_in_budget_config_over_the_cli_ceiling(self, capsys):
        # Without Kafka: 1 + 3*4 + 8 = 21GB, under 32GB.
        # +2*3 (kafka, 3 brokers) = 27GB -> still under the ceiling, passes.
        compose_cli._validate_ranges(
            _args(worker_count=3, worker_memory_gb=4, driver_memory_gb=8, include_kafka=True, kafka_broker_count=3)
        )

        # 1 + 5*4 + 8 = 29GB without Kafka (under ceiling); +2*2 (kafka, 2
        # brokers) = 33GB -> now rejected.
        with pytest.raises(SystemExit):
            compose_cli._validate_ranges(
                _args(worker_count=5, worker_memory_gb=4, driver_memory_gb=8, include_kafka=True, kafka_broker_count=2)
            )
        captured = capsys.readouterr()
        assert "33GB" in captured.err


class TestValidateRangesKafkaBrokerCountRange:
    """docs/architecture/multi-broker-kafka-cluster.md D-MBK4: CLI mirror of
    renderer.validate()'s kafka_broker_count range check (1-5) when
    include_kafka is set."""

    def test_broker_count_too_low_rejected(self):
        with pytest.raises(SystemExit):
            compose_cli._validate_ranges(_args(include_kafka=True, kafka_broker_count=0))

    def test_broker_count_too_high_rejected(self):
        with pytest.raises(SystemExit):
            compose_cli._validate_ranges(_args(include_kafka=True, kafka_broker_count=6))

    def test_out_of_range_broker_count_ignored_when_kafka_excluded(self):
        compose_cli._validate_ranges(_args(include_kafka=False, kafka_broker_count=99))  # should not raise/exit

    def test_boundary_values_pass(self):
        compose_cli._validate_ranges(_args(include_kafka=True, kafka_broker_count=1))
        compose_cli._validate_ranges(_args(include_kafka=True, kafka_broker_count=5))
