"""Tests for tools/kafka_producer/produce.py (docs/architecture/
kafka-streaming-infra.md D5) -- the pure/argparse-level logic that doesn't
need a live broker: input validation at the boundary (G4's datagen
precedent), the event schema shape (key/event_time/value + the late-frac
back-dating knob), and the partitions-uncontrollable warning.

produce.py imports `kafka.KafkaProducer` at module import time and exits if
it's missing (the package is only baked into the driver image / required on
a host run per the module docstring, not part of this repo's own app/
requirements*.txt) -- a lightweight stub is injected into sys.modules before
import so this suite doesn't need `pip install kafka-python` or a real
broker for logic that never actually constructs a producer.
"""
from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

PRODUCER_DIR = Path(__file__).resolve().parents[2] / "tools" / "kafka_producer"
if str(PRODUCER_DIR) not in sys.path:
    sys.path.insert(0, str(PRODUCER_DIR))

if "kafka" not in sys.modules:
    _stub_kafka = types.ModuleType("kafka")

    class _StubKafkaProducer:  # pragma: no cover - never actually invoked by these tests
        def __init__(self, *args, **kwargs):
            raise AssertionError("KafkaProducer should not be constructed by these unit tests")

    _stub_kafka.KafkaProducer = _StubKafkaProducer
    sys.modules["kafka"] = _stub_kafka

import produce  # noqa: E402


def _args(**overrides):
    base = dict(
        bootstrap=produce.DEFAULT_BOOTSTRAP,
        topic=produce.DEFAULT_TOPIC,
        rate=produce.DEFAULT_RATE,
        partitions=produce.DEFAULT_PARTITIONS,
        key_space=produce.DEFAULT_KEY_SPACE,
        late_frac=produce.DEFAULT_LATE_FRAC,
        late_seconds=produce.DEFAULT_LATE_SECONDS,
        count=None,
        self_check=False,
    )
    base.update(overrides)
    return produce.argparse.Namespace(**base)


class TestValidateArgsRejectsBadInput:
    def test_empty_topic_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(topic=""))
        assert "--topic" in capsys.readouterr().err

    def test_whitespace_only_topic_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(topic="   "))
        assert "--topic" in capsys.readouterr().err

    def test_zero_rate_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(rate=0))
        assert "--rate" in capsys.readouterr().err

    def test_negative_rate_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(rate=-5))
        assert "--rate" in capsys.readouterr().err

    def test_zero_partitions_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(partitions=0))
        assert "--partitions" in capsys.readouterr().err

    def test_negative_partitions_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(partitions=-1))
        assert "--partitions" in capsys.readouterr().err

    def test_late_frac_below_zero_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(late_frac=-0.1))
        assert "--late-frac" in capsys.readouterr().err

    def test_late_frac_above_one_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(late_frac=1.1))
        assert "--late-frac" in capsys.readouterr().err

    def test_late_frac_boundaries_0_and_1_are_accepted(self):
        produce._validate_args(_args(late_frac=0.0))
        produce._validate_args(_args(late_frac=1.0))

    def test_zero_key_space_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(key_space=0))
        assert "--key-space" in capsys.readouterr().err

    def test_negative_key_space_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(key_space=-3))
        assert "--key-space" in capsys.readouterr().err

    def test_zero_count_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(count=0))
        assert "--count" in capsys.readouterr().err

    def test_negative_count_rejected(self, capsys):
        with pytest.raises(SystemExit):
            produce._validate_args(_args(count=-10))
        assert "--count" in capsys.readouterr().err

    def test_count_none_is_accepted(self):
        produce._validate_args(_args(count=None))  # unbounded run -- should not raise

    def test_valid_defaults_are_accepted(self):
        produce._validate_args(_args())  # should not raise/exit


class TestMakeEvent:
    def test_schema_has_key_event_time_and_value(self):
        event = produce._make_event(key_space=8, late_frac=0.0, late_seconds=60)
        assert set(event.keys()) == {"key", "event_time", "value"}
        assert event["key"].startswith("key-")
        assert isinstance(event["value"], float)
        # event_time must be a parseable ISO-8601 UTC timestamp.
        datetime.fromisoformat(event["event_time"])

    def test_key_is_within_the_requested_key_space(self):
        for _ in range(50):
            event = produce._make_event(key_space=3, late_frac=0.0, late_seconds=60)
            n = int(event["key"].removeprefix("key-"))
            assert 0 <= n < 3

    def test_late_frac_1_always_back_dates(self):
        # random.random() is always < 1.0, so late_frac=1.0 deterministically
        # takes the back-dated branch every call -- no need to mock random.
        before = datetime.now(timezone.utc)
        event = produce._make_event(key_space=8, late_frac=1.0, late_seconds=3600)
        event_time = datetime.fromisoformat(event["event_time"])
        assert event_time < before  # back-dated by ~3600s, well before "now"

    def test_late_frac_0_never_back_dates(self):
        before = datetime.now(timezone.utc)
        event = produce._make_event(key_space=8, late_frac=0.0, late_seconds=3600)
        after = datetime.now(timezone.utc)
        event_time = datetime.fromisoformat(event["event_time"])
        assert before <= event_time <= after


class TestWarnIfPartitionsUncontrollable:
    def test_warns_when_partitions_differ_from_default(self, capsys):
        produce._warn_if_partitions_uncontrollable(_args(partitions=produce.DEFAULT_PARTITIONS + 1))
        captured = capsys.readouterr()
        assert "WARNING" in captured.err
        assert "--partitions" in captured.err

    def test_no_warning_when_partitions_match_default(self, capsys):
        produce._warn_if_partitions_uncontrollable(_args(partitions=produce.DEFAULT_PARTITIONS))
        captured = capsys.readouterr()
        assert captured.err == ""
