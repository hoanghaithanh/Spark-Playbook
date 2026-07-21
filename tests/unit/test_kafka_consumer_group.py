"""Tests for tools/kafka_consumer_group/member.py (US-KC4, issue #65) -- the
pure/argparse-level logic that doesn't need a live broker: input validation
at the boundary (mirrors test_kafka_producer.py's coverage of produce.py),
the assignment-listener's print format, and `_consume_loop`'s
poll/process/commit accounting against a fake `KafkaConsumer`.

member.py imports `kafka.KafkaConsumer`/`KafkaProducer`/etc. at module import
time and exits if missing -- same reasoning as test_kafka_producer.py, a
lightweight stub is injected into sys.modules before import so this suite
never needs `pip install kafka-python` or a real broker.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

MEMBER_DIR = Path(__file__).resolve().parents[2] / "tools" / "kafka_consumer_group"
if str(MEMBER_DIR) not in sys.path:
    sys.path.insert(0, str(MEMBER_DIR))

if "kafka" not in sys.modules:
    _stub_kafka = types.ModuleType("kafka")

    class _StubConsumerRebalanceListener:  # pragma: no cover - subclassed, never invoked directly
        pass

    class _StubKafkaConsumer:  # pragma: no cover - never actually invoked by these tests
        def __init__(self, *args, **kwargs):
            raise AssertionError("KafkaConsumer should not be constructed by these unit tests")

    class _StubKafkaProducer:  # pragma: no cover - never actually invoked by these tests
        def __init__(self, *args, **kwargs):
            raise AssertionError("KafkaProducer should not be constructed by these unit tests")

    class _StubTopicPartition:
        # _consume_loop's manual-commit branch (issue #76) really constructs
        # one of these per commit -- unlike KafkaConsumer/KafkaProducer, this
        # needs to be a working value type (with equality/hash) so tests can
        # assert on the dict of offsets passed to commit(), not a stub that
        # only proves "never invoked".
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

        def __eq__(self, other):
            return isinstance(other, _StubTopicPartition) and (self.topic, self.partition) == (
                other.topic, other.partition,
            )

        def __hash__(self):
            return hash((self.topic, self.partition))

        def __repr__(self):
            return f"TopicPartition(topic={self.topic!r}, partition={self.partition!r})"

    class _StubOffsetAndMetadata:
        # Same reasoning as _StubTopicPartition above -- issue #76's fix
        # constructs a real OffsetAndMetadata(offset, metadata) per commit.
        def __init__(self, offset, metadata):
            self.offset = offset
            self.metadata = metadata

        def __eq__(self, other):
            return isinstance(other, _StubOffsetAndMetadata) and (self.offset, self.metadata) == (
                other.offset, other.metadata,
            )

        def __hash__(self):
            return hash((self.offset, self.metadata))

        def __repr__(self):
            return f"OffsetAndMetadata(offset={self.offset!r}, metadata={self.metadata!r})"

    _stub_kafka.ConsumerRebalanceListener = _StubConsumerRebalanceListener
    _stub_kafka.KafkaConsumer = _StubKafkaConsumer
    _stub_kafka.KafkaProducer = _StubKafkaProducer
    _stub_kafka.TopicPartition = _StubTopicPartition
    sys.modules["kafka"] = _stub_kafka

    _stub_kafka_structs = types.ModuleType("kafka.structs")
    _stub_kafka_structs.OffsetAndMetadata = _StubOffsetAndMetadata
    sys.modules["kafka.structs"] = _stub_kafka_structs

import member  # noqa: E402


def _args(**overrides):
    base = dict(
        bootstrap=member.DEFAULT_BOOTSTRAP,
        topic=member.DEFAULT_TOPIC,
        group="test-group",
        label="test-member",
        commit_mode=member.DEFAULT_COMMIT_MODE,
        auto_commit_interval_ms=member.DEFAULT_AUTO_COMMIT_INTERVAL_MS,
        process_delay=member.DEFAULT_PROCESS_DELAY,
        batch_size=member.DEFAULT_BATCH_SIZE,
        max_messages=None,
        self_check=False,
    )
    base.update(overrides)
    return member.argparse.Namespace(**base)


class TestValidateArgsRejectsBadInput:
    def test_empty_topic_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(topic=""))
        assert "--topic" in capsys.readouterr().err

    def test_whitespace_only_topic_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(topic="   "))
        assert "--topic" in capsys.readouterr().err

    def test_empty_group_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(group=""))
        assert "--group" in capsys.readouterr().err

    def test_missing_group_is_accepted_when_self_check(self):
        # --self-check builds its own throwaway group internally -- --group
        # is only required for a real run().
        member._validate_args(_args(group=None, self_check=True))  # should not raise

    def test_empty_label_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(label=""))
        assert "--label" in capsys.readouterr().err

    def test_negative_process_delay_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(process_delay=-0.1))
        assert "--process-delay" in capsys.readouterr().err

    def test_zero_process_delay_is_accepted(self):
        member._validate_args(_args(process_delay=0.0))  # no work simulated -- should not raise

    def test_zero_batch_size_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(batch_size=0))
        assert "--batch-size" in capsys.readouterr().err

    def test_negative_batch_size_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(batch_size=-1))
        assert "--batch-size" in capsys.readouterr().err

    def test_zero_auto_commit_interval_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(auto_commit_interval_ms=0))
        assert "--auto-commit-interval-ms" in capsys.readouterr().err

    def test_zero_max_messages_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(max_messages=0))
        assert "--max-messages" in capsys.readouterr().err

    def test_negative_max_messages_rejected(self, capsys):
        with pytest.raises(SystemExit):
            member._validate_args(_args(max_messages=-5))
        assert "--max-messages" in capsys.readouterr().err

    def test_max_messages_none_is_accepted(self):
        member._validate_args(_args(max_messages=None))  # unbounded run -- should not raise

    def test_valid_defaults_are_accepted(self):
        member._validate_args(_args())  # should not raise/exit


class TestAssignmentPrinter:
    def test_assigned_prints_sorted_partition_numbers_with_label(self, capsys):
        class _FakeTP:
            def __init__(self, partition):
                self.partition = partition

        listener = member._AssignmentPrinter("m1")
        listener.on_partitions_assigned([_FakeTP(2), _FakeTP(0), _FakeTP(1)])
        out = capsys.readouterr().out
        assert "[m1] ASSIGNED [0, 1, 2]" in out

    def test_revoked_prints_sorted_partition_numbers_with_label(self, capsys):
        class _FakeTP:
            def __init__(self, partition):
                self.partition = partition

        listener = member._AssignmentPrinter("m2")
        listener.on_partitions_revoked([_FakeTP(1)])
        out = capsys.readouterr().out
        assert "[m2] REVOKED [1]" in out


class _FakeRecord:
    def __init__(self, key, offset, partition, topic="t"):
        self.key = key
        self.offset = offset
        self.partition = partition
        self.topic = topic


class _FakeConsumer:
    """Stands in for KafkaConsumer.poll()/commit(): returns pre-baked batches
    off a queue, one per poll() call, and records commit()/poll(0) calls so
    tests can assert on commit timing/count."""

    def __init__(self, batches):
        self._batches = list(batches)
        self.commit_calls = 0
        self.commit_offsets_calls = []  # each `offsets` dict passed to commit()
        self.zero_timeout_poll_calls = 0

    def poll(self, timeout_ms=0, max_records=None):
        if timeout_ms == 0:
            self.zero_timeout_poll_calls += 1
            return {}
        if self._batches:
            return self._batches.pop(0)
        return {}

    def commit(self, offsets=None):
        self.commit_calls += 1
        self.commit_offsets_calls.append(offsets)


class TestConsumeLoop:
    def test_manual_mode_commits_after_every_message(self):
        records = [_FakeRecord(key=b"k", offset=i, partition=0) for i in range(3)]
        consumer = _FakeConsumer([{("t", 0): records}])

        processed = member._consume_loop(
            consumer, "m", commit_mode="manual", process_delay=0.0, batch_size=10, max_messages=3
        )

        assert processed == 3
        assert consumer.commit_calls == 3

    def test_manual_mode_commits_explicit_offset_plus_one_not_bare_commit(self):
        # Regression guard for issue #76: a bare consumer.commit() defaults to
        # kafka-python's ambient fetch position (already advanced for the
        # whole last poll() batch), not "offset of the record just finished".
        # Each commit() call must instead carry an explicit
        # {TopicPartition: OffsetAndMetadata(record.offset + 1, None)} so the
        # committed offset only ever reflects completed work, regardless of
        # how many records one poll() already fetched.
        records = [_FakeRecord(key=b"k", offset=5, partition=2), _FakeRecord(key=b"k", offset=6, partition=2)]
        consumer = _FakeConsumer([{("t", 2): records}])

        member._consume_loop(
            consumer, "m", commit_mode="manual", process_delay=0.0, batch_size=10, max_messages=2
        )

        assert consumer.commit_calls == 2
        expected_tp_5 = member.TopicPartition("t", 2)
        expected_tp_6 = member.TopicPartition("t", 2)
        assert consumer.commit_offsets_calls[0] == {expected_tp_5: member.OffsetAndMetadata(6, None)}
        assert consumer.commit_offsets_calls[1] == {expected_tp_6: member.OffsetAndMetadata(7, None)}

    def test_auto_mode_never_calls_commit_but_ticks_the_coordinator(self):
        records = [_FakeRecord(key=b"k", offset=i, partition=0) for i in range(3)]
        consumer = _FakeConsumer([{("t", 0): records}])

        processed = member._consume_loop(
            consumer, "m", commit_mode="auto", process_delay=0.0, batch_size=10, max_messages=3
        )

        assert processed == 3
        assert consumer.commit_calls == 0
        # One zero-timeout "tick" poll() per processed message -- the
        # mechanism that lets kafka-python's auto-commit deadline check run
        # even mid-batch (module docstring's whole point).
        assert consumer.zero_timeout_poll_calls == 3

    def test_stops_exactly_at_max_messages_across_multiple_batches(self):
        batch1 = {("t", 0): [_FakeRecord(key=b"k", offset=0, partition=0)]}
        batch2 = {("t", 0): [_FakeRecord(key=b"k", offset=1, partition=0),
                              _FakeRecord(key=b"k", offset=2, partition=0)]}
        consumer = _FakeConsumer([batch1, batch2])

        processed = member._consume_loop(
            consumer, "m", commit_mode="manual", process_delay=0.0, batch_size=10, max_messages=2
        )

        assert processed == 2  # stops mid-second-batch, does not drain the 3rd record

    def test_prints_one_processed_line_per_message_with_offset(self, capsys):
        records = [_FakeRecord(key=b"crash-demo-key", offset=5, partition=1)]
        consumer = _FakeConsumer([{("t", 1): records}])

        member._consume_loop(consumer, "m1", commit_mode="manual", process_delay=0.0, batch_size=10, max_messages=1)

        out = capsys.readouterr().out
        assert "[m1] PROCESSED" in out
        assert "offset=5" in out
        assert "partition=1" in out

    def test_keyboard_interrupt_mid_poll_returns_partial_count_without_raising(self):
        # SIGTERM (via _raise_keyboard_interrupt) or Ctrl-C lands here as a
        # KeyboardInterrupt raised out of poll() -- this is the graceful-stop
        # path run()'s `finally: consumer.close()` depends on: _consume_loop
        # must swallow it and return whatever was processed so far, not crash
        # or lose the count.
        class _InterruptingConsumer(_FakeConsumer):
            def poll(self, timeout_ms=0, max_records=None):
                if timeout_ms == 0:
                    self.zero_timeout_poll_calls += 1
                    return {}
                if self._batches:
                    return self._batches.pop(0)
                raise KeyboardInterrupt()

        records = [_FakeRecord(key=b"k", offset=0, partition=0)]
        consumer = _InterruptingConsumer([{("t", 0): records}])

        processed = member._consume_loop(
            consumer, "m", commit_mode="manual", process_delay=0.0, batch_size=10, max_messages=None
        )

        assert processed == 1  # one message got processed+committed before the interrupt
        assert consumer.commit_calls == 1


    def test_auto_mode_tick_queues_rather_than_drops_extra_records(self):
        # Regression guard for issue #74 (Section 6 partial-loss retiming):
        # with --batch-size smaller than what a single poll() call already
        # fetched, the auto-commit "tick" poll() legitimately returns more
        # already-buffered records -- these must be processed (queued), not
        # asserted away as if they were unexpected concurrent production.
        class _TickReturnsMoreConsumer(_FakeConsumer):
            def poll(self, timeout_ms=0, max_records=None):
                if timeout_ms == 0:
                    self.zero_timeout_poll_calls += 1
                    if self.zero_timeout_poll_calls == 1:
                        # Simulates the rest of an already-fetched broker
                        # response surfacing on the first tick.
                        return {("t", 0): [_FakeRecord(key=b"k", offset=1, partition=0)]}
                    return {}
                if self._batches:
                    return self._batches.pop(0)
                return {}

        consumer = _TickReturnsMoreConsumer([{("t", 0): [_FakeRecord(key=b"k", offset=0, partition=0)]}])

        processed = member._consume_loop(
            consumer, "m", commit_mode="auto", process_delay=0.0, batch_size=1, max_messages=2
        )

        assert processed == 2  # both offset 0 (main batch) and offset 1 (tick) got processed, not dropped


class TestAdvancedPartition:
    class _FakeTP:
        def __init__(self, partition):
            self.partition = partition

    class _FakeAssignmentConsumer:
        def __init__(self, positions):
            self._positions = positions  # {partition: position}

        def assignment(self):
            return [TestAdvancedPartition._FakeTP(p) for p in self._positions]

        def position(self, tp):
            return self._positions[tp.partition]

    def test_returns_partition_whose_position_advanced(self):
        # Regression guard for issue #75: the fixed self-check key can hash
        # to any partition depending on the topic's partition count -- here
        # it lands on partition 1, not 0, and self_check() must find it
        # dynamically instead of hardcoding TopicPartition(topic, 0).
        consumer = self._FakeAssignmentConsumer({0: 0, 1: 10, 2: 0})
        assert member._advanced_partition(consumer) == 1

    def test_returns_none_when_nothing_advanced(self):
        consumer = self._FakeAssignmentConsumer({0: 0, 1: 0})
        assert member._advanced_partition(consumer) is None


class TestMakeConsumer:
    def test_sets_auto_offset_reset_to_earliest(self, monkeypatch):
        # Regression guard for issue #73: every group this module creates is
        # brand-new (no prior committed offset), so without this kwarg
        # kafka-python's own default ("latest") makes a fresh consumer seek
        # to the log end and never see any pre-produced backlog.
        captured_kwargs = {}

        def _fake_kafka_consumer(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return object()

        monkeypatch.setattr(member, "KafkaConsumer", _fake_kafka_consumer)

        member._make_consumer("kafka-1:9092", "test-group", "manual", 5000)

        assert captured_kwargs.get("auto_offset_reset") == "earliest"
