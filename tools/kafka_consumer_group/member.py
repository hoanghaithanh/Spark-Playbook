#!/usr/bin/env python3
"""Spark Playbook — one consumer-group member (US-KC4, issue #65).

Standalone CLI, one OS process per group member -- mirrors
`tools/kafka_producer/produce.py`'s conventions exactly (argparse CLI, clear
stderr validation errors, no `app/` import) and the same "launch as its own
process, don't call it from the current kernel" idiom `driver/playbook/
producer.py` already documents for D5. That idiom matters even more here:
US-KC4's crash/restart acceptance criterion needs a *real* OS-level kill
(`proc.kill()`, SIGKILL, uncatchable) so the group coordinator's background
heartbeat thread genuinely dies with the process -- an in-kernel thread
can't be killed that cleanly (its heartbeat thread would keep the group
membership alive right through a simulated "crash", defeating the whole
point of the demo). Scaling a consumer group up/down for the rebalance
acceptance criteria has the same requirement: each member must be a
genuinely independent client the broker can see joining/leaving.

Each member:
  - `subscribe()`s (not `assign()`) to participate in real consumer-group
    rebalancing, with a `ConsumerRebalanceListener` that prints every
    assignment change -- e.g. `[m2] ASSIGNED [1]` -- so the notebook can
    read a spawned process's stdout instead of re-deriving assignment state
    itself.
  - Processes messages one at a time (`--process-delay` simulates work),
    printing one `PROCESSED ...` line per message *after* that message's
    simulated work finishes -- the notebook's crash demo watches for these
    lines and SIGKILLs the process after a chosen count, to kill it
    deterministically mid-batch rather than racing a wall-clock sleep.
  - Commits per message in `--commit-mode manual` (`enable_auto_commit=False`,
    explicit `consumer.commit()` right after each message's work finishes --
    never before, so a commit always means "this message's work is done").
  - In `--commit-mode auto` (`enable_auto_commit=True`), relies on
    kafka-python's own background auto-commit, which commits
    `consumer.position()` -- and `position()` already advances the moment
    `poll()` hands records to the application, *before* this script's
    per-message work loop runs. That gap is the whole point of the auto vs.
    manual contrast (concept.md): an auto-commit that fires after `poll()`
    returns a batch but before the app finishes acting on it can commit past
    messages the app never actually finished -- a crash right there loses
    them silently (they're never redelivered, since the offset already moved
    past them). Manual commit can't do this: the offset only ever advances
    to a message whose work already completed.

Runs until SIGINT/SIGTERM (graceful: commits/closes normally) or SIGKILL
(the notebook's deliberate crash simulation: no commit, no clean group
leave -- exactly a real crash), or for `--max-messages` messages then exits
cleanly (used by `self_check()` and the notebook's bounded demo runs).
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
import uuid

try:
    from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndMetadata
except ImportError:  # pragma: no cover
    print(
        "ERROR: the 'kafka-python' package is required. Install it with:\n"
        "    pip install kafka-python\n"
        "(already baked into the spark-driver image, same as tools/kafka_producer/produce.py).",
        file=sys.stderr,
    )
    sys.exit(1)

DEFAULT_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092"  # in-cluster DNS, 3-broker Kafka-track shape
DEFAULT_TOPIC = "consumer-groups-demo"
DEFAULT_COMMIT_MODE = "manual"
DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000
DEFAULT_PROCESS_DELAY = 0.05
DEFAULT_BATCH_SIZE = 10


def _raise_keyboard_interrupt(signum, frame) -> None:
    """SIGTERM doesn't raise KeyboardInterrupt by default -- route it through
    the same clean-shutdown path as Ctrl-C (same fix as produce.py's
    identically-named handler). SIGKILL (the crash simulation) can't be
    caught at all, by design -- that's exactly the point."""
    raise KeyboardInterrupt()


def _validate_args(args: argparse.Namespace) -> None:
    """Input validation at the boundary (same discipline as produce.py's
    `_validate_args`): reject nonsense with a clear error, not a silent
    no-op."""
    errors = []
    if not args.topic or not args.topic.strip():
        errors.append("--topic must not be empty")
    # --self-check builds its own throwaway group internally (self_check()'s
    # own docstring) -- --group is only meaningful, and only required, for a
    # real run() invocation.
    if not args.self_check and (not args.group or not args.group.strip()):
        errors.append("--group must not be empty")
    if not args.label or not args.label.strip():
        errors.append("--label must not be empty")
    if args.process_delay < 0:
        errors.append("--process-delay must not be negative")
    if args.batch_size <= 0:
        errors.append("--batch-size must be a positive integer")
    if args.auto_commit_interval_ms <= 0:
        errors.append("--auto-commit-interval-ms must be a positive integer")
    if args.max_messages is not None and args.max_messages <= 0:
        errors.append("--max-messages must be a positive integer")

    if errors:
        print("ERROR: invalid arguments:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)


class _AssignmentPrinter(ConsumerRebalanceListener):
    """Prints every partition-assignment change to stdout, prefixed with the
    member's label -- the notebook reads a spawned process's stdout for this
    line rather than needing its own thread-safe way to inspect another
    process's consumer state."""

    def __init__(self, label: str):
        self.label = label

    def on_partitions_revoked(self, revoked) -> None:
        parts = sorted(tp.partition for tp in revoked)
        print(f"[{self.label}] REVOKED {parts}", flush=True)

    def on_partitions_assigned(self, assigned) -> None:
        parts = sorted(tp.partition for tp in assigned)
        print(f"[{self.label}] ASSIGNED {parts}", flush=True)


def _make_consumer(bootstrap: str, group: str, commit_mode: str, auto_commit_interval_ms: int) -> "KafkaConsumer":
    return KafkaConsumer(
        bootstrap_servers=bootstrap.split(","),
        group_id=group,
        enable_auto_commit=(commit_mode == "auto"),
        auto_commit_interval_ms=auto_commit_interval_ms,
        # Every group this module creates is brand-new (no prior committed
        # offset) -- without this, kafka-python's default "latest" seeks
        # straight to the log end on first poll(), so a fresh consumer never
        # sees any backlog produced before it started (issue #73).
        auto_offset_reset="earliest",
        # Explicit key/value bytes -- this script processes exclusively via
        # bytes, no JSON/schema needed for the offset-commit lesson.
    )


def _consume_loop(
    consumer: "KafkaConsumer",
    label: str,
    commit_mode: str,
    process_delay: float,
    batch_size: int,
    max_messages: int | None,
) -> int:
    """Poll/process/commit loop shared by `run()` and `self_check()`.
    Returns the number of messages whose simulated work actually finished
    (i.e. a `PROCESSED` line was printed for them) -- deliberately distinct
    from how many the broker *delivered*, since a message that's mid-flight
    when this process is killed never gets counted here.

    Records are drained one at a time off a `pending` queue rather than a
    nested "one poll() call -> one batch -> process every record in it"
    loop: with `--batch-size` smaller than what kafka-python already fetched
    from the broker in a single response (issue #74's Section 6 partial-loss
    retiming needs `--batch-size` < the backlog size to make `position()`
    advance incrementally instead of jumping straight to the log end), the
    auto-commit "tick" poll() below legitimately drains more already-buffered
    records too -- this queue lets those get processed like any other
    fetched record instead of being asserted away."""
    processed = 0
    pending: list = []  # records fetched but not yet processed, in fetch order
    try:
        while max_messages is None or processed < max_messages:
            if not pending:
                batch = consumer.poll(timeout_ms=1000, max_records=batch_size)
                for _tp, records in batch.items():
                    pending.extend(records)
                if not pending:
                    continue
            record = pending.pop(0)
            time.sleep(process_delay)  # simulated work -- finishes BEFORE any commit
            processed += 1
            print(
                f"[{label}] PROCESSED key={record.key!r} offset={record.offset} "
                f"partition={record.partition} total={processed}",
                flush=True,
            )
            if commit_mode == "manual":
                # Explicit per-record offset -- NOT a bare consumer.commit(),
                # which defaults to _subscription.all_consumed_offsets() (the
                # fetch *position*, already advanced for everything the last
                # poll() handed back). That races ahead of processing exactly
                # like auto-commit does (issue #76), defeating manual
                # commit's whole point. Committing offset+1 for the record
                # just finished ties the commit to completed work only,
                # regardless of batch_size/poll sizing.
                consumer.commit({
                    TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1, None)
                })
            else:
                # kafka-python only checks its auto-commit deadline inside
                # poll() (piggybacked on `_coordinator.poll()`) -- with a
                # whole batch already fetched and no further poll() call due
                # until this batch finishes, the timer would never get a
                # chance to fire mid-batch, and the very race this script
                # exists to demonstrate couldn't happen. This zero-timeout
                # poll() exists to tick the coordinator -- exactly the same
                # "auto-commit sneaks in between poll() calls, indifferent to
                # whether the app finished the last batch" behavior that
                # bites real consumers with slow per-message processing. Any
                # records it returns are already-fetched data (this batch's
                # remainder, or -- with a small `--batch-size` -- the next
                # chunk of the same broker response) queued for processing
                # below rather than dropped: dropping a record kafka-python
                # already handed us would itself be the exact silent-loss bug
                # this module exists to demonstrate, not commit.
                tick_batch = consumer.poll(timeout_ms=0, max_records=batch_size)
                for _tp, records in tick_batch.items():
                    pending.extend(records)
    except KeyboardInterrupt:
        pass
    return processed


def run(args: argparse.Namespace) -> int:
    consumer = _make_consumer(args.bootstrap, args.group, args.commit_mode, args.auto_commit_interval_ms)
    consumer.subscribe([args.topic], listener=_AssignmentPrinter(args.label))
    print(
        f"[{args.label}] started topic={args.topic!r} group={args.group!r} "
        f"commit_mode={args.commit_mode}",
        flush=True,
    )
    try:
        processed = _consume_loop(
            consumer, args.label, args.commit_mode, args.process_delay, args.batch_size, args.max_messages
        )
    finally:
        consumer.close()
    print(f"[{args.label}] stopped, processed {processed} message(s)", flush=True)
    return 0


def _advanced_partition(consumer: "KafkaConsumer") -> int | None:
    """Returns the partition (among this consumer's current assignment)
    whose fetch position advanced past 0 -- i.e. the one that actually
    received records. `self_check()` uses this instead of assuming partition
    0 (issue #75): a fixed key's default-partitioner hash can land on any
    partition depending on the topic's partition count, and hardcoding 0
    fails deterministically whenever it lands elsewhere. Returns `None` if no
    assigned partition ever advanced (nothing was consumed)."""
    for tp in consumer.assignment():
        if consumer.position(tp) > 0:
            return tp.partition
    return None


def self_check(args: argparse.Namespace) -> int:
    """The smallest runnable check that fails if the poll/process/commit loop
    or the manual-commit offset math breaks (same 'one runnable check'
    requirement produce.py's own self_check() satisfies): publish N messages
    under one fixed key (so they all land on the same partition,
    deterministic), consume+manually-commit all N via `_consume_loop`, then
    open a fresh consumer in the same group and assert the committed offset
    on that partition equals N -- i.e. every processed message really was
    committed, nothing double-counted or skipped."""
    topic = f"selfcheck-member-{uuid.uuid4().hex[:8]}"
    group = f"selfcheck-group-{uuid.uuid4().hex[:8]}"
    n = args.max_messages or 10

    producer = KafkaProducer(bootstrap_servers=args.bootstrap.split(","))
    try:
        futures = [producer.send(topic, key=b"selfcheck-key", value=str(i).encode()) for i in range(n)]
        producer.flush()
        for future in futures:
            future.get(timeout=10)  # raises on a broker-reported failure
    finally:
        producer.close()

    consumer = _make_consumer(args.bootstrap, group, "manual", args.auto_commit_interval_ms)
    consumer.subscribe([topic], listener=_AssignmentPrinter(args.label))
    try:
        processed = _consume_loop(consumer, args.label, "manual", 0.0, args.batch_size, n)
        partition = _advanced_partition(consumer)  # find it dynamically -- issue #75
    finally:
        consumer.close()
    assert processed == n, f"self-check failed: only {processed}/{n} messages processed"
    assert partition is not None, "self-check failed: no partition's fetch position advanced -- nothing was consumed"

    check_consumer = KafkaConsumer(bootstrap_servers=args.bootstrap.split(","), group_id=group)
    committed = check_consumer.committed(TopicPartition(topic, partition))
    check_consumer.close()
    assert committed == n, f"self-check failed: committed offset {committed} != {n} messages processed"

    print(f"SELF-CHECK OK: processed {processed}/{n}, committed offset={committed} (topic={topic!r} group={group!r}).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Spark Playbook Kafka consumer-group member (US-KC4)")
    p.add_argument(
        "--bootstrap", default=DEFAULT_BOOTSTRAP,
        help=f"Comma-separated Kafka bootstrap servers (default {DEFAULT_BOOTSTRAP!r}).",
    )
    p.add_argument("--topic", default=DEFAULT_TOPIC, help=f"Topic to consume (default {DEFAULT_TOPIC!r}).")
    p.add_argument(
        "--group", default=None,
        help="Consumer group id -- shared across every member in the group. Required unless --self-check "
             "(which builds its own throwaway group).",
    )
    p.add_argument("--label", default="member", help="Short id printed on every line (default 'member').")
    p.add_argument(
        "--commit-mode", choices=("manual", "auto"), default=DEFAULT_COMMIT_MODE,
        help=f"'manual' commits once per processed message; 'auto' relies on kafka-python's "
             f"background auto-commit (default {DEFAULT_COMMIT_MODE!r}).",
    )
    p.add_argument(
        "--auto-commit-interval-ms", type=int, default=DEFAULT_AUTO_COMMIT_INTERVAL_MS,
        help=f"Only used in --commit-mode auto (default {DEFAULT_AUTO_COMMIT_INTERVAL_MS}).",
    )
    p.add_argument(
        "--process-delay", type=float, default=DEFAULT_PROCESS_DELAY,
        help=f"Seconds of simulated work per message (default {DEFAULT_PROCESS_DELAY}).",
    )
    p.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Max records per poll() call (default {DEFAULT_BATCH_SIZE}).",
    )
    p.add_argument(
        "--max-messages", type=int, default=None,
        help="Stop cleanly after processing exactly this many messages, instead of running until "
             "SIGINT/SIGTERM/SIGKILL.",
    )
    p.add_argument(
        "--self-check", action="store_true",
        help="Run the smallest end-to-end check (publish, consume, commit, verify offset) and exit.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args)
    if args.self_check:
        return self_check(args)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
