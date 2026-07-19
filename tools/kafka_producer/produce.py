#!/usr/bin/env python3
"""Spark Playbook — synthetic Kafka producer (Phase 3, docs/architecture/
kafka-streaming-infra.md D5).

Standalone, rate-controlled CLI that publishes bounded, keyed JSON events to
a Kafka topic -- the minimal viable schema/knobs (D5/OQ-2) for a streaming
notebook (#18) to genuinely exercise watermarks and checkpoint-recovery
against (G1: real behaviour, not toy data). Mirrors compose/cli.py's
standalone-script conventions: self-contained argparse CLI, clear stderr
error messages on bad input, no `app/` import.

Runs either:
  - inside the driver container (kafka-python baked into the image, D4):
        docker exec spark-driver python /workspace/tools/kafka_producer/produce.py \\
            --topic events --rate 100
    reaching the in-cluster listener `kafka:9092` by container DNS (D3).
  - from a host shell (requires `pip install kafka-python` on the host,
    OQ-1 resolved 2026-07-19):
        python tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 --topic events
    DEVIATION FROM THE ADR: OQ-1's own example used `localhost:9092`, but
    verified live (kafka-python 2.0.2 against a real apache/kafka:3.9.0
    broker, Windows host) that "localhost" resolves IPv6 first (`::1`),
    nothing is published there (D3 publishes 127.0.0.1 only, IPv4), and the
    client never falls back in time -- `NoBrokersAvailable` every time.
    `127.0.0.1` works immediately, with the exact `kafka-python` version D4
    pins, no other change needed. Same root cause `app/config.py::CLUSTER_HOST`
    already documents and works around for the app's own server-side calls --
    host-shell producer runs need the same explicit-IP workaround.

Schema (minimal, generic -- #18 refines the lesson-specific shape, D5/OQ-2):
    {"key": "<key-N>", "event_time": "<ISO-8601 UTC>", "value": <float>}
A `--late-frac` fraction of events carry a back-dated `event_time`
(`--late-seconds` behind "now"), so #18 can demonstrate past-watermark
dropping.

Publishes until SIGINT/SIGTERM (Ctrl-C, or `driver/playbook/producer.py`'s
`stop()`), or for exactly `--count` events if given. Independent of any
Spark streaming query's own lifecycle (D5) -- it is only a Kafka client.

Partition count (--partitions): the ADR's original design pre-created the
topic via `KafkaAdminClient` so `--partitions` could set an exact count.
DEVIATION FROM THE ADR, verified live: `KafkaAdminClient.__init__`
unconditionally calls `_refresh_controller_id()`, which raises
`NodeNotReadyError` against this single-node combined broker+controller
KRaft broker (apache/kafka:3.9.0) -- reproduced with both `kafka-python`
2.0.2 (D4's pin) and the actively-maintained `kafka-python-ng` fork, so it
is a real client-library limitation with KRaft controller discovery, not a
config mistake or a stale-package issue (the same libraries' plain
`KafkaProducer` connects and produces successfully). Dropped the
admin-client dependency entirely; the broker's
`KAFKA_NUM_PARTITIONS` (compose template) is set to match this script's own
`DEFAULT_PARTITIONS`, so the common case (default `--partitions`) is
auto-created with the right count with zero admin-client involvement.
`--partitions` is still accepted and validated (a topic already created with
a different count is a broker-side fact this script can't retroactively
change), but a non-default value only takes effect if the topic doesn't
exist yet *and* the broker's own default happens to match, or the operator
creates the topic out-of-band (`kafka-topics.sh --create --partitions N`)
before running this script -- flagged with a one-line warning, not silently
ignored.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

try:
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover
    print(
        "ERROR: the 'kafka-python' package is required. Install it with:\n"
        "    pip install kafka-python\n"
        "(already baked into the spark-driver image for in-cluster runs, D4).",
        file=sys.stderr,
    )
    sys.exit(1)

DEFAULT_BOOTSTRAP = "kafka:9092"   # in-cluster DNS (D3); host runs pass --bootstrap 127.0.0.1:9092
DEFAULT_TOPIC = "events"
DEFAULT_RATE = 100.0
DEFAULT_PARTITIONS = 3             # must match compose/templates/docker-compose.yml.j2's KAFKA_NUM_PARTITIONS
DEFAULT_KEY_SPACE = 8
DEFAULT_LATE_FRAC = 0.05
DEFAULT_LATE_SECONDS = 60


def _raise_keyboard_interrupt(signum, frame) -> None:
    """SIGTERM (sent by `driver/playbook/producer.py::stop()` or a plain
    `kill`) doesn't raise KeyboardInterrupt by default -- only SIGINT does --
    so the produce loop's flush-and-exit `finally` block below would
    otherwise be skipped on SIGTERM. Route both signals through the same
    clean-shutdown path (module docstring: 'until SIGINT/SIGTERM')."""
    raise KeyboardInterrupt()


def _validate_args(args: argparse.Namespace) -> None:
    """Input validation at the boundary (G4's datagen-generator precedent,
    D5): reject nonsense with a clear error, not a silent no-op."""
    errors = []
    if not args.topic or not args.topic.strip():
        errors.append("--topic must not be empty")
    if args.rate <= 0:
        errors.append("--rate must be a positive number")
    if args.partitions <= 0:
        errors.append("--partitions must be a positive integer")
    if not (0.0 <= args.late_frac <= 1.0):
        errors.append("--late-frac must be between 0.0 and 1.0")
    if args.key_space <= 0:
        errors.append("--key-space must be a positive integer")
    if args.count is not None and args.count <= 0:
        errors.append("--count must be a positive integer")

    if errors:
        print("ERROR: invalid arguments:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)


def _warn_if_partitions_uncontrollable(args: argparse.Namespace) -> None:
    """No admin-client topic pre-creation here (module docstring's "Partition
    count" deviation note) -- the broker auto-creates the topic on first send
    (KAFKA_AUTO_CREATE_TOPICS_ENABLE=true) with its own KAFKA_NUM_PARTITIONS
    default (compose template, kept equal to DEFAULT_PARTITIONS). A
    --partitions value that doesn't match that broker default has no effect
    on an as-yet-nonexistent topic -- flag it clearly instead of silently
    ignoring it."""
    if args.partitions != DEFAULT_PARTITIONS:
        print(
            f"WARNING: --partitions {args.partitions} was requested, but this script no longer "
            f"pre-creates the topic (see module docstring) -- an auto-created topic gets the "
            f"broker's own KAFKA_NUM_PARTITIONS default ({DEFAULT_PARTITIONS}), not this value. "
            f"Create {args.topic!r} out-of-band first (e.g. kafka-topics.sh --create "
            f"--partitions {args.partitions}) if you need a different count.",
            file=sys.stderr,
        )


def _make_event(key_space: int, late_frac: float, late_seconds: int) -> dict:
    now = datetime.now(timezone.utc)
    if random.random() < late_frac:
        now -= timedelta(seconds=late_seconds)
    return {
        "key": f"key-{random.randrange(key_space)}",
        "event_time": now.isoformat(),
        "value": round(random.uniform(0.0, 100.0), 3),
    }


def _make_producer(bootstrap: str, **kwargs) -> "KafkaProducer":
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        **kwargs,
    )


def produce(args: argparse.Namespace) -> int:
    _warn_if_partitions_uncontrollable(args)
    producer = _make_producer(args.bootstrap)

    interval_s = 1.0 / args.rate
    flush_every = max(int(args.rate), 1)
    sent = 0
    print(
        f"Publishing to {args.bootstrap} topic={args.topic!r} rate={args.rate}/s "
        f"partitions={args.partitions} late_frac={args.late_frac} "
        f"({'until Ctrl-C/SIGTERM' if args.count is None else f'{args.count} events'})..."
    )
    try:
        while args.count is None or sent < args.count:
            event = _make_event(args.key_space, args.late_frac, args.late_seconds)
            producer.send(args.topic, key=event["key"], value=event)
            sent += 1
            if sent % flush_every == 0:
                producer.flush()
            time.sleep(interval_s)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
    print(f"Sent {sent} events.")
    return 0


def self_check(args: argparse.Namespace) -> int:
    """The smallest runnable check that fails if the produce loop or the
    Kafka connection breaks (D5's 'one runnable check' requirement):
    publishes N events to a throwaway topic and asserts N were accepted."""
    n = args.count or 20
    topic = f"selfcheck-{uuid.uuid4().hex[:8]}"

    producer = _make_producer(args.bootstrap, acks="all")
    accepted = 0
    try:
        futures = []
        for _ in range(n):
            event = _make_event(args.key_space, 0.0, args.late_seconds)
            futures.append(producer.send(topic, key=event["key"], value=event))
        producer.flush()
        for future in futures:
            future.get(timeout=10)  # raises on a broker-reported failure
            accepted += 1
    finally:
        producer.close()

    assert accepted == n, f"self-check failed: only {accepted}/{n} events were accepted on {args.bootstrap!r}"
    print(f"SELF-CHECK OK: {accepted}/{n} events accepted on {args.bootstrap} (topic={topic!r}).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Spark Playbook synthetic Kafka producer (Phase 3)")
    p.add_argument(
        "--bootstrap", default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap servers (default {DEFAULT_BOOTSTRAP!r}, in-cluster DNS; "
             "host runs pass 127.0.0.1:9092 -- not 'localhost', see module docstring, D3/OQ-1).",
    )
    p.add_argument("--topic", default=DEFAULT_TOPIC, help=f"Topic to publish to (default {DEFAULT_TOPIC!r}).")
    p.add_argument("--rate", type=float, default=DEFAULT_RATE, help=f"Target events/sec (default {DEFAULT_RATE}).")
    p.add_argument(
        "--partitions", type=int, default=DEFAULT_PARTITIONS,
        help=f"Expected/desired partition count (default {DEFAULT_PARTITIONS}); only takes effect via "
             "the broker's own auto-create default -- see module docstring's 'Partition count' note.",
    )
    p.add_argument(
        "--key-space", type=int, default=DEFAULT_KEY_SPACE,
        help=f"Number of distinct keys (default {DEFAULT_KEY_SPACE}) -- kept small so stateful "
             "aggregation has real groups.",
    )
    p.add_argument(
        "--late-frac", type=float, default=DEFAULT_LATE_FRAC,
        help=f"Fraction of events with a back-dated event_time, for watermark/late-data demos "
             f"(default {DEFAULT_LATE_FRAC}).",
    )
    p.add_argument(
        "--late-seconds", type=int, default=DEFAULT_LATE_SECONDS,
        help=f"How far back a late event's event_time is dated, in seconds (default {DEFAULT_LATE_SECONDS}).",
    )
    p.add_argument(
        "--count", type=int, default=None,
        help="Publish exactly this many events then stop, instead of running until SIGINT/SIGTERM.",
    )
    p.add_argument(
        "--self-check", action="store_true",
        help="Publish a small throwaway batch and assert it was accepted, then exit (D5's runnable check).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args)
    if args.self_check:
        return self_check(args)
    return produce(args)


if __name__ == "__main__":
    sys.exit(main())
