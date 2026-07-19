# tools/kafka_producer — synthetic Kafka producer (Phase 3)

Rate-controlled CLI that publishes bounded, keyed JSON events to a Kafka
topic, for the streaming-topic notebook (#18) to exercise watermarks and
checkpoint-recovery against. Design: `docs/architecture/kafka-streaming-infra.md`
(D5). Only runs when a topic's manifest sets `requires_kafka: true`, which
also conditionally spawns the `kafka` broker service (D1/D2).

## Prerequisites

- `kafka-python` — already baked into the `sparkpb/spark` image
  (`compose/Dockerfile.spark`, D4) for in-cluster runs. For a host-shell run:
  ```bash
  pip install kafka-python
  ```

## Usage

**In-cluster** (inside the `spark-driver` container, reaches the broker by
Docker DNS at `kafka:9092`, D3):

```bash
docker exec spark-driver python /workspace/tools/kafka_producer/produce.py \
    --topic events --rate 100
```

**From a host shell** (reaches the loopback host publish at `127.0.0.1:9092`,
D3/OQ-1 — **use the literal IP, not `localhost`**: verified live that
`localhost` resolves IPv6 first on Windows, nothing is published on `::1`,
and the client never falls back in time, same class of issue
`app/config.py::CLUSTER_HOST` already documents for the app's own calls):

```bash
python tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 --topic events
```

Runs until `Ctrl-C`/`SIGTERM`, or for exactly `--count` events if given.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | `kafka:9092` | Kafka bootstrap servers. |
| `--topic` | `events` | Topic to publish to (must be non-empty). |
| `--rate` | `100` | Target events/sec (must be positive; approximate, not hard-real-time). |
| `--partitions` | `3` | Expected/desired partition count — see the caveat below (must be positive). |
| `--key-space` | `8` | Number of distinct keys — kept small so stateful aggregation has real groups. |
| `--late-frac` | `0.05` | Fraction of events with a back-dated `event_time`, for watermark/late-data demos. |
| `--late-seconds` | `60` | How far back a late event's `event_time` is dated. |
| `--count` | (unset) | Publish exactly this many events, then stop. |
| `--self-check` | off | Publish a small throwaway batch and assert it was accepted, then exit. |

### Event schema

```json
{"key": "key-3", "event_time": "2026-07-19T12:34:56.789012+00:00", "value": 42.5}
```

Deliberately generic/minimal (D5/OQ-2) — #18 refines the exact lesson schema
(fields, key semantics, window size) as it builds the streaming notebook.

### `--partitions` caveat (deviation from the original design)

The topic is **not** pre-created via an admin client — verified live that
`kafka-python-ng`'s `KafkaAdminClient` raises `NodeNotReadyError` doing
controller discovery against this single-node combined broker+controller
KRaft setup (a documented client-library limitation, not a config mistake;
the plain producer path works fine). Instead, the broker's own
`KAFKA_NUM_PARTITIONS` (`compose/templates/docker-compose.yml.j2`) is set to
match this script's `DEFAULT_PARTITIONS` (3), so the default case is
auto-created with the right count with no admin client involved. Passing a
different `--partitions` only prints a warning — it has no effect unless you
create the topic out-of-band first (`kafka-topics.sh --create --partitions
N`) or bump the broker's `KAFKA_NUM_PARTITIONS` to match.

## Self-check

```bash
python tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 --self-check
```

Publishes a small batch of events to a throwaway `selfcheck-<random>` topic
and asserts every one was accepted by the broker — the smallest thing that
fails if the produce loop or the Kafka connection breaks. Requires a live
broker (spawn a streaming topic first, or `compose/cli.py render
--include-kafka && compose/cli.py up`).

## Notebook usage (`driver/playbook/producer.py`)

A thin wrapper that launches `produce.py` as its own background OS process
(not a call into the current kernel), so a streaming query can be
stopped/restarted against its checkpoint without touching the data feed:

```python
import sys
sys.path.insert(0, "/workspace")
from driver.playbook import producer

proc = producer.start(topic="events", rate=100)
# ... run/stop/restart the streaming query in other cells ...
producer.stop(proc)
```
